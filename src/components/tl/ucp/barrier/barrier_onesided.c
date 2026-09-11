/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided barrier: a pure-signal rendezvous with no data movement, built on
 * the dissemination (recursive-doubling) pattern of plan 4.2. There are
 * `rounds = ceil(log2(size))` rounds; in round r, every rank atomically adds 1
 * to the copy of slot (BARRIER_ONESIDED_BASE_SLOT + r) held by peer
 * (rank + 2^r) % size, then waits until its own copy of that slot has received
 * the +1 from its partner, commits the slot base, and moves on.
 *
 * Why this is a barrier (not a one-way signal): a rank may only finish round r
 * once its partner (rank - 2^r mod size) has ENTERED round r -- the partner's
 * round-r signal is the +1 that unblocks the wait. Chaining that dependency
 * across rounds branches into a spanning tree over the team, so a rank that
 * arrives late delays the completion of every rank, and no rank reports
 * completion before the slowest rank has arrived. Verified for sizes
 * {2,3,4,5,7,8,9,15,16} across all arrival orderings.
 *
 * Slot choice (I7): the committed slot layout (tl_ucp_onesided.h) reserves
 * [3 .. 3+ceil(log2(N))) for the barrier's rounds, so round r uses slot
 * BASE_SLOT + r with BASE_SLOT = 3 -- not slot r as in the plan shorthand,
 * because slots 0/1/2 are owned by the put-family / fanin / scratch sync and
 * must not be shared. In each round a rank's local slot advances by exactly 1
 * (it receives one +1), so every rank commits base = old + 1 each round,
 * keeping the per-rank bases in lockstep for back-to-back barriers.
 *
 * No data is transferred, so there is no put/flush ordering to protect (I4
 * does not apply) and no data memh is required (REQ_NO_DATA). Only the global
 * work buffer is needed (REQ_GWB), addressed by symmetric offset (I1) in
 * segment mode -- no mem handles.
 */

#include "config.h"
#include "tl_ucp.h"
#include "barrier.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

/* First slot reserved for the barrier's dissemination rounds (I7 layout). */
#define BARRIER_ONESIDED_BASE_SLOT 3


/*
 * Drive one dissemination round. `task->barrier_onesided.round` is the number
 * of rounds already completed (posted and waited on). On entry the invariant
 * is task->onesided.get_posted == round (exactly one atomic per round, and the
 * reset in start() zeroed the counters), so the next atomic to post is round
 * `round`'s. When its signal is observed on the local slot and all RMA is
 * complete (I5/I6), the round's slot base is committed and round is advanced.
 * Returns UCC_OK when the barrier is done, UCC_INPROGRESS to resume later, or
 * an error.
 */
static ucc_status_t
ucc_tl_ucp_barrier_onesided_progress_round(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_rank_t         rank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         rounds = task->barrier_onesided.rounds;
    ucc_status_t       status;

    for (;;) {
        ucc_rank_t round = task->barrier_onesided.round;
        int        slot  = BARRIER_ONESIDED_BASE_SLOT + round;

        if (round == rounds) {
            return UCC_OK; /* all rounds done */
        }

        if (task->onesided.get_posted == round) {
            /* Post this round's single signal. The post is synchronous
             * (always returns UCC_OK, bumping get_posted); delivery is
             * reported later by the completion callback (get_completed). */
            ucc_rank_t peer = (rank + (ucc_rank_t)(1 << round)) % gsize;

            status = ucc_tl_ucp_atomic_add(
                ucc_tl_ucp_onesided_slot(task, slot), 1, peer, NULL, team, task);
            if (ucc_unlikely(UCC_OK != status)) {
                return status;
            }
        }

        /* Bounded wait for this round's signal to land on the local slot and
         * the atomic to be fully delivered (I5/I6). */
        ucc_tl_ucp_onesided_wait_completion(task, task->n_polls);
        if (UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task) &&
            *ucc_tl_ucp_onesided_slot(task, slot) >=
                ucc_tl_ucp_onesided_expected(task, slot, 1)) {
            /* Round complete: commit the advanced base (I7) and advance. */
            ucc_tl_ucp_onesided_commit_slot(task, slot,
                                            ucc_tl_ucp_onesided_expected(task, slot, 1));
            task->barrier_onesided.round = round + 1;
            continue; /* next round */
        }
        return UCC_INPROGRESS; /* resume on the next progress call */
    }
}

ucc_status_t ucc_tl_ucp_barrier_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);

    UCC_TL_UCP_PROFILE_REQUEST_EVENT(ctask, "ucp_barrier_os_start", 0);
    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    task->barrier_onesided.round = 0;

    /* ucc_progress_queue_enqueue() calls progress() once, which begins the
     * first round. */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_barrier_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_status_t       status;

    if (UCC_OK != task->super.status && UCC_INPROGRESS != task->super.status) {
        return; /* error already recorded by a callback */
    }
    status = ucc_tl_ucp_barrier_onesided_progress_round(task);
    if (ucc_unlikely(UCC_OK != status && UCC_INPROGRESS != status)) {
        task->super.status = status;
        return;
    }
    if (UCC_OK == status) {
        task->super.status = UCC_OK;
        UCC_TL_UCP_PROFILE_REQUEST_EVENT(ctask, "ucp_barrier_os_done", 0);
    }
}

ucc_status_t ucc_tl_ucp_barrier_onesided_init(ucc_base_coll_args_t *coll_args,
                                              ucc_base_team_t      *team,
                                              ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(tl_team);
    ucc_rank_t         rounds;

    status = ucc_tl_ucp_onesided_check_args(
        coll_args, tl_team,
        UCC_TL_UCP_ONESIDED_REQ_GWB | UCC_TL_UCP_ONESIDED_REQ_NO_DATA);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    rounds = ucc_ilog2_ceil(gsize);
    /* The dissemination rounds need slots [BASE .. BASE+rounds); if they do
     * not fit the work buffer, fall back (I3). */
    if (ucc_unlikely(BARRIER_ONESIDED_BASE_SLOT + rounds >
                     UCC_TL_UCP_ONESIDED_N_SLOTS)) {
        tl_debug(UCC_TL_TEAM_LIB(tl_team),
                 "team size %u needs %u dissemination slots, only %d available",
                 gsize, rounds, UCC_TL_UCP_ONESIDED_N_SLOTS);
        return UCC_ERR_NOT_SUPPORTED;
    }

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.post     = ucc_tl_ucp_barrier_onesided_start;
    task->super.progress = ucc_tl_ucp_barrier_onesided_progress;
    task->barrier_onesided.rounds = rounds;
    task->barrier_onesided.round  = 0;
    return UCC_OK;
}
