/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided fanout: a pure-signal collective with no data movement. The root
 * signals every rank's copy of slot 0 (the symmetric work buffer): one +1 to
 * each peer via an atomic, and one +1 to its own slot via a local increment
 * (the RMA self case -- a self-directed atomic is a wasteful and, on
 * loopback-only transports, unreliable self-RMA, so it is done locally).
 * After the round, every rank's local slot 0 is base + 1. Non-root ranks
 * complete once the root's signal to them has been delivered (*slot >= base +
 * 1); the root completes once all of its atomics to the other ranks have been
 * delivered (its own slot is updated synchronously).
 *
 * Completion accounting (I7): the slot base is per-rank bookkeeping that must
 * track each rank's OWN local slot-0 value. In fanout every rank's local slot 0
 * advances by exactly 1 each round (the root via its local self-signal, each
 * non-root via the root's signal to it), so every rank commits base += 1.
 *
 * No data is transferred, so there is no put/flush ordering to protect (I4
 * does not apply) and no data memh is required (REQ_NO_DATA). Only the
 * global work buffer is needed (REQ_GWB), addressed by symmetric offset (I1)
 * in segment mode -- no mem handles.
 */

#include "config.h"
#include "tl_ucp.h"
#include "fanout.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

/*
 * Post the root's signals for the next peers, honoring the pacing window.
 * Resumable: the next peer is recomputed from get_posted, which each
 * ucc_tl_ucp_atomic_add() bumps (atomics are accounted in the get counters).
 * The root's own signal (peer == root) is a local increment of the local slot,
 * completed synchronously, so it bumps both get_posted and get_completed.
 * Returns UCC_OK when all size signals are posted, UCC_INPROGRESS when the
 * window requires yielding, or an error.
 */
static ucc_status_t
ucc_tl_ucp_fanout_onesided_post(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_rank_t         root  = TASK_ARGS(task).root;
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         peer  = (root + task->onesided.get_posted) % gsize;
    long              *slot  = ucc_tl_ucp_onesided_slot(task, 0);

    for (; task->onesided.get_posted < gsize; peer = (peer + 1) % gsize) {
        if (peer == root) {
            /* RMA self case: increment the local slot directly. The slot is
             * updated synchronously, so account the signal as posted and
             * completed at once (no RMA to issue, no delivery to wait for). */
            *slot += 1;
            task->onesided.get_posted++;
            task->onesided.get_completed++;
            continue;
        }
        ucc_status_t status = ucc_tl_ucp_atomic_add(slot, 1, peer, NULL, team,
                                                     task);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
        if (!ucc_tl_ucp_onesided_window_check(
                task, &task->onesided.get_posted,
                &task->onesided.get_completed,
                &task->fanout_onesided.window)) {
            return UCC_INPROGRESS; /* window full: yield, resume later */
        }
    }
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_fanout_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* Completion target (I7). Non-roots wait for the root's signal (base + 1);
     * the root completes once all of its atomics are delivered. */
    task->fanout_onesided.expected =
        ucc_tl_ucp_onesided_expected(task, 0, 1);

    /* ucc_progress_queue_enqueue() calls progress() once, which begins the
     * posting (root) or the slot wait (non-root). */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_fanout_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ucc_rank_t         rank   = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         root   = TASK_ARGS(task).root;
    ucc_status_t       status;

    if (UCC_OK != task->super.status &&
        UCC_INPROGRESS != task->super.status) {
        return; /* error already recorded by a callback */
    }
    if (root == rank) {
        status = ucc_tl_ucp_fanout_onesided_post(task);
        if (ucc_unlikely(UCC_OK != status && UCC_INPROGRESS != status)) {
            task->super.status = status;
            return;
        }
        if (UCC_INPROGRESS == status) {
            return; /* window full: wait for the next progress round */
        }
        /* All atomics to peers posted: bounded wait for their delivery
         * (I5/I6). The root's own slot was updated locally in post, so its
         * local slot is already base + 1. */
        ucc_tl_ucp_onesided_wait_completion(
            task, task->fanout_onesided.window.npolls);
        if (UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
            task->super.status = UCC_OK;
            ucc_tl_ucp_onesided_commit_slot(task, 0,
                                            task->fanout_onesided.expected);
        }
    } else {
        /* Non-root: bounded wait for the root's signal to be delivered. */
        ucc_tl_ucp_onesided_wait_completion(
            task, task->fanout_onesided.window.npolls);
        if (UCC_OK ==
            ucc_tl_ucp_test_onesided_slot(task, 0,
                                          task->fanout_onesided.expected)) {
            task->super.status = UCC_OK;
            ucc_tl_ucp_onesided_commit_slot(task, 0,
                                            task->fanout_onesided.expected);
        }
    }
}

ucc_status_t ucc_tl_ucp_fanout_onesided_init(ucc_base_coll_args_t *coll_args,
                                             ucc_base_team_t      *team,
                                             ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;
    ucc_sbgp_t        *sbgp;
    ucc_rank_t         concurrency = 1;

    status = ucc_tl_ucp_onesided_check_args(
        coll_args, tl_team,
        UCC_TL_UCP_ONESIDED_REQ_GWB | UCC_TL_UCP_ONESIDED_REQ_NO_DATA);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.post     = ucc_tl_ucp_fanout_onesided_start;
    task->super.progress = ucc_tl_ucp_fanout_onesided_progress;

    /* Pacing window (3.4): bounds outstanding atomics per progress round. */
    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);
    if (sbgp->status != UCC_SBGP_NOT_EXISTS) {
        concurrency = sbgp->group_size;
    }
    status = ucc_tl_ucp_onesided_window_init(task, 0, concurrency,
                                             &task->fanout_onesided.window);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    return UCC_OK;
}
