/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided fanin: a pure-signal collective with no data movement. Every
 * non-root rank atomically adds 1 to the ROOT's copy of slot 1 (the symmetric
 * work buffer); the root completes once its local slot 1 has received the
 * size-1 peer signals (expected = base + (size - 1)). Non-root ranks complete
 * once their own atomic to the root has been delivered.
 *
 * Fanin uses slot 1, not slot 0: in fanin the root's local slot advances by
 * (size-1) per round while a non-root's local slot never changes, whereas
 * slot 0 (the put-family / fanout slot) grows by exactly 1 on every rank. The
 * per-rank base must track each rank's own local slot value, so sharing slot 0
 * with fanin would desynchronize the base and a fanout/fanin race would lose a
 * signal (I7). Keeping fanin on its own slot keeps the bases in lockstep.
 *
 * Completion accounting (I7): the root's local slot 1 advances by (size-1) each
 * round, so the root commits base = expected; non-roots' local slot 1 is never
 * touched by this collective, so they commit nothing and their base is left
 * unchanged.
 *
 * No data is transferred, so there is no put/flush ordering to protect (I4
 * does not apply) and no data memh is required (REQ_NO_DATA). Only the
 * global work buffer is needed (REQ_GWB), addressed by symmetric offset (I1)
 * in segment mode -- no mem handles.
 */

#include "config.h"
#include "tl_ucp.h"
#include "fanin.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

#define FANIN_ONESIDED_SLOT 1

static ucc_status_t
ucc_tl_ucp_fanin_onesided_post(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_rank_t         rank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         root = TASK_ARGS(task).root;

    if (root == rank) {
        return UCC_OK; /* root posts nothing; it only waits on its slot */
    }
    /* Non-root: signal the root's slot (symmetric work buffer). */
    return ucc_tl_ucp_atomic_add(
        ucc_tl_ucp_onesided_slot(task, FANIN_ONESIDED_SLOT), 1, root, NULL,
        team, task);
}

ucc_status_t ucc_tl_ucp_fanin_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* Completion target (I7): the root waits for one +1 per non-root.
     * (size-1, not size: the root does not self-signal in fanin.) */
    task->fanin_onesided.expected =
        ucc_tl_ucp_onesided_expected(task, FANIN_ONESIDED_SLOT, gsize - 1);

    /* Enqueue calls progress() once, which posts (non-root) or begins the
     * slot wait (root). */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_fanin_onesided_progress(ucc_coll_task_t *ctask)
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
    status = ucc_tl_ucp_fanin_onesided_post(task);
    if (ucc_unlikely(UCC_OK != status && UCC_INPROGRESS != status)) {
        task->super.status = status;
        return;
    }
    if (UCC_INPROGRESS == status) {
        return;
    }
    if (root == rank) {
        /* Root: complete once every non-root has delivered its signal. Its
         * local slot advanced by (size-1), so commit the advanced base. */
        if (UCC_OK == ucc_tl_ucp_test_onesided_slot(
                          task, FANIN_ONESIDED_SLOT,
                          task->fanin_onesided.expected)) {
            task->super.status = UCC_OK;
            ucc_tl_ucp_onesided_commit_slot(task, FANIN_ONESIDED_SLOT,
                                            task->fanin_onesided.expected);
        }
    } else {
        /* Non-root: its local slot 1 is untouched by this collective, so it
         * completes once its own atomic to the root has been delivered, and
         * commits nothing (its slot-1 base is unchanged). */
        ucc_tl_ucp_onesided_wait_completion(
            task, task->fanin_onesided.window.npolls);
        if (UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
            task->super.status = UCC_OK;
        }
    }
}

ucc_status_t ucc_tl_ucp_fanin_onesided_init(ucc_base_coll_args_t *coll_args,
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
    task->super.post     = ucc_tl_ucp_fanin_onesided_start;
    task->super.progress = ucc_tl_ucp_fanin_onesided_progress;

    /* npolls for the completion wait (token pacing is moot: at most one
     * atomic is posted per rank). */
    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);
    if (sbgp->status != UCC_SBGP_NOT_EXISTS) {
        concurrency = sbgp->group_size;
    }
    status = ucc_tl_ucp_onesided_window_init(task, 0, concurrency,
                                             &task->fanin_onesided.window);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    return UCC_OK;
}
