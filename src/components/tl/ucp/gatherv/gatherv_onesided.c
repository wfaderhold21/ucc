/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided gatherv: root-driven get (plan 5.3). Unlike gather-put, a
 * non-root CANNOT compute where its block belongs in the root's aggregated
 * dst: only the root holds the per-rank counts and displacements. So the
 * leaf-to-root data path runs in two phases:
 *
 *   1. Fanin. Every non-root rank publishes "my src is ready" with a +1 on
 *      the ROOT's copy of slot 2 (the symmetric global work buffer), using
 *      ucc_tl_ucp_atomic_add. The root local-copies its own block into
 *      dst + displs[root] (its own get would be a local-to-local copy).
 *
 *   2. Get. Once the root's slot 2 has received the size-1 signals, the root
 *      issues one get per peer p into dst + displs[p] * dt, reading
 *      counts[p] * dt from p's src. The get target is the root's *local* src
 *      VA at offset 0; because every rank registers its src at the same
 *      symmetric offset (I1), that local VA resolves to peer p's src at the
 *      same offset. A get completes when the data is in the local
 *      destination (I5), so no flush/signal is needed on the data path.
 *
 * Slot choice (I7): only the ROOT's local slot 2 advances (by size-1 per
 * round); a non-root's local slot 2 is never touched (its atomic targets the
 * root's slot). This is the same fanin root-only-commit topology as
 * gather-put, and the slot table reserves slot 2 for the reduce family /
 * gather-get / gatherv, so gatherv uses slot 2 here. The root commits
 * base = expected on completion; non-roots commit nothing.
 *
 * Completion accounting (I7): the root's local slot 2 advances by (size-1),
 * so the root commits base = expected once the gets land; non-roots' local
 * slot 2 is untouched, so they complete as soon as their own fanin atomic is
 * delivered (P2P_COMPLETE) and commit nothing.
 *
 * The get cursor reuses the task's shared onesided.get_posted counter (the
 * same word get_nb/atomic_add bump, reset by task_reset). The root posts no
 * fanin atomic -- it local-copies its own block -- so on the root
 * get_posted counts only the gets it has issued and is a clean peer cursor,
 * exactly as alltoall_onesided_get_progress uses it. The root posts no
 * atomic, so get_posted is never inflated by fanin accounting.
 *
 * Posting the fanin atomic happens exactly once: start() is invoked once by
 * the framework at submit (single-shot), and progress() -- re-invoked while
 * INPROGRESS -- only posts the root's gets (resumable via get_posted) and
 * never re-posts the non-root atomic. This mirrors the fanin/gather
 * single-post discipline (see the I4 note in gather_onesided.c).
 *
 * Preconditions (I2/I3, enforced in init): no in-place; every rank supplies
 * a full-size aggregated dst and a per-rank src, both registered as
 * symmetric (segment) buffers; the symmetric global work buffer carries the
 * fanin signals. check_args requires only REQ_GWB (as in gather-put): the
 * data path resolves by symmetric offset in segment mode, and the perftest
 * GLOBAL-memh variant passes its src/dst global handles straight into get_nb
 * without forcing them through check_args.
 */

#include "config.h"
#include "tl_ucp.h"
#include "gatherv.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

#define GATHERV_ONESIDED_SLOT 2

/*
 * Non-root fanin post: signal the root's slot 2 (symmetric work buffer).
 * Returns UCC_OK for the root (it posts nothing; it only waits on its slot).
 */
static ucc_status_t
ucc_tl_ucp_gatherv_onesided_fanin_post(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_rank_t         rank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         root = TASK_ARGS(task).root;

    if (root == rank) {
        return UCC_OK;
    }
    /* Segment mode (memh == NULL): resolve the root's slot by symmetric
     * offset (I1). */
    return ucc_tl_ucp_atomic_add(ucc_tl_ucp_onesided_slot(task,
                                                          GATHERV_ONESIDED_SLOT),
                                 1, root, NULL, team, task);
}

ucc_status_t ucc_tl_ucp_gatherv_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_rank_t         rank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         root = TASK_ARGS(task).root;
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    ucc_coll_args_t  *args  = &TASK_ARGS(task);
    ucc_status_t       status;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* Completion target (I7): the root waits for one +1 per non-root.
     * (size-1, not size: the root does not self-signal.) */
    task->gatherv_onesided.expected =
        ucc_tl_ucp_onesided_expected(task, GATHERV_ONESIDED_SLOT, gsize - 1);
    /* get cursor for the root's phase-2 loop (reset each round). */
    task->gatherv_onesided.peer = 0;

    /* Root local-copies its own block into dst + displs[root] now; a get
     * from root to root would be a local-to-local copy, so do it inline. */
    if (root == rank) {
        size_t dt       = ucc_dt_size(args->dst.info_v.datatype);
        size_t my_count = ucc_coll_args_get_count(args,
                                    args->dst.info_v.counts, root);
        size_t my_displ = ucc_coll_args_get_displacement(
                                    args, args->dst.info_v.displacements,
                                    root);
        status = ucc_mc_memcpy(PTR_OFFSET(args->dst.info_v.buffer,
                                          my_displ * dt),
                               args->src.info.buffer,
                               my_count * dt,
                               args->dst.info_v.mem_type,
                               args->src.info.mem_type);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
    }

    /* Single-shot fanin post (non-root). start() is invoked once at submit;
     * progress() never re-posts it. */
    status = ucc_tl_ucp_gatherv_onesided_fanin_post(task);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    /* Enqueue calls progress() once, which begins the completion wait (root)
     * or the fanin-delivery wait (non-root). */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_gatherv_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ucc_rank_t         rank   = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         root   = TASK_ARGS(task).root;
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_coll_args_t  *args    = &TASK_ARGS(task);

    if (UCC_OK != task->super.status &&
        UCC_INPROGRESS != task->super.status) {
        return; /* error already recorded by a completion callback */
    }

    if (root != rank) {
        /* Non-root: its local slot 2 is untouched by this collective, so it
         * completes once its own fanin atomic to the root has been delivered
         * and commits nothing (its slot-2 base is unchanged). */
        ucc_tl_ucp_onesided_wait_completion(
            task, task->gatherv_onesided.window.npolls);
        if (UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
            task->super.status = UCC_OK;
        }
        return;
    }

    /* Root: phase 1 -- wait for the size-1 fanin signals. This is a plain
     * slot-value test (NOT ucc_tl_ucp_test_onesided_slot, which also requires
     * P2P_COMPLETE and would falsely fail once the root's gets are in flight
     * after a pacing yield). The slot only grows, so this is idempotent and
     * re-entry simply resumes the phase-2 get loop from gatherv_onesided.peer. */
    if (*ucc_tl_ucp_onesided_slot(task, GATHERV_ONESIDED_SLOT) <
        task->gatherv_onesided.expected) {
        return;
    }

    /* Root: phase 2 -- issue one get per peer p into dst + displs[p] * dt,
     * reading counts[p] * dt from p's src (local src VA at offset 0 -> peer
     * p's src by symmetric offset, I1). Paced by the window; resumed on the
     * next progress(). get_posted is the cursor: the root posts no fanin
     * atomic, so it counts only the gets it has issued. */
    {
        size_t             dt = ucc_dt_size(args->dst.info_v.datatype);
        uint32_t          *posted    = &task->onesided.get_posted;
        uint32_t          *completed = &task->onesided.get_completed;
        ucc_mem_map_mem_h  local_memh;
        ucc_mem_map_mem_h *remote_memh;
        ucc_rank_t         peer = (ucc_rank_t)task->gatherv_onesided.peer;

        /* local buffer = the root's dst (where the get lands): its memh as a
         * value (NULL in segment mode). */
        local_memh = TASK_ARGS(task).dst_memh.global_memh
                         ? TASK_ARGS(task).dst_memh.global_memh[rank]
                         : TASK_ARGS(task).dst_memh.local_memh;
        /* remote target = the peer's src: resolved via the src global array
         * (segment mode: NULL -> symmetric offset, I1). */
        remote_memh = TASK_ARGS(task).src_memh.global_memh;

        /* The get cursor is a resumable peer index (gatherv_onesided.peer):
         * get_nb() drives get_posted itself (line 683), and the root skips
         * itself (local copy), so get_posted is NOT a clean peer index.
         * peer counts every rank visited (incl. the skipped root), so it is
         * a valid resume point and distinct from get_posted. */
        while (peer < gsize) {
            ucc_rank_t p = peer;
            peer += 1;
            task->gatherv_onesided.peer = peer;
            if (p == root) {
                continue; /* own block is local-copied in start() */
            }
            size_t count = ucc_coll_args_get_count(args,
                                       args->dst.info_v.counts, p);
            size_t displ = ucc_coll_args_get_displacement(
                                       args, args->dst.info_v.displacements,
                                       p);
            UCPCHECK_GOTO(
                ucc_tl_ucp_get_nb(PTR_OFFSET(args->dst.info_v.buffer,
                                             displ * dt),
                                  args->src.info.buffer,
                                  count * dt,
                                  args->dst.info_v.mem_type,
                                  p,
                                  local_memh, /* local (root dst) memh */
                                  remote_memh, /* remote (peer src) memh ptr */
                                  team, task),
                task, out);
            if (!ucc_tl_ucp_onesided_window_check(task, posted, completed,
                                                  &task->gatherv_onesided.window)) {
                return;
            }
        }

        /* All gets posted: wait for them to land locally, then commit the
         * advanced slot-2 base (I7). */
        ucc_tl_ucp_onesided_wait_completion(
            task, task->gatherv_onesided.window.npolls);
        if (UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
            task->super.status = UCC_OK;
            ucc_tl_ucp_onesided_commit_slot(task, GATHERV_ONESIDED_SLOT,
                                            task->gatherv_onesided.expected);
        }
    }
out:
    return;
}

ucc_status_t ucc_tl_ucp_gatherv_onesided_init(ucc_base_coll_args_t *coll_args,
                                              ucc_base_team_t      *team,
                                              ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_task_t *task;
    ucc_coll_args_t   *args = &coll_args->args;
    ucc_status_t        status;
    ucc_sbgp_t         *sbgp;
    ucc_rank_t          concurrency = 1;
    size_t              msg_size, dt;
    ucc_rank_t          rank;

    status = ucc_tl_ucp_onesided_check_args(
        coll_args, tl_team, UCC_TL_UCP_ONESIDED_REQ_GWB);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    /* The root must hold the per-rank counts/displacements (the RMA
     * metadata). Non-roots leave them NULL. */
    rank = UCC_TL_TEAM_RANK(tl_team);
    if (rank == args->root &&
        (NULL == args->dst.info_v.counts ||
         NULL == args->dst.info_v.displacements)) {
        tl_debug(UCC_TL_TEAM_LIB(tl_team),
                 "one-sided gatherv requires counts/displacements on root");
        return UCC_ERR_NOT_SUPPORTED;
    }
    /* Every rank provides a full-size dst so the root can address
     * dst + displs[p] at the same symmetric offset (I1). */
    if (NULL == args->dst.info_v.buffer ||
        NULL == args->src.info.buffer) {
        tl_debug(UCC_TL_TEAM_LIB(tl_team),
                 "one-sided gatherv requires a dst and src buffer on every rank");
        return UCC_ERR_NOT_SUPPORTED;
    }

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.post     = ucc_tl_ucp_gatherv_onesided_start;
    task->super.progress = ucc_tl_ucp_gatherv_onesided_progress;

    /* Pacing window for the root's get batch. msg_size = the largest single
     * get. The root reads it from counts[]; non-roots have no counts, so use
     * their own block as a conservative proxy (they post no gets, so the
     * window is only used for their completion-wait npolls). */
    dt       = ucc_dt_size(args->dst.info_v.datatype);
    if (rank == args->root) {
        msg_size = ucc_coll_args_get_max_count(
                       args, args->dst.info_v.counts, UCC_TL_TEAM_SIZE(tl_team))
                   * dt;
    } else {
        msg_size = (size_t)args->src.info.count * dt;
    }
    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);
    if (sbgp->status != UCC_SBGP_NOT_EXISTS) {
        concurrency = sbgp->group_size;
    }
    status = ucc_tl_ucp_onesided_window_init(task, msg_size, concurrency,
                                             &task->gatherv_onesided.window);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    return UCC_OK;
}
