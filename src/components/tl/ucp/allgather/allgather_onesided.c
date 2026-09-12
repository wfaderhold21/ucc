/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided allgather (put variant, plan 5.1): every rank puts its own
 * per-rank block into every peer's dst at offset grank*blk, and local-copies
 * its own block into its own dst at the same offset. Each remote put is
 * published with the put -> ep_flush -> atomic_add(1) chain on the peer's
 * copy of slot 0 (I4); the self case is a local copy plus a local
 * self-increment of the slot (I8). Every rank's local slot 0 therefore
 * advances by exactly size per round (one +1 from each of the size senders,
 * including the self-increment), so every rank completes once its local slot
 * reaches base + size and commits the slot-0 base (I7).
 *
 * Slot choice (I7): allgather is a full-mesh put (every rank sends to every
 * other), so every rank's local slot 0 advances by size per round -- the same
 * per-rank lockstep as alltoallv (which also signals every rank). allgather
 * shares slot 0 with the put-family (alltoallv, allgather-put, scatter,
 * scatterv, fanout, bcast-linear).
 *
 * Completion accounting (I5/I7): a rank is not done until it has observed the
 * size-1 remote puts land (their flush+signal delivered) AND its own size-1
 * puts + self-copy are posted. ucc_tl_ucp_test_onesided_slot() requires
 * P2P_COMPLETE (all local put/flush counters drained) AND *slot >= expected,
 * so it covers both: the local RMA is drained and the remote signals have
 * arrived. Counters are never reset, so a fast peer racing into the next
 * collective cannot lose a signal (I7).
 *
 * Posting is resumable: the next peer is recomputed from the shared
 * onesided.put_posted cursor (reset by task_reset, incremented on every put),
 * honoring the pacing window (3.4) -- an unthrottled N-way put storm is how
 * linear one-sided collectives lose to two-sided ones at scale. start() only
 * enqueues; the first progress() pass begins posting.
 *
 * Addressing (I1): a rank puts to peer p using its OWN dst VA at offset
 * grank*blk as the target base; because every rank registers a dst at the
 * same symmetric offset (dst.info.buffer, full size*size), that resolves to
 * peer p's dst at offset grank*blk, in both segment mode (gtest) and
 * global-memh mode (perftest).
 *
 * Preconditions (I2/I3, enforced in init): no in-place (the put layout is
 * non-in-place; in-place allgather is left to the two-sided algorithms);
 * every rank supplies a src block (count) and a full dst (size*count);
 * predefined datatype; memory-mapped buffers; global work buffer. check_args
 * requires only REQ_GWB (segment-mode gtest, no data memh fields): the data
 * path resolves by symmetric offset in segment mode, and the perftest
 * GLOBAL-memh variant passes its src/dst global handles straight into
 * put_signal without forcing them through check_args.
 */

#include "config.h"
#include "tl_ucp.h"
#include "allgather.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

#define ALLGATHER_ONESIDED_SLOT 0

/*
 * Post put_signal(s) for the next peers, honoring the pacing window
 * (resumable: the next peer is recomputed from put_posted, as in
 * alltoallv_onesided). The self target (peer == rank) is a local copy plus a
 * local slot increment, done synchronously (no RMA to issue, no delivery to
 * wait for), so it does not consume a window token. Returns UCC_OK when all
 * targets are handled, UCC_INPROGRESS when the window requires yielding, or
 * an error.
 */
static ucc_status_t
ucc_tl_ucp_allgather_onesided_post(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_coll_args_t   *args  = &TASK_ARGS(task);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         rank  = UCC_TL_TEAM_RANK(team);
    long              *slot  = ucc_tl_ucp_onesided_slot(task,
                                                        ALLGATHER_ONESIDED_SLOT);
    size_t             dt    = ucc_dt_size(args->src.info.datatype);
    size_t             blk   = (size_t)args->src.info.count * dt;
    ucc_memory_type_t  mtype = args->src.info.mem_type;

    /* The cursor is the task's own allgather_onesided.peer (initialized to 0
     * in start(); the task memory is pool-reused, so it cannot be trusted
     * from a previous task). ucc_tl_ucp_put_signal advances
     * onesided.put_posted itself for the size-1 remote puts, and the window
     * check consumes from it -- so put_posted ends exactly at size-1, and
     * P2P_COMPLETE (put_posted == put_completed) holds. The self target is a
     * local copy (I8), no RMA, so it does not touch put_posted. */
    while (task->allgather_onesided.peer < gsize) {
        ucc_rank_t pidx = task->allgather_onesided.peer;
        /* Advance the cursor before issuing the RMA: on a window-yield the
         * loop is re-entered from progress(), and the cursor must point at
         * the *next* target, never a peer that was already posted (a re-post
         * would double-signal that peer and desync the slot base). */
        task->allgather_onesided.peer++;
        ucc_rank_t peer = (rank + pidx) % gsize;
        void       *dst = PTR_OFFSET(args->dst.info.buffer,
                                     (size_t)rank * blk);
        ucc_status_t status;
        if (peer == rank) {
            /* RMA self case (I8): copy this rank's own block into its dst at
             * offset rank*blk and advance the local slot directly. Synchronous,
             * so it cannot leave the cursor half-consumed. No RMA is issued,
             * so put_posted is untouched (it counts only the size-1 remote
             * puts, matching put_completed on P2P_COMPLETE). */
            status = ucc_mc_memcpy(dst, args->src.info.buffer, blk,
                                   args->dst.info.mem_type, mtype);
            if (ucc_unlikely(UCC_OK != status)) {
                return status;
            }
            *slot += 1;
            continue;
        }
        /* Put this rank's block (src at offset 0) into peer p's dst at
         * offset rank*blk, signaling p's slot 0 on delivery (I4). The target
         * is the rank's OWN dst VA at offset rank*blk, which I1 resolves to
         * peer p's dst at the same offset. */
        status = ucc_tl_ucp_put_signal(
            args->src.info.buffer, dst, blk, mtype, peer, slot,
            TASK_ARGS(task).src_memh.local_memh,
            TASK_ARGS(task).dst_memh.global_memh, team, task);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
        if (!ucc_tl_ucp_onesided_window_check(
                task, &task->onesided.put_posted,
                &task->onesided.put_completed,
                &task->allgather_onesided.window)) {
            return UCC_INPROGRESS; /* window full: yield, resume later */
        }
    }
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_allgather_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* The resumable posting cursor must start at 0; the task memory is
     * pool-reused (see ucc_tl_ucp_get_task) and task_reset() only zeroes the
     * counter union, so a stale cursor from a previous task would make the
     * rank skip every target (post() sees cursor >= gsize and posts nothing,
     * then completes without ever signaling the peers). */
    task->allgather_onesided.peer = 0;
    /* Completion target (I7): every rank's local slot 0 advances by exactly
     * size this round (size-1 remote signals + 1 self-increment), so the
     * expected value is base + size on every rank. */
    task->allgather_onesided.expected =
        ucc_tl_ucp_onesided_expected(task, ALLGATHER_ONESIDED_SLOT,
                                     UCC_TL_TEAM_SIZE(team));

    /* ucc_progress_queue_enqueue() calls progress() once, which begins the
     * posting; the task is enqueued if it is not done after that first pass. */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_allgather_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_status_t       status;

    if (UCC_OK != task->super.status &&
        UCC_INPROGRESS != task->super.status) {
        return; /* error already recorded by a completion callback */
    }
    status = ucc_tl_ucp_allgather_onesided_post(task);
    if (ucc_unlikely(UCC_OK != status && UCC_INPROGRESS != status)) {
        task->super.status = status;
        return;
    }
    if (UCC_INPROGRESS == status) {
        return; /* window full: wait for the next progress round */
    }
    /* All puts posted (and the self-copy done): bounded wait for the local
     * RMA to drain and the remote signals to be delivered (I4/I5). */
    ucc_tl_ucp_onesided_wait_completion(
        task, task->allgather_onesided.window.npolls);
    if (UCC_OK ==
        ucc_tl_ucp_test_onesided_slot(task, ALLGATHER_ONESIDED_SLOT,
                                      task->allgather_onesided.expected)) {
        task->super.status = UCC_OK;
        ucc_tl_ucp_onesided_commit_slot(task, ALLGATHER_ONESIDED_SLOT,
                                        task->allgather_onesided.expected);
    }
}

ucc_status_t ucc_tl_ucp_allgather_onesided_init(ucc_base_coll_args_t *coll_args,
                                                ucc_base_team_t      *team,
                                                ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_coll_args_t   *args    = &coll_args->args;
    ucc_tl_ucp_task_t *task;
    ucc_sbgp_t        *sbgp;
    ucc_rank_t         concurrency = 1;
    size_t             blk;
    ucc_status_t       status;

    status = ucc_tl_ucp_onesided_check_args(
        coll_args, tl_team, UCC_TL_UCP_ONESIDED_REQ_GWB);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    blk = (size_t)args->src.info.count * ucc_dt_size(args->src.info.datatype);
    if (0 == args->src.info.count || NULL == args->src.info.buffer ||
        NULL == args->dst.info.buffer) {
        tl_debug(UCC_TL_TEAM_LIB(tl_team),
                 "one-sided allgather requires a src block and a full dst on "
                 "every rank");
        return UCC_ERR_NOT_SUPPORTED;
    }

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.post     = ucc_tl_ucp_allgather_onesided_start;
    task->super.progress = ucc_tl_ucp_allgather_onesided_progress;

    /* Pacing window (plan 3.4): bounds the outstanding put_signals; msglen =
     * the per-peer block, concurrency = processes per node sharing the NIC. */
    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);
    if (sbgp->status != UCC_SBGP_NOT_EXISTS) {
        concurrency = sbgp->group_size;
    }
    status = ucc_tl_ucp_onesided_window_init(task, blk, concurrency,
                                             &task->allgather_onesided.window);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    return UCC_OK;
}
