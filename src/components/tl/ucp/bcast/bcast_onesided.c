/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided bcast: root-driven put + atomic signal (I4). The root is the only
 * rank that holds the data, so it is the only rank that posts RMA: for every
 * non-root peer p it puts the whole source buffer (src at offset 0) directly
 * into p's in-place buffer at offset 0 and publishes it with the put ->
 * ep_flush -> atomic_add(1) chain on p's copy of slot 0. The root's own buffer
 * already holds the data (bcast is in-place), so its self-case is just a local
 * slot increment -- no copy, no RMA (the RMA self case, as in fanout: a
 * self-directed RMA is wasteful and unreliable on loopback-only transports).
 *
 * Bcast has a single in-place buffer: args->src.info.{buffer,count,datatype}
 * is both the root's input and every rank's output. There is no separate dst.
 *
 * Slot choice (I7): bcast is a FANOUT topology -- the root signals every rank
 * -- so every rank's local slot 0 advances by exactly 1 per round (the root via
 * its self-increment, each non-root via the root's signal to it). That is the
 * same per-rank lockstep as fanout, so bcast shares slot 0 with the put-family
 * (alltoallv, allgather-put, allgatherv-put, fanout, scatter, scatterv): every
 * rank commits base += 1 on completion.
 *
 * Completion accounting (I7): non-roots post no RMA of their own; they complete
 * once the root's put->flush->signal to them is delivered (*slot >= base + 1),
 * at which point their data is visible (I4/I5). The root completes once all of
 * its put->flush->signal chains to the other ranks have been delivered
 * (P2P_COMPLETE). Both then commit the slot-0 base.
 *
 * The root posts its size-1 put_signals from within progress() (resumable via
 * the bcast_onesided.peer cursor) honoring the pacing window, exactly as
 * scatter and fanout do -- an unthrottled N-way put storm is how linear
 * one-sided collectives lose to two-sided ones at scale (plan 3.4/3.5).
 * start() only enqueues; the first progress() pass begins posting.
 *
 * Addressing (I1): the root computes the put target as its OWN in-place
 * buffer VA at offset 0; because every rank registers its in-place buffer at
 * the same symmetric offset, that resolves to peer p's buffer at offset 0
 * (resolve_p2p_by_va). The RMA target is passed as dst_memh.global_memh; in
 * segment mode (gtest) it is NULL and resolution is by symmetric offset, in
 * global-memh mode (perftest) it is the array of every rank's buffer. The
 * put source is src_memh.local_memh -- NULL in both modes, so the root reads
 * its own local buffer directly.
 *
 * Preconditions (I2/I3, enforced in init): every rank supplies a non-empty
 * in-place buffer; the datatype is predefined and the buffers memory-mapped;
 * a global work buffer is provided. check_args requires only REQ_GWB.
 */

#include "config.h"
#include "tl_ucp.h"
#include "bcast.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

#define BCAST_ONESIDED_SLOT 0

/*
 * Post the root's put_signals for the next targets, honoring the pacing
 * window. Resumable: the next target is recomputed from the peer cursor. The
 * root's own target (peer == root) is a local slot increment -- its buffer
 * already holds the bcast data -- so it does not consume a window token.
 * Returns UCC_OK when all targets are handled, UCC_INPROGRESS when the window
 * requires yielding, or an error.
 */
static ucc_status_t
ucc_tl_ucp_bcast_onesided_post(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_coll_args_t   *args  = &TASK_ARGS(task);
    ucc_rank_t         root  = TASK_ARGS(task).root;
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    long              *slot  = ucc_tl_ucp_onesided_slot(task, BCAST_ONESIDED_SLOT);
    size_t             size  = (size_t)args->src.info.count *
                             ucc_dt_size(args->src.info.datatype);
    ucc_memory_type_t  mtype = args->src.info.mem_type;
    ucc_status_t       status;

    while (task->bcast_onesided.peer < gsize) {
        ucc_rank_t pidx = task->bcast_onesided.peer;
        /* Advance the cursor before issuing the RMA: on a window-yield the
         * loop is re-entered from progress(), and the cursor must point at the
         * *next* target, never a peer that was already posted (a re-post would
         * double-signal that peer and desync the slot base). */
        task->bcast_onesided.peer++;
        ucc_rank_t peer = (root + pidx) % gsize;
        if (peer == root) {
            /* RMA self case: the root's buffer already holds the bcast data;
             * just advance the local slot. Synchronous, so it cannot leave the
             * cursor half-consumed. */
            *slot += 1;
            continue;
        }
        status = ucc_tl_ucp_put_signal(
            args->src.info.buffer,          /* local root buffer @ offset 0   */
            args->src.info.buffer,          /* peer in-place buffer @ offset 0 */
            size, mtype, peer, slot,
            TASK_ARGS(task).src_memh.local_memh,
            TASK_ARGS(task).dst_memh.global_memh, team, task);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
        if (!ucc_tl_ucp_onesided_window_check(
                task, &task->onesided.put_posted,
                &task->onesided.put_completed,
                &task->bcast_onesided.window)) {
            return UCC_INPROGRESS; /* window full: yield, resume later */
        }
    }
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_bcast_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* The resumable posting cursor must start at 0; the task memory is
     * pool-reused (see ucc_tl_ucp_get_task) and task_reset() only zeroes the
     * counter union, so a stale cursor from a previous task would make the
     * root skip every target (post() sees cursor >= gsize and posts nothing,
     * then completes without ever signaling the peers). */
    task->bcast_onesided.peer = 0;
    /* Completion target (I7): every rank's local slot 0 advances by exactly 1
     * this round (root via self-increment, non-roots via the root's signal),
     * so the expected value is base + 1 on every rank. */
    task->bcast_onesided.expected =
        ucc_tl_ucp_onesided_expected(task, BCAST_ONESIDED_SLOT, 1);

    /* ucc_progress_queue_enqueue() calls progress() once, which begins the
     * root's posting or the non-root's slot wait. */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_bcast_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_rank_t         rank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         root = TASK_ARGS(task).root;
    ucc_status_t       status;

    if (UCC_OK != task->super.status &&
        UCC_INPROGRESS != task->super.status) {
        return; /* error already recorded by a completion callback */
    }
    if (root == rank) {
        status = ucc_tl_ucp_bcast_onesided_post(task);
        if (ucc_unlikely(UCC_OK != status && UCC_INPROGRESS != status)) {
            task->super.status = status;
            return;
        }
        if (UCC_INPROGRESS == status) {
            return; /* window full: wait for the next progress round */
        }
        /* All put_signals to the non-roots posted: bounded wait for the chains
         * to be delivered (I5). */
        ucc_tl_ucp_onesided_wait_completion(
            task, task->bcast_onesided.window.npolls);
        if (UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
            task->super.status = UCC_OK;
            ucc_tl_ucp_onesided_commit_slot(task, BCAST_ONESIDED_SLOT,
                                            task->bcast_onesided.expected);
        }
    } else {
        /* Non-root: no RMA of its own; complete once the root's
         * put->flush->signal to it is delivered, which also makes its data
         * visible (I4/I5). Its local slot 0 advanced by 1, so commit. */
        ucc_tl_ucp_onesided_wait_completion(
            task, task->bcast_onesided.window.npolls);
        if (UCC_OK ==
            ucc_tl_ucp_test_onesided_slot(task, BCAST_ONESIDED_SLOT,
                                          task->bcast_onesided.expected)) {
            task->super.status = UCC_OK;
            ucc_tl_ucp_onesided_commit_slot(task, BCAST_ONESIDED_SLOT,
                                            task->bcast_onesided.expected);
        }
    }
}

ucc_status_t ucc_tl_ucp_bcast_onesided_init(ucc_base_coll_args_t *coll_args,
                                            ucc_base_team_t      *team,
                                            ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t  *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_coll_args_t    *args    = &coll_args->args;
    ucc_tl_ucp_task_t  *task;
    ucc_status_t        status;
    ucc_sbgp_t         *sbgp;
    ucc_rank_t          concurrency = 1;
    size_t              size;

    status = ucc_tl_ucp_onesided_check_args(
        coll_args, tl_team, UCC_TL_UCP_ONESIDED_REQ_GWB);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    size = (size_t)args->src.info.count *
           ucc_dt_size(args->src.info.datatype);
    if (0 == args->src.info.count || NULL == args->src.info.buffer) {
        tl_debug(UCC_TL_TEAM_LIB(tl_team),
                 "one-sided bcast requires a non-empty in-place buffer on "
                 "every rank");
        return UCC_ERR_NOT_SUPPORTED;
    }

    task                = ucc_tl_ucp_init_task(coll_args, team);
    *task_h             = &task->super;
    task->super.post    = ucc_tl_ucp_bcast_onesided_start;
    task->super.progress = ucc_tl_ucp_bcast_onesided_progress;

    /* Pacing window (plan 3.4): bounds the root's outstanding put_signals. */
    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);
    if (sbgp->status != UCC_SBGP_NOT_EXISTS) {
        concurrency = sbgp->group_size;
    }
    status = ucc_tl_ucp_onesided_window_init(task, size, concurrency,
                                             &task->bcast_onesided.window);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    return UCC_OK;
}
