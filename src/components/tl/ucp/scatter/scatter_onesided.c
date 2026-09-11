/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided scatter: root-driven put + atomic signal (I4). The root is the only
 * rank that holds the full source buffer, so it is the only rank that posts RMA:
 * for every non-root peer p it puts block p (src + p*blk) directly into p's dst
 * at offset 0 and publishes it with the put -> ep_flush -> atomic_add(1) chain
 * on p's copy of slot 0. The root local-copies its own block (src + root*blk)
 * into its own dst and advances its own local slot 0 with a plain increment
 * (the RMA self case, as in fanout: a self-directed RMA is wasteful and
 * unreliable on loopback-only transports, so it is done locally).
 *
 * Slot choice (I7): scatter is a FANOUT topology -- the root signals every rank
 * -- so every rank's local slot 0 advances by exactly 1 per round (the root via
 * its self-increment, each non-root via the root's signal to it). That is the
 * same per-rank lockstep as fanout, so scatter shares slot 0 with the put-family
 * (alltoallv, allgather-put, fanout, bcast-linear): every rank commits
 * base += 1 on completion.
 *
 * Completion accounting (I7): non-roots post no RMA of their own; they complete
 * once the root's put->flush->signal to them is delivered (*slot >= base + 1),
 * at which point their data is visible (I4/I5). The root completes once all of
 * its put->flush->signal chains to the other ranks have been delivered
 * (P2P_COMPLETE). Both then commit the slot-0 base.
 *
 * The root posts its size-1 put_signals from within progress() (resumable via
 * the scatter_onesided.peer cursor) honoring the pacing window, exactly as
 * alltoall_onesided and fanout do -- an unthrottled N-way put storm is how
 * linear one-sided collectives lose to two-sided ones at scale (plan 3.4/3.5).
 * start() only enqueues; the first progress() pass begins posting.
 *
 * Addressing (I1): the root computes the put target as its OWN dst VA at offset
 * 0; because every rank registers a dst at the same symmetric offset, that
 * resolves to peer p's dst at offset 0 (resolve_p2p_by_va), in both segment
 * mode (gtest) and global-memh mode (perftest).
 *
 * Preconditions (I2/I3, enforced in init): no in-place (the root would need its
 * block relocated within its own src, which the put layout does not support);
 * every rank supplies a dst block; the root supplies the full src
 * (size * dst.count). check_args requires only REQ_GWB (segment-mode gtest, no
 * data memh fields): the data path resolves by symmetric offset in segment
 * mode, and the perftest GLOBAL-memh variant passes its src/dst global handles
 * straight into put_signal without forcing them through check_args.
 */

#include "config.h"
#include "tl_ucp.h"
#include "scatter.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

#define SCATTER_ONESIDED_SLOT 0

/*
 * Post the root's put_signals for the next targets, honoring the pacing
 * window. Resumable: the next target is recomputed from the peer cursor. The
 * root's own target (peer == root) is a local copy plus a local slot increment,
 * done synchronously (no RMA to issue, no delivery to wait for), so it does not
 * consume a window token. Returns UCC_OK when all targets are handled,
 * UCC_INPROGRESS when the window requires yielding, or an error.
 */
static ucc_status_t
ucc_tl_ucp_scatter_onesided_post(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team     = TASK_TEAM(task);
    ucc_coll_args_t   *args     = &TASK_ARGS(task);
    ucc_rank_t         root     = TASK_ARGS(task).root;
    ucc_rank_t         gsize    = UCC_TL_TEAM_SIZE(team);
    long              *slot     = ucc_tl_ucp_onesided_slot(task,
                                                           SCATTER_ONESIDED_SLOT);
    size_t             blk      = (size_t)args->dst.info.count *
                                  ucc_dt_size(args->dst.info.datatype);
    ucc_memory_type_t  mtype    = args->src.info.mem_type;
    ucc_status_t       status;

    while (task->scatter_onesided.peer < gsize) {
        ucc_rank_t pidx = task->scatter_onesided.peer;
        /* Advance the cursor before issuing the RMA: on a window-yield the
         * loop is re-entered from progress(), and the cursor must point at
         * the *next* target, never a peer that was already posted (a re-post
         * would double-signal that peer and desync the slot base). */
        task->scatter_onesided.peer++;
        ucc_rank_t peer = (root + pidx) % gsize;
        if (peer == root) {
            /* RMA self case: copy the root's own block (src + root*blk) into
             * its dst and advance the local slot directly. Synchronous, so it
             * cannot leave the cursor half-consumed. */
            status = ucc_mc_memcpy(args->dst.info.buffer,
                                   PTR_OFFSET(args->src.info.buffer,
                                              (size_t)root * blk),
                                   blk, args->dst.info.mem_type, mtype);
            if (ucc_unlikely(UCC_OK != status)) {
                return status;
            }
            *slot += 1;
            continue;
        }
        status = ucc_tl_ucp_put_signal(
            PTR_OFFSET(args->src.info.buffer, (size_t)peer * blk),
            args->dst.info.buffer, blk, mtype, peer, slot,
            TASK_ARGS(task).src_memh.local_memh,
            TASK_ARGS(task).dst_memh.global_memh, team, task);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
        if (!ucc_tl_ucp_onesided_window_check(
                task, &task->onesided.put_posted,
                &task->onesided.put_completed,
                &task->scatter_onesided.window)) {
            return UCC_INPROGRESS; /* window full: yield, resume later */
        }
    }
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_scatter_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* The resumable posting cursor must start at 0; the task memory is
     * pool-reused (see ucc_tl_ucp_get_task) and task_reset() only zeroes the
     * counter union, so a stale cursor from a previous task would make the
     * root skip every target (post() sees cursor >= gsize and posts nothing,
     * then completes without ever signaling the peers). */
    task->scatter_onesided.peer = 0;
    /* Completion target (I7): every rank's local slot 0 advances by exactly 1
     * this round (root via self-increment, non-roots via the root's signal),
     * so the expected value is base + 1 on every rank. */
    task->scatter_onesided.expected =
        ucc_tl_ucp_onesided_expected(task, SCATTER_ONESIDED_SLOT, 1);

    /* ucc_progress_queue_enqueue() calls progress() once, which begins the
     * root's posting or the non-root's slot wait. */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_scatter_onesided_progress(ucc_coll_task_t *ctask)
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
        status = ucc_tl_ucp_scatter_onesided_post(task);
        if (ucc_unlikely(UCC_OK != status && UCC_INPROGRESS != status)) {
            task->super.status = status;
            return;
        }
        if (UCC_INPROGRESS == status) {
            return; /* window full: wait for the next progress round */
        }
        /* All put_signals to the non-roots posted (and the root's own block
         * local-copied): bounded wait for the chains to be delivered (I5). */
        ucc_tl_ucp_onesided_wait_completion(
            task, task->scatter_onesided.window.npolls);
        if (UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
            task->super.status = UCC_OK;
            ucc_tl_ucp_onesided_commit_slot(task, SCATTER_ONESIDED_SLOT,
                                            task->scatter_onesided.expected);
        }
    } else {
        /* Non-root: no RMA of its own; complete once the root's
         * put->flush->signal to it is delivered, which also makes its data
         * visible (I4/I5). Its local slot 0 advanced by 1, so commit. */
        ucc_tl_ucp_onesided_wait_completion(
            task, task->scatter_onesided.window.npolls);
        if (UCC_OK ==
            ucc_tl_ucp_test_onesided_slot(task, SCATTER_ONESIDED_SLOT,
                                          task->scatter_onesided.expected)) {
            task->super.status = UCC_OK;
            ucc_tl_ucp_onesided_commit_slot(task, SCATTER_ONESIDED_SLOT,
                                            task->scatter_onesided.expected);
        }
    }
}

ucc_status_t ucc_tl_ucp_scatter_onesided_init(ucc_base_coll_args_t *coll_args,
                                              ucc_base_team_t      *team,
                                              ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_coll_args_t   *args    = &coll_args->args;
    ucc_rank_t          rank   = UCC_TL_TEAM_RANK(tl_team);
    ucc_rank_t          root   = (ucc_rank_t)args->root;
    ucc_rank_t          gsize  = UCC_TL_TEAM_SIZE(tl_team);
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;
    ucc_sbgp_t        *sbgp;
    ucc_rank_t         concurrency = 1;
    size_t             blk;

    status = ucc_tl_ucp_onesided_check_args(
        coll_args, tl_team, UCC_TL_UCP_ONESIDED_REQ_GWB);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    blk = (size_t)args->dst.info.count *
          ucc_dt_size(args->dst.info.datatype);
    if (0 == args->dst.info.count || NULL == args->dst.info.buffer) {
        tl_debug(UCC_TL_TEAM_LIB(tl_team),
                 "one-sided scatter requires a dst block on every rank");
        return UCC_ERR_NOT_SUPPORTED;
    }
    if (rank == root) {
        if (args->src.info.count != (ucc_count_t)(gsize * args->dst.info.count) ||
            NULL == args->src.info.buffer) {
            tl_debug(UCC_TL_TEAM_LIB(tl_team),
                     "one-sided scatter requires the full src on the root");
            return UCC_ERR_NOT_SUPPORTED;
        }
    }

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.post     = ucc_tl_ucp_scatter_onesided_start;
    task->super.progress = ucc_tl_ucp_scatter_onesided_progress;

    /* Pacing window (plan 3.4): bounds the root's outstanding put_signals. */
    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);
    if (sbgp->status != UCC_SBGP_NOT_EXISTS) {
        concurrency = sbgp->group_size;
    }
    status = ucc_tl_ucp_onesided_window_init(task, blk, concurrency,
                                             &task->scatter_onesided.window);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    return UCC_OK;
}
