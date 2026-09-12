/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided reduce_scatter: direct, single phase (plan 6.2).
 *
 * Data semantics. src is N*rcount elements (N = team size, rcount = the
 * per-rank output length, count / N); dst is rcount elements. Rank r's output
 * is the element-wise reduction, over every rank q, of the r-th block of
 * rank q's src:
 *
 *     dst_r[i] = (op over q in [0, N)) src_q[rcount*r + i]
 *
 * so rank r needs block r of every rank's src.
 *
 * RMA (I1/I4). Each rank r is the ONLY issuer of RMA toward its own output:
 * for every peer p != r it put_signals its own block p (src + p*blk) into
 * peer p's scratch at offset r*blk. The scratch is the internal one-sided
 * scratch segment (plan 6.1), registered at the SAME symmetric offset on every
 * rank, so the put target `scratch + r*blk` (a local VA) resolves by
 * intra-segment offset to peer p's scratch + r*blk (resolve_p2p_by_va, I1).
 * The put -> ep_flush -> atomic_add(1) chain publishes each block (I4/I5).
 * Rank r's own block is never put to itself (I8): r local-copies
 * src + r*blk into scratch + r*blk (the gap no peer writes) and self-
 * increments the slot.
 *
 * After the round, rank r's scratch holds every rank's block r at offset
 * q*blk for q != r plus r's own block at r*blk, i.e. scratch[0..N-1] is fully
 * populated. The local combine reduces all N contiguous scratch blocks into
 * dst via ucc_dt_reduce_strided on the task's executor (non-blocking
 * EXEC_TASK_TEST + SAVE_STATE, per ucc-opt.md 2).
 *
 * Slot choice (I7). This is a full-mesh fanout/fanin: every rank's local
 * slot 0 advances by exactly N per round (N-1 remote atomic_adds targeting it,
 * one from each other rank's put, plus 1 self-increment). That is uniform
 * across all ranks, so every rank commits base += N -- the same per-rank
 * lockstep as the allgather put-family, so reduce_scatter shares slot 0.
 *
 * Completion (I7). post() is resumable via the peer cursor and honors the
 * pacing window (3.4). progress() then waits for P2P_COMPLETE (all local
 * put/flush/atomic counters drained, I5) AND *slot >= base + N (all remote
 * blocks landed and delivered), gap-fills its own block, runs the local
 * reduce, commits the slot-0 base, and releases the scratch region.
 *
 * Preconditions (I2/I3, enforced in init): no in-place, predefined datatype,
 * memory-mapped buffers, global work buffer, count divisible by N, and a
 * scratch region of N*blk bytes available. Any unmet precondition returns
 * UCC_ERR_NOT_SUPPORTED so the core falls back to ring/knomial.
 */

#include "config.h"
#include "tl_ucp.h"
#include "reduce_scatter.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "utils/ucc_dt_reduce.h"
#include "components/ec/ucc_ec.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

#define RS_ONESIDED_SLOT 0

enum {
    UCC_RS_ONESIDED_PHASE_POST = 0,
    UCC_RS_ONESIDED_PHASE_REDUCE
};

#define SAVE_STATE(_phase)                                                     \
    do {                                                                       \
        task->reduce_scatter_onesided.phase = _phase;                          \
    } while (0)

/*
 * Post put_signals for the next peers, honoring the pacing window. Resumable:
 * the next target is recomputed from the peer cursor. The self case
 * (peer == rank) is a local slot increment -- its block is never put to
 * itself (I8) -- so it does not consume a window token. Returns UCC_OK when
 * all targets are handled, UCC_INPROGRESS when the window requires yielding,
 * or an error.
 */
static ucc_status_t
ucc_tl_ucp_reduce_scatter_onesided_post(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_coll_args_t   *args  = &TASK_ARGS(task);
    ucc_rank_t         rank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    long              *slot  = ucc_tl_ucp_onesided_slot(task, RS_ONESIDED_SLOT);
    size_t             blk   = (size_t)args->dst.info.count *
                             ucc_dt_size(args->src.info.datatype);
    ucc_memory_type_t  mtype = args->src.info.mem_type;
    ucc_status_t       status;

    while (task->reduce_scatter_onesided.peer < gsize) {
        ucc_rank_t pidx = task->reduce_scatter_onesided.peer;
        /* Advance the cursor before issuing the RMA: on a window-yield the
         * loop is re-entered from progress(), and the cursor must point at the
         * *next* target, never a peer that was already posted (a re-post would
         * double-signal that peer and desync the slot base). */
        task->reduce_scatter_onesided.peer++;
        ucc_rank_t peer = (rank + pidx) % gsize;
        if (peer == rank) {
            /* RMA self case (I8): this rank's own block is not put to itself;
             * just advance the local slot. Synchronous, so it cannot leave the
             * cursor half-consumed. (The own block is copied into the scratch
             * gap during the reduce phase.) */
            *slot += 1;
            continue;
        }
        /* Put this rank's block for `peer` (src at offset peer*blk) into
         * peer's scratch at offset rank*blk, signaling peer's slot 0 on
         * delivery (I4). Peer `peer`'s output is the reduction over every
         * rank's `peer`-th block, so it needs THIS rank's block `peer`.
         * The target is the rank's OWN scratch VA at offset rank*blk; I1
         * resolves it to peer's scratch at the same offset (the scratch
         * segment is registered at a uniform offset on every rank). The
         * source is the rank's local block -- UCX reads it directly, so
         * src_memh is NULL; the dst is the internal scratch segment, so
         * dst_memh is NULL and resolution is by intra-segment offset. */
        status = ucc_tl_ucp_put_signal(
            PTR_OFFSET(args->src.info.buffer, (size_t)peer * blk),
            PTR_OFFSET(task->reduce_scatter_onesided.scratch, (size_t)rank * blk),
            blk, mtype, peer, slot, NULL, NULL, team, task);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
        if (!ucc_tl_ucp_onesided_window_check(
                task, &task->onesided.put_posted,
                &task->onesided.put_completed,
                &task->reduce_scatter_onesided.window)) {
            return UCC_INPROGRESS; /* window full: yield, resume later */
        }
    }
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_reduce_scatter_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_status_t       status;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* Pool-reused task memory (see ucc_tl_ucp_get_task): task_reset() only
     * zeroes the counter union, so every piece of per-round state must be
     * re-established here or a stale value from a previous task would break
     * this round (a stale cursor would skip every target, a stale
     * gap_filled would skip the own-block copy, a stale etask would test a
     * foreign executor task). */
    task->reduce_scatter_onesided.peer = 0;
    task->reduce_scatter_onesided.phase = UCC_RS_ONESIDED_PHASE_POST;
    task->reduce_scatter_onesided.etask = NULL;
    task->reduce_scatter_onesided.gap_filled = 0;
    /* The task's schedule (and with it the executor) is attached by the core
     * after init, so it can only be resolved here at post time -- the same
     * place knomial and allreduce resolve theirs. */
    status = ucc_coll_task_get_executor(&task->super,
                                        &task->reduce_scatter_onesided.executor);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    /* Completion target (I7): every rank's local slot 0 advances by exactly
     * size this round (size-1 remote signals + 1 self-increment), so the
     * expected value is base + size on every rank. */
    task->reduce_scatter_onesided.expected =
        ucc_tl_ucp_onesided_expected(task, RS_ONESIDED_SLOT,
                                     UCC_TL_TEAM_SIZE(TASK_TEAM(task)));

    /* ucc_progress_queue_enqueue() calls progress() once, which begins
     * posting; the task is enqueued if it is not done after that first pass. */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(TASK_TEAM(task))->pq,
                                      &task->super);
}

void ucc_tl_ucp_reduce_scatter_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_coll_args_t   *args = &TASK_ARGS(task);
    ucc_rank_t         rank = UCC_TL_TEAM_RANK(team);
    size_t             rcount, blk;
    ucc_memory_type_t  mtype;
    ucc_status_t       status;

    if (UCC_OK != task->super.status &&
        UCC_INPROGRESS != task->super.status) {
        return; /* error already recorded by a completion callback */
    }
    /* RMA phase: post the put_signals (resumable, window-bounded). */
    if (UCC_RS_ONESIDED_PHASE_POST == task->reduce_scatter_onesided.phase) {
        status = ucc_tl_ucp_reduce_scatter_onesided_post(task);
        if (ucc_unlikely(UCC_OK != status && UCC_INPROGRESS != status)) {
            task->super.status = status;
            return;
        }
        if (UCC_INPROGRESS == status) {
            return; /* window full: wait for the next progress round */
        }
        task->reduce_scatter_onesided.phase = UCC_RS_ONESIDED_PHASE_REDUCE;
    }

    /* Local-combine phase: once the put_signals are posted, wait for the
     * chains to be delivered (I5) and the remote atomic signals to land on
     * this rank's slot (I7). On first entry the local reduce is then set up
     * (gap-fill + post) and the phase stays REDUCE, so every later entry only
     * tests the reduce task -- the gap-fill and the reduce post each happen
     * exactly once. */
    ucc_tl_ucp_onesided_wait_completion(
        task, task->reduce_scatter_onesided.window.npolls);
    if (UCC_OK !=
        ucc_tl_ucp_test_onesided_slot(task, RS_ONESIDED_SLOT,
                                      task->reduce_scatter_onesided.expected)) {
        return; /* not all remote blocks are visible yet */
    }

    if (NULL == task->reduce_scatter_onesided.etask &&
        UCC_RS_ONESIDED_PHASE_REDUCE == task->reduce_scatter_onesided.phase &&
        !task->reduce_scatter_onesided.gap_filled) {
        size_t size = UCC_TL_TEAM_SIZE(team);
        rcount = (size_t)args->dst.info.count;
        blk    = rcount * ucc_dt_size(args->src.info.datatype);
        mtype  = args->src.info.mem_type;
        /* Gap-fill (I8): no peer put this rank's own block into its scratch,
         * so copy it in now, making scratch[0..size-1] fully populated and
         * contiguous for the combine. Marked done so a re-entry (the reduce
         * task still in flight) does not copy it a second time. */
        status = ucc_mc_memcpy(
            PTR_OFFSET(task->reduce_scatter_onesided.scratch,
                       (size_t)rank * blk),
            PTR_OFFSET(args->src.info.buffer, (size_t)rank * blk), blk,
            UCC_MEMORY_TYPE_HOST, mtype);
        if (ucc_unlikely(UCC_OK != status)) {
            tl_error(UCC_TASK_LIB(task), "failed to copy own block into scratch");
            task->super.status = status;
            return;
        }
        task->reduce_scatter_onesided.gap_filled = 1;
        if (size > 1) {
            /* Local combine: reduce the size scratch blocks (each rcount
             * elements) into dst. src1 = block 0, src2 = block 1, stride =
             * one block, n_vectors = size-1, covering blocks 0..size-1. For
             * UCC_OP_AVG the single-phase full reduce multiplies the sum by
             * 1/size (AVG_ALPHA) to produce the mean. The phase is already
             * REDUCE (set on first entry), so a re-entry only tests the task
             * below. */
            status = ucc_dt_reduce_strided(
                task->reduce_scatter_onesided.scratch,
                PTR_OFFSET(task->reduce_scatter_onesided.scratch, blk),
                args->dst.info.buffer, size - 1, rcount, blk,
                args->src.info.datatype, args,
                args->op == UCC_OP_AVG ? UCC_EEE_TASK_FLAG_REDUCE_WITH_ALPHA : 0,
                AVG_ALPHA(task), task->reduce_scatter_onesided.executor,
                &task->reduce_scatter_onesided.etask);
            if (ucc_unlikely(UCC_OK != status)) {
                tl_error(UCC_TASK_LIB(task), "failed to post local reduce");
                task->super.status = status;
                return;
            }
        } else {
            /* size == 1: the "reduction" is the single block already in
             * scratch; ucc_dt_reduce_strided would no-op (n_vectors = 0),
             * so copy the one block out to dst directly. */
            status = ucc_mc_memcpy(
                args->dst.info.buffer, task->reduce_scatter_onesided.scratch,
                blk, mtype, UCC_MEMORY_TYPE_HOST);
            if (ucc_unlikely(UCC_OK != status)) {
                tl_error(UCC_TASK_LIB(task), "failed to copy result to dst");
                task->super.status = status;
                return;
            }
        }
    }
    if (task->reduce_scatter_onesided.etask != NULL) {
        EXEC_TASK_TEST(UCC_RS_ONESIDED_PHASE_REDUCE,
                       "failed to perform local reduce",
                       task->reduce_scatter_onesided.etask);
        return; /* reduce in flight: test again next round */
    }

    task->super.status = UCC_OK;
    ucc_tl_ucp_onesided_commit_slot(task, RS_ONESIDED_SLOT,
                                    task->reduce_scatter_onesided.expected);
    ucc_tl_ucp_onesided_scratch_release(team);
}

ucc_status_t ucc_tl_ucp_reduce_scatter_onesided_finalize(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);

    /* Release the scratch region if the round never reached its commit (e.g.
     * an early error), so a later collective on this team can reuse it. The
     * refcount is idempotent: scratch_release only decrements when in use. */
    ucc_tl_ucp_onesided_scratch_release(TASK_TEAM(task));
    return ucc_tl_ucp_coll_finalize(ctask);
}

ucc_status_t ucc_tl_ucp_reduce_scatter_onesided_init(
    ucc_base_coll_args_t *coll_args, ucc_base_team_t *team,
    ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_coll_args_t   *args    = &coll_args->args;
    ucc_tl_ucp_task_t *task;
    ucc_rank_t          gsize  = UCC_TL_TEAM_SIZE(tl_team);
    size_t              count  = (size_t)args->src.info.count;
    size_t              dt_size = ucc_dt_size(args->src.info.datatype);
    ucc_sbgp_t         *sbgp;
    ucc_rank_t          concurrency = 1;
    size_t              rcount, blk, scratch_size;
    ucc_status_t        status;

    status = ucc_tl_ucp_onesided_check_args(
        coll_args, tl_team, UCC_TL_UCP_ONESIDED_REQ_GWB);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    if (0 == count || (count % gsize) != 0) {
        tl_debug(UCC_TL_UCP_TEAM_LIB(tl_team),
                 "one-sided reduce_scatter requires count divisible by size");
        return UCC_ERR_NOT_SUPPORTED;
    }
    rcount = count / gsize;
    if (0 == rcount || NULL == args->src.info.buffer ||
        NULL == args->dst.info.buffer) {
        tl_debug(UCC_TL_UCP_TEAM_LIB(tl_team),
                 "one-sided reduce_scatter requires non-empty src and dst");
        return UCC_ERR_NOT_SUPPORTED;
    }
    /* The landing pad is the internal host scratch segment and the local
     * combine is a host-side executor reduce that writes dst in place, so
     * dst must be host memory (src may be any mapped type -- the RMA puts
     * read it via ucp_put_nbx with its memory type). */
    if (UCC_MEMORY_TYPE_HOST != args->dst.info.mem_type) {
        tl_debug(UCC_TL_UCP_TEAM_LIB(tl_team),
                 "one-sided reduce_scatter requires a host dst buffer");
        return UCC_ERR_NOT_SUPPORTED;
    }

    blk = rcount * dt_size;
    scratch_size = (size_t)gsize * blk;

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.flags   |= UCC_COLL_TASK_FLAG_EXECUTOR;
    task->super.post     = ucc_tl_ucp_reduce_scatter_onesided_start;
    task->super.progress = ucc_tl_ucp_reduce_scatter_onesided_progress;
    task->super.finalize = ucc_tl_ucp_reduce_scatter_onesided_finalize;

    /* The landing pad: N blocks of blk bytes at the same symmetric offset on
     * every rank (I1). If the internal scratch segment is disabled or the
     * region is too small / busy, fall back to ring/knomial. */
    status = ucc_tl_ucp_onesided_scratch_alloc(tl_team, scratch_size,
                                               &task->reduce_scatter_onesided.scratch);
    if (ucc_unlikely(UCC_OK != status)) {
        ucc_tl_ucp_coll_finalize(&task->super);
        return status;
    }
    /* Pacing window (plan 3.4): bounds the outstanding put_signals; msglen =
     * the per-peer block, concurrency = processes per node sharing the NIC. */
    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);
    if (sbgp->status != UCC_SBGP_NOT_EXISTS) {
        concurrency = sbgp->group_size;
    }
    status = ucc_tl_ucp_onesided_window_init(task, blk, concurrency,
                                             &task->reduce_scatter_onesided.window);
    if (ucc_unlikely(UCC_OK != status)) {
        ucc_tl_ucp_onesided_scratch_release(tl_team);
        ucc_tl_ucp_coll_finalize(&task->super);
        return status;
    }
    return UCC_OK;
}
