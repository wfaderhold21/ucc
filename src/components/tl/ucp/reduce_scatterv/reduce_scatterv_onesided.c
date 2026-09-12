/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided reduce_scatterv: direct, single phase (plan 6.3).
 *
 * reduce_scatterv generalizes reduce_scatter (plan 6.2) to per-rank
 * variable block sizes: the output of rank r is dst (counts[r] elements, at
 * offset 0 of the non-inplace dst), the element-wise reduction, over every
 * rank q, of the r-th block of rank q's src. The layout of a rank's src is
 * described by the counts vector: block p holds counts[p] elements at offset
 * sum_{j<p} counts[j] (the ring's get_block_offset). The counts are uniform
 * across all ranks, so every rank can compute every block offset.
 *
 * RMA (I1/I4). Each rank r is the ONLY issuer of RMA toward its own output:
 * for every peer p != r it put_signals its own block p (src at offset
 * sum_{j<p} counts[j], length counts[p]*dt) into peer p's scratch at offset
 * r*counts[p]*dt. The scratch is the internal one-sided scratch segment
 * (plan 6.1), registered at the same symmetric offset on every rank, so the
 * put target `scratch + r*counts[p]*dt` (a local VA) resolves by intra-
 * segment offset to peer p's scratch at the same offset (I1). The put ->
 * ep_flush -> atomic_add(1) chain publishes each block (I4/I5). Rank r's own
 * block is never put to itself (I8): r local-copies src + sum_{j<r}
 * counts[j] into scratch at offset r*counts[r]*dt (the gap no peer writes)
 * and self-increments the slot.
 *
 * The scratch region is sized (size * max_count * dt) bytes, where
 * max_count = max_r counts[r], so every rank reserves the same symmetric
 * region size and, crucially, every rank's init() succeeds or fails
 * together -- two ranks must not select different algorithms for the same
 * collective, so the region check cannot depend on a rank-local value.
 * Rank r only uses size * counts[r] * dt of it, laid out as counts[r]-wide
 * blocks at stride counts[r]*dt.
 *
 * After the round, rank r's scratch holds every rank's block r: scratch[q]
 * = rank q's r-th block, for all q. The local combine reduces the size
 * contiguous scratch blocks (each counts[r] elements) into dst via
 * ucc_dt_reduce_strided on the task's executor (non-blocking EXEC_TASK_TEST
 * + SAVE_STATE, per ucc-opt.md 2).
 *
 * Slot choice (I7). Full-mesh fanout/fanin: every rank's local slot 0
 * advances by exactly size per round (size-1 remote signals + 1 self-
 * increment), uniform across ranks, so every rank commits base += size --
 * the same lockstep as reduce_scatter, so the two share slot 0.
 *
 * Completion (I7). post() is resumable via the peer cursor and honors the
 * pacing window. progress() waits for P2P_COMPLETE (all local put/flush/
 * atomic counters drained) AND *slot >= base + size (all remote blocks
 * landed), gap-fills its own block, runs the local reduce, commits the slot
 * base, and releases the scratch region.
 *
 * Preconditions (I2/I3, enforced in init): no in-place, predefined
 * datatype, memory-mapped buffers, global work buffer, all counts positive,
 * src count >= the total (so the per-block src reads are in bounds), a host
 * dst, and a scratch region of size*max_count*dt bytes. Any unmet
 * precondition returns UCC_ERR_NOT_SUPPORTED so the core falls back to ring.
 */

#include "config.h"
#include "tl_ucp.h"
#include "reduce_scatterv.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "utils/ucc_dt_reduce.h"
#include "components/ec/ucc_ec.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

#define RSV_ONESIDED_SLOT 0

enum {
    UCC_RSV_ONESIDED_PHASE_POST = 0,
    UCC_RSV_ONESIDED_PHASE_REDUCE
};

#define SAVE_STATE(_phase)                                                     \
    do {                                                                       \
        task->reduce_scatterv_onesided.phase = _phase;                         \
    } while (0)

/* Byte offset of block `block` in a rank's src (sum_{j<block} counts[j]). */
static inline size_t
ucc_tl_ucp_reduce_scatterv_onesided_offset(const ucc_coll_args_t *args,
                                           ucc_rank_t             block)
{
    size_t offset = 0;
    ucc_rank_t i;
    for (i = 0; i < block; i++) {
        offset += ucc_coll_args_get_count(args, args->dst.info_v.counts, i);
    }
    return offset;
}

/*
 * Post put_signals for the next peers, honoring the pacing window. Resumable:
 * the next target is recomputed from the peer cursor. The self case
 * (peer == rank) is a local slot increment -- its block is never put to
 * itself (I8) -- so it does not consume a window token. Returns UCC_OK when
 * all targets are handled, UCC_INPROGRESS when the window requires yielding,
 * or an error.
 */
static ucc_status_t
ucc_tl_ucp_reduce_scatterv_onesided_post(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ucc_coll_args_t   *args   = &TASK_ARGS(task);
    ucc_rank_t         rank   = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    size_t             dt_size = ucc_dt_size(args->dst.info_v.datatype);
    long             *slot   = ucc_tl_ucp_onesided_slot(task, RSV_ONESIDED_SLOT);
    ucc_memory_type_t  mtype = args->src.info.mem_type;
    ucc_status_t       status;

    while (task->reduce_scatterv_onesided.peer < gsize) {
        ucc_rank_t pidx = task->reduce_scatterv_onesided.peer;
        /* Advance the cursor before issuing the RMA: on a window-yield the
         * loop is re-entered from progress(), and the cursor must point at
         * the *next* target, never a peer that was already posted (a re-
         * post would double-signal that peer and desync the slot base). */
        task->reduce_scatterv_onesided.peer++;
        ucc_rank_t peer = (rank + pidx) % gsize;
        if (peer == rank) {
            /* RMA self case (I8): this rank's own block is not put to
             * itself; just advance the local slot. Synchronous, so it
             * cannot leave the cursor half-consumed. (The own block is
             * copied into the scratch gap during the reduce phase.) */
            *slot += 1;
            continue;
        }
        /* Put this rank's block for `peer` (src at offset
         * sum_{j<peer} counts[j], length counts[peer]*dt) into peer's
         * scratch at offset rank*counts[peer]*dt, signaling peer's slot 0
         * on delivery (I4). Peer `peer`'s output is the reduction over
         * every rank's `peer`-th block, so it needs THIS rank's block
         * `peer`. The target is the rank's OWN scratch VA at that offset;
         * I1 resolves it to peer's scratch at the same offset. The source
         * is the rank's local block -- UCX reads it directly, so src_memh
         * is NULL; the dst is the internal scratch segment, so dst_memh is
         * NULL and resolution is by intra-segment offset. */
        size_t pcount =
            ucc_coll_args_get_count(args, args->dst.info_v.counts, peer);
        size_t plen   = pcount * dt_size;
        status = ucc_tl_ucp_put_signal(
            PTR_OFFSET(args->src.info.buffer,
                       ucc_tl_ucp_reduce_scatterv_onesided_offset(args, peer) *
                           dt_size),
            PTR_OFFSET(task->reduce_scatterv_onesided.scratch,
                       (size_t)rank * pcount * dt_size),
            plen, mtype, peer, slot, NULL, NULL, team, task);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
        if (!ucc_tl_ucp_onesided_window_check(
                task, &task->onesided.put_posted,
                &task->onesided.put_completed,
                &task->reduce_scatterv_onesided.window)) {
            return UCC_INPROGRESS; /* window full: yield, resume later */
        }
    }
    return UCC_OK;
}

ucc_status_t
ucc_tl_ucp_reduce_scatterv_onesided_start(ucc_coll_task_t *ctask)
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
    task->reduce_scatterv_onesided.peer = 0;
    task->reduce_scatterv_onesided.phase = UCC_RSV_ONESIDED_PHASE_POST;
    task->reduce_scatterv_onesided.etask = NULL;
    task->reduce_scatterv_onesided.gap_filled = 0;
    /* The task's schedule (and with it the executor) is attached by the core
     * after init, so it can only be resolved here at post time. */
    status = ucc_coll_task_get_executor(&task->super,
                                        &task->reduce_scatterv_onesided.executor);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    /* Completion target (I7): every rank's local slot 0 advances by exactly
     * size this round (size-1 remote signals + 1 self-increment), so the
     * expected value is base + size on every rank. */
    task->reduce_scatterv_onesided.expected =
        ucc_tl_ucp_onesided_expected(task, RSV_ONESIDED_SLOT,
                                     UCC_TL_TEAM_SIZE(TASK_TEAM(task)));

    /* ucc_progress_queue_enqueue() calls progress() once, which begins
     * posting; the task is enqueued if it is not done after that first pass. */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(TASK_TEAM(task))->pq,
                                      &task->super);
}

void ucc_tl_ucp_reduce_scatterv_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_coll_args_t   *args = &TASK_ARGS(task);
    ucc_rank_t         rank = UCC_TL_TEAM_RANK(team);
    size_t             rcount, blk, dt_size;
    ucc_memory_type_t  mtype;
    ucc_status_t       status;

    if (UCC_OK != task->super.status &&
        UCC_INPROGRESS != task->super.status) {
        return; /* error already recorded by a completion callback */
    }
    /* RMA phase: post the put_signals (resumable, window-bounded). */
    if (UCC_RSV_ONESIDED_PHASE_POST == task->reduce_scatterv_onesided.phase) {
        status = ucc_tl_ucp_reduce_scatterv_onesided_post(task);
        if (ucc_unlikely(UCC_OK != status && UCC_INPROGRESS != status)) {
            task->super.status = status;
            return;
        }
        if (UCC_INPROGRESS == status) {
            return; /* window full: wait for the next progress round */
        }
        task->reduce_scatterv_onesided.phase = UCC_RSV_ONESIDED_PHASE_REDUCE;
    }

    /* Local-combine phase: once the put_signals are posted, wait for the
     * chains to be delivered (I5) and the remote atomic signals to land on
     * this rank's slot (I7). On first entry the local reduce is then set up
     * (gap-fill + post) and the phase stays REDUCE, so every later entry
     * only tests the reduce task. */
    ucc_tl_ucp_onesided_wait_completion(
        task, task->reduce_scatterv_onesided.window.npolls);
    if (UCC_OK !=
        ucc_tl_ucp_test_onesided_slot(task, RSV_ONESIDED_SLOT,
                                      task->reduce_scatterv_onesided.expected)) {
        return; /* not all remote blocks are visible yet */
    }

    if (NULL == task->reduce_scatterv_onesided.etask &&
        UCC_RSV_ONESIDED_PHASE_REDUCE == task->reduce_scatterv_onesided.phase &&
        !task->reduce_scatterv_onesided.gap_filled) {
        size_t size = UCC_TL_TEAM_SIZE(team);
        dt_size  = ucc_dt_size(args->dst.info_v.datatype);
        rcount   =
            ucc_coll_args_get_count(args, args->dst.info_v.counts, rank);
        blk = rcount * dt_size;
        mtype = args->src.info.mem_type;
        /* Gap-fill (I8): no peer put this rank's own block into its scratch,
         * so copy it in now, making scratch[0..size-1] fully populated and
         * contiguous for the combine. The own block lives at
         * sum_{j<rank} counts[j] in the src. Marked done so a re-entry does
         * not copy it a second time. */
        status = ucc_mc_memcpy(
            PTR_OFFSET(task->reduce_scatterv_onesided.scratch,
                       (size_t)rank * rcount * dt_size),
            PTR_OFFSET(args->src.info.buffer,
                       ucc_tl_ucp_reduce_scatterv_onesided_offset(args, rank) *
                           dt_size),
            blk, UCC_MEMORY_TYPE_HOST, mtype);
        if (ucc_unlikely(UCC_OK != status)) {
            tl_error(UCC_TASK_LIB(task),
                     "failed to copy own block into scratch");
            task->super.status = status;
            return;
        }
        task->reduce_scatterv_onesided.gap_filled = 1;
        if (size > 1) {
            /* Local combine: reduce the size scratch blocks (each rcount
             * elements, contiguous at stride blk) into dst. src1 = block 0,
             * src2 = block 1, stride = blk, n_vectors = size-1, covering
             * blocks 0..size-1. For UCC_OP_AVG the single-phase full reduce
             * multiplies the sum by 1/size (AVG_ALPHA) to produce the mean.
             * The phase is already REDUCE, so a re-entry only tests the
             * task below. */
            status = ucc_dt_reduce_strided(
                task->reduce_scatterv_onesided.scratch,
                PTR_OFFSET(task->reduce_scatterv_onesided.scratch, blk),
                args->dst.info_v.buffer, size - 1, rcount, blk,
                args->dst.info_v.datatype, args,
                args->op == UCC_OP_AVG
                    ? UCC_EEE_TASK_FLAG_REDUCE_WITH_ALPHA
                    : 0,
                AVG_ALPHA(task), task->reduce_scatterv_onesided.executor,
                &task->reduce_scatterv_onesided.etask);
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
                args->dst.info_v.buffer,
                task->reduce_scatterv_onesided.scratch, blk, mtype,
                UCC_MEMORY_TYPE_HOST);
            if (ucc_unlikely(UCC_OK != status)) {
                tl_error(UCC_TASK_LIB(task), "failed to copy result to dst");
                task->super.status = status;
                return;
            }
        }
    }
    if (task->reduce_scatterv_onesided.etask != NULL) {
        EXEC_TASK_TEST(UCC_RSV_ONESIDED_PHASE_REDUCE,
                       "failed to perform local reduce",
                       task->reduce_scatterv_onesided.etask);
        return; /* reduce in flight: test again next round */
    }

    task->super.status = UCC_OK;
    ucc_tl_ucp_onesided_commit_slot(task, RSV_ONESIDED_SLOT,
                                    task->reduce_scatterv_onesided.expected);
    ucc_tl_ucp_onesided_scratch_release(team);
}

ucc_status_t
ucc_tl_ucp_reduce_scatterv_onesided_finalize(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);

    /* Release the scratch region if the round never reached its commit (e.g.
     * an early error), so a later collective on this team can reuse it. */
    ucc_tl_ucp_onesided_scratch_release(TASK_TEAM(task));
    return ucc_tl_ucp_coll_finalize(ctask);
}

ucc_status_t ucc_tl_ucp_reduce_scatterv_onesided_init(
    ucc_base_coll_args_t *coll_args, ucc_base_team_t *team,
    ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_coll_args_t   *args    = &coll_args->args;
    ucc_tl_ucp_task_t *task;
    ucc_rank_t          gsize     = UCC_TL_TEAM_SIZE(tl_team);
    size_t              dt_size = ucc_dt_size(args->dst.info_v.datatype);
    size_t              total = 0, max_count = 0;
    ucc_sbgp_t         *sbgp;
    ucc_rank_t          concurrency = 1;
    ucc_rank_t          i;
    ucc_status_t        status;

    status = ucc_tl_ucp_onesided_check_args(
        coll_args, tl_team, UCC_TL_UCP_ONESIDED_REQ_GWB);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    if (NULL == args->src.info.buffer || NULL == args->dst.info_v.buffer ||
        NULL == args->dst.info_v.counts) {
        tl_debug(UCC_TL_UCP_TEAM_LIB(tl_team),
                 "one-sided reduce_scatterv requires src, dst and counts");
        return UCC_ERR_NOT_SUPPORTED;
    }
    /* Every count must be positive and the src must hold the full layout
     * (so the per-block src reads are in bounds). */
    for (i = 0; i < gsize; i++) {
        size_t c =
            ucc_coll_args_get_count(args, args->dst.info_v.counts, i);
        if (0 == c) {
            tl_debug(UCC_TL_UCP_TEAM_LIB(tl_team),
                     "one-sided reduce_scatterv requires positive counts");
            return UCC_ERR_NOT_SUPPORTED;
        }
        total += c;
        if (c > max_count) {
            max_count = c;
        }
    }
    if ((size_t)args->src.info.count < total) {
        tl_debug(UCC_TL_UCP_TEAM_LIB(tl_team),
                 "one-sided reduce_scatterv src count %zu < total %zu",
                 (size_t)args->src.info.count, total);
        return UCC_ERR_NOT_SUPPORTED;
    }
    /* The landing pad is the internal host scratch segment and the local
     * combine is a host-side executor reduce that writes dst in place, so
     * dst must be host memory (src may be any mapped type -- the RMA puts
     * read it via ucp_put_nbx with its memory type). */
    if (UCC_MEMORY_TYPE_HOST != args->dst.info_v.mem_type) {
        tl_debug(UCC_TL_UCP_TEAM_LIB(tl_team),
                 "one-sided reduce_scatterv requires a host dst buffer");
        return UCC_ERR_NOT_SUPPORTED;
    }

    /* Region size is size*max_count*dt -- the SAME on every rank -- so all
     * ranks' init() succeed or fail together (a per-rank size could make two
     * ranks select different algorithms). */

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.flags   |= UCC_COLL_TASK_FLAG_EXECUTOR;
    task->super.post     = ucc_tl_ucp_reduce_scatterv_onesided_start;
    task->super.progress = ucc_tl_ucp_reduce_scatterv_onesided_progress;
    task->super.finalize = ucc_tl_ucp_reduce_scatterv_onesided_finalize;

    /* The landing pad: size*max_count*dt bytes at the same symmetric offset
     * on every rank (I1). If the internal scratch segment is disabled or the
     * region is too small / busy, fall back to ring. */
    status = ucc_tl_ucp_onesided_scratch_alloc(
        tl_team, (size_t)gsize * max_count * dt_size,
        &task->reduce_scatterv_onesided.scratch);
    if (ucc_unlikely(UCC_OK != status)) {
        ucc_tl_ucp_coll_finalize(&task->super);
        return status;
    }
    /* Pacing window (plan 3.4): bounds the outstanding put_signals; msglen
     * = the largest per-peer block, concurrency = processes per node. */
    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);
    if (sbgp->status != UCC_SBGP_NOT_EXISTS) {
        concurrency = sbgp->group_size;
    }
    status = ucc_tl_ucp_onesided_window_init(task, max_count * dt_size,
                                             concurrency,
                                             &task->reduce_scatterv_onesided.window);
    if (ucc_unlikely(UCC_OK != status)) {
        ucc_tl_ucp_onesided_scratch_release(tl_team);
        ucc_tl_ucp_coll_finalize(&task->super);
        return status;
    }
    return UCC_OK;
}
