/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided reduce: a knomial tree of put+signal (plan 6.4).
 *
 * Data semantics. src is `count` elements on every rank; only the root's dst
 * (count elements) holds the result:
 *
 *     dst_root[i] = (op over q in [0, size)) src_q[i]
 *
 * Tree (mirrors the two-sided knomial, reduce_knomial.c). With
 * vrank = (rank - root + size) % size and radix = min(cfg.reduce_kn_radix,
 * size), there are nlevels levels, level L at dist = radix^L (dist runs 1,
 * radix, radix^2, ... up to max_dist from CALC_KN_TREE_DIST). At a level:
 *   - a rank with vrank % dist == 0 participates;
 *   - pos = (vrank / dist) % radix;
 *   - pos == 0  -> PARENT: its children are vpeer = vrank + i*dist
 *     (i = 1..radix-1, while < size);
 *   - pos != 0  -> CHILD: its parent is vrank - pos*dist (real rank
 *     INV_VRANK(vroot, root, size)).
 * A rank is a parent at levels 0..k-1 and a child at exactly one level k
 * (k = the radix-power factor of vrank); vrank = 0 (the root) is a parent at
 * every level and never a child. This one-sided version keeps that exact
 * per-level role; the only difference is the transport: a child put_signals
 * its data up to its parent's scratch, and a parent reduces once every child
 * has delivered (I4/I5 via the put->flush->signal chain).
 *
 * rbuf (the running reduction landing pad) is dst for the root and the
 * internal host scratch (offset 0) for every other rank.
 *
 * Scratch layout (uniform, symmetric offset on every rank -- I1):
 *
 *     [ rbuf (data_size) ][ received_0 ][ received_1 ] ... [ received_{L-1} ]
 *
 * Level L's received region is at scratch + data_size + L*(radix-1)*data_size
 * and holds up to (radix-1) blocks of data_size bytes; a child with `pos`
 * lands at received_L + (pos-1)*data_size (children are pos = 1..children
 * and contiguous, so the region is the first `children` slots). Total =
 * data_size * (1 + nlevels*(radix-1)).
 *
 * Two races the layout/choice eliminates:
 *   (1) a single shared sync slot would let a fast deep child signal before a
 *       slow shallow child arrives -> premature reduce. Fix: per-level slots
 *       slot_base + L.
 *   (2) a single shared received region would let a level-1 child of the root
 *       clobber the root's level-0 received data while the root is still
 *       reducing it. Fix: per-level received sub-regions.
 *
 * Slot bases (I7). Slot bases are PER-RANK (team->onesided_slot_base[] is a
 * per-rank array), so no cross-rank lockstep is needed: a rank only commits
 * the slots of the levels at which it is a parent. A parent at level L is
 * signaled by exactly its `children` (each child put_signals +1), so
 * expected = base[slot_base+L] + children, and the parent commits base =
 * expected after its level-L reduce completes. slot_base = 3 +
 * ceil(log2(size)) is immediately after the barrier's reserved block
 * [3 .. 3+ceil(log2(size))), so the two never overlap.
 *
 * AVG. Mirrors the two-sided is_avg exactly: is_avg = (op == AVG) &&
 * (avg_pre_op ? dist == 1 : dist == max_dist), with AVG_ALPHA = 1/size.
 * Every element is alpha-scaled exactly once (at its level-0 parent when
 * avg_pre_op, or at the root's final level otherwise); the remaining levels
 * are plain sums, so the root's final rbuf is alpha * (op over all) = the
 * mean. No self_avg pre-scale is needed for the sizes we support.
 *
 * Degenerate case (NOT_SUPPORTED). If (size-1) % radix == 0, the rank with
 * vrank = size-1 is a level-0 parent with ZERO children (vpeer = size-1 + i
 * are all >= size) yet a participant at level 1; its rbuf would never be
 * written before it must push up. The two-sided knomial handles this with a
 * self_avg pre-scale; we instead return UCC_ERR_NOT_SUPPORTED so the core
 * falls back to the two-sided knomial (I3). None of the gtest sizes
 * {2,3,4,8,16} with radix = min(4,size) hit this.
 *
 * Preconditions (I2/I3, enforced in init): no in-place, predefined datatype,
 * memory-mapped buffers, global work buffer, host src AND host dst (dst is
 * the root's rbuf landing pad and the reduce writes it; src is read by the
 * CPU executor as src1 at dist == 1), count > 0, non-NULL src+dst, size >= 2,
 * slot_base + nlevels <= N_SLOTS, and a scratch region of the needed size.
 * Any unmet precondition returns UCC_ERR_NOT_SUPPORTED so the core falls back
 * to the two-sided knomial/dbt.
 */

#include "config.h"
#include "tl_ucp.h"
#include "reduce.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "utils/ucc_dt_reduce.h"
#include "components/ec/ucc_ec.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

/* Single logical phase: the level cursor (reduce_onesided.level) plus the
 * in-flight reduce (reduce_onesided.etask) fully determine the resume point,
 * so saving the phase is a no-op. */
#define SAVE_STATE(_phase) do {} while (0)

#define UCC_REDUCE_ONESIDED_PHASE_LEVEL 0

void ucc_tl_ucp_reduce_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_coll_args_t   *args = &TASK_ARGS(task);
    ucc_rank_t         rank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         size = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         root = (ucc_rank_t)args->root;
    ucc_rank_t         vrank = (rank - root + size) % size;
    uint32_t           radix = task->reduce_onesided.radix;
    int                avg_pre_op =
        UCC_TL_UCP_TEAM_LIB(team)->cfg.reduce_avg_pre_op;
    void              *rbuf = (rank == root) ? args->dst.info.buffer
                                              : task->reduce_onesided.scratch;
    size_t             count = args->src.info.count;
    size_t             data_size =
        count * ucc_dt_size(args->src.info.datatype);
    ucc_memory_type_t  mtype = args->src.info.mem_type;
    ucc_status_t       status;

    if (UCC_OK != task->super.status &&
        UCC_INPROGRESS != task->super.status) {
        return; /* error already recorded by a completion callback */
    }

    while (task->reduce_onesided.level < task->reduce_onesided.nlevels) {
        ucc_rank_t dist  = task->reduce_onesided.dist;
        int        slot  =
            task->reduce_onesided.slot_base + task->reduce_onesided.level;

        if (vrank % dist == 0) {
            ucc_rank_t pos = (vrank / dist) % radix;
            if (pos == 0) {
                /* PARENT: wait for every child's put to be delivered (I4/I5)
                 * via this level's slot, then reduce the children's blocks
                 * into rbuf. The reduce is posted once and tested on later
                 * entries (non-blocking, EXEC_TASK_TEST). */
                ucc_rank_t children = 0;
                uint32_t   i;
                for (i = 1; i < radix; i++) {
                    if (vrank + i * dist < size) {
                        children++;
                    } else {
                        break; /* children are contiguous; stop at size */
                    }
                }
                long expected =
                    ucc_tl_ucp_onesided_expected(task, slot, children);

                if (task->reduce_onesided.etask != NULL) {
                    /* Reduce posted on an earlier entry; test it. */
                    EXEC_TASK_TEST(UCC_REDUCE_ONESIDED_PHASE_LEVEL,
                                   "failed to perform local reduce",
                                   task->reduce_onesided.etask);
                } else {
                    if (UCC_OK != ucc_tl_ucp_test_onesided_slot(
                                       task, slot, expected)) {
                        return; /* not all children delivered yet */
                    }
                    {
                        int  is_avg = args->op == UCC_OP_AVG &&
                                       (avg_pre_op ? (dist == 1)
                                                   : (dist ==
                                                      task->reduce_onesided
                                                          .max_dist));
                        void *received = PTR_OFFSET(
                            task->reduce_onesided.scratch,
                            data_size +
                            (size_t)task->reduce_onesided.level *
                                (radix - 1) * data_size);
                        /* src1 is the parent's own current value: its raw
                         * src at dist == 1, else its previous level's rbuf.
                         * n_vectors == 0 (no children) is a no-op. */
                        status = ucc_dt_reduce_strided(
                            (dist == 1) ? args->src.info.buffer : rbuf,
                            received, rbuf, children, count, data_size,
                            args->src.info.datatype, args,
                            is_avg ? UCC_EEE_TASK_FLAG_REDUCE_WITH_ALPHA
                                   : 0,
                            AVG_ALPHA(task), task->reduce_onesided.executor,
                            &task->reduce_onesided.etask);
                        if (ucc_unlikely(UCC_OK != status)) {
                            tl_error(UCC_TASK_LIB(task),
                                     "failed to post local reduce");
                            task->super.status = status;
                            return;
                        }
                        if (task->reduce_onesided.etask != NULL) {
                            return; /* in flight: test next round */
                        }
                        /* no children: the reduce was a no-op; fall through */
                    }
                }

                /* Reduce done: commit this level's slot base (I7) and move
                 * up the tree. */
                ucc_tl_ucp_onesided_commit_slot(task, slot, expected);
                task->reduce_onesided.level++;
                task->reduce_onesided.dist *= radix;
            } else {
                /* CHILD: push its current value (raw src at dist == 1, else
                 * its previous level's rbuf) into its parent's level-`slot`
                 * received region at offset (pos-1). This is the rank's only
                 * RMA at this level, and its last RMA (a child is never a
                 * parent again). Advance the cursor before posting so a
                 * re-entry cannot double-put (double-signal). */
                ucc_rank_t vroot  = vrank - pos * dist;
                ucc_rank_t parent = INV_VRANK(vroot, root, size);
                void       *src   =
                    (dist == 1) ? args->src.info.buffer : rbuf;
                void       *dst = PTR_OFFSET(
                    task->reduce_onesided.scratch,
                    data_size +
                    (size_t)task->reduce_onesided.level * (radix - 1) *
                        data_size + (pos - 1) * data_size);

                task->reduce_onesided.level++;
                task->reduce_onesided.dist *= radix;

                status = ucc_tl_ucp_put_signal(
                    src, dst, data_size, mtype, parent,
                    ucc_tl_ucp_onesided_slot(task, slot), NULL, NULL, team,
                    task);
                if (ucc_unlikely(UCC_OK != status)) {
                    task->super.status = status;
                    return;
                }
            }
        } else {
            /* Non-participant at this level: just climb. */
            task->reduce_onesided.level++;
            task->reduce_onesided.dist *= radix;
        }
    }

    /* All levels are done. A non-root rank issued exactly one child put; it
     * must be fully delivered (I5) before the task may complete. The root
     * issues no RMA, so P2P_COMPLETE is trivially true for it. */
    ucc_tl_ucp_onesided_wait_completion(task, task->n_polls);
    if (!UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
        return;
    }
    task->super.status = UCC_OK;
    ucc_tl_ucp_onesided_scratch_release(team);
}

ucc_status_t ucc_tl_ucp_reduce_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_status_t       status;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* Pool-reused task memory (see ucc_tl_ucp_get_task): task_reset() only
     * zeroes the counter union, so every piece of per-round state must be
     * re-established here (a stale level/dist would skip levels, a stale
     * etask would test a foreign executor task). */
    task->reduce_onesided.level  = 0;
    task->reduce_onesided.dist   = 1;
    task->reduce_onesided.etask  = NULL;

    /* The task's schedule (and with it the executor) is attached by the core
     * after init, so it can only be resolved here at post time -- the same
     * place knomial resolves its executor. */
    status = ucc_coll_task_get_executor(&task->super,
                                        &task->reduce_onesided.executor);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(TASK_TEAM(task))->pq,
                                      &task->super);
}

ucc_status_t ucc_tl_ucp_reduce_onesided_finalize(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);

    /* Release the scratch region if the round never reached its commit (e.g.
     * an early error), so a later collective on this team can reuse it. The
     * refcount is idempotent: scratch_release only decrements when in use. */
    ucc_tl_ucp_onesided_scratch_release(TASK_TEAM(task));
    return ucc_tl_ucp_coll_finalize(ctask);
}

ucc_status_t ucc_tl_ucp_reduce_onesided_init(
    ucc_base_coll_args_t *coll_args, ucc_base_team_t *team,
    ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_coll_args_t   *args    = &coll_args->args;
    ucc_tl_ucp_task_t *task;
    ucc_rank_t         gsize   = UCC_TL_TEAM_SIZE(tl_team);
    size_t             count   = (size_t)args->src.info.count;
    size_t             dt_size = ucc_dt_size(args->src.info.datatype);
    uint32_t           radix;
    ucc_rank_t         max_dist;
    uint32_t           nlevels, d;
    uint32_t           slot_base;
    size_t             data_size, scratch_size;
    ucc_status_t       status;

    status = ucc_tl_ucp_onesided_check_args(
        coll_args, tl_team, UCC_TL_UCP_ONESIDED_REQ_GWB);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    if (gsize < 2 || 0 == count || NULL == args->src.info.buffer ||
        NULL == args->dst.info.buffer) {
        tl_debug(UCC_TL_TEAM_LIB(tl_team),
                 "one-sided reduce requires size >= 2 and non-empty src/dst");
        return UCC_ERR_NOT_SUPPORTED;
    }
    /* The root's rbuf landing pad is dst and the CPU executor reduce writes
     * it; at dist == 1 the parent also reads its own src as src1 via the CPU
     * executor. Both must therefore be host memory. */
    if (UCC_MEMORY_TYPE_HOST != args->src.info.mem_type ||
        UCC_MEMORY_TYPE_HOST != args->dst.info.mem_type) {
        tl_debug(UCC_TL_UCP_TEAM_LIB(tl_team),
                 "one-sided reduce requires host src and dst buffers");
        return UCC_ERR_NOT_SUPPORTED;
    }

    radix = ucc_min(UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.reduce_kn_radix, gsize);
    CALC_KN_TREE_DIST(gsize, radix, max_dist);
    nlevels = 0;
    for (d = 1; d <= max_dist; d *= radix) {
        nlevels++;
    }
    slot_base = 3 + ucc_ilog2_ceil(gsize);
    if (ucc_unlikely(slot_base + nlevels > UCC_TL_UCP_ONESIDED_N_SLOTS)) {
        tl_debug(UCC_TL_UCP_TEAM_LIB(tl_team),
                 "team size %u needs slots %u..%u, only %d available", gsize,
                 slot_base, slot_base + nlevels - 1,
                 UCC_TL_UCP_ONESIDED_N_SLOTS);
        return UCC_ERR_NOT_SUPPORTED;
    }
    if (0 == ((gsize - 1) % radix)) {
        /* The rank with vrank = size-1 is a level-0 parent with no children
         * yet a level-1 participant; its rbuf would be read before it is
         * written. Fall back to the two-sided knomial (I3). */
        tl_debug(UCC_TL_UCP_TEAM_LIB(tl_team),
                 "one-sided reduce: degenerate (size-1)%%radix==0 (size=%u, "
                 "radix=%u); falling back", gsize, radix);
        return UCC_ERR_NOT_SUPPORTED;
    }

    data_size    = count * dt_size;
    scratch_size = data_size * (1 + (size_t)nlevels * (radix - 1));

    task               = ucc_tl_ucp_init_task(coll_args, team);
    *task_h            = &task->super;
    task->super.flags |= UCC_COLL_TASK_FLAG_EXECUTOR;
    task->super.post   = ucc_tl_ucp_reduce_onesided_start;
    task->super.progress = ucc_tl_ucp_reduce_onesided_progress;
    task->super.finalize = ucc_tl_ucp_reduce_onesided_finalize;

    task->reduce_onesided.radix     = radix;
    task->reduce_onesided.max_dist  = max_dist;
    task->reduce_onesided.nlevels   = nlevels;
    task->reduce_onesided.slot_base = slot_base;

    /* Landing pad + per-level received regions, at the same symmetric offset
     * on every rank (I1). If the scratch segment is disabled or the region is
     * too small / busy, fall back to the two-sided knomial/dbt. */
    status = ucc_tl_ucp_onesided_scratch_alloc(
        tl_team, scratch_size, &task->reduce_onesided.scratch);
    if (ucc_unlikely(UCC_OK != status)) {
        ucc_tl_ucp_coll_finalize(&task->super);
        return status;
    }
    return UCC_OK;
}
