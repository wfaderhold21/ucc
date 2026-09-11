/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_TL_UCP_ONESIDED_H_
#define UCC_TL_UCP_ONESIDED_H_

#include "tl_ucp.h"
#include "tl_ucp_coll.h" /* TASK_* helpers, UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE */

/*
 * Shared one-sided (RMA) helpers, used by all one-sided algorithms.
 *
 * Invariants (see ucc-onesided-plan.md):
 *   I1  Symmetric offsets: a local VA is translated to a peer's by offset
 *       within the registered segment, so all participating buffers must
 *       sit at the same offset on every rank.
 *   I2  One-sided algorithms only run when all buffers are memory-mapped
 *       (MEM_MAPPED_BUFFERS flag set).
 *   I3  Preconditions are enforced in init; unmet ones yield
 *       UCC_ERR_NOT_SUPPORTED so the core can fall back.
 *   I4  Every put must be followed by an EP flush before the sender
 *       signals the receiver (put -> flush -> signal chain).
 *   I5  Local put completion means "source reusable", not "data landed";
 *       flush completion means data is visible on the peer.
 *   I6  Tagged and onesided counters alias the same four union words;
 *       task resets rely on that aliasing (see tl_ucp_task.h).
 *   I7  Sync slots live in the global work buffer and are monotonic: the
 *       team keeps per-slot bases in team->onesided_slot_base[] and never
 *       resets a slot. A round posts n +1 signals, waits for
 *       *slot >= base + n, then commits base = base + n. Waiting with >=
 *       (not ==) keeps racing rounds safe.
 *
 * Slot layout (UCC_TL_UCP_ONESIDED_N_SLOTS slots, slot 0 = first long of
 * the global work buffer):
 *   [0]              put-family sync: alltoallv, allgather-put, scatter,
 *                    gather, fanout, bcast-linear. Every rank's local slot 0
 *                    grows by exactly 1 per round (fanout via the root's
 *                    signal + self-signal), so the per-rank base stays in
 *                    lockstep across interleaved rounds.
 *   [1]              fanin. The root's local slot 1 grows by (size-1) per
 *                    round and non-roots' never changes, so it must not share
 *                    slot 0 with the put-family.
 *   [2]              scratch sync: reduce family, gather-get, gatherv
 *   [3 .. 3+log2(N)) barrier rounds / tree levels
 *   [3+log2(N) .. ]  bcast knomial levels, allgather-rd levels
 */

typedef enum {
    UCC_TL_UCP_ONESIDED_REQ_GWB        = UCC_BIT(0), /* needs global work buffer   */
    UCC_TL_UCP_ONESIDED_REQ_SRC_GLOBAL = UCC_BIT(1), /* needs global src memh      */
    UCC_TL_UCP_ONESIDED_REQ_DST_GLOBAL = UCC_BIT(2), /* needs global dst memh      */
    UCC_TL_UCP_ONESIDED_REQ_NO_DATA    = UCC_BIT(3), /* pure-signal: no data buffers,
                                                     * mem-mapped/predefined-dt
                                                     * preconditions not required */
} ucc_tl_ucp_onesided_req_t;

/*
 * Validate the common one-sided preconditions (I2, I3): no in-place,
 * predefined datatype only, memory-mapped buffers, plus any global-memh
 * or global-work-buffer requirement in `reqs`. Returns
 * UCC_ERR_NOT_SUPPORTED when a precondition is unmet so that the core can
 * fall back to another TL/algorithm; unmet preconditions are logged at
 * tl_debug level because they are a normal fallback path.
 */
ucc_status_t ucc_tl_ucp_onesided_check_args(ucc_base_coll_args_t *coll_args,
                                            ucc_tl_ucp_team_t   *team,
                                            uint64_t              reqs);

/*
 * Atomically add `value` to the peer's copy of the counter at the same
 * symmetric offset as `local_slot` (I1: a local slot pointer is a valid
 * RMA target). `memh` is the destination memory handle array (global-memh
 * mode), or NULL for segment mode. Generalization of
 * ucc_tl_ucp_atomic_inc(); the request is accounted in the task's onesided
 * get counters, so completion is visible through
 * UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE.
 */
ucc_status_t ucc_tl_ucp_atomic_add(long *           local_slot,
                                   long             value,
                                   ucc_rank_t       peer,
                                   ucc_mem_map_mem_h *memh,
                                   ucc_tl_ucp_team_t *team,
                                   ucc_tl_ucp_task_t *task);

/*
 * THE ordering-safe publish (I4): put -> ep_flush -> atomic_add(1) on the
 * peer's slot. `slot` is the local symmetric address of the destination's
 * counter; `dst` is the local symmetric address of the data target. Data
 * is guaranteed visible on the peer before the signal is added. The put is
 * accounted in the task's onesided put counters; the flush and the atomic
 * are accounted in the flush and onesided get counters respectively. Use
 * this everywhere a peer must observe data; never open-code put+signal.
 */
ucc_status_t ucc_tl_ucp_put_signal(void *              src,
                                   void *              dst,
                                   size_t              len,
                                   ucc_memory_type_t   mtype,
                                   ucc_rank_t          peer,
                                   long *              slot,
                                   ucc_mem_map_mem_h   src_memh,
                                   ucc_mem_map_mem_h  *dst_memh,
                                   ucc_tl_ucp_team_t  *team,
                                   ucc_tl_ucp_task_t  *task);

/*
 * Address of slot `slot` in the task's global work buffer. The offset is
 * identical on every rank, so the returned pointer is a valid RMA target
 * under I1.
 */
static inline long *ucc_tl_ucp_onesided_slot(ucc_tl_ucp_task_t *task, int slot)
{
    return &((long *)TASK_ARGS(task).global_work_buffer)[slot];
}

/*
 * The expected slot value at the end of a round that posts `n` +1 signals
 * on `slot` (I7): the current team base plus the round increment.
 */
static inline long
ucc_tl_ucp_onesided_expected(ucc_tl_ucp_task_t *task, int slot, long n)
{
    ucc_tl_ucp_team_t *team = UCC_TL_UCP_TASK_TEAM(task);
    return team->onesided_slot_base[slot] + n;
}

/*
 * Bounded test for the progress loop (I7): the task can complete when all
 * its RMA work is done (I5/I6: put/get/flush counters) and `*slot` has
 * reached `expected`. Returns UCC_OK when done, UCC_INPROGRESS otherwise
 * (or an already-set error status).
 */
static inline ucc_status_t
ucc_tl_ucp_test_onesided_slot(ucc_tl_ucp_task_t *task, int slot, long expected)
{
    ucc_status_t status = UCC_INPROGRESS;
    if (UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task) &&
        *ucc_tl_ucp_onesided_slot(task, slot) >= expected) {
        status = UCC_OK;
    } else if (ucc_unlikely(UCC_OK != task->super.status)) {
        status = task->super.status;
    }
    return status;
}

/*
 * Commit the slot base after the round is observed complete (I7): the
 * next round's expected value is computed from the advanced base.
 */
static inline ucc_status_t
ucc_tl_ucp_onesided_commit_slot(ucc_tl_ucp_task_t *task, int slot, long expected)
{
    ucc_tl_ucp_team_t *team = UCC_TL_UCP_TASK_TEAM(task);
    team->onesided_slot_base[slot] = expected;
    return UCC_OK;
}

/*
 * Pacing / flow-control window: bounds the number of outstanding RMA ops
 * while a linear algorithm posts to every peer. The struct
 * (ucc_tl_ucp_onesided_window_t) is defined in tl_ucp_task.h because the
 * per-algorithm task state embeds it by value.
 */

/*
 * Initialize the pacing window. `concurrency` is the number of processes
 * per node sharing the NIC (from ucc_topo_get_sbgp(team->topo,
 * UCC_SBGP_NODE)->group_size, as alltoall does); `msg_size` is the bytes
 * per put. The token budget is derived from ucp_ep_evaluate_perf and
 * scaled by the ONESIDED_PERCENT_BW config knob.
 */
ucc_status_t ucc_tl_ucp_onesided_window_init(ucc_tl_ucp_task_t *task,
                                             size_t             msg_size,
                                             ucc_rank_t         concurrency,
                                             ucc_tl_ucp_onesided_window_t *win);

/*
 * Returns 1 = keep posting, 0 = yield (task stays UCC_INPROGRESS).
 * Mirrors alltoall_onesided_handle_completion().
 */
static inline int
ucc_tl_ucp_onesided_window_check(ucc_tl_ucp_task_t *task, uint32_t *posted,
                                 uint32_t *completed,
                                 const ucc_tl_ucp_onesided_window_t *win)
{
    int64_t polls = 0;

    if ((*posted - *completed) >= win->tokens) {
        while (polls < win->npolls) {
            ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
            ++polls;
            if ((*posted - *completed) < win->tokens) {
                break;
            }
        }
        if (polls >= win->npolls) {
            return 0; /* yield */
        }
    }
    return 1; /* keep posting */
}

/*
 * Block until every outstanding RMA op of the task is done
 * (mirrors alltoall_onesided_wait_completion()).
 */
static inline void
ucc_tl_ucp_onesided_wait_completion(ucc_tl_ucp_task_t *task, int64_t npolls)
{
    while (!UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
        ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
        if (npolls-- == 0) {
            break;
        }
    }
}

#endif
