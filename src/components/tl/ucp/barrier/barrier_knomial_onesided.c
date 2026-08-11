/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "barrier.h"
#include "core/ucc_progress_queue.h"
#include "tl_ucp_sendrecv.h"
#include "coll_patterns/recursive_knomial.h"
#include "utils/ucc_math.h"

#define SAVE_STATE(_phase)                                            \
    do {                                                              \
        task->barrier.phase = _phase;                                 \
    } while (0)

/*
 * One-sided knomial barrier: uses UCP atomic_inc on per-iteration counters
 * in each peer's global_work_buffer to reproduce two-sided lockstep
 * transitivity without send/recv pairs.
 *
 * Counter layout per rank (uint64 each):
 *   pSync[0 .. n_iters-1]  : per-iteration arrival counters
 *   pSync[n_iters]         : extra_arrive  (EXTRA -> PROXY)
 *   pSync[n_iters+1]       : release       (PROXY -> EXTRA)
 *
 * In-degree expected[iter] is precomputed at start time so the algorithm
 * does not assume symmetry near block boundaries.
 */

void ucc_tl_ucp_barrier_knomial_onesided_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t     *task     = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t     *team     = TASK_TEAM(task);
    ucc_rank_t             rank     = UCC_TL_TEAM_RANK(team);
    ucc_knomial_pattern_t *p        = &task->barrier.p;
    long                  *pSync    = TASK_ARGS(task).global_work_buffer;
    ucc_mem_map_mem_h     *dst_memh = TASK_ARGS(task).dst_memh.global_memh;
    ucc_rank_t             peer;
    ucc_kn_radix_t         step;

    UCC_KN_GOTO_PHASE(task->barrier.phase);

    /* EXTRA: signal arrival to PROXY and wait for release */
    if (KN_NODE_EXTRA == p->node_type) {
        peer = ucc_knomial_pattern_get_proxy(p, rank);
        UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(
                          PTR_OFFSET(pSync, p->n_iters * sizeof(uint64_t)),
                          peer, dst_memh, team),
                      task, out);
        goto UCC_KN_PHASE_EXTRA;
    }

    /* PROXY: wait for EXTRA arrival before entering loop */
    if (KN_NODE_PROXY == p->node_type) {
        if (*(long *)PTR_OFFSET(pSync, p->n_iters * sizeof(uint64_t)) != 1) {
            SAVE_STATE(UCC_KN_PHASE_EXTRA);
            return;
        }
    }

UCC_KN_PHASE_EXTRA:

    /* Main loop: iterate over knomial tree levels */
    while (!ucc_knomial_pattern_loop_done(p)) {
        /* Post atomic increments to peers */
        for (step = 1; step < p->radix; step++) {
            peer = ucc_knomial_pattern_get_loop_peer(p, rank, step);
            if (peer == UCC_KN_PEER_NULL) {
                continue;
            }
            UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(
                              PTR_OFFSET(pSync, p->iteration * sizeof(uint64_t)),
                              peer, dst_memh, team),
                          task, out);
        }

        /* Wait for P2P operations to complete */
UCC_KN_PHASE_LOOP:
        if (!UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
            SAVE_STATE(UCC_KN_PHASE_LOOP);
            return;
        }

        /* Detect: wait for expected[p->iteration] arrivals */
        if (*(long *)PTR_OFFSET(pSync, p->iteration * sizeof(uint64_t)) !=
            (long)task->barrier.expected[p->iteration]) {
            SAVE_STATE(UCC_KN_PHASE_LOOP);
            return;
        }

        ucc_knomial_pattern_next_iteration(p);
    }

    /* Post-loop: PROXY signals release to EXTRA */
    if (KN_NODE_PROXY == p->node_type) {
        peer = ucc_knomial_pattern_get_extra(p, rank);
        UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(
                          PTR_OFFSET(pSync, (p->n_iters + 1) * sizeof(uint64_t)),
                          peer, dst_memh, team),
                      task, out);
        goto UCC_KN_PHASE_PROXY;
    } else {
        goto completion;
    }

UCC_KN_PHASE_PROXY:
    if (!UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
        SAVE_STATE(UCC_KN_PHASE_PROXY);
        return;
    }

completion:
    /* Reset per-rank counters for reuse */
    for (step = 0; step < (ucc_kn_radix_t)(p->n_iters + 2); step++) {
        *(long *)PTR_OFFSET(pSync, step * sizeof(uint64_t)) = 0;
    }
    task->super.status = UCC_OK;
    UCC_TL_UCP_PROFILE_REQUEST_EVENT(coll_task, "ucp_barrier_kn_os_done", 0);
out:
    return;
}

ucc_status_t ucc_tl_ucp_barrier_knomial_onesided_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_rank_t         rank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         size = UCC_TL_TEAM_SIZE(team);
    ucc_kn_radix_t     radix = ucc_min(UCC_TL_UCP_TEAM_LIB(team)->cfg.barrier_kn_radix,
                                       size);
    ucc_knomial_pattern_t *p = &task->barrier.p;
    ucc_rank_t peer, iter;
    ucc_kn_radix_t step;

    UCC_TL_UCP_PROFILE_REQUEST_EVENT(coll_task, "ucp_barrier_kn_os_start", 0);
    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);

    ucc_knomial_pattern_init(size, rank, radix, p);

    /* Zero expected array */
    for (iter = 0; iter < p->n_iters; iter++) {
        task->barrier.expected[iter] = 0;
    }

    /*
     * Precompute in-degree expected[iter] for each iteration by scanning all
     * ranks and counting how many peers target me at that iteration.
     */
    for (peer = 0; peer < size; peer++) {
        ucc_knomial_pattern_t peer_p;
        ucc_knomial_pattern_init(size, peer, radix, &peer_p);
        if (peer_p.node_type == KN_NODE_EXTRA) {
            continue;
        }
        for (iter = 0; iter < peer_p.n_iters; iter++) {
            for (step = 1; step < peer_p.radix; step++) {
                ucc_rank_t target =
                    ucc_knomial_pattern_get_loop_peer(&peer_p, peer, step);
                if (target != UCC_KN_PEER_NULL && target == rank) {
                    ucc_assert(iter < p->n_iters);
                    task->barrier.expected[iter]++;
                }
            }
            ucc_knomial_pattern_next_iteration(&peer_p);
        }
    }

    task->barrier.phase = UCC_KN_PHASE_INIT;
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

ucc_status_t ucc_tl_ucp_barrier_knomial_onesided_init(
    ucc_base_coll_args_t *coll_args, ucc_base_team_t *team,
    ucc_coll_task_t **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;

    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER)) {
        tl_error(UCC_TL_TEAM_LIB(tl_team),
                 "global work buffer not provided for onesided barrier");
        status = UCC_ERR_NOT_SUPPORTED;
        goto out;
    }

    if (coll_args->args.mask & UCC_COLL_ARGS_FIELD_FLAGS) {
        if (!(coll_args->args.flags & UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS)) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                     "onesided barrier requires memory mapped buffers");
            status = UCC_ERR_NOT_SUPPORTED;
            goto out;
        }
    }

    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_MEM_MAP_SRC_MEMH)) {
        coll_args->args.src_memh.global_memh = NULL;
    }

    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_MEM_MAP_DST_MEMH)) {
        tl_error(UCC_TL_TEAM_LIB(tl_team),
                 "onesided barrier requires dst memory handles");
        status = UCC_ERR_NOT_SUPPORTED;
        goto out;
    }

    if (!(coll_args->args.flags & UCC_COLL_ARGS_FLAG_DST_MEMH_GLOBAL)) {
        tl_error(UCC_TL_TEAM_LIB(tl_team),
                 "onesided barrier requires global dst memory handles");
        status = UCC_ERR_INVALID_PARAM;
        goto out;
    }

    task = ucc_tl_ucp_init_task(coll_args, team);
    *task_h = &task->super;
    task->super.post = ucc_tl_ucp_barrier_knomial_onesided_start;
    task->super.progress = ucc_tl_ucp_barrier_knomial_onesided_progress;
    status = UCC_OK;
out:
    return status;
}
