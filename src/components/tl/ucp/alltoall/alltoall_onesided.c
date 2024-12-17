/**
 * Copyright (c) 2021-2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "alltoall.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "tl_ucp_coll.h"
#include "tl_ucp_sendrecv.h"

void ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask);
void ucc_tl_ucp_barrier_knomial_progress(ucc_coll_task_t *task);

ucc_status_t ucc_tl_ucp_alltoall_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ucc_tl_ucp_context_t *ctx     = UCC_TL_UCP_TEAM_CTX(team);
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start  = (grank + 1) % gsize;
    ucc_memory_type_t  mtype  = TASK_ARGS(task).src.info.mem_type;
    ucc_rank_t         peer;
    ucc_status_t       status;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    if (ctx->n_dynrinfo_segs > 0) {
        int i = 0;
        for (; i < ctx->n_dynrinfo_segs; i++) {
            if (src == (ptrdiff_t)ctx->dynamic_remote_info[i].va_base && ((TASK_ARGS(task).src.info.count) * ucc_dt_size(TASK_ARGS(task).src.info.datatype)) == ctx->dynamic_remote_info[i].len) {
                break;
            }
        }
        if (i >= ctx->n_dynrinfo_segs) {
            //unmap old
            ucc_tl_ucp_coll_dynamic_segment_finalize(task);
            // map new
            status = ucc_tl_ucp_coll_dynamic_segment_init(&TASK_ARGS(task), task);
            if (UCC_OK != status) {
                tl_error(UCC_TL_TEAM_LIB(team),
                         "failed to initialize dynamic segments");
            }

            status = ucc_tl_ucp_coll_dynamic_segment_exchange(task);
            if (UCC_OK != status) {
                task->super.status = status;
                goto out;
            }
        }
    } else {
        status = ucc_tl_ucp_coll_dynamic_segment_init(&TASK_ARGS(task), task);
        if (UCC_OK != status) {
            tl_error(UCC_TL_TEAM_LIB(team),
                     "failed to initialize dynamic segments");
        }

        // we were out
        status = ucc_tl_ucp_coll_dynamic_segment_exchange(task);
        if (UCC_OK != status) {
            task->super.status = status;
            goto out;
        }
    }

    /* TODO: change when support for library-based work buffers is complete */
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest   = dest + grank * nelems;
    UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)(src + start * nelems),
                                    (void *)dest, nelems, start, mtype, team,
                                    task),
                  task, out);
    for (peer = (start + 1) % gsize; peer != start; peer = (peer + 1) % gsize) {
        UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)(src + peer * nelems),
                                        (void *)dest, nelems, peer, mtype, team,
                                        task),
                      task, out);
    }

    task->barrier.phase = UCC_KN_PHASE_INIT;
    ucc_knomial_pattern_init(gsize, grank,
                             ucc_min(UCC_TL_UCP_TEAM_LIB(team)->
                                     cfg.barrier_kn_radix, gsize),
                             &task->barrier.p);


    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
out:
    return task->super.status;
}

void ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask)
{
#if 0
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    long *             pSync = TASK_ARGS(task).global_work_buffer;

    if (ucc_tl_ucp_test_onesided(task, gsize) == UCC_INPROGRESS) {
        return;
    }

    pSync[0]           = 0;
    task->super.status = UCC_OK;
    ucc_tl_ucp_coll_dynamic_segment_finalize(task);
#else
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    // move this logic around, this is breaking api
    if (task->barrier.phase == UCC_KN_PHASE_INIT) { 
        if (task->onesided.put_posted > task->onesided.put_completed) {
            ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->worker.ucp_worker);
            return;
        }
    }
    ucc_tl_ucp_barrier_knomial_progress(&task->super);
    if (task->super.status == UCC_INPROGRESS) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->worker.ucp_worker);
        return;
    }

    task->super.status = UCC_OK;
    //ucc_tl_ucp_coll_dynamic_segment_finalize(task);
#endif
}
