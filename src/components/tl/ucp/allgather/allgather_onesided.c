/**
 * Copyright (c) 2021-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "tl_ucp_coll.h"
#include "tl_ucp_sendrecv.h"
#include "core/ucc_progress_queue.h"
#include "components/mc/ucc_mc.h"
#include "coll_patterns/sra_knomial.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"

void ucc_tl_ucp_allgather_onesided_progress(ucc_coll_task_t *ctask);
void ucc_tl_ucp_barrier_knomial_progress(ucc_coll_task_t *task);

void ucc_tl_ucp_allgather_onesided_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
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
}

ucc_status_t ucc_tl_ucp_allgather_onesided_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
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
    UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)src,
                                    (void *)dest, nelems, start, mtype, team,
                                    task),
                  task, out);
    for (peer = (start + 1) % gsize; peer != start; peer = (peer + 1) % gsize) {
        UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)src,
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

ucc_status_t ucc_tl_ucp_allgather_onesided_init(ucc_base_coll_args_t *coll_args,
                                               ucc_base_team_t      *team,
                                               ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_task_t *task;

    task = ucc_tl_ucp_init_task(coll_args, team);
    task->super.post           = ucc_tl_ucp_allgather_onesided_start;
    task->super.progress       = ucc_tl_ucp_allgather_onesided_progress;
    *task_h                    = &task->super;
    return UCC_OK;
}
