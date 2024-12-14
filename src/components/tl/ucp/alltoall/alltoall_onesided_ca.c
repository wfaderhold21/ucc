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
#include "tl_ucp_sendrecv.h"

void ucc_tl_ucp_barrier_knomial_progress(ucc_coll_task_t *task);

ucc_status_t ucc_tl_ucp_alltoall_onesided_ca_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task     = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team     = TASK_TEAM(task);
    ucc_tl_ucp_context_t *ctx     = UCC_TL_UCP_TEAM_CTX(team);
    ptrdiff_t          src      = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest     = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    ucc_rank_t         grank    = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize    = UCC_TL_TEAM_SIZE(team);
    ucc_memory_type_t  smem     = TASK_ARGS(task).src.info.mem_type;
    //long              *pSync    = TASK_ARGS(task).global_work_buffer;
    ucc_rank_t         peer;
    size_t             ub_rate = UCC_TL_UCP_TEAM_LIB(team)->cfg.alltoall_onesided_ca_rate;/* assuming 100 gbps */
    int                ppn = UCC_TL_UCP_TEAM_LIB(team)->cfg.alltoall_onesided_ca_ppn;
    int                nreqs = 1;
    size_t             count = 1;
    int                cs_ratio = 1;
    ucc_status_t       status;

    int start = (grank + 1) % gsize;
    count = TASK_ARGS(task).src.info.count / gsize;
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

    task->alltoallv_auto.rate = 1;
    task->alltoallv_auto.token_rate = gsize;
    cs_ratio = count / ppn;
    if (cs_ratio < 1) {
        cs_ratio = 1;
    }
    nreqs = task->alltoallv_auto.token_rate = (task->alltoallv_auto.rate * ub_rate) / (cs_ratio);
    if (nreqs < 1) {
        nreqs = 1;
    }
    count = count * ucc_dt_size(TASK_ARGS(task).src.info.datatype);

   
    /* perform a put to each member peer using the peer's index in the
     * destination displacement. */
    for (peer = start; task->onesided.get_posted < gsize; //get_num_posts(team);
         peer = (peer + 1) % gsize) {

        UCPCHECK_GOTO(ucc_tl_ucp_get_nb(PTR_OFFSET(dest, grank * count),
                                        PTR_OFFSET(src, peer * count),
                                        count, peer, smem, team, task),
                      task, out);
        if ((task->onesided.get_posted - task->onesided.get_completed) >= nreqs) {
            // move this logic around, this is breaking api
            while (task->onesided.get_posted > task->onesided.get_completed) {
                ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->worker.ucp_worker);
            }
        }
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


void ucc_tl_ucp_alltoall_onesided_ca_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    // move this logic around, this is breaking api
    if (task->barrier.phase == UCC_KN_PHASE_INIT) { 
        if (task->onesided.get_posted > task->onesided.get_completed) {
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

ucc_status_t ucc_tl_ucp_alltoall_onesided_ca_init(ucc_base_coll_args_t *coll_args,
                                                  ucc_base_team_t      *team,
                                                  ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;

    ALLTOALL_TASK_CHECK(coll_args->args, tl_team);
    if (coll_args->args.mask & UCC_COLL_ARGS_FIELD_FLAGS) {
        if (!(coll_args->args.flags & UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS)) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                     "non memory mapped buffers are not supported");
            status = UCC_ERR_NOT_SUPPORTED;
            goto out;
        }
    }

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.post     = ucc_tl_ucp_alltoall_onesided_ca_start;
    task->super.progress = ucc_tl_ucp_alltoall_onesided_ca_progress;
    status               = UCC_OK;


out:
    return status;
}
