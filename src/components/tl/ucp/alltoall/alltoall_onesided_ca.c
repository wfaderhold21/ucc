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

ucc_status_t ucc_tl_ucp_alltoall_onesided_ca_sched_start(ucc_coll_task_t *ctask)
{
    return ucc_schedule_start(ctask);
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_ca_sched_finalize(ucc_coll_task_t *ctask)
{
    ucc_schedule_t *schedule = ucc_derived_of(ctask, ucc_schedule_t);
    ucc_status_t status;

    status = ucc_schedule_finalize(ctask);
    ucc_tl_ucp_put_schedule(schedule);

    return status;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_ca_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task     = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team     = TASK_TEAM(task);
    ptrdiff_t          src      = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest     = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    ucc_rank_t         grank    = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize    = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         peer;
    int                nreqs = 1;
    int start = (grank + 1) % gsize;
    int iteration = 0;
    size_t count = 0;

    count = TASK_ARGS(task).src.info.count / gsize;
    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);

    nreqs = task->alltoall_onesided_ca.tokens;
    count = count * ucc_dt_size(TASK_ARGS(task).src.info.datatype);

    /* perform a put to each member peer using the peer's index in the
     * destination displacement. */
    for (peer = start; task->onesided.get_posted < gsize;
         peer = (peer + 1) % gsize, ++iteration) {

        UCPCHECK_GOTO(ucc_tl_ucp_get_nb(PTR_OFFSET(dest, grank * count),
                                        PTR_OFFSET(src, peer * count),
                                        count, peer, team, task),
                      task, out);
        if ((task->onesided.get_posted - task->onesided.get_completed) >= nreqs) {
            task->alltoall_onesided_ca.iteration = iteration;
            ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
            break; 
        }
    }
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
out:
    return task->super.status;
}

void ucc_tl_ucp_alltoall_onesided_ca_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ptrdiff_t          src      = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest     = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    ucc_rank_t         grank    = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize    = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start = (grank + 1) % gsize;
    ucc_rank_t         peer = (start + task->alltoall_onesided_ca.iteration) % gsize;
    int iteration = task->alltoall_onesided_ca.iteration;
    size_t count;
    int                nreqs = 1;

    count = TASK_ARGS(task).src.info.count / gsize;
    count = count * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    nreqs = task->alltoall_onesided_ca.tokens;

    for (; task->onesided.get_posted < gsize;
         peer = (peer + 1) % gsize, ++iteration) {

        UCPCHECK_GOTO(ucc_tl_ucp_get_nb(PTR_OFFSET(dest, grank * count),
                                        PTR_OFFSET(src, peer * count),
                                        count, peer, team, task),
                      task, out);
        if ((task->onesided.get_posted - task->onesided.get_completed) >= nreqs) {
            task->alltoall_onesided_ca.iteration = iteration;
            ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
            return;
        }
    }

    task->super.status = UCC_OK;
out:
    return;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_ca_init(ucc_base_coll_args_t *coll_args,
                                                  ucc_base_team_t      *team,
                                                  ucc_coll_task_t     **task_h)
{
    ucc_schedule_t    *schedule = NULL;
    ucc_tl_ucp_team_t *tl_team  = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_base_coll_args_t barrier_coll_args = {
        .team = team->params.team,
        .args.coll_type = UCC_COLL_TYPE_BARRIER,
    };
    ucc_coll_task_t   *barrier_task;
    ucc_tl_ucp_task_t *task;
    ucc_coll_task_t   *a2a_task;
    ucc_status_t       status;
    ALLTOALL_TASK_CHECK(coll_args->args, tl_team);

    status = ucc_tl_ucp_get_schedule(tl_team, coll_args, (ucc_tl_ucp_schedule_t **)&schedule);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    *task_h             = &schedule->super;
    schedule->super.post = ucc_tl_ucp_alltoall_onesided_ca_sched_start;
    schedule->super.progress = NULL;
    schedule->super.finalize = ucc_tl_ucp_alltoall_onesided_ca_sched_finalize;

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    task->super.post     = ucc_tl_ucp_alltoall_onesided_ca_start;
    task->super.progress = ucc_tl_ucp_alltoall_onesided_ca_progress;
    a2a_task = &task->super;

    status = ucc_tl_ucp_coll_init(&barrier_coll_args, team, &barrier_task);
    if (status != UCC_OK) {
        return status;
    }

    int nelems = (TASK_ARGS(task).src.info.count / UCC_TL_TEAM_SIZE(tl_team)) *
                    ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    size_t rate = UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_ca_nic_rate *
                    UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_ca_frac_rate;
    int cs_ratio = nelems / UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_ca_ppn;


    if (cs_ratio < 1) {
        cs_ratio = 1;
    }
    task->alltoall_onesided_ca.tokens = (rate * 1000 * 1000) / (cs_ratio);
    if (task->alltoall_onesided_ca.tokens < 1) {
        task->alltoall_onesided_ca.tokens = 1;
    }
    task->alltoall_onesided_ca.iteration = 0;

    ucc_event_manager_subscribe(&schedule->super, UCC_EVENT_SCHEDULE_STARTED, a2a_task, ucc_task_start_handler);

    ucc_schedule_add_task(schedule, barrier_task);
    ucc_event_manager_subscribe(a2a_task,
                                UCC_EVENT_COMPLETED,
                                barrier_task,
                                ucc_task_start_handler);

#if 0
    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.post     = ucc_tl_ucp_alltoall_onesided_ca_start;
    task->super.progress = ucc_tl_ucp_alltoall_onesided_ca_progress;
    status               = UCC_OK;
#endif

out:
    return status;
}
