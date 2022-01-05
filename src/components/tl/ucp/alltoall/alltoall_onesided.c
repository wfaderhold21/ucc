/**
 * Copyright (c) 2021, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "alltoall.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "tl_ucp_sendrecv.h"

ucc_status_t ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask);
ucc_status_t ucc_tl_ucp_alltoall_onesided_barrier_progress(ucc_coll_task_t *ctask);
ucc_status_t ucc_tl_ucp_alltoall_onesided_get_progress(ucc_coll_task_t *ctask);

ucc_status_t ucc_tl_ucp_alltoall_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start  = (grank + 1) % gsize;
    long *             pSync  = TASK_ARGS(task).global_work_buffer;
    ucc_rank_t         peer;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* TODO: change when support for library-based work buffers is complete */
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest   = dest + grank * nelems;
    UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)(src + start * nelems),
                                    (void *)dest, nelems, start, team, task),
                  task, out);
    UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, start, team), task, out);

    for (peer = (start + 1) % gsize; peer != start; peer = (peer + 1) % gsize) {
        UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)(src + peer * nelems),
                                        (void *)dest, nelems, peer, team, task),
                      task, out);
        UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, peer, team), task,
                      out);
    }

    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
out:
    return task->super.status;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_barrier_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_status_t       status;

    task->barrier.phase = 0;
    task->super.super.status = UCC_INPROGRESS;
    status = ucc_tl_ucp_alltoall_onesided_barrier_progress(&task->super);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    task->super.super.status = status;
    ucc_task_complete(ctask);

    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_get_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_status_t       status;

    task->barrier.phase = 0;
    task->super.super.status = UCC_INPROGRESS;
    status = ucc_tl_ucp_alltoall_onesided_get_progress(&task->super);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    task->super.super.status = status;
    ucc_task_complete(ctask);

    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    long *             pSync = TASK_ARGS(task).global_work_buffer;

    if ((*pSync < gsize) ||
        (task->onesided.put_completed < task->onesided.put_posted)) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
        return;
    }

    pSync[0]           = 0;
    task->super.status = UCC_OK;
}
ucc_status_t ucc_tl_ucp_barrier_knomial_progress(ucc_coll_task_t *task);

ucc_status_t ucc_tl_ucp_alltoall_onesided_barrier_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start  = (grank + 1) % gsize;
    ucc_rank_t         peer;
    ucc_status_t       status;

    if (task->send_posted > 0) {
        if (task->send_completed < task->send_posted) {
            ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
            return UCC_INPROGRESS;
        }

        status = ucc_tl_ucp_barrier_knomial_progress(&task->super);
        if (UCC_INPROGRESS == status) {
            return UCC_INPROGRESS;
        }
        goto completed;
    }

    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest   = dest + grank * nelems;
    ucc_tl_ucp_put_nb((void *)(src + start * nelems), (void *)dest, nelems,
                      start, team, task);

    for (peer = (start + 1) % gsize; peer != start; peer = (peer + 1) % gsize) {
        ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest, nelems,
                          peer, team, task);
    }
    if (task->send_completed < task->send_posted) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
        return UCC_INPROGRESS;
    }
    if (task->barrier.phase == 0) {
        ucc_knomial_pattern_init(gsize, grank,
                                 ucc_min(UCC_TL_UCP_TEAM_LIB(team)->
                                         cfg.barrier_kn_radix, gsize),
                                 &task->barrier.p);
    }

completed:
    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return task->super.super.status;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_get_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start  = (grank + 1) % gsize;
    ucc_rank_t         peer;
    ucc_status_t       status;

    if (task->barrier.phase == UCC_KN_PHASE_PROXY + 1) {
        goto tests;
    }

    if (task->barrier.phase < (UCC_KN_PHASE_PROXY + 1)) {
        if (task->barrier.phase == 0) {
            ucc_knomial_pattern_init(gsize, grank,
                                 ucc_min(UCC_TL_UCP_TEAM_LIB(team)->
                                         cfg.barrier_kn_radix, gsize),
                                 &task->barrier.p);

        }
        status = ucc_tl_ucp_barrier_knomial_progress(&task->super);
        if (UCC_INPROGRESS == status) {
            return UCC_INPROGRESS;
        }
        task->barrier.phase = UCC_KN_PHASE_PROXY + 1;
        task->super.super.status = UCC_INPROGRESS;
    } 
    
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    src = src + grank * nelems;

    ucc_tl_ucp_get_nb((void *)dest + start * nelems, (void *)src, nelems,
                  start, team, task);

    for (peer = (start + 1) % gsize; peer != start; peer = (peer + 1) % gsize) {
        ucc_tl_ucp_get_nb((void *)(dest + peer * nelems), (void *)(src), nelems,
                          peer, team, task);
    }
tests:
    if (task->recv_completed < task->recv_posted) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
        return UCC_INPROGRESS;
    } 
    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return task->super.super.status;
}


