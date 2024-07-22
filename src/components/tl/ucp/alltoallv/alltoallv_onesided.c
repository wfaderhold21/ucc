/**
 * Copyright (c) 2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "alltoallv.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "tl_ucp_sendrecv.h"
#if 0
ucc_status_t ucc_tl_ucp_alltoallv_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task     = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team     = TASK_TEAM(task);
    ptrdiff_t          src      = (ptrdiff_t)TASK_ARGS(task).src.info_v.buffer;
    ptrdiff_t          dest     = (ptrdiff_t)TASK_ARGS(task).dst.info_v.buffer;
    ucc_rank_t         grank    = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize    = UCC_TL_TEAM_SIZE(team);
    long              *pSync    = TASK_ARGS(task).global_work_buffer;
    ucc_aint_t        *s_disp   = TASK_ARGS(task).src.info_v.displacements;
    ucc_aint_t        *d_disp   = TASK_ARGS(task).dst.info_v.displacements;
    size_t             sdt_size = ucc_dt_size(TASK_ARGS(task).src.info_v.datatype);
    size_t             rdt_size = ucc_dt_size(TASK_ARGS(task).dst.info_v.datatype);
    ucc_memory_type_t  mtype    = TASK_ARGS(task).src.info_v.mem_type;
    ucc_rank_t         peer;
    ucc_status_t       status;
    size_t             sd_disp, dd_disp, data_size;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    status = ucc_tl_ucp_coll_dynamic_segment_exchange(task);
    if (UCC_OK != status) {
        task->super.status = status;
        goto out;
    }

    /* perform a put to each member peer using the peer's index in the
     * destination displacement. */
    for (peer = (grank + 1) % gsize; task->onesided.put_posted < gsize;
         peer = (peer + 1) % gsize) {
        sd_disp =
            ucc_coll_args_get_displacement(&TASK_ARGS(task), s_disp, peer) *
            sdt_size;
        dd_disp =
            ucc_coll_args_get_displacement(&TASK_ARGS(task), d_disp, peer) *
            rdt_size;
        data_size =
            ucc_coll_args_get_count(&TASK_ARGS(task),
                                    TASK_ARGS(task).src.info_v.counts, peer) *
            sdt_size;

        UCPCHECK_GOTO(ucc_tl_ucp_put_nb(PTR_OFFSET(src, sd_disp),
                                        PTR_OFFSET(dest, dd_disp),
                                        data_size, peer, mtype, team, task),
                      task, out);
        UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, peer, team), task, out);
    }
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
out:
    return task->super.status;
}

void ucc_tl_ucp_alltoallv_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    long              *pSync = TASK_ARGS(task).global_work_buffer;

    if (ucc_tl_ucp_test_onesided(task, gsize) == UCC_INPROGRESS) {
        return;
    }

    pSync[0]           = 0;
    task->super.status = UCC_OK;
    ucc_tl_ucp_coll_dynamic_segment_finalize(task);
}

ucc_status_t ucc_tl_ucp_alltoallv_onesided_init(ucc_base_coll_args_t *coll_args,
                                                ucc_base_team_t      *team,
                                                ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;

    ALLTOALLV_TASK_CHECK(coll_args->args, tl_team);
    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER)) {
        tl_error(UCC_TL_TEAM_LIB(tl_team),
                 "global work buffer not provided nor associated with team");
        status = UCC_ERR_NOT_SUPPORTED;
        goto out;
    }
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
    task->super.post     = ucc_tl_ucp_alltoallv_onesided_start;
    task->super.progress = ucc_tl_ucp_alltoallv_onesided_progress;

    status = ucc_tl_ucp_coll_dynamic_segment_init(&coll_args->args, task);
    if (UCC_OK != status) {
        tl_error(UCC_TL_TEAM_LIB(tl_team),
                 "failed to initialize dynamic segments");
    }
out:
    return status;
}
#endif
/* congestion control */

static inline int network_chunk(int count, size_t total_data_size) {
    return 1024;
}

static inline int max_network_burst() {
    return 8192;
}

static ucc_rank_t get_num_posts(const ucc_tl_ucp_team_t *team)
{
    ucc_rank_t    tsize = UCC_TL_TEAM_SIZE(team);
    unsigned long posts = UCC_TL_UCP_TEAM_LIB(team)->cfg.alltoallv_pairwise_num_posts;
    posts = (posts > tsize || posts == 0) ? tsize: 1;
    return posts;
}

ucc_status_t ucc_tl_ucp_alltoallv_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task     = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team     = TASK_TEAM(task);
    ptrdiff_t          src      = (ptrdiff_t)TASK_ARGS(task).src.info_v.buffer;
    ptrdiff_t          dest     = (ptrdiff_t)TASK_ARGS(task).dst.info_v.buffer;
    ucc_rank_t         grank    = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize    = UCC_TL_TEAM_SIZE(team);
    long              *pSync    = TASK_ARGS(task).global_work_buffer;
    ucc_aint_t        *s_disp   = TASK_ARGS(task).src.info_v.displacements;
    ucc_aint_t        *d_disp   = TASK_ARGS(task).dst.info_v.displacements;
    size_t             sdt_size = ucc_dt_size(TASK_ARGS(task).src.info_v.datatype);
    size_t             rdt_size = ucc_dt_size(TASK_ARGS(task).dst.info_v.datatype);
    ucc_rank_t         peer;
    size_t             sd_disp, dd_disp, data_size;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    //printf("YES ONESIDED!\n");

    /* perform a put to each member peer using the peer's index in the
     * destination displacement. */
    for (peer = (grank + 1) % gsize; task->onesided.get_posted < get_num_posts(team);
         peer = (peer + 1) % gsize) {
        sd_disp =
            ucc_coll_args_get_displacement(&TASK_ARGS(task), s_disp, peer) *
            sdt_size;
        dd_disp =
            ucc_coll_args_get_displacement(&TASK_ARGS(task), d_disp, grank) *
            rdt_size;
        data_size =
            ucc_coll_args_get_count(&TASK_ARGS(task),
                                    TASK_ARGS(task).src.info_v.counts, peer) *
            sdt_size;

        UCPCHECK_GOTO(ucc_tl_ucp_get_nb(PTR_OFFSET(dest, dd_disp),
                                        PTR_OFFSET(src, sd_disp),
                                        data_size, peer, team, task),
                      task, out);
        UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, peer, team), task, out);
    }
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
out:
    return task->super.status;
}

void ucc_tl_ucp_alltoallv_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ptrdiff_t          src      = (ptrdiff_t)TASK_ARGS(task).src.info_v.buffer;
    ptrdiff_t          dest     = (ptrdiff_t)TASK_ARGS(task).dst.info_v.buffer;
    ucc_rank_t         grank    = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize    = UCC_TL_TEAM_SIZE(team);
    long              *pSync = TASK_ARGS(task).global_work_buffer;
    size_t             num_outstanding = task->onesided.get_posted - task->onesided.get_completed;
    ucc_aint_t        *s_disp   = TASK_ARGS(task).src.info_v.displacements;
    ucc_aint_t        *d_disp   = TASK_ARGS(task).dst.info_v.displacements;
    size_t             sdt_size = ucc_dt_size(TASK_ARGS(task).src.info_v.datatype);
    size_t             rdt_size = ucc_dt_size(TASK_ARGS(task).dst.info_v.datatype);
    ucc_rank_t         peer;
    size_t             sd_disp, dd_disp, data_size;


    if (task->onesided.get_posted < gsize) {
        // complete previous collectives
        if (num_outstanding) {
            for (int i = 0; i < 5; i++) {
                ucp_worker_progress(UCC_TL_UCP_TASK_TEAM(task)->worker->ucp_worker);
            }
        }

        for (peer = (grank + 1 + task->onesided.get_posted) % gsize;
             task->onesided.get_posted < gsize;
             peer = (peer + 1) % gsize) {
            sd_disp =
                ucc_coll_args_get_displacement(&TASK_ARGS(task), s_disp, peer) *
                sdt_size;
            dd_disp =
                ucc_coll_args_get_displacement(&TASK_ARGS(task), d_disp, grank) *
                rdt_size;
            data_size =
                ucc_coll_args_get_count(&TASK_ARGS(task),
                                        TASK_ARGS(task).src.info_v.counts, peer) *
                sdt_size;

            UCPCHECK_GOTO(ucc_tl_ucp_get_nb(PTR_OFFSET(dest, dd_disp),
                                        PTR_OFFSET(src, sd_disp),
                                        data_size, peer, team, task),
                      task, out);
            UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, peer, team), task, out);
        }
    }

    if (ucc_tl_ucp_test_onesided(task, gsize) == UCC_INPROGRESS) {
        return;
    }

    pSync[0]           = 0;
    task->super.status = UCC_OK;
out:
    return;

}

ucc_status_t ucc_tl_ucp_alltoallv_onesided_init(ucc_base_coll_args_t *coll_args,
                                                ucc_base_team_t      *team,
                                                ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;

    ALLTOALLV_TASK_CHECK(coll_args->args, tl_team);
    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER)) {
        tl_error(UCC_TL_TEAM_LIB(tl_team),
                 "global work buffer not provided nor associated with team");
        status = UCC_ERR_NOT_SUPPORTED;
        goto out;
    }
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
    task->super.post     = ucc_tl_ucp_alltoallv_onesided_start;
    task->super.progress = ucc_tl_ucp_alltoallv_onesided_progress;
    status               = UCC_OK;
out:
    return status;
}
