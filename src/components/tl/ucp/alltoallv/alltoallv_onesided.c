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

/* mirror alltoallv_pairwise NP_THRESH */
#define NP_THRESH 32

static inline ucc_rank_t get_peer(ucc_rank_t rank, ucc_rank_t size,
                                  ucc_rank_t step)
{
    return (rank + step + 1) % size;
}

static ucc_rank_t get_num_posts(const ucc_tl_ucp_team_t *team)
{
    unsigned long posts =
        UCC_TL_UCP_TEAM_LIB(team)->cfg.alltoallv_onesided_num_posts;
    ucc_rank_t    tsize = UCC_TL_TEAM_SIZE(team);

    if (posts == UCC_ULUNITS_AUTO) {
        posts = (tsize <= NP_THRESH) ? 0 : 1;
    }
    /* 0 or oversized => full posting */
    posts = (posts > tsize || posts == 0) ? tsize : posts;
    return (ucc_rank_t)posts;
}

ucc_status_t ucc_tl_ucp_alltoallv_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_alltoallv_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task     = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team     = TASK_TEAM(task);
    ptrdiff_t          src      = (ptrdiff_t)TASK_ARGS(task).src.info_v.buffer;
    ptrdiff_t          dest     = (ptrdiff_t)TASK_ARGS(task).dst.info_v.buffer;
    ucc_memory_type_t  mtype    = TASK_ARGS(task).src.info_v.mem_type;
    ucc_rank_t         grank    = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize    = UCC_TL_TEAM_SIZE(team);
    long              *pSync    = TASK_ARGS(task).global_work_buffer;
    ucc_aint_t        *s_disp   = TASK_ARGS(task).src.info_v.displacements;
    ucc_aint_t        *d_disp   = TASK_ARGS(task).dst.info_v.displacements;
    size_t             sdt_size = ucc_dt_size(TASK_ARGS(task).src.info_v.datatype);
    size_t             rdt_size = ucc_dt_size(TASK_ARGS(task).dst.info_v.datatype);
    ucc_mem_map_mem_h  src_memh = TASK_ARGS(task).src_memh.local_memh;
    ucc_mem_map_mem_h *dst_memh = TASK_ARGS(task).dst_memh.global_memh;
    ucc_rank_t         nposts   = get_num_posts(team);
    int64_t            npolls   = ucc_max(1, task->n_polls);
    int64_t            polls    = 0;
    ucc_rank_t         peer;
    size_t             sd_disp, dd_disp, data_size;

    /* completion-clocked posting: worker is progressed every outer iteration,
       so the window always reopens (no hang when nposts < gsize). */
    while ((task->onesided.put_posted < gsize) && (polls++ < npolls)) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->worker.ucp_worker);
        while ((task->onesided.put_posted < gsize) &&
               ((task->onesided.put_posted - task->onesided.put_completed) <
                nposts)) {
            peer    = get_peer(grank, gsize, task->onesided.put_posted);
            sd_disp = ucc_coll_args_get_displacement(
                          &TASK_ARGS(task), s_disp, peer) * sdt_size;
            dd_disp = ucc_coll_args_get_displacement(
                          &TASK_ARGS(task), d_disp, peer) * rdt_size;
            data_size = ucc_coll_args_get_count(
                            &TASK_ARGS(task),
                            TASK_ARGS(task).src.info_v.counts, peer) * sdt_size;

            UCPCHECK_GOTO(ucc_tl_ucp_put_nb(PTR_OFFSET(src, sd_disp),
                                            PTR_OFFSET(dest, dd_disp),
                                            data_size, mtype, peer, src_memh,
                                            dst_memh, team, task),
                          task, out);
            UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, peer, dst_memh, team),
                          task, out);
            polls = 0;
        }
    }

    if (task->onesided.put_posted < gsize) {
        return; /* window full or poll budget spent; re-enter later */
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
    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_MEM_MAP_SRC_MEMH)) {
        coll_args->args.src_memh.global_memh = NULL;
    }
    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_MEM_MAP_DST_MEMH)) {
        coll_args->args.dst_memh.global_memh = NULL;
    }

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.post     = ucc_tl_ucp_alltoallv_onesided_start;
    task->super.progress = ucc_tl_ucp_alltoallv_onesided_progress;
    task->n_polls        = ucc_max(1, task->n_polls);
    status               = UCC_OK;
out:
    return status;
}
