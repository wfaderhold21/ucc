/**
 * Copyright (C) Mellanox Technologies Ltd. 2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */
#include "config.h"
#include "tl_ucp.h"
#include "bcast.h"
#include "tl_ucp_sendrecv.h"

ucc_status_t ucc_tl_ucp_bcast_knomial_start(ucc_coll_task_t *task);
ucc_status_t ucc_tl_ucp_bcast_knomial_progress(ucc_coll_task_t *task);

ucc_base_coll_alg_info_t ucc_tl_ucp_bcast_algs[UCC_TL_UCP_BCAST_ALG_LAST + 1] = {
        [UCC_TL_UCP_BCAST_ALG_KNOMIAL] =
            {.id   = UCC_TL_UCP_BCAST_ALG_KNOMIAL,
             .name = "pairwise",
             .desc =
                 "pairwise two-sided implementation"},
        [UCC_TL_UCP_BCAST_ALG_ONESIDED] =
            {.id   = UCC_TL_UCP_BCAST_ALG_ONESIDED,
             .name = "onesided ring",
             .desc = ""},
        [UCC_TL_UCP_BCAST_ALG_LAST] = {
            .id = 0, .name = NULL, .desc = NULL}
};



ucc_status_t ucc_tl_ucp_bcast_init(ucc_tl_ucp_task_t *task)
{
    task->super.post     = ucc_tl_ucp_bcast_knomial_start;
    task->super.progress = ucc_tl_ucp_bcast_knomial_progress;
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_bcast_knomial_init(ucc_base_coll_args_t *coll_args,
                                   ucc_base_team_t *     team,
                                   ucc_coll_task_t **    task_h)
{
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;
    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    status = ucc_tl_ucp_bcast_init(task);
    return status;
}

ucc_status_t ucc_tl_ucp_bcast_os_start(ucc_coll_task_t *coll_task);
ucc_status_t ucc_tl_ucp_bcast_os_progress(ucc_coll_task_t *coll_task);


ucc_status_t bcast_os_init(ucc_tl_ucp_task_t *task)
{
    task->super.post     = ucc_tl_ucp_bcast_os_start;
    task->super.progress = ucc_tl_ucp_bcast_os_progress;
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_bcast_os_init(ucc_base_coll_args_t *coll_args,
                                   ucc_base_team_t *     team,
                                   ucc_coll_task_t **    task_h)
{
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;
    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    status = bcast_os_init(task);
    return status;
}


ucc_status_t ucc_tl_ucp_bcast_os_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = task->team;
    ucc_status_t       status;

    task->super.super.status = UCC_INPROGRESS;
    status                   = ucc_tl_ucp_bcast_os_progress(&task->super);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_UCP_TEAM_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    return ucc_task_complete(coll_task);
}

ucc_status_t ucc_tl_ucp_bcast_os_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t     *task       = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    void * buffer = task->args.src.info.buffer;
    ucc_rank_t root = (uint32_t) task->args.root;
    ucc_status_t status;

    status = ucc_tl_ucp_bcast_common_progress(buffer,
                                              task->args.src.info.count * ucc_dt_size(task->args.src.info.datatype),
                                              root,
                                              task->team,
                                              task);
    if (status == UCC_INPROGRESS) {
        return status;
    }
     
    task->super.super.status = status;
    ucc_task_complete(coll_task);
    return status;
}

//fan-out fan-in, more stable than other
ucc_status_t ucc_tl_ucp_bcast_common_progress(void * buffer,
                                              size_t elem_size,
                                              int root,
                                              ucc_tl_ucp_team_t *team,
                                              ucc_tl_ucp_task_t *task)
{
    ucc_rank_t         vrank     = (team->rank - root + team->size) % team->size;
    static long one = 1;

	int lc, rc, nr_kids;
    long * pSync = team->pSync;
    
    lc = (vrank << 1) + 1;
	rc = (vrank << 1) + 2;
    nr_kids = (lc < team->size) + (rc < team->size);
	lc = (lc + root) % team->size;
	rc = (rc + root) % team->size;

    if (team->rank != 0) {
        if (pSync[0] < 1) {
            return UCC_INPROGRESS;
        }
    }

    if (nr_kids == 2) {
    	ucc_tl_ucp_put_nb((void *) buffer, (void *) buffer, elem_size, lc, team, task);
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), lc, team, task);

        ucc_tl_ucp_put_nb((void *) buffer, (void *) buffer, elem_size, rc, team, task);
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), rc, team, task);
    } else if (nr_kids == 1) {
        ucc_tl_ucp_put_nb((void *) buffer, (void *) buffer, elem_size, lc, team, task);
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), lc, team, task);
    }
    
    pSync[0] = -1;
    
    return UCC_OK;
}

