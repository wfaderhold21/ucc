/**
 * Copyright (C) Mellanox Technologies Ltd. 2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */
#include "config.h"
#include "tl_ucp.h"
#include "barrier.h"

#include "tl_ucp_sendrecv.h"

ucc_status_t ucc_tl_ucp_barrier_knomial_start(ucc_coll_task_t *task);
ucc_status_t ucc_tl_ucp_barrier_knomial_progress(ucc_coll_task_t *task);

ucc_status_t ucc_tl_ucp_barrier_start(ucc_coll_task_t * task);
ucc_status_t ucc_tl_ucp_barrier_progress(ucc_coll_task_t * task);


ucc_status_t ucc_tl_ucp_barrier_init(ucc_tl_ucp_task_t *task)
{
    task->super.post     = ucc_tl_ucp_barrier_start;
    task->super.progress = ucc_tl_ucp_barrier_progress;
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_barrier_start(ucc_coll_task_t * coll_task)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = task->team;
    ucc_status_t       status;
    long * pSync = task->args.src.info.buffer;

    task->barrier.phase = 0;

    task->super.super.status = UCC_INPROGRESS;
    status = ucc_tl_ucp_barrier_common_progress(&task->super, pSync);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    task->super.super.status = status;
    ucc_task_complete(coll_task);
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_barrier_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t     *task       = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    long * pSync = task->args.src.info.buffer;
    ucc_status_t status;

    status = ucc_tl_ucp_barrier_common_progress(coll_task, pSync);
    if (status == UCC_INPROGRESS) {
        return status;
    }
     
    task->super.super.status = status;
    ucc_task_complete(coll_task);
    return status;
}

ucc_status_t ucc_tl_ucp_barrier_common_progress(ucc_coll_task_t *coll_task,
                                                long * va_psync)
{
    ucc_tl_ucp_task_t     *task       = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t     *team       = task->team;
	int * iter = &task->barrier.phase;
	int lc, rc, parent;
    static long one = 1, two = 2;
    long * pSync = va_psync;
    
	lc = (team->rank << 1) + 1;
	rc = (team->rank << 1) + 2;
	parent = (team->rank & 1) ? (team->rank >> 1) : (team->rank >> 1) - 1;
    
    if (*iter == 1) {
        goto phase2;
    }

    if (team->rank != 0) {
        if (pSync[0] < 1) {
            return UCC_INPROGRESS;
        }
    }

    if (lc < team->size) {
    	ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), lc, team, task);
    }
    if (rc < team->size) {
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), rc, team, task);
    }
    *iter = 1;
    
phase2:
    if (lc < team->size) {
        // i have a left child
        if (pSync[0] < 2) {
            return UCC_INPROGRESS;
        }
    } 

    if (rc < team->size) {
        // i have a right child
        if (pSync[1] < 2) {
            return UCC_INPROGRESS;
        }
    }
    
    if (team->rank & 1) {
        ucc_tl_ucp_put_nb((void *) &two, (void *) pSync, sizeof(long), parent, team, task);
    } else if (team->rank > 0) {
        ucc_tl_ucp_put_nb((void *) &two, (void *) &pSync[1], sizeof(long), parent, team, task);
    }
    pSync[0] = pSync[1] = -1;
    return UCC_OK;
}
