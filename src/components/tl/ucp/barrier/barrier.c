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
#if 0
ucc_status_t ucc_tl_ucp_barrier_init(ucc_tl_ucp_task_t *task)
{
    task->super.post     = ucc_tl_ucp_barrier_knomial_start;
    task->super.progress = ucc_tl_ucp_barrier_knomial_progress;
    return UCC_OK;
}
#endif
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

    task->barrier.phase = 0;

    task->super.super.status = UCC_INPROGRESS;
    status = ucc_tl_ucp_barrier_progress(&task->super);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    ucc_task_complete(coll_task);
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_barrier_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t     *task       = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t     *team       = task->team;
    long * pSync = task->args.src.info.buffer;
	int * iter = &task->barrier.phase;
	int lc, rc;
	int parent;

	//0->1 0->2, 1->3 1->4, 2->5 2->6, 3->7, 3->8

	lc = (team->rank << 1) + 1;
	rc = (team->rank << 1) + 2;
	parent = (team->rank % 2) ? (team->rank >> 1) : (team->rank >> 1) - 1;

	// wait, send, wait
	if (*iter == 0) {
		if (team->rank != 0) {
            // wait
            if (*pSync < 1) {
                return UCC_INPROGRESS;
            }
        }
        *iter = *iter + 1;
    } 

    if (*iter == 1) {
        *pSync = (long) 1;
        if (lc < team->size) {
    		ucc_tl_ucp_put_nb(pSync, pSync, sizeof(long), lc, team, task);
        }
        if (rc < team->size) {
            ucc_tl_ucp_put_nb(pSync, pSync, sizeof(long), rc, team, task);
        }
        *iter = *iter + 1;
        //ucc_tl_ucp_flush(team);
    } 
    
    if (*iter == 2) {
        if (lc >= team->size && rc >= team->size) {
             // i have no children
            *pSync = *pSync + 1;
            if (team->rank % 2) {
                ucc_tl_ucp_put_nb(pSync, pSync, sizeof(long), parent, team, task);
            } else {
                ucc_tl_ucp_put_nb(pSync, &pSync[1], sizeof(long), parent, team, task);
            }
        } else if (rc >= team->size) {
            // i have one left child
            if (*pSync < 2) {
                return UCC_INPROGRESS;
            }
            if (parent >= 0) {
                if (team->rank % 2) {
                    ucc_tl_ucp_put_nb(pSync, pSync, sizeof(long), parent, team, task);
                } else {
                    ucc_tl_ucp_put_nb(pSync, &pSync[1], sizeof(long), parent, team, task);
                }
            }
        } else {
            // two children
            if (*pSync < 2) {
                return UCC_INPROGRESS;
            }
            if (pSync[1] < 2) {
                return UCC_INPROGRESS;
            }

	        if (parent >= 0) {
                if (team->rank % 2) {
                    ucc_tl_ucp_put_nb(pSync, pSync, sizeof(long), parent, team, task);
                } else {
                    ucc_tl_ucp_put_nb(pSync, &pSync[1], sizeof(long), parent, team, task);
                }
            }         
        }
    }
       
    task->super.super.status = UCC_OK;
    ucc_task_complete(coll_task);
    return UCC_OK;
}

