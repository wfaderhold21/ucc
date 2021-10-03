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
ucc_status_t ucc_tl_ucp_barrier_knomial_onesided_progress(ucc_coll_task_t *task);

ucc_status_t ucc_tl_ucp_barrier_start(ucc_coll_task_t * task);
ucc_status_t ucc_tl_ucp_barrier_progress(ucc_coll_task_t * task);

ucc_status_t ucc_tl_ucp_barrier_common_test(ucc_coll_task_t *coll_task,
                                                long * va_psync);
ucc_status_t ucc_tl_ucp_barrier_common_ring(ucc_coll_task_t *coll_task,
                                                long * va_psync);


ucc_status_t ucc_tl_ucp_barrier_init(ucc_tl_ucp_task_t *task)
{
    task->super.post     = ucc_tl_ucp_barrier_knomial_start;
    task->super.progress = ucc_tl_ucp_barrier_knomial_onesided_progress;
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_barrier_start(ucc_coll_task_t * coll_task)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = task->team;
    ucc_status_t       status;
    long * pSync = task->args.src.info.buffer;//team->pSync; //task->args.src.info.buffer;

    task->barrier.phase = 0;

    task->super.super.status = UCC_INPROGRESS;
    status = ucc_tl_ucp_barrier_common_ring(&task->super, pSync);
    //status = ucc_tl_ucp_barrier_common_test(&task->super, pSync);
    //status = ucc_tl_ucp_barrier_common_progress(&task->super, pSync, 10);
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
    long * pSync = task->args.src.info.buffer;//task->team->pSync;
    ucc_status_t status;

    //status = ucc_tl_ucp_barrier_common_progress(coll_task, pSync, 10);
    //status = ucc_tl_ucp_barrier_common_test(&task->super, pSync);
    status = ucc_tl_ucp_barrier_common_ring(&task->super, pSync);
    if (status == UCC_INPROGRESS) {
        return status;
    }
     
    task->super.super.status = status;
    ucc_task_complete(coll_task);
    return status;
}

//fan-out fan-in, more stable than other
ucc_status_t ucc_tl_ucp_barrier_common_progress(ucc_coll_task_t *coll_task,
                                                long * va_psync,
                                                int probe_count)
{
    ucc_tl_ucp_task_t     *task       = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t     *team       = task->team;
	int * iter = &task->barrier.phase;
	int lc, rc, parent, nr_kids;
    static long one = 1, two = 2;
    long * pSync = va_psync;
    
	lc = (team->rank << 1) + 1;
	rc = (team->rank << 1) + 2;
    nr_kids = (lc < team->size) + (rc < team->size);
	parent = (team->rank & 1) ? (team->rank >> 1) : (team->rank >> 1) - 1;

    if (*iter == 1) {
        goto phase2;
    } 

    if (team->rank != 0) {
        do {
            --probe_count;
        } while (pSync[0] < 1 && probe_count);
        if (probe_count == 0) {
            return UCC_INPROGRESS;
        }
    }

    if (nr_kids == 2) {
    	ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), lc, team, task);
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), rc, team, task);
    } else if (nr_kids == 1) {
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), lc, team, task);
    }
    *iter = 1;
    
phase2:
    if (nr_kids == 2) {
        // i have a left child
        if (pSync[0] < 2 || pSync[1] < 2) {
            return UCC_INPROGRESS;
        }
    } else if (nr_kids == 1) {
        if (pSync[0] < 2) {
            return UCC_INPROGRESS;
        }
    } 

    pSync[0] = pSync[1] = -1;
    
    if (team->rank & 1) {
        ucc_tl_ucp_put_nb((void *) &two, (void *) pSync, sizeof(long), parent, team, task);
    } else if (team->rank > 0) {
        ucc_tl_ucp_put_nb((void *) &two, (void *) (pSync + 1), sizeof(long), parent, team, task);
    }
    return UCC_OK;
}

//fan-in fan-out, race condition
ucc_status_t ucc_tl_ucp_barrier_common_test(ucc_coll_task_t *coll_task,
                                                long * va_psync)
{
    ucc_tl_ucp_task_t     *task       = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t     *team       = task->team;
	int * iter = &task->barrier.phase;
	int lc, rc, parent;
    static long two = -1;
    static int one = 0;
//    int nr_kids;
    long * pSync = va_psync;
    int * ipSync = (int *) va_psync;
    
	lc = (team->rank << 1) + 1;
	rc = (team->rank << 1) + 2;
//    nr_kids = (lc < team->size) + (rc < team->size);
	parent = (team->rank & 1) ? (team->rank >> 1) : (team->rank >> 1) - 1; 

    if (*iter == 1) {
        goto phase2;
    }

    if (lc < team->size) {
        if (ipSync[0] != 0) {
            return UCC_INPROGRESS;
        }
    }
    if (rc < team->size) {
        if (ipSync[1] != 0) {
            return UCC_INPROGRESS;
        }
    }

    if (team->rank == 0) {
        pSync[0] = -1;
        if (lc < team->size) {
    //        printf("[0] putting to lc\n");
            ucc_tl_ucp_put_nb((void *) &two, (void *) pSync, sizeof(long), lc, team, task);
        }
        if (rc < team->size) {
            ucc_tl_ucp_put_nb((void *) &two, (void *) pSync, sizeof(long), rc, team, task);
        }
        goto out;
    } else {
        pSync[0] = 0;
        if (team->rank % 2) {
//            printf("[%d] odd putting\n", team->rank);
            ucc_tl_ucp_put_nb((void *) &one, (void *) &ipSync[0], sizeof(int), parent, team, task);
        } else {
  //          printf("[%d] even putting\n", team->rank);
            ucc_tl_ucp_put_nb((void *) &one, (void *) &ipSync[1], sizeof(int), parent, team, task);
        }
    }
    *iter = 1;
phase2:
    if (pSync[0] != -1) {
        return UCC_INPROGRESS;
    }

    //pSync[0] = -1;
    if (lc < team->size) {
        ucc_tl_ucp_put_nb((void *) &two, (void *) pSync, sizeof(long), lc, team, task);
    }
    if (rc < team->size) {
        ucc_tl_ucp_put_nb((void *) &two, (void *) pSync, sizeof(long), rc, team, task);
    }
out:
    //printf("[%d] completed barrier\n", team->rank);
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_barrier_common_ring(ucc_coll_task_t *coll_task,
                                                long * va_psync)
{
    ucc_tl_ucp_task_t     *task       = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t     *team       = task->team;
	int * iter = &task->barrier.phase;
    int partner_right = (team->rank == team->size - 1) ? 0 : team->rank + 1;
    static long one = 1, two = 2;
    long * pSync = va_psync;
    
    if (*iter == 1) {
        goto phase2;
    } else if (*iter == 2) {
        goto phase3;
    }


    ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), partner_right, team, task);
        
    *iter = 1;
phase2:
    if (pSync[0] < one) {
        return UCC_INPROGRESS;
    } 
    if (team->rank == 0) {
        ucc_tl_ucp_put_nb((void *) &two, (void *) pSync, sizeof(long), partner_right, team, task);
    }
    *iter = 2;
    
phase3:
    if (pSync[0] < two) {
        return UCC_INPROGRESS;
    } else if (team->rank != 0) {
        ucc_tl_ucp_put_nb((void *) &two, (void *) pSync, sizeof(long), partner_right, team, task);
    }
    pSync[0] = -1;
   
    return UCC_OK;
}
#if 0       
ucc_status_t ucc_tl_ucp_barrier_common_rdoubling(ucc_coll_task_t *coll_task,
                                                long * va_psync)
{
    ucc_tl_ucp_task_t     *task       = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t     *team       = task->team;
	int * iter = &task->barrier.phase;
    int partner_right = (team->rank == team->size - 1) ? 0 : team->rank + 1;
    static long one = 1, two = 2;
    long * pSync = va_psync;
    int log2_npes = log2(team->size);

    for (int i = 0; i < log2_npes; i++) {
        int peer = (team->rank ^ (1 << i));
        
        if (peer < team->rank) {
            // wait on them
            while (pSync[0]
        } else {
            // put on them
        }
    }
    
  
    return UCC_OK;
}
#endif
