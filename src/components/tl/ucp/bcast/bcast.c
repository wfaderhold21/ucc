/**
 * Copyright (C) Mellanox Technologies Ltd. 2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */
#include "config.h"
#include "tl_ucp.h"
#include "bcast.h"
#include "tl_ucp_sendrecv.h"

ucc_base_coll_alg_info_t
    ucc_tl_ucp_bcast_algs[UCC_TL_UCP_BCAST_ALG_LAST + 1] = {
        [UCC_TL_UCP_BCAST_ALG_KNOMIAL] =
            {.id   = UCC_TL_UCP_BCAST_ALG_KNOMIAL,
             .name = "knomial",
             .desc = "bcast over knomial tree with arbitrary radix "
                     "(latency oriented alg)"},
        [UCC_TL_UCP_BCAST_ALG_SAG_KNOMIAL] =
            {.id   = UCC_TL_UCP_BCAST_ALG_SAG_KNOMIAL,
             .name = "sag_knomial",
             .desc = "recursive k-nomial scatter followed by k-nomial "
                     "allgather (bw oriented alg)"},
        [UCC_TL_UCP_BCAST_ALG_ONESIDED] =
            {.id   = UCC_TL_UCP_BCAST_ALG_ONESIDED,
             .name = "onesided ring",
             .desc = ""},
        [UCC_TL_UCP_BCAST_ALG_LAST] = {
            .id = 0, .name = NULL, .desc = NULL}};

ucc_status_t ucc_tl_ucp_bcast_knomial_start(ucc_coll_task_t *task);
ucc_status_t ucc_tl_ucp_bcast_knomial_progress(ucc_coll_task_t *task);

ucc_status_t ucc_tl_ucp_bcast_knomial_os_progress(ucc_coll_task_t *coll_task);
ucc_status_t ucc_tl_ucp_bcast_knomial_os_start(ucc_coll_task_t *coll_task);


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

    task    = ucc_tl_ucp_init_task(coll_args, team);
    status  = ucc_tl_ucp_bcast_init(task);
    *task_h = &task->super;
    return status;
}


ucc_status_t ucc_tl_ucp_bcast_os_start(ucc_coll_task_t *coll_task);
ucc_status_t ucc_tl_ucp_bcast_os_progress(ucc_coll_task_t *coll_task);
ucc_status_t ucc_tl_ucp_bcast_get_progress(void * buffer,
                                              size_t elem_size,
                                              int root,
                                              ucc_tl_ucp_team_t *team,
                                              ucc_tl_ucp_task_t *task);

ucc_status_t ucc_tl_ucp_bcast_linear_progress(void * buffer,
                                              size_t elem_size,
                                              int root,
                                              ucc_tl_ucp_team_t *team,
                                              ucc_tl_ucp_task_t *task);


ucc_status_t bcast_os_init(ucc_tl_ucp_task_t *task)
{
/*
    task->super.post     = ucc_tl_ucp_bcast_os_start;
    task->super.progress = ucc_tl_ucp_bcast_os_progress;
*/

    task->super.post     = ucc_tl_ucp_bcast_knomial_os_start;
    task->super.progress = ucc_tl_ucp_bcast_knomial_os_progress;

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
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_status_t       status;

    task->barrier.phase = 0;

    task->super.super.status = UCC_INPROGRESS;
    status                   = ucc_tl_ucp_bcast_os_progress(&task->super);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    return ucc_task_complete(coll_task);
}

ucc_status_t ucc_tl_ucp_bcast_os_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t     *task       = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    void * buffer = TASK_ARGS(task).src.info.buffer;
    ucc_rank_t root = (uint32_t) TASK_ARGS(task).root;
    ucc_status_t status;

#if 1
/*    status = ucc_tl_ucp_bcast_common_progress(buffer,
                                              TASK_ARGS(task).src.info.count * ucc_dt_size(TASK_ARGS(task).src.info.datatype),
                                              root,
                                              TASK_TEAM(task),
                                              task);*/
    status = ucc_tl_ucp_bcast_linear_progress(buffer,
                                              TASK_ARGS(task).src.info.count * ucc_dt_size(TASK_ARGS(task).src.info.datatype),
                                              root,
                                              TASK_TEAM(task),
                                              task);

#else
    status = ucc_tl_ucp_bcast_get_progress(buffer,
                                              TASK_ARGS(task).src.info.count * ucc_dt_size(TASK_ARGS(task).src.info.datatype),
                                              root,
                                              TASK_TEAM(task),
                                              task);
#endif
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
    int *              phase  = &task->barrier.phase;
	int lc, rc, nr_kids;
    long * pSync = TASK_ARGS(task).global_work_buffer;
    
    lc = (vrank << 1) + 1;
	rc = (vrank << 1) + 2;
    nr_kids = (lc < team->size) + (rc < team->size);
	lc = (lc + root) % team->size;
	rc = (rc + root) % team->size;

    if (*phase == 1) {
        goto send;
    }

    if (team->rank != 0) {
        if (pSync[0] < 0) {
            return UCC_INPROGRESS;
        }
    }
    *phase = 1;
send:
    if (nr_kids == 2) {
    	ucc_tl_ucp_put_nb((void *) buffer, (void *) buffer, elem_size, lc, team, task);
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), lc, team, task);

        ucc_tl_ucp_put_nb((void *) buffer, (void *) buffer, elem_size, rc, team, task);
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), rc, team, task);

        ucc_tl_ucp_ep_flush(lc, team, task);
        ucc_tl_ucp_ep_flush(rc, team, task);
    } else if (nr_kids == 1) {
        ucc_tl_ucp_put_nb((void *) buffer, (void *) buffer, elem_size, lc, team, task);
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), lc, team, task);
        ucc_tl_ucp_ep_flush(lc, team, task);
    }
    
    pSync[0] = -1;
    
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_bcast_linear_progress(void * buffer,
                                              size_t elem_size,
                                              int root,
                                              ucc_tl_ucp_team_t *team,
                                              ucc_tl_ucp_task_t *task)
{
//    ucc_rank_t         vrank     = (team->rank - root + team->size) % team->size;
    static long one = 1;
    long * pSync = TASK_ARGS(task).global_work_buffer;
    
    if (team->rank == root) {
        for (int i = 1; i < team->size; i++) {
            int peer = (team->rank + i) % team->size;
            ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), peer, team, task);
            if (elem_size > (256*1024*1024)) { 
                ucc_tl_ucp_put_nb((void *) buffer, (void *) buffer, elem_size, peer, team, task);
            }   
        }
        ucc_tl_ucp_flush(team);
    } else {
        if (*pSync < 0) {
            return UCC_INPROGRESS;
        }
        if (elem_size <= (256*1024*1024)) {
            ucc_tl_ucp_get_nb((void *) buffer, (void *) buffer, elem_size, root, team, task);
            ucc_tl_ucp_ep_flush(root, team, task);
        }
    }
    *pSync = -1;

    return UCC_OK;
}

//fan-out fan-in, more stable than other
ucc_status_t ucc_tl_ucp_bcast_get_progress(void * buffer,
                                              size_t elem_size,
                                              int root,
                                              ucc_tl_ucp_team_t *team,
                                              ucc_tl_ucp_task_t *task)
{
    ucc_rank_t         vrank     = (team->rank - root + team->size) % team->size;
    static long one = 1;
	int lc, rc, parent, nr_kids;
    long * pSync = TASK_ARGS(task).global_work_buffer;
    
    parent = (vrank & 1) ? ((vrank - 1) >> 1) : ((vrank - 2) >> 1);
    lc = (vrank << 1) + 1;
	rc = (vrank << 1) + 2;
    nr_kids = (lc < team->size) + (rc < team->size);
	lc = (lc + root) % team->size;
	rc = (rc + root) % team->size;

/*
 * 0 -> -1, 1 -> 0, 2 -> 0, 3 -> 1, 4 -> 1, 5 -> 2, 6 -> 2 
 */

    if (team->rank != 0) {
        if (pSync[0] < 0) {
            return UCC_INPROGRESS;
        }
        pSync[0] = -1;
    	ucc_tl_ucp_get_nb((void *) buffer, (void *) buffer, elem_size, parent, team, task);
        ucc_tl_ucp_ep_flush(parent, team, task);
    }
    
    if (nr_kids == 2) {
//    	ucc_tl_ucp_put_nb((void *) buffer, (void *) buffer, elem_size, lc, team, task);
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), lc, team, task);

//        ucc_tl_ucp_put_nb((void *) buffer, (void *) buffer, elem_size, rc, team, task);
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), rc, team, task);

//        ucc_tl_ucp_ep_flush(lc, team, task);
//        ucc_tl_ucp_ep_flush(rc, team, task);
    } else if (nr_kids == 1) {
//        ucc_tl_ucp_put_nb((void *) buffer, (void *) buffer, elem_size, lc, team, task);
        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), lc, team, task);
//        ucc_tl_ucp_ep_flush(lc, team, task);
    }
    
    
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_bcast_knomial_os_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t *task      = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team      = TASK_TEAM(task);
    ucc_rank_t         myrank    = team->rank;
    ucc_rank_t         team_size = team->size;
    ucc_rank_t         root      = (uint32_t)TASK_ARGS(task).root;
    uint32_t           radix     = task->bcast_kn.radix;
    ucc_rank_t         vrank     = (myrank - root + team_size) % team_size;
    ucc_rank_t         dist      = task->bcast_kn.dist;
    int * phase = &task->send_completed;
    void              *buffer    = TASK_ARGS(task).src.info.buffer;
    size_t             data_size = TASK_ARGS(task).src.info.count *
                       ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    ucc_rank_t vpeer, peer, /* vroot_at_level, root_at_level,*/ pos;
    long * pSync = TASK_ARGS(task).global_work_buffer;
    uint64_t one = 1;
    uint32_t i;
    
    if (*phase == 1) {
        goto wait;
    }
    while (dist >= 1) {
        if (vrank % dist == 0) {
            pos = (vrank / dist) % radix;
            if (pos == 0) {
                for (i = radix - 1; i >= 1; i--) {
                    vpeer = vrank + i * dist;
                    if (vpeer < team_size) {
                        peer = (vpeer + root) % team_size;
//                        if (data_size < (16 * 1024)) {
                        ucc_tl_ucp_put_nb((void *) buffer, (void *) buffer, data_size, peer, team, task);
/*                        } else {
                            for (int j = 0; j < data_size; j += (16 * 1024)) {
                                ucc_tl_ucp_put_nb((void *) (buffer + j), (buffer + j), 16 * 1024, peer, team, task);
                            }
                        }*/
                        ucc_tl_ucp_put_nb((void *) &one, (void *) pSync, sizeof(long), peer, team, task);
    //                    ucc_tl_ucp_ep_flush(peer, team, task);
                    }
                }
//                ucc_tl_ucp_flush(team);
//                goto out;
            } else {
                *phase = 1;
                task->bcast_kn.dist = dist;
wait:
                if (*pSync < 0) {
                    return task->super.super.status;
                }
                *phase = 0;
            }
        }
        dist /= radix;
    }
//out:
    *pSync = -1;
//    ucc_assert(UCC_TL_UCP_TASK_P2P_COMPLETE(task));
    task->super.super.status = UCC_OK;
//    UCC_TL_UCP_PROFILE_REQUEST_EVENT(coll_task, "ucp_bcast_kn_done", 0);
    return task->super.super.status;
}

ucc_status_t ucc_tl_ucp_bcast_knomial_os_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_status_t       status;

    ucc_tl_ucp_task_reset(task);

    task->send_completed = 0;

    task->bcast_kn.radix =
        ucc_min(UCC_TL_UCP_TEAM_LIB(team)->cfg.bcast_kn_radix, team->size);
    CALC_KN_TREE_DIST(team->size, task->bcast_kn.radix, task->bcast_kn.dist);

    status = ucc_tl_ucp_bcast_knomial_os_progress(&task->super);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    return ucc_task_complete(coll_task);
}
