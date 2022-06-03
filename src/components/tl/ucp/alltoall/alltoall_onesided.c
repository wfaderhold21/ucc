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
#include <omp.h>

void ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask);
void ucc_tl_ucp_alltoall_onesided_barrier_progress(ucc_coll_task_t *ctask);
void ucc_tl_ucp_alltoall_onesided_get_progress(ucc_coll_task_t *ctask);
void ucc_tl_ucp_alltoall_onesided_limit_progress(ucc_coll_task_t *ctask);
void ucc_tl_ucp_alltoall_onesided_sm_progress(ucc_coll_task_t *ctask);

ucc_status_t ucc_tl_ucp_alltoall_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems = TASK_ARGS(task).src.info.count;
//    size_t             elms_pe;
    ucc_rank_t         grank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start;//  = (grank + 1) % gsize;
    //int nr_nodes = gsize / stride;
    ucc_rank_t         peer;
    //long *             pSync  = TASK_ARGS(task).global_work_buffer;
//    task->barrier.phase = 0;
    task->barrier.phase = UCC_KN_PHASE_INIT;
    ucc_knomial_pattern_init(gsize, grank,
                             ucc_min(UCC_TL_UCP_TEAM_LIB(team)->
                                     cfg.barrier_kn_radix, gsize),
                             &task->barrier.p);


    #if 1 

//    int stride         = 8;
//    int node_leader = stride * (grank / stride); 
/*    if (grank & 1) {
        start = ((node_leader + stride) + (grank - node_leader)) % gsize;
    }*/
#if 0
    peer = start;

    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest = dest + grank * nelems;
    for (;task->onesided.put_posted < gsize;) {
//        if ((task->onesided.put_posted - task->onesided.put_completed) < nreqs) {
//            if (peer < node_leader || peer >= (node_leader + stride)) {
                UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)(src + peer * nelems),
                                        (void *)(dest), nelems, start, team, task),
                                        task, out);
/*            } else {
                if (grank == 2) {
                    printf("called on peer %d\n", peer);
                }
                ucc_tl_ucp_put_nb((void *)(src + peer * nelems),
                                        (void *)(dest + grank * nelems), nelems, start, team, task);
*                ucc_tl_ucp_get_nb((void *)(dest + peer * nelems), (void *)(src + grank * nelems), nelems,
                                peer, team, task);
            } */
/*            if (grank == 2) {
                printf("peer: %d\n", peer);
            }*/
            peer = (peer + 1) % gsize;
    }
    printf("[%d] number posts: %d out of %d\n", grank, task->onesided.put_posted, gsize);
    /*
    for (peer = (start + 1) % gsize; peer != start; peer = (peer + 1) % gsize) {
        ucc_tl_ucp_get_nb((void *)(dest + peer * nelems), (void *)src, nelems,
                          peer, team, task);
    }*/
#endif
    int posts = UCC_TL_UCP_TEAM_LIB(team)->cfg.alltoall_pairwise_num_posts;
    int nreqs = (posts > gsize || posts == 0) ? gsize : posts;
    int stride         = 32;
    int node_leader = stride * (grank / stride); 
    if (grank & 1) {
        start = ((node_leader + stride) + (grank - node_leader)) % gsize;
    } else {
        start = (grank + 1) % gsize;
    }

    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);

    peer = start;
    for (peer = start; task->onesided.put_posted < gsize; peer = (peer + 1) % gsize) {
        if ((task->onesided.put_posted - task->onesided.put_completed) < nreqs) {

        if (peer < node_leader || peer >= (node_leader + stride)) {
            ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest + grank * nelems, nelems,
                          peer, team, task);

        } else {
//        if (peer >= node_leader && peer < (node_leader + stride)) {
           ucc_tl_ucp_get_nb((void *)(dest + peer * nelems), (void *)(src + grank * nelems), nelems,
                                peer, team, task);
//        if (peer < node_leader || peer >= (node_leader + stride)) { 
//            ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest + grank * nelems, nelems,
//                          peer, team, task);
//        } else {
        }
/*        peer = (peer + 1) % gsize;
        if (peer == start) {
            break;
        }*/
        } else {
            ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
        }

    }

    //ucc_tl_ucp_flush(team);
      
    task->super.super.status = UCC_INPROGRESS;
    ucc_tl_ucp_alltoall_onesided_barrier_progress(&task->super);
    //status = ucc_tl_ucp_alltoall_onesided_sm_progress(&task->super);
    if (task->super.super.status == UCC_INPROGRESS) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }

    #else
   
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
    task->super.super.status = UCC_INPROGRESS;
    status = ucc_tl_ucp_alltoall_onesided_progress(&task->super);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    #endif
    ucc_task_complete(ctask);
//out:
    return task->super.super.status;
}
ucc_status_t ucc_tl_ucp_alltoall_onesided_sm_start(ucc_coll_task_t *ctask)
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
#if 0
    int stride = 128;
    int node_leader = stride * (grank / stride); 
    int nr_nodes = gsize / stride;
#endif
    long *             pSync  = TASK_ARGS(task).global_work_buffer;

    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    src   = src + grank * nelems;
    ucc_tl_ucp_get_nb((void *)(dest + start * nelems), (void *)src, nelems,
                      start, team, task);
    UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, start, team), task, out);

    for (peer = (start + 1) % gsize; peer != start; peer = (peer + 1) % gsize) {
        ucc_tl_ucp_get_nb((void *)(dest + peer * nelems), (void *)src, nelems,
                          peer, team, task);
        UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, peer, team), task,
                      out);
    }
#if 0
    while (*pSync < stride || task->onesided.put_completed < task->onesided.put_posted) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
    }   

    if (nr_nodes > 1) {     
        nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype) * stride;
   
        if (grank == node_leader) {
            dest   = dest + grank * nelems;
            peer   = (grank + stride) % gsize;
            for (int i = 0; i < nr_nodes; i++) {
                UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)(src + peer * nelems),
                                                (void *)dest, nelems, peer, team, task),
                              task, out);
                UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, peer, team), task,
                              out);
                peer = (peer + stride) % gsize;
            }
            while (*pSync < (stride + nr_nodes) || task->onesided.put_completed < task->onesided.put_posted) {
                ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
            }
            
            for (int i = 0; i < stride; i++) {
                 
        } else {
            while (*pSync < (stride + 1)) {
                sched_yeild();
            }
        } 
    }
#endif
    task->super.super.status = UCC_INPROGRESS;
    ucc_tl_ucp_alltoall_onesided_progress(&task->super);
    if (UCC_INPROGRESS == task->super.super.status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    ucc_task_complete(ctask);
out:
    return task->super.status;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_barrier_start(ucc_coll_task_t *ctask)
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

    task->barrier.phase = UCC_KN_PHASE_INIT;
    ucc_knomial_pattern_init(gsize, grank,
                             ucc_min(UCC_TL_UCP_TEAM_LIB(team)->
                                     cfg.barrier_kn_radix, gsize),
                             &task->barrier.p);

    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest   = dest + grank * nelems;
    ucc_tl_ucp_put_nb((void *)(src + start * nelems), (void *)dest, nelems,
                      start, team, task);

    for (peer = (start + 1) % gsize; peer != start; peer = (peer + 1) % gsize) {
            ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest, nelems,
                          peer, team, task);
    }

    task->super.super.status = UCC_INPROGRESS;
    ucc_tl_ucp_alltoall_onesided_barrier_progress(&task->super);
    if (UCC_INPROGRESS == task->super.super.status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    ucc_task_complete(ctask);

    return UCC_OK;
}

#if 1
ucc_status_t ucc_tl_ucp_alltoall_onesided_get_start(ucc_coll_task_t *ctask)
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

    task->barrier.phase = UCC_KN_PHASE_INIT;
    ucc_knomial_pattern_init(gsize, grank,
                             ucc_min(UCC_TL_UCP_TEAM_LIB(team)->
                                     cfg.barrier_kn_radix, gsize),
                             &task->barrier.p);

    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    src   = src + grank * nelems;
    ucc_tl_ucp_get_nb((void *)(dest + start * nelems), (void *)src, nelems,
                      start, team, task);

    for (peer = (start + 1) % gsize; peer != start; peer = (peer + 1) % gsize) {
        ucc_tl_ucp_get_nb((void *)(dest + peer * nelems), (void *)src, nelems,
                          peer, team, task);
    }

    task->super.super.status = UCC_INPROGRESS;
    ucc_tl_ucp_alltoall_onesided_get_progress(&task->super);
    if (UCC_INPROGRESS == task->super.super.status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    ucc_task_complete(ctask);

    return UCC_OK;
}
#else
ucc_status_t ucc_tl_ucp_alltoall_onesided_get_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start  = (grank + 1) % gsize;
    int         peer;
    ucc_status_t       status;
    int posts = UCC_TL_UCP_TEAM_LIB(team)->cfg.alltoall_pairwise_num_posts;
    int nreqs = (posts > gsize || posts == 0) ? gsize : posts;
#if 1
    task->barrier.phase = UCC_KN_PHASE_INIT;
    ucc_knomial_pattern_init(gsize, grank,
                             ucc_min(UCC_TL_UCP_TEAM_LIB(team)->
                                     cfg.barrier_kn_radix, gsize),
                             &task->barrier.p);
#endif
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
/*    if (nelems <= 512) {
        nreqs = 32;
    } else if (nelems <= 8192) {
        nreqs = 22;
    } else {
        nreqs = 11;
    }
*/
/*    if ((nelems * gsize) < 262144) {
        nreqs = gsize;
    } else if ((nelems * gsize) < (1024*1024)) {
        nreqs = 22;
    } else {
        nreqs = 11;
    }
    if (nelems > 2048 && nelems <= 8192) {
        nreqs = 22;
    } else if (nelems > 8192) {
        nreqs = 11;
    }*/
    dest   = dest + grank * nelems;
    ucc_tl_ucp_put_nb((void *)(src + start * nelems), (void *)dest, nelems,
                      start, team, task);
    for (peer = (start + 1) % gsize; peer != start;) {
        int dist = peer - grank;
        if (dist < 40 && dist >= 0) {
                ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest, nelems,
                              peer, team, task);
                peer = (peer + 1) % gsize;
        } else { 
            if (((task->onesided.put_posted - task->onesided.put_completed)) < nreqs) {
                ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest, nelems,
                              peer, team, task);
                peer = (peer + 1) % gsize;
            } else {
                ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
            }
        } 
           
    }

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
#endif

ucc_status_t ucc_tl_ucp_alltoall_onesided_limit_start(ucc_coll_task_t *ctask)
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
/*    int posts = UCC_TL_UCP_TEAM_LIB(team)->cfg.alltoall_pairwise_num_posts;
    int nreqs = (posts > gsize || posts == 0) ? gsize : posts;*/
///    int                polls = 0;

    /* TODO: change when support for library-based work buffers is complete */
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
/*    if (nelems < 131072) { 
        nreqs = 11;
    } else {
        nreqs = 22;
    }*/
    dest   = dest + grank * nelems;
    for (peer = start; task->onesided.put_posted < gsize;) {
//        int peer = (start + task->onesided.put_posted) % gsize;
/*        if ((task->onesided.put_posted - task->onesided.put_completed) < nreqs) {*/
            (ucc_tl_ucp_put_nb((void *)(src + peer * nelems),
                                    (void *)dest, nelems, peer, team, task));//,
            (ucc_tl_ucp_atomic_inc(pSync, peer, team));
            peer = (peer + 1) % gsize;
/*        } else {
            ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
//            break;
    //        polls++;
        } */
/*        if (polls >= task->n_polls) {
            break;
    //        return UCC_INPROGRESS;
        }*/
    }
/*
    for (int i = 0; i < gsize; i++) {
            (ucc_tl_ucp_atomic_inc(pSync, i, team));
    }
*/ 
    task->super.super.status = UCC_INPROGRESS;
    ucc_tl_ucp_alltoall_onesided_progress(&task->super);
    /*status = ucc_tl_ucp_alltoall_onesided_limit_progress(&task->super);*/
    if (UCC_INPROGRESS == task->super.super.status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    ucc_task_complete(ctask);
    return task->super.super.status;
}

void ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    long *             pSync = TASK_ARGS(task).global_work_buffer;

    if (*pSync < gsize || task->onesided.put_completed < task->onesided.put_posted) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
        return ;
    }

    pSync[0]                 = 0;
    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return;
}
#if 1

static inline int send_to_parent(ucc_rank_t grank, ucc_rank_t parent, long * pSync, ucc_tl_ucp_team_t * team, ucc_tl_ucp_task_t * task)
{
    if (parent >= 0 && parent < UCC_TL_TEAM_SIZE(team)) { 
        if (grank & 1) { 
            return ucc_tl_ucp_put_nb(pSync, pSync, sizeof(long), parent, team, task);
        } else {
            return ucc_tl_ucp_put_nb(pSync, &pSync[1], sizeof(long), parent, team, task);
        }
    }
    return -1;
}

void ucc_tl_ucp_alltoall_onesided_sm_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_rank_t         grank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    long *             pSync = TASK_ARGS(task).global_work_buffer;
    int * iter = &task->barrier.phase;
/*
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems = TASK_ARGS(task).src.info.count;
    int                stride = 32;*/
    int l_child = (grank<<1) + 1;
    int r_child = (grank<<1) + 2; 
    int parent = (grank & 1) ? (grank>>1) : (grank>>1) - 1;

    if (*iter == 0) {
        if (task->onesided.put_completed < task->onesided.put_posted) {
            ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
            return ;
        }

        if (grank != 0) {
            if (*pSync < 1) {
                return ;
            }
        }
        *iter = *iter + 1;
    }

    if (*iter == 1) {
        *pSync = (long) 1;
        if (l_child < gsize) {
            ucc_tl_ucp_put_nb(pSync, pSync, sizeof(long), l_child, team, task);
        }
        if (r_child < gsize) {
            ucc_tl_ucp_put_nb(pSync, pSync, sizeof(long), r_child, team, task);
        }
        *iter = *iter + 1;
    }

    if (*iter == 2) {
        if (l_child >= gsize && r_child >= gsize) {
            *pSync = *pSync + 1;
            send_to_parent(grank, parent, pSync, team, task);
        } else if (r_child >= gsize) {
            if (*pSync < 2) {
                return ;
            }
            send_to_parent(grank, parent, pSync, team, task);
        } else {
            if (pSync[0] < 2) {
                return ;
            }
            if (pSync[1] < 2) {
                return ;
            }
            send_to_parent(grank, parent, pSync, team, task);
        }
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
    }
/*
    if (task->onesided.put_completed < task->onesided.put_posted) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
        return UCC_INPROGRESS;
    }
*/
    pSync[0]                 = 0;
    pSync[1]                 = 0;
    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return;
}
#endif
void ucc_tl_ucp_alltoall_onesided_limit_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start  = (grank + 1) % gsize;
    long *             pSync  = TASK_ARGS(task).global_work_buffer;
    int posts = UCC_TL_UCP_TEAM_LIB(team)->cfg.alltoall_pairwise_num_posts;
    int nreqs = (posts > gsize || posts == 0) ? gsize : posts;
//    int                polls = 0;

    /* TODO: change when support for library-based work buffers is complete */
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
/*    if (nelems < 131072) { 
        nreqs = 11;
    } else {
        nreqs = 22;
    }*/

    dest   = dest + grank * nelems;
    for (;task->onesided.put_posted < gsize;) {
        int peer = (start + task->onesided.put_posted) % gsize;
        if ((task->onesided.put_posted - task->onesided.put_completed) < nreqs) {
            (ucc_tl_ucp_put_nb((void *)(src + peer * nelems),
                                    (void *)dest, nelems, peer, team, task));//,
            (ucc_tl_ucp_atomic_inc(pSync, peer, team));
        } else {
            ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
            return;
//            polls++;
        }
/*        if (polls >= task->n_polls) {
            return UCC_INPROGRESS;
        }*/
    }
//    ucc_tl_ucp_flush(team);
/*
    for (int i = 0; i < gsize; i++) {
            (ucc_tl_ucp_atomic_inc(pSync, i, team));
    }
*/ 
    if (*pSync < gsize || task->onesided.put_completed < task->onesided.put_posted) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
        return;
    }

    pSync[0]           = 0;
    task->super.status = UCC_OK;
}
void ucc_tl_ucp_barrier_knomial_progress(ucc_coll_task_t *task);

void ucc_tl_ucp_alltoall_onesided_barrier_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
#if 1   
    if (task->onesided.put_posted > task->onesided.put_completed) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
        return ;
    }
#endif
 #if 1
    ucc_tl_ucp_barrier_knomial_progress(&task->super);
    if (task->super.super.status == UCC_INPROGRESS) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
        return ;
    }
#endif
   
    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return;
}

void ucc_tl_ucp_alltoall_onesided_get_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
#if 0
    if (task->onesided.get_posted < task->onesided.get_completed) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
        return ;
    }
    printf("im stuck in progress!\n");
#endif
#if 1
    ucc_tl_ucp_barrier_knomial_progress(&task->super);
    if (task->super.super.status == UCC_INPROGRESS) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->ucp_worker);
        return ;
    } 
    #endif
  
    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return ;
}



