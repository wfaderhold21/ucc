/**
 * Copyright (C) Mellanox Technologies Ltd. 2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "alltoall.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "tl_ucp_sendrecv.h"
#include "barrier/barrier.h"
#include "bcast/bcast.h"

// a % b
static inline int p2_mod(int a, int b)
{
    if ((b & (b - 1)) == 0) {
        return a & (b - 1);
    } else {
        return a % b;
    }
}

ucc_status_t ucc_tl_ucp_barrier_a2a_knomial_progress(ucc_coll_task_t *coll_task);
ucc_status_t ucc_tl_ucp_alltoall_bcast_onesided_progress(ucc_coll_task_t *ctask);
ucc_status_t ucc_tl_ucp_alltoall_onesided_nosync_progress(ucc_coll_task_t *ctask);


ucc_status_t ucc_tl_ucp_alltoall_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ucc_status_t status;
    
//    task->barrier.phase = 0;

    task->barrier.phase = UCC_KN_PHASE_INIT;
    ucc_knomial_pattern_init(team->size, team->rank,
                             ucc_min(UCC_TL_UCP_TEAM_LIB(team)->
                                     cfg.barrier_kn_radix, team->size),
                             &task->barrier.p);


    // alltoall checks here?
    task->super.super.status = UCC_INPROGRESS;
    //status = ucc_tl_ucp_alltoall_bcast_onesided_progress(&task->super);
    status = ucc_tl_ucp_alltoall_onesided_progress(&task->super);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    task->super.super.status = status;
    ucc_task_complete(ctask);

    return UCC_OK;
}

#if 1
ucc_status_t ucc_tl_ucp_alltoall_os_bruck_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ucc_status_t status;
    
//    task->barrier.phase = 0;
/*
    task->barrier.phase = UCC_KN_PHASE_INIT;
    ucc_knomial_pattern_init(team->size, team->rank,
                             ucc_min(UCC_TL_UCP_TEAM_LIB(team)->
                                     cfg.barrier_kn_radix, team->size),
                             &task->barrier.p);
*/

    // alltoall checks here?
    task->super.super.status = UCC_INPROGRESS;
    status = ucc_tl_ucp_alltoall_onesided_nosync_progress(&task->super);
    //status = ucc_tl_ucp_alltoall_onesided_get_progress(&task->super);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    task->super.super.status = status;
    ucc_task_complete(ctask);

    return UCC_OK;
}
#else
ucc_status_t ucc_tl_ucp_alltoall_os_bruck_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ucc_status_t status;
    
    //task->barrier.phase = 0;

    task->barrier.phase = UCC_KN_PHASE_INIT;
    ucc_knomial_pattern_init(team->size, team->rank,
                             ucc_min(UCC_TL_UCP_TEAM_LIB(team)->
                                     cfg.barrier_kn_radix, team->size),
                             &task->barrier.p);

    // alltoall checks here?
    task->super.super.status = UCC_INPROGRESS;
    status = ucc_tl_ucp_alltoall_os_bruck_progress(&task->super);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    ucc_task_complete(ctask);

    return UCC_OK;
}
#endif
static inline void bruck_phase1_rotate(void * src, void * dst, size_t count, size_t datasize, 
                                       ucc_tl_ucp_team_t * team)
{
    int rank = team->rank;
    uint64_t * d = (uint64_t *) dst;
    uint64_t * s = (uint64_t *) src;

    // copy from src + rank to end
    for (int i = 0; i < (count - rank); i++) {
        d[i] = s[i + rank];
    }

    memcpy(d + (count - rank), s, rank * datasize);
/*
    for (int i = 0; i < (count - rank); i++) {
        d[i] = s[i + rank];
    }
*/
}

static inline void bruck_phase2_comm(void * src, 
                                     void * dst, 
                                     size_t count, 
                                     size_t datasize,
                                     ucc_tl_ucp_team_t * team,
                                     ucc_tl_ucp_task_t * task)
{
    int npes = team->size;
    int rounds = (count & 1) ? log2(count) + 1 : log2(count);
    int low_index = 1;
    int high_index = count - 1;
    uint64_t * s = (uint64_t *) src;
    uint64_t * d = (uint64_t *) dst;

    for (int i = 0; i < rounds; i++) {
        int low_peer = p2_mod(team->rank + low_index, npes);
        int high_peer = p2_mod(team->rank + high_index, npes);

        ucc_tl_ucp_put_nb(&s[low_index], &d[low_index], sizeof(uint64_t), low_peer, team, task);
        ucc_tl_ucp_put_nb(&s[high_index], &d[high_index], sizeof(uint64_t), high_peer, team, task);

        ++low_index;
        --high_index;
    }
}

static inline void invert(uint64_t * block, size_t datasize, int low, int high)
{
    for (; low < high; low++, high--) {
        uint64_t tmp = block[low];
        block[low] = block[high];
        block[high] = tmp;
    }
}

static inline void bruck_phase3_invert(void * dst, size_t count, size_t datasize, 
                                       ucc_tl_ucp_team_t * team, ucc_tl_ucp_task_t * task)
{
    invert(dst, datasize, 0, team->rank);
    invert(dst, datasize, team->rank + 1, count - 1);
}

ucc_status_t ucc_tl_ucp_alltoall_os_bruck_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t * task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    void * src = task->args.src.info.buffer;
    void * dst = task->args.dst.info.buffer;
    size_t datasize = (task->args.src.info.datatype == UCC_DT_INT64) ? sizeof(uint64_t) : sizeof(uint32_t); // FIXME
    size_t nelems = task->args.src.info.count / datasize * team->size;
    //long * pSync = team->pSync;
    int * phase = team->pSync;//&task->barrier.phase;
    ucc_status_t status = UCC_INPROGRESS; 
    void * tmp = NULL;

    printf("count: %lu\n", nelems);

    if (*phase == 1 || *phase == 2) {
        goto phase2;
    } else if (*phase == 3) {
        goto phase3;
    }
    
    tmp = malloc(task->args.src.info.count);

    if (team->rank != 0) {
        bruck_phase1_rotate(src, tmp, nelems, datasize, team);
        memcpy(dst, tmp, nelems * datasize);
    } else {
        memcpy(dst, src, nelems * datasize);
        memcpy(tmp, src, nelems * datasize);
    }

    bruck_phase2_comm(tmp, dst, nelems, datasize, team, task);
   // ucc_tl_ucp_flush(team);
    if (tmp) {
        free(tmp);
    }

    *phase = 2;
    return UCC_INPROGRESS;
phase2:
#if 1
    status = ucc_tl_ucp_barrier_a2a_knomial_progress(ctask);
    if (status == UCC_INPROGRESS) {
        return status;
    }
#endif
    *phase = 3;
    return UCC_INPROGRESS;
phase3:
    bruck_phase3_invert(dst, nelems, datasize, team, task);
    *phase = -1;

    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return UCC_OK;
}


#define SAVE_STATE(_phase)                                            \
    do {                                                              \
        task->barrier.phase = _phase;                                 \
    } while (0)



ucc_status_t ucc_tl_ucp_barrier_a2a_knomial_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t     *task       = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t     *team       = task->team;
    ucc_kn_radix_t         radix      = task->barrier.p.radix;
    uint8_t                node_type  = task->barrier.p.node_type;
    ucc_knomial_pattern_t *p          = &task->barrier.p;
    ucc_memory_type_t      mtype      = UCC_MEMORY_TYPE_UNKNOWN;
    ucc_rank_t             peer;
    ucc_kn_radix_t         loop_step;

    UCC_KN_GOTO_PHASE(task->barrier.phase);
    if (KN_NODE_EXTRA == node_type) {
        peer = ucc_knomial_pattern_get_proxy(p, team->rank);
        UCPCHECK_GOTO(ucc_tl_ucp_send_nb(NULL, 0, mtype, peer, team, task),
                      task, out);
        UCPCHECK_GOTO(ucc_tl_ucp_recv_nb(NULL, 0, mtype, peer, team, task),
                      task, out);
    }

    if (KN_NODE_PROXY == node_type) {
        peer = ucc_knomial_pattern_get_extra(p, team->rank);
        UCPCHECK_GOTO(ucc_tl_ucp_recv_nb(NULL, 0, mtype, peer, team, task),
                      task, out);
    }
UCC_KN_PHASE_EXTRA:
    if (KN_NODE_PROXY == node_type || KN_NODE_EXTRA == node_type) {
        if (UCC_INPROGRESS == ucc_tl_ucp_test(task)) {
            SAVE_STATE(UCC_KN_PHASE_EXTRA);
            return UCC_INPROGRESS;
        }
        if (KN_NODE_EXTRA == node_type) {
            goto completion;
        }
    }

    while(!ucc_knomial_pattern_loop_done(p)) {
        for (loop_step = 1; loop_step < radix; loop_step++) {
            peer = ucc_knomial_pattern_get_loop_peer(p, team->rank,
                                                     team->size, loop_step);
            if (peer == UCC_KN_PEER_NULL)
                continue;
            UCPCHECK_GOTO(ucc_tl_ucp_send_nb(NULL, 0, mtype, peer, team, task),
                          task, out);
        }

        for (loop_step = 1; loop_step < radix; loop_step++) {
            peer = ucc_knomial_pattern_get_loop_peer(p, team->rank,
                                                     team->size, loop_step);
            if (peer == UCC_KN_PEER_NULL)
                continue;
            UCPCHECK_GOTO(ucc_tl_ucp_recv_nb(NULL, 0, mtype, peer, team, task),
                          task, out);
        }
    UCC_KN_PHASE_LOOP:
        if (UCC_INPROGRESS == ucc_tl_ucp_test(task)) {
            SAVE_STATE(UCC_KN_PHASE_LOOP);
            return UCC_INPROGRESS;
        }
        ucc_knomial_pattern_next_iteration(p);
    }
    if (KN_NODE_PROXY == node_type) {
        peer = ucc_knomial_pattern_get_extra(p, team->rank);
        UCPCHECK_GOTO(ucc_tl_ucp_send_nb(NULL, 0, mtype, peer, team, task),
                      task, out);
        goto UCC_KN_PHASE_PROXY;
    } else {
        goto completion;
    }

UCC_KN_PHASE_PROXY:
    if (UCC_INPROGRESS == ucc_tl_ucp_test(task)) {
        SAVE_STATE(UCC_KN_PHASE_PROXY);
        return UCC_INPROGRESS;
    }

completion:
    return UCC_OK;

out:
    abort();
}


#if 1
/*
 * linear 
 */
ucc_status_t ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ptrdiff_t src = (ptrdiff_t) task->args.src.info.buffer;
    ptrdiff_t dest = (ptrdiff_t) task->args.dst.info.buffer;
    size_t nelems = task->args.src.info.count;
    ucc_rank_t mype = team->rank;
    ucc_rank_t npes = team->size;
    //int * phase = &task->barrier.phase;
    ucc_rank_t peer;
    ucc_status_t status = UCC_INPROGRESS;

    //if (*phase != 0) {
    if (nelems > 0) {
        // put most of the work here
        // alltoall
        dest = dest + mype * nelems;
        ucc_rank_t start = p2_mod(mype + 1, npes);
        ucc_tl_ucp_put_nb((void *)(src + start * nelems), (void *)dest, nelems, start, team, task);
        for (peer = p2_mod(start + 1, npes); peer != start; peer = p2_mod(peer + 1, npes)) {
            ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest, nelems, peer, team, task);
        }
            
/*
        peer = start;
        do {
            ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest, nelems, peer, team, task);
            peer = p2_mod(peer + 1, npes);
        } while (peer != start);
*/
        task->args.src.info.count = 0;
    }

    // barrier
    status = ucc_tl_ucp_barrier_a2a_knomial_progress(ctask);
    if (status == UCC_INPROGRESS) {
        return status;
    }
   
    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_nosync_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ptrdiff_t src = (ptrdiff_t) task->args.src.info.buffer;
    ptrdiff_t dest = (ptrdiff_t) task->args.dst.info.buffer;
    size_t nelems = task->args.src.info.count;
    ucc_rank_t mype = team->rank;
    ucc_rank_t npes = team->size;
    ucc_rank_t peer;

    if (nelems > 0) {
        // put most of the work here
        // alltoall
        dest = dest + mype * nelems;
        ucc_rank_t start = p2_mod(mype + 1, npes);
        ucc_tl_ucp_put_nb((void *)(src + start * nelems), (void *)dest, nelems, start, team, task);
        for (peer = p2_mod(start + 1, npes); peer != start; peer = p2_mod(peer + 1, npes)) {
            ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest, nelems, peer, team, task);
        }
            
        task->args.src.info.count = 0;
        return UCC_INPROGRESS;
    }
    ucc_tl_ucp_flush(team);

    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return UCC_OK;
}
#endif
#if 0
/*
 * linear (get)
 */
ucc_status_t ucc_tl_ucp_alltoall_onesided_get_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ptrdiff_t src = (ptrdiff_t) task->args.src.info.buffer;
    ptrdiff_t dest = (ptrdiff_t) task->args.dst.info.buffer;
    size_t nelems = task->args.src.info.count;
    ucc_rank_t mype = team->rank;
    ucc_rank_t npes = team->size;
    ucc_rank_t peer;
    ucc_status_t status = UCC_INPROGRESS;

    // barrier
    status = ucc_tl_ucp_barrier_a2a_knomial_progress(ctask);
    if (status == UCC_INPROGRESS) {
        return status;
    }
 
    // put most of the work here
    // alltoall
    dest = dest + mype * nelems;
    ucc_rank_t start = p2_mod(mype + 1, npes);
    peer = start;
    do {
        ucc_tl_ucp_get_nb((void *)(dest + peer * nelems), (void *)src, nelems, peer, team, task);
        peer = p2_mod(peer + 1, npes);//(peer + 1) % npes;
    } while (peer != start);

    //ucc_tl_ucp_flush(team);

    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return UCC_OK;
}
#else
ucc_status_t ucc_tl_ucp_alltoall_onesided_get_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ptrdiff_t src = (ptrdiff_t) task->args.src.info.buffer;
    ptrdiff_t dest = (ptrdiff_t) task->args.dst.info.buffer;
    size_t nelems = task->args.src.info.count;

    if (team->rank == 0) {
        ucc_tl_ucp_put_nb((void *)(src), (void *)dest, nelems, 1, team, task);
        ucc_tl_ucp_flush(team);
    }
    
    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return UCC_OK;
}

#endif


#if 1

/*
 * bcast version. n nb broadcasts 
 */
ucc_status_t ucc_tl_ucp_alltoall_bcast_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ptrdiff_t src = (ptrdiff_t) task->args.src.info.buffer;
    ptrdiff_t dest = (ptrdiff_t) task->args.dst.info.buffer;
    size_t nelems = task->args.src.info.count;
    ucc_status_t status = UCC_INPROGRESS;
/*    int index[8] = {1,1,1,1,1,1,1,1};
    int total = team->size; */

    // how to store this?

//    while (total > 0) {
        for (int i = 0; i < team->size; i++) {
            printf("[%d] loop %d\n", team->rank, i);
//            if (index[i]) {
                if (i == team->rank) {
                    while(UCC_INPROGRESS == (status = ucc_tl_ucp_bcast_common_progress((void *)src, nelems, i, team, task)));
                } else {
                    while (UCC_INPROGRESS == (status = ucc_tl_ucp_bcast_common_progress((void *) (dest + i * nelems), nelems, i, team, task)));
                }
/*                if (status == UCC_OK) {
                    index[i] = 0;
                    --total;
                }*/
//            }
        }
//    }

    return UCC_OK;
}
#endif

