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

ucc_status_t ucc_tl_ucp_alltoall_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ucc_status_t status;
    
    task->barrier.phase = 0;

    // alltoall checks here?
    task->super.super.status = UCC_INPROGRESS;
    status = ucc_tl_ucp_alltoall_onesided_progress(&task->super);
    if (UCC_INPROGRESS == status) {
        ucc_progress_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
        return UCC_OK;
    }
    ucc_task_complete(ctask);

    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_alltoall_os_bruck_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ucc_status_t status;
    
    task->barrier.phase = 0;

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

static inline void bruck_phase1_rotate(void * src, void * dst, size_t count, size_t datasize, 
                                       ucc_tl_ucp_team_t * team)
{
    int rank = team->rank;
    uint64_t * d = (uint64_t *) dst;
    uint64_t * s = (uint64_t *) src;

    memcpy(dst, src, rank * count);

    for (int i = 0; i < rank; i++) {
        s[i] = s[i + rank];
    }
    for (int i = rank; i < rank + 1; i++) {
        s[i] = d[0 + i - rank];
    }
    //memcpy(src, src + rank * datasize, (count - rank) * datasize);
/*
    for (int i = 0; i < count - rank; i++) {
        src[i] = src[i + rank];
    }
*/

    //memcpy(src + rank * datasize, dst, rank * datasize);
//    memcpy(dst, src, count * datasize);
}

static inline void bruck_phase2_comm(void * src, 
                                     void * dst, 
                                     size_t count, 
                                     size_t datasize,
                                     ucc_tl_ucp_team_t * team,
                                     ucc_tl_ucp_task_t * task)
{
    int npes = team->size;
//    int rounds = (npes & 1) ? log2(npes) + 1 : log2(npes);
    int low_index = 0;
    int high_index = npes - 1;

    for (int i = 1; i < team->size; i<<=1) {
        int low_peer = (team->rank + low_index) % npes;
        int high_peer = (team->rank + high_index) % npes;

        printf("[%d] low %d, high %d\n", team->rank, low_peer, high_peer);

        ucc_tl_ucp_put_nb((void *)((ptrdiff_t) src + (ptrdiff_t) low_index * count * datasize), 
                          (void *)((ptrdiff_t) dst + (ptrdiff_t) low_index * count * datasize), 
                          datasize, low_peer, team, task);

        ucc_tl_ucp_put_nb((void *)((ptrdiff_t) src + (ptrdiff_t) high_index *count* datasize), 
                          (void *)((ptrdiff_t) dst + (ptrdiff_t) high_index *count* datasize), 
                          datasize, high_peer, team, task);


        ++low_index;
        --high_index;
    }
}

static inline void invert(void * block, size_t datasize, int low, int high)
{
    for (; low < high; ++low, --high) {
        uint64_t tmp;
        memcpy(&tmp, block + low * datasize, datasize);
        memcpy(block + low * datasize, block + high * datasize, datasize);
        memcpy(block + high * datasize, &tmp, datasize);
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
    size_t nelems = task->args.src.info.count;
    size_t datasize = sizeof(uint64_t); //(task->args.src.info.datatype == UCC_DT_INT64) ? sizeof(uint64_t) : sizeof(uint32_t); // FIXME
    long * pSync = team->pSync;
    ucc_status_t status = UCC_INPROGRESS; 

    printf("count: %lu\n", nelems);

    // FIXME: convert to non-blocking...

    if (team->rank != 0) {
        bruck_phase1_rotate(src, dst, nelems, datasize, team);
    }
    bruck_phase2_comm(src, dst, nelems, datasize, team, task);
    while (UCC_INPROGRESS == (status = ucc_tl_ucp_barrier_common_progress(ctask, 2)));
    pSync[0] = -1;
    pSync[1] = -1;
    bruck_phase3_invert(dst, nelems, datasize, team, task);

    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return UCC_OK;
}
#if 0
/*
 * linear 
 */
ucc_status_t ucc_tl_ucp_alltoall_onesided_progress_sync_entry(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ptrdiff_t src = (ptrdiff_t) task->args.src.info.buffer;
    ptrdiff_t dest = (ptrdiff_t) task->args.dst.info.buffer;
    size_t nelems = task->args.src.info.count;
    long * pSync = team->pSync; 
    ucc_rank_t mype = team->rank;
    ucc_rank_t npes = team->size;
    ucc_status_t status = UCC_INPROGRESS;

    if (nelems > 0) {
    // put most of the work here
    // alltoall
        ucc_rank_t peer;
        dest = dest + mype * nelems;
        ucc_rank_t start = (mype + 1) % npes;
        peer = start;
        do {
            ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest, nelems, peer, team, task);
            ++peer;
            peer = peer % npes;
        } while (peer != start);

        task->args.src.info.count = 0;
    }

    // locally check for completion 

/*
    // barrier
    status = ucc_tl_ucp_barrier_common_progress(ctask, pSync);
//    status = UCC_OK;
    if (status == UCC_INPROGRESS) {
        return status;
    }

    pSync[0] = -1;
    pSync[1] = -1;
*/
    task->super.super.status = status;
    ucc_task_complete(ctask);
    return status;
}
#endif
//ucc_status_t ucc_tl_ucp_barrier_knomial_progress(ucc_coll_task_t *coll_task);

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

/*
 * linear 
 */
ucc_status_t ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    //ucc_tl_ucp_context_t *ctx  = UCC_TL_UCP_TEAM_CTX(team);
    ptrdiff_t src = (ptrdiff_t) task->args.src.info.buffer;
    ptrdiff_t dest = (ptrdiff_t) task->args.dst.info.buffer;
    size_t nelems = task->args.src.info.count;
    ucc_rank_t mype = team->rank;
    ucc_rank_t npes = team->size;
    ucc_rank_t peer;
    ucc_status_t status = UCC_INPROGRESS;

    if (nelems > 0) {
    // put most of the work here
    // alltoall
        dest = dest + mype * nelems;
        ucc_rank_t start = (mype + 1) % npes;
        peer = start;
        do {
            ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest, nelems, peer, team, task);
            ++peer;
            peer = peer % npes;
        } while (peer != start);
      //  ucc_tl_ucp_flush(team);


        task->args.src.info.count = 0;
    //    task->barrier.phase = 0;//UCC_KN_PHASE_INIT;
/*        ucc_knomial_pattern_init(team->size, team->rank,
                             ucc_min(UCC_TL_UCP_TEAM_LIB(team)->
                                     cfg.barrier_kn_radix, team->size),
                             &task->barrier.p);
*/
    }
#if 1
    // barrier
    status = ucc_tl_ucp_barrier_common_progress(ctask, 0);
    if (status == UCC_INPROGRESS) {
     //   ucc_tl_ucp_flush(team);
        return status;
    }
/*
    status = ucc_tl_ucp_barrier_a2a_knomial_progress(&task->super);
    if (status == UCC_INPROGRESS) {
        return status;
    }
*/
/*
    while (UCC_INPROGRESS == (status = ucc_tl_ucp_barrier_a2a_knomial_progress(ctask))) {
        ucc_context_progress((ucc_context_h) ctx->super.super.ucc_context);
    }
  */  
    
#else
    task->barrier.phase = 0;
    while (UCC_INPROGRESS == (status = ucc_tl_ucp_barrier_common_progress(ctask, 2)));
    long * pSync = team->pSync; 
    pSync[2] = pSync[3] = -1;
#endif
    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return UCC_OK;
}
