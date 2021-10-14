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
    task->super.super.status = status;
    ucc_task_complete(ctask);

    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t     *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t * team = task->team;
    ptrdiff_t src = (ptrdiff_t) task->args.src.info.buffer;
    ptrdiff_t dest = (ptrdiff_t) task->args.dst.info.buffer;
    size_t nelems = task->args.src.info.count;
    ucc_rank_t mype = team->rank;
    ucc_rank_t npes = team->size;
    int * phase = &task->barrier.phase;
    ucc_status_t status = UCC_INPROGRESS;
    long * pSync = task->args.pSync;
    ucc_rank_t peer;

    if (*phase == 1) {
        goto wait;
    }

    dest = dest + mype * nelems;
    ucc_rank_t start = (mype + 1) % npes; //p2_mod(mype + 1, npes);
    ucc_tl_ucp_put_nb((void *)(src + start * nelems), (void *)dest, nelems, start, team, task);
    ucc_tl_ucp_atomic_inc();
    for (peer = (start + 1) % npes; peer != start; peer = (peer + 1) % npes) {
        ucc_tl_ucp_put_nb((void *)(src + peer * nelems), (void *)dest, nelems, peer, team, task);
        ucc_tl_ucp_atomic_inc();
    }

wait:
    if (*pSync < npes - 2) {
        return UCC_INPROGRESS;
    }
      
    task->super.super.status = UCC_OK;
    ucc_task_complete(ctask);
    return UCC_OK;
}

