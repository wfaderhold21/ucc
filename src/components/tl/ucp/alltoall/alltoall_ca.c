/**
 * Copyright (c) 2021-2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "alltoall.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "tl_ucp_sendrecv.h"

/* update when pinger rtt complete */
#define MAGIC_NUMBER    32

void ucc_tl_ucp_alltoall_onesided_ca_progress(ucc_coll_task_t *ctask);

ucc_status_t ucc_tl_ucp_alltoall_onesided_ca_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ucc_tl_ucp_context_t *ctx            = UCC_TL_UCP_TEAM_CTX(team);
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start  = (grank + 1) % gsize;
    long *             pSync  = TASK_ARGS(task).global_work_buffer;
    int                revisit[128] = {0};
    int                nr_revisit = 0;
    int nr_revisit_max = 0;
    pinger_rtt_t       rtt = 1;
    ucc_rank_t         peer;
    int j = 0;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* TODO: change when support for library-based work buffers is complete */
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest   = dest + grank * nelems;
    /* maybe have a list of processes to send to, cut them out of the process list */
    for (peer = start; j < gsize; j++ ) {
        if (peer != grank) {
            pinger_query(ctx->pinger, ctx->pinger_peer[peer], &rtt);
//            printf("[%d] peer %d rtt: %ld\n", grank, peer, rtt);
        } else {
            rtt = 0;
        }
        if (rtt <= MAGIC_NUMBER) {
            UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)(src + peer * nelems),
                                            (void *)dest, nelems, peer, team, task),
                          task, out);
            UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, peer, team), task,
                          out);
        } else {
            revisit[nr_revisit++] = peer;
        }
        peer = (peer + 1) % gsize;
    }

    while (nr_revisit) {
        nr_revisit_max = nr_revisit;
        nr_revisit = 0;
        for (int i = 0; i < nr_revisit_max; i++) {
            peer = revisit[i];
            pinger_query(ctx->pinger, ctx->pinger_peer[peer], &rtt);
            if (rtt <= MAGIC_NUMBER) {
                UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)(src + peer * nelems),
                                            (void *)dest, nelems, peer, team, task),
                          task, out);
                UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, peer, team), task,
                          out);
            } else {
                revisit[nr_revisit++] = peer;
            }
        }
    }

    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
out:
    return task->super.status;
}

void ucc_tl_ucp_alltoall_onesided_ca_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    long *             pSync = TASK_ARGS(task).global_work_buffer;

    if (ucc_tl_ucp_test_onesided(task, gsize) == UCC_INPROGRESS) {
        return;
    }

    pSync[0]           = 0;
    task->super.status = UCC_OK;
}
