/**
 * Copyright (c) 2021, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */
#include "config.h"
#include "tl_ucp.h"
#include "barrier.h"
#include "barrier_onesided.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "tl_ucp_sendrecv.h"

ucc_status_t ucc_tl_ucp_barrier_knomial_start(ucc_coll_task_t *task);
void ucc_tl_ucp_barrier_knomial_progress(ucc_coll_task_t *task);

ucc_status_t ucc_tl_ucp_barrier_onesided_start(ucc_coll_task_t *task);
void ucc_tl_ucp_barrier_onesided_progress(ucc_coll_task_t *task);

ucc_base_coll_alg_info_t
    ucc_tl_ucp_barrier_algs[UCC_TL_UCP_BARRIER_ALG_LAST + 1] = {
        [UCC_TL_UCP_BARRIER_ALG_KNOMIAL] =
            {.id   = UCC_TL_UCP_BARRIER_ALG_KNOMIAL,
             .name = "knomial",
             .desc = "O(log(n)) steps Knomial Barrier"},
        [UCC_TL_UCP_BARRIER_ALG_ONESIDED] =
            {.id   = UCC_TL_UCP_BARRIER_ALG_ONESIDED,
             .name = "onesided",
             .desc = "O(log(n)) steps Onesided Barrier"},
        [UCC_TL_UCP_BARRIER_ALG_LAST] = {
            .id = 0, .name = NULL, .desc = NULL}};

ucc_status_t ucc_tl_ucp_barrier_knomial_init(ucc_base_coll_args_t *coll_args,
                                            ucc_base_team_t *team,
                                            ucc_coll_task_t **task_h)
{
    ucc_tl_ucp_task_t *task = ucc_tl_ucp_init_task(coll_args, team);
    if (!task) {
        return UCC_ERR_NO_MEMORY;
    }

    task->super.post     = ucc_tl_ucp_barrier_knomial_start;
    task->super.progress = ucc_tl_ucp_barrier_knomial_progress;
    *task_h = &task->super;
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_barrier_init(ucc_tl_ucp_task_t *task)
{
    task->super.post     = ucc_tl_ucp_barrier_knomial_start;
    task->super.progress = ucc_tl_ucp_barrier_knomial_progress;

    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_barrier_onesided_init(ucc_base_coll_args_t *coll_args,
                                             ucc_base_team_t *team,
                                             ucc_coll_task_t **task_h)
{
    ucc_tl_ucp_task_t *task = ucc_tl_ucp_init_task(coll_args, team);
    if (!task) {
        return UCC_ERR_NO_MEMORY;
    }

    task->super.post     = ucc_tl_ucp_barrier_onesided_start;
    task->super.progress = ucc_tl_ucp_barrier_onesided_progress;
    *task_h = &task->super;
    return UCC_OK;
}
