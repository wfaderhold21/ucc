/**
 * Copyright (c) 2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "gatherv.h"
#include "utils/ucc_coll_utils.h"

ucc_status_t ucc_tl_ucp_gatherv_linear_start(ucc_coll_task_t *task);

void ucc_tl_ucp_gatherv_linear_progress(ucc_coll_task_t *task);

ucc_base_coll_alg_info_t
    ucc_tl_ucp_gatherv_algs[UCC_TL_UCP_GATHERV_ALG_LAST + 1] = {
        [UCC_TL_UCP_GATHERV_ALG_LINEAR] =
            {.id   = UCC_TL_UCP_GATHERV_ALG_LINEAR,
             .name = "linear",
             .desc = "linear gatherv algorithm"},
        [UCC_TL_UCP_GATHERV_ALG_ONESIDED] =
            {.id   = UCC_TL_UCP_GATHERV_ALG_ONESIDED,
             .name = "onesided",
             .desc = "one-sided gatherv (root-driven get)"},
        [UCC_TL_UCP_GATHERV_ALG_LAST] = {
            .id = 0, .name = NULL, .desc = NULL}};

ucc_status_t ucc_tl_ucp_gatherv_linear_init_common(ucc_tl_ucp_task_t *task);

ucc_status_t ucc_tl_ucp_gatherv_init(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_coll_args_t   *args  = &TASK_ARGS(task);
    ucc_rank_t         trank = UCC_TL_TEAM_RANK(team);

    if (!ucc_coll_args_is_predefined_dt(args, trank)) {
        return UCC_ERR_NOT_SUPPORTED;
    }

    return ucc_tl_ucp_gatherv_linear_init_common(task);
}

/*
 * Score-path (ucc_base_coll_init_fn_t) wrapper for the two-sided linear
 * algorithm: create the task, then delegate to the 1-arg common init. This
 * mirrors ucc_tl_ucp_gather_knomial_init. The legacy coll_init path
 * (ucc_tl_ucp_gatherv_init) calls the common init directly on its own task.
 */
ucc_status_t ucc_tl_ucp_gatherv_linear_init(ucc_base_coll_args_t *coll_args,
                                            ucc_base_team_t *team,
                                            ucc_coll_task_t **task_h)
{
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;

    task = ucc_tl_ucp_init_task(coll_args, team);
    if (ucc_unlikely(NULL == task)) {
        return UCC_ERR_NO_MEMORY;
    }
    status = ucc_tl_ucp_gatherv_linear_init_common(task);
    if (ucc_unlikely(UCC_OK != status)) {
        ucc_tl_ucp_put_task(task);
        return status;
    }
    *task_h = &task->super;
    return UCC_OK;
}
