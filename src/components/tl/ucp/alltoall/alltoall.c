/**
 * Copyright (C) Mellanox Technologies Ltd. 2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "alltoall.h"

//ucc_status_t ucc_tl_ucp_alltoall_pairwise_start(ucc_coll_task_t *task);
//ucc_status_t ucc_tl_ucp_alltoall_pairwise_progress(ucc_coll_task_t *task);

ucc_base_coll_alg_info_t
    ucc_tl_ucp_alltoall_algs[UCC_TL_UCP_ALLTOALL_ALG_LAST + 1] = {
        [UCC_TL_UCP_ALLTOALL_ALG_ONESIDED] =
            {.id   = UCC_TL_UCP_ALLTOALL_ALG_ONESIDED,
             .name = "onesided ring",
             .desc = ""},


        [UCC_TL_UCP_ALLTOALL_ALG_OS_BRUCK] =
            {.id   = UCC_TL_UCP_ALLTOALL_ALG_OS_BRUCK,
             .name = "onesided bruck",
             .desc = ""},


        [UCC_TL_UCP_ALLTOALL_ALG_PAIRWISE] =
            {.id   = UCC_TL_UCP_ALLTOALL_ALG_PAIRWISE,
             .name = "pairwise",
             .desc =
                 "pairwise two-sided implementation"},
        [UCC_TL_UCP_ALLTOALL_ALG_LAST] = {
            .id = 0, .name = NULL, .desc = NULL}};


ucc_status_t ucc_tl_ucp_alltoall_init(ucc_tl_ucp_task_t *task)
{
    if ((task->args.mask & UCC_COLL_ARGS_FIELD_FLAGS) &&
        (task->args.flags & UCC_COLL_ARGS_FLAG_IN_PLACE)) {
        tl_debug(UCC_TL_TEAM_LIB(task->team),
                 "inplace alltoall is not supported");
        return UCC_ERR_NOT_SUPPORTED;
    }
    if ((task->args.src.info.datatype == UCC_DT_USERDEFINED) ||
        (task->args.dst.info.datatype == UCC_DT_USERDEFINED)) {
        tl_debug(UCC_TL_TEAM_LIB(task->team),
                 "user defined datatype is not supported");
        return UCC_ERR_NOT_SUPPORTED;
    }
    task->super.post     = ucc_tl_ucp_alltoall_pairwise_start;
    task->super.progress = ucc_tl_ucp_alltoall_pairwise_progress;
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_alltoall_pairwise_init(ucc_base_coll_args_t *coll_args,
                                                ucc_base_team_t *     team,
                                                ucc_coll_task_t **    task_h)
{
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;
    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    status = ucc_tl_ucp_alltoall_init(task);
    return status;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_init(ucc_base_coll_args_t *coll_args,
                                                ucc_base_team_t *     team,
                                                ucc_coll_task_t **    task_h)
{
    ucc_tl_ucp_task_t *task;
    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
//    status = ucc_tl_ucp_alltoall_init(task);

    if ((task->args.mask & UCC_COLL_ARGS_FIELD_FLAGS) &&
        (task->args.flags & UCC_COLL_ARGS_FLAG_IN_PLACE)) {
        tl_debug(UCC_TL_TEAM_LIB(task->team),
                 "inplace alltoall is not supported");
        return UCC_ERR_NOT_SUPPORTED;
    }
    if ((task->args.src.info.datatype == UCC_DT_USERDEFINED) ||
        (task->args.dst.info.datatype == UCC_DT_USERDEFINED)) {
        tl_debug(UCC_TL_TEAM_LIB(task->team),
                 "user defined datatype is not supported");
        return UCC_ERR_NOT_SUPPORTED;
    }
    task->super.post     = ucc_tl_ucp_alltoall_onesided_start;
    task->super.progress = ucc_tl_ucp_alltoall_onesided_progress;
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_nosync_progress(ucc_coll_task_t *ctask);
ucc_status_t ucc_tl_ucp_alltoall_os_bruck_init(ucc_base_coll_args_t *coll_args,
                                               ucc_base_team_t *     team,
                                               ucc_coll_task_t **    task_h)
{
    ucc_tl_ucp_task_t *task;
    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;

    if ((task->args.mask & UCC_COLL_ARGS_FIELD_FLAGS) &&
        (task->args.flags & UCC_COLL_ARGS_FLAG_IN_PLACE)) {
        tl_debug(UCC_TL_TEAM_LIB(task->team),
                 "inplace alltoall is not supported");
        return UCC_ERR_NOT_SUPPORTED;
    }
    if ((task->args.src.info.datatype == UCC_DT_USERDEFINED) ||
        (task->args.dst.info.datatype == UCC_DT_USERDEFINED)) {
        tl_debug(UCC_TL_TEAM_LIB(task->team),
                 "user defined datatype is not supported");
        return UCC_ERR_NOT_SUPPORTED;
    }
    task->super.post     = ucc_tl_ucp_alltoall_os_bruck_start;
    task->super.progress = ucc_tl_ucp_alltoall_onesided_nosync_progress;
    return UCC_OK;
}
