/**
 * Copyright (c) 2021, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "cl_urom.h"
#include "utils/ucc_coll_utils.h"

#include "components/tl/ucp/tl_ucp.h"

#include <urom/api/urom.h>
#include <urom/api/urom_ucc.h>

ucc_status_t ucc_cl_urom_alltoall_full_init(
                         ucc_base_coll_args_t *coll_args, ucc_base_team_t *team,
                         ucc_coll_task_t **task);

ucc_status_t ucc_cl_urom_alltoallv_full_init(
                         ucc_base_coll_args_t *coll_args, ucc_base_team_t *team,
                         ucc_coll_task_t **task);

ucc_status_t ucc_cl_urom_coll_init(ucc_base_coll_args_t *coll_args,
                                   ucc_base_team_t      *team,
                                   ucc_coll_task_t     **task)
{
    ucc_cl_urom_team_t *cl_team = ucc_derived_of(team, ucc_cl_urom_team_t);
    ucc_cl_urom_context_t *ctx  = UCC_CL_UROM_TEAM_CTX(cl_team);
    ucc_cl_urom_lib_t *urom_lib = ucc_derived_of(ctx->super.super.lib, ucc_cl_urom_lib_t);
    int ucp_index = urom_lib->tl_ucp_index;
    ucc_tl_ucp_context_t *tl_ctx = ucc_derived_of(ctx->super.tl_ctxs[ucp_index], ucc_tl_ucp_context_t);
    urom_status_t urom_status;
    if (!urom_lib->urom_ctx.pass_dc_exist) {
        urom_worker_cmd_t pass_dc_cmd = {
            .cmd_type = UROM_WORKER_CMD_UCC,
            .ucc.cmd_type = UROM_WORKER_CMD_CREATE_PASSIVE_DATA_CHANNEL,
            .ucc.dpu_worker_id = ctx->ctx_rank,
            .ucc.pass_dc_create_cmd.ucp_addr = tl_ctx->worker.worker_address,
            .ucc.pass_dc_create_cmd.addr_len = tl_ctx->worker.ucp_addrlen,
        };
        urom_worker_notify_t *notif;

        urom_worker_push_cmdq(urom_lib->urom_ctx.urom_worker, 0, &pass_dc_cmd);
        while (UROM_ERR_QUEUE_EMPTY ==
               (urom_status = urom_worker_pop_notifyq(urom_lib->urom_ctx.urom_worker, 0, &notif))) {
            sched_yield();
        }
        if ((ucc_status_t) notif->ucc.status != UCC_OK) {
            return (ucc_status_t) notif->ucc.status;
        }
        urom_lib->urom_ctx.pass_dc_exist = 1;
    }
    switch (coll_args->args.coll_type) {
        case UCC_COLL_TYPE_ALLTOALL:
            return ucc_cl_urom_alltoall_full_init(coll_args, team, task);
        case UCC_COLL_TYPE_ALLTOALLV:
            return ucc_cl_urom_alltoallv_full_init(coll_args, team, task);
        default:
            cl_error(urom_lib, "coll_type %s is not supported", ucc_coll_type_str(coll_args->args.coll_type));
            break;
    }
    return UCC_ERR_NOT_SUPPORTED;
}
