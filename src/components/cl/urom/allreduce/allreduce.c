/**
 * Copyright (c) 2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "allreduce.h"
#include "core/ucc_team.h"
#include "core/ucc_service_coll.h"
#include "components/tl/ucp/tl_ucp.h"

#include <ucp/api/ucp.h>

ucc_base_coll_alg_info_t
    ucc_cl_urom_allreduce_algs[UCC_CL_UROM_ALLREDUCE_ALG_LAST + 1] = {
        [UCC_CL_UROM_ALLREDUCE_ALG_FULL] =
            {.id   = UCC_CL_UROM_ALLREDUCE_ALG_FULL,
             .name = "full",
             .desc = "full dpu-based offload"},
        [UCC_CL_UROM_ALLREDUCE_ALG_LAST] = {
            .id = 0, .name = NULL, .desc = NULL}};

ucc_status_t ucc_cl_urom_allreduce_triggered_post_setup(ucc_coll_task_t *task)
{
    return UCC_OK;
}
#if 1
static size_t dt_size(ucc_datatype_t ucc_dt)
{
    size_t size_mod = 8;

    switch(ucc_dt) {
        case UCC_DT_INT8:
        case UCC_DT_UINT8:
            size_mod = sizeof(char);
            break;
        case UCC_DT_INT32:
        case UCC_DT_UINT32:
        case UCC_DT_FLOAT32:
            size_mod = sizeof(int);
            break;
        case UCC_DT_INT64:
        case UCC_DT_UINT64:
        case UCC_DT_FLOAT64:
            size_mod = sizeof(uint64_t);
            break;
        case UCC_DT_INT128:
        case UCC_DT_UINT128:
        case UCC_DT_FLOAT128:
            size_mod = sizeof(__int128_t);
            break;
        default:
            break;
    }

    return size_mod;
}
#endif

ucc_status_t ucc_cl_urom_allreduce_triggered_post(ucc_ee_h ee, ucc_ev_t *ev,
                                        ucc_coll_task_t *coll_task)
{
    ucc_status_t        status;
    ucc_ev_t            post_event;

    ucc_assert(ee->ee_type == UCC_EE_CUDA_STREAM);
    coll_task->ee = ee;
    status = coll_task->post(coll_task);
    if (ucc_likely(status == UCC_OK)) {
        post_event.ev_type         = UCC_EVENT_COLLECTIVE_POST;
        post_event.ev_context_size = 0;
        post_event.ev_context      = NULL;
        post_event.req             = &coll_task->super;
        ucc_ee_set_event_internal(coll_task->ee, &post_event,
                                  &coll_task->ee->event_out_queue);
    }
    return status;
}

static ucc_status_t ucc_cl_urom_allreduce_full_start(ucc_coll_task_t *task)
{
    ucc_cl_urom_team_t *cl_team =
        ucc_derived_of(task->team, ucc_cl_urom_team_t);
    ucc_cl_urom_context_t *ctx = UCC_CL_UROM_TEAM_CTX(cl_team);
    ucc_cl_urom_lib_t     *cl_lib =
        ucc_derived_of(ctx->super.super.lib, ucc_cl_urom_lib_t);
    ucc_coll_args_t  *coll_args = &task->bargs.args;
    urom_status_t     urom_status;
    urom_worker_cmd_t coll_cmd = {
        .cmd_type               = UROM_WORKER_CMD_UCC,
        .ucc.dpu_worker_id      = UCC_CL_TEAM_RANK(cl_team),
        .ucc.cmd_type           = UROM_WORKER_CMD_UCC_COLL,
        .ucc.coll_cmd.coll_args = coll_args,
        .ucc.coll_cmd.team      = cl_team->teams[0],
        .ucc.coll_cmd.use_xgvmi = 0, //ctx->xgvmi_enabled,
    };
    if (coll_args->src.info.mem_type == UCC_MEMORY_TYPE_CUDA) {
        coll_args->src.info.mem_type = UCC_MEMORY_TYPE_HOST;
        coll_args->dst.info.mem_type = UCC_MEMORY_TYPE_HOST;

        urom_status =
            urom_worker_push_cmdq(cl_lib->urom_ctx.urom_worker, 0, &coll_cmd);
        coll_args->src.info.mem_type = UCC_MEMORY_TYPE_CUDA;
        coll_args->dst.info.mem_type = UCC_MEMORY_TYPE_CUDA;
    } else {
        urom_status =
            urom_worker_push_cmdq(cl_lib->urom_ctx.urom_worker, 0, &coll_cmd);
    }

    if (UROM_OK != urom_status) {
        cl_debug(&cl_lib->super, "failed to push collective to urom");
        return UCC_ERR_NO_MESSAGE;
    }

    task->status = UCC_INPROGRESS;
    cl_debug(&cl_lib->super, "pushed the collective to urom");
    return ucc_progress_queue_enqueue(ctx->super.super.ucc_context->pq, task);
}

static ucc_status_t ucc_cl_urom_allreduce_full_finalize(ucc_coll_task_t *task)
{
    ucc_cl_urom_schedule_t *schedule =
        ucc_derived_of(task, ucc_cl_urom_schedule_t);
    ucc_status_t status;

    status = ucc_schedule_finalize(task);
    ucc_cl_urom_put_schedule(&schedule->super.super);
    return status;
}

static void ucc_cl_urom_allreduce_full_progress(ucc_coll_task_t *ctask)
{
    ucc_cl_urom_team_t *cl_team =
        ucc_derived_of(ctask->team, ucc_cl_urom_team_t);
    ucc_cl_urom_context_t *ctx = UCC_CL_UROM_TEAM_CTX(cl_team);
    ucc_cl_urom_lib_t     *cl_lib =
        ucc_derived_of(ctx->super.super.lib, ucc_cl_urom_lib_t);
    urom_status_t         urom_status = 0;
    urom_worker_notify_t *notif;

    urom_status =
        urom_worker_pop_notifyq(cl_lib->urom_ctx.urom_worker, 0, &notif);
    if (UROM_ERR_QUEUE_EMPTY == urom_status) {
        return;
    }

    if (urom_status < 0) {
        cl_error(cl_lib, "Error in UROM");
        ctask->status = UCC_ERR_NO_MESSAGE;
        return;
    }

    if (notif->notify_type != UROM_WORKER_NOTIFY_UCC) {
        cl_debug(cl_lib, "WRONG NOTIFICATION (%ld != %d)", notif->notify_type,
                 UROM_WORKER_NOTIFY_UCC);
        return;
    }
    size_t size_mod = dt_size(ctask->bargs.args.dst.info.datatype);

    if ((ucc_status_t)notif->ucc.status == UCC_OK) {
        ucc_mc_memcpy(ctx->old_dest, ctask->bargs.args.dst.info.buffer,
                      ctask->bargs.args.dst.info.count * size_mod,
                      ctask->bargs.args.dst.info.mem_type,
                      UCC_MEMORY_TYPE_HOST);
        ctask->bargs.args.dst.info.buffer = ctx->old_dest;
        ctask->bargs.args.src.info.buffer = ctx->old_src;
    }
    cl_debug(&cl_lib->super, "completed the collective from urom");
    cl_debug(&cl_lib->super, "performing barrier");

    ctask->status = (ucc_status_t)notif->ucc.status;
}

ucc_status_t ucc_cl_urom_allreduce_full_init(
                         ucc_base_coll_args_t *coll_args, ucc_base_team_t *team,
                         ucc_coll_task_t **task)
{
    ucc_cl_urom_team_t     *cl_team = ucc_derived_of(team, ucc_cl_urom_team_t);
    ucc_cl_urom_context_t *ctx  = UCC_CL_UROM_TEAM_CTX(cl_team);
    ucc_cl_urom_lib_t *cl_lib = ucc_derived_of(ctx->super.super.lib, ucc_cl_urom_lib_t);

    ucc_cl_urom_schedule_t *cl_schedule;
    ucc_base_coll_args_t    args;
    ucc_schedule_t         *schedule;
    ucc_status_t            status;

    cl_schedule = ucc_cl_urom_get_schedule(cl_team);
    if (ucc_unlikely(!cl_schedule)) {
        return UCC_ERR_NO_MEMORY;
    }
    schedule = &cl_schedule->super.super;
    size_t size_mod = dt_size(coll_args->args.src.info.datatype);
    size_t count = coll_args->args.src.info.count * size_mod;
    //memcpy args to xgvmi buffer
    void * ptr = ctx->xgvmi.xgvmi_buffer + (cl_lib->cfg.xgvmi_buffer_size * (schedule->super.seq_num % cl_lib->cfg.num_buffers));

    ctx->old_src = coll_args->args.src.info.buffer;
    coll_args->args.src.info.buffer = ptr;
    ctx->old_dest = coll_args->args.dst.info.buffer;
    coll_args->args.dst.info.buffer = ptr + count;
    memcpy(&args, coll_args, sizeof(args));
    status = ucc_schedule_init(schedule, &args, team); 
    if (UCC_OK != status) {
        ucc_cl_urom_put_schedule(schedule);
        return status;
    }

    schedule->super.post           = ucc_cl_urom_allreduce_full_start;
    schedule->super.progress       = ucc_cl_urom_allreduce_full_progress;
    schedule->super.finalize       = ucc_cl_urom_allreduce_full_finalize;
    schedule->super.triggered_post = ucc_cl_urom_allreduce_triggered_post;
    schedule->super.triggered_post_setup =
        ucc_cl_urom_allreduce_triggered_post_setup;

    *task = &schedule->super;
    cl_debug(cl_lib, "urom coll init'd");
    return UCC_OK;
}
