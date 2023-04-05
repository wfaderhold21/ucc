/**
 * Copyright (c) 2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * Copyright (c) Meta Platforms, Inc. and affiliates. 2022.
 *
 * See file LICENSE for terms.
 */

#include "alltoallv.h"
#include "core/ucc_team.h"
#include "core/ucc_service_coll.h"
#include "components/tl/ucp/tl_ucp.h"

#include <ucp/api/ucp.h>

ucc_base_coll_alg_info_t
    ucc_cl_urom_alltoallv_algs[UCC_CL_UROM_ALLTOALLV_ALG_LAST + 1] = {
        [UCC_CL_UROM_ALLTOALLV_ALG_FULL] =
            {.id   = UCC_CL_UROM_ALLTOALLV_ALG_FULL,
             .name = "urom_full_offload",
             .desc = "full offload of alltoallv"},
        [UCC_CL_UROM_ALLTOALLV_ALG_LAST] = {
            .id = 0, .name = NULL, .desc = NULL}};

ucc_status_t ucc_cl_urom_alltoallv_triggered_post_setup(ucc_coll_task_t *task)
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

ucc_status_t ucc_cl_urom_alltoallv_triggered_post(ucc_ee_h ee, ucc_ev_t *ev,
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

//static int n_cache_el = 0;

static ucc_status_t ucc_cl_urom_alltoallv_full_start(ucc_coll_task_t *task)
{
    ucc_cl_urom_team_t     *cl_team = ucc_derived_of(task->team, ucc_cl_urom_team_t);
    ucc_cl_urom_context_t *ctx  = UCC_CL_UROM_TEAM_CTX(cl_team);
    ucc_cl_urom_lib_t *cl_lib = ucc_derived_of(ctx->super.super.lib, ucc_cl_urom_lib_t);
//    ucc_team_t *core_team = cl_team->super.super.params.team;
    ucc_coll_args_t        *coll_args = &task->bargs.args;
/*    ucc_subset_t subset = {.map.type = UCC_EP_MAP_FULL,
                           .map.ep_num = core_team->size,
                           .myrank = core_team->rank};
    ucc_service_coll_req_t *scoll_req;*/
    urom_status_t       urom_status;
    urom_worker_cmd_t   coll_cmd = {
        .cmd_type = UROM_WORKER_CMD_UCC,
        .ucc.dpu_worker_id = UCC_CL_TEAM_RANK(cl_team),
        .ucc.cmd_type      = UROM_WORKER_CMD_UCC_COLL,
        .ucc.coll_cmd.coll_args = coll_args,
        .ucc.coll_cmd.team = cl_team->teams[0],
        .ucc.coll_cmd.use_xgvmi = ctx->xgvmi_enabled,
        .ucc.coll_cmd.work_buffer_size = 0,
        .ucc.coll_cmd.work_buffer = NULL,
        .ucc.coll_cmd.team_size = UCC_CL_TEAM_SIZE(cl_team),
    };
#if 0
    ucp_mem_map_params_t mem_params;
    //ucc_status_t ucc_status;
    ucs_status_t ucs_status;
    if ((cl_team->cache_el[n_cache_el].base_src_va != coll_args->src.info.buffer
      && cl_team->cache_el[n_cache_el].src_va_len != coll_args->src.info.count * ucc_dt_size(coll_args->src.info.datatype)) //||
        /*cl_team->cache_el[n_cache_el].base_dst_va != coll_args->dst.info.buffer*/) {
        ucp_memh_pack_params_t pack_params;
        ucc_tl_ucp_context_t *tl_ctx;
        tl_ctx = ucc_derived_of(ctx->super.tl_ctxs[cl_lib->tl_ucp_index], ucc_tl_ucp_context_t);
    // check if already did this 
    //
    // src
        mem_params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
        cl_team->cache_el[n_cache_el].base_src_va = mem_params.address = coll_args->src.info.buffer;
        cl_team->cache_el[n_cache_el].src_va_len = mem_params.length = coll_args->src.info.count * ucc_dt_size(coll_args->src.info.datatype);
        printf("MAPPING %ld bytes\n", coll_args->src.info.count * ucc_dt_size(coll_args->src.info.datatype));
    
        ucs_status = ucp_mem_map(tl_ctx->worker.ucp_context, &mem_params, &cl_team->cache_el[n_cache_el].src_memh);
        assert(ucs_status == UCS_OK);

        cl_team->cache_el[n_cache_el].base_dst_va = mem_params.address = coll_args->dst.info.buffer;
        cl_team->cache_el[n_cache_el].dst_va_len = mem_params.length = coll_args->dst.info.count * ucc_dt_size(coll_args->dst.info.datatype);

        ucs_status = ucp_mem_map(tl_ctx->worker.ucp_context, &mem_params, &cl_team->cache_el[n_cache_el].dst_memh);
        assert(ucs_status == UCS_OK);
   
        if (cl_lib->cfg.use_xgvmi) {
            pack_params.field_mask = UCP_MEMH_PACK_PARAM_FIELD_FLAGS;
            pack_params.flags = UCP_MEMH_PACK_FLAG_EXPORT;
            ctx->xgvmi_enabled = 1;
    
            ucs_status = ucp_memh_pack(cl_team->cache_el[n_cache_el].src_memh, &pack_params, &cl_team->cache_el[n_cache_el].src_xgvmi_memh, &cl_team->cache_el[n_cache_el].src_packed_xgvmi_len);
            if (ucs_status != UCS_OK) {
                cl_error(&cl_lib->super.super, "ucp_memh_pack() returned error: %s", ucs_status_string(ucs_status));
                ctx->xgvmi_enabled = 0;
            } else {
                ctx->xgvmi_enabled = 1;
            }

            if (ctx->xgvmi_enabled) {
               ucs_status = ucp_memh_pack(cl_team->cache_el[n_cache_el].dst_memh, &pack_params, &cl_team->cache_el[n_cache_el].dst_xgvmi_memh, &cl_team->cache_el[n_cache_el].dst_packed_xgvmi_len);
                if (ucs_status != UCS_OK) {
                    cl_error(&cl_lib->super.super, "ucp_memh_pack() returned error: %s", ucs_status_string(ucs_status));
                    ctx->xgvmi_enabled = 0;
                } else {
                    ctx->xgvmi_enabled = 1;
                }
            }

        } else {
            ctx->xgvmi_enabled = 0;
//            printf("rkey packing\n");
            ucs_status = ucp_rkey_pack(tl_ctx->worker.ucp_context, cl_team->cache_el[n_cache_el].src_memh, &cl_team->cache_el[n_cache_el].packed_src_key, &cl_team->cache_el[n_cache_el].packed_src_key_len);
            assert(ucs_status == UCS_OK);
 
            ucs_status = ucp_rkey_pack(tl_ctx->worker.ucp_context, cl_team->cache_el[n_cache_el].dst_memh, &cl_team->cache_el[n_cache_el].packed_dst_key, &cl_team->cache_el[n_cache_el].packed_dst_key_len);
            assert(ucs_status == UCS_OK);
        }


    // pack the data
    // FIXME: assuming same size buffer lengths for now
        pass_info_t * sinfo;//, *rinfo;
        size_t el_size;
        size_t offset = 0;
        if (ctx->xgvmi_enabled == 1) {
            el_size = sizeof(size_t) * 3 + cl_team->cache_el[n_cache_el].src_packed_xgvmi_len + cl_team->cache_el[n_cache_el].dst_packed_xgvmi_len;
            sinfo = ucc_malloc(el_size);
            sinfo->src_xgvmi_size = cl_team->cache_el[n_cache_el].src_packed_xgvmi_len;
            sinfo->dst_xgvmi_size = cl_team->cache_el[n_cache_el].dst_packed_xgvmi_len;

            memcpy(sinfo->rkeys + offset, cl_team->cache_el[n_cache_el].src_xgvmi_memh, cl_team->cache_el[n_cache_el].src_packed_xgvmi_len);
            offset += cl_team->cache_el[n_cache_el].src_packed_xgvmi_len;
            memcpy(sinfo->rkeys + offset, cl_team->cache_el[n_cache_el].dst_xgvmi_memh, cl_team->cache_el[n_cache_el].dst_packed_xgvmi_len);
        } else {
            //printf("USING RKEYS!\n");
            el_size = sizeof(size_t) * 3 + cl_team->cache_el[n_cache_el].packed_src_key_len + cl_team->cache_el[n_cache_el].packed_dst_key_len;
            sinfo = ucc_malloc(el_size);
            sinfo->src_xgvmi_size = cl_team->cache_el[n_cache_el].packed_src_key_len;
            sinfo->dst_xgvmi_size = cl_team->cache_el[n_cache_el].packed_dst_key_len;

            memcpy(sinfo->rkeys + offset, cl_team->cache_el[n_cache_el].packed_src_key, cl_team->cache_el[n_cache_el].packed_src_key_len);
            offset += cl_team->cache_el[n_cache_el].packed_src_key_len;
            memcpy(sinfo->rkeys + offset, cl_team->cache_el[n_cache_el].packed_dst_key, cl_team->cache_el[n_cache_el].packed_dst_key_len);
        }
        sinfo->xgvmi_flag = ctx->xgvmi_enabled;

    // add in the psync
    if (coll_args->src.info.mem_type != UCC_MEMORY_TYPE_CUDA) {
        coll_args->mask |= UCC_COLL_ARGS_FIELD_FLAGS | UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER,
        coll_args->flags |= UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS,
        coll_args->global_work_buffer = sinfo;
        cl_team->cache_el[n_cache_el].pass_info = sinfo;
        cl_team->cache_el[n_cache_el].pass_info_size = el_size;
        coll_cmd.ucc.coll_cmd.work_buffer_size = el_size;
    } 
} else {
    if (coll_args->src.info.mem_type != UCC_MEMORY_TYPE_CUDA) {
        coll_args->mask |= UCC_COLL_ARGS_FIELD_FLAGS | UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER,
        coll_args->flags |= UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS,
        coll_args->global_work_buffer = cl_team->cache_el[n_cache_el].pass_info;
        coll_cmd.ucc.coll_cmd.work_buffer_size = cl_team->cache_el[n_cache_el].pass_info_size;
    }
}
    if (coll_args->src.info.mem_type != UCC_MEMORY_TYPE_CUDA) {
        coll_cmd.ucc.coll_cmd.work_buffer = coll_args->global_work_buffer;
    } 
#endif
        if (ctx->req_mc) {
            size_t size_mod = dt_size(coll_args->src.info_v.datatype);
            size_t total_count = 0;
            for (int i = 0; i < UCC_CL_TEAM_SIZE(cl_team); i++) { 
                if ((coll_args->mask & UCC_COLL_ARGS_FIELD_FLAGS) && 
                    (coll_args->flags & UCC_COLL_ARGS_FLAG_COUNT_64BIT)) {
                    uint64_t count = coll_args->src.info_v.counts[i] * size_mod;
                    total_count += count;
                } else {
                    uint32_t count = coll_args->src.info_v.counts[i] * size_mod;
                    total_count += count;
                }

                //printf("count: %lu\n", total_count);
            }
            //memcpy args to xgvmi buffer
            void * ptr = ctx->xgvmi.xgvmi_buffer;// + (cl_lib->cfg.xgvmi_buffer_size * (schedule->super.seq_num % cl_lib->cfg.num_buffers));
            //printf("ptr %p, old_src %p, size %lu bytes\n", ptr, ctx->old_src, total_count);
            ucc_mc_memcpy(ptr, ctx->old_src /*coll_args->args.src.info.buffer*/, total_count, UCC_MEMORY_TYPE_HOST, coll_args->src.info_v.mem_type);
        }

    if (coll_args->src.info_v.mem_type == UCC_MEMORY_TYPE_CUDA) {
        coll_args->src.info_v.mem_type = UCC_MEMORY_TYPE_HOST;
        coll_args->dst.info_v.mem_type = UCC_MEMORY_TYPE_HOST;


        urom_status = urom_worker_push_cmdq(cl_lib->urom_ctx.urom_worker, 0, &coll_cmd);
        coll_args->src.info_v.mem_type = UCC_MEMORY_TYPE_CUDA;
        coll_args->dst.info_v.mem_type = UCC_MEMORY_TYPE_CUDA;
    } else {
        urom_status = urom_worker_push_cmdq(cl_lib->urom_ctx.urom_worker, 0, &coll_cmd);
    }
    if (UROM_OK != urom_status) {
        cl_debug(&cl_lib->super, "failed to push collective to urom");
        return UCC_ERR_NO_MESSAGE;
    }

    task->status = UCC_INPROGRESS;
    cl_debug(&cl_lib->super, "pushed the collective to urom");
    return ucc_progress_queue_enqueue(ctx->super.super.ucc_context->pq, task);
}

static ucc_status_t ucc_cl_urom_alltoallv_full_finalize(ucc_coll_task_t *task)
{
    ucc_cl_urom_schedule_t *schedule =
        ucc_derived_of(task, ucc_cl_urom_schedule_t);
    ucc_status_t status;

    status = ucc_schedule_finalize(task);
    ucc_cl_urom_put_schedule(&schedule->super.super);
    return status;
}

static void ucc_cl_urom_alltoallv_full_progress(ucc_coll_task_t *ctask)
{
    ucc_cl_urom_team_t     *cl_team = ucc_derived_of(ctask->team, ucc_cl_urom_team_t);
    ucc_cl_urom_context_t *ctx  = UCC_CL_UROM_TEAM_CTX(cl_team);
    ucc_cl_urom_lib_t *cl_lib = ucc_derived_of(ctx->super.super.lib, ucc_cl_urom_lib_t);
    urom_status_t           urom_status = 0;
    urom_worker_notify_t   *notif;

    urom_status = urom_worker_pop_notifyq(cl_lib->urom_ctx.urom_worker, 0, &notif);
    if (UROM_ERR_QUEUE_EMPTY == urom_status) {
        return;
    }

    if (urom_status < 0) {
        cl_error(cl_lib, "Error in UROM");
        ctask->status = UCC_ERR_NO_MESSAGE;
        return;
    }

    if (notif->notify_type != UROM_WORKER_NOTIFY_UCC) {
        cl_debug(cl_lib, "WRONG NOTIFICATION (%ld != %d)", notif->notify_type, UROM_WORKER_NOTIFY_UCC);
        return;
    }
    if (ctx->req_mc) {
//        size_t size_mod = dt_size(ctask->bargs.args.dst.info.datatype);

        if ((ucc_status_t) notif->ucc.status == UCC_OK) {
            //ucc_mc_memcpy(ctx->old_dest, ctask->bargs.args.dst.info_v.buffer, ctask->bargs.args.dst.info_v.count * size_mod, ctask->bargs.args.dst.info_v.mem_type, UCC_MEMORY_TYPE_HOST);
            ctask->bargs.args.dst.info_v.buffer = ctx->old_dest;
            ctask->bargs.args.src.info_v.buffer = ctx->old_src;
        }
    }
    cl_debug(&cl_lib->super, "completed the collective from urom");
    cl_debug(&cl_lib->super, "performing barrier");

    ctask->status = (ucc_status_t) notif->ucc.status;
}  

ucc_status_t ucc_cl_urom_alltoallv_full_init(
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
    if (ctx->req_mc) {
//        size_t size_mod = dt_size(coll_args->args.src.info.datatype);
//        size_t count = coll_args->args.src.info.count * size_mod;
        //memcpy args to xgvmi buffer
        void * ptr = ctx->xgvmi.xgvmi_buffer;// + (cl_lib->cfg.xgvmi_buffer_size * (schedule->super.seq_num % cl_lib->cfg.num_buffers));

        ctx->old_src = coll_args->args.src.info_v.buffer;
        coll_args->args.src.info_v.buffer = ptr;
        ctx->old_dest = coll_args->args.dst.info_v.buffer;
        coll_args->args.dst.info_v.buffer = ptr;// + count;
    }
    memcpy(&args, coll_args, sizeof(args));
    status = ucc_schedule_init(schedule, &args, team); 
    if (UCC_OK != status) {
        ucc_cl_urom_put_schedule(schedule);
        return status;
    }

    schedule->super.post           = ucc_cl_urom_alltoallv_full_start;
    schedule->super.progress       = ucc_cl_urom_alltoallv_full_progress;
    schedule->super.finalize       = ucc_cl_urom_alltoallv_full_finalize;
    schedule->super.triggered_post = ucc_cl_urom_alltoallv_triggered_post;
    schedule->super.triggered_post_setup =
        ucc_cl_urom_alltoallv_triggered_post_setup;

    *task = &schedule->super;
    cl_debug(cl_lib, "urom coll init'd");
    return UCC_OK;
}
