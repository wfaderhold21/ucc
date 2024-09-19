/**
 * Copyright (c) 2020-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "cl_urom.h"
#include "cl_urom_coll.h"
#include "utils/ucc_malloc.h"

#include "components/tl/ucp/tl_ucp.h"

#define XGVMI_SIZE (1<<30)

ucc_status_t memcpy_init(void);

UCC_CLASS_INIT_FUNC(ucc_cl_urom_context_t,
                    const ucc_base_context_params_t *params,
                    const ucc_base_config_t *config)
{
    const ucc_cl_context_config_t *cl_config =
        ucc_derived_of(config, ucc_cl_context_config_t);
    ucc_config_names_array_t *tls = &cl_config->cl_lib->tls.array;
    ucc_cl_urom_lib_t *urom_lib = ucc_derived_of(cl_config->cl_lib, ucc_cl_urom_lib_t);
    ucc_mem_map_params_t ucc_mem_params = params->params.mem_params; 
    ucc_lib_params_t lib_params = {
        .mask = UCC_LIB_PARAM_FIELD_THREAD_MODE,
        .thread_mode = UCC_THREAD_SINGLE,
    };
    urom_worker_cmd_t init_cmd = {
        .cmd_type = UROM_WORKER_CMD_UCC,
        .ucc.cmd_type = UROM_WORKER_CMD_UCC_LIB_CREATE,
        .ucc.lib_create_cmd.params = &lib_params,
    };
    urom_worker_cmd_t ctx_cmd = {
        .cmd_type = UROM_WORKER_CMD_UCC,
        .ucc.dpu_worker_id = params->params.oob.oob_ep,
        .ucc.cmd_type = UROM_WORKER_CMD_UCC_CONTEXT_CREATE,
        .ucc.context_create_cmd = 
        {
            .start = 0,
            .stride = 1,
            .size = params->params.oob.n_oob_eps,
        },
    };

    ucc_tl_ucp_context_t *tl_ctx;
    ucc_status_t status;
    urom_status_t urom_status;
    int          i;
    int          ucp_index = -1;
    int          n_segments = 0;
    urom_mem_map_t *domain_mem_map;
    urom_domain_params_t urom_domain_params;
    urom_worker_notify_t *notif_lib = NULL;
    urom_worker_notify_t *notif_ctx = NULL;
    urom_worker_params_t worker_params;
    ucp_mem_map_params_t mem_params;
    ucp_memh_pack_params_t pack_params;
    ucs_status_t ucs_status;
    int xgvmi_level = 0;

    UCC_CLASS_CALL_SUPER_INIT(ucc_cl_context_t, cl_config,
                              params->context);
    if (tls->count == 1 && !strcmp(tls->names[0], "all")) {
        tls = &params->context->all_tls;
    }
    self->super.tl_ctxs = ucc_malloc(sizeof(ucc_tl_context_t*) * tls->count,
                               "cl_urom_tl_ctxs");
    if (!self->super.tl_ctxs) {
        cl_error(cl_config->cl_lib, "failed to allocate %zd bytes for tl_ctxs",
                 sizeof(ucc_tl_context_t**) * tls->count);
        return UCC_ERR_NO_MEMORY;
    }
    self->super.n_tl_ctxs = 0;
    for (i = 0; i < tls->count; i++) {
        ucc_debug("TL NAME[%d]: %s", i, tls->names[i]);
        status = ucc_tl_context_get(params->context, tls->names[i],
                                   &self->super.tl_ctxs[self->super.n_tl_ctxs]);
        if (UCC_OK != status) {
            cl_debug(cl_config->cl_lib,
                     "TL %s context is not available, skipping", tls->names[i]);
        } else {
            if (strcmp(tls->names[i], "ucp") == 0) {
                ucp_index = self->super.n_tl_ctxs;
                urom_lib->tl_ucp_index = ucp_index;
            }
            self->super.n_tl_ctxs++;
        }
    }
    if (0 == self->super.n_tl_ctxs) {
        cl_error(cl_config->cl_lib, "no TL contexts are available");
        ucc_free(self->super.tl_ctxs);
        self->super.tl_ctxs = NULL;
        return UCC_ERR_NOT_FOUND;
    }

    if (!urom_lib->urom_ctx.urom_worker) {
        /* issues
         *  1. user creates multiple contexts? 
         *  2. worker sharing
         */
        urom_lib->urom_ctx.worker_id = UROM_WORKER_ID_ANY;
        self->ctx_rank = params->params.oob.oob_ep;
        urom_status = urom_worker_spawn(
            urom_lib->urom_ctx.urom_service, UROM_WORKER_TYPE_UCC, urom_lib->urom_ctx.urom_worker_addr,
            &urom_lib->urom_ctx.urom_worker_len, &urom_lib->urom_ctx.worker_id);
        if (UROM_OK != urom_status) {
            cl_error(&urom_lib->super, "failed to connect to urom worker");
            return UCC_ERR_NO_MESSAGE;
        }

        worker_params.serviceh        = urom_lib->urom_ctx.urom_service;
        worker_params.addr            = urom_lib->urom_ctx.urom_worker_addr;
        worker_params.addr_len        = urom_lib->urom_ctx.urom_worker_len;
        worker_params.num_cmd_notifyq = 1;

        urom_status = urom_worker_connect(&worker_params, &urom_lib->urom_ctx.urom_worker);
        if (UROM_OK != urom_status) {
            cl_error(&urom_lib->super, "failed to perform urom_worker_connect() with error: %s",
                     urom_status_string(urom_status));
            return UCC_ERR_NO_MESSAGE;
        }
        cl_debug(&urom_lib->super, "connected to the urom worker");
    }
    tl_ctx = ucc_derived_of(self->super.tl_ctxs[ucp_index], ucc_tl_ucp_context_t);
    urom_domain_params.flags = UROM_DOMAIN_WORKER_ADDR;
    urom_domain_params.mask = UROM_DOMAIN_PARAM_FIELD_OOB |
                              UROM_DOMAIN_PARAM_FIELD_WORKER |
                              UROM_DOMAIN_PARAM_FIELD_WORKER_ID; 
    urom_domain_params.oob.allgather = (urom_status_t (*)(void *, void *, size_t, void *, void **))params->params.oob.allgather;
    urom_domain_params.oob.req_test = (urom_status_t (*)(void *))params->params.oob.req_test;
    urom_domain_params.oob.req_free = (urom_status_t (*)(void *))params->params.oob.req_free;
    urom_domain_params.oob.coll_info = params->params.oob.coll_info;
    urom_domain_params.oob.n_oob_indexes = params->params.oob.n_oob_eps;
    urom_domain_params.oob.oob_index = params->params.oob.oob_ep;

    urom_domain_params.domain_worker_id = params->params.oob.oob_ep;
    urom_domain_params.workers = &urom_lib->urom_ctx.urom_worker;
    urom_domain_params.num_workers = 1,
    urom_domain_params.domain_size = params->params.oob.n_oob_eps;
    self->req_mc = 1; /* requires a memcpy */

    if (params->context->params.mask & UCC_CONTEXT_PARAM_FIELD_OOB &&
        params->context->params.mask & UCC_CONTEXT_PARAM_FIELD_MEM_PARAMS) {
        
        /* remap the segments for xgvmi if enabled */
        self->xgvmi.xgvmi_buffer = ucc_mem_params.segments[0].address;
        self->xgvmi.xgvmi_size = ucc_mem_params.segments[0].len;
        if (urom_lib->cfg.use_xgvmi) {
            self->req_mc = 0;
        }
    } else {
        self->xgvmi.xgvmi_size = urom_lib->cfg.num_buffers * urom_lib->cfg.xgvmi_buffer_size;
        self->xgvmi.xgvmi_buffer = ucc_calloc(1, self->xgvmi.xgvmi_size, "xgvmi buffer");
        cl_debug(&urom_lib->super.super, "Allocated xgvmi buffer of size %lu\n", self->xgvmi.xgvmi_size);
        if (!self->xgvmi.xgvmi_buffer) {
            return UCC_ERR_NO_MEMORY;
        }
    }
    n_segments = 1; /* FIXME: just for now */

    domain_mem_map = ucc_calloc(n_segments, sizeof(urom_mem_map_t),
                                "urom_domain_mem_map");
    if (!domain_mem_map) {
        cl_error(&urom_lib->super.super, "Failed to allocate urom_mem_map");
        return UCC_ERR_NO_MEMORY;
    }

    // mem_map the segment
    mem_params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    mem_params.address = self->xgvmi.xgvmi_buffer;
    mem_params.length = self->xgvmi.xgvmi_size;

    ucs_status = ucp_mem_map(tl_ctx->worker.ucp_context, &mem_params, &self->xgvmi.xgvmi_memh);
    assert(ucs_status == UCS_OK);

    if (urom_lib->cfg.use_xgvmi) {
        pack_params.field_mask = UCP_MEMH_PACK_PARAM_FIELD_FLAGS;
        pack_params.flags = UCP_MEMH_PACK_FLAG_EXPORT;

        ucs_status = ucp_memh_pack(self->xgvmi.xgvmi_memh, &pack_params, &self->xgvmi.packed_xgvmi_memh, &self->xgvmi.packed_xgvmi_len);
        if (ucs_status != UCS_OK) {
            cl_error(&urom_lib->super.super, "ucp_memh_pack() returned error: %s", ucs_status_string(ucs_status));
            cl_error(&urom_lib->super.super, "xgvmi will be disabled");
            xgvmi_level = 0;
        } else {
            xgvmi_level = 1;
        }
    }

    ucs_status = ucp_rkey_pack(tl_ctx->worker.ucp_context, self->xgvmi.xgvmi_memh, &self->xgvmi.packed_mkey,
                               &self->xgvmi.packed_mkey_len);
    if (UCS_OK != ucs_status) {
        printf("ucp_rkey_pack() returned error: %s\n",
               ucs_status_string(ucs_status));
        return UCC_ERR_NO_RESOURCE;
    }
    domain_mem_map[0].mask = UROM_WORKER_MEM_MAP_FIELD_BASE_VA | UROM_WORKER_MEM_MAP_FIELD_MKEY;
    domain_mem_map[0].base_va = (uint64_t)self->xgvmi.xgvmi_buffer;
    domain_mem_map[0].len = self->xgvmi.xgvmi_size;
    domain_mem_map[0].mkey = self->xgvmi.packed_mkey;
    domain_mem_map[0].mkey_len = self->xgvmi.packed_mkey_len;
    printf("xgvmi_level: %d\n", xgvmi_level);
    if (xgvmi_level) {
        domain_mem_map[0].mask |= UROM_WORKER_MEM_MAP_FIELD_MEMH;
        domain_mem_map[0].memh = self->xgvmi.packed_xgvmi_memh;
        domain_mem_map[0].memh_len = self->xgvmi.packed_xgvmi_len;
        printf("added in a xgvmi segment %lx with len %lu\n", domain_mem_map[0].base_va, domain_mem_map[0].memh_len);
    }
    urom_domain_params.mask |= UROM_DOMAIN_PARAM_FIELD_MEM_MAP;
    urom_domain_params.mem_map.segments = domain_mem_map;
    urom_domain_params.mem_map.n_segments = 1;
    self->xgvmi_enabled = xgvmi_level; //FIXME: for now, just use xgvmi buffers

    urom_status = urom_domain_create_post(&urom_domain_params, &self->urom_domain);
    if (urom_status < UROM_OK) {
        cl_error(&urom_lib->super.super, "failed to post urom domain: %s", urom_status_string(urom_status));
        return UCC_ERR_NO_MESSAGE;
    }

    while (UROM_INPROGRESS == (urom_status = urom_domain_create_test(self->urom_domain)));
    if (urom_status < UROM_OK) {
        cl_error(&urom_lib->super.super, "failed to create urom domain: %s", urom_status_string(urom_status));
        return UCC_ERR_NO_MESSAGE;
    }

    urom_worker_push_cmdq(urom_lib->urom_ctx.urom_worker, 0, &init_cmd);
    
    while (UROM_ERR_QUEUE_EMPTY ==
           (urom_status = urom_worker_pop_notifyq(urom_lib->urom_ctx.urom_worker, 0, &notif_lib))) {
        sched_yield();
    }
    if ((ucc_status_t) notif_lib->ucc.status != UCC_OK) {
        cl_debug(&urom_lib->super.super, "debug: lib create notif->status: %d\n", notif_lib->ucc.status);
        return (ucc_status_t) notif_lib->ucc.status;
    } else {
        cl_debug(&urom_lib->super.super, "debug: lib created\n");
    }

    ctx_cmd.ucc.context_create_cmd.base_va = self->xgvmi.xgvmi_buffer;
    ctx_cmd.ucc.context_create_cmd.len = self->xgvmi.xgvmi_size;
    urom_worker_push_cmdq(urom_lib->urom_ctx.urom_worker, 0, &ctx_cmd);
    while (UROM_ERR_QUEUE_EMPTY ==
           (urom_status = urom_worker_pop_notifyq(urom_lib->urom_ctx.urom_worker, 0, &notif_ctx)) 
            ) {
        sched_yield();
    }
    if ((ucc_status_t) notif_ctx->ucc.status != UCC_OK) {
        cl_error(&urom_lib->super.super, "failed to create ucc ctx");
        return (ucc_status_t) notif_ctx->ucc.status;
    }

    self->urom_ucc_ctx_h = notif_ctx->ucc.context_create_nqe.context;

    status = ucc_mpool_init(&self->sched_mp, 0, sizeof(ucc_cl_urom_schedule_t),
                            0, UCC_CACHE_LINE_SIZE, 2, UINT_MAX,
                            &ucc_coll_task_mpool_ops, params->thread_mode,
                            "cl_urom_sched_mp");
    if (UCC_OK != status) {
        cl_error(cl_config->cl_lib, "failed to initialize cl_urom_sched mpool");
        return UCC_ERR_NO_MESSAGE;
    }

    cl_debug(cl_config->cl_lib, "initialized cl context: %p", self);
    return UCC_OK;
}

UCC_CLASS_CLEANUP_FUNC(ucc_cl_urom_context_t)
{
    ucc_cl_urom_lib_t *urom_lib = ucc_derived_of(self->super.super.lib, ucc_cl_urom_lib_t);
    urom_worker_cmd_t ctx_destroy_cmd = {
        .cmd_type = UROM_WORKER_CMD_UCC,
        .ucc.cmd_type = UROM_WORKER_CMD_UCC_CONTEXT_DESTROY,
        .ucc.context_destroy_cmd = 
        {
            .context_h = self->urom_ucc_ctx_h,
        },
    };
    urom_worker_notify_t *notif;
    urom_status_t urom_status;
    int i;

    cl_debug(self->super.super.lib, "finalizing cl context: %p", self);
    for (i = 0; i < self->super.n_tl_ctxs; i++) {
        ucc_tl_context_put(self->super.tl_ctxs[i]);
    }
    ucc_free(self->super.tl_ctxs);
    
    urom_worker_push_cmdq(urom_lib->urom_ctx.urom_worker, 0, &ctx_destroy_cmd);
    while (UROM_ERR_QUEUE_EMPTY ==
           (urom_status = urom_worker_pop_notifyq(urom_lib->urom_ctx.urom_worker, 0, &notif))) {
        sched_yield();
    }
    if (self->req_mc) {
        ucc_free(self->xgvmi.xgvmi_buffer);
    }
}

UCC_CLASS_DEFINE(ucc_cl_urom_context_t, ucc_cl_context_t);

ucc_status_t
ucc_cl_urom_get_context_attr(const ucc_base_context_t *context,
                              ucc_base_ctx_attr_t      *attr)
{
    if (attr->attr.mask & UCC_CONTEXT_ATTR_FIELD_CTX_ADDR_LEN) {
        attr->attr.ctx_addr_len = 0;
    }

    return UCC_OK;
}
