/**
 * Copyright (c) 2020-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "components/mc/base/ucc_mc_base.h"
#include "ucc_mc.h"
#include "ucc_mc_plugin.h"
#include "utils/ucc_malloc.h"
#include "utils/ucc_log.h"

#ifdef HAVE_PROFILING_MC
#include "utils/profile/ucc_profile.h"
#else
#include "utils/profile/ucc_profile_off.h"
#endif
#define UCC_MC_PROFILE_FUNC UCC_PROFILE_FUNC

static const ucc_mc_ops_t *mc_ops[UCC_MEMORY_TYPE_LAST];

/* Helper function to get mc_ops including plugins */
static inline const ucc_mc_ops_t *ucc_mc_get_ops(ucc_memory_type_t mem_type)
{
    if (mem_type < UCC_MEMORY_TYPE_LAST) {
        /* Built-in component - direct array lookup */
        return mc_ops[mem_type];
    } else {
        /* Plugin component - lookup in registry */
        ucc_mc_plugin_entry_t *plugin = ucc_mc_plugin_get_entry(mem_type);
        return plugin ? ucc_mc_plugin_get_ops(plugin) : NULL;
    }
}

#define UCC_CHECK_MC_AVAILABLE(mc)                                             \
    do {                                                                       \
        const ucc_mc_ops_t *ops = ucc_mc_get_ops(mc);                          \
        if (ucc_unlikely(NULL == ops)) {                                       \
            if (mc < UCC_MEMORY_TYPE_LAST) {                                   \
                ucc_error(                                                     \
                    "memory type %s not initialized",                          \
                    ucc_memory_type_names[mc]);                                \
            } else {                                                           \
                ucc_error("MC plugin with memory type %d not registered", mc); \
            }                                                                  \
            return UCC_ERR_NOT_SUPPORTED;                                      \
        }                                                                      \
    } while (0)

ucc_status_t ucc_mc_init(const ucc_mc_params_t *mc_params)
{
    int               i, n_mcs;
    ucc_mc_base_t    *mc;
    ucc_status_t      status;
    ucc_mc_attr_t     attr;
    int               is_plugin;
    ucc_memory_type_t assigned_type;

    memset(mc_ops, 0, UCC_MEMORY_TYPE_LAST * sizeof(ucc_mc_ops_t *));
    n_mcs = ucc_global_config.mc_framework.n_components;
    for (i = 0; i < n_mcs; i++) {
        mc = ucc_derived_of(
            ucc_global_config.mc_framework.components[i], ucc_mc_base_t);

        /* Detect if this is a plugin component (loaded from plugin path) */
        is_plugin = (mc->type == UCC_MEMORY_TYPE_LAST);

        if (mc->ref_cnt == 0) {
            mc->config = ucc_malloc(mc->config_table.size);
            if (!mc->config) {
                ucc_error(
                    "failed to allocate %zd bytes for mc config",
                    mc->config_table.size);
                continue;
            }
            status = ucc_config_parser_fill_opts(
                mc->config, &mc->config_table, "UCC_", 1);
            if (UCC_OK != status) {
                ucc_debug(
                    "failed to parse config for mc: %s (%d)",
                    mc->super.name,
                    status);
                ucc_free(mc->config);
                continue;
            }
            status = mc->init(mc_params);
            if (UCC_OK != status) {
                ucc_debug(
                    "mc_init failed for component: %s, skipping (%d)",
                    mc->super.name,
                    status);
                ucc_config_parser_release_opts(
                    mc->config, mc->config_table.table);
                ucc_free(mc->config);
                continue;
            }
            ucc_debug("mc %s initialized", mc->super.name);
        } else {
            attr.field_mask = UCC_MC_ATTR_FIELD_THREAD_MODE;
            status          = mc->get_attr(&attr);
            if (status != UCC_OK) {
                return status;
            }
            if (attr.thread_mode < mc_params->thread_mode) {
                ucc_info(
                    "mc %s was allready initilized with "
                    "different thread mode: current tm %d, provided tm %d",
                    mc->super.name,
                    attr.thread_mode,
                    mc_params->thread_mode);
            }
        }
        mc->ref_cnt++;

        /* Register component in appropriate table */
        if (is_plugin) {
            /* Register as plugin - assigns unique memory type >= UCC_MEMORY_TYPE_LAST */
            status = ucc_mc_plugin_register_autodiscovered(mc, &assigned_type);
            if (status != UCC_OK) {
                ucc_error(
                    "failed to register autodiscovered plugin %s",
                    mc->super.name);
                continue;
            }
            mc->type = assigned_type;
            ucc_info(
                "MC plugin '%s' registered with memory_type=%d",
                mc->super.name,
                assigned_type);
        } else {
            /* Built-in component - register in fixed ops table */
            if (mc->type >= UCC_MEMORY_TYPE_LAST) {
                ucc_error(
                    "invalid memory type %d for built-in component %s",
                    mc->type,
                    mc->super.name);
                continue;
            }
            mc_ops[mc->type] = &mc->ops;
        }
    }

    return UCC_OK;
}

ucc_status_t ucc_mc_available(ucc_memory_type_t mem_type)
{
    const ucc_mc_ops_t *ops;

    mem_type = (mem_type == UCC_MEMORY_TYPE_CUDA_MANAGED) ?
                   UCC_MEMORY_TYPE_CUDA : mem_type;

    ops = ucc_mc_get_ops(mem_type);
    if (NULL == ops) {
        return UCC_ERR_NOT_FOUND;
    }

    return UCC_OK;
}

/* Context for memory query iteration */
typedef struct {
    const void      *ptr;
    ucc_mem_attr_t  *mem_attr;
    ucc_status_t     result;
} ucc_mc_mem_query_ctx_t;

static ucc_status_t ucc_mc_mem_query_cb(ucc_mc_plugin_entry_t *plugin, void *ctx)
{
    ucc_mc_mem_query_ctx_t *query_ctx = (ucc_mc_mem_query_ctx_t *)ctx;
    ucc_status_t            status;

    if (plugin->desc.ops.mem_query) {
        status = plugin->desc.ops.mem_query(
            query_ctx->ptr, query_ctx->mem_attr);
        if (UCC_OK == status) {
            query_ctx->result = UCC_OK;
            return UCC_ERR_LAST; /* Stop iteration */
        }
    }
    return UCC_OK; /* Continue iteration */
}

ucc_status_t ucc_mc_get_mem_attr(const void *ptr, ucc_mem_attr_t *mem_attr)
{
    ucc_status_t      status;
    ucc_memory_type_t mt;
    const ucc_mc_ops_t *ops;
    ucc_mc_mem_query_ctx_t query_ctx;

    mem_attr->mem_type     = UCC_MEMORY_TYPE_HOST;
    mem_attr->base_address = (void *)ptr;
    if (ptr == NULL) {
        mem_attr->alloc_length = 0;
        return UCC_OK;
    }

    /* Check builtin memory types */
    mt = (ucc_memory_type_t)(UCC_MEMORY_TYPE_HOST + 1);
    for (; mt < UCC_MEMORY_TYPE_LAST; mt++) {
        ops = ucc_mc_get_ops(mt);
        if (NULL != ops) {
            status = ops->mem_query(ptr, mem_attr);
            if (UCC_OK == status) {
                return UCC_OK;
            }
        }
    }

    /* Check plugin memory types */
    query_ctx.ptr      = ptr;
    query_ctx.mem_attr = mem_attr;
    query_ctx.result   = UCC_ERR_NOT_FOUND;
    ucc_mc_plugin_iterate(ucc_mc_mem_query_cb, &query_ctx);
    if (query_ctx.result == UCC_OK) {
        return UCC_OK;
    }

    return UCC_OK;
}

/* Helper: Find EC plugin type for a given MC plugin memory type */
static ucc_status_t ucc_mc_get_plugin_ee_type(
    ucc_memory_type_t mem_type, ucc_ee_type_t *ee_type)
{
    ucc_mc_plugin_entry_t *mc_plugin;
    const char            *mc_name;
    char                   ec_name_buf[256];
    size_t                 mc_name_len;
    int                    i;

    mc_plugin = ucc_mc_plugin_get_entry(mem_type);
    if (!mc_plugin) {
        return UCC_ERR_NOT_FOUND;
    }

    /* Get MC plugin name, e.g., "opencl mc" */
    mc_name = ucc_mc_plugin_get_name(mem_type);
    if (!mc_name) {
        return UCC_ERR_NOT_FOUND;
    }

    /* Try to construct EC plugin name by replacing " mc" with " ec" */
    mc_name_len = strlen(mc_name);
    if (mc_name_len >= 4 && strcmp(mc_name + mc_name_len - 3, " mc") == 0) {
        /* Found " mc" suffix - replace with " ec" */
        strncpy(ec_name_buf, mc_name, mc_name_len - 3);
        ec_name_buf[mc_name_len - 3] = '\0';
        strcat(ec_name_buf, " ec");

        /* Search for EC plugin with this name */
        /* Start from UCC_EE_LAST to search plugins */
        for (i = UCC_EE_LAST; i < UCC_EE_LAST + 100; i++) {
            const char *test_name = ucc_ec_plugin_get_name((ucc_ee_type_t)i);
            if (test_name && strcmp(test_name, ec_name_buf) == 0) {
                *ee_type = (ucc_ee_type_t)i;
                return UCC_OK;
            }
        }
    }

    /* No matching EC plugin found - fall back to CPU executor */
    *ee_type = UCC_EE_CPU_THREAD;
    return UCC_OK;
}

ucc_status_t ucc_mc_get_execution_engine_type(
    ucc_memory_type_t mem_type, ucc_ee_type_t *ee_type)
{
    switch (mem_type) {
    case UCC_MEMORY_TYPE_CUDA:
    case UCC_MEMORY_TYPE_CUDA_MANAGED:
        *ee_type = UCC_EE_CUDA_STREAM;
        return UCC_OK;
    case UCC_MEMORY_TYPE_ROCM:
    case UCC_MEMORY_TYPE_ROCM_MANAGED:
        *ee_type = UCC_EE_ROCM_STREAM;
        return UCC_OK;
    case UCC_MEMORY_TYPE_HOST:
        *ee_type = UCC_EE_CPU_THREAD;
        return UCC_OK;
    default:
        /* Plugin memory type - look for matching EC plugin */
        return ucc_mc_get_plugin_ee_type(mem_type, ee_type);
    }
}

ucc_status_t ucc_mc_get_attr(ucc_mc_attr_t *attr, ucc_memory_type_t mem_type)
{
    ucc_memory_type_t      mt = (mem_type == UCC_MEMORY_TYPE_CUDA_MANAGED)
                                    ? UCC_MEMORY_TYPE_CUDA
                                    : mem_type;
    const ucc_mc_ops_t    *ops;
    ucc_mc_base_t         *mc;
    ucc_mc_plugin_entry_t *plugin;

    UCC_CHECK_MC_AVAILABLE(mt);

    if (mt < UCC_MEMORY_TYPE_LAST) {
        mc = ucc_container_of(mc_ops[mt], ucc_mc_base_t, ops);
        return mc->get_attr(attr);
    } else {
        /* Plugin */
        plugin = ucc_mc_plugin_get_entry(mt);
        if (plugin && plugin->desc.get_attr) {
            return plugin->desc.get_attr(attr);
        }
        return UCC_ERR_NOT_SUPPORTED;
    }
}

/* TODO: add the flexbility to bypass the mpool if the user asks for it */
UCC_MC_PROFILE_FUNC(ucc_status_t, ucc_mc_alloc, (h_ptr, size, mem_type),
                    ucc_mc_buffer_header_t **h_ptr, size_t size,
                    ucc_memory_type_t mem_type)
{
    ucc_memory_type_t mt = (mem_type == UCC_MEMORY_TYPE_CUDA_MANAGED) ?
                               UCC_MEMORY_TYPE_CUDA : mem_type;
    const ucc_mc_ops_t *ops;

    UCC_CHECK_MC_AVAILABLE(mt);
    ops = ucc_mc_get_ops(mt);
    return ops->mem_alloc(h_ptr, size, mem_type);
}

ucc_status_t ucc_mc_free(ucc_mc_buffer_header_t *h_ptr)
{
    ucc_memory_type_t mt = (h_ptr->mt == UCC_MEMORY_TYPE_CUDA_MANAGED) ?
                               UCC_MEMORY_TYPE_CUDA : h_ptr->mt;
    const ucc_mc_ops_t *ops;

    UCC_CHECK_MC_AVAILABLE(mt);
    ops = ucc_mc_get_ops(mt);
    return ops->mem_free(h_ptr);
}

UCC_MC_PROFILE_FUNC(ucc_status_t, ucc_mc_memcpy,
                    (dst, src, len, dst_mem, src_mem), void *dst,
                    const void *src, size_t len, ucc_memory_type_t dst_mem,
                    ucc_memory_type_t src_mem)

{
    ucc_memory_type_t   mt;
    const ucc_mc_ops_t *ops;
    
    if (src_mem == UCC_MEMORY_TYPE_UNKNOWN ||
        dst_mem == UCC_MEMORY_TYPE_UNKNOWN) {
        return UCC_ERR_INVALID_PARAM;
    } else if (src_mem == UCC_MEMORY_TYPE_HOST &&
               dst_mem == UCC_MEMORY_TYPE_HOST) {
        UCC_CHECK_MC_AVAILABLE(UCC_MEMORY_TYPE_HOST);
        ops = ucc_mc_get_ops(UCC_MEMORY_TYPE_HOST);
        return ops->memcpy(dst, src, len,
                          UCC_MEMORY_TYPE_HOST,
                          UCC_MEMORY_TYPE_HOST);
    }
    /* take any non host MC component */
    mt = (dst_mem == UCC_MEMORY_TYPE_HOST) ? src_mem : dst_mem;
    mt = (mt == UCC_MEMORY_TYPE_CUDA_MANAGED) ? UCC_MEMORY_TYPE_CUDA : mt;
    UCC_CHECK_MC_AVAILABLE(mt);
    ops = ucc_mc_get_ops(mt);
    return ops->memcpy(dst, src, len, dst_mem, src_mem);
}

ucc_status_t ucc_mc_memset(void *ptr, int value, size_t size,
                           ucc_memory_type_t mem_type)
{
    const ucc_mc_ops_t *ops;
    
    mem_type = (mem_type == UCC_MEMORY_TYPE_CUDA_MANAGED) ?
                   UCC_MEMORY_TYPE_CUDA : mem_type;

    UCC_CHECK_MC_AVAILABLE(mem_type);
    ops = ucc_mc_get_ops(mem_type);
    return ops->memset(ptr, value, size);
}

ucc_status_t ucc_mc_flush(ucc_memory_type_t mem_type)
{
    const ucc_mc_ops_t *ops;
    
    mem_type = (mem_type == UCC_MEMORY_TYPE_CUDA_MANAGED) ?
                   UCC_MEMORY_TYPE_CUDA : mem_type;

    UCC_CHECK_MC_AVAILABLE(mem_type);
    ops = ucc_mc_get_ops(mem_type);
    if (ops->flush) {
        return ops->flush();
    }
    return UCC_OK;
}

ucc_status_t ucc_mc_finalize()
{
   ucc_memory_type_t  mt;
   ucc_mc_base_t     *mc;

    for (mt = UCC_MEMORY_TYPE_HOST; mt < UCC_MEMORY_TYPE_LAST; mt++) {
        if (NULL != mc_ops[mt]) {
            mc = ucc_container_of(mc_ops[mt], ucc_mc_base_t, ops);
            mc->ref_cnt--;
            if (mc->ref_cnt == 0) {
                mc->finalize();
                ucc_config_parser_release_opts(mc->config,
                                               mc->config_table.table);
                ucc_free(mc->config);
                mc_ops[mt] = NULL;
            }
        }
    }

    return UCC_OK;
}
