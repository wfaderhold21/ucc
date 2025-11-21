/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

/**
 * OpenCL UCC Memory Component Plugin
 *
 * This plugin enables UCC to work with OpenCL devices (GPUs, FPGAs, accelerators).
 * It provides a real, working example of a UCC MC plugin using OpenCL APIs.
 *
 * Supports: Intel GPUs, AMD GPUs (non-ROCm), ARM Mali, FPGAs, and other OpenCL devices
 */

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#include "components/mc/base/ucc_mc_base.h"
#include "components/mc/ucc_mc_log.h"
#include "utils/ucc_malloc.h"
#include <string.h>

/* Plugin structure */
typedef struct ucc_mc_opencl {
    ucc_mc_base_t     super;
    ucc_thread_mode_t thread_mode;
    cl_context        context;
    cl_device_id      device;
    cl_command_queue  queue;
    int               initialized;
} ucc_mc_opencl_t;

/* Global instance */
extern ucc_mc_opencl_t ucc_mc_opencl;

/* Configuration structure */
typedef struct ucc_mc_opencl_config {
    ucc_mc_config_t super;
    int             device_id;
    int             use_host_ptr;
} ucc_mc_opencl_config_t;

static ucc_config_field_t ucc_mc_opencl_config_table[] = {
    {"DEVICE_ID",
     "0",
     "OpenCL device ID to use (default: 0)",
     ucc_offsetof(ucc_mc_opencl_config_t, device_id),
     UCC_CONFIG_TYPE_UINT},

    {"USE_HOST_PTR",
     "no",
     "Use CL_MEM_ALLOC_HOST_PTR for allocations (useful for debugging)",
     ucc_offsetof(ucc_mc_opencl_config_t, use_host_ptr),
     UCC_CONFIG_TYPE_BOOL},

    {NULL}};

#define MC_OPENCL_CONFIG                                                       \
    (ucc_derived_of(ucc_mc_opencl.super.config, ucc_mc_opencl_config_t))

/* Initialize OpenCL plugin */
static ucc_status_t ucc_mc_opencl_init(const ucc_mc_params_t *mc_params)
{
    ucc_mc_opencl_config_t *cfg;
    cl_int                  err;
    cl_uint                 num_platforms, num_devices;
    cl_platform_id          platform;
    cl_device_id           *devices     = NULL;
    cl_device_type          device_type = CL_DEVICE_TYPE_GPU;
    char                    device_name[128];

    cfg = ucc_derived_of(ucc_mc_opencl.super.config, ucc_mc_opencl_config_t);
    ucc_strncpy_safe(
        cfg->super.log_component.name,
        ucc_mc_opencl.super.super.name,
        sizeof(cfg->super.log_component.name));

    ucc_mc_opencl.thread_mode = mc_params->thread_mode;
    ucc_mc_opencl.initialized = 0;

    /* Get OpenCL platform */
    err                       = clGetPlatformIDs(1, &platform, &num_platforms);
    if (err != CL_SUCCESS || num_platforms == 0) {
        mc_debug(&ucc_mc_opencl.super, "no OpenCL platforms found: %d", err);
        return UCC_ERR_NO_RESOURCE;
    }

    /* Get OpenCL device */
    err = clGetDeviceIDs(platform, device_type, 0, NULL, &num_devices);
    if (err != CL_SUCCESS || num_devices == 0) {
        /* Try any device type */
        device_type = CL_DEVICE_TYPE_ALL;
        err = clGetDeviceIDs(platform, device_type, 0, NULL, &num_devices);
        if (err != CL_SUCCESS || num_devices == 0) {
            mc_debug(&ucc_mc_opencl.super, "no OpenCL devices found: %d", err);
            return UCC_ERR_NO_RESOURCE;
        }
    }

    if (cfg->device_id >= (int)num_devices) {
        mc_error(
            &ucc_mc_opencl.super,
            "requested device_id %d but only %d devices available",
            cfg->device_id,
            num_devices);
        return UCC_ERR_INVALID_PARAM;
    }

    devices = ucc_malloc(num_devices * sizeof(cl_device_id), "opencl_devices");
    if (!devices) {
        return UCC_ERR_NO_MEMORY;
    }

    err = clGetDeviceIDs(platform, device_type, num_devices, devices, NULL);
    if (err != CL_SUCCESS) {
        mc_error(&ucc_mc_opencl.super, "failed to get device IDs: %d", err);
        ucc_free(devices);
        return UCC_ERR_NO_RESOURCE;
    }

    ucc_mc_opencl.device = devices[cfg->device_id];
    ucc_free(devices);

    /* Get device name */
    err = clGetDeviceInfo(
        ucc_mc_opencl.device,
        CL_DEVICE_NAME,
        sizeof(device_name),
        device_name,
        NULL);
    if (err != CL_SUCCESS) {
        strcpy(device_name, "Unknown");
    }

    /* Create OpenCL context */
    ucc_mc_opencl.context = clCreateContext(
        NULL, 1, &ucc_mc_opencl.device, NULL, NULL, &err);
    if (err != CL_SUCCESS) {
        mc_error(
            &ucc_mc_opencl.super, "failed to create OpenCL context: %d", err);
        return UCC_ERR_NO_RESOURCE;
    }

    /* Create command queue */
    ucc_mc_opencl.queue = clCreateCommandQueue(
        ucc_mc_opencl.context, ucc_mc_opencl.device, 0, &err);
    if (err != CL_SUCCESS) {
        mc_error(
            &ucc_mc_opencl.super, "failed to create command queue: %d", err);
        clReleaseContext(ucc_mc_opencl.context);
        return UCC_ERR_NO_RESOURCE;
    }

    ucc_mc_opencl.initialized = 1;

    mc_info(
        &ucc_mc_opencl.super,
        "OpenCL plugin initialized: device=%d (%s), %d devices available",
        cfg->device_id,
        device_name,
        num_devices);

    return UCC_OK;
}

/* Get plugin attributes */
static ucc_status_t ucc_mc_opencl_get_attr(ucc_mc_attr_t *mc_attr)
{
    if (mc_attr->field_mask & UCC_MC_ATTR_FIELD_THREAD_MODE) {
        mc_attr->thread_mode = ucc_mc_opencl.thread_mode;
    }
    if (mc_attr->field_mask & UCC_MC_ATTR_FIELD_FAST_ALLOC_SIZE) {
        mc_attr->fast_alloc_size = 0;
    }
    return UCC_OK;
}

/* Allocate OpenCL device memory */
static ucc_status_t ucc_mc_opencl_mem_alloc(
    ucc_mc_buffer_header_t **h_ptr, size_t size, ucc_memory_type_t mt)
{
    ucc_mc_buffer_header_t *h;
    cl_int                  err;
    cl_mem_flags            flags;
    ucc_mc_opencl_config_t *cfg;

    if (!ucc_mc_opencl.initialized) {
        mc_error(&ucc_mc_opencl.super, "OpenCL not initialized");
        return UCC_ERR_NOT_SUPPORTED;
    }

    h = (ucc_mc_buffer_header_t *)ucc_malloc(
        sizeof(ucc_mc_buffer_header_t), "mc opencl");
    if (!h) {
        mc_error(&ucc_mc_opencl.super, "failed to allocate header");
        return UCC_ERR_NO_MEMORY;
    }

    cfg   = MC_OPENCL_CONFIG;
    flags = CL_MEM_READ_WRITE;
    if (cfg->use_host_ptr) {
        flags |= CL_MEM_ALLOC_HOST_PTR;
    }

    /* Allocate OpenCL buffer */
    h->addr = clCreateBuffer(ucc_mc_opencl.context, flags, size, NULL, &err);
    if (err != CL_SUCCESS) {
        mc_error(
            &ucc_mc_opencl.super,
            "clCreateBuffer failed for %zu bytes: %d",
            size,
            err);
        ucc_free(h);
        return UCC_ERR_NO_MEMORY;
    }

    h->from_pool = 0;
    h->mt        = mt;
    *h_ptr       = h;

    mc_trace(
        &ucc_mc_opencl.super, "allocated %zu bytes (cl_mem=%p)", size, h->addr);

    return UCC_OK;
}

/* Free OpenCL memory */
static ucc_status_t ucc_mc_opencl_mem_free(ucc_mc_buffer_header_t *h_ptr)
{
    cl_int err;

    if (!ucc_mc_opencl.initialized) {
        return UCC_ERR_NOT_SUPPORTED;
    }

    err = clReleaseMemObject((cl_mem)h_ptr->addr);
    if (err != CL_SUCCESS) {
        mc_error(&ucc_mc_opencl.super, "clReleaseMemObject failed: %d", err);
        /* Continue anyway to free header */
    }

    mc_trace(&ucc_mc_opencl.super, "freed cl_mem=%p", h_ptr->addr);
    ucc_free(h_ptr);

    return UCC_OK;
}

/* Copy memory (device-to-device, host-to-device, device-to-host) */
static ucc_status_t ucc_mc_opencl_memcpy(
    void *dst, const void *src, size_t len, ucc_memory_type_t dst_mem,
    ucc_memory_type_t src_mem)
{
    cl_int err;

    if (!ucc_mc_opencl.initialized) {
        return UCC_ERR_NOT_SUPPORTED;
    }

    /* Determine copy direction */
    if (dst_mem == UCC_MEMORY_TYPE_HOST && src_mem != UCC_MEMORY_TYPE_HOST) {
        /* Device to Host */
        err = clEnqueueReadBuffer(
            ucc_mc_opencl.queue,
            (cl_mem)src,
            CL_TRUE,
            0,
            len,
            dst,
            0,
            NULL,
            NULL);
        if (err != CL_SUCCESS) {
            mc_error(
                &ucc_mc_opencl.super, "clEnqueueReadBuffer failed: %d", err);
            return UCC_ERR_NO_MESSAGE;
        }
    } else if (
        dst_mem != UCC_MEMORY_TYPE_HOST && src_mem == UCC_MEMORY_TYPE_HOST) {
        /* Host to Device */
        err = clEnqueueWriteBuffer(
            ucc_mc_opencl.queue,
            (cl_mem)dst,
            CL_TRUE,
            0,
            len,
            src,
            0,
            NULL,
            NULL);
        if (err != CL_SUCCESS) {
            mc_error(
                &ucc_mc_opencl.super, "clEnqueueWriteBuffer failed: %d", err);
            return UCC_ERR_NO_MESSAGE;
        }
    } else if (
        dst_mem != UCC_MEMORY_TYPE_HOST && src_mem != UCC_MEMORY_TYPE_HOST) {
        /* Device to Device */
        err = clEnqueueCopyBuffer(
            ucc_mc_opencl.queue,
            (cl_mem)src,
            (cl_mem)dst,
            0,
            0,
            len,
            0,
            NULL,
            NULL);
        if (err != CL_SUCCESS) {
            mc_error(
                &ucc_mc_opencl.super, "clEnqueueCopyBuffer failed: %d", err);
            return UCC_ERR_NO_MESSAGE;
        }
    } else {
        /* Host to Host - shouldn't happen */
        memcpy(dst, src, len);
    }

    mc_trace(
        &ucc_mc_opencl.super,
        "memcpy %zu bytes (dst_mem=%d, src_mem=%d)",
        len,
        dst_mem,
        src_mem);

    return UCC_OK;
}

/* Set memory */
static ucc_status_t ucc_mc_opencl_memset(void *dst, int value, size_t len)
{
    cl_int   err;
    cl_uchar pattern = (cl_uchar)value;

    if (!ucc_mc_opencl.initialized) {
        return UCC_ERR_NOT_SUPPORTED;
    }

    err = clEnqueueFillBuffer(
        ucc_mc_opencl.queue,
        (cl_mem)dst,
        &pattern,
        sizeof(pattern),
        0,
        len,
        0,
        NULL,
        NULL);
    if (err != CL_SUCCESS) {
        mc_error(&ucc_mc_opencl.super, "clEnqueueFillBuffer failed: %d", err);
        return UCC_ERR_NO_MESSAGE;
    }

    return UCC_OK;
}

/* Query memory attributes */
static ucc_status_t ucc_mc_opencl_mem_query(
    const void *ptr, ucc_mem_attr_t *mem_attr)
{
    cl_mem             cl_buf;
    cl_mem_object_type obj_type;
    cl_context         buf_ctx;
    size_t             size;
    cl_int             err;

    if (!ucc_mc_opencl.initialized) {
        return UCC_ERR_NOT_SUPPORTED;
    }

    cl_buf = (cl_mem)ptr;

    /* Try to query the object - if it's a valid cl_mem, this will succeed.
     * This works for ANY cl_mem, whether allocated by UCC or directly by user. */
    err    = clGetMemObjectInfo(
        cl_buf, CL_MEM_TYPE, sizeof(obj_type), &obj_type, NULL);
    if (err != CL_SUCCESS) {
        /* Not a valid cl_mem or not accessible */
        return UCC_ERR_NOT_SUPPORTED;
    }

    /* Verify it belongs to our context (optional but safer) */
    err = clGetMemObjectInfo(
        cl_buf, CL_MEM_CONTEXT, sizeof(buf_ctx), &buf_ctx, NULL);
    if (err == CL_SUCCESS && buf_ctx != ucc_mc_opencl.context) {
        /* Valid cl_mem, but not from our context */
        return UCC_ERR_NOT_SUPPORTED;
    }

    /* It's a valid OpenCL memory object from our context! */
    if (mem_attr->field_mask & UCC_MEM_ATTR_FIELD_MEM_TYPE) {
        mem_attr->mem_type = ucc_mc_opencl.super.type;
    }

    if (mem_attr->field_mask & UCC_MEM_ATTR_FIELD_BASE_ADDRESS) {
        mem_attr->base_address = (void *)ptr;
    }

    if (mem_attr->field_mask & UCC_MEM_ATTR_FIELD_ALLOC_LENGTH) {
        err = clGetMemObjectInfo(
            cl_buf, CL_MEM_SIZE, sizeof(size), &size, NULL);
        if (err == CL_SUCCESS) {
            mem_attr->alloc_length = size;
        } else {
            mem_attr->alloc_length = 0;
        }
    }

    mc_trace(
        &ucc_mc_opencl.super, "mem_query: cl_mem=%p is OpenCL buffer", ptr);

    return UCC_OK;
}

/* Finalize OpenCL plugin */
static ucc_status_t ucc_mc_opencl_finalize(void)
{
    if (ucc_mc_opencl.initialized) {
        clReleaseCommandQueue(ucc_mc_opencl.queue);
        clReleaseContext(ucc_mc_opencl.context);
        ucc_mc_opencl.initialized = 0;
        mc_info(&ucc_mc_opencl.super, "OpenCL plugin finalized");
    }
    return UCC_OK;
}

/**
 * Export the component interface
 * Symbol name: ucc_mc_opencl (matches library: libucc_mc_opencl.so)
 */
ucc_mc_opencl_t ucc_mc_opencl = {
    .super.super.name    = "opencl mc",
    .super.ref_cnt       = 0,
    .super.ee_type       = UCC_EE_LAST,
    .super.type          = UCC_MEMORY_TYPE_LAST, /* Auto-assigned */
    .super.init          = ucc_mc_opencl_init,
    .super.get_attr      = ucc_mc_opencl_get_attr,
    .super.finalize      = ucc_mc_opencl_finalize,
    .super.ops.mem_query = ucc_mc_opencl_mem_query,
    .super.ops.mem_alloc = ucc_mc_opencl_mem_alloc,
    .super.ops.mem_free  = ucc_mc_opencl_mem_free,
    .super.ops.memcpy    = ucc_mc_opencl_memcpy,
    .super.ops.memset    = ucc_mc_opencl_memset,
    .super.ops.flush     = NULL,
    .super.config_table =
        {
            .name   = "OpenCL memory component",
            .prefix = "MC_OPENCL_",
            .table  = ucc_mc_opencl_config_table,
            .size   = sizeof(ucc_mc_opencl_config_t),
        },
    .initialized = 0,
};

/* Register config table */
UCC_CONFIG_REGISTER_TABLE_ENTRY(
    &ucc_mc_opencl.super.config_table, &ucc_config_global_list);

