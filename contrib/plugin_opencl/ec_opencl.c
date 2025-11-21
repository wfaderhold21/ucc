/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

/**
 * OpenCL UCC Execution Component Plugin
 *
 * This plugin enables UCC to use OpenCL events for synchronization and
 * triggered collective operations.
 *
 * Works with any OpenCL 1.2+ device (Intel GPUs, AMD GPUs, FPGAs, ARM Mali, etc.)
 */

#ifdef __APPLE__
#include <OpenCL/opencl.h>
#else
#include <CL/cl.h>
#endif

#include "components/ec/base/ucc_ec_base.h"
#include "components/ec/ucc_ec_log.h"
#include "utils/ucc_malloc.h"
#include <string.h>

/* Forward declarations */
typedef struct ucc_opencl_executor      ucc_opencl_executor_t;
typedef struct ucc_opencl_executor_task ucc_opencl_executor_task_t;

/* Plugin structure */
typedef struct ucc_ec_opencl {
    ucc_ec_base_t     super;
    ucc_thread_mode_t thread_mode;
    cl_context        context;
    cl_device_id      device;
    cl_command_queue  queue;
    cl_program        reduce_program; /* Compiled reduction kernels */
    cl_kernel         reduce_kernel;  /* Generic reduction kernel */
    int               initialized;
    int               kernels_built;
} ucc_ec_opencl_t;

/* Executor structure - one per user request */
typedef struct ucc_opencl_executor {
    ucc_ee_executor_t base;
    cl_command_queue  exec_queue; /* Dedicated queue for this executor */
    uint64_t          task_types; /* Supported task types */
} ucc_opencl_executor_t;

/* Task structure - one per operation */
typedef struct ucc_opencl_executor_task {
    ucc_ee_executor_task_t base;
    cl_event               event; /* For async completion tracking */
    int                    completed;
} ucc_opencl_executor_task_t;

/* Global instance */
extern ucc_ec_opencl_t ucc_ec_opencl;

/* Configuration structure */
typedef struct ucc_ec_opencl_config {
    ucc_ec_config_t super;
    int             device_id;
} ucc_ec_opencl_config_t;

static ucc_config_field_t ucc_ec_opencl_config_table[] = {
    {"",
     "",
     NULL,
     ucc_offsetof(ucc_ec_opencl_config_t, super),
     UCC_CONFIG_TYPE_TABLE(ucc_ec_config_table)},

    {"DEVICE_ID",
     "0",
     "OpenCL device ID to use (default: 0, should match MC_OPENCL_DEVICE_ID)",
     ucc_offsetof(ucc_ec_opencl_config_t, device_id),
     UCC_CONFIG_TYPE_UINT},

    {NULL}};

#define EC_OPENCL_CONFIG                                                       \
    (ucc_derived_of(ucc_ec_opencl.super.config, ucc_ec_opencl_config_t))

/* OpenCL kernel source for basic reductions */
static const char
    *opencl_reduce_kernel_source =
"__kernel void reduce_sum_float(__global float* dst,\n"
"                                __global const float* src1,\n"
"                                __global const float* src2,\n"
"                                const ulong count) {\n"
"    ulong gid = get_global_id(0);\n"
"    if (gid < count) {\n"
"        dst[gid] = src1[gid] + src2[gid];\n"
"    }\n"
"}\n"
"\n"
"__kernel void reduce_sum_double(__global double* dst,\n"
"                                 __global const double* src1,\n"
"                                 __global const double* src2,\n"
"                                 const ulong count) {\n"
"    ulong gid = get_global_id(0);\n"
"    if (gid < count) {\n"
"        dst[gid] = src1[gid] + src2[gid];\n"
"    }\n"
"}\n"
"\n"
"__kernel void memcpy_kernel(__global char* dst,\n"
"                             __global const char* src,\n"
"                             const ulong len) {\n"
"    ulong gid = get_global_id(0);\n"
"    if (gid < len) {\n"
"        dst[gid] = src[gid];\n"
"    }\n"
"}\n";

/* Build OpenCL kernels for executor operations */
static ucc_status_t ucc_ec_opencl_build_kernels(void)
{
    cl_int err;
    size_t kernel_size;

    if (ucc_ec_opencl.kernels_built) {
        return UCC_OK;
    }

    kernel_size                  = strlen(opencl_reduce_kernel_source);
    ucc_ec_opencl.reduce_program = clCreateProgramWithSource(
        ucc_ec_opencl.context,
        1,
        &opencl_reduce_kernel_source,
        &kernel_size,
        &err);

    if (err != CL_SUCCESS) {
        ec_error(
            &ucc_ec_opencl.super,
            "failed to create program from source: %d",
            err);
        return UCC_ERR_NO_RESOURCE;
    }

    err = clBuildProgram(
        ucc_ec_opencl.reduce_program,
        1,
        &ucc_ec_opencl.device,
        NULL,
        NULL,
        NULL);

    if (err != CL_SUCCESS) {
        size_t log_size;
        char  *build_log;

        clGetProgramBuildInfo(
            ucc_ec_opencl.reduce_program,
            ucc_ec_opencl.device,
            CL_PROGRAM_BUILD_LOG,
            0,
            NULL,
            &log_size);
        build_log = ucc_malloc(log_size + 1, "opencl_build_log");
        if (build_log) {
            clGetProgramBuildInfo(
                ucc_ec_opencl.reduce_program,
                ucc_ec_opencl.device,
                CL_PROGRAM_BUILD_LOG,
                log_size,
                build_log,
                NULL);
            build_log[log_size] = '\0';
            ec_error(
                &ucc_ec_opencl.super,
                "kernel build failed: %d\nBuild log:\n%s",
                err,
                build_log);
            ucc_free(build_log);
        }
        clReleaseProgram(ucc_ec_opencl.reduce_program);
        return UCC_ERR_NO_RESOURCE;
    }

    ucc_ec_opencl.kernels_built = 1;
    ec_info(
        &ucc_ec_opencl.super, "OpenCL executor kernels compiled successfully");

    return UCC_OK;
}

/* Initialize OpenCL EC plugin */
static ucc_status_t ucc_ec_opencl_init(const ucc_ec_params_t *ec_params)
{
    ucc_ec_opencl_config_t *cfg;
    cl_int                  err;
    cl_uint                 num_platforms, num_devices;
    cl_platform_id          platform;
    cl_device_id           *devices     = NULL;
    cl_device_type          device_type = CL_DEVICE_TYPE_GPU;
    char                    device_name[128];

    cfg = ucc_derived_of(ucc_ec_opencl.super.config, ucc_ec_opencl_config_t);
    ucc_strncpy_safe(
        cfg->super.log_component.name,
        ucc_ec_opencl.super.super.name,
        sizeof(cfg->super.log_component.name));

    ucc_ec_opencl.thread_mode = ec_params->thread_mode;
    ucc_ec_opencl.initialized = 0;

    /* Get OpenCL platform */
    err                       = clGetPlatformIDs(1, &platform, &num_platforms);
    if (err != CL_SUCCESS || num_platforms == 0) {
        ec_debug(&ucc_ec_opencl.super, "no OpenCL platforms found: %d", err);
        return UCC_ERR_NO_RESOURCE;
    }

    /* Get OpenCL device */
    err = clGetDeviceIDs(platform, device_type, 0, NULL, &num_devices);
    if (err != CL_SUCCESS || num_devices == 0) {
        /* Try any device type */
        device_type = CL_DEVICE_TYPE_ALL;
        err = clGetDeviceIDs(platform, device_type, 0, NULL, &num_devices);
        if (err != CL_SUCCESS || num_devices == 0) {
            ec_debug(&ucc_ec_opencl.super, "no OpenCL devices found: %d", err);
            return UCC_ERR_NO_RESOURCE;
        }
    }

    if (cfg->device_id >= (int)num_devices) {
        ec_error(
            &ucc_ec_opencl.super,
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
        ec_error(&ucc_ec_opencl.super, "failed to get device IDs: %d", err);
        ucc_free(devices);
        return UCC_ERR_NO_RESOURCE;
    }

    ucc_ec_opencl.device = devices[cfg->device_id];
    ucc_free(devices);

    /* Get device name */
    err = clGetDeviceInfo(
        ucc_ec_opencl.device,
        CL_DEVICE_NAME,
        sizeof(device_name),
        device_name,
        NULL);
    if (err != CL_SUCCESS) {
        strcpy(device_name, "Unknown");
    }

    /* Create OpenCL context */
    ucc_ec_opencl.context = clCreateContext(
        NULL, 1, &ucc_ec_opencl.device, NULL, NULL, &err);
    if (err != CL_SUCCESS) {
        ec_error(
            &ucc_ec_opencl.super, "failed to create OpenCL context: %d", err);
        return UCC_ERR_NO_RESOURCE;
    }

    /* Create command queue */
    ucc_ec_opencl.queue = clCreateCommandQueue(
        ucc_ec_opencl.context, ucc_ec_opencl.device, 0, &err);
    if (err != CL_SUCCESS) {
        ec_error(
            &ucc_ec_opencl.super, "failed to create command queue: %d", err);
        clReleaseContext(ucc_ec_opencl.context);
        return UCC_ERR_NO_RESOURCE;
    }

    ucc_ec_opencl.initialized    = 1;
    ucc_ec_opencl.kernels_built  = 0;
    ucc_ec_opencl.reduce_program = NULL;
    ucc_ec_opencl.reduce_kernel  = NULL;

    ec_info(
        &ucc_ec_opencl.super,
        "OpenCL EC plugin initialized: device=%d (%s)",
        cfg->device_id,
        device_name);

    return UCC_OK;
}

/* Get plugin attributes */
static ucc_status_t ucc_ec_opencl_get_attr(ucc_ec_attr_t *ec_attr)
{
    if (ec_attr->field_mask & UCC_EC_ATTR_FIELD_THREAD_MODE) {
        ec_attr->thread_mode = ucc_ec_opencl.thread_mode;
    }
    return UCC_OK;
}

/* Create OpenCL event for synchronization */
static ucc_status_t ucc_ec_opencl_create_event(void **event)
{
    cl_int    err;
    cl_event *cl_ev;

    if (!ucc_ec_opencl.initialized) {
        ec_error(&ucc_ec_opencl.super, "OpenCL EC not initialized");
        return UCC_ERR_NOT_SUPPORTED;
    }

    cl_ev = (cl_event *)ucc_malloc(sizeof(cl_event), "opencl_event");
    if (!cl_ev) {
        ec_error(&ucc_ec_opencl.super, "failed to allocate event");
        return UCC_ERR_NO_MEMORY;
    }

    /* Create user event - can be manually signaled */
    *cl_ev = clCreateUserEvent(ucc_ec_opencl.context, &err);
    if (err != CL_SUCCESS) {
        ec_error(&ucc_ec_opencl.super, "clCreateUserEvent failed: %d", err);
        ucc_free(cl_ev);
        return UCC_ERR_NO_RESOURCE;
    }

    *event = (void *)cl_ev;

    ec_trace(
        &ucc_ec_opencl.super, "created event %p (cl_event=%p)", cl_ev, *cl_ev);

    return UCC_OK;
}

/* Destroy OpenCL event */
static ucc_status_t ucc_ec_opencl_destroy_event(void *event)
{
    cl_event *cl_ev = (cl_event *)event;
    cl_int    err;

    if (!ucc_ec_opencl.initialized) {
        return UCC_ERR_NOT_SUPPORTED;
    }

    if (!cl_ev) {
        return UCC_ERR_INVALID_PARAM;
    }

    err = clReleaseEvent(*cl_ev);
    if (err != CL_SUCCESS) {
        ec_error(&ucc_ec_opencl.super, "clReleaseEvent failed: %d", err);
        /* Continue anyway to free wrapper */
    }

    ec_trace(&ucc_ec_opencl.super, "destroyed event %p", cl_ev);
    ucc_free(cl_ev);

    return UCC_OK;
}

/* Post event to OpenCL command queue (signal completion) */
static ucc_status_t ucc_ec_opencl_event_post(void *ee_context, void *event)
{
    cl_event        *cl_ev = (cl_event *)event;
    cl_command_queue queue = (cl_command_queue)ee_context;
    cl_int           err;

    if (!ucc_ec_opencl.initialized) {
        return UCC_ERR_NOT_SUPPORTED;
    }

    if (!cl_ev) {
        return UCC_ERR_INVALID_PARAM;
    }

    /* Use the queue from context or default queue */
    if (!queue) {
        queue = ucc_ec_opencl.queue;
    }

    /* Signal the user event as complete */
    err = clSetUserEventStatus(*cl_ev, CL_COMPLETE);
    if (err != CL_SUCCESS) {
        ec_error(&ucc_ec_opencl.super, "clSetUserEventStatus failed: %d", err);
        return UCC_ERR_NO_MESSAGE;
    }

    ec_trace(&ucc_ec_opencl.super, "posted event %p to queue %p", cl_ev, queue);

    return UCC_OK;
}

/* Test if OpenCL event has completed */
static ucc_status_t ucc_ec_opencl_event_test(void *event)
{
    cl_event *cl_ev = (cl_event *)event;
    cl_int    err, status;

    if (!ucc_ec_opencl.initialized) {
        return UCC_ERR_NOT_SUPPORTED;
    }

    if (!cl_ev) {
        return UCC_ERR_INVALID_PARAM;
    }

    /* Query event status */
    err = clGetEventInfo(
        *cl_ev,
        CL_EVENT_COMMAND_EXECUTION_STATUS,
        sizeof(cl_int),
        &status,
        NULL);
    if (err != CL_SUCCESS) {
        ec_error(&ucc_ec_opencl.super, "clGetEventInfo failed: %d", err);
        return UCC_ERR_NO_MESSAGE;
    }

    /* Check if completed */
    if (status == CL_COMPLETE) {
        ec_trace(&ucc_ec_opencl.super, "event %p completed", cl_ev);
        return UCC_OK;
    } else if (status < 0) {
        /* Negative values indicate errors */
        ec_error(
            &ucc_ec_opencl.super,
            "event %p failed with status %d",
            cl_ev,
            status);
        return UCC_ERR_NO_MESSAGE;
    }

    /* Event still in progress */
    return UCC_INPROGRESS;
}

/* Finalize OpenCL EC plugin */
static ucc_status_t ucc_ec_opencl_finalize(void)
{
    if (ucc_ec_opencl.initialized) {
        if (ucc_ec_opencl.kernels_built) {
            if (ucc_ec_opencl.reduce_kernel) {
                clReleaseKernel(ucc_ec_opencl.reduce_kernel);
            }
            if (ucc_ec_opencl.reduce_program) {
                clReleaseProgram(ucc_ec_opencl.reduce_program);
            }
        }
        clReleaseCommandQueue(ucc_ec_opencl.queue);
        clReleaseContext(ucc_ec_opencl.context);
        ucc_ec_opencl.initialized = 0;
        ec_info(&ucc_ec_opencl.super, "OpenCL EC plugin finalized");
    }
    return UCC_OK;
}

/* Executor operations - Full OpenCL implementation */

/* Initialize a new executor */
static ucc_status_t ucc_opencl_executor_init(
    const ucc_ee_executor_params_t *params, ucc_ee_executor_t **executor)
{
    ucc_opencl_executor_t *exec;
    cl_int                 err;
    ucc_status_t           status;

    if (!ucc_ec_opencl.initialized) {
        ec_error(&ucc_ec_opencl.super, "OpenCL EC not initialized");
        return UCC_ERR_NOT_SUPPORTED;
    }

    /* Build kernels on first executor creation */
    status = ucc_ec_opencl_build_kernels();
    if (status != UCC_OK) {
        return status;
    }

    exec = (ucc_opencl_executor_t *)ucc_malloc(
        sizeof(ucc_opencl_executor_t), "opencl_executor");
    if (!exec) {
        ec_error(&ucc_ec_opencl.super, "failed to allocate executor");
        return UCC_ERR_NO_MEMORY;
    }

    /* Create dedicated command queue for this executor */
    exec->exec_queue = clCreateCommandQueue(
        ucc_ec_opencl.context, ucc_ec_opencl.device, 0, &err);
    if (err != CL_SUCCESS) {
        ec_error(
            &ucc_ec_opencl.super,
            "failed to create executor command queue: %d",
            err);
        ucc_free(exec);
        return UCC_ERR_NO_RESOURCE;
    }

    exec->base.ee_type    = params->ee_type;
    exec->base.ee_context = (void *)exec->exec_queue;
    exec->task_types      = params->task_types;

    *executor             = &exec->base;

    ec_trace(
        &ucc_ec_opencl.super,
        "executor initialized: %p, queue=%p",
        exec,
        exec->exec_queue);

    return UCC_OK;
}

/* Check executor status */
static ucc_status_t ucc_opencl_executor_status(
    const ucc_ee_executor_t *executor)
{
    /* Always ready - OpenCL queues are always available */
    return UCC_OK;
}

/* Start executor (no-op for OpenCL - queues are always active) */
static ucc_status_t ucc_opencl_executor_start(
    ucc_ee_executor_t *executor, void *ee_context)
{
    ucc_opencl_executor_t *exec = (ucc_opencl_executor_t *)executor;

    /* Update context if provided */
    if (ee_context) {
        exec->base.ee_context = ee_context;
        ec_trace(
            &ucc_ec_opencl.super,
            "executor %p using context %p",
            exec,
            ee_context);
    }

    return UCC_OK;
}

/* Stop executor (no-op for OpenCL) */
static ucc_status_t ucc_opencl_executor_stop(ucc_ee_executor_t *executor)
{
    ucc_opencl_executor_t *exec = (ucc_opencl_executor_t *)executor;
    cl_int                 err;

    /* Finish all pending operations */
    err = clFinish(exec->exec_queue);
    if (err != CL_SUCCESS) {
        ec_error(&ucc_ec_opencl.super, "clFinish failed: %d", err);
        return UCC_ERR_NO_MESSAGE;
    }

    ec_trace(&ucc_ec_opencl.super, "executor %p stopped", exec);

    return UCC_OK;
}

/* Finalize executor and release resources */
static ucc_status_t ucc_opencl_executor_finalize(ucc_ee_executor_t *executor)
{
    ucc_opencl_executor_t *exec = (ucc_opencl_executor_t *)executor;

    if (exec->exec_queue) {
        clFinish(exec->exec_queue);
        clReleaseCommandQueue(exec->exec_queue);
    }

    ec_trace(&ucc_ec_opencl.super, "executor %p finalized", exec);
    ucc_free(exec);

    return UCC_OK;
}

/* Post a task to the executor */
static ucc_status_t ucc_opencl_executor_task_post(
    ucc_ee_executor_t *executor, const ucc_ee_executor_task_args_t *task_args,
    ucc_ee_executor_task_t **task)
{
    ucc_opencl_executor_t      *exec = (ucc_opencl_executor_t *)executor;
    ucc_opencl_executor_task_t *opencl_task;
    cl_int                      err;

    opencl_task = (ucc_opencl_executor_task_t *)ucc_malloc(
        sizeof(ucc_opencl_executor_task_t), "opencl_exec_task");
    if (!opencl_task) {
        ec_error(&ucc_ec_opencl.super, "failed to allocate task");
        return UCC_ERR_NO_MEMORY;
    }

    opencl_task->base.eee    = executor;
    opencl_task->base.status = UCC_INPROGRESS;
    opencl_task->completed   = 0;
    opencl_task->event       = NULL;

    /* Execute the task based on type */
    switch (task_args->task_type) {
    case UCC_EE_EXECUTOR_TASK_COPY:
    {
        const ucc_eee_task_copy_t *copy = &task_args->copy;

        /* Use clEnqueueCopyBuffer for device-to-device copies */
        err                             = clEnqueueCopyBuffer(
            exec->exec_queue,
            (cl_mem)copy->src,
            (cl_mem)copy->dst,
            0,
            0,
            copy->len,
            0,
            NULL,
            &opencl_task->event);

        if (err != CL_SUCCESS) {
            ec_error(
                &ucc_ec_opencl.super, "clEnqueueCopyBuffer failed: %d", err);
            ucc_free(opencl_task);
            return UCC_ERR_NO_MESSAGE;
        }

        ec_trace(
            &ucc_ec_opencl.super,
            "task posted: COPY src=%p dst=%p len=%zu",
            copy->src,
            copy->dst,
            copy->len);
        break;
    }

    case UCC_EE_EXECUTOR_TASK_REDUCE:
    {
        const ucc_eee_task_reduce_t *reduce = &task_args->reduce;

        /* For now, only support 2-source SUM reduction */
        if (reduce->n_srcs != 2) {
            ec_error(
                &ucc_ec_opencl.super,
                "only 2-source reductions supported, got %d",
                reduce->n_srcs);
            ucc_free(opencl_task);
            return UCC_ERR_NOT_SUPPORTED;
        }

        if (reduce->op != UCC_OP_SUM) {
            ec_error(&ucc_ec_opencl.super, "only SUM operation supported");
            ucc_free(opencl_task);
            return UCC_ERR_NOT_SUPPORTED;
        }

        /* Select kernel based on datatype */
        const char *kernel_name;
        size_t      elem_size;

        switch (reduce->dt) {
        case UCC_DT_FLOAT32:
            kernel_name = "reduce_sum_float";
            elem_size   = 4;
            break;
        case UCC_DT_FLOAT64:
            kernel_name = "reduce_sum_double";
            elem_size   = 8;
            break;
        default:
            ec_error(
                &ucc_ec_opencl.super, "unsupported datatype: %d", reduce->dt);
            ucc_free(opencl_task);
            return UCC_ERR_NOT_SUPPORTED;
        }

        /* Create kernel if not already created */
        cl_kernel kernel = clCreateKernel(
            ucc_ec_opencl.reduce_program, kernel_name, &err);
        if (err != CL_SUCCESS) {
            ec_error(&ucc_ec_opencl.super, "clCreateKernel failed: %d", err);
            ucc_free(opencl_task);
            return UCC_ERR_NO_RESOURCE;
        }

        /* Set kernel arguments */
        void   **srcs  = (task_args->flags & UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT)
                             ? reduce->srcs_ext
                             : (void **)reduce->srcs;

        cl_ulong count = reduce->count;
        clSetKernelArg(kernel, 0, sizeof(cl_mem), reduce->dst);
        clSetKernelArg(kernel, 1, sizeof(cl_mem), &srcs[0]);
        clSetKernelArg(kernel, 2, sizeof(cl_mem), &srcs[1]);
        clSetKernelArg(kernel, 3, sizeof(cl_ulong), &count);

        /* Launch kernel */
        size_t global_size = reduce->count;
        size_t local_size  = 256; /* Typical work-group size */

        /* Round up to multiple of local_size */
        global_size        = ((global_size + local_size - 1) / local_size) *
                      local_size;

        err = clEnqueueNDRangeKernel(
            exec->exec_queue,
            kernel,
            1,
            NULL,
            &global_size,
            &local_size,
            0,
            NULL,
            &opencl_task->event);

        clReleaseKernel(kernel);

        if (err != CL_SUCCESS) {
            ec_error(
                &ucc_ec_opencl.super, "clEnqueueNDRangeKernel failed: %d", err);
            ucc_free(opencl_task);
            return UCC_ERR_NO_MESSAGE;
        }

        ec_trace(
            &ucc_ec_opencl.super,
            "task posted: REDUCE op=%d dt=%d count=%zu",
            reduce->op,
            reduce->dt,
            reduce->count);
        break;
    }

    default:
        ec_error(
            &ucc_ec_opencl.super,
            "unsupported task type: %d",
            task_args->task_type);
        ucc_free(opencl_task);
        return UCC_ERR_NOT_SUPPORTED;
    }

    *task = &opencl_task->base;
    return UCC_OK;
}

/* Test if a task has completed */
static ucc_status_t ucc_opencl_executor_task_test(
    const ucc_ee_executor_task_t *task)
{
    ucc_opencl_executor_task_t *opencl_task = (ucc_opencl_executor_task_t *)
        task;
    cl_int err, status;

    if (opencl_task->completed) {
        return UCC_OK;
    }

    if (!opencl_task->event) {
        /* No event means task completed synchronously */
        opencl_task->completed = 1;
        return UCC_OK;
    }

    /* Query event status */
    err = clGetEventInfo(
        opencl_task->event,
        CL_EVENT_COMMAND_EXECUTION_STATUS,
        sizeof(cl_int),
        &status,
        NULL);

    if (err != CL_SUCCESS) {
        ec_error(&ucc_ec_opencl.super, "clGetEventInfo failed: %d", err);
        return UCC_ERR_NO_MESSAGE;
    }

    if (status == CL_COMPLETE) {
        opencl_task->completed = 1;
        ec_trace(&ucc_ec_opencl.super, "task %p completed", opencl_task);
        return UCC_OK;
    } else if (status < 0) {
        ec_error(
            &ucc_ec_opencl.super,
            "task %p failed with status %d",
            opencl_task,
            status);
        return UCC_ERR_NO_MESSAGE;
    }

    return UCC_INPROGRESS;
}

/* Finalize a task and release resources */
static ucc_status_t ucc_opencl_executor_task_finalize(
    ucc_ee_executor_task_t *task)
{
    ucc_opencl_executor_task_t *opencl_task = (ucc_opencl_executor_task_t *)
        task;

    if (opencl_task->event) {
        clReleaseEvent(opencl_task->event);
    }

    ec_trace(&ucc_ec_opencl.super, "task %p finalized", opencl_task);
    ucc_free(opencl_task);

    return UCC_OK;
}

/**
 * Export the component interface
 * Symbol name: ucc_ec_opencl (matches library: libucc_ec_opencl.so)
 */
ucc_ec_opencl_t ucc_ec_opencl = {
    .super.super.name                 = "opencl ec",
    .super.ref_cnt                    = 0,
    .super.type                       = UCC_EE_LAST, /* Auto-assigned by UCC */
    .super.init                       = ucc_ec_opencl_init,
    .super.get_attr                   = ucc_ec_opencl_get_attr,
    .super.finalize                   = ucc_ec_opencl_finalize,
    .super.ops.create_event           = ucc_ec_opencl_create_event,
    .super.ops.destroy_event          = ucc_ec_opencl_destroy_event,
    .super.ops.event_post             = ucc_ec_opencl_event_post,
    .super.ops.event_test             = ucc_ec_opencl_event_test,
    /* Executor operations - Full OpenCL implementation with kernels */
    .super.executor_ops.init          = ucc_opencl_executor_init,
    .super.executor_ops.start         = ucc_opencl_executor_start,
    .super.executor_ops.status        = ucc_opencl_executor_status,
    .super.executor_ops.stop          = ucc_opencl_executor_stop,
    .super.executor_ops.finalize      = ucc_opencl_executor_finalize,
    .super.executor_ops.task_post     = ucc_opencl_executor_task_post,
    .super.executor_ops.task_test     = ucc_opencl_executor_task_test,
    .super.executor_ops.task_finalize = ucc_opencl_executor_task_finalize,
    .super.config_table =
        {
            .name   = "OpenCL execution component",
            .prefix = "EC_OPENCL_",
            .table  = ucc_ec_opencl_config_table,
            .size   = sizeof(ucc_ec_opencl_config_t),
        },
    .initialized    = 0,
    .kernels_built  = 0,
    .reduce_program = NULL,
    .reduce_kernel  = NULL,
};

/* Register config table */
UCC_CONFIG_REGISTER_TABLE_ENTRY(
    &ucc_ec_opencl.super.config_table, &ucc_config_global_list);

