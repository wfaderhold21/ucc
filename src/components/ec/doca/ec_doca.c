/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ec_doca.h"
#include "components/ec/ucc_ec.h"
#include "components/mc/ucc_mc.h"
#include "components/ec/base/ucc_ec_host_ops.h"
#include "utils/ucc_atomic.h"
#include <limits.h>
#include <stdlib.h>
#include <stdbool.h>

/*
 * DOCA (BlueField DPU) execution component.
 *
 * Task 583 (skeleton): registers the UCC_EE_DOCA executor backend + the
 * ucc_ee_type_t wiring so DOCA-capable reduce/copy work can be routed to the
 * DPU (gated by the UCC_USE_DOCA_REDUCE global option in core/ucc_coll.c).
 *
 * Task 586 (host fallback): the DOCA command channel (task 584) is not
 * connected yet and requires a BlueField DPU, so a reduce that is actually
 * selected for the DOCA executor (UCC_USE_DOCA_REDUCE=1) must still produce
 * correct results.  This mirrors the CUDA/ROCm executors' nested-CPU pattern:
 * each executor owns a nested CPU executor (the Milestone-B host-offload path)
 * and, when a reduce cannot run on the DPU, delegates it to that CPU executor
 * at post time and completes it there.  A task type the CPU path cannot serve
 * (e.g. COPY while the DPU channel is absent) reports UCC_ERR_NOT_SUPPORTED so
 * a caller can still detect the gap.
 */

static ucc_config_field_t ucc_ec_doca_config_table[] = {
    {"", "", NULL, ucc_offsetof(ucc_ec_doca_config_t, super),
     UCC_CONFIG_TYPE_TABLE(ucc_ec_config_table)},

    {"DOCA_REDUCE_LIMIT", "65536",
     "Maximum reduce size (bytes) routed to the DPU / host offload; larger "
     "reduces are rejected (task 585 will make this a policy knob)",
     ucc_offsetof(ucc_ec_doca_config_t, reduce_limit),
     UCC_CONFIG_TYPE_ULUNITS},

    {"DPU_NUM_WORKERS", "4",
     "Number of DPU ARM-core workers for the DOCA reduce pool (task 584)",
     ucc_offsetof(ucc_ec_doca_config_t, dpu_num_workers),
     UCC_CONFIG_TYPE_ULUNITS},

    {"DPU_AFFINITY", "0",
     "DPU worker core affinity (0 = leave to the scheduler)",
     ucc_offsetof(ucc_ec_doca_config_t, dpu_affinity),
     UCC_CONFIG_TYPE_INT},

    {"DEVICE", "",
     "IB device of the BlueField DPU to connect the DOCA command channel "
     "on (584); empty = channel not connected and reduces use the host "
     "fallback",
     ucc_offsetof(ucc_ec_doca_config_t, device),
     UCC_CONFIG_TYPE_STRING},

    {NULL}
};

/*
 * Is a pointer directly readable by the host CPU executor?  Host and
 * CUDA-managed pointers are; true device-resident pointers are not.  Uses the
 * UCC memory API (not cudaPointerGetAttributes) so this component stays
 * CUDA-independent — the DOCA path serves CUDA buffers without linking CUDA.
 * An unknown/plain-malloc pointer is treated as host-accessible (it is).
 */
static int ec_doca_ptr_host_accessible(const void *ptr)
{
    ucc_mem_attr_t mem_attr;
    ucc_status_t   status;

    if (ptr == NULL) {
        return 1;
    }
    mem_attr.field_mask = UCC_MEM_ATTR_FIELD_MEM_TYPE;
    status              = ucc_mc_get_mem_attr(ptr, &mem_attr);
    if (status != UCC_OK) {
        return 1; /* unknown pointer: assume host-accessible */
    }
    return (mem_attr.mem_type != UCC_MEMORY_TYPE_CUDA) ? 1 : 0;
}

/*
 * True when every buffer the reduce touches (dst + all sources) is directly
 * readable by the host CPU executor: host, managed, or zero-copy.  Mirrors
 * ec_cuda_reduce_bufs_host_accessible().
 */
static int ec_doca_reduce_bufs_host_accessible(const ucc_ee_executor_task_args_t *task_args)
{
    if (task_args->task_type == UCC_EE_EXECUTOR_TASK_REDUCE) {
        const ucc_eee_task_reduce_t *r = &task_args->reduce;
        int                          i;

        if (!ec_doca_ptr_host_accessible(r->dst)) {
            return 0;
        }
        for (i = 0; i < (int)r->n_srcs; i++) {
            const void *src = ((task_args->flags &
                                UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT) ?
                               r->srcs_ext[i] : r->srcs[i]);
            if (!ec_doca_ptr_host_accessible(src)) {
                return 0;
            }
        }
        return 1;
    }
    /* UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED */
    {
        const ucc_eee_task_reduce_strided_t *s = &task_args->reduce_strided;

        if (!ec_doca_ptr_host_accessible(s->dst) ||
            !ec_doca_ptr_host_accessible(s->src1)) {
            return 0;
        }
        return (s->n_src2 == 0) ? 1 : ec_doca_ptr_host_accessible(s->src2);
    }
}

/*
 * Host-fallback policy (task 586): with the DOCA command channel absent, an
 * eligible reduce runs on the nested CPU executor.  Eligibility mirrors the
 * Milestone-B host-offload policy: a supported size, a host-supported datatype,
 * and (with the default residency gate) buffers directly readable by the CPU.
 */
static bool ec_doca_use_host_ops(const ucc_ee_executor_task_args_t *task_args)
{
    if (task_args->task_type != UCC_EE_EXECUTOR_TASK_REDUCE &&
        task_args->task_type != UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED) {
        return false;
    }
    if (ucc_ec_host_total_reduce_len(task_args) >
        (int)EC_DOCA_CONFIG->reduce_limit) {
        return false;
    }
    if (!ec_doca_reduce_bufs_host_accessible(task_args)) {
        return false;
    }
    return ucc_ec_host_dt_supported(task_args);
}

static ucc_status_t ucc_ec_doca_init(const ucc_ec_params_t *ec_params)
{
    const char   *device;
    int           rc;
    ucc_status_t  status;

    ucc_strncpy_safe(ucc_ec_doca.super.config->log_component.name,
                     ucc_ec_doca.super.super.name,
                     sizeof(ucc_ec_doca.super.config->log_component.name));
    ucc_ec_doca.thread_mode = ec_params->thread_mode;
    ucc_ec_doca.channel_connected = 0;

    status = ucc_mpool_init(&ucc_ec_doca.executors, 0,
                            sizeof(ucc_ec_doca_executor_t),
                            0, UCC_CACHE_LINE_SIZE, 16, UINT_MAX, NULL,
                            ec_params->thread_mode, "ec doca executors");
    if (status != UCC_OK) {
        ec_error(&ucc_ec_doca.super, "failed to create ec doca executors mpool");
        return status;
    }

    status = ucc_mpool_init(&ucc_ec_doca.task_wrappers, 0,
                            sizeof(ucc_ec_doca_task_t),
                            0, UCC_CACHE_LINE_SIZE, 16, UINT_MAX, NULL,
                            ec_params->thread_mode, "ec doca task wrappers");
    if (status != UCC_OK) {
        ec_error(&ucc_ec_doca.super,
                 "failed to create ec doca task wrappers mpool");
        ucc_mpool_cleanup(&ucc_ec_doca.executors, 1);
        return status;
    }

    /*
     * 584: attempt to bring up the host-side DOCA UROM command channel on
     * the configured DPU.  A failure (no DOCA SDK, no DPU, device not
     * found) is not fatal: the component stays available and every reduce
     * uses the host fallback (task 586).
     */
    device = EC_DOCA_CONFIG->device;
    if (device && device[0] != '\0') {
        rc = ecc_doca_channel_connect(&ecc_doca_urom, device);
        if (rc == ECC_DOCA_SUCCESS) {
            ucc_ec_doca.channel_connected = 1;
            ec_info(&ucc_ec_doca.super,
                    "doca ec: DOCA command channel connected on %s", device);
        } else {
            ec_info(&ucc_ec_doca.super,
                    "doca ec: DOCA command channel unavailable on %s (rc=%d); "
                    "reduces use the host fallback", device, rc);
        }
    } else {
        ec_info(&ucc_ec_doca.super,
                "doca ec initialized (host fallback; no DOCA_DEVICE set)");
    }

    return UCC_OK;
}

static ucc_status_t ucc_ec_doca_get_attr(ucc_ec_attr_t *ec_attr)
{
    if (ec_attr->field_mask & UCC_EC_ATTR_FIELD_THREAD_MODE) {
        ec_attr->thread_mode = ucc_ec_doca.thread_mode;
    }

    return UCC_OK;
}

/*
 * Task 588 / C4: the DOCA offload target is the DPU command channel.  It is
 * "supported" (reachable) only when a channel was actually connected to a
 * DPU at init (584).  When no DPU is present the collective routing must keep
 * the CUDA-stream path (Milestone B) rather than route to a DOCA executor
 * that cannot serve the collective's decomposed tasks — so report
 * NOT_SUPPORTED here.  This is the clean-fallback gate for USE_DOCA_REDUCE.
 */
static ucc_status_t ucc_ec_doca_offload_supported(void)
{
    if (ucc_ec_doca.channel_connected) {
        return UCC_OK;
    }

    return UCC_ERR_NOT_SUPPORTED;
}

static ucc_status_t ucc_ec_doca_finalize()
{
    ecc_doca_channel_disconnect(&ecc_doca_urom);
    ucc_mpool_cleanup(&ucc_ec_doca.task_wrappers, 1);
    ucc_mpool_cleanup(&ucc_ec_doca.executors, 1);

    return UCC_OK;
}

/*
 * Executor lifecycle.  init allocates the executor pool, the task wrapper
 * pool, and a nested CPU executor (the host-offload fallback, task 586);
 * start/stop/status are no-op stubs (no DPU stream to arm in the skeleton);
 * finalize returns the CPU executor first, then the two pools.  Mirrors
 * ucc_cuda_executor_init.
 */
ucc_status_t ucc_doca_executor_init(const ucc_ee_executor_params_t *params,
                                    ucc_ee_executor_t **executor)
{
    ucc_ec_doca_executor_t *eee =
        (ucc_ec_doca_executor_t *)ucc_mpool_get(&ucc_ec_doca.executors);
    ucc_ee_executor_params_t cpu_params = {
        .mask    = UCC_EE_EXECUTOR_PARAM_FIELD_TYPE,
        .ee_type = UCC_EE_CPU_THREAD
    };
    ucc_status_t             status;

    ec_trace(&ucc_ec_doca.super, "executor init, eee: %p", (void *)eee);
    if (ucc_unlikely(!eee)) {
        ec_error(&ucc_ec_doca.super, "failed to allocate executor");
        return UCC_ERR_NO_MEMORY;
    }

    eee->super.ee_type = params->ee_type;
    eee->state         = UCC_EC_DOCA_EXECUTOR_INITIALIZED;
    eee->cpu_executor  = NULL;

    /* Host-fallback executor (task 586): the DOCA path serves reduces by
     * delegating to the Milestone-B CPU executor, so it is required. */
    status = ucc_ee_executor_init(&cpu_params, &eee->cpu_executor);
    if (status != UCC_OK) {
        ec_error(&ucc_ec_doca.super,
                 "failed to init nested CPU executor for DOCA host fallback");
        ucc_mpool_put(eee);
        return status;
    }

    *executor = &eee->super;
    return UCC_OK;
}

ucc_status_t ucc_doca_executor_start(ucc_ee_executor_t *executor, //NOLINT
                                     void *ee_context)            //NOLINT
{
    (void)ee_context;
    ((ucc_ec_doca_executor_t *)executor)->state = UCC_EC_DOCA_EXECUTOR_STARTED;
    return UCC_OK;
}

ucc_status_t ucc_doca_executor_status(const ucc_ee_executor_t *executor) //NOLINT
{
    (void)executor;
    return UCC_OK;
}

ucc_status_t ucc_doca_executor_stop(ucc_ee_executor_t *executor) //NOLINT
{
    ((ucc_ec_doca_executor_t *)executor)->state = UCC_EC_DOCA_EXECUTOR_STOPPED;
    return UCC_OK;
}

/*
 * Task post.  The DPU-side reduce worker (task 584, the DOCA worker that
 * executes the reduce on the DPU ARM cores) is not part of this build —
 * it requires a BlueField DPU and reuses the plugin queue/command
 * infrastructure in contrib/doca_urom_ucc_plugin/dpu/worker_ucc.c — so every
 * eligible reduce is delegated to the nested CPU executor (task 586): the
 * CPU task is run to completion here (it is a host reduce), then a completed
 * wrapper is returned.  The host-side DOCA command channel (task 584) is
 * attempted at component init when EC_DOCA_DEVICE is set, but a reduce still
 * completes on the host path until the DPU worker lands.  An ineligible
 * reduce (too large, unsupported datatype, or device-resident buffers with
 * the default residency gate) and any task type the CPU path cannot serve
 * (e.g. COPY) report UCC_ERR_NOT_SUPPORTED.
 */
ucc_status_t ucc_doca_executor_task_post(ucc_ee_executor_t *executor,
                                         const ucc_ee_executor_task_args_t *task_args,
                                         ucc_ee_executor_task_t **task)
{
    ucc_ec_doca_executor_t *eee =
        (ucc_ec_doca_executor_t *)executor;
    ucc_ec_doca_task_t     *doca_task;
    ucc_ee_executor_task_t *cpu_task;
    ucc_status_t            status;

    if (ec_doca_use_host_ops(task_args) && eee->cpu_executor) {
        ec_trace(&ucc_ec_doca.super,
                 "routing reduce to CPU executor (DOCA host fallback)");

        /* Run the CPU reduce to completion (host work), mirroring the CUDA
         * host-offload delegation. */
        status = ucc_ee_executor_task_post(eee->cpu_executor,
                                           task_args, &cpu_task);
        if (ucc_unlikely(status != UCC_OK)) {
            return status;
        }
        do {
            status = ucc_ee_executor_task_test(cpu_task);
        } while (status == UCC_INPROGRESS);
        if (ucc_unlikely(status != UCC_OK)) {
            ucc_ee_executor_task_finalize(cpu_task);
            return status;
        }
        ucc_ee_executor_task_finalize(cpu_task);
    } else {
        /* Nothing can run this task on the DPU or the host path. */
        *task = NULL;
        return UCC_ERR_NOT_SUPPORTED;
    }
    /* The CPU reduce already finished; the wrapper just reports completion. */
    doca_task = (ucc_ec_doca_task_t *)ucc_mpool_get(&ucc_ec_doca.task_wrappers);
    if (ucc_unlikely(!doca_task)) {
        return UCC_ERR_NO_MEMORY;
    }
    doca_task->super.eee    = executor;
    doca_task->super.status = UCC_INPROGRESS;
    memcpy(&doca_task->super.args, task_args,
           sizeof(ucc_ee_executor_task_args_t));
    doca_task->finished     = 1;
    *task                   = &doca_task->super;
    return UCC_OK;
}

ucc_status_t ucc_doca_executor_task_test(const ucc_ee_executor_task_t *task)
{
    const ucc_ec_doca_task_t *doca_task =
        (const ucc_ec_doca_task_t *)task;

    if (doca_task->finished) {
        return UCC_OK;
    }
    return UCC_INPROGRESS;
}

ucc_status_t ucc_doca_executor_task_finalize(ucc_ee_executor_task_t *task)
{
    ucc_mpool_put(task);
    return UCC_OK;
}

ucc_status_t ucc_doca_executor_finalize(ucc_ee_executor_t *executor)
{
    ucc_ec_doca_executor_t *eee = (ucc_ec_doca_executor_t *)executor;

    ec_trace(&ucc_ec_doca.super, "executor finalize, eee: %p", executor);
    if (eee->cpu_executor) {
        ucc_ee_executor_finalize(eee->cpu_executor);
        eee->cpu_executor = NULL;
    }
    ucc_mpool_put(executor);

    return UCC_OK;
}

ucc_ec_doca_t ucc_ec_doca = {
    .super.super.name                 = "doca ec",
    .super.ref_cnt                    = 0,
    .super.type                       = UCC_EE_DOCA,
    .super.init                       = ucc_ec_doca_init,
    .super.get_attr                   = ucc_ec_doca_get_attr,
    .super.finalize                   = ucc_ec_doca_finalize,
    .super.config_table =
        {
            .name   = "DOCA execution component",
            .prefix = "EC_DOCA_",
            .table  = ucc_ec_doca_config_table,
            .size   = sizeof(ucc_ec_doca_config_t),
        },
    .super.ops.create_event           = NULL,
    .super.ops.destroy_event          = NULL,
    .super.ops.event_post             = NULL,
    .super.ops.event_test             = NULL,
    .super.ops.offload_supported      = ucc_ec_doca_offload_supported,
    .super.executor_ops.init          = ucc_doca_executor_init,
    .super.executor_ops.start         = ucc_doca_executor_start,
    .super.executor_ops.status        = ucc_doca_executor_status,
    .super.executor_ops.stop          = ucc_doca_executor_stop,
    .super.executor_ops.task_post     = ucc_doca_executor_task_post,
    .super.executor_ops.task_test     = ucc_doca_executor_task_test,
    .super.executor_ops.task_finalize = ucc_doca_executor_task_finalize,
    .super.executor_ops.finalize      = ucc_doca_executor_finalize,
};

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_ec_doca.super.config_table,
                                &ucc_config_global_list);
