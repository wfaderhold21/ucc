/**
 * Copyright (c) 2022-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ec_cpu.h"
#ifdef HAVE_EC_THREADED_REDUCE
#include "ec_cpu_thread_pool.h"
#include "ec_cpu_reduce_threaded.h"
#endif
#include "utils/arch/cpu.h"
#include "components/mc/ucc_mc.h"
#include <limits.h>

static ucc_config_field_t ucc_ec_cpu_config_table[] = {
    {"", "", NULL, ucc_offsetof(ucc_ec_cpu_config_t, super),
     UCC_CONFIG_TYPE_TABLE(ucc_ec_config_table)},

#ifdef HAVE_EC_THREADED_REDUCE
    {"EXEC_NUM_WORKERS", "4",
     "Number of worker threads to use for CPU executor",
     ucc_offsetof(ucc_ec_cpu_config_t, exec_num_workers),
     UCC_CONFIG_TYPE_ULUNITS},

    {"EXEC_MAX_TASKS", "128",
     "Maximum number of outstanding tasks per executor",
     ucc_offsetof(ucc_ec_cpu_config_t, exec_max_tasks),
     UCC_CONFIG_TYPE_ULUNITS},

    {"REDUCE_CHUNK_SIZE", "1024",
     "Chunk size for threaded reductions",
     ucc_offsetof(ucc_ec_cpu_config_t, reduce_chunk_size),
     UCC_CONFIG_TYPE_ULUNITS},

    {"USE_THREADED_REDUCE", "0",
     "Enable threaded CPU reductions (requires --enable-ec-threaded-reduce "
     "at configure time)",
     ucc_offsetof(ucc_ec_cpu_config_t, use_threaded_reduce),
     UCC_CONFIG_TYPE_BOOL},

    {"PIN_THREADS", "0",
     "Pin worker threads to CPU cores for better performance",
     ucc_offsetof(ucc_ec_cpu_config_t, pin_threads),
     UCC_CONFIG_TYPE_BOOL},

    {"USE_TOPOLOGY_PINNING", "1",
     "Use UCC topology system for thread pinning (more robust than manual "
     "CPU IDs)",
     ucc_offsetof(ucc_ec_cpu_config_t, use_topology_pinning),
     UCC_CONFIG_TYPE_BOOL},

    {"PIN_TO_SOCKET", "0",
     "Pin threads to specific socket ID (used when USE_TOPOLOGY_PINNING=1)",
     ucc_offsetof(ucc_ec_cpu_config_t, pin_to_socket),
     UCC_CONFIG_TYPE_INT},

    {"PIN_TO_NUMA", "0",
     "Pin threads to specific NUMA node ID (used when USE_TOPOLOGY_PINNING=1)",
     ucc_offsetof(ucc_ec_cpu_config_t, pin_to_numa),
     UCC_CONFIG_TYPE_INT},

    {"START_CPU_ID", "0",
     "Starting CPU ID for thread pinning (fallback when "
     "USE_TOPOLOGY_PINNING=0)",
     ucc_offsetof(ucc_ec_cpu_config_t, start_cpu_id),
     UCC_CONFIG_TYPE_INT},
#else
    {"USE_THREADED_REDUCE", "0",
     "Enable threaded CPU reductions (disabled: not compiled with "
     "--enable-ec-threaded-reduce)",
     ucc_offsetof(ucc_ec_cpu_config_t, use_threaded_reduce),
     UCC_CONFIG_TYPE_BOOL},
#endif

    {NULL}
};

static ucc_status_t ucc_ec_cpu_init(const ucc_ec_params_t *ec_params)
{
    ucc_status_t status;
    ucc_ec_cpu_config_t *cfg = EC_CPU_CONFIG;

    ucc_strncpy_safe(ucc_ec_cpu.super.config->log_component.name,
                     ucc_ec_cpu.super.super.name,
                     sizeof(ucc_ec_cpu.super.config->log_component.name));
    ucc_ec_cpu.thread_mode = ec_params->thread_mode;

    status = ucc_mpool_init(&ucc_ec_cpu.executors, 0, sizeof(ucc_ee_executor_t),
                            0, UCC_CACHE_LINE_SIZE, 16, UINT_MAX, NULL,
                            ec_params->thread_mode, "ec cpu executors");
    if (status != UCC_OK) {
        ec_error(&ucc_ec_cpu.super, "failed to created ec cpu executors mpool");
        return status;
    }

    status = ucc_mpool_init(&ucc_ec_cpu.executor_tasks, 0,
                            sizeof(ucc_ee_executor_task_t),
                            0, UCC_CACHE_LINE_SIZE, 16, UINT_MAX, NULL,
                            ec_params->thread_mode, "ec cpu executor tasks");
    if (status != UCC_OK) {
        ec_error(&ucc_ec_cpu.super,
                 "failed to created ec cpu executor tasks mpool");
        ucc_mpool_cleanup(&ucc_ec_cpu.executors, 1);
        return status;
    }

    /* Initialize thread pool only if both configure-time and runtime options are enabled */
#ifdef HAVE_EC_THREADED_REDUCE
    if (cfg->use_threaded_reduce) {
        status = ucc_ec_cpu_thread_pool_init(&ucc_ec_cpu.thread_pool, cfg);
        if (status != UCC_OK) {
            ec_error(&ucc_ec_cpu.super, "failed to initialize CPU thread pool");
            ucc_mpool_cleanup(&ucc_ec_cpu.executors, 1);
            ucc_mpool_cleanup(&ucc_ec_cpu.executor_tasks, 1);
            return status;
        }
        ec_info(&ucc_ec_cpu.super, "CPU threaded reductions enabled with %lu workers", cfg->exec_num_workers);
    } else {
        ec_debug(&ucc_ec_cpu.super, "CPU threaded reductions disabled (runtime option not set)");
    }
#else
    if (cfg->use_threaded_reduce) {
        ec_warn(&ucc_ec_cpu.super, "CPU threaded reductions requested but not compiled (use --enable-ec-threaded-reduce)");
    }
#endif

    return UCC_OK;
}

static ucc_status_t ucc_ec_cpu_get_attr(ucc_ec_attr_t *ec_attr)
{
    if (ec_attr->field_mask & UCC_EC_ATTR_FIELD_THREAD_MODE) {
        ec_attr->thread_mode = ucc_ec_cpu.thread_mode;
    }

    return UCC_OK;
}

static ucc_status_t ucc_ec_cpu_finalize()
{
#ifdef HAVE_EC_THREADED_REDUCE
    ucc_ec_cpu_config_t *cfg = EC_CPU_CONFIG;
    if (cfg->use_threaded_reduce) {
        ucc_ec_cpu_thread_pool_finalize(&ucc_ec_cpu.thread_pool);
    }
#endif

    ucc_mpool_cleanup(&ucc_ec_cpu.executors, 1);
    ucc_mpool_cleanup(&ucc_ec_cpu.executor_tasks, 1);

    return UCC_OK;
}

ucc_status_t ucc_cpu_executor_init(const ucc_ee_executor_params_t *params,
                                   ucc_ee_executor_t **executor)
{
    ucc_ee_executor_t *eee = ucc_mpool_get(&ucc_ec_cpu.executors);

    ec_trace(&ucc_ec_cpu.super, "executor init, eee: %p", eee);
    if (ucc_unlikely(!eee)) {
        ec_error(&ucc_ec_cpu.super, "failed to allocate executor");
        return UCC_ERR_NO_MEMORY;
    }

    eee->ee_type = params->ee_type;
    *executor = eee;

    return UCC_OK;
}

ucc_status_t ucc_cpu_executor_start(ucc_ee_executor_t *executor, //NOLINT
                                    void *ee_context)            //NOLINT
{
    return UCC_OK;
}

ucc_status_t ucc_cpu_executor_status(const ucc_ee_executor_t *executor) //NOLINT
{
    return UCC_OK;
}

ucc_status_t ucc_cpu_executor_stop(ucc_ee_executor_t *executor) //NOLINT
{
    return UCC_OK;
}

ucc_status_t ucc_cpu_executor_task_post(ucc_ee_executor_t *executor,
                                        const ucc_ee_executor_task_args_t *task_args,
                                        ucc_ee_executor_task_t **task)
{
    ucc_status_t            status = UCC_OK;
    ucc_ec_cpu_config_t    *cfg   = EC_CPU_CONFIG;
    ucc_ee_executor_task_t *eee_task;

    eee_task = ucc_mpool_get(&ucc_ec_cpu.executor_tasks);
    if (ucc_unlikely(!eee_task)) {
        return UCC_ERR_NO_MEMORY;
    }

    eee_task->eee = executor;

    /* Copy task arguments */
    memcpy(&eee_task->args, task_args, sizeof(ucc_ee_executor_task_args_t));

    if (cfg->use_threaded_reduce &&
        (task_args->task_type == UCC_EE_EXECUTOR_TASK_REDUCE ||
         task_args->task_type == UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED)) {
        /* For very large message sizes, fall back to synchronous execution
         * to avoid memory pressure */
        size_t data_size = task_args->reduce.count *
                          ucc_dt_size(task_args->reduce.dt);
        if (data_size > 1024 * 1024) { /* 1MB threshold */
            /* Fall back to synchronous execution for large messages */
            switch (eee_task->args.task_type) {
            case UCC_EE_EXECUTOR_TASK_REDUCE:
                status = ucc_ec_cpu_reduce(&eee_task->args.reduce,
                                          eee_task->args.reduce.dst,
                                          (eee_task->args.flags &
                                           UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT) ?
                                              eee_task->args.reduce.srcs_ext :
                                              eee_task->args.reduce.srcs,
                                          eee_task->args.flags);
                break;
            case UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED:
                status = ucc_ec_cpu_reduce_strided(
                    eee_task->args.reduce_strided.src1,
                    eee_task->args.reduce_strided.src2,
                    eee_task->args.reduce_strided.dst,
                    eee_task->args.reduce_strided.n_src2 + 1,
                    eee_task->args.reduce_strided.count,
                    eee_task->args.reduce_strided.stride,
                    eee_task->args.reduce_strided.dt,
                    eee_task->args.reduce_strided.op,
                    eee_task->args.flags);
                break;
            default:
                status = UCC_ERR_NOT_SUPPORTED;
                break;
            }
        } else {
            /* Use threaded execution for smaller messages */
            status = ucc_ec_cpu_thread_pool_post_task(&ucc_ec_cpu.thread_pool,
                                                      eee_task);
            if (status == UCC_OK) {
                status = ucc_ec_cpu_thread_pool_wait_task(&ucc_ec_cpu.thread_pool,
                                                          eee_task);
            }
        }
    } else {
        /* Execute synchronously for non-reduction tasks or when threaded
         * reduction is disabled */
        switch (eee_task->args.task_type) {
        case UCC_EE_EXECUTOR_TASK_REDUCE:
            status = ucc_ec_cpu_reduce(&eee_task->args.reduce,
                                      eee_task->args.reduce.dst,
                                      (eee_task->args.flags &
                                       UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT) ?
                                          eee_task->args.reduce.srcs_ext :
                                          eee_task->args.reduce.srcs,
                                      eee_task->args.flags);
            break;
        case UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED:
            status = ucc_ec_cpu_reduce_strided(
                eee_task->args.reduce_strided.src1,
                eee_task->args.reduce_strided.src2,
                eee_task->args.reduce_strided.dst,
                eee_task->args.reduce_strided.n_src2 + 1,
                eee_task->args.reduce_strided.count,
                eee_task->args.reduce_strided.stride,
                eee_task->args.reduce_strided.dt,
                eee_task->args.reduce_strided.op,
                eee_task->args.flags);
            break;
        case UCC_EE_EXECUTOR_TASK_COPY:
            memcpy(eee_task->args.copy.dst, eee_task->args.copy.src,
                   eee_task->args.copy.len);
            status = UCC_OK;
            break;
        default:
            status = UCC_ERR_NOT_SUPPORTED;
            break;
        }
    }

    if (status == UCC_OK) {
        *task = eee_task;
    } else {
        ucc_mpool_put(eee_task);
    }

    return status;
}

ucc_status_t ucc_cpu_executor_task_test(const ucc_ee_executor_task_t *task)
{
    return task->status;
}

ucc_status_t ucc_cpu_executor_task_finalize(ucc_ee_executor_task_t *task)
{
    ucc_mpool_put(task);
    return UCC_OK;
}

ucc_status_t ucc_cpu_executor_finalize(ucc_ee_executor_t *executor)
{
    ec_trace(&ucc_ec_cpu.super, "executor finalize, eee: %p", executor);
    ucc_mpool_put(executor);

    return UCC_OK;
}

ucc_ec_cpu_t ucc_ec_cpu = {
    .super.super.name                 = "cpu ec",
    .super.ref_cnt                    = 0,
    .super.type                       = UCC_EE_CPU_THREAD,
    .super.init                       = ucc_ec_cpu_init,
    .super.get_attr                   = ucc_ec_cpu_get_attr,
    .super.finalize                   = ucc_ec_cpu_finalize,
    .super.config_table =
        {
            .name   = "CPU execution component",
            .prefix = "EC_CPU_",
            .table  = ucc_ec_cpu_config_table,
            .size   = sizeof(ucc_ec_cpu_config_t),
        },
    .super.ops.create_event           = NULL,
    .super.ops.destroy_event          = NULL,
    .super.ops.event_post             = NULL,
    .super.ops.event_test             = NULL,
    .super.executor_ops.init          = ucc_cpu_executor_init,
    .super.executor_ops.start         = ucc_cpu_executor_start,
    .super.executor_ops.status        = ucc_cpu_executor_status,
    .super.executor_ops.stop          = ucc_cpu_executor_stop,
    .super.executor_ops.task_post     = ucc_cpu_executor_task_post,
    .super.executor_ops.task_test     = ucc_cpu_executor_task_test,
    .super.executor_ops.task_finalize = ucc_cpu_executor_task_finalize,
    .super.executor_ops.finalize      = ucc_cpu_executor_finalize,
};

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_ec_cpu.super.config_table,
                                &ucc_config_global_list);
