/**
 * Copyright (c) 2022-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ec_cpu.h"
#include "utils/arch/cpu.h"
#include "components/mc/ucc_mc.h"
#include <limits.h>
#include <stdlib.h>
#include <unistd.h>

#ifdef HAVE_EC_THREADED_REDUCE
#include "ec_cpu_thread_pool.h"
#endif
static ucc_config_field_t ucc_ec_cpu_config_table[] = {
    {"", "", NULL, ucc_offsetof(ucc_ec_cpu_config_t, super),
     UCC_CONFIG_TYPE_TABLE(ucc_ec_config_table)},

#ifdef HAVE_EC_THREADED_REDUCE
    {"EXEC_NUM_WORKERS", "4",
     "Number of executor worker threads (pool size) for threaded reduce",
     ucc_offsetof(ucc_ec_cpu_config_t, exec_num_workers),
     UCC_CONFIG_TYPE_ULUNITS},

    {"EXEC_MAX_TASKS", "1024",
     "Maximum number of outstanding executor tasks in the worker pool",
     ucc_offsetof(ucc_ec_cpu_config_t, exec_max_tasks),
     UCC_CONFIG_TYPE_ULUNITS},

    {"REDUCE_CHUNK_SIZE", "1024",
     "Chunk size (in elements) at which a reduce is split across threads",
     ucc_offsetof(ucc_ec_cpu_config_t, reduce_chunk_size),
     UCC_CONFIG_TYPE_ULUNITS},

    {"USE_THREADED_REDUCE", "0",
     "Asynchronously post executor tasks onto the CPU worker pool "
     "(1 = threaded, 0 = run synchronously in task_post)",
     ucc_offsetof(ucc_ec_cpu_config_t, use_threaded_reduce),
     UCC_CONFIG_TYPE_BOOL},

    {"PIN_THREADS", "0",
     "Pin worker threads to CPU cores for NUMA locality "
     "(1 = pin, 0 = leave to the scheduler)",
     ucc_offsetof(ucc_ec_cpu_config_t, pin_threads),
     UCC_CONFIG_TYPE_BOOL},

    {"PIN_START_CPU", "0",
     "First CPU core for thread pinning (worker i -> start + i*stride)",
     ucc_offsetof(ucc_ec_cpu_config_t, pin_start_cpu),
     UCC_CONFIG_TYPE_INT},

    {"PIN_STRIDE", "1",
     "CPU core stride for thread pinning (1 = contiguous cores)",
     ucc_offsetof(ucc_ec_cpu_config_t, pin_stride),
     UCC_CONFIG_TYPE_INT},
#endif

    {NULL}

};

#ifdef HAVE_EC_THREADED_REDUCE

/*
 * Executor-level worker pool.  The CPU EC is a process singleton, so
 * there is one pool per process: it is created and started in
 * ucc_ec_cpu_init() (when USE_THREADED_REDUCE is set) and stopped in
 * ucc_ec_cpu_finalize().
 *
 * Thread-mode note: under UCC_THREAD_SINGLE / UCC_THREAD_FUNNELED the
 * caller still posts and tests from its own thread; the pool only
 * provides the worker that executes the posted task, so those modes
 * are safe to use with threaded reduce (the posting thread is the
 * single progress thread).
 */
static int ucc_ec_cpu_pool_active = 0;

static ucc_status_t ucc_ec_cpu_pool_start(void)
{
    ucc_ec_cpu_config_t            *cfg  = EC_CPU_CONFIG;
    ucc_ec_cpu_thread_pool_t       *pool = &ucc_ec_cpu.thread_pool;
    ucc_ec_cpu_thread_pool_pin_t    pin;
    ucc_status_t                    status;
    int                             n_workers;

    n_workers = (int)cfg->exec_num_workers;
    if (n_workers <= 0) {
        n_workers = (int)sysconf(_SC_NPROCESSORS_ONLN);
        if (n_workers <= 0) {
            n_workers = 1;
        }
    }
    memset(&pin, 0, sizeof(pin));
    pin.enable    = cfg->pin_threads;
    pin.start_cpu = cfg->pin_start_cpu;
    pin.stride    = (cfg->pin_stride > 0) ? cfg->pin_stride : 1;

    pool->ec = &ucc_ec_cpu.super;
    status   = ucc_ec_cpu_thread_pool_init(pool, n_workers,
                                          (int)cfg->exec_max_tasks, &pin);
    if (status != UCC_OK) {
        ec_error(&ucc_ec_cpu.super,
                 "failed to init threaded reduce pool: %s",
                 ucc_status_string(status));
        return status;
    }
    status = ucc_ec_cpu_thread_pool_start(pool);
    if (status != UCC_OK) {
        /* start failed mid-way (partial workers); the pool is fully
         * initialized, so finalize is valid. */
        ucc_ec_cpu_thread_pool_finalize(pool);
        ec_error(&ucc_ec_cpu.super,
                 "failed to start threaded reduce pool: %s",
                 ucc_status_string(status));
        return status;
    }
    ucc_ec_cpu_pool_active = 1;
    ec_info(&ucc_ec_cpu.super,
            "threaded reduce pool started: %d workers, max %lu tasks%s",
            n_workers, cfg->exec_max_tasks,
            pin.enable ? " (pinned)" : "");
    return UCC_OK;
}
static void ucc_ec_cpu_pool_stop(void)
{
    if (!ucc_ec_cpu_pool_active) {
        return;
    }
    ucc_ec_cpu_thread_pool_stop(&ucc_ec_cpu.thread_pool);
    ucc_ec_cpu_thread_pool_finalize(&ucc_ec_cpu.thread_pool);
    ucc_ec_cpu_pool_active = 0;
}

/*
 * With threaded reduce on, a reduce below REDUCE_CHUNK_SIZE elements is
 * still run synchronously in task_post: the enqueue / worker-wake /
 * status-handoff round trip (~10-20 us) costs more than the reduction
 * itself at small sizes.  Copies always go to the pool.
 */
static int ucc_ec_cpu_task_goes_to_pool(const ucc_ee_executor_task_args_t *task_args)
{
    switch (task_args->task_type) {
    case UCC_EE_EXECUTOR_TASK_REDUCE:
        return task_args->reduce.count >= EC_CPU_CONFIG->reduce_chunk_size;
    case UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED:
        return task_args->reduce_strided.count >= EC_CPU_CONFIG->reduce_chunk_size;
    case UCC_EE_EXECUTOR_TASK_COPY:
        return 1;
    default:
        /* unsupported: fall through to the synchronous switch, which
         * rejects it with UCC_ERR_NOT_SUPPORTED at post time. */
        return 0;
    }
}


#endif /* HAVE_EC_THREADED_REDUCE */

static ucc_status_t ucc_ec_cpu_init(const ucc_ec_params_t *ec_params)
{
    ucc_status_t status;

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

#ifdef HAVE_EC_THREADED_REDUCE
    if (EC_CPU_CONFIG->use_threaded_reduce) {
        status = ucc_ec_cpu_pool_start();
        if (status != UCC_OK) {
            ucc_mpool_cleanup(&ucc_ec_cpu.executor_tasks, 1);
            ucc_mpool_cleanup(&ucc_ec_cpu.executors, 1);
            return status;
        }
    } else {
        ec_debug(&ucc_ec_cpu.super, "threaded reduce disabled (USE_THREADED_REDUCE=0)");
    }
#else
    if (getenv("UCC_EC_CPU_USE_THREADED_REDUCE")) {
        ec_warn(&ucc_ec_cpu.super,
                "threaded reduce requested but not compiled in "
                "(rebuild with --enable-ec-threaded-reduce)");
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
    /*
     * EC finalize: all tasks must have been tested/finalized by the
     * caller (executor tasks are owned by the user until
     * task_finalize), so the queue is empty when we stop the workers.
     */
#ifdef HAVE_EC_THREADED_REDUCE
    ucc_ec_cpu_pool_stop();
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
    ucc_ee_executor_task_t *eee_task;

    eee_task = ucc_mpool_get(&ucc_ec_cpu.executor_tasks);
    if (ucc_unlikely(!eee_task)) {
        return UCC_ERR_NO_MEMORY;
    }

    eee_task->eee = executor;
#ifdef HAVE_EC_THREADED_REDUCE
    if (ucc_ec_cpu_pool_active && ucc_ec_cpu_task_goes_to_pool(task_args)) {
        /*
         * Async (USE_THREADED_REDUCE=1): copy the args into the pooled
         * task, mark it in-progress and hand it to a pool worker;
         * task_test() then polls the worker's release-stored status
         * (mirrors the CUDA interruptible executor).  task_finalize()
         * must be called after task_test() returns the final status,
         * exactly as on the synchronous path.  Reduces below
         * REDUCE_CHUNK_SIZE elements bypass the pool (synchronous) —
         * the handoff overhead exceeds the reduction at small sizes.
         */
        ucc_ec_cpu_task_set_status(eee_task, UCC_INPROGRESS);
        memcpy(&eee_task->args, task_args, sizeof(*task_args));
        status = ucc_ec_cpu_thread_pool_enqueue(&ucc_ec_cpu.thread_pool,
                                                eee_task);
        if (ucc_unlikely(status != UCC_OK)) {
            goto free_task;
        }
        *task = eee_task;
        return UCC_OK;
    }
#endif
    switch (task_args->task_type) {
    case UCC_EE_EXECUTOR_TASK_REDUCE:
        status = ucc_ec_cpu_reduce((ucc_eee_task_reduce_t *)&task_args->reduce, task_args->reduce.dst,
                                   (task_args->flags &
                                        UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT) ?
                                        task_args->reduce.srcs_ext :
                                        task_args->reduce.srcs,
                                    task_args->flags);
        if (ucc_unlikely(UCC_OK != status)) {
            goto free_task;
        }
        break;
    case UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED:
    {
        ucc_eee_task_reduce_strided_t *trs =
            (ucc_eee_task_reduce_strided_t *)&task_args->reduce_strided;
        size_t                n_srcs = trs->n_src2 + 1;
        uint16_t              flags  = task_args->flags;
        void **               srcs;
        ucc_eee_task_reduce_t tr;
        int                   i;

        if (n_srcs <= UCC_EE_EXECUTOR_NUM_BUFS) {
            srcs = &tr.srcs[0];
        } else {
            srcs = alloca(n_srcs * sizeof(void *));
            flags |= UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT;
            tr.srcs_ext = srcs;
        }
        srcs[0] = trs->src1;
        for (i = 0; i < n_srcs - 1; i++) {
            srcs[i + 1] = PTR_OFFSET(trs->src2, trs->stride * i);
        }
        tr.count  = trs->count;
        tr.dt     = trs->dt;
        tr.op     = trs->op;
        tr.n_srcs = n_srcs;
        tr.dst    = trs->dst;
        tr.alpha  = trs->alpha;

        status = ucc_ec_cpu_reduce(&tr, tr.dst, srcs, flags);
        if (ucc_unlikely(UCC_OK != status)) {
            goto free_task;
        }
    } break;
    case UCC_EE_EXECUTOR_TASK_COPY:
        memcpy(task_args->copy.dst, task_args->copy.src, task_args->copy.len);
        break;
    case UCC_EE_EXECUTOR_TASK_COPY_MULTI:
    default:
        status = UCC_ERR_NOT_SUPPORTED;
        goto free_task;
    }
    eee_task->status = status;
    *task = eee_task;

    return status;

free_task:
    ucc_mpool_put(eee_task);
    return status;
}

ucc_status_t ucc_cpu_executor_task_test(const ucc_ee_executor_task_t *task)
{
    return ucc_ec_cpu_task_get_status(task);
}

ucc_status_t ucc_cpu_executor_task_finalize(ucc_ee_executor_task_t *task)
{
    /* The task body ran either synchronously (above) or on a pool
     * worker (threaded mode); either way it is done and the pooled
     * task is ours again. */
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
