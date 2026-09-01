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

    {NULL}

};

#ifdef HAVE_EC_THREADED_REDUCE

/*
 * Executor-level worker pool: one pool per process (the CPU EC is a
 * process singleton), started lazily when the first executor task is
 * posted with UCC_USE_THREADED_REDUCE=1.  The config knobs
 * (EXEC_NUM_WORKERS / EXEC_MAX_TASKS) land with task 574; until then
 * the pool size is the env var or the hardware default.
 */
static ucc_ec_cpu_thread_pool_t ucc_ec_cpu_pool;
static int                      ucc_ec_cpu_pool_started;
static pthread_mutex_t          ucc_ec_cpu_pool_mu = PTHREAD_MUTEX_INITIALIZER;

static int ucc_ec_cpu_pool_env_num_workers(void)
{
    const char *env = getenv("UCC_EC_CPU_EXEC_NUM_WORKERS");
    int         n   = env ? atoi(env) : 0;

    if (n <= 0) {
        n = (int)sysconf(_SC_NPROCESSORS_ONLN);
    }
    return n;
}

static ucc_status_t ucc_ec_cpu_pool_ensure(void)
{
    static int         pool_inited;
    static ucc_status_t init_status = UCC_OK;
    ucc_status_t       status;
    int                n_workers;

    pthread_mutex_lock(&ucc_ec_cpu_pool_mu);
    if (!pool_inited) {
        n_workers = ucc_ec_cpu_pool_env_num_workers();
        status    = ucc_ec_cpu_thread_pool_init(&ucc_ec_cpu_pool, n_workers,
                                                1024);
        if (status == UCC_OK) {
            status = ucc_ec_cpu_thread_pool_start(&ucc_ec_cpu_pool);
            if (status == UCC_OK) {
                pool_inited             = 1;
                init_status             = UCC_OK;
                ucc_ec_cpu_pool_started = 1;
                ec_info(&ucc_ec_cpu.super,
                        "threaded reduce pool started with %d workers",
                        n_workers);
            } else {
                /* start failed mid-way (partial workers); the pool is
                 * fully initialized, so stop + finalize is valid. */
                ucc_ec_cpu_thread_pool_stop(&ucc_ec_cpu_pool);
                ucc_ec_cpu_thread_pool_finalize(&ucc_ec_cpu_pool);
                ec_error(&ucc_ec_cpu.super,
                         "failed to start threaded reduce pool: %s",
                         ucc_status_string(status));
            }
        } else {
            /* init failed before the pool was fully constructed: do not
             * touch the half-zeroed pool (it is never started, so the
             * teardown path is not reached either). */
            ec_error(&ucc_ec_cpu.super,
                     "failed to init threaded reduce pool: %s",
                     ucc_status_string(status));
        }
    }
    status = init_status;
    pthread_mutex_unlock(&ucc_ec_cpu_pool_mu);
    return status;
}

static void ucc_ec_cpu_pool_teardown(void)
{
    if (ucc_ec_cpu_pool_started) {
        ucc_ec_cpu_thread_pool_stop(&ucc_ec_cpu_pool);
        ucc_ec_cpu_thread_pool_finalize(&ucc_ec_cpu_pool);
        ucc_ec_cpu_pool_started = 0;
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
    ucc_ec_cpu_pool_teardown();
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
    if (getenv("UCC_USE_THREADED_REDUCE")) {
        /*
         * Async: copy the args into the pooled task, mark it in-progress
         * and hand it to a pool worker; task_test() then polls the
         * worker's release-stored status (mirrors the CUDA interruptible
         * executor).  task_finalize() must be called after task_test()
         * returns the final status, exactly as on the synchronous path.
         */
        ucc_ec_cpu_task_set_status(eee_task, UCC_INPROGRESS);
        memcpy(&eee_task->args, task_args, sizeof(*task_args));
        status = ucc_ec_cpu_pool_ensure();
        if (ucc_unlikely(status != UCC_OK)) {
            goto free_task;
        }
        status = ucc_ec_cpu_thread_pool_enqueue(&ucc_ec_cpu_pool, eee_task);
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
