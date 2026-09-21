/**
 * Copyright (c) 2022-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ec_cpu.h"
#include "utils/arch/cpu.h"
#include "utils/ucc_atomic.h"
#include "components/mc/ucc_mc.h"
#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>
#include <time.h>
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

    {"USE_SPLIT_REDUCE", "0",
     "Split a single reduce across REDUCE_NUM_THREADS threads in task_post "
     "(1 = split, 0 = reduce on one thread).  Takes precedence over "
     "USE_THREADED_REDUCE for reduce tasks: the two are alternative uses of "
     "the same cores, and splitting needs the posting thread to block",
     ucc_offsetof(ucc_ec_cpu_config_t, use_split_reduce),
     UCC_CONFIG_TYPE_BOOL},

    {"REDUCE_NUM_THREADS", "0",
     "Number of threads a split reduce is divided across (0 = auto).  "
     "Reduce streams n_srcs inputs plus one output, so it saturates memory "
     "bandwidth well before it saturates cores; more threads than the "
     "memory system can feed only adds handshake cost.  When "
     "REDUCE_THREADS_PER_MB is set this is the MAXIMUM thread count",
     ucc_offsetof(ucc_ec_cpu_config_t, reduce_num_threads),
     UCC_CONFIG_TYPE_ULUNITS},

    {"REDUCE_THREADS_PER_MB", "1",
     "Scale split-reduce breadth with message size instead of a fixed "
     "width: dispatch ceil(count*dt_size / (REDUCE_THREADS_PER_MB MiB)) "
     "threads for a reduce, capped at REDUCE_NUM_THREADS (default 8, the "
     "persistent worker-pool depth).  This is the default: it keeps "
     "small-message split overhead near zero (few threads) while "
     "capturing the large-message win.  Set 0 to use a fixed width "
     "(REDUCE_NUM_THREADS, or auto).  Idle workers stay parked in the "
     "pool, so small messages spawn fewer threads",
     ucc_offsetof(ucc_ec_cpu_config_t, reduce_threads_per_mb),
     UCC_CONFIG_TYPE_ULUNITS},

    {"REDUCE_SPLIT_THRESH", "4194304",
     "Minimum reduce size (in elements) worth splitting across threads.  "
     "Measured crossover on a 2x60-core Xeon 8580: a 1.6M-element reduce is "
     "2.4x slower split, a 6.3M-element one is 1.3x faster, so the cost is "
     "cross-core data movement rather than the batch handshake",
     ucc_offsetof(ucc_ec_cpu_config_t, reduce_split_thresh),
     UCC_CONFIG_TYPE_ULUNITS},

    {"PIN_THREADS", "0",
     "Pin worker threads to CPU cores for NUMA locality "
     "(1 = pin, 0 = leave to the scheduler).  Applies to the threaded "
     "worker pool (PIN_START_CPU/PIN_STRIDE) and, socket-aware, to "
     "split-reduce workers (pinned to free cores on the caller's socket, "
     "never sharing a core with the caller or with each other)",
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

/* Per-process reduce routing counters (pool worker vs inline in task_post).
 * Mirrors ucc_ec_cuda_{host,gpu}_reduce_cnt in the CUDA component. */
static volatile uint64_t ucc_ec_cpu_pool_reduce_cnt   = 0;
static volatile uint64_t ucc_ec_cpu_inline_reduce_cnt = 0;
static volatile uint64_t ucc_ec_cpu_split_reduce_cnt  = 0;

/* Wall time and element volume of the reduces that ran synchronously in
 * task_post (inline and split).  Together with the benchmark's own
 * latency these answer "what share of a collective is CPU reduce?".
 * The pool path is async and completes on a worker, so it is not timed
 * here. */
static volatile uint64_t ucc_ec_cpu_sync_ns_total    = 0;
static volatile uint64_t ucc_ec_cpu_sync_elems_total = 0;

static inline uint64_t ucc_ec_cpu_now_ns(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

uint64_t ucc_ec_cpu_pool_reduce_count(void)
{
    /* volatile read is a single aligned 8-byte load on the supported
     * archs; increments go through ucc_atomic_add64. */
    return ucc_ec_cpu_pool_reduce_cnt;
}

uint64_t ucc_ec_cpu_inline_reduce_count(void)
{
    return ucc_ec_cpu_inline_reduce_cnt;
}

uint64_t ucc_ec_cpu_split_reduce_count(void)
{
    return ucc_ec_cpu_split_reduce_cnt;
}

uint64_t ucc_ec_cpu_sync_reduce_ns(void)
{
    return ucc_ec_cpu_sync_ns_total;
}

uint64_t ucc_ec_cpu_sync_reduce_elems(void)
{
    return ucc_ec_cpu_sync_elems_total;
}

/* Split width: REDUCE_NUM_THREADS, or one thread per online core. */
static int ucc_ec_cpu_split_threads(void)
{
    long n;

    if (EC_CPU_CONFIG->reduce_num_threads > 0) {
        return (int)EC_CPU_CONFIG->reduce_num_threads;
    }
    n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n <= 0) {
        return 1;
    }
    /* Capped at 4: on a 2x60-core Xeon 8580 a 6.3M-element reduce took
     * 100ms at 4 threads and 96ms at 8, so the fourth thread is already
     * near the memory-bandwidth ceiling.  Staying low matters at high
     * ppn, where every rank spawns its own split threads. */
    return (n > 4) ? 4 : (int)n;
}

/*
 * Split width for a reduce of "bytes" of payload.  When
 * REDUCE_THREADS_PER_MB is set, breadth scales with message size: one
 * thread per (per_mb MiB), capped at REDUCE_NUM_THREADS (default 8, the
 * persistent worker-pool depth).  Small messages get few threads, so the
 * batch handshake cost is only paid when it can repay itself.  Otherwise
 * falls back to the fixed / auto width.
 */
static int ucc_ec_cpu_split_threads_by_bytes(size_t bytes)
{
    long per_mb = EC_CPU_CONFIG->reduce_threads_per_mb;
    long t, max_t;

    if (per_mb <= 0) {
        return ucc_ec_cpu_split_threads();
    }
    max_t = (EC_CPU_CONFIG->reduce_num_threads > 0)
                ? (long)EC_CPU_CONFIG->reduce_num_threads
                : 8; /* equal to UCC_EC_CPU_REDUCE_MAX_WORKERS */
    /* ceil(bytes / (per_mb * MiB)), clamped to [1, max_t] */
    t = (long)((bytes + (per_mb * 1048576UL) - 1) / (per_mb * 1048576UL));
    if (t < 1) {
        t = 1;
    }
    if (t > max_t) {
        t = max_t;
    }
    return (int)t;
}

/* One reduce, split across ucc_ec_cpu_split_threads() threads.  The
 * splitter falls back to the single-threaded reduce on its own when the
 * count is too small or a batch is already in flight. */
static ucc_status_t ucc_ec_cpu_reduce_split(ucc_eee_task_reduce_t *tr,
                                            void *dst, void *const *srcs,
                                            uint16_t flags)
{
    size_t chunk   = EC_CPU_CONFIG->reduce_chunk_size;
    size_t bytes   = tr->count * ucc_dt_size(tr->dt);
    int    threads = ucc_ec_cpu_split_threads_by_bytes(bytes);

    /* Below the threshold the batch handshake dominates, so reduce here
     * rather than paying for a split that cannot repay it. */
    if (tr->count < EC_CPU_CONFIG->reduce_split_thresh ||
        threads <= 1 || tr->n_srcs > UCC_EE_EXECUTOR_NUM_BUFS) {
        ucc_atomic_add64(&ucc_ec_cpu_inline_reduce_cnt, 1);
        return ucc_ec_cpu_reduce(tr, dst, srcs, flags);
    }
    ucc_atomic_add64(&ucc_ec_cpu_split_reduce_cnt, 1);
    return ucc_ec_cpu_reduce_threaded(tr, dst, srcs, flags, threads, chunk);
}

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
    /* Splitting owns the reduce path when enabled: handing the task to a
     * single pool worker would forfeit the split, and the splitter needs
     * the posting thread to block. */
    if (EC_CPU_CONFIG->use_split_reduce) {
        switch (task_args->task_type) {
        case UCC_EE_EXECUTOR_TASK_REDUCE:
        case UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED:
            return 0;
        default:
            break;
        }
    }
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
    if (EC_CPU_CONFIG->use_split_reduce) {
        ec_info(&ucc_ec_cpu.super,
                "split reduce enabled: %d threads, chunk %lu elements",
                ucc_ec_cpu_split_threads(), EC_CPU_CONFIG->reduce_chunk_size);
    }
#else
    if (getenv("UCC_EC_CPU_USE_THREADED_REDUCE") ||
        getenv("UCC_EC_CPU_USE_SPLIT_REDUCE")) {
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
    /* Logged whether or not the pool ran: a zero/zero line distinguishes
     * "no reduce ever reached the CPU EC" from "reduces ran inline". */
    ec_info(&ucc_ec_cpu.super,
            "reduce routing: %" PRIu64 " pooled, %" PRIu64 " split, %"
            PRIu64 " inline; sync reduce %" PRIu64 " us over %" PRIu64
            " elements",
            ucc_ec_cpu_pool_reduce_count(),
            ucc_ec_cpu_split_reduce_count(),
            ucc_ec_cpu_inline_reduce_count(),
            ucc_ec_cpu_sync_reduce_ns() / 1000,
            ucc_ec_cpu_sync_reduce_elems());
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
#ifdef HAVE_EC_THREADED_REDUCE
    int is_reduce = (task_args->task_type == UCC_EE_EXECUTOR_TASK_REDUCE ||
                     task_args->task_type ==
                         UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED);
#endif

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
        if (is_reduce) {
            ucc_atomic_add64(&ucc_ec_cpu_pool_reduce_cnt, 1);
        }
        *task = eee_task;
        return UCC_OK;
    }
    /* Split reduces are counted in ucc_ec_cpu_reduce_split, which knows
     * whether the split actually applied. */
    if (is_reduce && !EC_CPU_CONFIG->use_split_reduce) {
        ucc_atomic_add64(&ucc_ec_cpu_inline_reduce_cnt, 1);
    }
#endif
    switch (task_args->task_type) {
    case UCC_EE_EXECUTOR_TASK_REDUCE:
    {
        ucc_eee_task_reduce_t *tr =
            (ucc_eee_task_reduce_t *)&task_args->reduce;
        void *const *rsrcs =
            (task_args->flags & UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT) ?
                task_args->reduce.srcs_ext : task_args->reduce.srcs;

#ifdef HAVE_EC_THREADED_REDUCE
        uint64_t t0 = ucc_ec_cpu_now_ns();

        if (EC_CPU_CONFIG->use_split_reduce) {
            status = ucc_ec_cpu_reduce_split(tr, tr->dst, rsrcs,
                                             task_args->flags);
        } else
#endif
        {
            status = ucc_ec_cpu_reduce(tr, tr->dst, rsrcs, task_args->flags);
        }
#ifdef HAVE_EC_THREADED_REDUCE
        ucc_atomic_add64(&ucc_ec_cpu_sync_ns_total,
                         ucc_ec_cpu_now_ns() - t0);
        ucc_atomic_add64(&ucc_ec_cpu_sync_elems_total, tr->count);
#endif
        if (ucc_unlikely(UCC_OK != status)) {
            goto free_task;
        }
    } break;
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

#ifdef HAVE_EC_THREADED_REDUCE
        uint64_t t0 = ucc_ec_cpu_now_ns();

        if (EC_CPU_CONFIG->use_split_reduce) {
            status = ucc_ec_cpu_reduce_split(&tr, tr.dst, srcs, flags);
        } else
#endif
        {
            status = ucc_ec_cpu_reduce(&tr, tr.dst, srcs, flags);
        }
#ifdef HAVE_EC_THREADED_REDUCE
        ucc_atomic_add64(&ucc_ec_cpu_sync_ns_total,
                         ucc_ec_cpu_now_ns() - t0);
        ucc_atomic_add64(&ucc_ec_cpu_sync_elems_total, tr.count);
#endif
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
#ifdef HAVE_EC_THREADED_REDUCE
    /* Pool path: workers release-store the final status. */
    return ucc_ec_cpu_task_get_status(task);
#else
    /* Sync path: status is written by the posting thread only. */
    return task->status;
#endif
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
