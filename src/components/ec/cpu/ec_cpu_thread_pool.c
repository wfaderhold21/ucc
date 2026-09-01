/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ec_cpu.h"
#include "ec_cpu_thread_pool.h"
#include "utils/ucc_malloc.h"
#include <sched.h>
#include "utils/ucc_math.h"

typedef struct ucc_ec_cpu_pool_node {
    ucc_lf_queue_elem_t lf_elem; /* must be first: enqueued into the lf-queue */
    ucc_ee_executor_task_t *task;
} ucc_ec_cpu_pool_node_t;

static void ucc_ec_cpu_pool_node_obj_init(ucc_mpool_t *mp, void *obj,
                                          void *chunk)
{
    ucc_ec_cpu_pool_node_t *node = (ucc_ec_cpu_pool_node_t *)obj;

    ucc_lf_queue_init_elem(&node->lf_elem);
    node->task = NULL;
}

static ucc_mpool_ops_t ucc_ec_cpu_pool_node_ops = {
    .chunk_alloc   = ucc_mpool_hugetlb_malloc,
    .chunk_release = ucc_mpool_hugetlb_free,
    .obj_init      = ucc_ec_cpu_pool_node_obj_init,
    .obj_cleanup   = NULL,
};

/*
 * Worker main loop: drain the task queue, execute each task on this
 * thread, and park on the shared condvar when the queue is empty.
 * Completion is reported via the task's atomic status word (the caller
 * polls it through ucc_cpu_executor_task_test), so there is no
 * per-task condvar and no busy-wait.
 */
static void *ucc_ec_cpu_pool_worker(void *arg)
{
    ucc_ec_cpu_thread_pool_t *pool = (ucc_ec_cpu_thread_pool_t *)arg;

    /* Signal readiness to the starter, then enter the drain loop.
     *
     * Readiness is signaled on a dedicated condvar (cv_ready), separate
     * from the drain condvar (cv).  Sharing one condvar between the two
     * waiters is the anti-pattern: a drain-parked worker can consume a
     * readiness wakeup intended for start() and re-park, leaving the
     * starter stranded (a lost wakeup that plain signal cannot survive
     * because the two predicates differ).  cv_ready has exactly one
     * waiter class (the starter), so signal is correct and cheap. */
    pthread_mutex_lock(&pool->mu);
    pool->live++;
    pthread_cond_signal(&pool->cv_ready);
    pthread_mutex_unlock(&pool->mu);

    for (;;) {
        ucc_ee_executor_task_t *task;

        task = ucc_ec_cpu_thread_pool_dequeue(pool);
        if (task) {
            ucc_status_t status;

            /* Claimed: the slot is no longer queued.  "pending" counts
             * enqueued-but-not-claimed tasks, so it reflects queue depth
             * (not in-flight work) and idle workers can park while a
             * long task runs elsewhere. */
            atomic_fetch_sub_explicit(&pool->pending, 1, memory_order_release);
            status = ucc_ec_cpu_execute_task(&task->args);
            ucc_ec_cpu_task_set_status(task, status);
            continue;
        }
        /* Queue empty: park until the next enqueue (or stop) wakes us. */
        pthread_mutex_lock(&pool->mu);
        while (atomic_load_explicit(&pool->pending, memory_order_acquire) == 0 &&
               !pool->shutdown) {
            pthread_cond_wait(&pool->cv, &pool->mu);
        }
        if (pool->shutdown) {
            pthread_mutex_unlock(&pool->mu);
            break;
        }
        pthread_mutex_unlock(&pool->mu);
    }
    return NULL;
}

ucc_status_t ucc_ec_cpu_thread_pool_init(ucc_ec_cpu_thread_pool_t *pool,
                                         int n_workers, int max_tasks,
                                         const ucc_ec_cpu_thread_pool_pin_t *pin)
{
    ucc_status_t status;

    memset(pool, 0, sizeof(*pool));
    pool->n_workers = n_workers;
    pool->pin       = (pin != NULL) ? *pin : (ucc_ec_cpu_thread_pool_pin_t){0};
    pool->workers   = ucc_calloc(n_workers, sizeof(pthread_t), "ec cpu pool");
    if (!pool->workers) {
        return UCC_ERR_NO_MEMORY;
    }
    ucc_lf_queue_init(&pool->queue);
    status = ucc_mpool_init(&pool->pool_nodes, 0, sizeof(ucc_ec_cpu_pool_node_t),
                            0, UCC_CACHE_LINE_SIZE, 16, max_tasks,
                            &ucc_ec_cpu_pool_node_ops, UCC_THREAD_MULTIPLE,
                            "ec cpu pool nodes");
    if (status != UCC_OK) {
        ucc_free(pool->workers);
        pool->workers = NULL;
        return status;
    }
    pthread_mutex_init(&pool->mu, NULL);
    pthread_cond_init(&pool->cv, NULL);
    pthread_cond_init(&pool->cv_ready, NULL);
    atomic_init(&pool->pending, 0);
    return UCC_OK;
}

void ucc_ec_cpu_thread_pool_finalize(ucc_ec_cpu_thread_pool_t *pool)
{
    /* The queue must be drained before finalize (see stop()). */
    ucc_lf_queue_destroy(&pool->queue);
    ucc_mpool_cleanup(&pool->pool_nodes, 1);
    ucc_free(pool->workers);
    pthread_cond_destroy(&pool->cv);
    pthread_cond_destroy(&pool->cv_ready);
    pthread_mutex_destroy(&pool->mu);
}

ucc_status_t ucc_ec_cpu_thread_pool_start(ucc_ec_cpu_thread_pool_t *pool)
{
    int i;

    for (i = 0; i < pool->n_workers; i++) {
        if (pthread_create(&pool->workers[i], NULL, ucc_ec_cpu_pool_worker,
                           pool) != 0) {
            pool->n_workers = i; /* partial start */
            break;
        }
        /*
         * Pin right after create: the window where the unpinned worker
         * could run on another core (and pollute the NUMA locality of
         * later tasks) is as short as possible.  A failed bind only
         * warns; the worker keeps running unpinned.
         */
        if (pool->pin.enable) {
            cpu_set_t cpuset;
            int       cpu = pool->pin.start_cpu + i * pool->pin.stride;

            CPU_ZERO(&cpuset);
            CPU_SET(cpu, &cpuset);
            if (pthread_setaffinity_np(pool->workers[i], sizeof(cpu_set_t),
                                       &cpuset) != 0) {
                if (pool->ec) {
                    ec_warn(pool->ec, "failed to pin worker %d to CPU %d: %m",
                            i, cpu);
                }
            }
        }
    }

    /* Wait until every worker has signaled readiness (it parked with
     * live == n_workers under the mutex). */
    pthread_mutex_lock(&pool->mu);
    while (pool->live < pool->n_workers) {
        pthread_cond_wait(&pool->cv_ready, &pool->mu);
    }
    pthread_mutex_unlock(&pool->mu);
    return UCC_OK;
}

void ucc_ec_cpu_thread_pool_stop(ucc_ec_cpu_thread_pool_t *pool)
{
    pthread_mutex_lock(&pool->mu);
    pool->shutdown = 1;
    pthread_cond_broadcast(&pool->cv);
    pthread_mutex_unlock(&pool->mu);

    for (int i = 0; i < pool->n_workers; i++) {
        pthread_join(pool->workers[i], NULL);
    }
    pool->n_workers = 0;
}

ucc_status_t ucc_ec_cpu_thread_pool_enqueue(ucc_ec_cpu_thread_pool_t *pool,
                                            ucc_ee_executor_task_t *task)
{
    ucc_ec_cpu_pool_node_t *node = ucc_mpool_get(&pool->pool_nodes);

    if (ucc_unlikely(!node)) {
        return UCC_ERR_NO_MEMORY;
    }
    /* Hand off the task to the worker.  The slot CAS that delivers this node
     * is raw-asm (TSan-invisible), so use an explicit release-store here to
     * order the task->args copy the worker will read; the worker does the
     * matching acquire-load. */
    __atomic_store_n(&node->task, task, __ATOMIC_RELEASE);
    ucc_lf_queue_enqueue(&pool->queue, &node->lf_elem);
    atomic_fetch_add_explicit(&pool->pending, 1, memory_order_release);

    pthread_mutex_lock(&pool->mu);
    /* Wake one parked worker; if none is parked (all busy) the signal is
     * harmless and the task is picked up when a worker re-checks. */
    pthread_cond_signal(&pool->cv);
    pthread_mutex_unlock(&pool->mu);
    return UCC_OK;
}

ucc_ee_executor_task_t *
ucc_ec_cpu_thread_pool_dequeue(ucc_ec_cpu_thread_pool_t *pool)
{
    ucc_lf_queue_elem_t *elem;
    ucc_ec_cpu_pool_node_t *node;
    ucc_ee_executor_task_t *task;

    /*
     * The lf-queue is multi-consumer safe: the fast-path slots are claimed
     * via CAS and the overflow lists are guarded by per-pool locks, so any
     * pool worker may dequeue concurrently.
     */
    elem = ucc_lf_queue_dequeue(&pool->queue, 1);
    if (!elem) {
        return NULL;
    }
    node = ucc_container_of(elem, ucc_ec_cpu_pool_node_t, lf_elem);
    task = __atomic_load_n(&node->task, __ATOMIC_ACQUIRE);
    ucc_mpool_put(node);
    return task;
}

/*
 * Executes one executor task body on the calling thread.  The strided
 * reduce path needs up to (n_src2 + 1) source pointers; when that exceeds
 * the inline UCC_EE_EXECUTOR_NUM_BUFS array, an external array is
 * allocated for the duration of the call.
 */
ucc_status_t ucc_ec_cpu_execute_task(const ucc_ee_executor_task_args_t *args)
{
    ucc_status_t status = UCC_OK;

    switch (args->task_type) {
    case UCC_EE_EXECUTOR_TASK_REDUCE:
    {
        const ucc_eee_task_reduce_t *r = (const ucc_eee_task_reduce_t *)&args->reduce;
        void *const *srcs = (args->flags & UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT) ?
                                 (void *const *)r->srcs_ext :
                                 (void *const *)r->srcs;

        /*
         * One worker runs one task's SIMD reduce; the pool's N workers run
         * concurrently for parallelism.  ucc_ec_cpu_reduce_threaded (the
         * kernel pool) is process-singleton (shared job slots / batch
         * counters) and cannot be driven by concurrent workers.
         */
        status = ucc_ec_cpu_reduce((ucc_eee_task_reduce_t *)r, r->dst,
                                   (void *const *)srcs, args->flags);
    } break;
    case UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED:
    {
        const ucc_eee_task_reduce_strided_t *trs =
            (const ucc_eee_task_reduce_strided_t *)&args->reduce_strided;
        size_t  n_srcs = trs->n_src2 + 1;
        uint16_t flags = args->flags;
        void ** srcs;
        void   *ext    = NULL;
        ucc_eee_task_reduce_t tr;
        int     i;

        if (n_srcs <= UCC_EE_EXECUTOR_NUM_BUFS) {
            srcs = (void **)&tr.srcs[0];
        } else {
            ext  = ucc_malloc(n_srcs * sizeof(void *), "ec cpu strided srcs");
            if (ucc_unlikely(!ext)) {
                return UCC_ERR_NO_MEMORY;
            }
            srcs    = (void **)ext;
            flags  |= UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT;
        }
        srcs[0] = trs->src1;
        for (i = 0; i < (int)n_srcs - 1; i++) {
            srcs[i + 1] = PTR_OFFSET(trs->src2, trs->stride * i);
        }
        tr.count  = trs->count;
        tr.dt     = trs->dt;
        tr.op     = trs->op;
        tr.n_srcs = (uint16_t)n_srcs;
        tr.dst    = trs->dst;
        tr.alpha  = trs->alpha;

        status = ucc_ec_cpu_reduce(&tr, tr.dst, srcs, flags);
        if (ext) {
            ucc_free(ext);
        }
    } break;
    case UCC_EE_EXECUTOR_TASK_COPY:
        memcpy((void *)args->copy.dst, (const void *)args->copy.src,
               args->copy.len);
        break;
    case UCC_EE_EXECUTOR_TASK_REDUCE_MULTI_DST:
    case UCC_EE_EXECUTOR_TASK_COPY_MULTI:
    default:
        status = UCC_ERR_NOT_SUPPORTED;
        break;
    }
    return status;
}
