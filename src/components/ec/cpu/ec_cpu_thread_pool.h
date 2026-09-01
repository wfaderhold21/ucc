/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_EC_CPU_THREAD_POOL_H_
#define UCC_EC_CPU_THREAD_POOL_H_

#include "components/ec/base/ucc_ec_base.h"
#include "utils/ucc_lock_free_queue.h"
#include "utils/ucc_mpool.h"
#include <pthread.h>
#include <stdatomic.h>

/*
 * Optional worker pinning.  When "enable" is set, worker i is bound to
 * core (start_cpu + i * stride) after creation; a failed bind is a
 * warning, not an error (the worker runs unpinned).  Pass NULL for no
 * pinning.
 */
typedef struct ucc_ec_cpu_thread_pool_pin {
    int enable;
    int start_cpu;
    int stride;
} ucc_ec_cpu_thread_pool_pin_t;
/*
 * Executor-level worker pool for the CPU EC.  Distinct from the reduce
 * kernel pool in ec_cpu_reduce.c (which runs one caller-blocking batch):
 * these N resident workers drain a shared lock-free queue, execute one
 * executor task each, and report completion through the task's atomic
 * status word.  Idle workers park on one shared condvar (no spin-wait).
 */
typedef struct ucc_ec_cpu_thread_pool {
    /* MPSC/MPMC task queue + the node mpool carrying executor tasks */
    ucc_lf_queue_t  queue;
    ucc_mpool_t     pool_nodes;

    /* Workers */
    pthread_t       *workers;
    int             n_workers;
    int             live;           /* workers that have signaled ready   */
    int             shutdown;       /* set before stop broadcast           */
    pthread_mutex_t mu;             /* guards live/shutdown + condvars     */
    pthread_cond_t  cv;             /* workers park here when the queue is empty */
    pthread_cond_t  cv_ready;       /* start() waits here for readiness    */
    atomic_int      pending;        /* tasks enqueued but not yet claimed  */

    ucc_ec_cpu_thread_pool_pin_t pin; /* worker pinning (from init)        */

    /* Owning EC (set at init) for component-scoped log messages */
    ucc_ec_base_t *ec;
} ucc_ec_cpu_thread_pool_t;

/*
 * Executor task status is shared between the posting thread and the pool
 * workers; use these accessors (release-store / acquire-load) rather than
 * plain accesses so the handoff is data-race free.
 */
static inline void ucc_ec_cpu_task_set_status(ucc_ee_executor_task_t *task,
                                              ucc_status_t status)
{
    __atomic_store_n(&task->status, status, __ATOMIC_RELEASE);
}

static inline ucc_status_t ucc_ec_cpu_task_get_status(
    const ucc_ee_executor_task_t *task)
{
    return __atomic_load_n(&task->status, __ATOMIC_ACQUIRE);
}



ucc_status_t ucc_ec_cpu_thread_pool_init(ucc_ec_cpu_thread_pool_t *pool,
                                         int n_workers, int max_tasks,
                                         const ucc_ec_cpu_thread_pool_pin_t *pin);
void         ucc_ec_cpu_thread_pool_finalize(ucc_ec_cpu_thread_pool_t *pool);

/*
 * Start the worker threads (applying any pinning from init) and wait
 * until all of them have parked.  Must be called exactly once after
 * init, before any enqueue.
 */
ucc_status_t ucc_ec_cpu_thread_pool_start(ucc_ec_cpu_thread_pool_t *pool);

/*
 * Signal shutdown, wake all parked workers, and join them.  The caller
 * must have finished posting (all tasks drained or tasks left in the
 * queue are abandoned) before calling this.
 */
void ucc_ec_cpu_thread_pool_stop(ucc_ec_cpu_thread_pool_t *pool);

/*
 * MP-safe enqueue of an executor task.  The task's status must already
 * be UCC_INPROGRESS; it is left for the worker to set to the final
 * status.
 */
ucc_status_t ucc_ec_cpu_thread_pool_enqueue(ucc_ec_cpu_thread_pool_t *pool,
                                            ucc_ee_executor_task_t *task);

/*
 * Dequeue the next pending task (or NULL if the queue is empty).  The
 * underlying lf-queue is multi-consumer safe (CAS on the fast-path slots,
 * per-pool locks on the overflow lists), so any worker may call this
 * concurrently.  The caller (worker) is responsible for executing the
 * task and setting its final status.
 */
ucc_ee_executor_task_t *ucc_ec_cpu_thread_pool_dequeue(ucc_ec_cpu_thread_pool_t *pool);

/*
 * Run one executor task body (reduce / reduce_strided / copy) on the
 * calling thread and return its completion status without touching the
 * task's status word.  Shared by the synchronous executor path and the
 * pool workers.  REDUCE_MULTI_DST / COPY_MULTI return
 * UCC_ERR_NOT_SUPPORTED.
 */
ucc_status_t ucc_ec_cpu_execute_task(const ucc_ee_executor_task_args_t *args);

#endif
