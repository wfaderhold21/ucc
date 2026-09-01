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

typedef struct ucc_ec_cpu_thread_pool {
    ucc_lf_queue_t queue;    /* MPSC/MPMC: producers enqueue, N workers dequeue */
    ucc_mpool_t    pool_nodes; /* queue nodes carrying executor tasks */
} ucc_ec_cpu_thread_pool_t;

ucc_status_t ucc_ec_cpu_thread_pool_init(ucc_ec_cpu_thread_pool_t *pool, int max_tasks);
void         ucc_ec_cpu_thread_pool_finalize(ucc_ec_cpu_thread_pool_t *pool);

/*
 * MPSC enqueue of an executor task; safe to call concurrently from any
 * number of producer threads.  Returns a reference to the internal queue
 * node that now owns the task (kept alive until the worker dequeues it).
 */
ucc_status_t ucc_ec_cpu_thread_pool_enqueue(ucc_ec_cpu_thread_pool_t *pool,
                                            ucc_ee_executor_task_t *task);

/*
 * Dequeue the next pending task (or NULL if the queue is empty).  The
 * underlying lf-queue is multi-consumer safe (CAS on the fast-path slots,
 * per-pool locks on the overflow lists), so any pool worker may call this
 * concurrently.
 */
ucc_ee_executor_task_t *ucc_ec_cpu_thread_pool_dequeue(ucc_ec_cpu_thread_pool_t *pool);

#endif
