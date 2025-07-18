/**
 * Copyright (c) 2022-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_EC_CPU_THREAD_POOL_H_
#define UCC_EC_CPU_THREAD_POOL_H_

#include "components/ec/base/ucc_ec_base.h"
#include "utils/ucc_datastruct.h"
#include "utils/ucc_spinlock.h"
#include <pthread.h>

#ifdef HAVE_EC_THREADED_REDUCE

/* Forward declarations */
typedef struct ucc_ec_cpu_config ucc_ec_cpu_config_t;

typedef struct ucc_ec_cpu_task {
    ucc_ee_executor_task_t *task;
    ucc_status_t            status;
    int                     completed;
    pthread_mutex_t         completion_mutex;
    pthread_cond_t          completion_cond;
    ucc_list_link_t         list_elem;
} ucc_ec_cpu_task_t;

typedef struct ucc_ec_cpu_thread_pool {
    ucc_ec_cpu_config_t *config;
    int                  num_workers;
    pthread_t           *worker_threads;
    ucc_list_link_t      task_queue;
    ucc_list_link_t      completed_tasks;
    ucc_spinlock_t       task_queue_lock;
    ucc_spinlock_t       completed_tasks_lock;
    int                  shutdown;
} ucc_ec_cpu_thread_pool_t;

ucc_status_t ucc_ec_cpu_thread_pool_init(ucc_ec_cpu_thread_pool_t *pool,
                                         ucc_ec_cpu_config_t      *config);

ucc_status_t ucc_ec_cpu_thread_pool_finalize(ucc_ec_cpu_thread_pool_t *pool);

ucc_status_t ucc_ec_cpu_thread_pool_post_task(ucc_ec_cpu_thread_pool_t *pool,
                                               ucc_ee_executor_task_t    *task);

ucc_status_t ucc_ec_cpu_thread_pool_wait_task(ucc_ec_cpu_thread_pool_t *pool,
                                               ucc_ee_executor_task_t    *task);

#endif /* HAVE_EC_THREADED_REDUCE */

#endif /* UCC_EC_CPU_THREAD_POOL_H_ */ 