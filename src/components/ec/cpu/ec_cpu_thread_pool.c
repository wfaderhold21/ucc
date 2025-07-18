/**
 * Copyright (c) 2022-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ec_cpu_thread_pool.h"
#include "ec_cpu.h"
#include "utils/ucc_malloc.h"
#include "utils/ucc_math.h"
#include "components/topo/ucc_topo.h"
#include "core/ucc_context.h"
#include <pthread.h>
#include <sched.h>
#ifdef HAVE_EC_THREADED_REDUCE
#include <sys/syscall.h>
#include <unistd.h>
#endif

/* Helper function to set CPU affinity for a thread */
static int set_thread_affinity(pthread_t thread, int cpu_id)
{
    cpu_set_t cpuset;

    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);

    return pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
}

/* Helper function to discover available CPU cores using UCC topology */
static int discover_available_cores(int *cores, int max_cores, int socket_id,
                                   int numa_id)
{
    int    nr_cpus = sysconf(_SC_NPROCESSORS_CONF);
    int    i, core_count = 0;
    FILE  *fptr;
    char   str[1024];
    int    tmp_socket;

    if (nr_cpus <= 0) {
        return 0;
    }

    for (i = 0; i < nr_cpus && core_count < max_cores; i++) {
        /* Check socket ID */
        if (socket_id >= 0) {
            sprintf(str,
                    "/sys/devices/system/cpu/cpu%d/topology/physical_package_id",
                    i);
            fptr = fopen(str, "r");
            if (!fptr) {
                continue;
            }
            if (fscanf(fptr, "%d", &tmp_socket) != 1 ||
                tmp_socket != socket_id) {
                fclose(fptr);
                continue;
            }
            fclose(fptr);
        }

        /* Check NUMA node */
        if (numa_id >= 0) {
            sprintf(str, "/sys/devices/system/cpu/cpu%d/node0", i);
            fptr = fopen(str, "r");
            if (!fptr) {
                continue;
            }
            fclose(fptr);
            /* For now, we'll assume CPU i belongs to NUMA node 0 if the file
             * exists. In a more sophisticated implementation, we'd read the
             * actual NUMA node ID */
        }

        /* If we get here, this CPU core matches our criteria */
        cores[core_count++] = i;
    }

    return core_count;
}

/* Helper function to get CPU core for thread pinning using topology */
static int get_cpu_core_for_thread(int thread_id, ucc_ec_cpu_config_t *config)
{
    static int available_cores[256];  /* Static array for available cores */
    static int num_cores = -1;       /* -1 means not initialized */
    static int current_core = 0;     /* Round-robin assignment */
    int        core;

    /* Initialize available cores on first call */
    if (num_cores == -1) {
        if (config->use_topology_pinning) {
            /* Use topology-based discovery */
            num_cores = discover_available_cores(available_cores, 256,
                                               config->pin_to_socket,
                                               config->pin_to_numa);
        } else {
            /* Use manual CPU ID approach */
            num_cores = 1;
            available_cores[0] = config->start_cpu_id;
        }

        if (num_cores == 0) {
            /* Fallback to manual approach */
            num_cores = 1;
            available_cores[0] = config->start_cpu_id;
        }
    }

    if (num_cores == 0) {
        return -1;  /* No cores available */
    }

    /* Round-robin assignment */
    core = available_cores[current_core % num_cores];

    current_core++;

    return core;
}

static void *ucc_ec_cpu_worker_thread(void *arg)
{
    ucc_ec_cpu_thread_pool_t *pool = (ucc_ec_cpu_thread_pool_t *)arg;
    ucc_ec_cpu_task_t        *cpu_task;

    while (!pool->shutdown) {
        ucc_spin_lock(&pool->task_queue_lock);

        if (ucc_list_is_empty(&pool->task_queue)) {
            ucc_spin_unlock(&pool->task_queue_lock);
            sched_yield();
            continue;
        }

        cpu_task = ucc_list_head(&pool->task_queue, ucc_ec_cpu_task_t,
                                 list_elem);
        ucc_list_del(&cpu_task->list_elem);
        ucc_spin_unlock(&pool->task_queue_lock);

        /* Execute the task */
        switch (cpu_task->task->args.task_type) {
        case UCC_EE_EXECUTOR_TASK_REDUCE:
            cpu_task->status = ucc_ec_cpu_reduce(
                (ucc_eee_task_reduce_t *)&cpu_task->task->args.reduce,
                cpu_task->task->args.reduce.dst,
                (cpu_task->task->args.flags &
                 UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT) ?
                    cpu_task->task->args.reduce.srcs_ext :
                    cpu_task->task->args.reduce.srcs,
                cpu_task->task->args.flags);
            break;
        case UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED:
            cpu_task->status = ucc_ec_cpu_reduce_strided(
                cpu_task->task->args.reduce_strided.src1,
                cpu_task->task->args.reduce_strided.src2,
                cpu_task->task->args.reduce_strided.dst,
                cpu_task->task->args.reduce_strided.n_src2,
                cpu_task->task->args.reduce_strided.count,
                cpu_task->task->args.reduce_strided.stride,
                cpu_task->task->args.reduce_strided.dt,
                cpu_task->task->args.reduce_strided.op,
                cpu_task->task->args.flags);
            break;
        case UCC_EE_EXECUTOR_TASK_COPY:
            memcpy(cpu_task->task->args.copy.dst,
                   cpu_task->task->args.copy.src,
                   cpu_task->task->args.copy.len);
            cpu_task->status = UCC_OK;
            break;
        default:
            cpu_task->status = UCC_ERR_NOT_SUPPORTED;
            break;
        }

        /* Mark task as completed */
        pthread_mutex_lock(&cpu_task->completion_mutex);
        cpu_task->completed = 1;
        pthread_cond_signal(&cpu_task->completion_cond);
        pthread_mutex_unlock(&cpu_task->completion_mutex);

        /* Move to completed list */
        ucc_spin_lock(&pool->completed_tasks_lock);
        ucc_list_add_tail(&pool->completed_tasks, &cpu_task->list_elem);
        ucc_spin_unlock(&pool->completed_tasks_lock);
    }

    return NULL;
}

ucc_status_t ucc_ec_cpu_thread_pool_init(ucc_ec_cpu_thread_pool_t *pool,
                                         ucc_ec_cpu_config_t      *config)
{
    int i, ret, cpu_id;

    pool->config      = config;
    pool->num_workers = config->exec_num_workers;
    pool->shutdown    = 0;

    ucc_list_head_init(&pool->task_queue);
    ucc_list_head_init(&pool->completed_tasks);
    ucc_spinlock_init(&pool->task_queue_lock, 0);
    ucc_spinlock_init(&pool->completed_tasks_lock, 0);

    pool->worker_threads = ucc_malloc(pool->num_workers * sizeof(pthread_t),
                                     "cpu_worker_threads");
    if (!pool->worker_threads) {
        return UCC_ERR_NO_MEMORY;
    }

    for (i = 0; i < pool->num_workers; i++) {
        ret = pthread_create(&pool->worker_threads[i], NULL,
                           ucc_ec_cpu_worker_thread, pool);
        if (ret != 0) {
            ucc_error("failed to create worker thread %d: %s", i,
                      strerror(ret));
            /* Clean up already created threads */
            for (int j = 0; j < i; j++) {
                pthread_join(pool->worker_threads[j], NULL);
            }
            ucc_free(pool->worker_threads);
            return UCC_ERR_NO_MEMORY;
        }

        /* Pin thread to CPU core if enabled */
        if (config->pin_threads) {
            cpu_id = get_cpu_core_for_thread(i, config);
            if (cpu_id >= 0) {
                ret = set_thread_affinity(pool->worker_threads[i], cpu_id);
                if (ret == 0) {
                    ec_info(&ucc_ec_cpu.super,
                            "Successfully pinned thread %d to CPU %d "
                            "(topology-based)", i, cpu_id);
                } else {
                    ec_warn(&ucc_ec_cpu.super,
                            "Failed to pin thread %d to CPU %d: %s", i,
                            cpu_id, strerror(ret));
                }
            } else {
                ec_warn(&ucc_ec_cpu.super,
                        "No suitable CPU core found for thread %d", i);
            }
        }
    }

    return UCC_OK;
}

ucc_status_t ucc_ec_cpu_thread_pool_finalize(ucc_ec_cpu_thread_pool_t *pool)
{
    int i;

    pool->shutdown = 1;

    /* Wait for all worker threads to finish */
    for (i = 0; i < pool->num_workers; i++) {
        pthread_join(pool->worker_threads[i], NULL);
    }

    ucc_free(pool->worker_threads);

    return UCC_OK;
}

ucc_status_t ucc_ec_cpu_thread_pool_post_task(ucc_ec_cpu_thread_pool_t *pool,
                                               ucc_ee_executor_task_t    *task)
{
    ucc_ec_cpu_task_t *cpu_task;

    cpu_task = ucc_malloc(sizeof(ucc_ec_cpu_task_t), "cpu_task");
    if (!cpu_task) {
        return UCC_ERR_NO_MEMORY;
    }

    cpu_task->task = task;
    cpu_task->completed = 0;
    pthread_mutex_init(&cpu_task->completion_mutex, NULL);
    pthread_cond_init(&cpu_task->completion_cond, NULL);

    ucc_spin_lock(&pool->task_queue_lock);
    ucc_list_add_tail(&pool->task_queue, &cpu_task->list_elem);
    ucc_spin_unlock(&pool->task_queue_lock);

    return UCC_OK;
}

ucc_status_t ucc_ec_cpu_thread_pool_wait_task(ucc_ec_cpu_thread_pool_t *pool,
                                               ucc_ee_executor_task_t    *task)
{
    ucc_ec_cpu_task_t *cpu_task;
    ucc_list_link_t   *elem;

    /* Find the task in the completed list */
    ucc_spin_lock(&pool->completed_tasks_lock);
    for (elem = pool->completed_tasks.next; elem != &pool->completed_tasks;
         elem = elem->next) {
        cpu_task = ucc_list_head(elem, ucc_ec_cpu_task_t, list_elem);
        if (cpu_task->task == task) {
            ucc_list_del(&cpu_task->list_elem);
            ucc_spin_unlock(&pool->completed_tasks_lock);

            /* Wait for completion */
            pthread_mutex_lock(&cpu_task->completion_mutex);
            while (!cpu_task->completed) {
                pthread_cond_wait(&cpu_task->completion_cond,
                                 &cpu_task->completion_mutex);
            }
            pthread_mutex_unlock(&cpu_task->completion_mutex);

            /* Clean up */
            pthread_mutex_destroy(&cpu_task->completion_mutex);
            pthread_cond_destroy(&cpu_task->completion_cond);
            ucc_free(cpu_task);

            return UCC_OK;
        }
    }
    ucc_spin_unlock(&pool->completed_tasks_lock);

    return UCC_ERR_NOT_FOUND;
} 