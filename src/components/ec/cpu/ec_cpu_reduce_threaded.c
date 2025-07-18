/**
 * Copyright (c) 2022-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ec_cpu_reduce_threaded.h"
#include "utils/ucc_math.h"
#include <pthread.h>
#include <sched.h>

typedef struct ucc_ec_cpu_reduce_thread_args {
    void * const * restrict srcs;
    void * restrict dst;
    ucc_reduction_op_t op;
    ucc_datatype_t dt;
    size_t count;
    int n_srcs;
    uint16_t flags;
    size_t start_idx;
    size_t end_idx;
    ucc_status_t status;
} ucc_ec_cpu_reduce_thread_args_t;

static void *ucc_ec_cpu_reduce_thread_worker(void *arg)
{
    ucc_ec_cpu_reduce_thread_args_t *thread_args = 
        (ucc_ec_cpu_reduce_thread_args_t *)arg;
    
    /* Call the existing reduction function with the chunk */
    thread_args->status = ucc_ec_cpu_reduce_chunk(
        thread_args->srcs, thread_args->dst, thread_args->op,
        thread_args->dt, thread_args->count, thread_args->n_srcs,
        thread_args->flags, thread_args->start_idx, thread_args->end_idx);
    
    return NULL;
}

ucc_status_t ucc_ec_cpu_reduce_threaded(ucc_eee_task_reduce_t *task,
                                        void * restrict dst,
                                        void * const * restrict srcs,
                                        uint16_t flags,
                                        int num_threads,
                                        size_t chunk_size)
{
    pthread_t *threads;
    ucc_ec_cpu_reduce_thread_args_t *thread_args;
    size_t chunk_count, remaining;
    int i, ret;
    ucc_status_t status = UCC_OK;
    
    if (num_threads <= 1 || task->count < chunk_size * 2) {
        /* Fall back to single-threaded reduction */
        return ucc_ec_cpu_reduce(task, dst, srcs, flags);
    }
    
    threads = ucc_malloc(num_threads * sizeof(pthread_t), "reduce_threads");
    thread_args = ucc_malloc(num_threads * sizeof(ucc_ec_cpu_reduce_thread_args_t),
                            "reduce_thread_args");
    
    if (!threads || !thread_args) {
        ucc_free(threads);
        ucc_free(thread_args);
        return UCC_ERR_NO_MEMORY;
    }
    
    chunk_count = task->count / num_threads;
    remaining = task->count % num_threads;
    
    for (i = 0; i < num_threads; i++) {
        thread_args[i].srcs = srcs;
        thread_args[i].dst = dst;
        thread_args[i].op = task->op;
        thread_args[i].dt = task->dt;
        thread_args[i].count = task->count;
        thread_args[i].n_srcs = task->n_srcs;
        thread_args[i].flags = flags;
        
        /* Calculate chunk boundaries */
        thread_args[i].start_idx = i * chunk_count + 
                                  (i < remaining ? i : remaining);
        thread_args[i].end_idx = thread_args[i].start_idx + chunk_count +
                                (i < remaining ? 1 : 0);
        
        ret = pthread_create(&threads[i], NULL, 
                           ucc_ec_cpu_reduce_thread_worker, &thread_args[i]);
        if (ret != 0) {
            /* Cleanup already created threads */
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j], NULL);
            }
            ucc_free(threads);
            ucc_free(thread_args);
            return UCC_ERR_NO_RESOURCE;
        }
    }
    
    /* Wait for all threads to complete */
    for (i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
        if (thread_args[i].status != UCC_OK) {
            status = thread_args[i].status;
        }
    }
    
    ucc_free(threads);
    ucc_free(thread_args);
    
    return status;
}

/* Chunk-based reduction for a specific range */
ucc_status_t ucc_ec_cpu_reduce_chunk(void * const * restrict srcs,
                                     void * restrict dst,
                                     ucc_reduction_op_t op,
                                     ucc_datatype_t dt,
                                     size_t count,
                                     int n_srcs,
                                     uint16_t flags,
                                     size_t start_idx,
                                     size_t end_idx)
{
    /* This would implement the actual reduction logic for a specific chunk */
    /* For now, we'll call the existing reduction with adjusted pointers */
    
    /* Adjust pointers to the chunk range */
    void * restrict chunk_dst = (char *)dst + start_idx * ucc_dt_size(dt);
    void * const * restrict chunk_srcs = srcs; /* Would need adjustment for chunked srcs */
    
    /* Create a temporary task for the chunk */
    ucc_eee_task_reduce_t chunk_task = {
        .count = end_idx - start_idx,
        .dt = dt,
        .op = op,
        .n_srcs = n_srcs,
        .dst = chunk_dst,
        .alpha = 1.0 /* Would need to handle alpha properly */
    };
    
    return ucc_ec_cpu_reduce(&chunk_task, chunk_dst, chunk_srcs, flags);
} 