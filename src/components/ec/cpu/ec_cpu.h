/**
 * Copyright (c) 2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_EC_CPU_H_
#define UCC_EC_CPU_H_

#include "components/ec/base/ucc_ec_base.h"
#include "components/ec/ucc_ec_log.h"
#include "utils/ucc_mpool.h"

#ifdef HAVE_EC_THREADED_REDUCE
#include "ec_cpu_thread_pool.h"
#endif

typedef struct ucc_ec_cpu_config {
    ucc_ec_config_t super;
#ifdef HAVE_EC_THREADED_REDUCE
    /*
     * Threaded-reduce (async executor) configuration.  Parsed from the
     * UCC_EC_CPU_* environment / config file by ucc_ec_init(); consumed
     * in ucc_ec_cpu_init() (pool lifecycle) and ucc_cpu_executor_task_post
     * (threaded vs synchronous dispatch).
     */
    unsigned long exec_num_workers;   /* pool worker count               */
    unsigned long exec_max_tasks;     /* pool queue capacity (nodes)     */
    unsigned long reduce_chunk_size;  /* chunk split size for threading  */
    unsigned long reduce_num_threads; /* split width; 0 = auto (nproc)   */
    unsigned long reduce_threads_per_mb; /* 0=off, else 1 thr per MiB(cap) */
    unsigned long reduce_split_thresh; /* min elements worth splitting   */
    int           use_threaded_reduce; /* 1 = async post via worker pool */
    int           use_split_reduce;   /* 1 = split one reduce across threads */
    int           pin_threads;        /* pin workers (pool + split)       */
    int           pin_start_cpu;      /* first core for pinning          */
    int           pin_stride;         /* core stride: start + i*stride   */
#endif
} ucc_ec_cpu_config_t;

typedef struct ucc_ec_cpu {
    ucc_ec_base_t     super;
    ucc_thread_mode_t thread_mode;
    ucc_mpool_t       executors;
    ucc_mpool_t       executor_tasks;
    ucc_spinlock_t    init_spinlock;
#ifdef HAVE_EC_THREADED_REDUCE
    /*
     * Executor-level worker pool (process singleton; the CPU EC is a
     * process singleton).  Created in ucc_ec_cpu_init() when
     * USE_THREADED_REDUCE is set, torn down in ucc_ec_cpu_finalize().
     */
    ucc_ec_cpu_thread_pool_t thread_pool;
#endif
} ucc_ec_cpu_t;

extern ucc_ec_cpu_t ucc_ec_cpu;

#ifdef HAVE_EC_THREADED_REDUCE
#define EC_CPU_CONFIG                                                         \
    (ucc_derived_of(ucc_ec_cpu.super.config, ucc_ec_cpu_config_t))

/* Per-process reduce routing counters: reduce tasks handed to a pool
 * worker vs run inline in ucc_cpu_executor_task_post.  Both are also
 * logged at ec_info level from ucc_ec_cpu_finalize. */
uint64_t ucc_ec_cpu_pool_reduce_count(void);
uint64_t ucc_ec_cpu_inline_reduce_count(void);
uint64_t ucc_ec_cpu_split_reduce_count(void);

/* Effective worker ceiling for split-reduce: the core socket budget
 * (ucc_host_get_split_socket_budget) if set, else the pool max. */
int ucc_ec_cpu_split_socket_max(void);

/* Maximum worker threads the split-reduce pool may hold per rank. */
#define UCC_EC_CPU_REDUCE_MAX_WORKERS 8

/* Wall time / element volume of reduces run synchronously in task_post. */
uint64_t ucc_ec_cpu_sync_reduce_ns(void);
uint64_t ucc_ec_cpu_sync_reduce_elems(void);
#endif

ucc_status_t ucc_ec_cpu_reduce(ucc_eee_task_reduce_t *task, void * restrict dst, void * const * restrict srcs, uint16_t flags);

/*
 * Reduces the element range [start_idx, end_idx) of the "n_srcs" source
 * buffers (each "count" elements of "dt") into the corresponding range
 * of "dst"; see ucc_ec_cpu_reduce_chunk in ec_cpu_reduce.c.
 */
ucc_status_t ucc_ec_cpu_reduce_chunk(void * const * restrict srcs,
                                     void * restrict dst,
                                     ucc_reduction_op_t op,
                                     ucc_datatype_t dt,
                                     size_t count,
                                     uint16_t n_srcs,
                                     uint16_t flags,
                                     size_t start_idx,
                                     size_t end_idx,
                                     double alpha);

/*
 * Splits the reduction across "num_threads" threads, each reducing a
 * contiguous chunk via ucc_ec_cpu_reduce_chunk (see ec_cpu_reduce.c).
 * Falls back to ucc_ec_cpu_reduce when threading is not worthwhile
 * (num_threads <= 1 or count < 2*chunk_size).
 */
ucc_status_t ucc_ec_cpu_reduce_threaded(ucc_eee_task_reduce_t *task,
                                        void * restrict dst,
                                        void * const * restrict srcs,
                                        uint16_t flags,
                                        int num_threads,
                                        size_t chunk_size);
#endif
