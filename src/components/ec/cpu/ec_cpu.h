/**
 * Copyright (c) 2022-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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
    unsigned long exec_num_workers;    /* Number of worker threads */
    unsigned long exec_max_tasks;      /* Maximum number of outstanding tasks */
    unsigned long reduce_chunk_size;   /* Chunk size for threaded reductions */
    int          use_threaded_reduce;  /* Enable threaded reductions */
    int          pin_threads;          /* Pin worker threads to CPU cores */
    int          use_topology_pinning; /* Use UCC topology for thread pinning */
    int          pin_to_socket;        /* Pin threads to specific socket */
    int          pin_to_numa;          /* Pin threads to specific NUMA node */
    int          start_cpu_id;         /* Starting CPU ID for thread pinning */
#endif
} ucc_ec_cpu_config_t;

typedef struct ucc_ec_cpu {
    ucc_ec_base_t     super;
    ucc_thread_mode_t thread_mode;
    ucc_mpool_t       executors;
    ucc_mpool_t       executor_tasks;
    ucc_spinlock_t    init_spinlock;
#ifdef HAVE_EC_THREADED_REDUCE
    /* Thread pool for CPU execution */
    ucc_ec_cpu_thread_pool_t thread_pool;
#endif
} ucc_ec_cpu_t;

extern ucc_ec_cpu_t ucc_ec_cpu;

#define EC_CPU_CONFIG                                                         \
    (ucc_derived_of(ucc_ec_cpu.super.config, ucc_ec_cpu_config_t))

/* Function declarations */
ucc_status_t ucc_ec_cpu_reduce(ucc_eee_task_reduce_t *task, void * restrict dst,
                               void * const * restrict srcs, uint16_t flags);

ucc_status_t ucc_ec_cpu_reduce_strided(void *src1, void *src2, void *dst,
                                       size_t n_vectors, size_t count,
                                       size_t stride, ucc_datatype_t dt,
                                       ucc_reduction_op_t op, uint16_t flags);

#endif /* UCC_EC_CPU_H_ */
