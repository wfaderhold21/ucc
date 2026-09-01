/**
 * Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_EC_CUDA_H_
#define UCC_EC_CUDA_H_

#include "components/ec/base/ucc_ec_base.h"
#include "components/ec/ucc_ec_log.h"
#include "utils/arch/cuda_def.h"
#include "utils/ucc_mpool.h"
#include "ec_cuda_resources.h"
#include <cuda_runtime.h>
#include <stdbool.h>

typedef ucc_status_t (*ucc_ec_cuda_task_post_fn) (uint32_t *dev_status,
                                                  int blocking_wait,
                                                  cudaStream_t stream);

typedef struct ucc_ec_cuda {
    ucc_ec_base_t                  super;
    int                            exec_streams_initialized;
    ucc_ec_cuda_resources_hash_t  *resources_hash;
    ucc_thread_mode_t              thread_mode;
    ucc_ec_cuda_strm_task_mode_t   strm_task_mode;
    ucc_spinlock_t                 init_spinlock;
    ucc_ee_executor_t             *cpu_executor;
} ucc_ec_cuda_t;

typedef struct ucc_ec_cuda_stream_request {
    uint32_t            status;
    uint32_t           *dev_status;
    cudaStream_t        stream;
} ucc_ec_cuda_stream_request_t;

ucc_status_t ucc_ec_cuda_event_create(void **event);

ucc_status_t ucc_ec_cuda_event_destroy(void *event);

ucc_status_t ucc_ec_cuda_event_post(void *ee_context, void *event);

ucc_status_t ucc_ec_cuda_event_test(void *event);

ucc_status_t ucc_ec_cuda_get_resources(ucc_ec_cuda_resources_t **resources);

/* Host-offload policy for REDUCE/REDUCE_STRIDED: true when the task is
 * small enough (total bytes <= REDUCE_HOST_LIMIT), its datatype is
 * host-supported, and USE_HOST_REDUCE is enabled.  Mirrors
 * ec_rocm_use_host_ops() in the ROCm component. */
bool ec_cuda_use_host_ops(const ucc_ee_executor_task_args_t *task_args);

/* Per-process host-offload routing counters (host vs GPU reduce tasks). */
uint64_t ucc_ec_cuda_host_reduce_count(void);
uint64_t ucc_ec_cuda_gpu_reduce_count(void);

extern ucc_ec_cuda_t ucc_ec_cuda;

#define EC_CUDA_CONFIG                                                         \
    (ucc_derived_of(ucc_ec_cuda.super.config, ucc_ec_cuda_config_t))

#endif
