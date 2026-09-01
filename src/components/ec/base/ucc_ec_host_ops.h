/**
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_EC_HOST_OPS_H_
#define UCC_EC_HOST_OPS_H_

#include "components/ec/base/ucc_ec_base.h"
#include <stdbool.h>

/*
 * Shared host-offload capability helpers, used by the CUDA and ROCm ECs to
 * decide whether a reduce task can run on the host CPU executor instead of
 * on the accelerator.
 */

/*
 * Total reduce buffer length in bytes (count * element size) for a
 * REDUCE or REDUCE_STRIDED task.
 */
static inline int
ucc_ec_host_total_reduce_len(const ucc_ee_executor_task_args_t *task_args)
{
    int             total_len = 0;
    ucc_datatype_t  dt;
    size_t          count;

    if (task_args->task_type == UCC_EE_EXECUTOR_TASK_REDUCE) {
        dt    = task_args->reduce.dt;
        count = task_args->reduce.count;
    } else {
        ucc_assert(task_args->task_type == UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED);
        dt    = task_args->reduce_strided.dt;
        count = task_args->reduce_strided.count;
    }
    total_len += count * ucc_dt_size(dt);

    return total_len;
}

/*
 * True if the reduce task's datatype is supported by the host reduce kernel
 * (i.e. is not one of the accelerator-only types).
 */
static inline bool
ucc_ec_host_dt_supported(const ucc_ee_executor_task_args_t *task_args)
{
    bool            result = false;
    ucc_datatype_t  dt;

    if (task_args->task_type == UCC_EE_EXECUTOR_TASK_REDUCE) {
        dt     = task_args->reduce.dt;
    } else {
        ucc_assert(task_args->task_type == UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED);
        dt     = task_args->reduce_strided.dt;
    }
    if (dt != UCC_DT_BFLOAT16        &&
        dt != UCC_DT_FLOAT16         &&
        dt != UCC_DT_FLOAT32_COMPLEX &&
        dt != UCC_DT_FLOAT64_COMPLEX) {
        result = true;
    }
    return result;
}

#endif
