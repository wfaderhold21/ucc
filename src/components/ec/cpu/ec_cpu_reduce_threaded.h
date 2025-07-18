/**
 * Copyright (c) 2022-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_EC_CPU_REDUCE_THREADED_H_
#define UCC_EC_CPU_REDUCE_THREADED_H_

#include "ec_cpu.h"

ucc_status_t ucc_ec_cpu_reduce_threaded(ucc_eee_task_reduce_t *task,
                                        void * restrict dst,
                                        void * const * restrict srcs,
                                        uint16_t flags,
                                        int num_threads,
                                        size_t chunk_size);

ucc_status_t ucc_ec_cpu_reduce_chunk(void * const * restrict srcs,
                                     void * restrict dst,
                                     ucc_reduction_op_t op,
                                     ucc_datatype_t dt,
                                     size_t count,
                                     int n_srcs,
                                     uint16_t flags,
                                     size_t start_idx,
                                     size_t end_idx);

#endif /* UCC_EC_CPU_REDUCE_THREADED_H_ */ 