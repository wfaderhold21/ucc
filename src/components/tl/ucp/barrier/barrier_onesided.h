/**
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_TL_UCP_BARRIER_ONESIDED_H_
#define UCC_TL_UCP_BARRIER_ONESIDED_H_

#include "tl_ucp.h"
#include "barrier.h"

ucc_status_t ucc_tl_ucp_barrier_onesided_start(ucc_coll_task_t *coll_task);

ucc_status_t ucc_tl_ucp_barrier_onesided_finalize(ucc_coll_task_t *coll_task);

#endif 