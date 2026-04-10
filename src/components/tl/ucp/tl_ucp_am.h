/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_TL_UCP_AM_H_
#define UCC_TL_UCP_AM_H_

#include <stdint.h>

#define UCC_TL_UCP_AM_ID_BARRIER 0

typedef struct ucc_tl_ucp_am_barrier_hdr {
    uint16_t team_id;
    uint32_t coll_tag;
} ucc_tl_ucp_am_barrier_hdr_t;

#endif
