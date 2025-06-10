/**
 * Copyright (c) 2021, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */
#ifndef BARRIER_H_
#define BARRIER_H_
#include "../tl_ucp.h"
#include "../tl_ucp_coll.h"

#define UCC_TL_UCP_BARRIER_ONESIDED_DEFAULT_RADIX 2

#if 1
typedef struct ucc_tl_ucp_barrier_alg_info_t {
    ucc_tl_ucp_barrier_alg_t id;
    const char *name;
    const char *desc;
} ucc_tl_ucp_barrier_alg_info_t;
#endif
#if 0
enum {
    UCC_TL_UCP_BARRIER_ALG_KNOMIAL,
    UCC_TL_UCP_BARRIER_ALG_ONESIDED,
    UCC_TL_UCP_BARRIER_ALG_LAST
};
#endif
extern ucc_base_coll_alg_info_t
             ucc_tl_ucp_barrier_algs[UCC_TL_UCP_BARRIER_ALG_LAST + 1];

static inline int ucc_tl_ucp_barrier_alg_from_str(const char *str)
{
    int i;

    for (i = 0; i < UCC_TL_UCP_BARRIER_ALG_LAST; i++) {
        if (ucc_tl_ucp_barrier_algs[i].name &&
            strcasecmp(str, ucc_tl_ucp_barrier_algs[i].name) == 0) {
            return ucc_tl_ucp_barrier_algs[i].id;
        }
    }
    return -1;
}

ucc_status_t ucc_tl_ucp_barrier_init(ucc_tl_ucp_task_t *task);
ucc_status_t ucc_tl_ucp_barrier_knomial_init(ucc_base_coll_args_t *coll_args,
                                             ucc_base_team_t *team,
                                             ucc_coll_task_t **task_h);


ucc_status_t ucc_tl_ucp_barrier_onesided_init(ucc_base_coll_args_t *coll_args,
                                             ucc_base_team_t *team,
                                             ucc_coll_task_t **task_h);

#endif
