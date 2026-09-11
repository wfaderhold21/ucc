/**
 * Copyright (c) 2021, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */
#ifndef SCATTER_H_
#define SCATTER_H_
#include "../tl_ucp.h"
#include "../tl_ucp_coll.h"

enum {
    UCC_TL_UCP_SCATTER_ALG_KNOMIAL,
    UCC_TL_UCP_SCATTER_ALG_ONESIDED,
    UCC_TL_UCP_SCATTER_ALG_LAST
};

extern ucc_base_coll_alg_info_t
             ucc_tl_ucp_scatter_algs[UCC_TL_UCP_SCATTER_ALG_LAST + 1];

/* A set of convenience macros used to implement sw based progress
   of the scatter algorithm that uses kn pattern */
enum
{
    UCC_SCATTER_KN_PHASE_INIT,
    UCC_SCATTER_KN_PHASE_LOOP, /* main loop of recursive k-ing */
};

ucc_status_t ucc_tl_ucp_scatter_knomial_start(ucc_coll_task_t *task);

void ucc_tl_ucp_scatter_knomial_progress(ucc_coll_task_t *task);

ucc_status_t ucc_tl_ucp_scatter_knomial_finalize(ucc_coll_task_t *task);

/* Internal interface to KN scatter with custom radix */
ucc_status_t ucc_tl_ucp_scatter_knomial_init_r(
    ucc_base_coll_args_t *coll_args, ucc_base_team_t *team,
    ucc_coll_task_t **task_h, ucc_kn_radix_t radix);

ucc_status_t ucc_tl_ucp_scatter_init(ucc_tl_ucp_task_t *task);

ucc_status_t ucc_tl_ucp_scatter_knomial_init(ucc_base_coll_args_t *coll_args,
                                             ucc_base_team_t      *team,
                                             ucc_coll_task_t     **task_h);

ucc_status_t ucc_tl_ucp_scatter_onesided_init(ucc_base_coll_args_t *coll_args,
                                              ucc_base_team_t      *team,
                                              ucc_coll_task_t     **task_h);

static inline int ucc_tl_ucp_scatter_alg_from_str(const char *str)
{
    int i;
    for (i = 0; i < UCC_TL_UCP_SCATTER_ALG_LAST; i++) {
        if (0 == strcasecmp(str, ucc_tl_ucp_scatter_algs[i].name)) {
            break;
        }
    }
    return i;
}
#endif
