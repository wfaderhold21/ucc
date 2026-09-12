/**
 * Copyright (c) 2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef SCATTERV_H_
#define SCATTERV_H_

#include "../tl_ucp.h"
#include "../tl_ucp_coll.h"

enum {
    UCC_TL_UCP_SCATTERV_ALG_LINEAR,
    UCC_TL_UCP_SCATTERV_ALG_ONESIDED,
    UCC_TL_UCP_SCATTERV_ALG_LAST
};

extern ucc_base_coll_alg_info_t
             ucc_tl_ucp_scatterv_algs[UCC_TL_UCP_SCATTERV_ALG_LAST + 1];

ucc_status_t ucc_tl_ucp_scatterv_init(ucc_tl_ucp_task_t *task);

ucc_status_t ucc_tl_ucp_scatterv_linear_init(ucc_base_coll_args_t *coll_args,
                                             ucc_base_team_t *team,
                                             ucc_coll_task_t **task_h);

ucc_status_t ucc_tl_ucp_scatterv_onesided_init(ucc_base_coll_args_t *coll_args,
                                               ucc_base_team_t      *team,
                                               ucc_coll_task_t     **task_h);

static inline int ucc_tl_ucp_scatterv_alg_from_str(const char *str)
{
    int i;
    for (i = 0; i < UCC_TL_UCP_SCATTERV_ALG_LAST; i++) {
        if (0 == strcasecmp(str, ucc_tl_ucp_scatterv_algs[i].name)) {
            break;
        }
    }
    return i;
}
#endif
