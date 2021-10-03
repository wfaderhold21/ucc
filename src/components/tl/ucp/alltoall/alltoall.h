/**
 * Copyright (C) Mellanox Technologies Ltd. 2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#ifndef ALLTOALL_H_
#define ALLTOALL_H_

#include "../tl_ucp.h"
#include "../tl_ucp_coll.h"

enum {
    UCC_TL_UCP_ALLTOALL_ALG_ONESIDED,
    UCC_TL_UCP_ALLTOALL_ALG_OS_BRUCK,
    UCC_TL_UCP_ALLTOALL_ALG_PAIRWISE,
    UCC_TL_UCP_ALLTOALL_ALG_LAST
};

extern ucc_base_coll_alg_info_t ucc_tl_ucp_alltoall_algs[UCC_TL_UCP_ALLTOALL_ALG_LAST + 1];

ucc_status_t ucc_tl_ucp_alltoall_init(ucc_tl_ucp_task_t *task);

#define UCC_TL_UCP_ALLTOALL_DEFAULT_ALG_SELECT_STR \
    "alltoall:0-inf:@0"

ucc_status_t ucc_tl_ucp_alltoall_pairwise_init(ucc_base_coll_args_t *coll_args,
                                               ucc_base_team_t *     team,
                                               ucc_coll_task_t **    task_h);
ucc_status_t ucc_tl_ucp_alltoall_pairwise_start(ucc_coll_task_t *task);
ucc_status_t ucc_tl_ucp_alltoall_pairwise_progress(ucc_coll_task_t *task);


ucc_status_t ucc_tl_ucp_alltoall_onesided_init(ucc_base_coll_args_t *coll_args,
                                               ucc_base_team_t *     team,
                                               ucc_coll_task_t **    task_h);
ucc_status_t ucc_tl_ucp_alltoall_onesided_start(ucc_coll_task_t *task);
ucc_status_t ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *task);
ucc_status_t ucc_tl_ucp_alltoall_onesided_get_progress(ucc_coll_task_t *task);

ucc_status_t ucc_tl_ucp_alltoall_os_bruck_init(ucc_base_coll_args_t *coll_args,
                                               ucc_base_team_t *     team,
                                               ucc_coll_task_t **    task_h);
ucc_status_t ucc_tl_ucp_alltoall_os_bruck_start(ucc_coll_task_t *task);
ucc_status_t ucc_tl_ucp_alltoall_os_bruck_progress(ucc_coll_task_t *task);



static inline int ucc_tl_ucp_alltoall_alg_from_str(const char *str)
{
    int i;
    for (i = 0; i < UCC_TL_UCP_ALLTOALL_ALG_LAST; i++) {
        if (0 == strcasecmp(str, ucc_tl_ucp_alltoall_algs[i].name)) {
            break;
        }
    }
    return i;
}




#endif
