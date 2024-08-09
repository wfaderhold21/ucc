/**
 * Copyright (c) 2021-2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef ALLREDUCE_H_
#define ALLREDUCE_H_
#include "../cl_urom_coll.h"

enum {
    UCC_CL_UROM_ALLREDUCE_ALG_FULL,
    UCC_CL_UROM_ALLREDUCE_ALG_LAST,
};

extern ucc_base_coll_alg_info_t
    ucc_cl_urom_allreduce_algs[UCC_CL_UROM_ALLREDUCE_ALG_LAST + 1];

#define UCC_CL_UROM_ALLREDUCE_DEFAULT_ALG_SELECT_STR "allreduce:0-inf:@full"

ucc_status_t ucc_cl_urom_allreduce_full_init(ucc_base_coll_args_t *coll_args,
                                             ucc_base_team_t      *team,
                                             ucc_coll_task_t     **task);

static inline int ucc_cl_urom_allreduce_alg_from_str(const char *str)
{
    int i;

    for (i = 0; i < UCC_CL_UROM_ALLREDUCE_ALG_LAST; i++) {
        if (0 == strcasecmp(str, ucc_cl_urom_allreduce_algs[i].name)) {
            break;
        }
    }
    return i;
}

#endif
