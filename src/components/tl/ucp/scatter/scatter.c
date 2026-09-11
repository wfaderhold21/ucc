/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "utils/ucc_math.h"
#include "scatter.h"

ucc_base_coll_alg_info_t
    ucc_tl_ucp_scatter_algs[UCC_TL_UCP_SCATTER_ALG_LAST + 1] = {
        [UCC_TL_UCP_SCATTER_ALG_KNOMIAL] =
            {.id   = UCC_TL_UCP_SCATTER_ALG_KNOMIAL,
             .name = "knomial",
             .desc = "scatter over knomial tree with arbitrary radix "
                     "(optimized for latency)"},
        [UCC_TL_UCP_SCATTER_ALG_ONESIDED] =
            {.id   = UCC_TL_UCP_SCATTER_ALG_ONESIDED,
             .name = "onesided",
             .desc = "one-sided scatter (root-driven put + atomic signal)"},
        [UCC_TL_UCP_SCATTER_ALG_LAST] = {
            .id = 0, .name = NULL, .desc = NULL}};

/*
 * Legacy init (no score context): the two-sided knomial is the only
 * algorithm reachable this way, so wire its start/progress/finalize directly.
 * The score/tune path (ucc_tl_ucp_alg_id_to_init) uses the 3-arg *_init
 * wrappers.
 */
ucc_status_t ucc_tl_ucp_scatter_init(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_rank_t         size  = UCC_TL_TEAM_SIZE(team);
    ucc_kn_radix_t     radix;

    radix = ucc_min(UCC_TL_UCP_TEAM_LIB(team)->cfg.scatter_kn_radix, size);

    task->super.post         = ucc_tl_ucp_scatter_knomial_start;
    task->super.progress     = ucc_tl_ucp_scatter_knomial_progress;
    task->super.finalize     = ucc_tl_ucp_scatter_knomial_finalize;
    task->scatter_kn.p.radix = radix;
    return UCC_OK;
}
