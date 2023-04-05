/**
 * Copyright (c) 2020-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "cl_urom.h"
#include "utils/ucc_malloc.h"
#include "core/ucc_team.h"

UCC_CLASS_INIT_FUNC(ucc_cl_urom_team_t, ucc_base_context_t *cl_context,
                    const ucc_base_team_params_t *params)
{
    ucc_cl_urom_context_t *ctx       =
        ucc_derived_of(cl_context, ucc_cl_urom_context_t);
    ucc_cl_urom_lib_t *urom_lib = ucc_derived_of(ctx->super.super.lib, ucc_cl_urom_lib_t);
    urom_status_t   urom_status;
    ucc_status_t            status;
    urom_worker_cmd_t team_cmd = {
        .cmd_type = UROM_WORKER_CMD_UCC,
        .ucc.dpu_worker_id = ctx->ctx_rank,
        .ucc.cmd_type = UROM_WORKER_CMD_UCC_TEAM_CREATE,
        /* FIXME: proper way: use ec map.. for now assume linear */
        .ucc.team_create_cmd = 
        {
            .start = 0,
            .stride = 1,
            .size = params->size,
            .context_h = ctx->urom_ucc_ctx_h,
        },
    };

    UCC_CLASS_CALL_SUPER_INIT(ucc_cl_team_t, &ctx->super, params);
    self->teams = (ucc_team_h **)ucc_malloc(sizeof(ucc_team_h *) * 16);
    if (!self->teams) {
        cl_error(cl_context->lib, "failed to allocate %zd bytes for urom teams", sizeof(ucc_team_h *) * 16);
        status = UCC_ERR_NO_MEMORY;
        return status;
    }
    self->n_teams = 0;
    self->score_map = NULL;
   
    urom_status = urom_worker_push_cmdq(urom_lib->urom_ctx.urom_worker, 0, &team_cmd);
    if (UROM_OK != urom_status) {
        cl_error(cl_context->lib, "failed to create team");
        return UCC_ERR_NO_MESSAGE;
    }
    cl_debug(cl_context->lib, "posted cl team: %p", self);
    return UCC_OK;
}

UCC_CLASS_CLEANUP_FUNC(ucc_cl_urom_team_t)
{
    cl_debug(self->super.super.context->lib, "finalizing cl team: %p", self);
}

UCC_CLASS_DEFINE_DELETE_FUNC(ucc_cl_urom_team_t, ucc_base_team_t);
UCC_CLASS_DEFINE(ucc_cl_urom_team_t, ucc_cl_team_t);

ucc_status_t ucc_cl_urom_team_destroy(ucc_base_team_t *cl_team)
{
    return UCC_OK;    
}

ucc_status_t ucc_cl_urom_team_create_test(ucc_base_team_t *cl_team)
{
    ucc_cl_urom_team_t    *team = ucc_derived_of(cl_team, ucc_cl_urom_team_t);
    ucc_cl_urom_context_t *ctx  = UCC_CL_UROM_TEAM_CTX(team);
    ucc_cl_urom_lib_t *urom_lib = ucc_derived_of(ctx->super.super.lib, ucc_cl_urom_lib_t);
    ucc_memory_type_t           mem_types[2] = {UCC_MEMORY_TYPE_HOST,UCC_MEMORY_TYPE_CUDA};
    int mt_n = 2;
    ucc_coll_score_t       *score = NULL;
    ucc_coll_score_t       *score_next = NULL;
    ucc_coll_score_t       *score_merge = NULL;
    urom_status_t           urom_status;
    ucc_status_t            ucc_status;
    urom_worker_notify_t   *notif;

    urom_status = urom_worker_pop_notifyq(urom_lib->urom_ctx.urom_worker, 0, &notif);
    if (UROM_ERR_QUEUE_EMPTY != urom_status) {
        if (urom_status == UROM_OK) {
            if (notif->ucc.status == (urom_status_t)UCC_OK) {
                team->teams[team->n_teams] = notif->ucc.team_create_nqe.team;
                ++team->n_teams;
                ucc_status = ucc_coll_score_build_default(cl_team, UCC_CL_UROM_DEFAULT_SCORE,
                                          ucc_cl_urom_coll_init, UCC_COLL_TYPE_ALLTOALL,
                                          mem_types, mt_n, &score);
                if (UCC_OK != ucc_status) {
                    return ucc_status;
                }

                ucc_status = ucc_coll_score_build_default(cl_team, UCC_CL_UROM_DEFAULT_SCORE,
                                          ucc_cl_urom_coll_init, UCC_COLL_TYPE_ALLTOALLV,
                                          mem_types, mt_n, &score_next);
                if (UCC_OK != ucc_status) {
                    return ucc_status;
                }

                ucc_status = ucc_coll_score_merge(score, score_next, &score_merge, 1);
                if (UCC_OK != ucc_status) {
                    cl_error(ctx->super.super.lib, "failed to merge scores");
                    return ucc_status;
                }

                score = score_merge;
                ucc_status = ucc_coll_score_build_map(score, &team->score_map);
                if (UCC_OK != ucc_status) {
                    cl_error(ctx->super.super.lib, "failed to build score map");
                }
                team->score = score;
                ucc_coll_score_set(team->score, UCC_CL_UROM_DEFAULT_SCORE);

                return UCC_OK;
            }
        }
        return UCC_ERR_NO_MESSAGE;
    }
    return UCC_INPROGRESS;
}

ucc_status_t ucc_cl_urom_team_get_scores(ucc_base_team_t   *cl_team,
                                          ucc_coll_score_t **score)
{
    ucc_cl_urom_team_t *team = ucc_derived_of(cl_team, ucc_cl_urom_team_t);
    ucc_base_context_t  *ctx  = UCC_CL_TEAM_CTX(team);
    ucc_status_t         status;
    ucc_coll_score_team_info_t team_info;

    status = ucc_coll_score_dup(team->score, score);
    if (UCC_OK != status) {
        return status;
    }

    if (strlen(ctx->score_str) > 0) {
        team_info.alg_fn              = NULL;
        team_info.default_score       = UCC_CL_UROM_DEFAULT_SCORE;
        team_info.init                = NULL;
        team_info.num_mem_types       = 0;
        team_info.supported_mem_types = NULL; /* all memory types supported*/
        team_info.supported_colls     = UCC_COLL_TYPE_ALL;
        team_info.size                = UCC_CL_TEAM_SIZE(team);

        status = ucc_coll_score_update_from_str(ctx->score_str, &team_info, &team->super.super, *score);

        /* If INVALID_PARAM - User provided incorrect input - try to proceed */
        if ((status < 0) && (status != UCC_ERR_INVALID_PARAM) &&
            (status != UCC_ERR_NOT_SUPPORTED)) {
            goto err;
        }
    }
    return UCC_OK;
err:
    ucc_coll_score_free(*score);
    *score = NULL;
    return status;
}
