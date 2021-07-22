/**
 * Copyright (C) Mellanox Technologies Ltd. 2020-2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */

#include "tl_ucp.h"
#include "tl_ucp_ep.h"
#include "tl_ucp_addr.h"
#include "tl_ucp_coll.h"
#include "tl_ucp_sendrecv.h"
#include "utils/ucc_malloc.h"
#include "coll_score/ucc_coll_score.h"

/*static ucc_status_t ucc_tl_ucp_team_p2p_populate(ucc_tl_ucp_team_t * team,
                                                 ucc_team_p2p_conn_t p2p_conn,
                                                 ucc_tl_ucp_context_t * ctx);
*/

UCC_CLASS_INIT_FUNC(ucc_tl_ucp_team_t, ucc_base_context_t *tl_context,
                    const ucc_base_team_params_t *params)
{
    ucc_status_t          status = UCC_OK;
    ucc_tl_ucp_context_t *ctx =
        ucc_derived_of(tl_context, ucc_tl_ucp_context_t);
    UCC_CLASS_CALL_SUPER_INIT(ucc_tl_team_t, &ctx->super);
    /* TODO: init based on ctx settings and on params: need to check
             if all the necessary ranks mappings are provided */
    self->addr_storage       = NULL;
    self->preconnect_task    = NULL;
    self->pSync              = NULL;
    self->size               = params->params.oob.participants;
    self->scope              = params->scope;
    self->scope_id           = params->scope_id;
    self->rank               = params->rank;
    self->id                 = params->id;
    self->seq_num            = 0;
    self->status             = UCC_INPROGRESS;

    if (params->params.mask & UCC_TEAM_PARAM_FIELD_P2P_CONN) {
        self->p2p_conn = params->params.p2p_conn;
    }

    if (params->params.mask & UCC_TEAM_PARAM_FIELD_MEM_PARAMS) {
        self->pSync = params->params.mem_params.address;
    } 

    status                   = ucc_tl_ucp_addr_exchange_start(ctx,
                                                   params->params.oob,
                                                  &self->addr_storage);
    if (status == UCC_INPROGRESS) {
        /* exchange started but not complete return UCC_OK from post */
        status = UCC_OK;
    }
    tl_info(tl_context->lib, "posted tl team: %p", self);
    return status;
}

UCC_CLASS_CLEANUP_FUNC(ucc_tl_ucp_team_t)
{
    if (self->addr_storage) {
        ucc_tl_ucp_addr_storage_free(self->addr_storage);
    }

    tl_info(self->super.super.context->lib, "finalizing tl team: %p", self);
}

UCC_CLASS_DEFINE_DELETE_FUNC(ucc_tl_ucp_team_t, ucc_base_team_t);
UCC_CLASS_DEFINE(ucc_tl_ucp_team_t, ucc_tl_team_t);

ucc_status_t ucc_tl_ucp_team_destroy(ucc_base_team_t *tl_team)
{
    UCC_CLASS_DELETE_FUNC_NAME(ucc_tl_ucp_team_t)(tl_team);
    return UCC_OK;
}

static ucc_status_t ucc_tl_ucp_team_preconnect(ucc_tl_ucp_team_t *team)
{
    ucc_rank_t src, dst;
    ucc_status_t status;
    int i;
    if (!team->preconnect_task) {
        team->preconnect_task = ucc_tl_ucp_get_task(team);
        team->preconnect_task->tag = 0;
    }
    if (UCC_INPROGRESS == ucc_tl_ucp_test(team->preconnect_task)) {
        return UCC_INPROGRESS;
    }
    for (i = team->preconnect_task->send_posted; i < team->size; i++) {
        src = (team->rank - i + team->size) % team->size;
        dst = (team->rank + i) % team->size;
        status = ucc_tl_ucp_send_nb(NULL, 0, UCC_MEMORY_TYPE_UNKNOWN, src, team,
                                    team->preconnect_task);
        if (UCC_OK != status) {
            return status;
        }
        status = ucc_tl_ucp_recv_nb(NULL, 0, UCC_MEMORY_TYPE_UNKNOWN, dst, team,
                                    team->preconnect_task);
        if (UCC_OK != status) {
            return status;
        }
        if (UCC_INPROGRESS == ucc_tl_ucp_test(team->preconnect_task)) {
            return UCC_INPROGRESS;
        }
    }
    tl_debug(UCC_TL_TEAM_LIB(team), "preconnected tl team: %p, num_eps %d",
             team, team->size);
    ucc_tl_ucp_put_task(team->preconnect_task);
    team->preconnect_task = NULL;
    return UCC_OK;
}

#if 0
static ucc_status_t ucc_tl_ucp_team_p2p_populate(ucc_tl_ucp_team_t * team,
                                                 ucc_team_p2p_conn_t p2p_conn,
                                                 ucc_tl_ucp_context_t * ctx)
{
    ucc_tl_ucp_remote_info_t   **remote_info;

    /* only populate this hash IFF it has not already been populated */
    if (ctx->rinfo_hash == NULL) {
        ctx->rinfo_hash = kh_init(tl_ucp_rinfo_hash);
        remote_info = (ucc_tl_ucp_remote_info_t **) 
                            malloc(sizeof(ucc_tl_ucp_remote_info_t *) * team->size);
        
        for (int i = 0; i < team->size; i++) {
            ucc_context_id_t key = ucc_tl_ucp_get_rank_key(team, i);
            
            remote_info[i] = (ucc_tl_ucp_remote_info_t *) malloc(sizeof(ucc_tl_ucp_remote_info_t) * 2);
            memset(remote_info[i], 0, sizeof(ucc_tl_ucp_remote_info_t) * 2);
            if (i == team->rank) {
                p2p_conn.conn_info_lookup(NULL, i, (void ***) &remote_info, NULL);
            }
/*
            // TODO: fix with non-null values
            p2p_conn.conn_info_lookup(NULL, i, (void ***) &remote_info, NULL);
            
            // set rkey to NULL here... it'll be unpacked later
            remote_info[i][0].rkey = NULL;
            remote_info[i][1].rkey = NULL;
*/
            // populate the hash
            tl_ucp_rinfo_hash_put(ctx->rinfo_hash, key, (void **) &remote_info[i]);
        }
        ctx->remote_info = remote_info;
    }
    return UCC_OK;
}
#endif

ucc_status_t ucc_tl_ucp_team_create_test(ucc_base_team_t *tl_team)
{
    ucc_tl_ucp_team_t    *team = ucc_derived_of(tl_team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_context_t *ctx  = UCC_TL_UCP_TEAM_CTX(team);
    ucc_status_t          status;
    if (team->status == UCC_OK) {
        return UCC_OK;
    }
    if (team->addr_storage &&
        (team->addr_storage->state != UCC_TL_UCP_ADDR_EXCHANGE_COMPLETE)) {
        status = ucc_tl_ucp_addr_exchange_test(team->addr_storage);
        if (UCC_INPROGRESS == status) {
            return UCC_INPROGRESS;
        } else if (UCC_OK != status) {
            return status;
        }
    }
    if (team->size <= ctx->cfg.preconnect) {
        status = ucc_tl_ucp_team_preconnect(team);
        if (UCC_INPROGRESS == status) {
            return UCC_INPROGRESS;
        } else if (UCC_OK != status) {
            goto err_preconnect;
        }
    }
#if 0
    if (team->pSync) {
        status = ucc_tl_ucp_team_p2p_populate(team,
                                              team->p2p_conn,
                                              ctx);
        if (UCC_OK != status) {
            return status;
        }
    }
   #endif

    tl_info(tl_team->context->lib, "initialized tl team: %p", team);
    team->status = UCC_OK;
    return UCC_OK;

err_preconnect:
    return status;
}

ucc_status_t ucc_tl_ucp_team_get_scores(ucc_base_team_t   *tl_team,
                                        ucc_coll_score_t **score_p)
{
    ucc_tl_ucp_team_t *team = ucc_derived_of(tl_team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_lib_t  *lib  = UCC_TL_UCP_TEAM_LIB(team);
    ucc_coll_score_t  *score;
    ucc_status_t       status;
    unsigned           i;
    /* There can be a different logic for different coll_type/mem_type.
       Right now just init everything the same way. */
    status = ucc_coll_score_build_default(tl_team, UCC_TL_UCP_DEFAULT_SCORE,
                              ucc_tl_ucp_coll_init, UCC_TL_UCP_SUPPORTED_COLLS,
                              NULL, 0, &score);
    if (UCC_OK != status) {
        return status;
    }
    for (i = 0; i < UCC_TL_UCP_N_DEFAULT_ALG_SELECT_STR; i++) {
        status = ucc_coll_score_update_from_str(
            ucc_tl_ucp_default_alg_select_str[i], score, team->size,
            ucc_tl_ucp_coll_init, &team->super.super, UCC_TL_UCP_DEFAULT_SCORE,
            ucc_tl_ucp_alg_id_to_init);
        if (UCC_OK != status) {
            tl_error(tl_team->context->lib,
                     "failed to apply default coll select setting: %s",
                     ucc_tl_ucp_default_alg_select_str[i]);
            goto err;
        }
    }
    if (strlen(lib->super.super.score_str) > 0) {
        status = ucc_coll_score_update_from_str(
            lib->super.super.score_str, score, team->size, NULL,
            &team->super.super, UCC_TL_UCP_DEFAULT_SCORE,
            ucc_tl_ucp_alg_id_to_init);

        /* If INVALID_PARAM - User provided incorrect input - try to proceed */
        if ((status < 0) && (status != UCC_ERR_INVALID_PARAM) &&
            (status != UCC_ERR_NOT_SUPPORTED)) {
            goto err;
        }
    }
    *score_p = score;
    return UCC_OK;
err:
    ucc_coll_score_free(score);
    return status;
}
