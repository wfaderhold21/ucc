/**
 * Copyright (C) Mellanox Technologies Ltd. 2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */
#include "config.h"

#ifndef UCC_TL_UCP_EP_H_
#define UCC_TL_UCP_EP_H_
#include "ucc/api/ucc.h"
#include <ucp/api/ucp.h>
#include "tl_ucp.h"
#include "tl_ucp_addr.h"
typedef struct ucc_tl_ucp_context ucc_tl_ucp_context_t;
typedef struct ucc_tl_ucp_team    ucc_tl_ucp_team_t;


ucc_status_t ucc_tl_ucp_connect_team_ep(ucc_tl_ucp_team_t *team, ucc_rank_t team_rank,
                                        ucc_context_id_t key, ucp_ep_h *ep);
ucc_status_t ucc_tl_ucp_connect_ctx_ep(ucc_tl_ucp_context_t *ctx, ucc_rank_t ctx_rank);

ucc_status_t ucc_tl_ucp_close_eps(ucc_tl_ucp_context_t *ctx);

static inline ucc_context_id_t
ucc_tl_ucp_get_rank_key(ucc_tl_ucp_team_t *team, ucc_rank_t rank)
{
    ucc_assert(team->addr_storage);
    /* Currently team always has addr_storage by this moment.
       At some point we might add the logic when addressing stored on a context
       or even outside of UCC */
    size_t max_addrlen = team->addr_storage->max_addrlen; /* NOLINT */
    char *addresses    = (char*)team->addr_storage->addresses;
    ucc_tl_ucp_addr_t  *address = (ucc_tl_ucp_addr_t *)(addresses +
                                                        max_addrlen * rank);
    return address->id;
}

// FIXME: move to better location?
static inline ucc_status_t ucc_tl_ucp_resolve_info(ucc_tl_ucp_team_t *team, 
                                                   ucc_rank_t rank, 
                                                   ucc_context_id_t key, 
                                                   uint64_t target_va, 
                                                   uint64_t * rva, 
                                                   ucp_rkey_h * rkey)
{
    ucc_tl_ucp_remote_info_t p2p_info;
    ucc_tl_ucp_remote_info_t * p2p_info_p = &p2p_info;
    ucc_tl_ucp_remote_info_t ** p2p_info_pp = &p2p_info_p;

    p2p_info.va = (void *) target_va;
    p2p_info.rva = NULL;
    p2p_info.packed_key = NULL;

    team->p2p_conn.conn_info_lookup(NULL, rank, (void ***) &p2p_info_pp, NULL);
    
    if (p2p_info.rva == NULL) {
        return UCC_ERR_NO_MESSAGE;
    }

    *rva = (uint64_t) p2p_info.rva;
    if (rkey != NULL) {
        *rkey = p2p_info.packed_key;
    }
    return UCC_OK;
}

static inline ucc_status_t ucc_tl_ucp_get_ep(ucc_tl_ucp_team_t *team, ucc_rank_t rank,
                                             ucp_ep_h *ep)
{
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);
    ucc_context_id_t      key = ucc_tl_ucp_get_rank_key(team, rank);
    ucc_status_t          status;

    *ep = tl_ucp_hash_get(ctx->ep_hash, key);
    if (NULL == (*ep)) {
        /* Not connected yet */
        status = ucc_tl_ucp_connect_team_ep(team, rank, key, ep);
        if (ucc_unlikely(UCC_OK != status)) {
            tl_error(UCC_TL_TEAM_LIB(team), "failed to connect team ep");
            *ep = NULL;
            return status;
        }
    }
    return UCC_OK;
}

/*
 * target_va -> local target address [in]
 * rva -> calc'd remote va [out]
 * rkey -> rkey based on target/va [out]
 */
static inline ucc_status_t ucc_tl_ucp_get_rinfo(ucc_tl_ucp_team_t *team, ucc_rank_t rank,
                                               ucp_ep_h ep,
                                               uint64_t target_va, uint64_t * rva, 
                                               ucp_rkey_h *rkey)
{
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);
    ucc_context_id_t      key = ucc_tl_ucp_get_rank_key(team, rank);
    ucc_status_t          status;
    ucp_rkey_h rkey2;
        
    status = ucc_tl_ucp_resolve_info(team, rank, key, target_va, rva, &rkey2);
    if (UCC_OK != status) {
        tl_error(UCC_TL_TEAM_LIB(team), "failed to obtain rkey");
        *rkey = NULL;
        *rva = 0;
        return status;
    }

    *rkey = tl_ucp_rkey_hash_get(ctx->rkey_hash, (uint64_t) rkey2);
    if (NULL == (*rkey)) {
//        printf("[%d] unpacking (tva: %lx, rva: %lx, rkey: %p, rank: %d)\n", team->rank, target_va, *rva, rkey2, rank);
        ucp_ep_rkey_unpack(ep,
                           rkey2,
                           rkey);
        tl_ucp_rkey_hash_put(ctx->rkey_hash, (uint64_t) rkey2, *rkey);
    }

    return UCC_OK;
}



#endif
