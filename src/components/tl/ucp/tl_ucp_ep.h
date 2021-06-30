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

static inline ucc_status_t ucc_tl_ucp_get_rkey(ucc_tl_ucp_team_t *team,
                                               ucc_rank_t rank,
                                               ucp_rkey_h *rkey)
{
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);
    ucc_context_id_t      key = ucc_tl_ucp_get_rank_key(team, rank);
    ucc_tl_ucp_remote_info_t   **remote_info;

    remote_info = (ucc_tl_ucp_remote_info_t **) tl_ucp_rinfo_hash_get(ctx->rinfo_hash, key);
    *rkey = (ucp_rkey_h) remote_info[0]->rkey; //TODO: switch from 0 to segment
    return UCC_OK;
}

static inline ucc_status_t ucc_tl_ucp_rinfo_hash_update(ucc_tl_ucp_team_t *team, 
                                                        ucc_rank_t rank)
{
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);
    team->p2p_conn.conn_info_lookup(NULL, rank, (void ***) &ctx->remote_info, NULL);
    if (NULL == ctx->remote_info[rank]->packed_key) {
        return UCC_ERR_NO_MESSAGE;
    }

    return UCC_OK;
}

static inline ucc_status_t ucc_tl_ucp_get_bva(ucc_tl_ucp_team_t *team,
                                                       ucc_rank_t rank,
                                                       uint64_t * va)
{
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);
    ucc_context_id_t      key = ucc_tl_ucp_get_rank_key(team, rank);
    ucc_tl_ucp_remote_info_t   **remote_info;
    ucc_status_t status;

    remote_info = (ucc_tl_ucp_remote_info_t **) tl_ucp_rinfo_hash_get(ctx->rinfo_hash, key);
    if (NULL == remote_info[0]->va_base) {
        ucc_rank_t global_rank = ((ptrdiff_t) remote_info - (ptrdiff_t) &ctx->remote_info[0]) >> 3;

        status = ucc_tl_ucp_rinfo_hash_update(team, global_rank);
        if (UCC_OK != status) {
            return status;
        }
    } 

    if (remote_info[0][0].va_base < remote_info[0][1].va_base) {
        if (*va < (ptrdiff_t) remote_info[0][1].va_base) {
            *va = (uint64_t) remote_info[0][0].va_base;
        } else {
            *va = (uint64_t) remote_info[0][1].va_base;
        }
    } else {
        if (*va < (ptrdiff_t) remote_info[0][0].va_base) {
            *va = (uint64_t) remote_info[0][1].va_base;
        } else {
            *va = (uint64_t) remote_info[0][0].va_base;
        }
    }

    return UCC_OK;
}



static inline ucc_status_t ucc_tl_ucp_ep_unpack_rkey(ucc_tl_ucp_team_t *team,
                                                     ucc_rank_t rank,
                                                     ucp_ep_h *ep,
                                                     ucp_rkey_h *rkey)
{
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);
    ucc_context_id_t      key = ucc_tl_ucp_get_rank_key(team, rank);
    ucc_tl_ucp_remote_info_t   **remote_info;
    ucc_status_t status;

    remote_info = (ucc_tl_ucp_remote_info_t **) tl_ucp_rinfo_hash_get(ctx->rinfo_hash, key);
    if (NULL == remote_info[0]->packed_key) {
        ucc_rank_t global_rank = ((ptrdiff_t) remote_info - (ptrdiff_t) &ctx->remote_info[0]) >> 3;

        status = ucc_tl_ucp_rinfo_hash_update(team, global_rank);
        if (UCC_OK != status) {
            return status;
        }
    }

    ucp_ep_rkey_unpack(*ep,
                       remote_info[0]->packed_key,
                       (ucp_rkey_h *) &remote_info[0]->rkey);
    *rkey = (ucp_rkey_h) remote_info[0]->rkey;
    return UCC_OK;
}

static inline ucc_status_t ucc_tl_ucp_resolve_p2p_by_va(ucc_tl_ucp_team_t *team,
                                                            void * va,
                                                            ucp_ep_h * ep,
                                                            ucc_rank_t peer,
                                                            ucc_tl_ucp_remote_info_t ** rinfo,
                                                            ucc_tl_ucp_remote_info_t ** linfo)
{
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);
    ucc_context_id_t      rkey = ucc_tl_ucp_get_rank_key(team, peer);
    ucc_context_id_t      lkey = ucc_tl_ucp_get_rank_key(team, team->rank);
    ucc_tl_ucp_remote_info_t   **remote_info, **local_info;
    ucc_status_t status;
    ucc_rank_t global_rank = peer;
    int segment = 0;

    local_info = (ucc_tl_ucp_remote_info_t **) tl_ucp_rinfo_hash_get(ctx->rinfo_hash, lkey);
    remote_info = (ucc_tl_ucp_remote_info_t **) tl_ucp_rinfo_hash_get(ctx->rinfo_hash, rkey);

    /* do we have good info yet? */
    if (NULL == remote_info[0]->va_base) {
        global_rank = ((ptrdiff_t) remote_info - (ptrdiff_t) &ctx->remote_info[0]) >> 3;
       // printf("[%d] looking up good info on %d\n", team->rank, peer);

        status = ucc_tl_ucp_rinfo_hash_update(team, global_rank);
        if (UCC_OK != status) {
            return status;
        }
    } 

    /* which segment? */
    if (local_info[0][0].va_base < local_info[0][1].va_base) {
        if (va >= local_info[0][0].va_base && va < local_info[0][1].va_base) {
            segment = 0;
        } else {
            segment = 1;
        }
    } else {
        if (va >= local_info[0][1].va_base && va < local_info[0][0].va_base) {
            segment = 1;
        } else {
            segment = 0;
        }
    }

    /* is the rkey unpacked? */
    if (NULL == remote_info[0][segment].rkey) {
        /* we just looked things up, the packed_key should be present */
     //   printf("[%d:segment %d] unpacking rkey from %d\n", team->rank, segment, peer);
        ucp_ep_rkey_unpack(*ep,
                           remote_info[0][segment].packed_key,
                           (ucp_rkey_h *) &remote_info[0][segment].rkey);
  
    //    printf("[%d:segment %d] packed_key: %p rkey: %p\n", team->rank, segment, remote_info[0][segment].packed_key, remote_info[0][segment].rkey);
    }

    *rinfo = &remote_info[0][segment];
    *linfo = &local_info[0][segment];
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

#endif
