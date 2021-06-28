/**
 * Copyright (C) Mellanox Technologies Ltd. 2021.  ALL RIGHTS RESERVED.
 *
 * See file LICENSE for terms.
 */
#include "config.h"

#ifndef UCC_TL_UCP_SENDRECV_H_
#define UCC_TL_UCP_SENDRECV_H_
#include "tl_ucp_tag.h"
#include "tl_ucp_ep.h"
#include "utils/ucc_compiler_def.h"
#include "components/mc/base/ucc_mc_base.h"

extern ucs_memory_type_t ucc_memtype_to_ucs[UCC_MEMORY_TYPE_LAST+1];

void ucc_tl_ucp_send_completion_cb(void *request, ucs_status_t status,
                                   void *user_data);
void ucc_tl_ucp_recv_completion_cb(void *request, ucs_status_t status,
                                   const ucp_tag_recv_info_t *info,
                                   void *user_data);

#define UCC_TL_UCP_MAKE_TAG(_tag, _rank, _id, _scope_id, _scope)       \
    ((((uint64_t) (_tag))      << UCC_TL_UCP_TAG_BITS_OFFSET)      |   \
     (((uint64_t) (_rank))     << UCC_TL_UCP_SENDER_BITS_OFFSET)   |   \
     (((uint64_t) (_scope))    << UCC_TL_UCP_SCOPE_BITS_OFFSET)    |   \
     (((uint64_t) (_scope_id)) << UCC_TL_UCP_SCOPE_ID_BITS_OFFSET) |   \
     (((uint64_t) (_id))       << UCC_TL_UCP_ID_BITS_OFFSET))

#define UCC_TL_UCP_MAKE_SEND_TAG(_tag, _rank, _id, _scope_id, _scope)          \
    UCC_TL_UCP_MAKE_TAG(_tag, _rank, _id, _scope_id, _scope)

#define UCC_TL_UCP_MAKE_RECV_TAG(_ucp_tag, _ucp_tag_mask, _tag, _src, _id,     \
                                 _scope_id, _scope)                            \
    do {                                                                       \
        ucc_assert((_tag) <= UCC_TL_UCP_MAX_TAG);                              \
        ucc_assert((_src) <= UCC_TL_UCP_MAX_SENDER);                           \
        ucc_assert((_id) <= UCC_TL_UCP_MAX_ID);                                \
        (_ucp_tag_mask) = (uint64_t)(-1);                                      \
        (_ucp_tag) =                                                           \
            UCC_TL_UCP_MAKE_TAG((_tag), (_src), (_id), (_scope_id), (_scope)); \
    } while (0)

#define UCC_TL_UCP_CHECK_REQ_STATUS()                                          \
    do {                                                                       \
        if (ucc_unlikely(UCS_PTR_IS_ERR(ucp_status))) {                        \
            tl_error(UCC_TL_TEAM_LIB(team),                                    \
                     "tag %u; dest %d; team_id %u; errmsg %s", task->tag,      \
                     dest_group_rank, team->id,                                \
                     ucs_status_string(UCS_PTR_STATUS(ucp_status)));           \
            ucp_request_cancel(UCC_TL_UCP_WORKER(team), ucp_status);           \
            ucp_request_free(ucp_status);                                      \
            return UCC_ERR_NO_MESSAGE;                                         \
        }                                                                      \
    } while (0)

static inline ucc_status_t ucc_tl_ucp_send_nb(void *buffer, size_t msglen,
                                              ucc_memory_type_t mtype,
                                              ucc_rank_t dest_group_rank,
                                              ucc_tl_ucp_team_t *team,
                                              ucc_tl_ucp_task_t *task)
{
    ucp_request_param_t req_param;
    ucs_status_ptr_t    ucp_status;
    ucc_status_t        status;
    ucp_ep_h            ep;
    ucp_tag_t           ucp_tag;

    status = ucc_tl_ucp_get_ep(team, dest_group_rank, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    ucp_tag = UCC_TL_UCP_MAKE_SEND_TAG(task->tag, team->rank, team->id,
                                       team->scope_id, team->scope);
    req_param.op_attr_mask =
        UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_DATATYPE |
        UCP_OP_ATTR_FIELD_USER_DATA | UCP_OP_ATTR_FIELD_MEMORY_TYPE;
    req_param.datatype    = ucp_dt_make_contig(msglen);
    req_param.cb.send     = ucc_tl_ucp_send_completion_cb;
    req_param.memory_type = ucc_memtype_to_ucs[mtype];
    req_param.user_data   = (void *)task;
    ucp_status = ucp_tag_send_nbx(ep, buffer, 1, ucp_tag, &req_param);
    task->send_posted++;
    if (UCC_OK != ucp_status) {
        UCC_TL_UCP_CHECK_REQ_STATUS();
    } else {
        task->send_completed++;
    }
    return UCC_OK;
}

static inline ucc_status_t ucc_tl_ucp_put_nb(void * buffer, 
                                             void * target,
                                             size_t msglen,
                                             ucc_rank_t dest_group_rank,
                                             ucc_tl_ucp_team_t * team,
											 ucc_tl_ucp_task_t * task)
{
    ucp_request_param_t req_param;
    ucs_status_ptr_t    ucp_status;
    ucc_status_t        status;
    ucp_ep_h            ep;
    ucp_rkey_h          rkey;
    uint64_t            rva, lva;
    ucc_tl_ucp_remote_info_t *rinfo, *linfo;

    // get the endpoint or create if doesn't exist
    status = ucc_tl_ucp_get_ep(team, dest_group_rank, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    status = ucc_tl_ucp_resolve_p2p_by_va(team, target, &ep, dest_group_rank, &rinfo, &linfo);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    rva = (uint64_t) rinfo->va_base;
    rkey = rinfo->rkey;

    // get local base virtual address to compute offset on 
    // remote virtual address
    /*
    lva = (uint64_t) target;
    status = ucc_tl_ucp_get_bva(team,
                                team->rank,
                                (uint64_t *) &lva);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
*/
#if 0
    // get the rkey for an endpoint, if doesn't exist unpack rkey
    // to endpoint
    status = ucc_tl_ucp_get_rkey(team,
                                 dest_group_rank,
                                &rkey);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    if (NULL == rkey) {
        status = ucc_tl_ucp_ep_unpack_rkey(team,
                                           dest_group_rank,
                                           &ep,
                                           &rkey);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
    }
                                      
    // get remote base virtual address for upcoming operation
    status = ucc_tl_ucp_get_bva(team,
                                dest_group_rank,
                                (uint64_t *) &rva);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    // get local base virtual address to compute offset on 
    // remote virtual address
    status = ucc_tl_ucp_get_bva(team,
                                team->rank,
                                (uint64_t *) &lva);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
#endif

    // compute offset
    rva = rva + ((uint64_t )target - (uint64_t)linfo->va_base);
#if 0
    req_param.op_attr_mask =
        UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_DATATYPE |
        UCP_OP_ATTR_FIELD_USER_DATA ;
    req_param.datatype    = ucp_dt_make_contig(msglen);
    req_param.cb.send     = ucc_tl_ucp_send_completion_cb;
    req_param.user_data   = (void *)task;
#endif
    // issue operation
//    ucp_status = ucp_put_nbx(ep, buffer, 1, rva, rkey, &req_param);
    status = ucp_put_nbi(ep, buffer, msglen, rva, rkey);


    task->send_posted++;
    if (UCC_OK != status) {
//        UCC_TL_UCP_CHECK_REQ_STATUS();
    } else {
        task->send_completed++;
    }
    return UCC_OK;
}

static inline ucc_status_t ucc_tl_ucp_recv_nb(void *buffer, size_t msglen,
                                              ucc_memory_type_t mtype,
                                              ucc_rank_t dest_group_rank,
                                              ucc_tl_ucp_team_t *team,
                                              ucc_tl_ucp_task_t *task)
{
    ucp_request_param_t req_param;
    ucs_status_ptr_t    ucp_status;
    ucp_tag_t           ucp_tag, ucp_tag_mask;

    UCC_TL_UCP_MAKE_RECV_TAG(ucp_tag, ucp_tag_mask, task->tag, dest_group_rank,
                             team->id, team->scope_id, team->scope);
    req_param.op_attr_mask =
        UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_DATATYPE |
        UCP_OP_ATTR_FIELD_USER_DATA | UCP_OP_ATTR_FIELD_MEMORY_TYPE;
    req_param.datatype    = ucp_dt_make_contig(msglen);
    req_param.cb.recv     = ucc_tl_ucp_recv_completion_cb;
    req_param.memory_type = ucc_memtype_to_ucs[mtype];
    req_param.user_data   = (void *)task;
    ucp_status = ucp_tag_recv_nbx(UCC_TL_UCP_WORKER(team), buffer, 1, ucp_tag,
                                  ucp_tag_mask, &req_param);
    task->recv_posted++;
    if (UCC_OK != ucp_status) {
        UCC_TL_UCP_CHECK_REQ_STATUS();
    } else {
        task->recv_completed++;
    }
    return UCC_OK;
}

static inline ucc_status_t ucc_tl_ucp_get_nb(void * buffer, 
                                             void * target,
                                             size_t msglen,
                                             ucc_rank_t dest_group_rank,
                                             ucc_tl_ucp_team_t * team,
											 ucc_tl_ucp_task_t * task)
{
    ucp_request_param_t req_param;
    ucs_status_ptr_t    ucp_status;
    ucc_status_t        status;
    ucp_ep_h            ep;
    ucp_rkey_h          rkey;
    uint64_t            rva, lva;

    // get the endpoint or create if doesn't exist
    status = ucc_tl_ucp_get_ep(team, dest_group_rank, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    
    // get the rkey for an endpoint, if doesn't exist unpack rkey
    // to endpoint
    status = ucc_tl_ucp_get_rkey(team,
                                 dest_group_rank,
                                &rkey);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    if (NULL == rkey) {
        status = ucc_tl_ucp_ep_unpack_rkey(team,
                                           dest_group_rank,
                                           &ep,
                                           &rkey);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
    }
                                      
    // get remote base virtual address for upcoming operation
    status = ucc_tl_ucp_get_bva(team,
                                dest_group_rank,
                                (uint64_t *) &rva);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    // get local base virtual address to compute offset on 
    // remote virtual address
    status = ucc_tl_ucp_get_bva(team,
                                team->rank,
                                (uint64_t *) &lva);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    // compute offset
    rva = rva + ((uint64_t )target - lva);

    req_param.op_attr_mask =
        UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_DATATYPE |
        UCP_OP_ATTR_FIELD_USER_DATA ;
    req_param.datatype    = ucp_dt_make_contig(msglen);
    req_param.cb.send     = ucc_tl_ucp_send_completion_cb;
    req_param.user_data   = (void *)task;
    ucp_status = ucp_get_nbx(ep, buffer, 1, rva, rkey, &req_param);

    task->send_posted++;
    if (UCC_OK != ucp_status) {
        UCC_TL_UCP_CHECK_REQ_STATUS();
    } else {
        task->send_completed++;
    }
    return UCC_OK;
}

#define UCPCHECK_GOTO(_cmd, _task, _label)                                     \
    do {                                                                       \
        ucc_status_t _status = (_cmd);                                         \
        if (UCC_OK != _status) {                                               \
            _task->super.super.status = _status;                               \
            goto _label;                                                       \
        }                                                                      \
    } while (0)

#endif
