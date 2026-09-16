/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */


#include "tl_ucp_sendrecv.h"
#include "utils/ucc_malloc.h"

void ucc_tl_ucp_send_recv_counter_inc_st(uint32_t *counter)
{
    ++(*counter);
}

void ucc_tl_ucp_send_recv_counter_inc_mt(uint32_t *counter)
{
    ucc_atomic_add32(counter, 1);
}

void ucc_tl_ucp_send_completion_cb_st(void *request, ucs_status_t status,
                                      void *user_data)
{
    ucc_tl_ucp_task_t *task = (ucc_tl_ucp_task_t *)user_data;
    if (ucc_unlikely(UCS_OK != status)) {
        tl_error(UCC_TASK_LIB(task), "failure in send completion %s",
                 ucs_status_string(status));
        task->super.status = ucs_status_to_ucc_status(status);
    }
    ++task->tagged.send_completed;
    ucp_request_free(request);
}

void ucc_tl_ucp_send_completion_cb_mt(void *request, ucs_status_t status,
                                      void *user_data)
{
    ucc_tl_ucp_task_t *task = (ucc_tl_ucp_task_t *)user_data;
    if (ucc_unlikely(UCS_OK != status)) {
        tl_error(UCC_TASK_LIB(task), "failure in send completion %s",
                 ucs_status_string(status));
        task->super.status = ucs_status_to_ucc_status(status);
    }
    ucc_atomic_add32(&task->tagged.send_completed, 1);
    ucp_request_free(request);
}

void ucc_tl_ucp_put_completion_cb(void *request, ucs_status_t status,
                                  void *user_data)
{
    ucc_tl_ucp_task_t *task = (ucc_tl_ucp_task_t *)user_data;
    if (ucc_unlikely(UCS_OK != status)) {
        tl_error(UCC_TASK_LIB(task), "failure in put completion %s",
                 ucs_status_string(status));
        task->super.status = ucs_status_to_ucc_status(status);
    }
    task->onesided.put_completed++;
    ucp_request_free(request);
}

void ucc_tl_ucp_get_completion_cb(void *request, ucs_status_t status,
                                  void *user_data)
{
    ucc_tl_ucp_task_t *task = (ucc_tl_ucp_task_t *)user_data;
    if (ucc_unlikely(UCS_OK != status)) {
        tl_error(UCC_TASK_LIB(task), "failure in get completion %s",
                 ucs_status_string(status));
        task->super.status = ucs_status_to_ucc_status(status);
    }
    task->onesided.get_completed++;
    ucp_request_free(request);
}

void ucc_tl_ucp_flush_completion_cb(void *request, ucs_status_t status,
                                    void *user_data)
{
    ucc_tl_ucp_task_t *task = (ucc_tl_ucp_task_t *)user_data;
    if (ucc_unlikely(UCS_OK != status)) {
        tl_error(UCC_TASK_LIB(task), "failure in ep flush completion %s",
                 ucs_status_string(status));
        task->super.status = ucs_status_to_ucc_status(status);
    }
    task->flush_completed++;
    ucp_request_free(request);
}

void ucc_tl_ucp_recv_completion_cb_mt(void *request, ucs_status_t status,
                                      const ucp_tag_recv_info_t *info, /* NOLINT */
                                      void *user_data)
{
    ucc_tl_ucp_task_t *task = (ucc_tl_ucp_task_t *)user_data;
    if (ucc_unlikely(UCS_OK != status)) {
        tl_error(UCC_TASK_LIB(task), "failure in recv completion %s",
                 ucs_status_string(status));
        task->super.status = ucs_status_to_ucc_status(status);
    }
    ucc_atomic_add32(&task->tagged.recv_completed, 1);
    ucp_request_free(request);
}

void ucc_tl_ucp_recv_completion_cb_st(void *request, ucs_status_t status,
                                      const ucp_tag_recv_info_t *info, /* NOLINT */
                                      void *user_data)
{
    ucc_tl_ucp_task_t *task = (ucc_tl_ucp_task_t *)user_data;
    if (ucc_unlikely(UCS_OK != status)) {
        tl_error(UCC_TASK_LIB(task), "failure in recv completion %s",
                 ucs_status_string(status));
        task->super.status = ucs_status_to_ucc_status(status);
    }
    ++task->tagged.recv_completed;
    ucp_request_free(request);
}

ucc_status_t ucc_tl_ucp_send_nbx(void *buffer, size_t msglen,
                                 ucc_rank_t dest_group_rank,
                                 const ucp_request_param_t *req_param,
                                 ucc_tl_ucp_task_t *task)
{
    const ucc_coll_args_t *args = &TASK_ARGS(task);
    ucc_tl_ucp_team_t     *team = TASK_TEAM(task);
    ucc_status_t           status;
    ucp_ep_h               ep;
    ucp_tag_t              ucp_tag;
    ucs_status_ptr_t       ucp_status;

    status = ucc_tl_ucp_get_ep(team, dest_group_rank, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    ucp_tag = UCC_TL_UCP_MAKE_SEND_TAG((args->mask & UCC_COLL_ARGS_FIELD_TAG),
                                       task->tagged.tag, UCC_TL_TEAM_RANK(team),
                                       team->super.super.params.id,
                                       team->super.super.params.scope_id,
                                       team->super.super.params.scope);
    ucp_status = ucp_tag_send_nbx(ep, buffer, msglen, ucp_tag, req_param);
    task->tagged.send_posted++;

    if (UCS_OK != ucp_status) {
        UCC_TL_UCP_CHECK_REQ_STATUS();
    } else {
        UCC_TL_UCP_TEAM_CTX(team)->sendrecv_cbs.p2p_counter_inc(
            &task->tagged.send_completed);
    }

    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_recv_nbx(void *buffer, size_t msglen,
                                 ucc_rank_t dest_group_rank,
                                 const ucp_request_param_t *req_param,
                                 ucc_tl_ucp_task_t *task)
{
    const ucc_coll_args_t *args = &TASK_ARGS(task);
    ucc_tl_ucp_team_t     *team = TASK_TEAM(task);
    ucp_tag_t              ucp_tag, ucp_tag_mask;
    ucs_status_ptr_t       ucp_status;


    UCC_TL_UCP_MAKE_RECV_TAG(ucp_tag, ucp_tag_mask,
                             (args->mask & UCC_COLL_ARGS_FIELD_TAG),
                             task->tagged.tag, dest_group_rank,
                             team->super.super.params.id,
                             team->super.super.params.scope_id,
                             team->super.super.params.scope);

    ucp_status = ucp_tag_recv_nbx(team->worker->ucp_worker, buffer, msglen,
                                  ucp_tag, ucp_tag_mask, req_param);
    task->tagged.recv_posted++;

    if (UCS_OK != ucp_status) {
        UCC_TL_UCP_CHECK_REQ_STATUS();
    } else {
        UCC_TL_UCP_TEAM_CTX(team)->sendrecv_cbs.p2p_counter_inc(
            &task->tagged.recv_completed);
    }
    return UCC_OK;
}

/*
 * Accounted atomic post: ucp_atomic_op_nbx with a completion callback that
 * bumps the task's get counters (onesided ops are accounted in the tagged
 * union's word slots, keeping the 4-word aliasing intact).
 */
void ucc_tl_ucp_atomic_add_completion_cb(void *request, ucs_status_t status,
                                         void *user_data)
{
    ucc_tl_ucp_task_t *task = (ucc_tl_ucp_task_t *)user_data;

    if (ucc_unlikely(UCS_OK != status)) {
        tl_error(UCC_TASK_LIB(task), "failure in atomic add completion %s",
                 ucs_status_string(status));
        task->super.status = ucs_status_to_ucc_status(status);
    }
    task->onesided.get_completed++;
    ucp_request_free(request);
}

static ucc_status_t ucc_tl_ucp_atomic_add_nb(long *          local_slot,
                                             long            value,
                                             ucc_rank_t      peer,
                                             ucc_mem_map_mem_h *memh,
                                             ucc_tl_ucp_team_t *team,
                                             ucc_tl_ucp_task_t *task)
{
    ucp_request_param_t req_param = {0};
    int                 segment   = 0;
    ucp_rkey_h          rkey      = NULL;
    uint64_t            rva       = 0;
    ucs_status_ptr_t    ucp_status;
    ucc_status_t        status;
    ucp_ep_h            ep;
    uint64_t            val       = (uint64_t)value;

    status = ucc_tl_ucp_get_ep(team, peer, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    status = ucc_tl_ucp_resolve_p2p_by_va(team, local_slot, &ep, peer, &rva,
                                          &rkey, &segment, memh, task);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    req_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                             UCP_OP_ATTR_FIELD_USER_DATA |
                             UCP_OP_ATTR_FIELD_DATATYPE;
    req_param.cb.send      = ucc_tl_ucp_atomic_add_completion_cb;
    req_param.user_data    = (void *)task;
    req_param.datatype     = ucp_dt_make_contig(sizeof(uint64_t));

    ucp_status = ucp_atomic_op_nbx(ep, UCP_ATOMIC_OP_ADD, &val, 1, rva, rkey,
                                   &req_param);
    task->onesided.get_posted++;
    if (UCS_OK != ucp_status) {
        if (UCS_PTR_IS_ERR(ucp_status)) {
            return ucs_status_to_ucc_status(UCS_PTR_STATUS(ucp_status));
        }
    } else {
        task->onesided.get_completed++;
    }
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_atomic_add(long *          local_slot,
                                   long            value,
                                   ucc_rank_t      peer,
                                   ucc_mem_map_mem_h *memh,
                                   ucc_tl_ucp_team_t *team,
                                   ucc_tl_ucp_task_t *task)
{
    return ucc_tl_ucp_atomic_add_nb(local_slot, value, peer, memh, team,
                                    task);
}

/*
 * Ordering-safe publish: put -> ep_flush -> atomic_add(1) on the peer's slot.
 * The signal state machine lives in a small malloc'd context chained through
 * UCX completion callbacks, so the put's local completion triggers the flush,
 * and the flush's completion triggers the atomic. No busy-wait, no open-coded
 * put+signal.
 */
typedef struct ucc_tl_ucp_signal_req {
    ucc_tl_ucp_task_t   *task;
    ucc_rank_t           peer;
    long                *slot;  /* local symmetric slot (atomic target) */
    ucc_mem_map_mem_h  *memh;   /* global dst memh, or NULL for segments */
} ucc_tl_ucp_signal_req_t;

static void ucc_tl_ucp_signal_atomic(ucc_tl_ucp_signal_req_t *sig)
{
    ucc_status_t status;

    status = ucc_tl_ucp_atomic_add_nb(sig->slot, 1, sig->peer, sig->memh,
                                      TASK_TEAM(sig->task), sig->task);
    ucc_free(sig);
    if (ucc_unlikely(UCC_OK != status)) {
        tl_error(UCC_TL_TEAM_LIB(TASK_TEAM(sig->task)),
                 "failed to post atomic add of the signal: %s",
                 ucc_status_string(status));
    }
}

static void ucc_tl_ucp_signal_flush_cb(void *request, ucs_status_t status,
                                       void *user_data)
{
    ucc_tl_ucp_signal_req_t *sig  = (ucc_tl_ucp_signal_req_t *)user_data;
    ucc_tl_ucp_task_t       *task = sig->task;
    ucp_request_free(request);
    if (ucc_unlikely(UCS_OK != status)) {
        tl_error(UCC_TASK_LIB(task),
                 "failure in ep flush completion (signal) %s",
                 ucs_status_string(status));
        task->super.status = ucs_status_to_ucc_status(status);
        ucc_free(sig);
        return;
    }
    task->flush_completed++;
    ucc_tl_ucp_signal_atomic(sig);
}

static void ucc_tl_ucp_signal_post_flush(ucc_tl_ucp_signal_req_t *sig,
                                         ucp_ep_h                 ep)
{
    ucc_tl_ucp_task_t     *task      = sig->task;
    ucp_request_param_t    req_param = {0};
    ucs_status_ptr_t       req;

    /* Put's local completion does not mean data landed (I5); flush before
     * signaling (I4). */
    req_param.op_attr_mask =
        UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    req_param.cb.send   = ucc_tl_ucp_signal_flush_cb;
    req_param.user_data = (void *)sig;
    req                 = ucp_ep_flush_nbx(ep, &req_param);
    task->flush_posted++;
    if (UCS_OK != req) {
        if (UCS_PTR_IS_ERR(req)) {
            task->super.status = ucs_status_to_ucc_status(UCS_PTR_STATUS(req));
            ucc_free(sig);
        }
        /* else: in-progress — the callback will fire. */
    } else {
        /* Flush completed immediately: the callback won't fire. */
        task->flush_completed++;
        ucc_tl_ucp_signal_atomic(sig);
    }
}

static void ucc_tl_ucp_signal_put_cb(void *request, ucs_status_t status,
                                     void *user_data)
{
    ucc_tl_ucp_signal_req_t *sig  = (ucc_tl_ucp_signal_req_t *)user_data;
    ucc_tl_ucp_task_t       *task = sig->task;
    ucc_status_t             st;
    ucp_ep_h                 ep;

    ucp_request_free(request);
    if (ucc_unlikely(UCS_OK != status)) {
        tl_error(UCC_TASK_LIB(task), "failure in put completion (signal) %s",
                 ucs_status_string(status));
        task->super.status = ucs_status_to_ucc_status(status);
        ucc_free(sig);
        return;
    }
    task->onesided.put_completed++;
    st = ucc_tl_ucp_get_ep(TASK_TEAM(task), sig->peer, &ep);
    if (ucc_unlikely(UCC_OK != st)) {
        task->super.status = st;
        ucc_free(sig);
        return;
    }
    ucc_tl_ucp_signal_post_flush(sig, ep);
}

ucc_status_t ucc_tl_ucp_put_signal(void *             src,
                                   void *             dst,
                                   size_t             len,
                                   ucc_memory_type_t  mtype,
                                   ucc_rank_t         peer,
                                   long *             slot,
                                   ucc_mem_map_mem_h  src_memh,
                                   ucc_mem_map_mem_h *dst_memh,
                                   ucc_tl_ucp_team_t  *team,
                                   ucc_tl_ucp_task_t  *task)
{
    ucc_tl_ucp_signal_req_t *sig;
    ucp_request_param_t      req_param = {0};
    ucs_status_ptr_t         ucp_status;
    ucc_status_t             status;
    ucp_ep_h                 ep;
    uint64_t                 rva       = 0;
    ucp_rkey_h               rkey      = NULL;
    int                      segment   = 0;
    void                     *ucp_memh = NULL;

    sig = (ucc_tl_ucp_signal_req_t *)ucc_malloc(sizeof(*sig), "ucp_signal");
    if (ucc_unlikely(NULL == sig)) {
        return UCC_ERR_NO_MEMORY;
    }
    sig->task = task;
    sig->peer = peer;
    sig->slot = slot;
    sig->memh = dst_memh;

    status = ucc_tl_ucp_get_ep(team, peer, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        ucc_free(sig);
        return status;
    }
    if (src_memh) {
        status = ucc_tl_ucp_get_memh(team, src_memh, &ucp_memh);
        if (ucc_unlikely(UCC_OK != status)) {
            ucc_free(sig);
            return status;
        }
    }
    status = ucc_tl_ucp_resolve_p2p_by_va(team, dst, &ep, peer, &rva, &rkey,
                                          &segment, dst_memh, task);
    if (ucc_unlikely(UCC_OK != status)) {
        ucc_free(sig);
        return status;
    }

    req_param.op_attr_mask =
        UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA |
        UCP_OP_ATTR_FIELD_MEMORY_TYPE;
    req_param.cb.send     = ucc_tl_ucp_signal_put_cb;
    req_param.user_data   = (void *)sig;
    req_param.memory_type = ucc_memtype_to_ucs[mtype];
    if (ucp_memh) {
        req_param.op_attr_mask |= UCP_OP_ATTR_FIELD_MEMH;
        req_param.memh = ucp_memh;
    }
    ucp_status = ucp_put_nbx(ep, src, len, rva, rkey, &req_param);
    task->onesided.put_posted++;
    if (UCS_OK != ucp_status) {
        if (UCS_PTR_IS_ERR(ucp_status)) {
            ucc_free(sig);
            return ucs_status_to_ucc_status(UCS_PTR_STATUS(ucp_status));
        }
    } else {
        /* Put completed immediately: the callback won't fire, so drive the
         * chain (flush -> atomic) by hand. */
        task->onesided.put_completed++;
        ucc_tl_ucp_signal_post_flush(sig, ep);
    }
    return UCC_OK;
}
