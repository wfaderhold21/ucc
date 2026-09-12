/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "tl_ucp_task.h"
#include "tl_ucp_coll.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"
#include "utils/ucc_malloc.h"
#include "utils/ucc_debug.h"

/*
 * Shared one-sided (RMA) infrastructure.
 *
 * I1 — Symmetric offsets: ucc_tl_ucp_resolve_p2p_by_va() maps a local VA to
 * the peer's segment by offset, so all participating buffers must sit at the
 * same offset within each rank's registered segment / mem handle.
 *
 * I4 — Ordering: a ucp_put_nbx is NOT guaranteed visible before a later
 * ucp_atomic_op_nbx on the same endpoint. ucc_tl_ucp_put_signal() does
 * put -> ep_flush -> atomic_add, in that order, and nothing should open-code
 * put+signal instead.
 *
 * I5 — Completion: put completion means the source is reusable, not that
 * data landed; flush completion means data is visible on the peer; a get
 * completion means the data is in the local destination.
 *
 * I7 — Monotonic slots: signal slots are never reset to zero. Each task
 * computes expected = team->onesided_slot_base[slot] + k at post time and
 * commits the base on completion (see tl_ucp_onesided.h).
 */

/*
 * Shared argument validation (I2, I3). Unmet preconditions are a normal
 * fallback path, so they are logged at debug level.
 */
ucc_status_t ucc_tl_ucp_onesided_check_args(ucc_base_coll_args_t *coll_args,
                                            ucc_tl_ucp_team_t   *team,
                                            uint64_t              reqs)
{
    const ucc_coll_args_t *args = &coll_args->args;

    if (UCC_IS_INPLACE(*args)) {
        tl_debug(UCC_TL_TEAM_LIB(team), "in-place is not supported");
        return UCC_ERR_NOT_SUPPORTED;
    }
    if (!(reqs & UCC_TL_UCP_ONESIDED_REQ_NO_DATA)) {
        if (!ucc_coll_args_is_predefined_dt(args, UCC_RANK_INVALID)) {
            tl_debug(UCC_TL_TEAM_LIB(team),
                     "user-defined datatype is not supported");
            return UCC_ERR_NOT_SUPPORTED;
        }
        if (!(args->mask & UCC_COLL_ARGS_FIELD_FLAGS) ||
            !(args->flags & UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS)) {
            tl_debug(UCC_TL_TEAM_LIB(team),
                     "non memory-mapped buffers are not supported");
            return UCC_ERR_NOT_SUPPORTED;
        }
    }
    if (reqs & UCC_TL_UCP_ONESIDED_REQ_GWB) {
        if (!(args->mask & UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER)) {
            tl_debug(UCC_TL_TEAM_LIB(team),
                     "global work buffer not provided nor associated with team");
            return UCC_ERR_NOT_SUPPORTED;
        }
    }
    if (reqs & UCC_TL_UCP_ONESIDED_REQ_SRC_GLOBAL) {
        if (!(args->mask & UCC_COLL_ARGS_FIELD_MEM_MAP_SRC_MEMH)) {
            tl_debug(UCC_TL_TEAM_LIB(team),
                     "global memory handle for src buffers not provided");
            return UCC_ERR_NOT_SUPPORTED;
        }
        if (!(args->flags & UCC_COLL_ARGS_FLAG_SRC_MEMH_GLOBAL)) {
            tl_debug(UCC_TL_TEAM_LIB(team),
                     "src memory handle is not global");
            return UCC_ERR_NOT_SUPPORTED;
        }
    }
    if (reqs & UCC_TL_UCP_ONESIDED_REQ_DST_GLOBAL) {
        if (!(args->mask & UCC_COLL_ARGS_FIELD_MEM_MAP_DST_MEMH)) {
            tl_debug(UCC_TL_TEAM_LIB(team),
                     "global memory handle for dst buffers not provided");
            return UCC_ERR_NOT_SUPPORTED;
        }
        if (!(args->flags & UCC_COLL_ARGS_FLAG_DST_MEMH_GLOBAL)) {
            tl_debug(UCC_TL_TEAM_LIB(team),
                     "dst memory handle is not global");
            return UCC_ERR_NOT_SUPPORTED;
        }
    }

    /* Normalize absent memh fields, exactly as the algorithm inits used to. */
    if (!(args->mask & UCC_COLL_ARGS_FIELD_MEM_MAP_SRC_MEMH)) {
        coll_args->args.src_memh.global_memh = NULL;
    }
    if (!(args->mask & UCC_COLL_ARGS_FIELD_MEM_MAP_DST_MEMH)) {
        coll_args->args.dst_memh.global_memh = NULL;
    }
    return UCC_OK;
}

/*
 * One-sided scratch allocator (plan 6.1). The context's scratch segment is
 * partitioned into N_REGIONS equal regions; the team's region is indexed by
 * its deterministic ordinal (scratch_id, identical on every rank), so the
 * returned pointer sits at the same offset on every rank (I1) and two teams'
 * regions never overlap. The per-team refcount admits at most one in-flight
 * reduction per team so a second cannot clobber the first.
 */
ucc_status_t ucc_tl_ucp_onesided_scratch_alloc(ucc_tl_ucp_team_t *team,
                                               size_t             size,
                                               void             **ptr)
{
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);
    int                   id  = team->scratch_id;
    size_t                region, offset;

    if (ctx->scratch_seg < 0 || NULL == ctx->scratch) {
        tl_debug(UCC_TL_TEAM_LIB(team),
                 "one-sided scratch segment is disabled");
        return UCC_ERR_NOT_SUPPORTED;
    }
    if (id < 0 || id >= UCC_TL_UCP_ONESIDED_SCRATCH_N_REGIONS) {
        tl_debug(UCC_TL_TEAM_LIB(team),
                 "scratch region index %d out of range (%d)",
                 id, UCC_TL_UCP_ONESIDED_SCRATCH_N_REGIONS);
        return UCC_ERR_NOT_SUPPORTED;
    }
    region = ctx->scratch_size / UCC_TL_UCP_ONESIDED_SCRATCH_N_REGIONS;
    offset = (size_t)id * region;
    if (region < size) {
        tl_debug(UCC_TL_TEAM_LIB(team),
                 "scratch request %zu exceeds region size %zu", size, region);
        return UCC_ERR_NOT_SUPPORTED;
    }
    if (team->scratch_refcount > 0) {
        tl_debug(UCC_TL_TEAM_LIB(team),
                 "scratch region already in use by an in-flight reduction");
        return UCC_ERR_NOT_SUPPORTED;
    }

    team->scratch_refcount++;
    *ptr = PTR_OFFSET(ctx->scratch, offset);
    return UCC_OK;
}

void ucc_tl_ucp_onesided_scratch_release(ucc_tl_ucp_team_t *team)
{
    if (team->scratch_refcount > 0) {
        team->scratch_refcount--;
    }
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
 * THE ordering-safe publish: put -> ep_flush -> atomic_add(1) on the peer's
 * slot. The signal state machine lives in a small malloc'd context chained
 * through UCX completion callbacks, so the put's local completion triggers
 * the flush, and the flush's completion triggers the atomic. No busy-wait,
 * no open-coded put+signal.
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

/*
 * Pacing / flow-control window init. Mirrors alltoall_onesided's token
 * formula derived from ucp_ep_evaluate_perf.
 */
ucc_status_t ucc_tl_ucp_onesided_window_init(ucc_tl_ucp_task_t *task,
                                             size_t             msg_size,
                                             ucc_rank_t         concurrency,
                                             ucc_tl_ucp_onesided_window_t *win)
{
    ucc_tl_ucp_team_t  *team     = TASK_TEAM(task);
    size_t              perc_bw  =
        UCC_TL_UCP_TEAM_LIB(team)->cfg.onesided_percent_bw;
    ucp_ep_h                     ep;
    ucp_ep_evaluate_perf_param_t param;
    ucp_ep_evaluate_perf_attr_t  attr;
    double                       rate;
    size_t                       ratio;

    if (concurrency < 1) {
        concurrency = 1;
    }
    if (perc_bw > 100) {
        perc_bw = 100;
    } else if (perc_bw == 0) {
        perc_bw = 1;
    }

    win->tokens = 1;
    win->npolls = task->n_polls;
    if (0 == UCC_TL_TEAM_SIZE(team)) {
        return UCC_OK;
    }

    param.field_mask   = UCP_EP_PERF_PARAM_FIELD_MESSAGE_SIZE;
    attr.field_mask    = UCP_EP_PERF_ATTR_FIELD_ESTIMATED_TIME;
    param.message_size = msg_size;
    if (UCC_OK !=
        ucc_tl_ucp_get_ep(
            team, (UCC_TL_TEAM_RANK(team) + 1) % UCC_TL_TEAM_SIZE(team), &ep)) {
        return UCC_OK; /* pacing window of 1 is safe */
    }
    ucp_ep_evaluate_perf(ep, &param, &attr);

    rate  = (1 / attr.estimated_time) * (double)(perc_bw / 100.0);
    ratio = (msg_size > 0) ? msg_size * concurrency : 1;
    win->tokens = (uint32_t)(rate / ratio);
    if (win->tokens < 1) {
        win->tokens = 1;
    }
    return UCC_OK;
}
