/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "tl_ucp.h"
#include "tl_ucp_tag.h"
#include "tl_ucp_ep.h"
#include "tl_ucp_sendrecv.h"
#include <time.h>

/* Monotonic nanosecond clock.  UCX does not install ucs/time/time.h (no
 * ucs_get_time) in HPC-X, and ucc_get_time() is gettimeofday (wall clock,
 * NTP-skewable), which is unsuitable for RTT measurement. */
static inline uint64_t ucc_tl_ucp_quality_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* EWMA smoothing factor applied to RTT, RTT^2, and throughput samples. */
#define UCC_TL_UCP_QUALITY_EWMA_ALPHA 0.25

/* Message size used to derive the static bandwidth baseline. */
#define UCC_TL_UCP_QUALITY_BW_SAMPLE_MSG (1 << 20)

/* Throughput is judged only while the link recently carried user traffic;
 * an idle peer must not be falsely DEGRADED. */
#define UCC_TL_UCP_QUALITY_BW_FRESH_NS (1000000000ULL) /* 1 s */

/* Context-level probe traffic uses a fixed scope/id so it never collides with
 * user teams (which use tags < UCC_TL_UCP_MAX_COLL_TAG and their own ids). */
#define UCC_TL_UCP_QUALITY_PROBE_SCOPE    0
#define UCC_TL_UCP_QUALITY_PROBE_SCOPE_ID 0
#define UCC_TL_UCP_QUALITY_PROBE_ID       0

static inline ucc_rank_t
ucc_tl_ucp_quality_my_rank(ucc_tl_ucp_context_t *ctx)
{
    return ctx->super.super.ucc_context->rank;
}

static const char *ucc_tl_ucp_quality_state_name(ucc_tl_ucp_quality_state_t s)
{
    switch (s) {
    case UCC_TL_UCP_QUALITY_HEALTHY: return "HEALTHY";
    case UCC_TL_UCP_QUALITY_DEGRADED: return "DEGRADED";
    case UCC_TL_UCP_QUALITY_DEAD:     return "DEAD";
    default:                          return "?";
    }
}

static inline ucp_tag_t ucc_tl_ucp_quality_make_tag(uint16_t tag,
                                                    ucc_rank_t sender)
{
    return UCC_TL_UCP_MAKE_TAG(0, tag, sender, UCC_TL_UCP_QUALITY_PROBE_ID,
                               UCC_TL_UCP_QUALITY_PROBE_SCOPE_ID,
                               UCC_TL_UCP_QUALITY_PROBE_SCOPE);
}

/* Match the probe tag/scope/id exactly, but any sender (sender bits masked). */
static inline ucp_tag_t ucc_tl_ucp_quality_probe_tag_mask(void)
{
    return ~((uint64_t)UCC_MASK(UCC_TL_UCP_SENDER_BITS)
             << UCC_TL_UCP_SENDER_BITS_OFFSET);
}

static void ucc_tl_ucp_quality_post_req_recv(ucc_tl_ucp_context_t *ctx);
static void ucc_tl_ucp_quality_post_echo_recv(ucc_tl_ucp_context_t *ctx);

/* Probe request callback: echo the sender's timestamp back unchanged. */
static void ucc_tl_ucp_quality_req_recv_cb(void *request, ucs_status_t status,
                                           const ucp_tag_recv_info_t *info,
                                           void *user_data)
{
    ucc_tl_ucp_context_t *ctx = user_data;
    ucc_rank_t            sender;
    ucp_ep_h              ep;
    ucp_request_param_t   req_param = {0};
    ucp_tag_t             tag;
    ucs_status_ptr_t      s;

    if (status == UCS_OK) {
        sender  = UCC_TL_UCP_GET_SENDER(info->sender_tag);
        /* ucp_tag_send_nbx is async and may reference the buffer after this
         * callback returns, so the echoed ts must live in persistent storage,
         * not on the stack. */
        ctx->quality.echo_payload = ctx->quality.probe_req_ts;
        if (sender < ctx->quality.n_ranks) {
            ep = ctx->service_worker.eps[sender];
            if (ep != NULL) {
                tag = ucc_tl_ucp_quality_make_tag(UCC_TL_UCP_PROBE_ECHO_TAG,
                                                  ucc_tl_ucp_quality_my_rank(
                                                      ctx));
                req_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
                req_param.datatype =
                    ucp_dt_make_contig(sizeof(uint64_t));
                s = ucp_tag_send_nbx(ep, &ctx->quality.echo_payload, 1, tag,
                                     &req_param);
                if (UCS_PTR_IS_ERR(s)) {
                    tl_debug(ctx->super.super.lib,
                             "probe echo send failed: %s",
                             ucs_status_string(UCS_PTR_STATUS(s)));
                }
            }
        }
    }
    ucp_request_free(request);
    ucc_tl_ucp_quality_post_req_recv(ctx);
}

/* Probe echo callback: compute RTT for the sender and update its EWMA. */
static void ucc_tl_ucp_quality_echo_recv_cb(void *request, ucs_status_t status,
                                            const ucp_tag_recv_info_t *info,
                                            void *user_data)
{
    ucc_tl_ucp_context_t      *ctx = user_data;
    ucc_tl_ucp_peer_quality_t *q;
    ucc_rank_t                 sender;
    double                     rtt;
    uint64_t                   now;

    if (status == UCS_OK) {
        sender = UCC_TL_UCP_GET_SENDER(info->sender_tag);
        if (sender < ctx->quality.n_ranks) {
            q = &ctx->quality.peers[sender];
            if (q->probe_send_ts != 0) {
                now = ucc_tl_ucp_quality_now();
                rtt = (double)(now - ctx->quality.probe_echo_ts) * 1e-9;
                if (q->rtt_ewma == 0.0) {
                    q->rtt_ewma    = rtt;
                    q->rtt_sq_ewma = rtt * rtt;
                } else {
                    q->rtt_ewma += UCC_TL_UCP_QUALITY_EWMA_ALPHA *
                                   (rtt - q->rtt_ewma);
                    q->rtt_sq_ewma += UCC_TL_UCP_QUALITY_EWMA_ALPHA *
                                      (rtt * rtt - q->rtt_sq_ewma);
                }
                q->probe_send_ts = 0;
                tl_debug(ctx->super.super.lib,
                         "quality: echo from rank %u rtt=%.3fus "
                         "rtt_ewma=%.3fus",
                         sender, rtt * 1e6, q->rtt_ewma * 1e6);
            }
        }
    }
    ucp_request_free(request);
    ucc_tl_ucp_quality_post_echo_recv(ctx);
}

static void ucc_tl_ucp_quality_post_req_recv(ucc_tl_ucp_context_t *ctx)
{
    ucp_request_param_t req_param = {0};
    ucp_tag_t           tag, mask;
    ucs_status_ptr_t    s;

    tag  = ucc_tl_ucp_quality_make_tag(UCC_TL_UCP_PROBE_REQ_TAG, 0);
    mask = ucc_tl_ucp_quality_probe_tag_mask();
    req_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                             UCP_OP_ATTR_FIELD_DATATYPE |
                             UCP_OP_ATTR_FIELD_USER_DATA;
    req_param.datatype  = ucp_dt_make_contig(sizeof(uint64_t));
    req_param.cb.recv   = ucc_tl_ucp_quality_req_recv_cb;
    req_param.user_data = ctx;
    s = ucp_tag_recv_nbx(ctx->service_worker.ucp_worker,
                         &ctx->quality.probe_req_ts, 1, tag, mask,
                         &req_param);
    if (UCS_PTR_IS_ERR(s)) {
        tl_debug(ctx->super.super.lib, "failed to post probe req recv: %s",
                 ucs_status_string(UCS_PTR_STATUS(s)));
    }
}

static void ucc_tl_ucp_quality_post_echo_recv(ucc_tl_ucp_context_t *ctx)
{
    ucp_request_param_t req_param = {0};
    ucp_tag_t           tag, mask;
    ucs_status_ptr_t    s;

    tag  = ucc_tl_ucp_quality_make_tag(UCC_TL_UCP_PROBE_ECHO_TAG, 0);
    mask = ucc_tl_ucp_quality_probe_tag_mask();
    req_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                             UCP_OP_ATTR_FIELD_DATATYPE |
                             UCP_OP_ATTR_FIELD_USER_DATA;
    req_param.datatype  = ucp_dt_make_contig(sizeof(uint64_t));
    req_param.cb.recv   = ucc_tl_ucp_quality_echo_recv_cb;
    req_param.user_data = ctx;
    s = ucp_tag_recv_nbx(ctx->service_worker.ucp_worker,
                         &ctx->quality.probe_echo_ts, 1, tag, mask,
                         &req_param);
    if (UCS_PTR_IS_ERR(s)) {
        tl_debug(ctx->super.super.lib, "failed to post probe echo recv: %s",
                 ucs_status_string(UCS_PTR_STATUS(s)));
    }
}

/* Connect the service-worker endpoint to a peer's service worker.  The
 * service_worker.eps[] array is otherwise left empty by the existing code
 * (service endpoints are only connected for service/FT abort teams), so the
 * quality probe must establish them itself using the context address
 * exchange, which carries each rank's service-worker address. */
static ucc_status_t ucc_tl_ucp_quality_connect_peer(ucc_tl_ucp_context_t *ctx,
                                                    ucc_rank_t rank)
{
    ucp_ep_params_t ep_params;
    ucp_ep_h        ep;
    ucs_status_t    status;
    void           *addr;

    if (ctx->service_worker.eps[rank] != NULL) {
        return UCC_OK;
    }

    addr = ucc_get_team_ep_addr(ctx->super.super.ucc_context, NULL, rank,
                                ucc_tl_ucp.super.super.id);
    if (addr == NULL) {
        tl_debug(ctx->super.super.lib,
                 "quality: no addr for rank %u (id %lu)", rank,
                 ucc_tl_ucp.super.super.id);
        return UCC_ERR_NOT_FOUND;
    }
    addr = TL_UCP_EP_ADDR_WORKER_SERVICE(addr);
    tl_debug(ctx->super.super.lib,
             "quality: connect service ep to rank %u (addr %p)", rank, addr);

    ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address    = (ucp_address_t *)addr;
    status = ucp_ep_create(ctx->service_worker.ucp_worker, &ep_params, &ep);
    if (ucc_unlikely(status != UCS_OK)) {
        tl_debug(ctx->super.super.lib,
                 "quality: failed to connect service ep to rank %u: %s",
                 rank, ucs_status_string(status));
        return ucs_status_to_ucc_status(status);
    }
    ctx->service_worker.eps[rank] = ep;
    return UCC_OK;
}

static void ucc_tl_ucp_quality_send_probe(ucc_tl_ucp_context_t *ctx,
                                          ucc_rank_t rank)
{
    ucp_ep_h            ep = ctx->service_worker.eps[rank];
    ucp_request_param_t req_param = {0};
    ucp_tag_t           tag;
    ucs_status_ptr_t    s;

    if (ctx->quality.peers[rank].probe_send_ts != 0) {
        return;
    }

    if (ep == NULL) {
        if (ucc_tl_ucp_quality_connect_peer(ctx, rank) != UCC_OK) {
            return; /* addresses not exchanged yet; retry next round */
        }
        ep = ctx->service_worker.eps[rank];
    }

    ctx->quality.peers[rank].probe_send_ts = ucc_tl_ucp_quality_now();
    tag = ucc_tl_ucp_quality_make_tag(UCC_TL_UCP_PROBE_REQ_TAG,
                                      ucc_tl_ucp_quality_my_rank(ctx));
    req_param.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
    req_param.datatype = ucp_dt_make_contig(sizeof(uint64_t));
    s = ucp_tag_send_nbx(ep, &ctx->quality.peers[rank].probe_send_ts, 1, tag,
                         &req_param);
    if (UCS_PTR_IS_ERR(s)) {
        ctx->quality.peers[rank].probe_send_ts = 0;
        tl_debug(ctx->super.super.lib, "failed to send probe to rank %u: %s",
                 rank, ucs_status_string(UCS_PTR_STATUS(s)));
    } else {
        tl_debug(ctx->super.super.lib, "quality: sent probe to rank %u", rank);
    }
}

void ucc_tl_ucp_quality_progress(ucc_tl_ucp_context_t *ctx)
{
    ucc_tl_ucp_quality_t *q = &ctx->quality;
    ucc_rank_t            r;
    uint64_t              now;

    if (q->peers == NULL) {
        return;
    }

    now = ucc_tl_ucp_quality_now();
    if (now < q->next_probe_ts) {
        return;
    }
    q->next_probe_ts = now + (uint64_t)ctx->cfg.quality_probe_interval_usec *
                             1000;

    for (r = 0; r < q->n_ranks; r++) {
        if (r != ucc_tl_ucp_quality_my_rank(ctx)) {
            ucc_tl_ucp_quality_send_probe(ctx, r);
        }
    }

    /* Refresh the live per-peer state for observability.  This does NOT drive
     * the shrink/abort decision (that is a separate, later wiring step); it
     * only makes the classifier's output visible each probe round. */
    for (r = 0; r < q->n_ranks; r++) {
        ucc_tl_ucp_quality_state_t st;
        if (r == ucc_tl_ucp_quality_my_rank(ctx)) {
            continue;
        }
        st = ucc_tl_ucp_quality_classify(ctx, r);
        if (st != q->peers[r].state) {
            tl_debug(ctx->super.super.lib,
                     "quality: peer rank %u %s -> %s (rtt_ewma=%.3fus "
                     "bw_ewma=%.2f MB/s err=%u)",
                     r, ucc_tl_ucp_quality_state_name(q->peers[r].state),
                     ucc_tl_ucp_quality_state_name(st), q->peers[r].rtt_ewma * 1e6,
                     q->peers[r].throughput_ewma / 1e6, q->peers[r].err_count);
            q->peers[r].state = st;
        }
    }
}

void ucc_tl_ucp_quality_init(ucc_tl_ucp_context_t *ctx)
{
    ucc_tl_ucp_quality_t *q = &ctx->quality;
    ucc_rank_t            n_ranks;
    ucc_rank_t            r;

    if (!ctx->cfg.quality_monitor) {
        return;
    }

    if (ctx->service_worker.ucp_worker == NULL || !UCC_TL_CTX_HAS_OOB(ctx)) {
        tl_warn(ctx->super.super.lib,
                "QUALITY_MONITOR requires SERVICE_WORKER and an OOB context; "
                "disabled");
        return;
    }

    n_ranks = ctx->super.super.ucc_context->params.oob.n_oob_eps;
    q->peers = ucc_calloc(n_ranks, sizeof(*q->peers), "peer_quality");
    if (q->peers == NULL) {
        tl_error(ctx->super.super.lib, "failed to allocate peer_quality");
        return;
    }
    q->n_ranks      = n_ranks;
    q->next_probe_ts = ucc_tl_ucp_quality_now();

    for (r = 0; r < n_ranks; r++) {
        q->peers[r].static_latency = -1.0;
        q->peers[r].static_bw      = -1.0;
        q->peers[r].state          = UCC_TL_UCP_QUALITY_HEALTHY;
    }

    ucc_tl_ucp_quality_post_req_recv(ctx);
    ucc_tl_ucp_quality_post_echo_recv(ctx);
    tl_debug(ctx->super.super.lib, "quality: monitor enabled for %u ranks",
             n_ranks);
}

void ucc_tl_ucp_quality_finalize(ucc_tl_ucp_context_t *ctx)
{
    ucc_free(ctx->quality.peers);
    ctx->quality.peers   = NULL;
    ctx->quality.n_ranks = 0;
}

void ucc_tl_ucp_quality_add_tx(ucc_tl_ucp_context_t *ctx, ucc_rank_t ctx_rank,
                               size_t bytes)
{
    if (ctx->quality.peers != NULL && ctx_rank < ctx->quality.n_ranks) {
        ucc_tl_ucp_peer_quality_t *q = &ctx->quality.peers[ctx_rank];
        ucc_atomic_add64(&q->tx_bytes, bytes);
        if (bytes != 0) {
            q->last_data_ts = ucc_tl_ucp_quality_now();
        }
    }
}

void ucc_tl_ucp_quality_add_rx(ucc_tl_ucp_context_t *ctx, ucc_rank_t ctx_rank,
                               size_t bytes)
{
    if (ctx->quality.peers != NULL && ctx_rank < ctx->quality.n_ranks) {
        ucc_tl_ucp_peer_quality_t *q = &ctx->quality.peers[ctx_rank];
        ucc_atomic_add64(&q->rx_bytes, bytes);
        if (bytes != 0) {
            q->last_data_ts = ucc_tl_ucp_quality_now();
        }
    }
}

void ucc_tl_ucp_quality_mark_error(ucc_tl_ucp_context_t *ctx,
                                   ucc_rank_t ctx_rank)
{
    if (ctx->quality.peers != NULL && ctx_rank < ctx->quality.n_ranks) {
        ucc_atomic_add32(&ctx->quality.peers[ctx_rank].err_count, 1);
    }
}

static void ucc_tl_ucp_quality_update_baseline(ucc_tl_ucp_context_t *ctx,
                                               ucc_rank_t ctx_rank)
{
    ucc_tl_ucp_peer_quality_t    *q = &ctx->quality.peers[ctx_rank];
    ucp_ep_h                      ep;
    ucp_ep_evaluate_perf_param_t  param;
    ucp_ep_evaluate_perf_attr_t   attr;
    double                        latency = 0.0;

    if (q->static_latency >= 0.0 || q->static_bw >= 0.0) {
        return;
    }

    ep = ctx->service_worker.eps[ctx_rank];
    if (ep == NULL) {
        return;
    }

    param.field_mask   = UCP_EP_PERF_PARAM_FIELD_MESSAGE_SIZE;
    attr.field_mask    = UCP_EP_PERF_ATTR_FIELD_ESTIMATED_TIME;
    param.message_size = 0;
    if (ucp_ep_evaluate_perf(ep, &param, &attr) == UCS_OK) {
        latency           = attr.estimated_time;
        q->static_latency = latency;
    }

    param.message_size = UCC_TL_UCP_QUALITY_BW_SAMPLE_MSG;
    if (ucp_ep_evaluate_perf(ep, &param, &attr) == UCS_OK &&
        attr.estimated_time > latency) {
        q->static_bw = (double)UCC_TL_UCP_QUALITY_BW_SAMPLE_MSG /
                       (attr.estimated_time - latency);
    }
}

static void ucc_tl_ucp_quality_update_throughput(ucc_tl_ucp_context_t *ctx,
                                                 ucc_rank_t ctx_rank)
{
    ucc_tl_ucp_peer_quality_t *q = &ctx->quality.peers[ctx_rank];
    uint64_t                   now   = ucc_tl_ucp_quality_now();
    uint64_t                   bytes = q->tx_bytes + q->rx_bytes;
    uint64_t                   dt_ns;
    double                     inst_bw;

    if (q->last_sample_ts == 0) {
        q->last_sample_ts    = now;
        q->last_sample_bytes = bytes;
        return;
    }

    dt_ns = now - q->last_sample_ts;
    if (dt_ns == 0) {
        return;
    }

    inst_bw = (double)(bytes - q->last_sample_bytes) * 1e9 / (double)dt_ns;
    q->last_sample_ts    = now;
    q->last_sample_bytes = bytes;

    if (q->throughput_ewma == 0.0) {
        q->throughput_ewma = inst_bw;
    } else {
        q->throughput_ewma += UCC_TL_UCP_QUALITY_EWMA_ALPHA *
                              (inst_bw - q->throughput_ewma);
    }
}

ucc_tl_ucp_quality_state_t
ucc_tl_ucp_quality_classify(ucc_tl_ucp_context_t *ctx, ucc_rank_t ctx_rank)
{
    ucc_tl_ucp_peer_quality_t *q;
    uint64_t                   now;
    double                     rtt_ratio, bw_ratio;

    if (ctx->quality.peers == NULL || ctx_rank >= ctx->quality.n_ranks) {
        return UCC_TL_UCP_QUALITY_HEALTHY;
    }

    q = &ctx->quality.peers[ctx_rank];

    if (q->err_count >= ctx->cfg.quality_err_threshold) {
        return UCC_TL_UCP_QUALITY_DEGRADED;
    }

    ucc_tl_ucp_quality_update_baseline(ctx, ctx_rank);
    ucc_tl_ucp_quality_update_throughput(ctx, ctx_rank);

    if (q->rtt_ewma > 0.0 && q->static_latency > 0.0) {
        rtt_ratio = q->rtt_ewma / q->static_latency;
        if (rtt_ratio >
            (double)ctx->cfg.quality_rtt_degrade_ratio_pct / 100.0) {
            return UCC_TL_UCP_QUALITY_DEGRADED;
        }
    }

    /* Throughput is only judged for bandwidth-bound workloads, and only while
     * the link recently carried user traffic (otherwise an idle peer reads as
     * ~0 and is falsely DEGRADED).  Gated by QUALITY_BW_CHECK because the
     * static baseline is measured at 1 MiB; small-message/latency-bound
     * workloads inherently sit far below it. */
    if (ctx->cfg.quality_bw_check) {
        now = ucc_tl_ucp_quality_now();
        if (q->throughput_ewma > 0.0 && q->static_bw > 0.0 &&
            (q->last_data_ts != 0) &&
            (now - q->last_data_ts) < UCC_TL_UCP_QUALITY_BW_FRESH_NS) {
            bw_ratio = q->throughput_ewma / q->static_bw;
            if (bw_ratio <
                (double)ctx->cfg.quality_bw_degrade_ratio_pct / 100.0) {
                return UCC_TL_UCP_QUALITY_DEGRADED;
            }
        }
    }

    return UCC_TL_UCP_QUALITY_HEALTHY;
}
