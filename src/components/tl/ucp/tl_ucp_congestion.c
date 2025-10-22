/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "tl_ucp_congestion.h"
#include "tl_ucp_task.h"
#include "utils/ucc_time.h"
#include "utils/ucc_malloc.h"
#include "utils/ucc_math.h"
#include <math.h>

void ucc_tl_ucp_update_rtt_stats(ucc_tl_ucp_task_t *task, 
                                 uint32_t segment_idx, 
                                 double rtt_sample)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    double alpha = ctx->cfg.rtt_alpha;  /* Default: 0.125 (1/8) */
    double beta = ctx->cfg.rtt_beta;    /* Default: 0.25 (1/4) */
    
    /* RFC 6298: Computing TCP's Retransmission Timer
     * SRTT = (1 - alpha) * SRTT + alpha * RTT
     * RTTVAR = (1 - beta) * RTTVAR + beta * |SRTT - RTT|
     */
    if (task->onesided.sample_count == 0) {
        /* First measurement */
        task->onesided.smoothed_rtt = rtt_sample;
        task->onesided.rtt_variance = rtt_sample / 2.0;
    } else {
        /* Subsequent measurements: EWMA (Exponentially Weighted Moving Average) */
        double rtt_diff = fabs(rtt_sample - task->onesided.smoothed_rtt);
        task->onesided.rtt_variance = 
            (1.0 - beta) * task->onesided.rtt_variance + beta * rtt_diff;
        task->onesided.smoothed_rtt = 
            (1.0 - alpha) * task->onesided.smoothed_rtt + alpha * rtt_sample;
    }
    
    task->onesided.sample_count++;
    
    tl_trace(UCC_TASK_LIB(task),
             "RTT sample %u: %.6f ms, SRTT: %.6f ms, RTTVAR: %.6f ms",
             segment_idx, rtt_sample * 1000.0, 
             task->onesided.smoothed_rtt * 1000.0,
             task->onesided.rtt_variance * 1000.0);
    
    /* Adjust congestion window based on RTT */
    ucc_tl_ucp_adjust_congestion_window(task, rtt_sample);
}

void ucc_tl_ucp_adjust_congestion_window(ucc_tl_ucp_task_t *task, 
                                         double rtt_sample)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    double *cwnd = &task->onesided.congestion_window;
    double *ssthresh = &task->onesided.ssthresh;
    
    /* Detect congestion: RTT significantly higher than smoothed RTT
     * Using 4 standard deviations as threshold (similar to TCP)
     */
    double congestion_threshold = task->onesided.smoothed_rtt + 
                                  4.0 * task->onesided.rtt_variance;
    
    if (rtt_sample > congestion_threshold && task->onesided.sample_count > 1) {
        /* Congestion detected: multiplicative decrease */
        *ssthresh = ucc_max(*cwnd * ctx->cfg.cwnd_reduction_factor, 2.0);
        *cwnd = *ssthresh;
        
        tl_debug(UCC_TASK_LIB(task), 
                 "Congestion detected: RTT=%.6f ms > threshold=%.6f ms, "
                 "reducing cwnd from %.2f to %.2f",
                 rtt_sample * 1000.0, congestion_threshold * 1000.0,
                 *cwnd / ctx->cfg.cwnd_reduction_factor, *cwnd);
    } else {
        /* No congestion: increase window */
        if (*cwnd < *ssthresh) {
            /* Slow start: exponential growth */
            *cwnd += 1.0;
            tl_trace(UCC_TASK_LIB(task), 
                     "Slow start: cwnd=%.2f, ssthresh=%.2f", *cwnd, *ssthresh);
        } else {
            /* Congestion avoidance: linear growth */
            *cwnd += ctx->cfg.cwnd_growth_factor / *cwnd;
            tl_trace(UCC_TASK_LIB(task), 
                     "Congestion avoidance: cwnd=%.2f, ssthresh=%.2f", 
                     *cwnd, *ssthresh);
        }
        
        /* Cap at maximum */
        if (ctx->cfg.max_cwnd > 0 && *cwnd > (double)ctx->cfg.max_cwnd) {
            *cwnd = (double)ctx->cfg.max_cwnd;
            tl_trace(UCC_TASK_LIB(task), "cwnd capped at max: %.2f", *cwnd);
        }
    }
}

void ucc_tl_ucp_init_congestion_control(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    
    /* Initialize congestion control parameters */
    task->onesided.segment_size = ctx->cfg.rtt_segment_size;
    task->onesided.congestion_window = (double)ctx->cfg.initial_cwnd;
    task->onesided.ssthresh = (double)(ctx->cfg.initial_cwnd * 2);
    task->onesided.active_segments = 0;
    task->onesided.smoothed_rtt = 0.0;
    task->onesided.rtt_variance = 0.0;
    task->onesided.sample_count = 0;
    task->onesided.rtt_segments = NULL;
    task->onesided.max_segments = 0;
    
    tl_debug(UCC_TASK_LIB(task),
             "Initialized RTT congestion control: segment_size=%zu, "
             "initial_cwnd=%.2f, ssthresh=%.2f",
             task->onesided.segment_size,
             task->onesided.congestion_window,
             task->onesided.ssthresh);
}

void ucc_tl_ucp_cleanup_congestion_control(ucc_tl_ucp_task_t *task)
{
    if (task->onesided.rtt_segments) {
        tl_debug(UCC_TASK_LIB(task),
                 "Cleaning up RTT segments: collected %u samples, "
                 "final SRTT=%.6f ms, final cwnd=%.2f",
                 task->onesided.sample_count,
                 task->onesided.smoothed_rtt * 1000.0,
                 task->onesided.congestion_window);
        
        ucc_free(task->onesided.rtt_segments);
        task->onesided.rtt_segments = NULL;
        task->onesided.max_segments = 0;
    }
}

ucc_status_t ucc_tl_ucp_init_peer_rtt_tracking(ucc_tl_ucp_task_t *task, 
                                               ucc_rank_t gsize)
{
    size_t i;
    
    task->onesided.peer_rtt = ucc_malloc(gsize * sizeof(double), "peer_rtt");
    if (!task->onesided.peer_rtt) {
        return UCC_ERR_NO_MEMORY;
    }
    
    task->onesided.peer_rtt_timestamp = ucc_malloc(gsize * sizeof(double), 
                                                   "peer_rtt_timestamp");
    if (!task->onesided.peer_rtt_timestamp) {
        ucc_free(task->onesided.peer_rtt);
        task->onesided.peer_rtt = NULL;
        return UCC_ERR_NO_MEMORY;
    }
    
    task->onesided.peer_skip_count = ucc_malloc(gsize * sizeof(uint32_t), 
                                                "peer_skip_count");
    if (!task->onesided.peer_skip_count) {
        ucc_free(task->onesided.peer_rtt);
        ucc_free(task->onesided.peer_rtt_timestamp);
        task->onesided.peer_rtt = NULL;
        task->onesided.peer_rtt_timestamp = NULL;
        return UCC_ERR_NO_MEMORY;
    }
    
    /* Initialize all peers with 0 RTT (no measurement yet) */
    for (i = 0; i < gsize; i++) {
        task->onesided.peer_rtt[i] = 0.0;
        task->onesided.peer_rtt_timestamp[i] = 0.0;
        task->onesided.peer_skip_count[i] = 0;
    }
    
    tl_debug(UCC_TASK_LIB(task),
             "Initialized per-peer RTT tracking for %u peers", gsize);
    
    return UCC_OK;
}

void ucc_tl_ucp_update_peer_rtt(ucc_tl_ucp_task_t *task, 
                                ucc_rank_t peer_rank, 
                                double rtt)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    double alpha = ctx->cfg.rtt_alpha;  /* Smoothing factor */
    double rtt_us = rtt * 1000000.0;    /* Convert to microseconds */
    
    if (!task->onesided.peer_rtt) {
        return;  /* Not initialized */
    }
    
    /* Update with EWMA if we have previous measurements */
    if (task->onesided.peer_rtt[peer_rank] > 0.0) {
        task->onesided.peer_rtt[peer_rank] = 
            (1.0 - alpha) * task->onesided.peer_rtt[peer_rank] + alpha * rtt_us;
    } else {
        /* First measurement */
        task->onesided.peer_rtt[peer_rank] = rtt_us;
    }
    
    task->onesided.peer_rtt_timestamp[peer_rank] = ucc_get_time();
    
    tl_trace(UCC_TASK_LIB(task),
             "Updated peer %u RTT: %.2f us", peer_rank, 
             task->onesided.peer_rtt[peer_rank]);
}

int ucc_tl_ucp_should_skip_peer(ucc_tl_ucp_task_t *task,
                                ucc_rank_t peer_rank,
                                ucc_tl_ucp_context_t *ctx)
{
    if (!task->onesided.peer_rtt) {
        return 0;  /* Not initialized, don't skip */
    }
    
    /* Never skip if we haven't measured RTT yet */
    if (task->onesided.peer_rtt[peer_rank] == 0.0) {
        return 0;
    }
    
    /* Don't skip if we've already skipped too many times */
    if (task->onesided.peer_skip_count[peer_rank] >= 
        ctx->cfg.max_peer_skip_attempts) {
        tl_debug(UCC_TASK_LIB(task),
                 "Peer %u: forcing communication after %u skips (RTT=%.2f us)",
                 peer_rank, task->onesided.peer_skip_count[peer_rank],
                 task->onesided.peer_rtt[peer_rank]);
        return 0;
    }
    
    /* Skip if RTT is above threshold */
    if (task->onesided.peer_rtt[peer_rank] > ctx->cfg.rtt_threshold_us) {
        task->onesided.peer_skip_count[peer_rank]++;
        tl_debug(UCC_TASK_LIB(task),
                 "Peer %u: skipping due to high RTT (%.2f us > %.2f us threshold), "
                 "skip_count=%u",
                 peer_rank, task->onesided.peer_rtt[peer_rank],
                 ctx->cfg.rtt_threshold_us,
                 task->onesided.peer_skip_count[peer_rank]);
        return 1;
    }
    
    /* Reset skip count if RTT is good */
    task->onesided.peer_skip_count[peer_rank] = 0;
    return 0;
}

void ucc_tl_ucp_cleanup_peer_rtt_tracking(ucc_tl_ucp_task_t *task)
{
    if (task->onesided.peer_rtt) {
        tl_debug(UCC_TASK_LIB(task), "Cleaning up per-peer RTT tracking");
        ucc_free(task->onesided.peer_rtt);
        task->onesided.peer_rtt = NULL;
    }
    if (task->onesided.peer_rtt_timestamp) {
        ucc_free(task->onesided.peer_rtt_timestamp);
        task->onesided.peer_rtt_timestamp = NULL;
    }
    if (task->onesided.peer_skip_count) {
        ucc_free(task->onesided.peer_skip_count);
        task->onesided.peer_skip_count = NULL;
    }
}

