/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_TL_UCP_CONGESTION_H_
#define UCC_TL_UCP_CONGESTION_H_

#include "tl_ucp.h"

/* Forward declaration */
typedef struct ucc_tl_ucp_task ucc_tl_ucp_task_t;

/**
 * Update RTT statistics for a completed segment
 *
 * @param task         Task containing RTT tracking data
 * @param segment_idx  Index of the completed segment
 * @param rtt_sample   Measured RTT for this segment (in seconds)
 */
void ucc_tl_ucp_update_rtt_stats(ucc_tl_ucp_task_t *task, 
                                 uint32_t segment_idx, 
                                 double rtt_sample);

/**
 * Adjust congestion window based on RTT measurement
 *
 * @param task        Task containing congestion window state
 * @param rtt_sample  Measured RTT for current segment (in seconds)
 */
void ucc_tl_ucp_adjust_congestion_window(ucc_tl_ucp_task_t *task, 
                                         double rtt_sample);

/**
 * Initialize congestion control state for a task
 *
 * @param task  Task to initialize
 */
void ucc_tl_ucp_init_congestion_control(ucc_tl_ucp_task_t *task);

/**
 * Cleanup congestion control resources for a task
 *
 * @param task  Task to cleanup
 */
void ucc_tl_ucp_cleanup_congestion_control(ucc_tl_ucp_task_t *task);

/**
 * Initialize per-peer RTT tracking for threshold mode
 *
 * @param task  Task to initialize
 * @param gsize Number of peers in the team
 */
ucc_status_t ucc_tl_ucp_init_peer_rtt_tracking(ucc_tl_ucp_task_t *task, 
                                               ucc_rank_t gsize);

/**
 * Update RTT measurement for a specific peer
 *
 * @param task      Task containing peer RTT data
 * @param peer_rank Rank of the peer
 * @param rtt       Measured RTT in seconds
 */
void ucc_tl_ucp_update_peer_rtt(ucc_tl_ucp_task_t *task, 
                                ucc_rank_t peer_rank, 
                                double rtt);

/**
 * Check if a peer should be skipped based on RTT threshold
 *
 * @param task      Task containing peer RTT data
 * @param peer_rank Rank of the peer
 * @param ctx       Context containing threshold configuration
 * @return          1 if peer should be skipped, 0 otherwise
 */
int ucc_tl_ucp_should_skip_peer(ucc_tl_ucp_task_t *task,
                                ucc_rank_t peer_rank,
                                ucc_tl_ucp_context_t *ctx);

/**
 * Cleanup per-peer RTT tracking resources
 *
 * @param task  Task to cleanup
 */
void ucc_tl_ucp_cleanup_peer_rtt_tracking(ucc_tl_ucp_task_t *task);

#endif /* UCC_TL_UCP_CONGESTION_H_ */

