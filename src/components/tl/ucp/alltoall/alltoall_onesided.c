/**
 * Copyright (c) 2021-2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "alltoall.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_malloc.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_congestion.h"

void ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask);

ucc_status_t ucc_tl_ucp_alltoall_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t    *task     = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t    *team     = TASK_TEAM(task);
    ucc_tl_ucp_context_t *ctx      = TASK_CTX(task);
    ptrdiff_t             src      = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t             dest     = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t                nelems   = TASK_ARGS(task).src.info.count;
    ucc_rank_t            grank    = UCC_TL_TEAM_RANK(team);
    ucc_rank_t            gsize    = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t            start    = (grank + 1) % gsize;
    long                 *pSync    = TASK_ARGS(task).global_work_buffer;
    ucc_mem_map_mem_h     src_memh = TASK_ARGS(task).src_memh.local_memh;
    ucc_mem_map_mem_h    *dst_memh = TASK_ARGS(task).dst_memh.global_memh;
    ucc_rank_t            peer;
    int                   use_rtt_congestion_control;
    int                   use_rtt_threshold_mode;
    ucc_status_t          seg_status;
    ucc_status_t          status;

    if (TASK_ARGS(task).flags & UCC_COLL_ARGS_FLAG_SRC_MEMH_GLOBAL) {
        src_memh = TASK_ARGS(task).src_memh.global_memh[grank];
    }

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);

    /* TODO: change when support for library-based work buffers is complete */
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest   = dest + grank * nelems;

    /* Determine which RTT mode to use */
    use_rtt_congestion_control = ctx->cfg.enable_rtt_congestion_control &&
                                 !ctx->cfg.enable_rtt_threshold_mode &&
                                 (nelems >= ctx->cfg.rtt_segment_size);
    use_rtt_threshold_mode = ctx->cfg.enable_rtt_congestion_control &&
                             ctx->cfg.enable_rtt_threshold_mode;

    if (use_rtt_congestion_control) {
        /* Window-based congestion control mode */
        ucc_tl_ucp_init_congestion_control(task);
        tl_debug(UCC_TL_TEAM_LIB(team),
                 "RTT congestion control enabled for alltoall: "
                 "nelems=%zu, segment_size=%zu, initial_cwnd=%.2f",
                 nelems, task->onesided.segment_size,
                 task->onesided.congestion_window);
    } else if (use_rtt_threshold_mode) {
        /* Threshold-based peer selection mode */
        status = ucc_tl_ucp_init_peer_rtt_tracking(task, gsize);
        if (status != UCC_OK) {
            task->super.status = status;
            goto out;
        }
        tl_debug(UCC_TL_TEAM_LIB(team),
                 "RTT threshold mode enabled for alltoall: "
                 "nelems=%zu, threshold=%.2f us, max_skip=%u",
                 nelems, ctx->cfg.rtt_threshold_us,
                 ctx->cfg.max_peer_skip_attempts);
    }

    if (use_rtt_threshold_mode) {
        /* Threshold mode: Skip high-RTT peers and retry them later.
         * Only one PUT in flight at a time to accurately measure RTT. */
        uint32_t peers_completed = 0;
        uint32_t max_passes = ctx->cfg.max_peer_skip_attempts + 1;
        uint32_t pass       = 0;
        uint8_t *peer_done  = NULL;
        
        /* Allocate bitmap to track completed peers */
        peer_done = ucc_calloc(gsize, sizeof(uint8_t), "peer_done");
        if (!peer_done) {
            task->super.status = UCC_ERR_NO_MEMORY;
            goto out;
        }

        while (peers_completed < gsize && pass < max_passes) {
            for (peer = 0; peer < gsize; peer++) {
                ucc_rank_t dest_peer = (start + peer) % gsize;
                double     start_time, end_time;
                uint32_t   put_before;

                /* Skip if already completed */
                if (peer_done[dest_peer]) {
                    continue;
                }

                /* Check if we should skip this peer based on RTT */
                if (ucc_tl_ucp_should_skip_peer(task, dest_peer, ctx)) {
                    continue;
                }

                /* Ensure only one PUT in flight at a time */
                while (task->onesided.put_posted > task->onesided.put_completed) {
                    ucp_worker_progress(team->worker->ucp_worker);
                }

                /* Record state before posting */
                put_before = task->onesided.put_posted;
                start_time = ucc_get_time();

                UCPCHECK_GOTO(
                    ucc_tl_ucp_put_nb((void *)(src + dest_peer * nelems),
                                      (void *)dest, nelems, dest_peer, src_memh,
                                      dst_memh, team, task),
                    task, out_free);

                /* Flush the endpoint to ensure data is sent */
                UCPCHECK_GOTO(ucc_tl_ucp_ep_flush(dest_peer, team), task, out_free);

                /* Wait for this specific PUT to complete */
                while (task->onesided.put_completed <= put_before) {
                    ucp_worker_progress(team->worker->ucp_worker);
                }

                end_time = ucc_get_time();
                ucc_tl_ucp_update_peer_rtt(task, dest_peer,
                                           end_time - start_time);

                UCPCHECK_GOTO(
                    ucc_tl_ucp_atomic_inc(pSync, dest_peer, dst_memh, team),
                    task, out_free);

                /* Mark this peer as done */
                peer_done[dest_peer] = 1;
                peers_completed++;

                tl_trace(UCC_TL_TEAM_LIB(team),
                         "Peer %u completed: RTT=%.2f us, %u/%u done",
                         dest_peer, task->onesided.peer_rtt[dest_peer],
                         peers_completed, gsize);
            }
            pass++;
            
            if (peers_completed < gsize) {
                tl_debug(UCC_TL_TEAM_LIB(team),
                         "Pass %u complete: %u/%u peers done, retrying skipped",
                         pass, peers_completed, gsize);
            }
        }
        
        if (peers_completed < gsize) {
            tl_warn(UCC_TL_TEAM_LIB(team),
                    "Not all peers completed after %u passes (%u/%u)", pass,
                    peers_completed, gsize);
        }
        
        ucc_free(peer_done);
        goto done;
        
out_free:
        ucc_free(peer_done);
        goto out;
        
    } else {
        /* Window-based or no congestion control */
        peer = start;
        while (peer < start + gsize) {
            ucc_rank_t dest_peer = peer % gsize;

            /* Use segmented PUT if congestion control is enabled and message is large */
            if (use_rtt_congestion_control &&
                nelems > ctx->cfg.rtt_segment_size) {
                /* Keep posting segments until all are sent for this peer */
                do {
                    seg_status = ucc_tl_ucp_put_nb_segmented(
                        (void *)(src + dest_peer * nelems), (void *)dest,
                        nelems, dest_peer, src_memh, dst_memh, team, task);
                    if (seg_status == UCC_INPROGRESS) {
                        /* Congestion window full, progress the worker to complete some segments */
                        ucp_worker_progress(team->worker->ucp_worker);
                    } else if (seg_status != UCC_OK) {
                        task->super.status = seg_status;
                        goto out;
                    }
                } while (seg_status == UCC_INPROGRESS);

                /* All segments posted for this peer, reset state for next peer */
                task->onesided.segments_posted = 0;
                task->onesided.bytes_posted    = 0;
            } else {
                /* Use regular PUT for small messages */
                UCPCHECK_GOTO(
                    ucc_tl_ucp_put_nb((void *)(src + dest_peer * nelems),
                                      (void *)dest, nelems, dest_peer, src_memh,
                                      dst_memh, team, task),
                    task, out);
            }
            UCPCHECK_GOTO(
                ucc_tl_ucp_atomic_inc(pSync, dest_peer, dst_memh, team), task,
                out);
            peer++;
        }
    }

done:
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
out:
    return task->super.status;
}

void ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t    *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t    *team  = TASK_TEAM(task);
    ucc_tl_ucp_context_t *ctx   = TASK_CTX(task);
    ucc_rank_t            gsize = UCC_TL_TEAM_SIZE(team);
    long                 *pSync = TASK_ARGS(task).global_work_buffer;

    if (ucc_tl_ucp_test_onesided(task, gsize) == UCC_INPROGRESS) {
        return;
    }

    /* Cleanup congestion control resources if enabled */
    if (ctx->cfg.enable_rtt_congestion_control) {
        if (ctx->cfg.enable_rtt_threshold_mode) {
            ucc_tl_ucp_cleanup_peer_rtt_tracking(task);
        } else {
            ucc_tl_ucp_cleanup_congestion_control(task);
        }
    }

    pSync[0]           = 0;
    task->super.status = UCC_OK;
}
