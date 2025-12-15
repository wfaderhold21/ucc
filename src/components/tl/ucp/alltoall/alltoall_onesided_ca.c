/**
 * Copyright (c) 2021-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided alltoall with congestion avoidance.
 * Based on upstream UCC onesided implementation with:
 * - Token-based flow control (limits concurrent operations)
 * - RTT threshold-based peer selection (skips high-latency peers, retries later)
 */

#include "config.h"
#include "tl_ucp.h"
#include "alltoall.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_malloc.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_congestion.h"

#define CONGESTION_THRESHOLD 8

/* Common helper function to check completion and handle polling */
static inline int alltoall_onesided_ca_handle_completion(
    ucc_tl_ucp_task_t *task, uint32_t *posted, uint32_t *completed,
    uint32_t nreqs, int64_t npolls)
{
    int64_t polls = 0;

    if ((*posted - *completed) >= nreqs) {
        while (polls < npolls) {
            ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
            ++polls;
            if ((*posted - *completed) < nreqs) {
                break;
            }
        }
        if (polls >= npolls) {
            return 0; /* Return 0 to indicate should return */
        }
    }
    return 1; /* Return 1 to indicate should continue */
}

/* Common helper function to wait for all operations to complete */
static inline void alltoall_onesided_ca_wait_completion(ucc_tl_ucp_task_t *task,
                                                        int64_t npolls)
{
    int64_t polls = 0;

    if (!UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
        while (polls++ < npolls) {
            ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
            if (UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
                task->super.status = UCC_OK;
                return;
            }
        }
        return;
    }
    task->super.status = UCC_OK;
}

/* GET progress with RTT threshold-based peer selection and segmentation */
void ucc_tl_ucp_alltoall_onesided_ca_get_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t    *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t    *team      = TASK_TEAM(task);
    ucc_tl_ucp_context_t *ctx       = TASK_CTX(task);
    ptrdiff_t             src       = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t             dest      = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    ucc_rank_t            grank     = UCC_TL_TEAM_RANK(team);
    ucc_rank_t            gsize     = UCC_TL_TEAM_SIZE(team);
    uint32_t              ntokens   = task->alltoall_onesided_ca.tokens;
    int64_t               npolls    = task->alltoall_onesided_ca.npolls;
    int                   use_rtt   = ctx->cfg.enable_rtt_threshold_mode;
    uint8_t              *peer_done = task->alltoall_onesided_ca.peer_done;
    ucc_mem_map_mem_h    *dst_memh  = TASK_ARGS(task).src_memh.global_memh;
    uint32_t             *posted    = &task->onesided.get_posted;
    uint32_t             *completed = &task->onesided.get_completed;
    size_t                segment_size = 16384;//task->onesided.segment_size;
    ucc_mem_map_mem_h     src_memh;
    size_t                nelems;
    double                start_time = 0.0, end_time;
    uint32_t              get_before;
    ucc_rank_t            peer, i;
    int                   made_progress;
    int                   use_segments;

    nelems   = TASK_ARGS(task).src.info.count;
    nelems   = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    src_memh = (TASK_ARGS(task).flags & UCC_COLL_ARGS_FLAG_DST_MEMH_GLOBAL)
                   ? TASK_ARGS(task).dst_memh.global_memh[grank]
                   : TASK_ARGS(task).dst_memh.local_memh;

    /* Use segmentation if RTT mode is enabled and message is large enough */
    use_segments = use_rtt && (segment_size > 0) && (nelems > segment_size);

    /* Multi-pass loop until all peers are done */
    while (task->alltoall_onesided_ca.peers_completed < gsize) {
        made_progress = 0;
        
        for (i = 0; i < gsize; i++) {
            peer = (grank + i + 1) % gsize;
            
            /* Skip if already completed */
            if (peer_done[peer]) {
                continue;
            }
            
            /* RTT threshold check - skip high latency peers */
            if (use_rtt && ucc_tl_ucp_should_skip_peer(task, peer, ctx)) {
                continue;
            }

            if (use_segments) {
                /* Segmented transfer for large messages */
                size_t offset = 0;
                size_t remaining = nelems;
                
                while (remaining > 0) {
                    size_t chunk = ucc_min(segment_size, remaining);
                    
                    /* Check if we should skip this peer between segments */
                    if (offset > 0 && ucc_tl_ucp_should_skip_peer(task, peer, ctx)) {
                        /* Peer became congested mid-transfer, defer rest */
                        break;
                    }
                    
                    /* Ensure we don't exceed token limit */
                    while ((*posted - *completed) >= ntokens) {
                        ucp_worker_progress(ctx->worker.ucp_worker);
                    }

                    get_before = *posted;
                    start_time = ucc_get_time();

                    UCPCHECK_GOTO(ucc_tl_ucp_get_nb(PTR_OFFSET(dest, peer * nelems + offset),
                                                    PTR_OFFSET(src, grank * nelems + offset),
                                                    chunk, peer, src_memh, dst_memh, team,
                                                    task),
                                  task, out);

                    /* Wait for this segment to complete */
                    while (*completed <= get_before) {
                        ucp_worker_progress(ctx->worker.ucp_worker);
                    }
                    
                    end_time = ucc_get_time();
                    ucc_tl_ucp_update_peer_rtt(task, peer, end_time - start_time);
                    
                    offset += chunk;
                    remaining -= chunk;
                }
                
                /* Check if we completed all segments for this peer */
                if (remaining > 0) {
                    /* Didn't finish - skip to next peer, will retry later */
                    continue;
                }
            } else {
                /* Non-segmented transfer */
                /* Ensure we don't exceed token limit */
                while ((*posted - *completed) >= ntokens) {
                    ucp_worker_progress(ctx->worker.ucp_worker);
                }

                get_before = *posted;
                if (use_rtt) {
                    start_time = ucc_get_time();
                }

                UCPCHECK_GOTO(ucc_tl_ucp_get_nb(PTR_OFFSET(dest, peer * nelems),
                                                PTR_OFFSET(src, grank * nelems),
                                                nelems, peer, src_memh, dst_memh, team,
                                                task),
                              task, out);

                /* Wait for this specific op to complete */
                while (*completed <= get_before) {
                    ucp_worker_progress(ctx->worker.ucp_worker);
                }
                
                if (use_rtt) {
                    end_time = ucc_get_time();
                    ucc_tl_ucp_update_peer_rtt(task, peer, end_time - start_time);
                }
            }

            /* Mark peer as done */
            peer_done[peer] = 1;
            task->alltoall_onesided_ca.peers_completed++;
            made_progress = 1;

            tl_trace(UCC_TL_TEAM_LIB(team),
                     "GET peer %u done: %u/%u completed (segmented=%d)",
                     peer, task->alltoall_onesided_ca.peers_completed, gsize,
                     use_segments);
        }
        
        /* If no progress was made and not all peers done, we need to force some */
        if (!made_progress && task->alltoall_onesided_ca.peers_completed < gsize) {
            /* Progress worker to potentially update RTT stats */
            ucp_worker_progress(ctx->worker.ucp_worker);
        }
    }

    alltoall_onesided_ca_wait_completion(task, npolls);
    task->super.status = UCC_OK;
out:
    return;
}

/* PUT progress with RTT threshold-based peer selection and segmentation */
void ucc_tl_ucp_alltoall_onesided_ca_put_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t    *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t    *team      = TASK_TEAM(task);
    ucc_tl_ucp_context_t *ctx       = TASK_CTX(task);
    ptrdiff_t             src       = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t             dest      = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    ucc_rank_t            grank     = UCC_TL_TEAM_RANK(team);
    ucc_rank_t            gsize     = UCC_TL_TEAM_SIZE(team);
    uint32_t              ntokens   = task->alltoall_onesided_ca.tokens;
    int64_t               npolls    = task->alltoall_onesided_ca.npolls;
    int                   use_rtt   = ctx->cfg.enable_rtt_threshold_mode;
    uint8_t              *peer_done = task->alltoall_onesided_ca.peer_done;
    ucc_mem_map_mem_h    *dst_memh  = TASK_ARGS(task).dst_memh.global_memh;
    uint32_t             *posted    = &task->onesided.put_posted;
    uint32_t             *completed = &task->onesided.put_completed;
    size_t                segment_size = 16384;//task->onesided.segment_size;
    ucc_mem_map_mem_h     src_memh;
    size_t                nelems;
    double                start_time = 0.0, end_time;
    uint32_t              put_before;
    ucc_rank_t            peer, i;
    int                   made_progress;
    int                   use_segments;

    nelems   = TASK_ARGS(task).src.info.count;
    nelems   = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    src_memh = (TASK_ARGS(task).flags & UCC_COLL_ARGS_FLAG_SRC_MEMH_GLOBAL)
                   ? TASK_ARGS(task).src_memh.global_memh[grank]
                   : TASK_ARGS(task).src_memh.local_memh;

    //printf("use_rtt: %d, segment_size: %zu, nelems: %zu\n", use_rtt, segment_size, nelems);
    /* Use segmentation if RTT mode is enabled and message is large enough */
    use_segments = use_rtt && (segment_size > 0) && (nelems > segment_size);

    /* Multi-pass loop until all peers are done */
    while (task->alltoall_onesided_ca.peers_completed < gsize) {
        made_progress = 0;
        
        for (i = 0; i < gsize; i++) {
            peer = (grank + i + 1) % gsize;
            
            /* Skip if already completed */
            if (peer_done[peer]) {
                continue;
            }
            
            /* RTT threshold check - skip high latency peers */
            if (use_rtt && ucc_tl_ucp_should_skip_peer(task, peer, ctx)) {
                continue;
            }

            if (use_segments) {
                /* Segmented transfer for large messages */
                size_t offset = 0;
                size_t remaining = nelems;
                
                while (remaining > 0) {
                    size_t chunk = ucc_min(segment_size, remaining);
                    
                    /* Check if we should skip this peer between segments */
                    if (offset > 0 && ucc_tl_ucp_should_skip_peer(task, peer, ctx)) {
                        /* Peer became congested mid-transfer, defer rest */
                        break;
                    }
                    
                    /* Ensure we don't exceed token limit */
                    while ((*posted - *completed) >= ntokens) {
                        ucp_worker_progress(ctx->worker.ucp_worker);
                    }

                    put_before = *posted;
                    start_time = ucc_get_time();

                    UCPCHECK_GOTO(
                        ucc_tl_ucp_put_nb(PTR_OFFSET(src, peer * nelems + offset),
                                          PTR_OFFSET(dest, grank * nelems + offset), 
                                          chunk, peer, src_memh, dst_memh, team, task),
                        task, out);
                    UCPCHECK_GOTO(ucc_tl_ucp_ep_flush(peer, team), task, out);

                    /* Wait for this segment to complete */
                    while (*completed <= put_before) {
                        ucp_worker_progress(ctx->worker.ucp_worker);
                    }
                    
                    end_time = ucc_get_time();
                    ucc_tl_ucp_update_peer_rtt(task, peer, end_time - start_time);
                    
                    offset += chunk;
                    remaining -= chunk;
                }
                
                /* Check if we completed all segments for this peer */
                if (remaining > 0) {
                    /* Didn't finish - skip to next peer, will retry later */
                    continue;
                }
            } else {
                /* Non-segmented transfer */
                /* Ensure we don't exceed token limit */
                while ((*posted - *completed) >= ntokens) {
                    ucp_worker_progress(ctx->worker.ucp_worker);
                }

                put_before = *posted;
                if (use_rtt) {
                    start_time = ucc_get_time();
                }

                UCPCHECK_GOTO(
                    ucc_tl_ucp_put_nb(PTR_OFFSET(src, peer * nelems),
                                      PTR_OFFSET(dest, grank * nelems), nelems,
                                      peer, src_memh, dst_memh, team, task),
                    task, out);
                UCPCHECK_GOTO(ucc_tl_ucp_ep_flush(peer, team), task, out);

                /* Wait for this specific op to complete */
                while (*completed <= put_before) {
                    ucp_worker_progress(ctx->worker.ucp_worker);
                }
                
                if (use_rtt) {
                    end_time = ucc_get_time();
                    ucc_tl_ucp_update_peer_rtt(task, peer, end_time - start_time);
                }
            }

            /* Mark peer as done */
            peer_done[peer] = 1;
            task->alltoall_onesided_ca.peers_completed++;
            made_progress = 1;

            tl_trace(UCC_TL_TEAM_LIB(team),
                     "PUT peer %u done: %u/%u completed (segmented=%d)",
                     peer, task->alltoall_onesided_ca.peers_completed, gsize, 
                     use_segments);
        }
        
        /* If no progress was made and not all peers done, we need to force some */
        if (!made_progress && task->alltoall_onesided_ca.peers_completed < gsize) {
            /* Progress worker to potentially update RTT stats */
            ucp_worker_progress(ctx->worker.ucp_worker);
        }
    }

    alltoall_onesided_ca_wait_completion(task, npolls);
    task->super.status = UCC_OK;
out:
    return;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_ca_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    
    /* Reset peer tracking state for this collective */
    if (task->alltoall_onesided_ca.peer_done) {
        memset(task->alltoall_onesided_ca.peer_done, 0, 
               UCC_TL_TEAM_SIZE(team) * sizeof(uint8_t));
    }
    task->alltoall_onesided_ca.peers_completed = 0;
    
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_ca_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t    *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_context_t *ctx  = TASK_CTX(task);
    ucc_status_t          status;

    /* Cleanup RTT tracking if it was initialized */
    if (ctx->cfg.enable_rtt_threshold_mode) {
        ucc_tl_ucp_cleanup_peer_rtt_tracking(task);
    }
    
    /* Cleanup peer_done bitmap */
    if (task->alltoall_onesided_ca.peer_done) {
        ucc_free(task->alltoall_onesided_ca.peer_done);
        task->alltoall_onesided_ca.peer_done = NULL;
    }

    status = ucc_tl_ucp_coll_finalize(coll_task);
    if (ucc_unlikely(UCC_OK != status)) {
        tl_error(UCC_TASK_LIB(coll_task), "failed to finalize collective");
    }
    return status;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_ca_init(ucc_base_coll_args_t *coll_args,
                                                  ucc_base_team_t *team,
                                                  ucc_coll_task_t **task_h)
{
    ucc_tl_ucp_team_t    *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_context_t *ctx     = UCC_TL_UCP_TEAM_CTX(tl_team);
    ucc_tl_ucp_task_t    *task;
    ucc_status_t          status;
    size_t                nelems;
    ucc_sbgp_t           *sbgp;
    int64_t               npolls;
    ucc_rank_t            gsize;

    ALLTOALL_TASK_CHECK(coll_args->args, tl_team);

    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_FLAGS) ||
        (coll_args->args.mask & UCC_COLL_ARGS_FIELD_FLAGS &&
            (!(coll_args->args.flags &
               UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS)))) {
        tl_error(UCC_TL_TEAM_LIB(tl_team),
                 "non memory mapped buffers are not supported");
        status = UCC_ERR_NOT_SUPPORTED;
        goto out;
    }

    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_MEM_MAP_SRC_MEMH)) {
        coll_args->args.src_memh.global_memh = NULL;
    } else {
        if (!(coll_args->args.flags & UCC_COLL_ARGS_FLAG_SRC_MEMH_GLOBAL)) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                "onesided_ca alltoall requires global memory handles for src buffers");
            status = UCC_ERR_INVALID_PARAM;
            goto out;
        }
    }

    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_MEM_MAP_DST_MEMH)) {
        coll_args->args.dst_memh.global_memh = NULL;
    } else {
        if (!(coll_args->args.flags & UCC_COLL_ARGS_FLAG_DST_MEMH_GLOBAL)) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                "onesided_ca alltoall requires global memory handles for dst buffers");
            status = UCC_ERR_INVALID_PARAM;
            goto out;
        }
    }

    task = ucc_tl_ucp_init_task(coll_args, team);
    task->super.finalize = ucc_tl_ucp_alltoall_onesided_ca_finalize;
    *task_h = &task->super;
    
    gsize = UCC_TL_TEAM_SIZE(tl_team);

    /* Allocate peer_done bitmap */
    task->alltoall_onesided_ca.peer_done = ucc_calloc(gsize, sizeof(uint8_t), 
                                                      "peer_done");
    if (!task->alltoall_onesided_ca.peer_done) {
        tl_error(UCC_TL_TEAM_LIB(tl_team), "failed to allocate peer_done bitmap");
        status = UCC_ERR_NO_MEMORY;
        goto out;
    }
    task->alltoall_onesided_ca.peers_completed = 0;

    /* Get node subgroup for congestion threshold check */
    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);

    /* Calculate tokens - limit concurrent operations */
    nelems = TASK_ARGS(task).src.info.count;
    nelems = nelems / gsize;

    {
        size_t perc_bw = UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_percent_bw;
        
        /* Clamp to valid range */
        if (perc_bw > 100) {
            perc_bw = 100;
        } else if (perc_bw == 0) {
            perc_bw = 1;
        }
        
        /* Calculate tokens based on bandwidth percentage and node size */
        task->alltoall_onesided_ca.tokens = 1;
        if (sbgp && sbgp->status == UCC_SBGP_ENABLED && sbgp->group_size > 0) {
            /* Scale tokens: higher perc_bw = more tokens, larger node = fewer tokens */
            uint32_t base_tokens = (CONGESTION_THRESHOLD * perc_bw) / 100;
            task->alltoall_onesided_ca.tokens = 
                ucc_max(1, base_tokens / sbgp->group_size);
        }
    }

    /* Set npolls based on message size */
    npolls = nelems * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    if (npolls < task->n_polls) {
        npolls = task->n_polls;
    }
    task->alltoall_onesided_ca.npolls = npolls;

    /* Initialize RTT threshold tracking and segmentation if enabled */
    if (ctx->cfg.enable_rtt_threshold_mode) {
        status = ucc_tl_ucp_init_peer_rtt_tracking(task, gsize);
        if (status != UCC_OK) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                     "failed to initialize RTT tracking");
            ucc_free(task->alltoall_onesided_ca.peer_done);
            task->alltoall_onesided_ca.peer_done = NULL;
            goto out;
        }
        
        /* Initialize segment size from config for segmented transfers */
        task->onesided.segment_size = ctx->cfg.rtt_segment_size;
        
        tl_debug(UCC_TL_TEAM_LIB(tl_team),
                 "onesided_ca RTT threshold mode: threshold=%.2f us, max_skip=%u, segment_size=%zu",
                 ctx->cfg.rtt_threshold_us, ctx->cfg.max_peer_skip_attempts,
                 task->onesided.segment_size);
    }

    task->super.post = ucc_tl_ucp_alltoall_onesided_ca_start;

    /* Choose GET or PUT based on config or node size */
    {
        ucc_tl_ucp_alltoall_onesided_alg_t alg = 
            UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_alg;
        int use_get;
        
        if (alg == UCC_TL_UCP_ALLTOALL_ONESIDED_GET) {
            use_get = 1;
        } else if (alg == UCC_TL_UCP_ALLTOALL_ONESIDED_PUT) {
            use_get = 0;
        } else {
            /* AUTO: Use GET for larger node sizes to reduce congestion */
            use_get = (sbgp && sbgp->status == UCC_SBGP_ENABLED && 
                       sbgp->group_size >= CONGESTION_THRESHOLD);
        }

        if (use_get) {
            task->super.progress = ucc_tl_ucp_alltoall_onesided_ca_get_progress;
            tl_debug(UCC_TL_TEAM_LIB(tl_team),
                     "onesided_ca using GET: alg=%d, node_size=%d, tokens=%u, npolls=%ld, rtt_mode=%d",
                     alg, sbgp ? sbgp->group_size : 0, task->alltoall_onesided_ca.tokens, 
                     task->alltoall_onesided_ca.npolls, ctx->cfg.enable_rtt_threshold_mode);
        } else {
            task->super.progress = ucc_tl_ucp_alltoall_onesided_ca_put_progress;
            tl_debug(UCC_TL_TEAM_LIB(tl_team),
                     "onesided_ca using PUT: alg=%d, node_size=%d, tokens=%u, npolls=%ld, rtt_mode=%d",
                     alg, sbgp ? sbgp->group_size : 0, task->alltoall_onesided_ca.tokens,
                     task->alltoall_onesided_ca.npolls, ctx->cfg.enable_rtt_threshold_mode);
        }
    }

    status = UCC_OK;
out:
    return status;
}
