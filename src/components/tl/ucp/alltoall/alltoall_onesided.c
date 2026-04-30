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
#include "tl_ucp_sendrecv.h"

#define CONGESTION_THRESHOLD  8
#define SEG_SIZE              4096
#define CA_PROBE_WINDOW_MIN   4
#define CA_RTT_GOOD           4
#define CA_RTT_BAD            16
#define CA_STALL_THRESHOLD    128
#define CA_BULK_THRESHOLD     3
#define CA_BULK_MAX           (1 << 20)

/* Common helper function to check completion and handle polling */
static inline int alltoall_onesided_handle_completion(
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
static inline void alltoall_onesided_wait_completion(ucc_tl_ucp_task_t *task,
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

ucc_status_t ucc_tl_ucp_alltoall_onesided_sched_start(ucc_coll_task_t *ctask)
{
    return ucc_schedule_start(ctask);
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_sched_finalize(ucc_coll_task_t *ctask)
{
    ucc_schedule_t *schedule = ucc_derived_of(ctask, ucc_schedule_t);
    ucc_status_t    status;

    status = ucc_schedule_finalize(ctask);
    ucc_tl_ucp_put_schedule(schedule);
    return status;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_status_t       status;
    ucc_rank_t         i;

    if (task->alltoall_onesided.peer_done) {
        ucc_rank_t gsize = UCC_TL_TEAM_SIZE(TASK_TEAM(task));

        for (i = 0; i < gsize; i++) {
            if (task->alltoall_onesided.probe_req[i]) {
                ucp_request_free(task->alltoall_onesided.probe_req[i]);
            }
        }
        ucc_free(task->alltoall_onesided.peer_done);
        ucc_free(task->alltoall_onesided.probe_req);
        ucc_free(task->alltoall_onesided.peer_skip);
        ucc_free(task->alltoall_onesided.peer_offset);
        ucc_free(task->alltoall_onesided.peer_order);
        ucc_free(task->alltoall_onesided.peer_healthy);
    }
    status = ucc_tl_ucp_coll_finalize(coll_task);
    if (ucc_unlikely(UCC_OK != status)) {
        tl_error(UCC_TASK_LIB(coll_task), "failed to finalize collective");
    }
    return status;
}

void ucc_tl_ucp_alltoall_onesided_get_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team      = TASK_TEAM(task);
    ptrdiff_t          src       = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest      = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    ucc_memory_type_t  mtype     = TASK_ARGS(task).dst.info.mem_type;
    ucc_rank_t         grank     = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize     = UCC_TL_TEAM_SIZE(team);
    uint32_t           ntokens   = task->alltoall_onesided.tokens;
    int64_t            npolls    = task->alltoall_onesided.npolls;
    /* To resolve remote virtual addresses, the dst_memh is the one that must
     * have the rkey information. For this algorithm, we need to swap the
     * src and dst handles to operate correctly */
    ucc_mem_map_mem_h *dst_memh  = TASK_ARGS(task).src_memh.global_memh;
    uint32_t          *posted    = &task->onesided.get_posted;
    uint32_t          *completed = &task->onesided.get_completed;
    ucc_rank_t         peer      = (grank + *posted + 1) % gsize;
    ucc_mem_map_mem_h  src_memh;
    size_t             nelems;

    nelems   = TASK_ARGS(task).src.info.count;
    nelems   = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    src_memh = (TASK_ARGS(task).flags & UCC_COLL_ARGS_FLAG_DST_MEMH_GLOBAL)
                   ? TASK_ARGS(task).dst_memh.global_memh[grank]
                   : TASK_ARGS(task).dst_memh.local_memh;

    for (; *posted < gsize; peer = (peer + 1) % gsize) {
        UCPCHECK_GOTO(ucc_tl_ucp_get_nb(PTR_OFFSET(dest, peer * nelems),
                                        PTR_OFFSET(src, grank * nelems),
                                        nelems, mtype, peer, src_memh, dst_memh,
                                        team, task),
                      task, out);

        if (!alltoall_onesided_handle_completion(task, posted, completed,
                                                 ntokens, npolls)) {
            return;
        }
    }

    alltoall_onesided_wait_completion(task, npolls);
out:
    return;
}

void ucc_tl_ucp_alltoall_onesided_put_seg_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t  *task            = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t  *team            = TASK_TEAM(task);
    ptrdiff_t            src             = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t            dest            = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    ucc_memory_type_t    mtype           = TASK_ARGS(task).src.info.mem_type;
    ucc_rank_t           grank           = UCC_TL_TEAM_RANK(team);
    ucc_rank_t           gsize           = UCC_TL_TEAM_SIZE(team);
    int64_t              npolls          = task->alltoall_onesided.npolls;
    ucc_mem_map_mem_h   *dst_memh        = TASK_ARGS(task).dst_memh.global_memh;
    uint8_t             *peer_done       = task->alltoall_onesided.peer_done;
    uint32_t            *peer_cmp        = &task->alltoall_onesided.peers_completed;
    uint8_t             *peer_skip       = task->alltoall_onesided.peer_skip;
    ucs_status_ptr_t    *probe_req       = task->alltoall_onesided.probe_req;
    size_t              *peer_offset     = task->alltoall_onesided.peer_offset;
    uint32_t             window          = task->alltoall_onesided.tokens;
    uint32_t            *probes_inflight = &task->alltoall_onesided.probes_in_flight;
    ucc_rank_t          *peer_order      = task->alltoall_onesided.peer_order;
    uint8_t             *peer_healthy    = task->alltoall_onesided.peer_healthy;
    ucc_rank_t           peer;
    ucc_rank_t           idx;
    ucc_mem_map_mem_h    src_memh;
    size_t               nelems;
    size_t               offset;
    size_t               chunk;
    int64_t              polls;
    ucp_ep_h             ep;
    ucc_status_t         status;
    ucp_request_param_t  probe_flush_param;

    nelems   = TASK_ARGS(task).src.info.count;
    nelems   = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    src_memh = (TASK_ARGS(task).flags & UCC_COLL_ARGS_FLAG_SRC_MEMH_GLOBAL)
                   ? TASK_ARGS(task).src_memh.global_memh[grank]
                   : TASK_ARGS(task).src_memh.local_memh;

    /*
     * Injection phase: find peers ready for their next chunk (state 0) and
     * post a SEG_SIZE put+flush for each, up to the current window.  Local
     * peers bypass the window entirely — shared memory never touches the NIC.
     * Inline-completing flushes grow the window immediately (fast RTT signal).
     * The window is initialized conservatively and adapts via AIMD as flushes
     * complete in the service phase below.
     */
    for (idx = 0; idx < gsize && *probes_inflight < window; idx++) {
        peer = peer_order[idx];

        if (peer_done[peer]) {
            continue;
        }

        /* Local peer — send all data directly, no windowing needed */
        if (ucc_rank_on_local_node(peer, team->topo)) {
            UCPCHECK_GOTO(
                ucc_tl_ucp_put_nb(PTR_OFFSET(src, peer * nelems),
                                   PTR_OFFSET(dest, grank * nelems),
                                   nelems, mtype, peer, src_memh, dst_memh,
                                   team, task),
                task, out);
            UCPCHECK_GOTO(ucc_tl_ucp_ep_flush(peer, team, task),
                          task, out);
            peer_done[peer] = 3;
            (*peer_cmp)++;
            continue;
        }

        /*
         * Remote peer — post next chunk and a flush to measure RTT.
         * After CA_BULK_THRESHOLD consecutive healthy completions, stop
         * segmenting and send the entire remainder in one shot; the
         * trailing flush still gives us an RTT sample on the final chunk.
         */
        offset = peer_offset[peer];
        if (peer_healthy[peer] >= CA_BULK_THRESHOLD) {
            chunk = ucc_min((size_t)CA_BULK_MAX, nelems - offset);
        } else {
            chunk = ucc_min(SEG_SIZE, nelems - offset);
        }
        UCPCHECK_GOTO(
            ucc_tl_ucp_put_nb(PTR_OFFSET(src, peer * nelems + offset),
                               PTR_OFFSET(dest, grank * nelems + offset),
                               chunk, mtype, peer, src_memh, dst_memh,
                               team, task),
            task, out);

        status = ucc_tl_ucp_get_ep(team, peer, &ep);
        if (ucc_unlikely(UCC_OK != status)) {
            task->super.status = status;
            goto out;
        }
        memset(&probe_flush_param, 0, sizeof(probe_flush_param));
        probe_req[peer]   = ucp_ep_flush_nbx(ep, &probe_flush_param);
        peer_offset[peer] = offset + chunk;

        if (UCS_OK == probe_req[peer]) {
            /* Inline completion: fast RTT, grow window, stay in state 0 */
            probe_req[peer] = NULL;
            window = ucc_min(window + 1, (uint32_t)gsize);
            task->alltoall_onesided.tokens = window;
            if (peer_healthy[peer] < UINT8_MAX) {
                peer_healthy[peer]++;
            }
            if (peer_offset[peer] >= nelems) {
                peer_done[peer] = 3;
                (*peer_cmp)++;
            }
        } else if (UCS_PTR_IS_ERR(probe_req[peer])) {
            task->super.status = ucs_status_to_ucc_status(
                UCS_PTR_STATUS(probe_req[peer]));
            probe_req[peer] = NULL;
            goto out;
        } else {
            peer_done[peer] = 1; /* flush in flight, window slot occupied */
            peer_skip[peer] = 0;
            (*probes_inflight)++;
        }
    }

    /* Drive completions */
    polls = 0;
    while (polls++ < npolls) {
        ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
    }

    /*
     * Service phase: poll each in-flight flush.  On completion apply AIMD:
     * fast RTT (skip < CA_RTT_GOOD) grows the window; slow RTT
     * (skip >= CA_RTT_BAD) halves it.  Return the peer to state 0 for its
     * next chunk, or mark it done.  Stalled peers release their window slot
     * so other peers can proceed; the stalled peer is retried next call.
     */
    for (idx = 0; idx < gsize; idx++) {
        peer = peer_order[idx];

        if (peer_done[peer] != 1) {
            continue;
        }

        if (ucp_request_check_status(probe_req[peer]) == UCS_INPROGRESS) {
            if (peer_skip[peer] < CA_STALL_THRESHOLD) {
                peer_skip[peer]++;
                continue;
            }
            /* Stall: release window slot, retry this peer next call */
        }

        ucp_request_free(probe_req[peer]);
        probe_req[peer] = NULL;
        (*probes_inflight)--;

        /*
         * AIMD: adjust window based on observed flush latency.
         * Track per-peer healthy streaks for the adaptive bulk decision:
         * fast RTT increments, slow RTT resets so a single congested
         * sample knocks the peer back into segmented probing.
         */
        if (peer_skip[peer] < CA_RTT_GOOD) {
            window = ucc_min(window + 1, (uint32_t)gsize);
            if (peer_healthy[peer] < UINT8_MAX) {
                peer_healthy[peer]++;
            }
        } else if (peer_skip[peer] >= CA_RTT_BAD) {
            window = ucc_max(window >> 1, CA_PROBE_WINDOW_MIN);
            peer_healthy[peer] = 0;
        }
        task->alltoall_onesided.tokens = window;

        if (peer_offset[peer] >= nelems) {
            peer_done[peer] = 3;
            (*peer_cmp)++;
        } else {
            peer_done[peer] = 0; /* ready for next chunk */
            peer_skip[peer] = 0;
        }
    }

    if (*peer_cmp < gsize) {
        return;
    }

    alltoall_onesided_wait_completion(task, npolls);
out:
    return;
}

void ucc_tl_ucp_alltoall_onesided_put_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task      = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team      = TASK_TEAM(task);
    ptrdiff_t          src       = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest      = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    ucc_memory_type_t  mtype     = TASK_ARGS(task).src.info.mem_type;
    ucc_rank_t         grank     = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize     = UCC_TL_TEAM_SIZE(team);
    uint32_t           ntokens   = task->alltoall_onesided.tokens;
    int64_t            npolls    = task->alltoall_onesided.npolls;
    ucc_mem_map_mem_h *dst_memh  = TASK_ARGS(task).dst_memh.global_memh;
    uint32_t          *posted    = &task->onesided.put_posted;
    uint32_t          *completed = &task->onesided.put_completed;
    ucc_rank_t         peer      = (grank + *posted + 1) % gsize;
    ucc_mem_map_mem_h  src_memh;
    size_t             nelems;

    nelems   = TASK_ARGS(task).src.info.count;
    nelems   = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    src_memh = (TASK_ARGS(task).flags & UCC_COLL_ARGS_FLAG_SRC_MEMH_GLOBAL)
                   ? TASK_ARGS(task).src_memh.global_memh[grank]
                   : TASK_ARGS(task).src_memh.local_memh;

    for (; *posted < gsize; peer = (peer + 1) % gsize) {
        UCPCHECK_GOTO(
            ucc_tl_ucp_put_nb(PTR_OFFSET(src, peer * nelems),
                              PTR_OFFSET(dest, grank * nelems), nelems, mtype,
                              peer, src_memh, dst_memh, team, task),
            task, out);
        UCPCHECK_GOTO(ucc_tl_ucp_ep_flush(peer, team, task), task, out);

        if (!alltoall_onesided_handle_completion(task, posted, completed,
                                                 ntokens, npolls)) {
            return;
        }
    }

    alltoall_onesided_wait_completion(task, npolls);
out:
    return;
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

ucc_status_t ucc_tl_ucp_alltoall_onesided_init(ucc_base_coll_args_t *coll_args,
                                               ucc_base_team_t *team,
                                               ucc_coll_task_t **task_h)
{
    ucc_schedule_t              *schedule = NULL;
    ucc_tl_ucp_team_t           *tl_team  =
        ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_base_coll_args_t         barrier_coll_args = {
        .team = team->params.team,
        .args.coll_type = UCC_COLL_TYPE_BARRIER,
    };
    size_t                       perc_bw     =
        UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_percent_bw;
    ucc_tl_ucp_alltoall_onesided_alg_t alg   =
        UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_alg;
    ucc_tl_ucp_schedule_t       *tl_schedule = NULL;
    ucc_rank_t                   group_size  = 1;
    ucc_coll_task_t             *barrier_task;
    ucc_coll_task_t             *a2a_task;
    ucc_tl_ucp_task_t           *task;
    ucc_status_t                 status;
    size_t                       nelems;
    double                       rate;
    size_t                       ratio;
    ucp_ep_h                     ep;
    ucp_ep_evaluate_perf_param_t param;
    ucp_ep_evaluate_perf_attr_t  attr;
    int64_t                      npolls;
    ucc_sbgp_t                  *sbgp;

    ALLTOALL_TASK_CHECK(coll_args->args, tl_team);
    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_FLAGS) ||
        (coll_args->args.mask & UCC_COLL_ARGS_FIELD_FLAGS &&
            (!(coll_args->args.flags &
               UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS)))) {
        tl_error(UCC_TL_TEAM_LIB(tl_team),
                 "non memory mapped buffers are not supported");
        status = UCC_ERR_NOT_SUPPORTED;
        return status;
    }

    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_MEM_MAP_SRC_MEMH)) {
        coll_args->args.src_memh.global_memh = NULL;
    } else {
        if (!(coll_args->args.flags & UCC_COLL_ARGS_FLAG_SRC_MEMH_GLOBAL)) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                "onesided alltoall requires global memory handles for src buffers");
            status = UCC_ERR_INVALID_PARAM;
            return status;
        }
    }

    if (!(coll_args->args.mask & UCC_COLL_ARGS_FIELD_MEM_MAP_DST_MEMH)) {
        coll_args->args.dst_memh.global_memh = NULL;
    } else {
        if (!(coll_args->args.flags & UCC_COLL_ARGS_FLAG_DST_MEMH_GLOBAL)) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                "onesided alltoall requires global memory handles for dst buffers");
            status = UCC_ERR_INVALID_PARAM;
            return status;
        }
    }
    status = ucc_tl_ucp_get_schedule(tl_team, coll_args,
                                     (ucc_tl_ucp_schedule_t **)&tl_schedule);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    schedule = &tl_schedule->super.super;
    ucc_schedule_init(schedule, coll_args, team);
    schedule->super.post     = ucc_tl_ucp_alltoall_onesided_sched_start;
    schedule->super.progress = NULL;
    schedule->super.finalize = ucc_tl_ucp_alltoall_onesided_sched_finalize;

    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);
    if (sbgp->status == UCC_SBGP_NOT_EXISTS) {
        /* 1 PPN, use put */
        if (alg == UCC_TL_UCP_ALLTOALL_ONESIDED_AUTO) {
            alg = UCC_TL_UCP_ALLTOALL_ONESIDED_PUT;
        }
    } else {
        group_size = sbgp->group_size;
    }

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    task->super.finalize = ucc_tl_ucp_alltoall_onesided_finalize;
    /* Zero RTT pointers so finalize's NULL-guard is reliable even when this
     * task object is reused from the mpool without going through the RTT path */
    task->alltoall_onesided.peer_done       = NULL;
    task->alltoall_onesided.probe_req       = NULL;
    task->alltoall_onesided.peer_skip       = NULL;
    task->alltoall_onesided.peer_offset     = NULL;
    task->alltoall_onesided.peer_order      = NULL;
    task->alltoall_onesided.peer_healthy    = NULL;
    task->alltoall_onesided.probes_in_flight = 0;
    task->super.post     = ucc_tl_ucp_alltoall_onesided_start;
    a2a_task             = &task->super;
    npolls               = task->n_polls;

    status = ucc_tl_ucp_coll_init(&barrier_coll_args, team, &barrier_task);
    if (status != UCC_OK) {
        goto out;
    }
    if (perc_bw > 100) {
        perc_bw = 100;
    } else if (perc_bw == 0) {
        perc_bw = 1;
    }

    nelems             = TASK_ARGS(task).src.info.count;
    nelems             = nelems / UCC_TL_TEAM_SIZE(tl_team);
    param.field_mask   = UCP_EP_PERF_PARAM_FIELD_MESSAGE_SIZE;
    attr.field_mask    = UCP_EP_PERF_ATTR_FIELD_ESTIMATED_TIME;
    param.message_size = nelems * ucc_dt_size(TASK_ARGS(task).src.info.datatype);;
    ucc_tl_ucp_get_ep(
        tl_team, (UCC_TL_TEAM_RANK(tl_team) + 1) % UCC_TL_TEAM_SIZE(tl_team),
        &ep);
    ucp_ep_evaluate_perf(ep, &param, &attr);

    rate   = (1 / attr.estimated_time) * (double)(perc_bw / 100.0);
    ratio  = (nelems > 0) ? nelems * group_size : 1;

    task->alltoall_onesided.tokens = rate / ratio;
    if (task->alltoall_onesided.tokens < 1 &&
        alg != UCC_TL_UCP_ALLTOALL_ONESIDED_GET &&
        param.message_size >=
            UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_rtt_threshold) {
        ucc_rank_t team_size = UCC_TL_TEAM_SIZE(tl_team);

        task->alltoall_onesided.peers_completed  = 0;
        task->alltoall_onesided.tokens           = CA_PROBE_WINDOW_MIN;
        task->alltoall_onesided.probes_in_flight = 0;
        task->alltoall_onesided.peer_done        = ucc_calloc(team_size, sizeof(uint8_t), "peer rtt done");
        if (!task->alltoall_onesided.peer_done) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                "onesided alltoall OOM: unable to allocate peer_done array");
            status = UCC_ERR_NO_RESOURCE;
            goto out;
        }

        task->alltoall_onesided.probe_req = ucc_calloc(team_size, sizeof(ucs_status_ptr_t), "probe req");
        if (!task->alltoall_onesided.probe_req) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                "onesided alltoall OOM: unable to allocate probe_req array");
            status = UCC_ERR_NO_RESOURCE;
            ucc_free(task->alltoall_onesided.peer_done);
            goto out;
        }
        task->alltoall_onesided.peer_skip = ucc_calloc(team_size, sizeof(uint8_t), "peer skip");
        if (!task->alltoall_onesided.peer_skip) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                "onesided alltoall OOM: unable to allocate peer_skip array");
            status = UCC_ERR_NO_RESOURCE;
            ucc_free(task->alltoall_onesided.peer_done);
            ucc_free(task->alltoall_onesided.probe_req);
            goto out;
        }
        task->alltoall_onesided.peer_offset = ucc_calloc(team_size, sizeof(size_t), "peer offset");
        if (!task->alltoall_onesided.peer_offset) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                "onesided alltoall OOM: unable to allocate peer_offset array");
            status = UCC_ERR_NO_RESOURCE;
            ucc_free(task->alltoall_onesided.peer_done);
            ucc_free(task->alltoall_onesided.probe_req);
            ucc_free(task->alltoall_onesided.peer_skip);
            goto out;
        }
        task->alltoall_onesided.peer_order = ucc_malloc(team_size * sizeof(ucc_rank_t), "peer order");
        if (!task->alltoall_onesided.peer_order) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                "onesided alltoall OOM: unable to allocate peer_order array");
            status = UCC_ERR_NO_RESOURCE;
            ucc_free(task->alltoall_onesided.peer_done);
            ucc_free(task->alltoall_onesided.probe_req);
            ucc_free(task->alltoall_onesided.peer_skip);
            ucc_free(task->alltoall_onesided.peer_offset);
            goto out;
        }
        task->alltoall_onesided.peer_healthy = ucc_calloc(team_size, sizeof(uint8_t), "peer healthy");
        if (!task->alltoall_onesided.peer_healthy) {
            tl_error(UCC_TL_TEAM_LIB(tl_team),
                "onesided alltoall OOM: unable to allocate peer_healthy array");
            status = UCC_ERR_NO_RESOURCE;
            ucc_free(task->alltoall_onesided.peer_done);
            ucc_free(task->alltoall_onesided.probe_req);
            ucc_free(task->alltoall_onesided.peer_skip);
            ucc_free(task->alltoall_onesided.peer_offset);
            ucc_free(task->alltoall_onesided.peer_order);
            goto out;
        }
        {
            /*
             * Stagger NIC pressure across co-located ranks: half the local
             * ranks drain shared-memory peers first then hit the NIC; the
             * other half hit the NIC first then finish with shared memory.
             * This halves the number of co-located PEs racing for the NIC at
             * any moment without changing what each rank ultimately sends.
             */
            ucc_rank_t  grank      = UCC_TL_TEAM_RANK(tl_team);
            ucc_rank_t  gsize      = team_size;
            ucc_rank_t  local_rank = (sbgp && sbgp->status != UCC_SBGP_NOT_EXISTS)
                                         ? sbgp->group_rank : 0;
            ucc_rank_t  local_size = (sbgp && sbgp->status != UCC_SBGP_NOT_EXISTS)
                                         ? sbgp->group_size : 1;
            int         shm_first  = (local_rank < (local_size + 1) / 2);
            ucc_rank_t *order      = task->alltoall_onesided.peer_order;
            ucc_rank_t  i, p;
            ucc_rank_t  shm_idx;
            ucc_rank_t  rem_idx;
            ucc_rank_t  n_shm      = 0;
            ucc_rank_t  n_rem      = 0;

            /*
             * Self counts as a co-located (SHM) peer here so that
             * peer_done[grank] gets driven through the local-peer fast
             * path, matching the original loop's completion accounting.
             */
            for (i = 0; i < gsize; i++) {
                p = (grank + 1 + i) % gsize;
                if (ucc_rank_on_local_node(p, tl_team->topo)) {
                    n_shm++;
                } else {
                    n_rem++;
                }
            }
            shm_idx = shm_first ? 0      : n_rem;
            rem_idx = shm_first ? n_shm  : 0;
            for (i = 0; i < gsize; i++) {
                p = (grank + 1 + i) % gsize;
                if (ucc_rank_on_local_node(p, tl_team->topo)) {
                    order[shm_idx++] = p;
                } else {
                    order[rem_idx++] = p;
                }
            }
        }
        task->super.progress = ucc_tl_ucp_alltoall_onesided_put_seg_progress;
    } else {
        if (task->alltoall_onesided.tokens < 1) {
            task->alltoall_onesided.tokens = 1;
        }
        if (alg == UCC_TL_UCP_ALLTOALL_ONESIDED_GET ||
           (alg == UCC_TL_UCP_ALLTOALL_ONESIDED_AUTO &&
                                        sbgp->group_size >= CONGESTION_THRESHOLD)) {
            npolls = nelems * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
            if (npolls < task->n_polls) {
                npolls = task->n_polls;
            }
            task->super.progress = ucc_tl_ucp_alltoall_onesided_get_progress;
        } else {
            task->super.progress = ucc_tl_ucp_alltoall_onesided_put_progress;
        }
    }
    task->alltoall_onesided.npolls = npolls;

    ucc_schedule_add_task(schedule, a2a_task);
    ucc_task_subscribe_dep(&schedule->super, a2a_task,
                           UCC_EVENT_SCHEDULE_STARTED);

    ucc_schedule_add_task(schedule, barrier_task);
    ucc_task_subscribe_dep(a2a_task, barrier_task,
                           UCC_EVENT_COMPLETED);
    *task_h = &schedule->super;
    return status;
out:
    if (tl_schedule) {
        ucc_tl_ucp_put_schedule(&tl_schedule->super.super);
    }
    return status;
}
