/**
 * Copyright (c) 2021-2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "tl_ucp.h"
#include "alltoall.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_coll_utils.h"
#include "utils/ucc_math.h"
#include "tl_ucp_sendrecv.h"

#define CONGESTION_THRESHOLD 8

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

static inline void alltoall_onesided_get_fragment(
    ucc_tl_ucp_task_t *task, uint32_t op_idx, ucc_rank_t grank,
    ucc_rank_t gsize, size_t peer_msg_size, ucc_rank_t *peer,
    size_t *frag_offset, size_t *frag_size)
{
    uint32_t frag_idx = op_idx / gsize;
    uint32_t peer_idx = op_idx % gsize;
    uint32_t stride   = task->alltoall_onesided.stride;
    uint32_t nfrags   = task->alltoall_onesided.nfrags;

    *peer        = (grank + (peer_idx * stride) % gsize + 1) % gsize;
    *frag_offset = ucc_buffer_block_offset(peer_msg_size, nfrags, frag_idx);
    *frag_size   = ucc_buffer_block_count(peer_msg_size, nfrags, frag_idx);
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
    ucc_status_t status;

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
    uint32_t           total     = task->alltoall_onesided.total_posts;
    int64_t            npolls    = task->alltoall_onesided.npolls;
    /* To resolve remote virtual addresses, the dst_memh is the one that must
     * have the rkey information. For this algorithm, we need to swap the
     * src and dst handles to operate correctly */
    ucc_mem_map_mem_h *dst_memh  = TASK_ARGS(task).src_memh.global_memh;
    uint32_t          *posted    = &task->onesided.get_posted;
    uint32_t          *completed = &task->onesided.get_completed;
    ucc_rank_t         peer;
    ucc_mem_map_mem_h  src_memh;
    size_t             nelems, frag_offset, frag_size;
    uint32_t           op_idx;

    nelems   = TASK_ARGS(task).src.info.count;
    nelems   = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    src_memh = (TASK_ARGS(task).flags & UCC_COLL_ARGS_FLAG_DST_MEMH_GLOBAL)
                   ? TASK_ARGS(task).dst_memh.global_memh[grank]
                   : TASK_ARGS(task).dst_memh.local_memh;

    for (; *posted < total;) {
        if (task->alltoall_onesided.fractional_pacing) {
            if (ucc_get_time() < task->alltoall_onesided.next_post_time) {
                return;
            }
            task->alltoall_onesided.next_post_time +=
                task->alltoall_onesided.pacing_interval;
        }
        op_idx = *posted;
        alltoall_onesided_get_fragment(task, op_idx, grank, gsize, nelems,
                                       &peer, &frag_offset, &frag_size);
        UCPCHECK_GOTO(ucc_tl_ucp_get_nb(
                          PTR_OFFSET(dest, peer * nelems + frag_offset),
                          PTR_OFFSET(src, grank * nelems + frag_offset),
                          frag_size, mtype, peer, src_memh, dst_memh, team,
                          task),
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
    uint32_t           total     = task->alltoall_onesided.total_posts;
    int64_t            npolls    = task->alltoall_onesided.npolls;
    ucc_mem_map_mem_h *dst_memh  = TASK_ARGS(task).dst_memh.global_memh;
    uint32_t          *posted    = &task->onesided.put_posted;
    uint32_t          *completed = &task->onesided.put_completed;
    ucc_rank_t         peer;
    ucc_mem_map_mem_h  src_memh;
    size_t             nelems, frag_offset, frag_size;
    uint32_t           op_idx;

    nelems   = TASK_ARGS(task).src.info.count;
    nelems   = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    src_memh = (TASK_ARGS(task).flags & UCC_COLL_ARGS_FLAG_SRC_MEMH_GLOBAL)
                   ? TASK_ARGS(task).src_memh.global_memh[grank]
                   : TASK_ARGS(task).src_memh.local_memh;

    for (; *posted < total;) {
        if (task->alltoall_onesided.fractional_pacing) {
            if (ucc_get_time() < task->alltoall_onesided.next_post_time) {
                return;
            }
            task->alltoall_onesided.next_post_time +=
                task->alltoall_onesided.pacing_interval;
        }
        op_idx = *posted;
        alltoall_onesided_get_fragment(task, op_idx, grank, gsize, nelems,
                                       &peer, &frag_offset, &frag_size);
        UCPCHECK_GOTO(ucc_tl_ucp_put_nb(
                          PTR_OFFSET(src, peer * nelems + frag_offset),
                          PTR_OFFSET(dest, grank * nelems + frag_offset),
                          frag_size, mtype, peer, src_memh, dst_memh, team,
                          task),
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
    if (task->alltoall_onesided.fractional_pacing) {
        task->alltoall_onesided.next_post_time =
            ucc_get_time() + task->alltoall_onesided.stagger_s;
    }
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
    int                          fractional_pacing =
        UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_fractional_pacing;
    size_t                       seg_size =
        UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_seg_size;
    size_t                       rtt_threshold =
        UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_rtt_threshold;
    uint32_t                     initial_window =
        UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_initial_window;
    ucc_tl_ucp_alltoall_onesided_alg_t alg   =
        UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.alltoall_onesided_alg;
    ucc_tl_ucp_schedule_t       *tl_schedule = NULL;
    ucc_rank_t                   group_size  = 1;
    ucc_rank_t                   node_rank   = 0;
    ucc_rank_t                   node_size   = 1;
    ucc_coll_task_t             *barrier_task;
    ucc_coll_task_t             *a2a_task;
    ucc_tl_ucp_task_t           *task;
    ucc_status_t                 status;
    size_t                       nelems;
    double                       rate;
    double                       raw_tokens;
    size_t                       ratio;
    ucp_ep_h                     ep;
    ucp_ep_evaluate_perf_param_t param;
    ucp_ep_evaluate_perf_attr_t  attr;
    int64_t                      npolls;
    ucc_sbgp_t                  *sbgp;
    ucc_rank_t                   gsize;
    uint32_t                     stride;
    size_t                       dt_size;
    size_t                       msg_size;
    uint64_t                     nfrags;
    uint64_t                     total_posts;

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
        node_rank  = sbgp->group_rank;
        node_size  = sbgp->group_size;
    }

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    task->super.finalize = ucc_tl_ucp_alltoall_onesided_finalize;
    a2a_task             = &task->super;

    if (perc_bw > 100) {
        perc_bw = 100;
    } else if (perc_bw == 0) {
        perc_bw = 1;
    }

    dt_size            = ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    nelems             = TASK_ARGS(task).src.info.count;
    nelems             = nelems / UCC_TL_TEAM_SIZE(tl_team);
    msg_size           = nelems * dt_size;
    param.field_mask   = UCP_EP_PERF_PARAM_FIELD_MESSAGE_SIZE;
    attr.field_mask    = UCP_EP_PERF_ATTR_FIELD_ESTIMATED_TIME;
    param.message_size = msg_size;
    ucc_tl_ucp_get_ep(
        tl_team, (UCC_TL_TEAM_RANK(tl_team) + 1) % UCC_TL_TEAM_SIZE(tl_team),
        &ep);
    ucp_ep_evaluate_perf(ep, &param, &attr);

    rate       = (1 / attr.estimated_time) * (double)(perc_bw / 100.0);
    ratio      = (nelems > 0) ? nelems * group_size : 1;
    raw_tokens = rate / (double)ratio;
    if (raw_tokens < 1.0) {
        task->alltoall_onesided.tokens            = 1;
        task->alltoall_onesided.fractional_pacing = fractional_pacing;
        if (fractional_pacing) {
            /* pacing_interval = node_size / rate: time between admitted ops
             * for this rank, spreading node_size ranks across one transfer
             * window. Using node_size (not ratio=nelems*node_size) because
             * estimated_time already accounts for message size. */
            task->alltoall_onesided.pacing_interval =
                (double)node_size / rate;
            task->alltoall_onesided.stagger_s =
                (double)node_rank *
                    task->alltoall_onesided.pacing_interval /
                    (double)node_size;
            task->alltoall_onesided.next_post_time = 0.0;
        } else {
            task->alltoall_onesided.pacing_interval = 0.0;
            task->alltoall_onesided.stagger_s       = 0.0;
            task->alltoall_onesided.next_post_time  = 0.0;
        }
    } else {
        task->alltoall_onesided.tokens            = (uint32_t)raw_tokens;
        task->alltoall_onesided.fractional_pacing = 0;
        task->alltoall_onesided.pacing_interval   = 0.0;
        task->alltoall_onesided.stagger_s         = 0.0;
        task->alltoall_onesided.next_post_time    = 0.0;
    }
    if (initial_window == 0) {
        initial_window = 1;
    }
    task->alltoall_onesided.tokens =
        ucc_max(task->alltoall_onesided.tokens, initial_window);

    /* Option 3: stride-permuted peer ordering to desynchronize ranks.
     * Find the smallest stride coprime with gsize (i.e. gcd(stride,gsize)==1).
     * stride=1 degenerates to the original sequential order. */
    gsize  = UCC_TL_TEAM_SIZE(tl_team);
    stride = 1;
    if (gsize > 2) {
        uint32_t s;
        for (s = 2; s < gsize; s++) {
            uint32_t a = s, b = gsize;
            while (b) { uint32_t t = b; b = a % b; a = t; }
            if (a == 1) { stride = s; break; }
        }
    }
    task->alltoall_onesided.stride = stride;
    nfrags                         = 1;
    if (seg_size != UCC_MEMUNITS_AUTO && seg_size > 0) {
        if (rtt_threshold == UCC_MEMUNITS_AUTO) {
            rtt_threshold = 0;
        }
        if (msg_size >= rtt_threshold && msg_size > 0) {
            nfrags = ucc_div_round_up(msg_size, seg_size);
        }
    }
    total_posts = nfrags * gsize;
    if (nfrags > UCC_RANK_MAX || total_posts > UCC_RANK_MAX) {
        tl_error(UCC_TL_TEAM_LIB(tl_team),
                 "too many onesided alltoall fragments: nfrags %llu, "
                 "total posts %llu",
                 (unsigned long long)nfrags,
                 (unsigned long long)total_posts);
        status = UCC_ERR_INVALID_PARAM;
        goto out;
    }
    task->alltoall_onesided.nfrags      = (uint32_t)nfrags;
    task->alltoall_onesided.total_posts = (uint32_t)total_posts;

    task->super.post = ucc_tl_ucp_alltoall_onesided_start;
    npolls           = task->n_polls;
    if (alg == UCC_TL_UCP_ALLTOALL_ONESIDED_GET ||
       (alg == UCC_TL_UCP_ALLTOALL_ONESIDED_AUTO &&
                                    group_size >= CONGESTION_THRESHOLD)) {
        npolls = msg_size;
        if (npolls < task->n_polls) {
            npolls = task->n_polls;
        }
        task->super.progress = ucc_tl_ucp_alltoall_onesided_get_progress;
    } else {
        task->super.progress = ucc_tl_ucp_alltoall_onesided_put_progress;
    }
    task->alltoall_onesided.npolls = npolls;

    status = ucc_tl_ucp_coll_init(&barrier_coll_args, team, &barrier_task);
    if (status != UCC_OK) {
        goto out;
    }

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
