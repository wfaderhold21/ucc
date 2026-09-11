/**
 * Copyright (c) 2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided alltoallv: every rank puts its per-peer blocks directly into the
 * peers' dst segments and publishes each put with a put -> ep_flush ->
 * atomic_add chain on the peer's symmetric work buffer (I4). Completion is
 * detected with a monotonic expected value on slot 0 (I7): the task waits
 * for the local slot to reach base + gsize (one +1 from each of the gsize
 * senders, including the self-signal) and then commits the team's base.
 * Counters are never reset, so a fast peer that races into the next
 * collective cannot lose a signal (the I7 counter-reuse bug).
 *
 * Addressing (I1): rank r (the sender) places its block for peer p at the
 * peer's dst offset d_disp[r] -- the offset at which the peer expects data
 * from sender r (alltoallv dst[j] == data from sender j). This holds when
 * all ranks provide identical displacements (the symmetric/GWB setup); the
 * MPI test enforces it with an MPI_Alltoall transpose over the
 * displacements, and the gtest uses a uniform layout where it holds
 * trivially.
 *
 * Posting is resumable (3.5): a pacing window (3.4) bounds the number of
 * outstanding puts; when the window is full the task yields and resumes from
 * put_posted on the next progress (the alltoall idiom).
 */

#include "config.h"
#include "tl_ucp.h"
#include "alltoallv.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

/*
 * Post put_signal(s) for the next peers, honoring the pacing window
 * (resumable: the next peer is recomputed from put_posted, as in
 * alltoall_onesided_put_progress). Returns UCC_OK when the caller may
 * proceed to the completion wait, UCC_INPROGRESS when the window requires
 * yielding, or an error.
 */
static ucc_status_t
ucc_tl_ucp_alltoallv_onesided_post(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         rank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         peer  = (rank + task->onesided.put_posted + 1) % gsize;
    ucc_coll_args_t  *args   = &TASK_ARGS(task);
    size_t            dt     = ucc_dt_size(args->src.info_v.datatype);

    for (; task->onesided.put_posted < gsize; peer = (peer + 1) % gsize) {
        void *src = PTR_OFFSET(args->src.info_v.buffer,
                               ucc_coll_args_get_displacement(args,
                                                              args->src.info_v
                                                                  .displacements,
                                                              peer) * dt);
        void *dst = PTR_OFFSET(args->dst.info_v.buffer,
                               ucc_coll_args_get_displacement(args,
                                                              args->dst.info_v
                                                                  .displacements,
                                                              rank) * dt);
        size_t len = ucc_coll_args_get_count(args, args->src.info_v.counts,
                                             peer) *
                     dt;
        ucc_status_t status = ucc_tl_ucp_put_signal(
            src, dst, len, args->src.info_v.mem_type, peer,
            ucc_tl_ucp_onesided_slot(task, 0),
            TASK_ARGS(task).src_memh.local_memh,
            TASK_ARGS(task).dst_memh.global_memh, team, task);
        if (ucc_unlikely(UCC_OK != status)) {
            return status;
        }
        if (!ucc_tl_ucp_onesided_window_check(
                task, &task->onesided.put_posted,
                &task->onesided.put_completed,
                &task->alltoallv_onesided.window)) {
            return UCC_INPROGRESS; /* window full: yield, resume later */
        }
    }
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_alltoallv_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* Completion target (I7): base + one +1 per rank. The base is the
     * team's committed value from the previous round (zero for a fresh
     * team); it is stable for the whole round, so computing it here is
     * timing-independent. Reading the live slot instead would race with
     * peers that have already delivered their signals. The commit in
     * progress() advances the base, and test_onesided_slot() uses >=, so
     * a fast peer racing into the next round cannot lose a signal. */
    task->alltoallv_onesided.expected =
        ucc_tl_ucp_onesided_expected(task, 0, UCC_TL_TEAM_SIZE(team));

    /* ucc_progress_queue_enqueue() calls progress() once, which begins the
     * posting; the task is enqueued if it is not done after that first
     * pass. */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_alltoallv_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_status_t       status;

    if (UCC_OK != task->super.status &&
        UCC_INPROGRESS != task->super.status) {
        return; /* error already recorded by a callback */
    }
    status = ucc_tl_ucp_alltoallv_onesided_post(task);
    if (ucc_unlikely(UCC_OK != status && UCC_INPROGRESS != status)) {
        task->super.status = status;
        return;
    }
    if (UCC_INPROGRESS == status) {
        return; /* window full: wait for the next progress round */
    }
    /* All puts posted: bounded wait for their completion + the signals. */
    ucc_tl_ucp_onesided_wait_completion(task,
                                        task->alltoallv_onesided.window.npolls);
    if (UCC_OK ==
        ucc_tl_ucp_test_onesided_slot(task, 0,
                                      task->alltoallv_onesided.expected)) {
        task->super.status = UCC_OK;
        /* Commit the base so the next round's target advances (I7). */
        ucc_tl_ucp_onesided_commit_slot(task, 0,
                                        task->alltoallv_onesided.expected);
    }

}

ucc_status_t ucc_tl_ucp_alltoallv_onesided_init(ucc_base_coll_args_t *coll_args,
                                                ucc_base_team_t      *team,
                                                ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team  = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_task_t *task;
    ucc_rank_t          gsize       = UCC_TL_TEAM_SIZE(tl_team);
    ucc_sbgp_t        *sbgp;
    ucc_rank_t          concurrency = 1;
    ucc_coll_args_t   *args        = &coll_args->args;
    size_t              dt_size     = ucc_dt_size(args->src.info_v.datatype);
    size_t              total       = 0;
    size_t              msglen;
    ucc_rank_t          i;
    ucc_status_t        status;

    status = ucc_tl_ucp_onesided_check_args(
        coll_args, tl_team, UCC_TL_UCP_ONESIDED_REQ_GWB);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.post     = ucc_tl_ucp_alltoallv_onesided_start;
    task->super.progress = ucc_tl_ucp_alltoallv_onesided_progress;

    /* Pacing window (3.4): throttle puts while the NIC is congested.
     * concurrency = processes per node sharing the NIC; msglen = average
     * bytes per put (representative for the token formula). */
    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);
    if (sbgp->status != UCC_SBGP_NOT_EXISTS) {
        concurrency = sbgp->group_size;
    }
    for (i = 0; i < gsize; i++) {
        total += ucc_coll_args_get_count(args, args->src.info_v.counts, i);
    }
    msglen = (gsize > 0) ? (total * dt_size) / gsize : 0;
    status = ucc_tl_ucp_onesided_window_init(task, msglen, concurrency,
                                             &task->alltoallv_onesided.window);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    return UCC_OK;
}
