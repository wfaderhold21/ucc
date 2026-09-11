/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 *
 * One-sided gather: leaf-driven put + atomic signal (I4). Every non-root
 * rank puts its block directly into the ROOT's dst buffer at offset
 * rank*blk and publishes it with the put -> ep_flush -> atomic_add(1) chain
 * on the root's copy of slot 1. The root local-copies its own block into
 * dst + root*blk and completes once its local slot 1 has received the
 * size-1 peer signals (expected = base + (size - 1)).
 *
 * Data layout: rank r's block lands at dst + r*blk, i.e. actual-rank order.
 * This matches the two-sided knomial's final aggregation (the root keeps its
 * own block at data_size * root and collects each peer at data_size *
 * actual_rank), so the one-sided and two-sided algorithms are
 * layout-compatible.
 *
 * Addressing (I1): a non-root computes the put target as *its own* dst VA
 * plus rank*blk. Because every rank provides a registered dst of the full
 * size (single_rank_count * size) at the same symmetric offset, that local
 * VA translates to the root's dst at the same offset -- resolved by
 * resolve_p2p_by_va() either as a symmetric TL segment (dst_memh == NULL,
 * the gtest layout) or through the global dst memh (the perftest layout).
 *
 * Slot choice (I7): gather-put is a FANIN topology, not a put-family one.
 * Only the root's local slot 1 advances, by (size-1) per round; a non-root's
 * local slot is never touched. That is why gather shares slot 1 with fanin
 * (identical root-only commit) and must NOT use slot 0, whose per-rank base
 * must advance by exactly 1 on every rank per round.
 *
 * Completion accounting (I7): the root's local slot 1 advances by (size-1),
 * so the root commits base = expected once it observes the slot; non-roots'
 * local slot 1 is untouched, so they complete as soon as their own
 * put->flush->signal chain is delivered (P2P_COMPLETE) and commit nothing.
 *
 * Posting happens exactly once, in ucc_tl_ucp_gather_onesided_start(): the
 * framework invokes the task's post hook once at submit, and that is the
 * natural place for a single-shot RMA. progress() (re-invoked by the queue
 * while INPROGRESS) is pure completion logic and never re-posts. This is
 * what makes it safe to use a real (potentially async) data put here: unlike
 * fanin's tiny synchronous atomic -- which completes in its first pass and is
 * therefore never re-posted -- a large gather put may still be in flight when
 * the queue re-invokes progress(), and posting again would double-publish
 * (the root would count 2*(size-1) signals).
 *
 * Preconditions (I3, enforced in init): no in-place (the root would need its
 * contribution relocated within dst, which the put layout does not support);
 * every rank supplies a full-size dst (single_rank_count * size), otherwise
 * a non-root cannot address the root's dst. Unmet preconditions yield
 * UCC_ERR_NOT_SUPPORTED so the core falls back to knomial.
 */

#include "config.h"
#include "tl_ucp.h"
#include "gather.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "tl_ucp_sendrecv.h"
#include "tl_ucp_onesided.h"

#define GATHER_ONESIDED_SLOT 1

ucc_status_t ucc_tl_ucp_gather_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_rank_t         rank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         root = TASK_ARGS(task).root;
    ucc_coll_args_t  *args  = &TASK_ARGS(task);
    size_t             blk   = (size_t)args->src.info.count *
                               ucc_dt_size(args->src.info.datatype);
    ucc_status_t       status;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* Completion target (I7): the root waits for one +1 per non-root.
     * (size-1, not size: the root local-copies its own block and does not
     * self-signal.) */
    task->gather_onesided.expected =
        ucc_tl_ucp_onesided_expected(task, GATHER_ONESIDED_SLOT, gsize - 1);

    /* Single-shot post: root keeps its own block, non-roots publish theirs.
     * The put target uses the actual team rank as the destination offset,
     * matching knomial's final actual-rank aggregation. */
    if (root == rank) {
        status = ucc_mc_memcpy(PTR_OFFSET(args->dst.info.buffer,
                                          (size_t)rank * blk),
                               args->src.info.buffer, blk,
                               args->dst.info.mem_type,
                               args->src.info.mem_type);
    } else {
        /* The local dst VA (full size, same symmetric offset) translates to
         * the root's dst by I1. The root receives (size-1) puts, one from
         * each non-root, each landing at a distinct rank*blk offset. */
        status = ucc_tl_ucp_put_signal(args->src.info.buffer,
                                       PTR_OFFSET(args->dst.info.buffer,
                                                  (size_t)rank * blk),
                                       blk, args->src.info.mem_type, root,
                                       ucc_tl_ucp_onesided_slot(task,
                                                                GATHER_ONESIDED_SLOT),
                                       TASK_ARGS(task).src_memh.local_memh,
                                       TASK_ARGS(task).dst_memh.global_memh,
                                       team, task);
    }
    if (ucc_unlikely(UCC_OK != status)) {
        return status; /* surfaced by the caller; task not enqueued */
    }

    /* Enqueue calls progress() once, which begins the completion wait. */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_gather_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_rank_t         rank = UCC_TL_TEAM_RANK(TASK_TEAM(task));
    ucc_rank_t         root = TASK_ARGS(task).root;

    if (UCC_OK != task->super.status &&
        UCC_INPROGRESS != task->super.status) {
        return; /* error already recorded by a completion callback */
    }
    if (root == rank) {
        /* Root: no RMA of its own, so P2P_COMPLETE is trivially true; its
         * completion is purely the slot reaching expected. Its slot advanced
         * by (size-1), so commit the advanced base. */
        if (UCC_OK == ucc_tl_ucp_test_onesided_slot(
                          task, GATHER_ONESIDED_SLOT,
                          task->gather_onesided.expected)) {
            task->super.status = UCC_OK;
            ucc_tl_ucp_onesided_commit_slot(task, GATHER_ONESIDED_SLOT,
                                            task->gather_onesided.expected);
        }
    } else {
        /* Non-root: its local slot 1 is untouched by this collective, so it
         * completes once its own put->flush->signal chain is delivered, and
         * commits nothing (its slot-1 base is unchanged). */
        ucc_tl_ucp_onesided_wait_completion(
            task, task->gather_onesided.window.npolls);
        if (UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task)) {
            task->super.status = UCC_OK;
        }
    }
}

ucc_status_t ucc_tl_ucp_gather_onesided_init(ucc_base_coll_args_t *coll_args,
                                             ucc_base_team_t      *team,
                                             ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_task_t *task;
    ucc_coll_args_t   *args = &coll_args->args;
    ucc_status_t        status;
    ucc_sbgp_t         *sbgp;
    ucc_rank_t          concurrency = 1;
    size_t              blk;

    status = ucc_tl_ucp_onesided_check_args(
        coll_args, tl_team, UCC_TL_UCP_ONESIDED_REQ_GWB);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    /* Every rank must provide a dst of the full size so a non-root can
     * address the root's dst at the same symmetric offset (I1). (check_args
     * already rejects in-place for the data path, so no separate check.) */
    blk = (size_t)args->src.info.count *
          ucc_dt_size(args->src.info.datatype);
    if (args->dst.info.count !=
            (ucc_count_t)(UCC_TL_TEAM_SIZE(tl_team) *
                          args->src.info.count) ||
        NULL == args->dst.info.buffer) {
        tl_debug(UCC_TL_TEAM_LIB(tl_team),
                 "one-sided gather requires a full-size dst on every rank");
        return UCC_ERR_NOT_SUPPORTED;
    }

    task                 = ucc_tl_ucp_init_task(coll_args, team);
    *task_h              = &task->super;
    task->super.post     = ucc_tl_ucp_gather_onesided_start;
    task->super.progress = ucc_tl_ucp_gather_onesided_progress;

    /* npolls for the non-root completion wait (token pacing is moot: at most
     * one put is posted per non-root). */
    sbgp = ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE);
    if (sbgp->status != UCC_SBGP_NOT_EXISTS) {
        concurrency = sbgp->group_size;
    }
    status = ucc_tl_ucp_onesided_window_init(task, blk, concurrency,
                                             &task->gather_onesided.window);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    return UCC_OK;
}
