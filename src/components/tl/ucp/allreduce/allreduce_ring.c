/**
 * Copyright (c) 2021-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "allreduce.h"
#include "tl_ucp.h"
#include "tl_ucp_coll.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include "../reduce_scatter/reduce_scatter.h"
#include "../allgather/allgather.h"

/*
 * Ring Allreduce - Implements allreduce as a composition of:
 *   1. Reduce-scatter ring: Each rank reduces and scatters data
 *   2. Allgather ring: Collects the scattered results to all ranks
 *
 * This approach uses UCC schedules to chain the two operations,
 * providing a clean, composable implementation of ring allreduce.
 */

static ucc_status_t
ucc_tl_ucp_allreduce_ring_start(ucc_coll_task_t *task)
{
    UCC_TL_UCP_PROFILE_REQUEST_EVENT(task, "ucp_allreduce_ring_start", 0);
    return ucc_schedule_start(task);
}

static ucc_status_t
ucc_tl_ucp_allreduce_ring_finalize(ucc_coll_task_t *task)
{
    ucc_schedule_t *schedule = ucc_derived_of(task, ucc_schedule_t);
    ucc_status_t    status;

    UCC_TL_UCP_PROFILE_REQUEST_EVENT(schedule, "ucp_allreduce_ring_done", 0);
    status = ucc_schedule_finalize(task);
    ucc_tl_ucp_put_schedule(schedule);
    return status;
}

ucc_status_t ucc_tl_ucp_allreduce_ring_init(ucc_base_coll_args_t *coll_args,
                                            ucc_base_team_t *team,
                                            ucc_coll_task_t **task_h)
{
    ucc_tl_ucp_team_t     *tl_team   = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_rank_t             tsize     = UCC_TL_TEAM_SIZE(tl_team);
    size_t                 count     = coll_args->args.dst.info.count;
    ucc_schedule_t        *schedule;
    ucc_coll_task_t       *rs_task;
    ucc_coll_task_t       *ag_task;
    ucc_base_coll_args_t   rs_args;
    ucc_base_coll_args_t   ag_args;
    ucc_status_t           status;

    /* Check for predefined datatypes */
    if (!ucc_coll_args_is_predefined_dt(&coll_args->args, UCC_RANK_INVALID)) {
        tl_error(team->context->lib,
                 "user defined datatype is not supported");
        return UCC_ERR_NOT_SUPPORTED;
    }

    /* Check that count is divisible by team size for ring algorithm */
    if (count % tsize != 0) {
        tl_debug(team->context->lib,
                 "ring requires count (%zu) divisible by team size (%u)",
                 count, tsize);
        return UCC_ERR_NOT_SUPPORTED;
    }

    /* Check for same memory types (required by allreduce) */
    ALLREDUCE_TASK_CHECK(coll_args->args, tl_team);

    /* Allocate schedule */
    status = ucc_tl_ucp_get_schedule(tl_team, coll_args,
                                     (ucc_tl_ucp_schedule_t **)&schedule);
    if (ucc_unlikely(UCC_OK != status)) {
        goto out;
    }

    /*
     * Step 1: Reduce-Scatter Ring
     *
     * Takes the full input and produces a scattered partial result.
     * Each rank ends up with count/tsize elements of the reduced data.
     *
     * For reduce_scatter_ring:
     * - dst.info.count should be the per-rank output count (count/tsize)
     * - The algorithm internally computes total = dst.info.count * tsize
     */
    rs_args                     = *coll_args;
    rs_args.args.coll_type      = UCC_COLL_TYPE_REDUCE_SCATTER;
    rs_args.args.dst.info.count = count / tsize;

    status = ucc_tl_ucp_reduce_scatter_ring_init(&rs_args, team, &rs_task);
    if (ucc_unlikely(UCC_OK != status)) {
        tl_error(team->context->lib, "failed to init reduce_scatter_ring task");
        goto err_schedule;
    }

    /* Add reduce-scatter to schedule and subscribe to schedule start */
    UCC_CHECK_GOTO(ucc_schedule_add_task(schedule, rs_task), err_rs, status);
    UCC_CHECK_GOTO(ucc_task_subscribe_dep(&schedule->super, rs_task,
                                          UCC_EVENT_SCHEDULE_STARTED),
                   err_rs, status);

    /*
     * Step 2: Allgather Ring
     *
     * Collects the scattered partial results from all ranks.
     * After reduce-scatter, each rank has its portion in the dst buffer
     * at offset (rank * count/tsize). Allgather performs in-place.
     *
     * For allgather_ring:
     * - dst.info.count should be the total output count
     * - IN_PLACE flag since data is already in correct position in dst buffer
     */
    ag_args                     = *coll_args;
    ag_args.args.coll_type      = UCC_COLL_TYPE_ALLGATHER;
    ag_args.args.mask          |= UCC_COLL_ARGS_FIELD_FLAGS;
    ag_args.args.flags          = UCC_COLL_ARGS_FLAG_IN_PLACE;
    ag_args.args.dst.info.count = count;

    status = ucc_tl_ucp_allgather_ring_init(&ag_args, team, &ag_task);
    if (ucc_unlikely(UCC_OK != status)) {
        tl_error(team->context->lib, "failed to init allgather_ring task");
        goto err_rs;
    }

    /* Add allgather to schedule and subscribe to reduce-scatter completion */
    UCC_CHECK_GOTO(ucc_schedule_add_task(schedule, ag_task), err_ag, status);
    UCC_CHECK_GOTO(ucc_task_subscribe_dep(rs_task, ag_task,
                                          UCC_EVENT_COMPLETED),
                   err_ag, status);

    /* Set up schedule callbacks */
    schedule->super.post     = ucc_tl_ucp_allreduce_ring_start;
    schedule->super.finalize = ucc_tl_ucp_allreduce_ring_finalize;

    *task_h = &schedule->super;
    return UCC_OK;

err_ag:
    ag_task->finalize(ag_task);
err_rs:
    rs_task->finalize(rs_task);
err_schedule:
    ucc_tl_ucp_put_schedule(schedule);
out:
    return status;
}
