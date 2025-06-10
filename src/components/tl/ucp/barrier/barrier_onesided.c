/**
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include <math.h>
#include "tl_ucp.h"
#include "barrier.h"
#include "core/ucc_progress_queue.h"
#include "utils/ucc_math.h"
#include "tl_ucp_sendrecv.h"

void ucc_tl_ucp_barrier_onesided_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_rank_t rank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t size = UCC_TL_TEAM_SIZE(team);
    int i, peer, offset;
    ucc_status_t status;
    int local_flag = 1;
    int polls = 0;

    if (task->barrier_onesided.p.iteration >= task->barrier_onesided.p.n_iters) {
        if (task->onesided.put_completed != task->onesided.put_posted) {
            while (polls++ < task->n_polls) {
                ucp_worker_progress(team->worker->ucp_worker);
            }
            if (task->onesided.put_completed != task->onesided.put_posted) {
                return;
            }
        }
        task->super.status = UCC_OK;
        return;
    }

    // If current phase is not complete, check if all peers have arrived
    if (!task->barrier_onesided.phase_complete[task->barrier_onesided.p.iteration]) {
        // Check if we've received flags from all peers in this phase
        while (polls++ < task->n_polls) {
            if (task->barrier_onesided.counters[task->barrier_onesided.p.iteration] >= task->barrier_onesided.p.radix - 1) {
                task->barrier_onesided.phase_complete[task->barrier_onesided.p.iteration] = 1;
                task->barrier_onesided.p.iteration++;
                // Continue to next phase instead of returning
                break;
            }
            ucp_worker_progress(team->worker->ucp_worker);
        }
        if (polls > task->n_polls) {
            return;
        }
    }

    // Calculate peers for current phase
    for (i = 1; i < task->barrier_onesided.p.radix; i++) {
        offset = i * pow(task->barrier_onesided.p.radix, task->barrier_onesided.p.iteration);
        peer = (rank + offset) % size;
        
        // Send our local flag to peer's flag array
        status = ucc_tl_ucp_put_nb(&task->barrier_onesided.flags[peer], &local_flag, 
                                  sizeof(int), peer, team, task);
        if (UCC_OK != status) {
            task->super.status = status;
            return;
        }
    }
}

ucc_status_t ucc_tl_ucp_barrier_onesided_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team = TASK_TEAM(task);
    ucc_rank_t size = UCC_TL_TEAM_SIZE(team);
    int radix = UCC_TL_UCP_BARRIER_ONESIDED_DEFAULT_RADIX;
    int *work_buffer;

    // Initialize knomial pattern
    ucc_knomial_pattern_init(size, UCC_TL_TEAM_RANK(team), radix, &task->barrier_onesided.p);

    // Use global work buffer for flags, counters and phase_complete
    work_buffer = (int*)TASK_ARGS(task).global_work_buffer;
    if (!work_buffer) {
        return UCC_ERR_INVALID_PARAM;
    }

    // Layout of work buffer:
    // [0..size-1]: flags array
    // [size..size+n_iters-1]: counters array
    // [size+n_iters..size+2*n_iters-1]: phase_complete array
    task->barrier_onesided.flags = work_buffer;
    task->barrier_onesided.counters = work_buffer + size;
    task->barrier_onesided.phase_complete = work_buffer + size + task->barrier_onesided.p.n_iters;

    // Initialize counters and flags
    memset(task->barrier_onesided.flags, 0, size * sizeof(int));
    memset(task->barrier_onesided.counters, 0, task->barrier_onesided.p.n_iters * sizeof(int));
    memset(task->barrier_onesided.phase_complete, 0, task->barrier_onesided.p.n_iters * sizeof(int));

    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}