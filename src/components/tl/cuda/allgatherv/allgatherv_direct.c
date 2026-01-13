/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "allgatherv.h"
#include "components/ec/ucc_ec.h"
#include "tl_cuda_cache.h"
#include "utils/arch/cpu.h"
#include "utils/arch/cuda_def.h"

#include <string.h>

/*
 * Direct allgatherv algorithm using IPC to peer destination buffers
 *
 * Description:
 *  This algorithm directly writes to peer destination buffers via CUDA IPC,
 *  avoiding the use of scratch memory. This benefits from mem_map pre-registration
 *  as it eliminates per-collective cudaIpcGetMemHandle() calls.
 *
 *  Algorithm:
 *      1. Each rank exports IPC handle for its dst buffer
 *      2. Exchange handles via shared memory barrier
 *      3. Each rank opens peer dst buffer handles via IPC
 *      4. Each rank copies its src data to ALL peers' dst buffers at correct offset
 *      5. Barrier to ensure all writes complete
 *      6. Unmap peer handles
 *
 *  Requires: Fully connected topology (all ranks can access each other via IPC)
 */

enum {
    STAGE_SYNC,    /* Wait for free SYNC segment, exchange handles */
    STAGE_SETUP,   /* Open peer IPC handles */
    STAGE_COPIES,  /* Post copies to all peers */
    STAGE_WAIT,    /* Wait for copies to complete */
    STAGE_BARRIER, /* Final barrier before cleanup */
};

ucc_status_t ucc_tl_cuda_allgatherv_direct_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_cuda_task_t *task = ucc_derived_of(coll_task, ucc_tl_cuda_task_t);

    tl_trace(UCC_TASK_LIB(task), "finalizing allgatherv_direct task %p", task);
    ucc_tl_cuda_task_put(task);
    return UCC_OK;
}

static ucc_status_t
ucc_tl_cuda_allgatherv_direct_setup_start(ucc_tl_cuda_task_t *task)
{
    ucc_tl_cuda_team_t     *team  = TASK_TEAM(task);
    ucc_rank_t              trank = UCC_TL_TEAM_RANK(team);
    volatile ucc_tl_cuda_sync_t *sync = TASK_SYNC(task, trank);
    ucc_status_t            status;

    /* Copy local dst mem_info to shared memory for other ranks */
    memcpy((void *)&sync->mem_info_dst, &task->allgatherv_direct.mem_info_dst,
           sizeof(ucc_tl_cuda_mem_info_t));

    ucc_memory_cpu_store_fence();
    status = ucc_tl_cuda_shm_barrier_start(trank, task->bar);
    if (ucc_unlikely(status != UCC_OK)) {
        return status;
    }

    return UCC_OK;
}

static ucc_status_t
ucc_tl_cuda_allgatherv_direct_setup_test(ucc_tl_cuda_task_t *task)
{
    ucc_tl_cuda_team_t          *team  = TASK_TEAM(task);
    ucc_rank_t                   trank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t                   tsize = UCC_TL_TEAM_SIZE(team);
    volatile ucc_tl_cuda_sync_t *peer_sync;
    ucc_tl_cuda_cache_t         *cache;
    ucc_status_t                 status;
    ucc_rank_t                   i;

    status = ucc_tl_cuda_shm_barrier_test(trank, task->bar);
    if (status != UCC_OK) {
        return status;
    }

    /* Open IPC handles to all peers' dst buffers */
    for (i = 0; i < tsize; i++) {
        if (i == trank) {
            /* Local rank - use direct pointer */
            task->allgatherv_direct.peer_dst_addr[i] =
                task->allgatherv_direct.rbuf;
            continue;
        }

        cache = ucc_tl_cuda_get_cache(team, i);
        if (ucc_unlikely(!cache)) {
            return UCC_ERR_NO_MESSAGE;
        }

        /* Use global_memh if available, otherwise read from shared memory */
        if (task->allgatherv_direct.use_global_memh &&
            task->allgatherv_direct.global_memh_dst != NULL) {
            ucc_tl_cuda_mem_info_t peer_mi;
            status = ucc_tl_cuda_mem_info_from_global_memh(
                task->allgatherv_direct.global_memh_dst, i, &peer_mi);
            if (status == UCC_OK) {
                status = ucc_tl_cuda_map_memhandle(
                    peer_mi.ptr, peer_mi.length, peer_mi.handle,
                    &task->allgatherv_direct.peer_dst_addr[i], cache);
            }
        } else {
            peer_sync = TASK_SYNC(task, i);
            status = ucc_tl_cuda_map_memhandle(
                peer_sync->mem_info_dst.ptr, peer_sync->mem_info_dst.length,
                peer_sync->mem_info_dst.handle,
                &task->allgatherv_direct.peer_dst_addr[i], cache);
        }

        if (status != UCC_OK) {
            tl_error(UCC_TASK_LIB(task),
                     "failed to map peer %d dst handle", i);
            return UCC_ERR_INVALID_PARAM;
        }
    }

    return UCC_OK;
}

static ucc_status_t
ucc_tl_cuda_allgatherv_direct_post_copies(ucc_tl_cuda_task_t *task)
{
    ucc_tl_cuda_team_t         *team    = TASK_TEAM(task);
    ucc_coll_args_t            *args    = &TASK_ARGS(task);
    ucc_rank_t                  trank   = UCC_TL_TEAM_RANK(team);
    ucc_rank_t                  tsize   = UCC_TL_TEAM_SIZE(team);
    ucc_datatype_t              dt      = task->allgatherv_direct.dt;
    size_t                      dt_size = ucc_dt_size(dt);
    ucc_ee_executor_t          *exec;
    ucc_ee_executor_task_args_t eargs;
    ucc_status_t                status;
    ucc_rank_t                  i;
    void                       *sbuf;
    size_t                      send_count, send_size, my_offset;

    status = ucc_coll_task_get_executor(&task->super, &exec);
    if (ucc_unlikely(status != UCC_OK)) {
        return status;
    }

    /* Get my send count and offset in dst buffer */
    send_count = task->allgatherv_direct.get_count(task, trank);
    send_size  = send_count * dt_size;
    my_offset  = task->allgatherv_direct.get_offset(task, trank) * dt_size;

    /* Source buffer */
    if (UCC_IS_INPLACE(*args)) {
        sbuf = PTR_OFFSET(task->allgatherv_direct.rbuf, my_offset);
    } else {
        sbuf = task->allgatherv_direct.sbuf;
    }

    if (send_size == 0) {
        return UCC_OK;
    }

    /* Post multi-destination copy to all peers */
    eargs.task_type = UCC_EE_EXECUTOR_TASK_COPY_MULTI;
    eargs.copy_multi.num_vectors = 0;

    for (i = 0; i < tsize; i++) {
        if (i == trank && !UCC_IS_INPLACE(*args)) {
            /* Copy to local dst buffer (not inplace) */
            eargs.copy_multi.src[eargs.copy_multi.num_vectors] = sbuf;
            eargs.copy_multi.dst[eargs.copy_multi.num_vectors] =
                PTR_OFFSET(task->allgatherv_direct.rbuf, my_offset);
            eargs.copy_multi.counts[eargs.copy_multi.num_vectors] = send_size;
            eargs.copy_multi.num_vectors++;
        } else if (i != trank) {
            /* Copy to peer's dst buffer */
            eargs.copy_multi.src[eargs.copy_multi.num_vectors] = sbuf;
            eargs.copy_multi.dst[eargs.copy_multi.num_vectors] =
                PTR_OFFSET(task->allgatherv_direct.peer_dst_addr[i], my_offset);
            eargs.copy_multi.counts[eargs.copy_multi.num_vectors] = send_size;
            eargs.copy_multi.num_vectors++;
        }
    }

    if (eargs.copy_multi.num_vectors > 0) {
        status = ucc_ee_executor_task_post(exec, &eargs,
                                           &task->allgatherv_direct.exec_task);
        if (ucc_unlikely(status != UCC_OK)) {
            return status;
        }
    }

    return UCC_OK;
}

static ucc_status_t
ucc_tl_cuda_allgatherv_direct_wait_copies(ucc_tl_cuda_task_t *task)
{
    ucc_status_t status;

    if (task->allgatherv_direct.exec_task == NULL) {
        return UCC_OK;
    }

    status = ucc_ee_executor_task_test(task->allgatherv_direct.exec_task);
    if (status == UCC_OK) {
        ucc_ee_executor_task_finalize(task->allgatherv_direct.exec_task);
        task->allgatherv_direct.exec_task = NULL;
    }
    return status;
}

static ucc_status_t
ucc_tl_cuda_allgatherv_direct_cleanup(ucc_tl_cuda_task_t *task)
{
    ucc_tl_cuda_team_t          *team  = TASK_TEAM(task);
    ucc_rank_t                   trank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t                   tsize = UCC_TL_TEAM_SIZE(team);
    volatile ucc_tl_cuda_sync_t *peer_sync;
    ucc_tl_cuda_cache_t         *cache;
    ucc_status_t                 status;
    ucc_rank_t                   i;

    /* Unmap peer handles */
    for (i = 0; i < tsize; i++) {
        if (i == trank) {
            continue;
        }

        cache = ucc_tl_cuda_get_cache(team, i);
        if (!cache || !task->allgatherv_direct.peer_dst_addr[i]) {
            continue;
        }

        if (task->allgatherv_direct.use_global_memh &&
            task->allgatherv_direct.global_memh_dst != NULL) {
            ucc_tl_cuda_mem_info_t peer_mi;
            status = ucc_tl_cuda_mem_info_from_global_memh(
                task->allgatherv_direct.global_memh_dst, i, &peer_mi);
            if (status == UCC_OK) {
                ucc_tl_cuda_unmap_memhandle(
                    (uintptr_t)peer_mi.ptr,
                    task->allgatherv_direct.peer_dst_addr[i], cache, 0);
            }
        } else {
            peer_sync = TASK_SYNC(task, i);
            ucc_tl_cuda_unmap_memhandle(
                (uintptr_t)peer_sync->mem_info_dst.ptr,
                task->allgatherv_direct.peer_dst_addr[i], cache, 0);
        }
        task->allgatherv_direct.peer_dst_addr[i] = NULL;
    }

    return UCC_OK;
}

ucc_status_t ucc_tl_cuda_allgatherv_direct_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_cuda_task_t *task = ucc_derived_of(coll_task, ucc_tl_cuda_task_t);
    ucc_tl_cuda_team_t *team = TASK_TEAM(task);

    tl_trace(UCC_TASK_LIB(task), "start allgatherv_direct task %p", task);

    task->allgatherv_direct.stage = STAGE_SYNC;
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_cuda_allgatherv_direct_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_cuda_task_t *task   = ucc_derived_of(coll_task, ucc_tl_cuda_task_t);
    ucc_tl_cuda_team_t *team   = TASK_TEAM(task);
    ucc_status_t        status = UCC_INPROGRESS;

    switch (task->allgatherv_direct.stage) {
    case STAGE_SYNC:
        status = ucc_tl_cuda_allgatherv_direct_setup_start(task);
        if (status != UCC_OK) {
            goto complete;
        }
        task->allgatherv_direct.stage = STAGE_SETUP;
        /* fall through */
    case STAGE_SETUP:
        status = ucc_tl_cuda_allgatherv_direct_setup_test(task);
        if (status != UCC_OK) {
            goto out;
        }
        task->allgatherv_direct.stage = STAGE_COPIES;
        /* fall through */
    case STAGE_COPIES:
        status = ucc_tl_cuda_allgatherv_direct_post_copies(task);
        if (status != UCC_OK) {
            goto complete;
        }
        task->allgatherv_direct.stage = STAGE_WAIT;
        /* fall through */
    case STAGE_WAIT:
        status = ucc_tl_cuda_allgatherv_direct_wait_copies(task);
        if (status != UCC_OK) {
            goto out;
        }
        /* Start final barrier */
        status = ucc_tl_cuda_shm_barrier_start(UCC_TL_TEAM_RANK(team),
                                               task->bar);
        if (status != UCC_OK) {
            goto complete;
        }
        task->allgatherv_direct.stage = STAGE_BARRIER;
        /* fall through */
    case STAGE_BARRIER:
        status = ucc_tl_cuda_shm_barrier_test(UCC_TL_TEAM_RANK(team),
                                              task->bar);
        if (status != UCC_OK) {
            goto out;
        }
        /* Cleanup */
        ucc_tl_cuda_allgatherv_direct_cleanup(task);
        status = UCC_OK;
        break;
    }

complete:
    task->super.status = status;
out:
    if (status != UCC_INPROGRESS) {
        if (status != UCC_OK) {
            ucc_tl_cuda_allgatherv_direct_cleanup(task);
        }
    }
}

ucc_status_t ucc_tl_cuda_allgatherv_direct_init(ucc_base_coll_args_t *coll_args,
                                                ucc_base_team_t      *tl_team,
                                                ucc_coll_task_t     **task_p)
{
    ucc_tl_cuda_team_t *team = ucc_derived_of(tl_team, ucc_tl_cuda_team_t);
    ucc_tl_cuda_task_t *task;
    ucc_coll_args_t    *args;
    ucc_status_t        status;
    size_t              data_len;
    int                 use_memh = 0;
    ucc_rank_t          i;

    /* Require fully connected topology */
    if (!ucc_tl_cuda_team_topo_is_fully_connected(team->topo)) {
        tl_debug(UCC_TL_TEAM_LIB(team),
                 "allgatherv_direct requires fully connected topology");
        return UCC_ERR_NOT_SUPPORTED;
    }

    /* Check team size limit */
    if (UCC_TL_TEAM_SIZE(team) > UCC_EE_EXECUTOR_MULTI_OP_NUM_BUFS) {
        tl_debug(UCC_TL_TEAM_LIB(team),
                 "team size %d exceeds multi-op limit %d",
                 UCC_TL_TEAM_SIZE(team), UCC_EE_EXECUTOR_MULTI_OP_NUM_BUFS);
        return UCC_ERR_NOT_SUPPORTED;
    }

    status = ucc_tl_cuda_task_init(coll_args, team, &task);
    if (ucc_unlikely(status != UCC_OK)) {
        return status;
    }

    args = &TASK_ARGS(task);

    task->allgatherv_direct.get_count     = ucc_tl_cuda_allgatherv_get_count;
    task->allgatherv_direct.get_offset    = ucc_tl_cuda_allgatherv_get_offset;
    task->allgatherv_direct.dt            = args->dst.info_v.datatype;
    task->allgatherv_direct.sbuf          = args->src.info.buffer;
    task->allgatherv_direct.rbuf          = args->dst.info_v.buffer;
    task->allgatherv_direct.exec_task     = NULL;
    task->allgatherv_direct.global_memh_dst = NULL;
    task->allgatherv_direct.use_global_memh = 0;

    for (i = 0; i < UCC_TL_TEAM_SIZE(team); i++) {
        task->allgatherv_direct.peer_dst_addr[i] = NULL;
    }

    /* Calculate total dst buffer size */
    data_len = ucc_coll_args_get_total_count(args, args->dst.info_v.counts,
                                             UCC_TL_TEAM_SIZE(team)) *
               ucc_dt_size(args->dst.info_v.datatype);

    /* Check if global_memh is available */
    if ((args->mask & UCC_COLL_ARGS_FIELD_MEM_MAP_DST_MEMH) &&
        (args->flags & UCC_COLL_ARGS_FLAG_DST_MEMH_GLOBAL) &&
        args->dst_memh.global_memh != NULL) {
        task->allgatherv_direct.global_memh_dst = args->dst_memh.global_memh;
        task->allgatherv_direct.use_global_memh = 1;
        use_memh = 1;
        tl_trace(UCC_TL_TEAM_LIB(team),
                 "using global mem_map handles for dst buffer");
        /* Get our own rank's handle for local mem_info */
        status = ucc_tl_cuda_mem_info_from_global_memh(
            args->dst_memh.global_memh, UCC_TL_TEAM_RANK(team),
            &task->allgatherv_direct.mem_info_dst);
        if (status != UCC_OK) {
            tl_debug(UCC_TL_TEAM_LIB(team),
                     "failed to get local dst handle from global_memh");
            task->allgatherv_direct.use_global_memh = 0;
            use_memh = 0;
        }
    }
    /* Check if local_memh is available */
    else if ((args->mask & UCC_COLL_ARGS_FIELD_MEM_MAP_DST_MEMH) &&
             args->dst_memh.local_memh != NULL) {
        status = ucc_tl_cuda_mem_info_from_memh(
            args->dst_memh.local_memh, &task->allgatherv_direct.mem_info_dst);
        if (status == UCC_OK) {
            use_memh = 1;
            tl_trace(UCC_TL_TEAM_LIB(team),
                     "using pre-registered mem_map handle for dst buffer");
        }
    }

    if (!use_memh) {
        /* Fallback: get IPC handle inline */
        status = ucc_tl_cuda_mem_info_get(args->dst.info_v.buffer, data_len,
                                          &task->allgatherv_direct.mem_info_dst);
        if (ucc_unlikely(status != UCC_OK)) {
            ucc_tl_cuda_task_put(task);
            return status;
        }
    }

    task->super.flags    |= UCC_COLL_TASK_FLAG_EXECUTOR;
    task->super.post      = ucc_tl_cuda_allgatherv_direct_start;
    task->super.progress  = ucc_tl_cuda_allgatherv_direct_progress;
    task->super.finalize  = ucc_tl_cuda_allgatherv_direct_finalize;
    task->bar             = TASK_BAR(task);

    *task_p = &task->super;
    return UCC_OK;
}
