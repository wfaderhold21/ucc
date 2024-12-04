/**
 * Copyright(c) 2021-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "allreduce.h"
#include "allreduce_sliding_window.h"
#include "../allgather/allgather.h"
#include "../barrier/barrier.h"
#include "utils/ucc_dt_reduce.h"
#include "tl_ucp_ep.h"
#include "tl_ucp_sendrecv.h"

#include <omp.h>

ucc_status_t ucc_tl_ucp_barrier_knomial_start(ucc_coll_task_t *task);

//static void * memtest[UCC_TL_UCP_MAX_THREADS]  = {0};

ucc_status_t
ucc_tl_ucp_allreduce_sliding_window_oshmem_alloc_pipe(ucc_base_team_t   *team,
                                               ucc_tl_ucp_task_t *task)
{
    int                      i;
    ucc_tl_ucp_team_t       *tl_team   = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_rank_t               team_size = (ucc_rank_t)team->params.size;
    ucc_tl_ucp_lib_config_t *cfg       = &UCC_TL_UCP_TEAM_LIB(tl_team)->cfg;

    size_t buf_size = cfg->allreduce_sliding_window_buf_size;
    int   put_window_size = cfg->allreduce_sliding_window_put_window_size;
    int      num_get_bufs = cfg->allreduce_sliding_window_num_get_bufs;
    size_t nelems = TASK_ARGS(task).src.info.count;
    nelems = nelems * ucc_dt_size(TASK_ARGS(task).src.info.datatype);

    ucc_tl_ucp_allreduce_sw_pipeline *pipe =
        (ucc_tl_ucp_allreduce_sw_pipeline *)ucc_malloc(
            sizeof(ucc_tl_ucp_allreduce_sw_pipeline));
    if (pipe == NULL) {
        goto err;
    }

    if (put_window_size <= 0) {
        put_window_size = team_size;
    }

    if (num_get_bufs <= 0) {
        num_get_bufs = team_size;
    }
/*
    if (nelems >= 131072) { // 1 / 4 cache size
        tl_debug(UCC_TASK_LIB(task), "resetting buffer size to 1/4 cache size");
        buf_size = 131072;
    } else {
        buf_size = nelems;
    }
*/
    ucc_assert(num_get_bufs > 0);
    ucc_assert(put_window_size > 0);

    pipe->accbuf.buf = ucc_malloc(buf_size);
    if (pipe->accbuf.buf == NULL) {
        goto free_pipe;
    }
    pipe->getbuf = (ucc_tl_ucp_allreduce_sw_buf_t *)ucc_malloc(
        num_get_bufs * sizeof(ucc_tl_ucp_allreduce_sw_buf_t));
    if (pipe->getbuf == NULL) {
        goto free_acc;
    }
    for (i = 0; i < num_get_bufs; i++) {
        pipe->getbuf[i].buf = NULL;
    }
    for (i = 0; i < num_get_bufs; i++) {
        pipe->getbuf[i].buf = ucc_malloc(buf_size);
        if (pipe->getbuf[i].buf == NULL) {
            goto free_getbuf;
        }
    }

    pipe->buffer_size  = buf_size;
    pipe->num_buffers  = num_get_bufs;

    task->allreduce_sliding_window.pipe = pipe;
    //printf("STARTED\n");
    return UCC_OK;

free_getbuf:
    for (i = 0; i < num_get_bufs; i++) {
        if (pipe->getbuf[i].buf == NULL)
            break;
        ucc_free(pipe->getbuf[i].buf);
    }
    ucc_free(pipe->getbuf);
free_acc:
    ucc_free(pipe->accbuf.buf);
free_pipe:
    ucc_free(pipe);
err:
    tl_error(UCC_TL_TEAM_LIB(tl_team), "error allocating sliding window pipe\n");
    return UCC_ERR_NO_RESOURCE;
}


static inline void
ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_buf(ucc_tl_ucp_allreduce_sw_buf_t *buf)
{
    buf->state   = FREE;
    buf->count   = 0;
    buf->bytes   = 0;
    buf->ucp_req = NULL;
}

static inline void ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_pipeline(
    ucc_tl_ucp_allreduce_sw_pipeline_t *pipe, ucc_rank_t rank,
    size_t put_window_size)
{
    int i;

    pipe->avail_buffs   = pipe->num_buffers;
    pipe->src_rank      = pipe->dst_rank       = rank;
    pipe->get_idx       = pipe->red_idx        = 0;
    pipe->done_get      = pipe->done_red       = 0;
    pipe->done_put      = pipe->posted_put     = 0;
    pipe->count_reduced = pipe->count_serviced = 0;
    pipe->my_count      = pipe->my_offset      = 0;
    pipe->count_received = 0;

    ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_buf(&pipe->accbuf);
    for (i = 0; i < pipe->num_buffers; i++) {
        ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_buf(&pipe->getbuf[i]);
    }
}

ucc_status_t
ucc_tl_ucp_allreduce_sliding_window_oshmem_alloc_pipe(ucc_base_team_t   *team,
                                               ucc_tl_ucp_task_t *task);

ucc_status_t
ucc_tl_ucp_allreduce_sliding_window_oshmem_start(ucc_coll_task_t *coll_task)
{

    ucc_tl_ucp_allreduce_sw_pipeline_t       *pipe;
    ucc_base_coll_args_t *coll_args   = &coll_task->bargs;
    ucc_schedule_t       *schedule    = ucc_derived_of(coll_task,
                                                       ucc_schedule_t);
    ucc_base_team_t      *base_team   = schedule->super.team;
    ucc_tl_ucp_team_t    *team        = ucc_derived_of(base_team,
                                                       ucc_tl_ucp_team_t);
    ucc_rank_t            rank        = UCC_TL_TEAM_RANK(team);
    uint32_t              count_total = coll_args->args.dst.info.count;
    ucc_rank_t            size        = coll_task->team->params.size;
    ucc_datatype_t        dtype       = coll_args->args.dst.info.datatype;
    size_t                dt_size     = ucc_dt_size(dtype);
    int                   put_window_size = UCC_TL_UCP_TEAM_LIB(team)
                                 ->cfg.allreduce_sliding_window_put_window_size;
    ucc_tl_ucp_task_t *rdma_task = ucc_derived_of(coll_task,
                                                  ucc_tl_ucp_task_t);
    ucc_tl_ucp_task_reset(rdma_task, UCC_INPROGRESS);

    ucc_tl_ucp_allreduce_sliding_window_oshmem_alloc_pipe(base_team, rdma_task);
    pipe           = rdma_task->allreduce_sliding_window.pipe;

    rdma_task->allreduce_sliding_window.bufs =
        ucc_malloc(sizeof(ucc_tl_ucp_dpu_offload_buf_info_t));
    if (rdma_task->allreduce_sliding_window.bufs == NULL) {
        return UCC_ERR_NO_MESSAGE;
    }
    if (put_window_size <= 0)
        put_window_size = size;

    ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_pipeline(
        pipe, rank, put_window_size);

    pipe->my_count  = count_total / size;
    pipe->my_offset = pipe->my_count * dt_size * rank;
    if (rank == size - 1) {
        pipe->my_count += count_total % size;
    }
    rdma_task->allreduce_sliding_window.reduce_task  = NULL;
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &rdma_task->super);//ucc_schedule_start(coll_task);
}

void
ucc_tl_ucp_allreduce_sliding_window_oshmem_free_task(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t    *task    = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);

    ucc_free(task->allreduce_sliding_window.bufs);
}

void
ucc_tl_ucp_allreduce_sliding_window_oshmem_free_pipe(ucc_coll_task_t *coll_task)
{
    int                   i;
    ucc_base_team_t      *team    = coll_task->team;
    ucc_tl_ucp_team_t    *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_task_t    *task    = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_allreduce_sw_pipeline *pipe =
        task->allreduce_sliding_window.pipe;
    int num_get_bufs =
        UCC_TL_UCP_TEAM_LIB(tl_team)->cfg.allreduce_sliding_window_num_get_bufs;

    ucc_free(pipe->accbuf.buf);
    for (i = 0; i < num_get_bufs; i++) {
        ucc_free(pipe->getbuf[i].buf);
    }
    ucc_free(pipe->getbuf);
    ucc_free(pipe);
}

ucc_status_t
ucc_tl_ucp_allreduce_sliding_window_oshmem_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_status_t status = UCC_OK;

    ucc_tl_ucp_allreduce_sliding_window_oshmem_free_task(coll_task);
    ucc_tl_ucp_allreduce_sliding_window_oshmem_free_pipe(coll_task);

    status = ucc_tl_ucp_coll_finalize(coll_task);
    if (ucc_unlikely(status != UCC_OK)) {
        tl_error(UCC_TASK_LIB(task), "failed to finalize collective");
    }

    return status;
}

static inline void ucc_tl_ucp_allreduce_sliding_window_oshmem_reduction(
    ucc_coll_task_t *coll_task, ucc_tl_ucp_allreduce_sw_buf_t *accbuf,
    ucc_tl_ucp_allreduce_sw_buf_t *getbuf)
{
    ucc_ee_executor_t *exec;//[UCC_TL_UCP_MAX_THREADS];
    ucc_status_t       status = UCC_OK;
    ucc_tl_ucp_task_t *task   = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_coll_args_t   *args   = &TASK_ARGS(task);
    ucc_datatype_t     dt     = TASK_ARGS(task).dst.info.datatype;
    status = ucc_coll_task_get_executor(&task->super, &exec);
    if (ucc_unlikely(status != UCC_OK)) {
        tl_error(UCC_TASK_LIB(task), "failed to get executor");
    }


    {
        ucc_status_t pstatus;
        size_t count = accbuf->count;

        //printf("reducing %ld bytes of %ld\n", getbuf->bytes, getbuf->bytes);
        pstatus =
            ucc_dt_reduce(accbuf->buf, getbuf->buf, accbuf->buf, count, dt,
                          args, 0, 0, exec,
                          &task->allreduce_sliding_window.reduce_task);
        if (pstatus != UCC_OK) {
            tl_error(UCC_TASK_LIB(task), "failed to perform dt reduction\n");
        }
    }
}

static inline void
ucc_tl_ucp_allreduce_sliding_window_oshmem_test_reduction(ucc_tl_ucp_task_t *task)
{
    ucc_status_t status;

    #define SAVE_STATE(_phase)

    EXEC_TASK_TEST(NULL, "failed to perform dt reduction", task->allreduce_sliding_window.reduce_task);
    task->allreduce_sliding_window.reduce_task = NULL;
    /*
    if (task->allreduce_sliding_window.reduce_task != NULL) {                          
        int i = 0;
        for (i = 0; i < 5; i++) {
            status = ucc_ee_executor_task_test(task->allreduce_sliding_window.reduce_task);
            if (status == 0) {                          
                ucc_ee_executor_task_finalize(task->allreduce_sliding_window.reduce_task);     
                task->allreduce_sliding_window.reduce_task = NULL;
                break;
            }
        }
    } */                                             

    // If it didn't complete, we would have returned by now. So, clear the flag
}

void ucc_tl_ucp_allreduce_sliding_window_oshmem_rdma_progress(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_allreduce_sw_buf_t *redbuf;
    ucc_tl_ucp_allreduce_sw_buf_t *getbuf;
    size_t                         remaining_elems;
    size_t                         get_idx;
    size_t                         count;
    size_t                         get_offset;
    size_t                         data_size;
    ucc_rank_t                     src_rank;
    ucc_rank_t                     dst_rank;
    void                          *src_addr; //= TASK_ARGS(task).src.info.buffer;
    void                          *dst_addr;// = TASK_ARGS(task).dst.info.buffer;
    size_t                         red_idx;
    size_t                         put_offset;
    int                            window;
    //int                            put_idx;
    ucc_tl_ucp_task_t *task    = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    //long *pSync = TASK_ARGS(task).global_work_buffer;
    //ucc_rank_t         size    = (ucc_rank_t)task->subset.map.ep_num;
    ucc_datatype_t     dtype   = TASK_ARGS(task).dst.info.datatype;
    ucc_base_team_t   *base_team      = coll_task->team;
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(base_team, ucc_tl_ucp_team_t);
    size_t             dt_size = ucc_dt_size(dtype);
    uint32_t           host_team_size = UCC_TL_TEAM_SIZE(tl_team);//size;
    ucc_tl_ucp_allreduce_sw_pipeline_t *pipe =
        task->allreduce_sliding_window.pipe;
    //ucc_tl_ucp_context_t          *tl_ctx    = UCC_TL_UCP_TEAM_CTX(tl_team);
    ucc_tl_ucp_allreduce_sw_buf_t *accbuf    = &pipe->accbuf;
    int                            i         = 0;
    int              put_window_size =
        UCC_TL_UCP_TEAM_LIB(tl_team)->
            cfg.allreduce_sliding_window_put_window_size;

    ucc_assert(host_team_size > 0);
    if (task->allreduce_sliding_window.reduce_task != NULL) { 
       // We've previously started a reduction on the accbuf that hasn't yet
        // completed.
        ucc_tl_ucp_allreduce_sliding_window_oshmem_test_reduction(task);

        if (task->allreduce_sliding_window.reduce_task != NULL) {
            return;
        }
        red_idx = pipe->red_idx % pipe->num_buffers;
        redbuf  = &pipe->getbuf[red_idx];

            redbuf->state = FREE;
            pipe->avail_buffs++;
            pipe->red_idx++;
            pipe->done_red++;

            if (pipe->done_red == host_team_size - 1) {
                accbuf->state = REDUCED;
                pipe->count_reduced += accbuf->count;
            }

    }
    if (pipe->count_serviced < pipe->my_count) {
        //printf("pipe->avail_buffs: %ld, pipe->done_get: %d, pipe->count_received: %ld\n", pipe->avail_buffs, pipe->done_get, pipe->count_received);
        while ((pipe->count_received < pipe->my_count) &&
            (pipe->done_get < host_team_size) && (pipe->avail_buffs > 0) &&
            (accbuf->state != REDUCED && accbuf->state != SENDING)) {
            remaining_elems = pipe->my_count - pipe->count_received;
            get_idx         = pipe->get_idx % pipe->num_buffers;
            count      = ucc_min(pipe->buffer_size / dt_size, remaining_elems);
            get_offset = pipe->count_received * dt_size + pipe->my_offset;
            data_size  = count * dt_size;
            src_rank   = pipe->src_rank;
            getbuf = accbuf->state == FREE ? accbuf : &pipe->getbuf[get_idx];
            src_addr = TASK_ARGS(task).src.info.buffer;/*(char*)
                task->allreduce_sliding_window.bufs->sbufs[src_rank] +
                get_offset; */
            dst_addr = getbuf->buf;

            ucc_assert(getbuf->state == FREE);

            getbuf->state   = RECVING;
            getbuf->count   = count;
            getbuf->bytes   = data_size;
            //printf("dst %p, src %p, offset %lx\n", dst_addr, src_addr, get_offset);
            ucc_tl_ucp_get_nb(dst_addr, src_addr + get_offset, getbuf->bytes, src_rank, tl_team, task);
            pipe->src_rank = (src_rank + 1) % host_team_size;

            if (getbuf != accbuf) {
                pipe->avail_buffs--;
                pipe->get_idx++;
            }

            pipe->done_get++;
            if (pipe->done_get == host_team_size) {
                pipe->count_received += count;
            }
        }

        if (accbuf->state == RECVING) {
            int probes = 0;
            //request = accbuf->ucp_req;
            for (probes = 0; probes < 5; probes++){ 
                if (task->onesided.get_posted == task->onesided.get_completed) {
                    accbuf->state   = REDUCING;
                    accbuf->ucp_req = NULL;
                    break;
                } else {
                    ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
                }
            }
            if (probes == 5) {
                return;
            }
        }

        red_idx = pipe->red_idx % pipe->num_buffers;
        redbuf  = &pipe->getbuf[red_idx];
        if (accbuf->state == REDUCING && redbuf->state == RECVING) {
                if (task->onesided.get_posted == task->onesided.get_completed) {
                    redbuf->state   = REDUCING;

                    ucc_tl_ucp_allreduce_sliding_window_oshmem_reduction(coll_task, accbuf,
                                                                  redbuf);

                    ucc_tl_ucp_allreduce_sliding_window_oshmem_test_reduction(task);

                    if (task->allreduce_sliding_window.reduce_task != NULL) {
                        return;
                    }

                    redbuf->state = FREE;
                    pipe->avail_buffs++;
                    pipe->red_idx++;
                    pipe->done_red++;

                    if (pipe->done_red == host_team_size - 1) {
                        accbuf->state = REDUCED;
                        pipe->count_reduced += accbuf->count;
                    }
                } else {
                    ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
                }
        }

        if ((pipe->count_serviced < pipe->count_reduced) &&
            (accbuf->state == REDUCED)) {
            data_size  = accbuf->bytes;
            put_offset = pipe->count_serviced * dt_size + pipe->my_offset;

            if (put_window_size <= 0)
                put_window_size = host_team_size;

            ucc_assert(put_window_size > 0);

            window = ucc_min(put_window_size,
                             host_team_size - pipe->posted_put);

            for (i = 0; i < window; i++) {
                dst_rank = pipe->dst_rank;
                src_addr = accbuf->buf;
                dst_addr = TASK_ARGS(task).dst.info.buffer + put_offset;/*(char*)
                    task->allreduce_sliding_window.bufs->rbufs[dst_rank] +
                    put_offset; */
                //put_idx = pipe->posted_put %
                //          put_window_size;

                //ucp_worker_fence(tl_ctx->worker.ucp_worker);
                ucc_tl_ucp_put_nb(src_addr, dst_addr, data_size, dst_rank, tl_team, task);
                pipe->posted_put++;
                pipe->dst_rank = (dst_rank + 1) % host_team_size;
            }
//    if (pipe->count_serviced < pipe->count_reduced) {
        while (task->onesided.put_posted > task->onesided.put_completed) {
            ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
        }
        pipe->done_put += pipe->posted_put - pipe->done_put;
#if 0
            for (int z = 0; z < 5; z++) {
                if (task->onesided.put_posted == task->onesided.put_completed) {
                    pipe->done_put += pipe->posted_put - pipe->done_put;
/*                pipe->count_serviced += accbuf->count;
    
                ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_buf(accbuf);
                pipe->done_get = 0;
                pipe->done_red = pipe->done_put = pipe->posted_put = 0;*/
                    break;
                }
                ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
            }
        }
#endif
//    }
/*
            while (task->onesided.put_posted > task->onesided.put_completed) {
            //for (int z = 0; z < 5; z++) {
                ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
            }
            //}
 */
            if (pipe->done_put == host_team_size) {           
                pipe->count_serviced += accbuf->count;
    
                ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_buf(accbuf);
                pipe->done_get = 0;
                pipe->done_red = pipe->done_put = pipe->posted_put = 0;
            }
            //printf("puts complete\n");
        }
#if 0            
        if (task->onesided.put_posted > task->onesided.put_completed) {
            for (int z = 0; z < 5; z++) {
                if (task->onesided.put_posted == task->onesided.put_completed) {
                    pipe->count_serviced += accbuf->count;

                    ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_buf(accbuf);
                    pipe->done_get = 0;
                    pipe->done_red = pipe->done_put = pipe->posted_put = 0;
                    break;
                }
            }
        }
#endif
    //    ucp_worker_progress(tl_ctx->worker.ucp_worker);
    }

    if (pipe->count_serviced == pipe->my_count) {
#if 0 
        int pcount = 0;
        if (pSync[1] == 0) {
            for (int z = 0; z < host_team_size; z++) {
                ucc_tl_ucp_atomic_inc(pSync, z, tl_team);
            }
            pSync[1] = 1;
        }
        for (pcount = 0; pcount < 5; pcount++) {
            if (pSync[0] != host_team_size) {
                ucp_worker_progress(tl_ctx->worker.ucp_worker);
            } else {
                break;
            }
        }
        if (pcount == 5) {
            return;
        }
        pSync[0] = 0;
        pSync[1] = 0;
#endif
        task->super.status                          = UCC_OK;
    }
}
