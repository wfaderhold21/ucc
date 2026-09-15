/**
 * Copyright(c) 2021-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "allreduce.h"
#include "allreduce_sliding_window.h"
#include "utils/ucc_dt_reduce.h"
#include "utils/ucc_math.h"
#include "tl_ucp_ep.h"
#include "tl_ucp_sendrecv.h"

#include <string.h>

#define SAVE_STATE(_phase)

static inline ucc_status_t
ucc_tl_ucp_allreduce_sw_req_test(ucs_status_ptr_t request,
                                 ucc_tl_ucp_task_t *task)
{
    if (request == NULL) {
        return UCC_OK;
    }
    if (UCS_PTR_IS_ERR(request)) {
        tl_error(UCC_TASK_LIB(task), "unable to complete UCX request=%p: %d",
                 request, UCS_PTR_STATUS(request));
        return ucs_status_to_ucc_status(UCS_PTR_STATUS(request));
    }
    return ucs_status_to_ucc_status(ucp_request_check_status(request));
}

static inline ucc_status_t
ucc_tl_ucp_allreduce_sw_get_req(void *buffer, void *target, size_t msglen,
                                ucc_rank_t rank, ucc_tl_ucp_team_t *team,
                                ucs_status_ptr_t *req)
{
    ucp_request_param_t req_param = {0};
    int                 segment   = 0;
    ucp_rkey_h          rkey      = NULL;
    uint64_t            rva       = 0;
    ucs_status_ptr_t    ucp_status;
    ucc_status_t        status;
    ucp_ep_h            ep;

    status = ucc_tl_ucp_get_ep(team, rank, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    status = ucc_tl_ucp_resolve_p2p_by_va(team, target, &ep, rank, &rva, &rkey,
                                          &segment, NULL);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    ucp_status = ucp_get_nbx(ep, buffer, msglen, rva, rkey, &req_param);
    if (UCS_PTR_IS_ERR(ucp_status)) {
        return ucs_status_to_ucc_status(UCS_PTR_STATUS(ucp_status));
    }
    *req = ucp_status;
    return UCC_OK;
}

static inline ucc_status_t
ucc_tl_ucp_allreduce_sw_put_req(void *buffer, void *target, size_t msglen,
                                ucc_rank_t rank, ucc_tl_ucp_team_t *team,
                                ucs_status_ptr_t *req)
{
    ucp_request_param_t req_param = {0};
    int                 segment   = 0;
    ucp_rkey_h          rkey      = NULL;
    uint64_t            rva       = 0;
    ucs_status_ptr_t    ucp_status;
    ucc_status_t        status;
    ucp_ep_h            ep;

    status = ucc_tl_ucp_get_ep(team, rank, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    status = ucc_tl_ucp_resolve_p2p_by_va(team, target, &ep, rank, &rva, &rkey,
                                          &segment, NULL);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    ucp_status = ucp_put_nbx(ep, buffer, msglen, rva, rkey, &req_param);
    if (UCS_PTR_IS_ERR(ucp_status)) {
        return ucs_status_to_ucc_status(UCS_PTR_STATUS(ucp_status));
    }
    *req = ucp_status;
    return UCC_OK;
}

static inline void ucc_tl_ucp_allreduce_sw_mark_redbuf_free(
    ucc_tl_ucp_allreduce_sw_pipeline_t *pipe,
    ucc_tl_ucp_allreduce_sw_buf_t      *accbuf,
    ucc_tl_ucp_allreduce_sw_buf_t      *redbuf,
    ucc_rank_t                          host_team_size)
{
    redbuf->state = FREE;
    pipe->avail_buffs++;
    pipe->red_idx++;
    pipe->done_red++;

    if (pipe->done_red == host_team_size - 1) {
        accbuf->state = REDUCED;
        pipe->count_reduced += accbuf->count;
    }
}

ucc_status_t
ucc_tl_ucp_allreduce_sliding_window_oshmem_alloc_pipe(ucc_base_team_t   *team,
                                                       ucc_tl_ucp_task_t *task)
{
    int                      i;
    ucc_tl_ucp_team_t       *tl_team   = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_rank_t               team_size = (ucc_rank_t)team->params.size;
    ucc_tl_ucp_lib_config_t *cfg       = &UCC_TL_UCP_TEAM_LIB(tl_team)->cfg;
    size_t buf_size        = cfg->allreduce_sliding_window_buf_size;
    int    put_window_size = cfg->allreduce_sliding_window_put_window_size;
    int    num_get_bufs    = cfg->allreduce_sliding_window_num_get_bufs;
    ucc_tl_ucp_allreduce_sw_pipeline *pipe;

    if (put_window_size <= 0 || put_window_size > team_size) {
        put_window_size = team_size;
    }
    if (num_get_bufs <= 0) {
        num_get_bufs = team_size;
    }

    ucc_assert(num_get_bufs > 0);
    ucc_assert(put_window_size > 0);

    pipe = ucc_malloc(sizeof(*pipe));
    if (pipe == NULL) {
        goto err;
    }

    pipe->accbuf.buf = ucc_malloc(buf_size);
    if (pipe->accbuf.buf == NULL) {
        goto free_pipe;
    }
    pipe->getbuf = ucc_malloc(num_get_bufs * sizeof(*pipe->getbuf));
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
    pipe->put_requests =
        ucc_malloc(put_window_size * sizeof(*pipe->put_requests));
    if (pipe->put_requests == NULL) {
        goto free_getbuf;
    }

    pipe->buffer_size     = buf_size;
    pipe->num_buffers     = num_get_bufs;
    pipe->put_window_size = put_window_size;

    task->allreduce_sliding_window.pipe = pipe;
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
    ucc_tl_ucp_allreduce_sw_pipeline_t *pipe, ucc_rank_t rank)
{
    int i;

    pipe->avail_buffs    = pipe->num_buffers;
    pipe->src_rank       = pipe->dst_rank = rank;
    pipe->get_idx        = pipe->red_idx  = 0;
    pipe->done_get       = pipe->done_red = 0;
    pipe->done_put       = pipe->posted_put = 0;
    pipe->count_reduced  = pipe->count_serviced = 0;
    pipe->my_count       = pipe->my_offset = 0;
    pipe->count_received = 0;
    pipe->sync_posted    = 0;

    ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_buf(&pipe->accbuf);
    for (i = 0; i < pipe->num_buffers; i++) {
        ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_buf(&pipe->getbuf[i]);
    }

    memset(pipe->put_requests, 0,
           pipe->put_window_size * sizeof(*pipe->put_requests));
}

ucc_status_t
ucc_tl_ucp_allreduce_sliding_window_oshmem_start(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_allreduce_sw_pipeline_t *pipe;
    ucc_base_coll_args_t *coll_args = &coll_task->bargs;
    ucc_schedule_t       *schedule  = ucc_derived_of(coll_task, ucc_schedule_t);
    ucc_base_team_t      *base_team = schedule->super.team;
    ucc_tl_ucp_team_t    *team      = ucc_derived_of(base_team, ucc_tl_ucp_team_t);
    ucc_rank_t            rank      = UCC_TL_TEAM_RANK(team);
    ucc_count_t           count_total = coll_args->args.dst.info.count;
    ucc_rank_t            size      = coll_task->team->params.size;
    ucc_datatype_t        dtype     = coll_args->args.dst.info.datatype;
    size_t                dt_size   = ucc_dt_size(dtype);
    ucc_tl_ucp_task_t *rdma_task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_status_t       status;

    ucc_tl_ucp_task_reset(rdma_task, UCC_INPROGRESS);

    status = ucc_tl_ucp_allreduce_sliding_window_oshmem_alloc_pipe(base_team,
                                                                   rdma_task);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    pipe = rdma_task->allreduce_sliding_window.pipe;

    ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_pipeline(pipe, rank);

    pipe->my_count  = count_total / size;
    pipe->my_offset = pipe->my_count * dt_size * rank;
    if (rank == size - 1) {
        pipe->my_count += count_total % size;
    }
    rdma_task->allreduce_sliding_window.reduce_task = NULL;

    /* Reset the pSync counter consumed by the completion barrier. */
    ((long *)TASK_ARGS(rdma_task).global_work_buffer)[0] = 0;

    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq,
                                      &rdma_task->super);
}

void
ucc_tl_ucp_allreduce_sliding_window_oshmem_free_pipe(ucc_coll_task_t *coll_task)
{
    int                   i;
    ucc_tl_ucp_task_t    *task = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_tl_ucp_allreduce_sw_pipeline *pipe =
        task->allreduce_sliding_window.pipe;

    ucc_free(pipe->accbuf.buf);
    for (i = 0; i < pipe->num_buffers; i++) {
        ucc_free(pipe->getbuf[i].buf);
    }
    ucc_free(pipe->getbuf);
    ucc_free(pipe->put_requests);
    ucc_free(pipe);
}

ucc_status_t
ucc_tl_ucp_allreduce_sliding_window_oshmem_finalize(ucc_coll_task_t *coll_task)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_status_t       status = UCC_OK;

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
    ucc_ee_executor_t *exec;
    ucc_status_t       status;
    ucc_tl_ucp_task_t *task  = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_coll_args_t   *args  = &TASK_ARGS(task);
    ucc_datatype_t     dt    = TASK_ARGS(task).dst.info.datatype;
    size_t             count = accbuf->count;

    status = ucc_coll_task_get_executor(&task->super, &exec);
    if (ucc_unlikely(status != UCC_OK)) {
        tl_error(UCC_TASK_LIB(task), "failed to get executor");
        task->super.status = status;
        return;
    }

    status = ucc_dt_reduce(accbuf->buf, getbuf->buf, accbuf->buf, count, dt,
                           args, 0, 0, exec,
                           &task->allreduce_sliding_window.reduce_task);
    if (ucc_unlikely(status != UCC_OK)) {
        tl_error(UCC_TASK_LIB(task), "failed to perform dt reduction\n");
        task->super.status = status;
    }
}

static inline void
ucc_tl_ucp_allreduce_sliding_window_oshmem_test_reduction(ucc_tl_ucp_task_t *task)
{
    ucc_status_t status;

    EXEC_TASK_TEST(NULL, "failed to perform dt reduction",
                   task->allreduce_sliding_window.reduce_task);
    task->allreduce_sliding_window.reduce_task = NULL;
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
    void                          *src_addr;
    void                          *dst_addr;
    size_t                         red_idx;
    size_t                         put_offset;
    int                            window;
    int                            put_idx;
    ucs_status_ptr_t               request;
    ucc_status_t                   status;
    ucc_tl_ucp_task_t *task    = ucc_derived_of(coll_task, ucc_tl_ucp_task_t);
    ucc_datatype_t     dtype   = TASK_ARGS(task).dst.info.datatype;
    ucc_base_team_t   *base_team = coll_task->team;
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(base_team, ucc_tl_ucp_team_t);
    size_t             dt_size = ucc_dt_size(dtype);
    uint32_t           host_team_size = UCC_TL_TEAM_SIZE(tl_team);
    ucc_tl_ucp_allreduce_sw_pipeline_t *pipe =
        task->allreduce_sliding_window.pipe;
    ucc_tl_ucp_allreduce_sw_buf_t *accbuf = &pipe->accbuf;
    int                            i      = 0;

    ucc_assert(host_team_size > 0);

    if (task->allreduce_sliding_window.reduce_task != NULL) {
        /* A reduction started on a previous call is still pending. */
        ucc_tl_ucp_allreduce_sliding_window_oshmem_test_reduction(task);
        if (task->allreduce_sliding_window.reduce_task != NULL) {
            return;
        }
        red_idx = pipe->red_idx % pipe->num_buffers;
        redbuf  = &pipe->getbuf[red_idx];
        ucc_tl_ucp_allreduce_sw_mark_redbuf_free(pipe, accbuf, redbuf,
                                                 host_team_size);
    }

    if (pipe->count_serviced < pipe->my_count) {
        /* Post at most one get per call; avail_buffs bounds the number of
         * in-flight gets so the window slides as reductions complete. */
        if ((pipe->count_received < pipe->my_count) &&
            (pipe->done_get < host_team_size) && (pipe->avail_buffs > 0) &&
            (accbuf->state != REDUCED && accbuf->state != SENDING)) {
            remaining_elems = pipe->my_count - pipe->count_received;
            get_idx         = pipe->get_idx % pipe->num_buffers;
            count      = ucc_min(pipe->buffer_size / dt_size, remaining_elems);
            get_offset = pipe->count_received * dt_size + pipe->my_offset;
            data_size  = count * dt_size;
            src_rank   = pipe->src_rank;
            getbuf = accbuf->state == FREE ? accbuf : &pipe->getbuf[get_idx];
            src_addr = TASK_ARGS(task).src.info.buffer;
            dst_addr = getbuf->buf;

            ucc_assert(getbuf->state == FREE);

            getbuf->state = RECVING;
            getbuf->count = count;
            getbuf->bytes = data_size;
            status = ucc_tl_ucp_allreduce_sw_get_req(
                dst_addr, PTR_OFFSET(src_addr, get_offset), data_size, src_rank,
                tl_team, &getbuf->ucp_req);
            if (ucc_unlikely(UCC_OK != status)) {
                task->super.status = status;
                return;
            }
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
            request = accbuf->ucp_req;
            status  = ucc_tl_ucp_allreduce_sw_req_test(request, task);
            if (status == UCC_OK) {
                if (request) {
                    ucp_request_free(request);
                }
                accbuf->state   = REDUCING;
                accbuf->ucp_req = NULL;
            } else if (status < 0) {
                task->super.status = status;
                return;
            }
        }

        red_idx = pipe->red_idx % pipe->num_buffers;
        redbuf  = &pipe->getbuf[red_idx];
        if (accbuf->state == REDUCING && redbuf->state == RECVING) {
            request = redbuf->ucp_req;
            status  = ucc_tl_ucp_allreduce_sw_req_test(request, task);
            if (status == UCC_OK) {
                if (request) {
                    ucp_request_free(request);
                }
                redbuf->state   = REDUCING;
                redbuf->ucp_req = NULL;

                ucc_tl_ucp_allreduce_sliding_window_oshmem_reduction(
                    coll_task, accbuf, redbuf);
                ucc_tl_ucp_allreduce_sliding_window_oshmem_test_reduction(task);
                if (task->allreduce_sliding_window.reduce_task != NULL) {
                    return;
                }
                ucc_tl_ucp_allreduce_sw_mark_redbuf_free(pipe, accbuf, redbuf,
                                                         host_team_size);
            } else if (status < 0) {
                task->super.status = status;
                return;
            }
        }

        if ((pipe->count_serviced < pipe->count_reduced) &&
            (accbuf->state == REDUCED)) {
            data_size  = accbuf->bytes;
            put_offset = pipe->count_serviced * dt_size + pipe->my_offset;

            window = ucc_min(pipe->put_window_size,
                             host_team_size - pipe->posted_put);

            for (i = 0; i < window; i++) {
                dst_rank = pipe->dst_rank;
                src_addr = accbuf->buf;
                dst_addr = TASK_ARGS(task).dst.info.buffer;
                put_idx  = pipe->posted_put % pipe->put_window_size;

                if (pipe->put_requests[put_idx] != NULL) {
                    /* Already posted here and not yet complete. */
                    break;
                }
                status = ucc_tl_ucp_allreduce_sw_put_req(
                    src_addr, PTR_OFFSET(dst_addr, put_offset), data_size,
                    dst_rank, tl_team, &pipe->put_requests[put_idx]);
                if (ucc_unlikely(UCC_OK != status)) {
                    task->super.status = status;
                    return;
                }
                pipe->posted_put++;
                pipe->dst_rank = (dst_rank + 1) % host_team_size;
            }

            for (i = pipe->done_put; i < pipe->posted_put; i++) {
                put_idx = i % pipe->put_window_size;
                request = pipe->put_requests[put_idx];
                if (ucc_tl_ucp_allreduce_sw_req_test(request, task) != UCC_OK) {
                    break;
                }
                if (request) {
                    ucp_request_free(request);
                }
                pipe->put_requests[put_idx] = NULL;
                pipe->done_put++;
            }

            if (pipe->done_put == host_team_size) {
                pipe->count_serviced += accbuf->count;

                ucc_tl_ucp_allreduce_sliding_window_oshmem_reset_buf(accbuf);
                pipe->done_get = 0;
                pipe->done_red = pipe->done_put = pipe->posted_put = 0;
            }
        }

        ucp_worker_progress(TASK_CTX(task)->worker.ucp_worker);
    }

    if (pipe->count_serviced == pipe->my_count) {
        long *pSync = TASK_ARGS(task).global_work_buffer;

        /* Signal every peer (including self) that our puts are done, then
         * wait until all peers have signalled us. The atomic provides the
         * remote-visibility guarantee a local put completion does not. */
        if (!pipe->sync_posted) {
            for (i = 0; i < host_team_size; i++) {
                ucc_tl_ucp_atomic_inc(pSync, i, NULL, tl_team);
            }
            pipe->sync_posted = 1;
        }
        if (ucc_tl_ucp_test_onesided(task, host_team_size) == UCC_INPROGRESS) {
            return;
        }
        pSync[0]           = 0;
        task->super.status = UCC_OK;
    }
}
