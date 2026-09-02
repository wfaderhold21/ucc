/**
 * Copyright (c) 2020-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ec_cuda_executor.h"
#include "components/ec/ucc_ec.h"
#include "components/ec/base/ucc_ec_host_ops.h"
#include "utils/ucc_atomic.h"
#include "utils/ucc_malloc.h"
#include "utils/ucc_math.h"
#include <cuda.h>
#include <stdbool.h>

/*
 * Host-offload routing for small reduces: the interruptible executor posts
 * to the device stream; when the policy below matches, the reduce is offloaded
 * to the nested CPU executor (mirrors ec_rocm_executor_interruptible.c:
 * ec_rocm_use_host_ops + delegation).  Device-resident buffers are staged
 * D2H (sources) / H2D (result) with a CUDA event fence so the offload is
 * ordered on the stream and task_test() only completes after the H2D (580);
 * managed/zero-copy pointers go straight to the CPU executor.
 */

static volatile uint64_t ucc_ec_cuda_host_reduce_cnt = 0;
static volatile uint64_t ucc_ec_cuda_gpu_reduce_cnt  = 0;

bool ec_cuda_use_host_ops(const ucc_ee_executor_task_args_t *task_args)
{
    if (task_args->task_type != UCC_EE_EXECUTOR_TASK_REDUCE &&
        task_args->task_type != UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED) {
        return false;
    }
    if (!EC_CUDA_CONFIG->use_host_reduce) {
        return false;
    }
    if (ucc_ec_host_total_reduce_len(task_args) > EC_CUDA_CONFIG->reduce_host_limit) {
        return false;
    }
    return ucc_ec_host_dt_supported(task_args);
}

uint64_t ucc_ec_cuda_host_reduce_count(void)
{
    /* volatile read is a single aligned 8-byte load on the supported
     * archs; increments go through ucc_atomic_add64. */
    return ucc_ec_cuda_host_reduce_cnt;
}

uint64_t ucc_ec_cuda_gpu_reduce_count(void)
{
    return ucc_ec_cuda_gpu_reduce_cnt;
}

/*
 * Is the buffer device-resident?  Host, managed and zero-copy pointers
 * are directly accessible to the CPU executor; only true device pointers
 * need D2H/H2D staging.
 */
static int ucc_ec_cuda_buf_is_device(const void *ptr)
{
    struct cudaPointerAttributes attr;
    cudaError_t                  err;

    if (ptr == NULL) {
        return 0;
    }
    err = cudaPointerGetAttributes(&attr, ptr);
    if (err != cudaSuccess) {
        /* Unknown pointer (e.g. plain malloc): assume host-accessible. */
        cudaGetLastError();
        return 0;
    }
    return (attr.type == cudaMemoryTypeDevice) ? 1 : 0;
}

/* Grow a task's pinned staging buffer to at least "need" bytes. */
static ucc_status_t
ucc_ec_cuda_host_staging_grow(void  **buf,
                              size_t *cap,
                              size_t need)
{
    void        *new_buf;
    cudaError_t  err;

    if (*cap >= need) {
        return UCC_OK;
    }
    new_buf = NULL;
    err = cudaHostAlloc(&new_buf, need, 0);
    if (err != cudaSuccess) {
        ucc_error("cudaHostAlloc() failed: %d(%s)", err,
                  cudaGetErrorString(err));
        return UCC_ERR_NO_MEMORY;
    }
    if (*buf) {
        CUDA_FUNC(cudaFreeHost(*buf));
    }
    *buf = new_buf;
    *cap = need;
    return UCC_OK;
}

/*
 * Host-offload a small reduce (580).  All device copies are enqueued on
 * the executor's device stream, so they are ordered after any pending GPU
 * work on that stream:
 *
 *   [pending GPU work] -> D2H(srcs) -> streamSync -> CPU reduce
 *                        -> H2D(dst) -> event_record
 *
 * The CPU reduce runs in the posting thread (the CPU executor's sync
 * pattern; USE_THREADED_REDUCE is respected inside it).  The returned
 * task is fenced by "event" at the same offset as
 * ucc_ec_cuda_executor_interruptible_task_t.event, so the interruptible
 * task_test()/task_finalize() operate on it unchanged.
 */
ucc_status_t ucc_ec_cuda_host_offload_post(ucc_ee_executor_t *executor,
                                           const ucc_ee_executor_task_args_t *task_args,
                                           ucc_ee_executor_task_t **task)
{
    ucc_ec_cuda_resources_t                 *resources;
    ucc_ec_cuda_executor_host_staging_task_t *stg;
    ucc_ee_executor_task_args_t               local;
    ucc_ee_executor_task_t                   *cpu_task = NULL;
    ucc_status_t                              status;
    cudaStream_t                              stream = NULL;
    size_t                                    total;
    int                                       dst_is_device;
    int                                       i;

    status = ucc_ec_cuda_get_resources(&resources);
    if (ucc_unlikely(status != UCC_OK)) {
        return status;
    }
    status = ucc_cuda_executor_interruptible_get_stream(&stream);
    if (ucc_unlikely(status != UCC_OK)) {
        return status;
    }

    stg = ucc_mpool_get(&resources->executor_host_staging_tasks);
    if (ucc_unlikely(!stg)) {
        return UCC_ERR_NO_MEMORY;
    }

    status = ucc_ec_cuda_event_create(&stg->event);
    if (ucc_unlikely(status != UCC_OK)) {
        goto free_stg;
    }

    memcpy(&local, task_args, sizeof(local));

    if (task_args->task_type == UCC_EE_EXECUTOR_TASK_REDUCE) {
        char   *slots;
        void  **src_ptrs;
        void   *dst_orig;
        size_t  n_srcs;
        size_t  slot_size;

        total = local.reduce.count * ucc_dt_size(local.reduce.dt);
        dst_is_device = ucc_ec_cuda_buf_is_device(local.reduce.dst);
        dst_orig      = local.reduce.dst;

        /* Device sources are staged D2H into slot i of src_h. */
        n_srcs = local.reduce.n_srcs;
        status = ucc_ec_cuda_host_staging_grow(&stg->src_h,
                                               &stg->src_h_cap,
                                               total * n_srcs);
        if (ucc_unlikely(status != UCC_OK)) {
            goto free_event;
        }
        slots = (char *)stg->src_h;
        slot_size = total;

        /*
         * Build the host-visible source pointer array: device pointers are
         * replaced by their D2H staging slot, host/managed/zero-copy
         * pointers are kept.  n_srcs may exceed the inline 9-entry srcs[]
         * (UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT), so the array is grown
         * per task and the task is re-flagged SRCS_EXT; the CPU executor
         * (sync and threaded paths) reads srcs_ext for the full n_srcs.
         */
        if (stg->src_ptrs && stg->src_ptrs_cap >= n_srcs) {
            src_ptrs = stg->src_ptrs;
        } else {
            void *new_ptrs = ucc_malloc(n_srcs * sizeof(void *),
                                        "ec cuda host offload srcs");
            if (ucc_unlikely(!new_ptrs)) {
                status = UCC_ERR_NO_MEMORY;
                goto free_event;
            }
            if (stg->src_ptrs) {
                ucc_free(stg->src_ptrs);
            }
            stg->src_ptrs     = (void **)new_ptrs;
            stg->src_ptrs_cap = n_srcs;
            src_ptrs          = stg->src_ptrs;
        }

        for (i = 0; i < (int)n_srcs; i++) {
            const void *src =
                ((task_args->flags & UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT) ?
                 task_args->reduce.srcs_ext[i] :
                 task_args->reduce.srcs[i]);
            if (ucc_ec_cuda_buf_is_device(src)) {
                status = CUDA_FUNC(
                    cudaMemcpyAsync(slots + i * slot_size, src, total,
                                     cudaMemcpyDeviceToHost, stream));
                if (ucc_unlikely(status != UCC_OK)) {
                    goto free_event;
                }
                src_ptrs[i] = slots + i * slot_size;
            } else {
                src_ptrs[i] = (void *)src;
            }
        }
        local.flags         |= UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT;
        local.reduce.srcs_ext = src_ptrs;

        /* The result lands in a host slot when dst is device. */
        if (dst_is_device) {
            status = ucc_ec_cuda_host_staging_grow(&stg->dst_h,
                                                   &stg->dst_h_cap, total);
            if (ucc_unlikely(status != UCC_OK)) {
                goto free_event;
            }
            local.reduce.dst = stg->dst_h;
        }

        /* Wait for the D2H before the CPU reduce reads the buffers. */
        status = CUDA_FUNC(cudaStreamSynchronize(stream));
        if (ucc_unlikely(status != UCC_OK)) {
            goto free_event;
        }

        /* Run the reduce on the nested CPU executor over host views. */
        status = ucc_ee_executor_task_post(ucc_ec_cuda.cpu_executor,
                                           &local, &cpu_task);
        if (ucc_unlikely(status != UCC_OK)) {
            goto free_event;
        }
        do {
            status = ucc_ee_executor_task_test(cpu_task);
        } while (status == UCC_INPROGRESS);
        if (ucc_unlikely(status != UCC_OK)) {
            ucc_ee_executor_task_finalize(cpu_task);
            goto free_event;
        }
        ucc_ee_executor_task_finalize(cpu_task);

        /* Return the result to the device. */
        if (dst_is_device) {
            status = CUDA_FUNC(cudaMemcpyAsync(dst_orig, stg->dst_h, total,
                                               cudaMemcpyHostToDevice, stream));
            if (ucc_unlikely(status != UCC_OK)) {
                goto free_event;
            }
        }
    } else {
        /* UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED */
        char   *src_h;
        void   *dst_orig;
        size_t  off1;
        size_t  region;
        int     dev_src1;
        int     dev_src2;

        total         = local.reduce_strided.count *
                        ucc_dt_size(local.reduce_strided.dt);
        dst_orig      = local.reduce_strided.dst;
        dst_is_device = ucc_ec_cuda_buf_is_device(dst_orig);
        dev_src1      = ucc_ec_cuda_buf_is_device(local.reduce_strided.src1);
        dev_src2      = (local.reduce_strided.n_src2 > 0) &&
                        ucc_ec_cuda_buf_is_device(local.reduce_strided.src2);
        /* src2 covers (n_src2-1)*stride + total bytes. */
        region = (local.reduce_strided.n_src2 > 0)
                     ? (size_t)(local.reduce_strided.n_src2 - 1) *
                           local.reduce_strided.stride +
                           total
                     : 0;

        status = ucc_ec_cuda_host_staging_grow(&stg->src_h,
                                               &stg->src_h_cap,
                                               (size_t)total * dev_src1 +
                                                   region * dev_src2);
        if (ucc_unlikely(status != UCC_OK)) {
            goto free_event;
        }
        src_h = (char *)stg->src_h;
        off1  = (size_t)total * dev_src1;

        if (dev_src1) {
            status = CUDA_FUNC(cudaMemcpyAsync(src_h, local.reduce_strided.src1,
                                               total, cudaMemcpyDeviceToHost,
                                               stream));
            if (ucc_unlikely(status != UCC_OK)) {
                goto free_event;
            }
            local.reduce_strided.src1 = src_h;
        }
        if (dev_src2) {
            status = CUDA_FUNC(
                cudaMemcpyAsync(src_h + off1, local.reduce_strided.src2,
                                 region, cudaMemcpyDeviceToHost, stream));
            if (ucc_unlikely(status != UCC_OK)) {
                goto free_event;
            }
            local.reduce_strided.src2 = src_h + off1;
        }

        if (dst_is_device) {
            status = ucc_ec_cuda_host_staging_grow(&stg->dst_h,
                                                   &stg->dst_h_cap, total);
            if (ucc_unlikely(status != UCC_OK)) {
                goto free_event;
            }
            local.reduce_strided.dst = stg->dst_h;
        }

        status = CUDA_FUNC(cudaStreamSynchronize(stream));
        if (ucc_unlikely(status != UCC_OK)) {
            goto free_event;
        }

        status = ucc_ee_executor_task_post(ucc_ec_cuda.cpu_executor,
                                           &local, &cpu_task);
        if (ucc_unlikely(status != UCC_OK)) {
            goto free_event;
        }
        do {
            status = ucc_ee_executor_task_test(cpu_task);
        } while (status == UCC_INPROGRESS);
        if (ucc_unlikely(status != UCC_OK)) {
            ucc_ee_executor_task_finalize(cpu_task);
            goto free_event;
        }
        ucc_ee_executor_task_finalize(cpu_task);

        if (dst_is_device) {
            status = CUDA_FUNC(cudaMemcpyAsync(dst_orig, stg->dst_h, total,
                                               cudaMemcpyHostToDevice, stream));
            if (ucc_unlikely(status != UCC_OK)) {
                goto free_event;
            }
        }
    }

    /* Fence the H2D (or the completed stream position) for task_test(). */
    status = ucc_ec_cuda_event_post(stream, stg->event);
    if (ucc_unlikely(status != UCC_OK)) {
        goto free_event;
    }

    stg->super.eee    = executor;
    stg->super.status = UCC_INPROGRESS;
    memcpy(&stg->super.args, task_args, sizeof(ucc_ee_executor_task_args_t));
    *task = &stg->super;
    return UCC_OK;

free_event:
    ucc_ec_cuda_event_destroy(stg->event);
    stg->event = NULL;
free_stg:
    ucc_mpool_put(stg);
    return status;
}
ucc_status_t ucc_cuda_executor_interruptible_get_stream(cudaStream_t *stream)
{
    static uint32_t          last_used   = 0;
    int                      num_streams = EC_CUDA_CONFIG->exec_num_streams;
    ucc_ec_cuda_resources_t *resources;
    ucc_status_t             st;
    int                      i, j;
    uint32_t                 id;

    ucc_assert(num_streams > 0);
    if (ucc_unlikely(num_streams <= 0)) {
        return UCC_ERR_INVALID_PARAM;
    }
    st = ucc_ec_cuda_get_resources(&resources);
    if (ucc_unlikely(st != UCC_OK)) {
        return st;
    }

    if (ucc_unlikely(!resources->streams_initialized)) {
        ucc_spin_lock(&ucc_ec_cuda.init_spinlock);
        if (resources->streams_initialized) {
            goto unlock;
        }

        for(i = 0; i < num_streams; i++) {
            st = CUDA_FUNC(cudaStreamCreateWithFlags(&resources->exec_streams[i],
                                                     cudaStreamNonBlocking));
            if (st != UCC_OK) {
                for (j = 0; j < i; j++) {
                    CUDA_FUNC(cudaStreamDestroy(resources->exec_streams[j]));
                }
                ucc_spin_unlock(&ucc_ec_cuda.init_spinlock);
                return st;
            }
        }
        resources->streams_initialized = 1;
unlock:
        ucc_spin_unlock(&ucc_ec_cuda.init_spinlock);
    }

    id = ucc_atomic_fadd32(&last_used, 1);
    *stream = resources->exec_streams[id % num_streams];
    return UCC_OK;
}


ucc_status_t ucc_ec_cuda_copy_multi_kernel(const ucc_ee_executor_task_args_t *args,
                                           cudaStream_t stream);

ucc_status_t
ucc_cuda_executor_interruptible_task_post(ucc_ee_executor_t *executor,
                                         const ucc_ee_executor_task_args_t *task_args,
                                         ucc_ee_executor_task_t **task)
{
    cudaStream_t stream    = NULL;
    size_t       num_nodes = UCC_EE_EXECUTOR_MULTI_OP_NUM_BUFS;
    ucc_ec_cuda_executor_interruptible_task_t *ee_task;
    ucc_status_t status;
    cudaGraphNode_t nodes[UCC_EE_EXECUTOR_MULTI_OP_NUM_BUFS];
    ucc_ec_cuda_resources_t *resources;
    int i;

    /* Host-offload: small, host-supported reduces run on the nested CPU
     * executor (580).  Device-resident buffers are staged D2H/H2D with a
     * CUDA event fence; host/managed/zero-copy buffers go straight to the
     * CPU executor. */
    if (ec_cuda_use_host_ops(task_args)) {
        ucc_atomic_add64(&ucc_ec_cuda_host_reduce_cnt, 1);
        ec_trace(&ucc_ec_cuda.super,
                 "routing reduce (host policy) to CPU executor");
        status = ucc_ec_cuda_host_offload_post(executor, task_args, task);
        if (ucc_unlikely(status != UCC_OK)) {
            ec_error(&ucc_ec_cuda.super,
                     "failed to execute host reduce from CUDA component");
        }
        return status;
    }
    if (task_args->task_type == UCC_EE_EXECUTOR_TASK_REDUCE ||
        task_args->task_type == UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED) {
        ucc_atomic_add64(&ucc_ec_cuda_gpu_reduce_cnt, 1);
    }

    status = ucc_ec_cuda_get_resources(&resources);
    if (ucc_unlikely(status != UCC_OK)) {
        return status;
    }

    status = ucc_cuda_executor_interruptible_get_stream(&stream);
    if (ucc_unlikely(status != UCC_OK)) {
        return status;
    }

    ee_task = ucc_mpool_get(&resources->executor_interruptible_tasks);
    if (ucc_unlikely(!ee_task)) {
        return UCC_ERR_NO_MEMORY;
    }

    status  = ucc_ec_cuda_event_create(&ee_task->event);
    if (ucc_unlikely(status != UCC_OK)) {
        ucc_mpool_put(ee_task);
        return status;
    }
    ee_task->super.status = UCC_INPROGRESS;
    ee_task->super.eee    = executor;
    memcpy(&ee_task->super.args, task_args, sizeof(ucc_ee_executor_task_args_t));
    switch (task_args->task_type) {
    case UCC_EE_EXECUTOR_TASK_COPY:
        status = CUDA_FUNC(
            cudaMemcpyAsync(task_args->copy.dst, task_args->copy.src,
                            task_args->copy.len, cudaMemcpyDefault, stream));
        if (ucc_unlikely(status != UCC_OK)) {
            ec_error(&ucc_ec_cuda.super, "failed to start memcpy op");
            goto free_task;
        }
        break;
    case UCC_EE_EXECUTOR_TASK_COPY_MULTI:
        if ((task_args->copy_multi.counts[0] > EC_CUDA_CONFIG->exec_copy_thresh) &&
            (task_args->copy_multi.num_vectors > 2)) {
            status = CUDA_FUNC(cudaGraphGetNodes(ee_task->graph, nodes,
                                                 &num_nodes));
            if (ucc_unlikely(status != UCC_OK)) {
                ec_error(&ucc_ec_cuda.super, "failed to get graph nodes");
                goto free_task;
            }
            for (i = 0; i < task_args->copy_multi.num_vectors; i++) {
                status = CUDA_FUNC(
                    cudaGraphExecMemcpyNodeSetParams1D(ee_task->graph_exec, nodes[i],
                                                       task_args->copy_multi.dst[i],
                                                       task_args->copy_multi.src[i],
                                                       task_args->copy_multi.counts[i],
                                                       cudaMemcpyDefault));
                if (ucc_unlikely(status != UCC_OK)) {
                    ec_error(&ucc_ec_cuda.super, "failed to instantiate graph");
                    goto free_task;
                }

            }
            for (; i < UCC_EE_EXECUTOR_MULTI_OP_NUM_BUFS; i++) {
                status = CUDA_FUNC(
                    cudaGraphExecMemcpyNodeSetParams1D(ee_task->graph_exec, nodes[i],
                                                       task_args->copy_multi.dst[0],
                                                       task_args->copy_multi.src[0],
                                                       1, cudaMemcpyDefault));
                if (ucc_unlikely(status != UCC_OK)) {
                    ec_error(&ucc_ec_cuda.super, "failed to instantiate graph");
                    goto free_task;
                }
            }

            status = CUDA_FUNC(cudaGraphLaunch(ee_task->graph_exec, stream));
            if (ucc_unlikely(status != UCC_OK)) {
                ec_error(&ucc_ec_cuda.super, "failed to instantiate graph");
                goto free_task;
            }

        } else {
            status = ucc_ec_cuda_copy_multi_kernel(task_args, stream);
            if (ucc_unlikely(status != UCC_OK)) {
                ec_error(&ucc_ec_cuda.super, "failed to start copy multi op");
                goto free_task;
            }
        }
        break;
    case UCC_EE_EXECUTOR_TASK_REDUCE:
    case UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED:
    case UCC_EE_EXECUTOR_TASK_REDUCE_MULTI_DST:
        status = ucc_ec_cuda_reduce(
            (ucc_ee_executor_task_args_t *)task_args,
            resources->num_threads_reduce,
            resources->num_blocks_reduce,
            stream);
        if (ucc_unlikely(status != UCC_OK)) {
            ec_error(&ucc_ec_cuda.super, "failed to start reduce op");
            goto free_task;
        }
        break;
    default:
        ec_error(&ucc_ec_cuda.super, "executor operation %d is not supported",
                 task_args->task_type);
        status = UCC_ERR_INVALID_PARAM;
        goto free_task;
    }

    status = ucc_ec_cuda_event_post(stream, ee_task->event);
    if (ucc_unlikely(status != UCC_OK)) {
        goto free_task;
    }

    *task = &ee_task->super;
    return UCC_OK;

free_task:
    ucc_ec_cuda_event_destroy(ee_task->event);
    ucc_mpool_put(ee_task);
    return status;
}

ucc_status_t
ucc_cuda_executor_interruptible_task_test(const ucc_ee_executor_task_t *task)
{
    ucc_ec_cuda_executor_interruptible_task_t *ee_task =
        ucc_derived_of(task, ucc_ec_cuda_executor_interruptible_task_t);

    ee_task->super.status = ucc_ec_cuda_event_test(ee_task->event);
    return ee_task->super.status;
}

ucc_status_t
ucc_cuda_executor_interruptible_task_finalize(ucc_ee_executor_task_t *task)
{
    ucc_ec_cuda_executor_interruptible_task_t *ee_task =
        ucc_derived_of(task, ucc_ec_cuda_executor_interruptible_task_t);
    ucc_status_t status;

    ucc_assert(task->status == UCC_OK);
    status = ucc_ec_cuda_event_destroy(ee_task->event);
    ucc_mpool_put(task);
    return status;
}

ucc_status_t ucc_cuda_executor_interruptible_start(ucc_ee_executor_t *executor)
{
    ucc_ec_cuda_executor_t *eee = ucc_derived_of(executor,
                                                 ucc_ec_cuda_executor_t);

    eee->mode  = UCC_EC_CUDA_EXECUTOR_MODE_INTERRUPTIBLE;
    eee->state = UCC_EC_CUDA_EXECUTOR_STARTED;

    eee->ops.task_post     = ucc_cuda_executor_interruptible_task_post;
    eee->ops.task_test     = ucc_cuda_executor_interruptible_task_test;
    eee->ops.task_finalize = ucc_cuda_executor_interruptible_task_finalize;

    return UCC_OK;
}

ucc_status_t ucc_cuda_executor_interruptible_stop(ucc_ee_executor_t *executor)
{
    ucc_ec_cuda_executor_t *eee = ucc_derived_of(executor,
                                                 ucc_ec_cuda_executor_t);

    eee->state = UCC_EC_CUDA_EXECUTOR_INITIALIZED;
    return UCC_OK;
}
