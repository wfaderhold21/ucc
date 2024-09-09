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

void ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask);

typedef struct pass_info {
    uint64_t xgvmi_flag;
    size_t src_xgvmi_size;
    size_t dst_xgvmi_size;
    char rkeys[];
} pass_info_t;

typedef struct ex_info {
    uint64_t dst;
    uint64_t dst_key_len;
    char dst_key[600];
} ex_info_t;

static ucp_mem_h latest_src_xgvmi_mh = NULL;
static ucp_mem_h latest_dest_xgvmi_mh = NULL;
static uint64_t latest_src_size = 0;
static uint64_t latest_dst_addrs[128] = {0};
void * latest_dst_packed_keys[128] = {0};
//static ucp_rkey_h src_keys[128];
static ucp_rkey_h dst_keys[128] = {0};

static inline ucc_status_t put_nb(void *buffer, size_t target, size_t target_offset,
                                  size_t             msglen,
                                  ucc_rank_t         dest_group_rank,
                                  void * packed_key,
                                  ucc_memory_type_t mtype,
                                  ucp_rkey_h *rkey,
                                  ucc_tl_ucp_team_t *team,
                                  ucc_tl_ucp_task_t *task)
{
    ucp_request_param_t req_param = {0};
    uint64_t            rva       = 0;
    ucs_status_ptr_t    ucp_status = UCS_OK;
    ucc_status_t        status;
    ucp_ep_h            ep;
    ucp_rkey_h          actual_rkey;
    ucs_status_t ucs_status;

    status = ucc_tl_ucp_get_ep(team, dest_group_rank, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    
    rva = target + target_offset;
    if (packed_key != NULL) {
        ucs_status = ucp_ep_rkey_unpack(ep, packed_key, &actual_rkey);
        if (ucs_status != UCS_OK) {
            return UCC_ERR_NO_MESSAGE;
        }
        *rkey = actual_rkey;
    }
 
    req_param.op_attr_mask =
        UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    req_param.cb.send   = ucc_tl_ucp_put_completion_cb;
    req_param.user_data = (void *)task;

    if (latest_src_xgvmi_mh) {
        req_param.op_attr_mask |= UCP_OP_ATTR_FIELD_MEMH;
        req_param.memh = latest_src_xgvmi_mh; //PTR_OFFSET(ipass->rkeys, ipass->src_size + ipass->dst_size);
    }

//    if (rva != (uint64_t) buffer) {
        ucp_status = ucp_put_nbx(ep, buffer, msglen, rva, *rkey, &req_param);
/*    } else {
        task->onesided.put_posted++;
        task->onesided.put_completed++;
        return UCC_OK;
    }*/

    task->onesided.atomic_posted++;
    if (UCS_OK != ucp_status) {
        if (UCS_PTR_IS_ERR(ucp_status)) {
            return ucs_status_to_ucc_status(UCS_PTR_STATUS(ucp_status));
        }
    } else {
        task->onesided.atomic_completed++;
    }
    return UCC_OK;
}

static inline ucc_status_t get_nb(void *buffer, size_t target, size_t target_offset,
                                  size_t             msglen,
                                  ucc_rank_t         dest_group_rank,
                                  void * packed_key,
                                  ucc_memory_type_t mtype,
                                  ucp_rkey_h *rkey,
                                  ucc_tl_ucp_team_t *team,
                                  ucc_tl_ucp_task_t *task)
{
    ucp_request_param_t req_param = {0};
    uint64_t            rva       = 0;
    ucs_status_ptr_t    ucp_status = UCS_OK;
    ucc_status_t        status;
    ucp_ep_h            ep;
    ucp_rkey_h          actual_rkey;
    ucs_status_t ucs_status;

    status = ucc_tl_ucp_get_ep(team, dest_group_rank, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }
    
    rva = target + target_offset;
    if (packed_key != NULL) {
        ucs_status = ucp_ep_rkey_unpack(ep, packed_key, &actual_rkey);
        if (ucs_status != UCS_OK) {
            return UCC_ERR_NO_MESSAGE;
        }
        *rkey = actual_rkey;
    }
 
    req_param.op_attr_mask =
        UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    req_param.cb.send   = ucc_tl_ucp_get_completion_cb;
    req_param.user_data = (void *)task;

    if (latest_dest_xgvmi_mh) {
        req_param.op_attr_mask |= UCP_OP_ATTR_FIELD_MEMH;
        req_param.memh = latest_dest_xgvmi_mh; //PTR_OFFSET(ipass->rkeys, ipass->src_size + ipass->dst_size);
    }

    if (rva != (uint64_t) buffer) {
        ucp_status = ucp_get_nbx(ep, buffer, msglen, rva, *rkey, &req_param);
    } else {
        task->onesided.get_posted++;
        task->onesided.get_completed++;
        return UCC_OK;
    }

    task->onesided.get_posted++;
    if (UCS_OK != ucp_status) {
        if (UCS_PTR_IS_ERR(ucp_status)) {
            return ucs_status_to_ucc_status(UCS_PTR_STATUS(ucp_status));
        }
    } else {
        task->onesided.get_completed++;
    }
    return UCC_OK;
}

#if 0
static inline ucc_status_t ucc_tl_ucp_get_nb(void *buffer, void *target,
                                             size_t             msglen,
                                             ucc_rank_t         dest_group_rank,
                                             ucc_tl_ucp_team_t *team,
                                             ucc_tl_ucp_task_t *task)
{
    ucp_request_param_t req_param = {0};
    int                 segment   = 0;
    ucp_rkey_h          rkey      = NULL;
    uint64_t            rva       = 0;
    ucs_status_ptr_t    ucp_status;
    ucc_status_t        status;
    ucp_ep_h            ep;
    void               *packed_memh = NULL;
    ucc_memory_type_t   mtype;

    status = ucc_tl_ucp_get_ep(team, dest_group_rank, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    status =
        ucc_tl_ucp_resolve_p2p_by_va(team, target, &ep, dest_group_rank, &rva,
                                     &rkey, &packed_memh, &mtype, &segment);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    req_param.op_attr_mask =
        UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
    req_param.cb.send   = ucc_tl_ucp_get_completion_cb;
    req_param.user_data = (void *)task;
    if (packed_memh) {
        req_param.op_attr_mask |= UCP_OP_ATTR_FIELD_MEMH;
        req_param.memh = packed_memh;
    }
    ucp_status = ucp_get_nbx(ep, buffer, msglen, rva, rkey, &req_param);

    task->onesided.get_posted++;
    if (UCS_OK != ucp_status) {
        if (UCS_PTR_IS_ERR(ucp_status)) {
            return ucs_status_to_ucc_status(UCS_PTR_STATUS(ucp_status));
        }
    } else {
        task->onesided.get_completed++;
    }

    return UCC_OK;
}

static inline ucc_status_t ucc_tl_ucp_atomic_inc(size_t target_offset,
                                                 ucc_rank_t dest_group_rank,
                                                 pass_info_t *pass_info,
                                                 ucc_tl_ucp_team_t *team,
                                                 ucc_tl_ucp_task_t *task)
{
    ucp_request_param_t req_param = {0};
    int                 segment   = 0;
    uint64_t            one       = 1;
    ucp_rkey_h          rkey      = NULL;
    uint64_t            rva       = 0;
    ucs_status_ptr_t    ucp_status;
    ucc_status_t        status;
    ucp_ep_h            ep;
    void               *packed_memh = NULL;
    ucc_memory_type_t   mtype;

    status = ucc_tl_ucp_get_ep(team, dest_group_rank, &ep);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    status =
        ucc_tl_ucp_resolve_p2p_by_va(team, target, &ep, dest_group_rank, &rva,
                                     &rkey, &packed_memh, &mtype, &segment);
    if (ucc_unlikely(UCC_OK != status)) {
        return status;
    }

    req_param.op_attr_mask =
        UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA | UCP_OP_ATTR_FIELD_DATATYPE;
    req_param.cb.send   = ucc_tl_ucp_atomic_completion_cb;
    req_param.user_data = (void *)task;
    req_param.datatype     = ucp_dt_make_contig(sizeof(uint64_t));

    ucp_status = ucp_atomic_op_nbx(ep, UCP_ATOMIC_OP_ADD, &one, 1, rva, rkey,
                                   &req_param);

    task->onesided.atomic_posted++;
    if (UCS_OK != ucp_status) {
        if (UCS_PTR_IS_ERR(ucp_status)) {
            return ucs_status_to_ucc_status(UCS_PTR_STATUS(ucp_status));
        }
        ucp_request_free(ucp_status);
    } else {
        task->onesided.atomic_completed++;
    }
    return UCC_OK;
}

#define UCPCHECK_GOTO(_cmd, _task, _label)                                     \
    do {                                                                       \
        ucc_status_t _status = (_cmd);                                         \
        if (UCC_OK != _status) {                                               \
            _task->super.status = _status;                                     \
            goto _label;                                                       \
        }                                                                      \
    } while (0)

#endif

/*
 * coll args should have base vas... rkeys are in global_work_buffer
 */
#if 1


ucc_status_t ucc_tl_ucp_alltoall_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    ucc_memory_type_t  mtype  = TASK_ARGS(task).src.info.mem_type;
    size_t             nelems = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start  = (grank + 1) % gsize;
    pass_info_t *pass_info = (pass_info_t *)TASK_ARGS(task).global_work_buffer;
    ucc_rank_t         peer;
    static ex_info_t *sbuf = NULL, *rbuf = NULL;
    void *dst_key;
    size_t dst_key_len;
    ucs_status_t ucs_status;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    task->barrier.phase = UCC_KN_PHASE_INIT;
    ucc_knomial_pattern_init(gsize, grank,                                      
                             ucc_min(UCC_TL_UCP_TEAM_LIB(team)->                
                                     cfg.barrier_kn_radix, gsize),              
                             &task->barrier.p);

    //printf("[%d] coll args src %p, dst %p, xgvmi flag: %lu, %lu %lu, pass_info->src size: %lu\n", grank, (void *)src, (void *)dest, pass_info->xgvmi_flag, nelems, latest_src_size, pass_info->src_xgvmi_size);
#if 1
    if (pass_info->xgvmi_flag && nelems != latest_src_size) {
        //printf("needing to allgather! src (nelems %lu, latest_src_size %lu)\n", nelems, latest_src_size);
        ucp_mem_map_params_t src_map_params = {
            .field_mask = UCP_MEM_MAP_PARAM_FIELD_EXPORTED_MEMH_BUFFER,
            .exported_memh_buffer = pass_info->rkeys,
        };
        ucp_mem_map_params_t dst_map_params = {
            .field_mask = UCP_MEM_MAP_PARAM_FIELD_EXPORTED_MEMH_BUFFER,
            .exported_memh_buffer = PTR_OFFSET(pass_info->rkeys, pass_info->src_xgvmi_size),
        };

        ucs_status = ucp_mem_map(ctx->worker.ucp_context, &src_map_params, &latest_src_xgvmi_mh);
        if (ucs_status == UCS_ERR_UNREACHABLE) {
            printf("exported memh unsupported\n");
        } else if (ucs_status < UCS_OK) {
            printf("error on ucp_mem_map\n");
        } 
        ucs_status = ucp_mem_map(ctx->worker.ucp_context, &dst_map_params, &latest_dest_xgvmi_mh);

        if (ucs_status == UCS_ERR_UNREACHABLE) {
            printf("exported memh unsupported\n");
        } else if (ucs_status < UCS_OK) {
            printf("error on ucp_mem_map\n");
        }
        ucs_status = ucp_rkey_pack(ctx->worker.ucp_context, latest_dest_xgvmi_mh, &dst_key, &dst_key_len);
        assert(UCS_OK == ucs_status);

        if (!sbuf) {
            sbuf = malloc(sizeof(ex_info_t));
        }
        sbuf->dst = dest;
        sbuf->dst_key_len = dst_key_len;
        memcpy(sbuf->dst_key, dst_key, dst_key_len);

        if (!rbuf) {
            rbuf = calloc(gsize, sizeof(ex_info_t));
        }
        // exchange keys
        for (int i = 0; i < gsize; i++) {
            ucc_tl_ucp_recv_nz(&rbuf[i], sizeof(ex_info_t), UCC_MEMORY_TYPE_HOST, i, team, task);
        }

        for (int i = 0; i < gsize; i++) {
            ucc_tl_ucp_send_nz(sbuf, (sizeof(ex_info_t)), UCC_MEMORY_TYPE_HOST, i, team, task);
        }

        while (task->tagged.recv_completed < gsize || 
               task->tagged.send_completed < gsize) {
            ucp_worker_progress(ctx->worker.ucp_worker);
        }

        for (int i = 0; i < gsize; i++) {
            ex_info_t *p = &rbuf[i];
            latest_dst_addrs[i] = p->dst;
            //printf("latest_dst_addrs[%d]: %lx, key len: %lu\n", i, latest_dst_addrs[i], dst_key_len);
            if (latest_dst_packed_keys[i]) {
                free(latest_dst_packed_keys[i]);
            }
            latest_dst_packed_keys[i] = malloc(dst_key_len);
            memcpy(latest_dst_packed_keys[i], p->dst_key, dst_key_len);
        }
        free(sbuf);
        free(rbuf);
    }
#endif
    /* TODO: change when support for library-based work buffers is complete */
    latest_src_size = nelems;
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest   = dest + grank * nelems;
    if (ucc_likely(pass_info->xgvmi_flag)) {
        for (peer = start; task->onesided.atomic_posted < gsize;) {
            //printf("[%d] peer %d\n", grank, peer);
            /*UCPCHECK_GOTO(*/put_nb(PTR_OFFSET(src, peer * nelems),
                                 latest_dst_addrs[peer], grank * nelems, nelems, peer, latest_dst_packed_keys[peer], mtype, &dst_keys[peer], team, task);/*,
                                  task, out);*/
            peer = (peer + 1) % gsize;
        }
        //printf("put posted: %u\n", task->onesided.atomic_posted);
    } else {
        //printf("perf original");
        for (peer = start; task->onesided.atomic_posted < gsize;) {       
            UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)(src + peer * nelems),
                                        (void *)dest, nelems, peer, team, task),
                          task, out);
        }
    }
    ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->worker.ucp_worker);

    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
out:
    return task->super.status;
}
void ucc_tl_ucp_barrier_knomial_progress(ucc_coll_task_t *task);

void ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
//    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
//    long *             pSync = TASK_ARGS(task).global_work_buffer;

//    printf("progressing\n");
    for (int i = 0; i < 5; i++) {
        if (//(*pSync < gsize) ||
            (task->onesided.atomic_completed < task->onesided.atomic_posted) 
            /*(task->onesided.atomic_completed < (task->onesided.atomic_posted))*/) {
            ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->worker.ucp_worker);
            return;
        } else {
            break;
        }
    }
    ucc_tl_ucp_barrier_knomial_progress(&task->super);
    if (task->super.status == UCC_INPROGRESS) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->worker.ucp_worker);
        return;
    }

//    pSync[0]           = 0;
    task->super.status = UCC_OK;
    //printf("rank %d finished\n", UCC_TL_TEAM_RANK(team));
}

#else
ucc_status_t ucc_tl_ucp_alltoall_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start  = (grank + 1) % gsize;
    long *             pSync  = TASK_ARGS(task).global_work_buffer;
    ucc_rank_t         peer;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* TODO: change when support for library-based work buffers is complete */
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest   = dest + grank * nelems;
    UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)(src + start * nelems),
                                    (void *)dest, nelems, start, team, task),
                  task, out);
    UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, start, team, task), task, out);

    for (peer = (start + 1) % gsize; peer != start; peer = (peer + 1) % gsize) {
        UCPCHECK_GOTO(ucc_tl_ucp_put_nb((void *)(src + peer * nelems),
                                        (void *)dest, nelems, peer, team, task),
                      task, out);
        UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, peer, team, task), task,
                      out);
    }
    ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->worker.ucp_worker);

    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
out:
    return task->super.status;
}

void ucc_tl_ucp_alltoall_onesided_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
//    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    long *             pSync = TASK_ARGS(task).global_work_buffer;

    if (//(*pSync < gsize) ||
        (task->onesided.put_completed < task->onesided.put_posted) ||
        (task->onesided.atomic_completed < (task->onesided.atomic_posted))) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->worker.ucp_worker);
        return;
    }

    pSync[0]           = 0;
    task->super.status = UCC_OK;
}
#endif
#define N_SIZE 4

ucc_status_t ucc_tl_ucp_alltoall_onesided_auto_limit_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);
    ptrdiff_t          src    = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest   = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank  = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize  = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start  = (grank + 1) % gsize;
    int                stride = N_SIZE;//UCC_TL_UCP_TEAM_LIB(team)->cfg.alltoall_limit_ppn;
    int           node_leader = stride * (grank / stride); 
    int                nreqs;
    ucc_rank_t         peer;

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    if (grank & 1) {
        nreqs = stride;
        start = ((node_leader + stride) + (grank - node_leader)) % gsize;
    } else {
        nreqs = 8192 / nelems;
        if (nreqs < 1) {
            nreqs = 1;
        }
    }

    src   = src + grank * nelems;
    for (peer = start; task->onesided.get_posted < gsize;) {
        if ((task->onesided.get_posted - task->onesided.get_completed) < nreqs) {
            ucc_tl_ucp_get_nb((void *)(dest + peer * nelems), (void *)src, nelems,
                              peer, team, task);
            peer = (peer + 1) % gsize;
        } else {
            ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->worker.ucp_worker);
        }
    }
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_alltoall_onesided_get_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task   = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team   = TASK_TEAM(task);

    if (task->onesided.get_posted > task->onesided.get_completed) {
        ucp_worker_progress(UCC_TL_UCP_TEAM_CTX(team)->worker.ucp_worker);
        return;
    }
  
    task->super.status = UCC_OK;
}
