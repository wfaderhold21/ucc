/**
 * Copyright (c) 2021-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ucc_pt_coll.h"
#include "ucc_perftest.h"
#include <ucc/api/ucc.h>
#include <utils/ucc_math.h>
#include <utils/ucc_coll_utils.h>

/*
 * One-sided bcast runs with a global (mem-mapped) mapping so the root can
 * issue RMA puts into every peer's in-place buffer and the symmetric global
 * work buffer carries the completion signals. Bcast has a single in-place
 * buffer (src == dst); the root's buffer holds the data and every rank's buffer
 * is an RMA target. Every rank registers its buffer as a symmetric global mem
 * handle and the handles are broadcast + imported across ranks exactly like
 * alltoallv's global mapping -- the exchange is a uniform all-ranks collective
 * (the perftest comm's allreduce/bcast are real collectives), so the memh
 * exchange must stay symmetric between the root and the other ranks. All
 * buffers and mem handles are allocated once in the constructor and released in
 * the destructor (free_args stays a no-op for the global map type), so
 * init_args only sets the per-iteration count. Selection of the onesided
 * algorithm is opt-in via UCC_TL_UCP_TUNE (e.g. "bcast:0-inf:@onesided");
 * without it the two-sided knomial runs (map type NONE).
 */
ucc_pt_coll_bcast::ucc_pt_coll_bcast(ucc_datatype_t dt, ucc_memory_type mt,
                                     int root_shift, bool is_persistent,
                                     ucc_pt_map_type_t map_type,
                                     ucc_pt_comm *communicator,
                                     ucc_pt_generator_base *generator)
                   : ucc_pt_coll(communicator, generator)
{
    size_t         count_max = generator->get_src_count_max();
    ucc_status_t   st;

    has_inplace_   = false;
    has_reduction_ = false;
    has_range_     = true;
    has_bw_        = true;
    root_shift_    = root_shift;
    map_type_      = map_type;

    /* Zero-init the coll args: the memh exchange below runs before
     * init_args (which sets the real root), so root must be a valid rank
     * here (0, matching the sel/default) for the per-rank decisions to line
     * up. Without this the uninitialized root mis-routes the global_memh
     * assignment and the root's RMA target is unresolvable. */
    memset(&coll_args, 0, sizeof(coll_args));
    coll_args.mask              = 0;
    coll_args.flags             = 0;
    coll_args.coll_type         = UCC_COLL_TYPE_BCAST;
    coll_args.src.info.datatype = dt;
    coll_args.src.info.mem_type = mt;

    if (is_persistent) {
        coll_args.mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
        coll_args.flags |= UCC_COLL_ARGS_FLAG_PERSISTENT;
    }

    if (map_type == UCC_PT_MAP_TYPE_GLOBAL) {
        /*
         * Onesided (root-driven put): every rank provides its single in-place
         * buffer (count_max) and registers it as a symmetric global mem handle
         * (I1): the root's put at its local buffer offset 0 resolves to the
         * peer's buffer at the same offset. The root's buffer holds the data;
         * every other rank's buffer is an RMA target. The symmetric work
         * buffer carries the completion signals.
         */
        UCCCHECK_GOTO(ucc_pt_alloc(&src_header,
                                   count_max * ucc_dt_size(dt), mt),
                      exit, st);
        coll_args.src.info.buffer = src_header->addr;

        {
            ucc_context_h        ctx             = comm->get_context();
            ucc_mem_map_t        segments[1];
            ucc_mem_map_params_t mem_map_params;
            uint64_t             src_memh_size, src_memh_size_max;

            coll_args.mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
            coll_args.flags |= UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS;
            mem_map_params.n_segments = 1;
            mem_map_params.segments   = segments;

            /*
             * bcast's single in-place buffer is BOTH the root's put source and
             * every peer's RMA target. Only the TARGET needs a global memh:
             * dst_memh.global_memh carries every rank's symmetric buffer so
             * the root can resolve each peer's in-place destination (I1).
             * The SOURCE is the root's own local buffer -- read directly by
             * UCX, no memh needed (src_memh stays NULL, exactly as in scatter,
             * whose src_memh guard never fires in the GLOBAL path). Every rank
             * exports its buffer, keeping the exchange a uniform all-ranks
             * collective (the comm's allreduce/bcast are real collectives).
             */
            mem_map_params.segments[0].address = src_header->addr;
            mem_map_params.segments[0].len     = count_max * ucc_dt_size(dt);
            UCCCHECK_GOTO(ucc_mem_map(ctx, UCC_MEM_MAP_MODE_EXPORT,
                                      &mem_map_params, &src_memh_size,
                                      &src_memh),
                          free_src, st);
            comm->allreduce(&src_memh_size, &src_memh_size_max, 1, UCC_OP_MAX,
                            UCC_DT_UINT64);
            src_memh_global = new ucc_mem_map_mem_h[comm->get_size()];
            for (int i = 0; i < comm->get_size(); i++) {
                src_memh_global[i] = new char[src_memh_size_max];
                if (i == comm->get_rank()) {
                    memcpy(src_memh_global[i], src_memh, src_memh_size);
                }
                comm->bcast(src_memh_global[i], src_memh_size_max, i);
            }
            for (int i = 0; i < comm->get_size(); i++) {
                ucc_mem_map(ctx, UCC_MEM_MAP_MODE_IMPORT, &mem_map_params,
                            &src_memh_size_max, &src_memh_global[i]);
            }
            coll_args.dst_memh.global_memh = src_memh_global;
            coll_args.mask  |= UCC_COLL_ARGS_FIELD_MEM_MAP_DST_MEMH;
            coll_args.flags |= UCC_COLL_ARGS_FLAG_DST_MEMH_GLOBAL;
        }

        /* Onesided needs the symmetric global work buffer. */
        coll_args.global_work_buffer = comm->get_onesided_buf();
        coll_args.mask |= UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER;
        return;
    }
    return;
free_src:
    ucc_pt_free(src_header);
    src_header = NULL;
exit:
    throw std::runtime_error("failed to initialize bcast arguments");
}

ucc_status_t ucc_pt_coll_bcast::init_args(ucc_pt_test_args_t &test_args)
{
    ucc_coll_args_t &args    = test_args.coll_args;
    size_t           dt_size = ucc_dt_size(coll_args.src.info.datatype);
    ucc_status_t     st;

    coll_args.root      = test_args.coll_args.root;
    args                = coll_args;
    args.src.info.count = generator->get_src_count();
    if (map_type_ == UCC_PT_MAP_TYPE_GLOBAL) {
        /* Buffer and global mem handle are pre-allocated in the constructor;
         * only the per-iteration count changes. */
        return UCC_OK;
    }
    st = UCC_OK;
    UCCCHECK_GOTO(ucc_pt_alloc(&src_header,
                               generator->get_src_count() * dt_size,
                               args.src.info.mem_type),
                  exit, st);
    args.src.info.buffer = src_header->addr;
exit:
    return st;
}

void ucc_pt_coll_bcast::free_args(ucc_pt_test_args_t &test_args)
{
    if (map_type_ == UCC_PT_MAP_TYPE_GLOBAL) {
        return; /* buffer + global mem handle are ctor-allocated, freed in dtor */
    }
    ucc_pt_free(src_header);
}

float ucc_pt_coll_bcast::get_bw(float time_ms, int grsize,
                                ucc_pt_test_args_t test_args)
{
    ucc_coll_args_t &args = test_args.coll_args;
    float            S    = args.src.info.count *
                            ucc_dt_size(args.src.info.datatype);
    float            N    = grsize - 1;

    return (S * N) / time_ms / 1000.0;
}

ucc_pt_coll_bcast::~ucc_pt_coll_bcast()
{
    if (src_memh) {
        ucc_mem_unmap(&src_memh);
    }
    if (src_memh_global) {
        for (int i = 0; i < comm->get_size(); i++) {
            if (src_memh_global[i]) {
                ucc_mem_unmap(&src_memh_global[i]);
                delete[] static_cast<char *>(src_memh_global[i]);
            }
        }
        delete[] src_memh_global;
    }
    if (src_header) {
        ucc_pt_free(src_header);
    }
}
