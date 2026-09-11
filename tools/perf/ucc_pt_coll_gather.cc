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
 * One-sided gather runs with a global (mem-mapped) dst handle so every rank
 * can address the root's full-size dst at the same symmetric offset (I1).
 * Unlike the two-sided knomial (dst allocated on the root only), the onesided
 * path allocates the full dst (single_rank_count * size) and a per-rank src,
 * then exports a dst memh that is broadcast + imported across ranks exactly
 * like alltoallv's global mapping. All buffers and mem handles are allocated
 * once in the constructor and released in the destructor (free_args stays the
 * base no-op), so init_args only updates the per-iteration count. The
 * symmetric global work buffer is supplied from the perftest onesided
 * segment. Selection of the onesided algorithm is opt-in via UCC_TL_UCP_TUNE
 * (e.g. "gather:0-inf:@onesided"); without it the two-sided knomial runs
 * (map_type NONE).
 */
ucc_pt_coll_gather::ucc_pt_coll_gather(ucc_datatype_t dt, ucc_memory_type mt,
                       bool is_inplace, bool is_persistent, int root_shift,
                       ucc_pt_map_type_t map_type, ucc_pt_comm *communicator,
                       ucc_pt_generator_base *generator):
                       ucc_pt_coll(communicator, generator)
{
    size_t src_count_max = generator->get_src_count_max();
    size_t nprocs        = comm->get_size();
    bool   is_root       = (comm->get_rank() == coll_args.root);
    ucc_status_t st;

    has_inplace_   = true;
    has_reduction_ = false;
    has_range_     = true;
    has_bw_        = true;
    root_shift_    = root_shift;

    coll_args.mask              = 0;
    coll_args.flags             = 0;
    coll_args.coll_type         = UCC_COLL_TYPE_GATHER;
    coll_args.src.info.datatype = dt;
    coll_args.src.info.mem_type = mt;
    coll_args.dst.info.datatype = dt;
    coll_args.dst.info.mem_type = mt;

    if (is_inplace) {
        coll_args.mask  = UCC_COLL_ARGS_FIELD_FLAGS;
        coll_args.flags = UCC_COLL_ARGS_FLAG_IN_PLACE;
    }

    if (is_persistent) {
        coll_args.mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
        coll_args.flags |= UCC_COLL_ARGS_FLAG_PERSISTENT;
    }

    if (map_type == UCC_PT_MAP_TYPE_GLOBAL) {
        /*
         * Onesided (leaf-driven put): the root's full dst is the RMA target,
         * addressed through the global dst memh (I1). Every rank allocates
         * the full dst and a per-rank src, exports a dst memh, and broadcasts
         * + imports it on all ranks.
         */
        UCCCHECK_GOTO(ucc_pt_alloc(&dst_header,
                                   src_count_max * nprocs * ucc_dt_size(dt),
                                   mt),
                      exit, st);
        coll_args.dst.info.buffer = dst_header->addr;

        UCCCHECK_GOTO(ucc_pt_alloc(&src_header,
                                   src_count_max * ucc_dt_size(dt), mt),
                      free_dst, st);
        coll_args.src.info.buffer = src_header->addr;

        {
            ucc_context_h        ctx = comm->get_context();
            ucc_mem_map_t        segments[1];
            ucc_mem_map_params_t mem_map_params;
            uint64_t             dst_memh_size, src_memh_size;
            uint64_t             dst_memh_size_max;

            coll_args.mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
            coll_args.flags |= UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS;
            mem_map_params.n_segments = 1;
            mem_map_params.segments   = segments;

            /* Global dst memh: full-size dst, symmetric across ranks. */
            mem_map_params.segments[0].address = dst_header->addr;
            mem_map_params.segments[0].len     =
                src_count_max * nprocs * ucc_dt_size(dt);
            UCCCHECK_GOTO(ucc_mem_map(ctx, UCC_MEM_MAP_MODE_EXPORT,
                                      &mem_map_params, &dst_memh_size,
                                      &dst_memh),
                          free_src, st);
            comm->allreduce(&dst_memh_size, &dst_memh_size_max, 1, UCC_OP_MAX,
                            UCC_DT_UINT64);
            dst_memh_global = new ucc_mem_map_mem_h[comm->get_size()];
            for (int i = 0; i < comm->get_size(); i++) {
                dst_memh_global[i] = new char[dst_memh_size_max];
                if (i == comm->get_rank()) {
                    memcpy(dst_memh_global[i], dst_memh, dst_memh_size);
                }
                comm->bcast(dst_memh_global[i], dst_memh_size_max, i);
            }
            for (int i = 0; i < comm->get_size(); i++) {
                ucc_mem_map(ctx, UCC_MEM_MAP_MODE_IMPORT, &mem_map_params,
                            &dst_memh_size_max, &dst_memh_global[i]);
            }
            coll_args.dst_memh.global_memh = dst_memh_global;
            coll_args.mask |= UCC_COLL_ARGS_FIELD_MEM_MAP_DST_MEMH;
            coll_args.flags |= UCC_COLL_ARGS_FLAG_DST_MEMH_GLOBAL;

            /* Local src memh: each rank reads its own src for the put. */
            mem_map_params.segments[0].address = src_header->addr;
            mem_map_params.segments[0].len     =
                src_count_max * ucc_dt_size(dt);
            UCCCHECK_GOTO(ucc_mem_map(ctx, UCC_MEM_MAP_MODE_EXPORT,
                                      &mem_map_params, &src_memh_size,
                                      &src_memh),
                          free_src, st);
            coll_args.src_memh.local_memh = src_memh;
            coll_args.mask |= UCC_COLL_ARGS_FIELD_MEM_MAP_SRC_MEMH;
        }

        /* Onesided needs the symmetric global work buffer. */
        coll_args.global_work_buffer = comm->get_onesided_buf();
        coll_args.mask |= UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER;
        return;
    }

    /* Two-sided knomial: dst allocated on the root only. */
    if (is_root || root_shift_) {
        UCCCHECK_GOTO(ucc_pt_alloc(&dst_header,
                                   src_count_max * nprocs * ucc_dt_size(dt),
                                   mt),
                      exit, st);
        coll_args.dst.info.buffer = dst_header->addr;
    }
    if (!is_root || !is_inplace || root_shift_) {
        UCCCHECK_GOTO(ucc_pt_alloc(&src_header,
                                   src_count_max * ucc_dt_size(dt), mt),
                      free_dst, st);
        coll_args.src.info.buffer = src_header->addr;
    }
    return;
free_src:
    ucc_pt_free(src_header);
    src_header = NULL;
free_dst:
    ucc_pt_free(dst_header);
    dst_header = NULL;
exit:
    throw std::runtime_error("failed to initialize gather arguments");
}

ucc_status_t ucc_pt_coll_gather::init_args(ucc_pt_test_args_t &test_args)
{
    ucc_coll_args_t &args  = test_args.coll_args;
    size_t           count = generator->get_src_count();

    coll_args.root      = test_args.coll_args.root;
    args                = coll_args;
    args.dst.info.count = count * comm->get_size();
    if (!UCC_IS_INPLACE(args)) {
        args.src.info.count = count;
    }
    return UCC_OK;
}

float ucc_pt_coll_gather::get_bw(float time_ms, int grsize,
                                 ucc_pt_test_args_t test_args)
{
    ucc_coll_args_t &args = test_args.coll_args;
    float            N    = grsize - 1;
    float            S    = args.src.info.count *
                            ucc_dt_size(args.src.info.datatype);

    return (S * N) / time_ms / 1000.0;
}

ucc_pt_coll_gather::~ucc_pt_coll_gather()
{
    if (src_memh) {
        ucc_mem_unmap(&src_memh);
    }
    if (dst_memh) {
        ucc_mem_unmap(&dst_memh);
    }
    if (dst_memh_global) {
        for (int i = 0; i < comm->get_size(); i++) {
            if (dst_memh_global[i]) {
                ucc_mem_unmap(&dst_memh_global[i]);
                delete[] static_cast<char *>(dst_memh_global[i]);
            }
        }
        delete[] dst_memh_global;
    }
    if (src_header) {
        ucc_pt_free(src_header);
    }
    if (dst_header) {
        ucc_pt_free(dst_header);
    }
}
