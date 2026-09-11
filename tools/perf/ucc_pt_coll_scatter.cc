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
 * One-sided scatter runs with a global (mem-mapped) mapping so the root can
 * issue RMA puts into every peer's dst and the symmetric global work buffer
 * carries the completion signals. The root provides the full src (count_max *
 * size) and every rank provides its dst block (count_max); both are registered
 * as symmetric global mem handles and broadcast + imported across ranks exactly
 * like alltoallv's global mapping. All buffers and mem handles are allocated
 * once in the constructor and released in the destructor (free_args stays a
 * no-op for the global map type), so init_args only sets the per-iteration
 * counts. Selection of the onesided algorithm is opt-in via UCC_TL_UCP_TUNE
 * (e.g. "scatter:0-inf:@onesided"); without it the two-sided knomial runs
 * (map type NONE).
 */
ucc_pt_coll_scatter::ucc_pt_coll_scatter(ucc_datatype_t dt,
                         ucc_memory_type mt, bool is_inplace,
                         bool is_persistent, int root_shift,
                         ucc_pt_map_type_t map_type,
                         ucc_pt_comm *communicator,
                         ucc_pt_generator_base *generator)
                   : ucc_pt_coll(communicator, generator)
{
    size_t src_count_max = generator->get_src_count_max();
    size_t dst_count_max = generator->get_dst_count_max();
    ucc_status_t st;

    has_inplace_   = true;
    has_reduction_ = false;
    has_range_     = true;
    has_bw_        = true;
    root_shift_    = root_shift;
    map_type_      = map_type;

    coll_args.mask              = 0;
    coll_args.flags             = 0;
    coll_args.coll_type         = UCC_COLL_TYPE_SCATTER;
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
         * Onesided (root-driven put): the root provides the full src
         * (src_count_max = count_max * size, in peer order) and every rank
         * provides its dst block (dst_count_max = count_max). Both are
         * registered as symmetric global mem handles (I1): the root's put at
         * its local dst offset 0 resolves to the peer's dst at the same
         * offset. The symmetric work buffer carries the completion signals.
         *
         * The memh exchange is a uniform all-ranks collective (the
         * perftest comm's allreduce/bcast are real collectives), so every
         * rank exports a src handle even though only the root holds a real
         * src buffer: non-roots export their dst as a placeholder and never
         * touch the src handle (only the root issues RMA, and it reads its
         * own src directly). Keeping the exchange symmetric avoids a
         * collective mismatch between the root and the other ranks.
         */
        UCCCHECK_GOTO(ucc_pt_alloc(&dst_header,
                                   dst_count_max * ucc_dt_size(dt), mt),
                      exit, st);
        coll_args.dst.info.buffer = dst_header->addr;

        if (comm->get_rank() == coll_args.root) {
            UCCCHECK_GOTO(ucc_pt_alloc(&src_header,
                                       src_count_max * ucc_dt_size(dt), mt),
                          free_dst, st);
            coll_args.src.info.buffer = src_header->addr;
        }

        {
            ucc_context_h        ctx            = comm->get_context();
            ucc_mem_map_t        segments[1];
            ucc_mem_map_params_t mem_map_params;
            uint64_t             dst_memh_size, src_memh_size;
            uint64_t             dst_memh_size_max, src_memh_size_max;

            coll_args.mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
            coll_args.flags |= UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS;
            mem_map_params.n_segments = 1;
            mem_map_params.segments   = segments;

            /* Global dst memh: per-rank block, symmetric across ranks. */
            mem_map_params.segments[0].address = dst_header->addr;
            mem_map_params.segments[0].len     = dst_count_max * ucc_dt_size(dt);
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

            /* Global src memh: root exports its real src; non-roots export
             * their dst as a placeholder (uniform collective, see above).
             * Only the root's own entry is ever dereferenced. */
            mem_map_params.segments[0].address =
                (comm->get_rank() == coll_args.root) ? src_header->addr
                                                     : dst_header->addr;
            mem_map_params.segments[0].len =
                (comm->get_rank() == coll_args.root) ?
                    src_count_max * ucc_dt_size(dt) :
                    dst_count_max * ucc_dt_size(dt);
            UCCCHECK_GOTO(ucc_mem_map(ctx, UCC_MEM_MAP_MODE_EXPORT,
                                      &mem_map_params, &src_memh_size,
                                      &src_memh),
                          free_dst, st);
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
            if (comm->get_rank() == coll_args.root) {
                coll_args.src_memh.global_memh = src_memh_global;
                coll_args.mask |= UCC_COLL_ARGS_FIELD_MEM_MAP_SRC_MEMH;
                coll_args.flags |= UCC_COLL_ARGS_FLAG_SRC_MEMH_GLOBAL;
            }
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
free_dst:
    ucc_pt_free(dst_header);
    dst_header = NULL;
exit:
    throw std::runtime_error("failed to initialize scatter arguments");
}

ucc_status_t ucc_pt_coll_scatter::init_args(ucc_pt_test_args_t &test_args)
{
    ucc_coll_args_t &args   = test_args.coll_args;
    size_t           dt_size = ucc_dt_size(coll_args.dst.info.datatype);
    ucc_status_t st;
    bool         is_root;

    coll_args.root = test_args.coll_args.root;
    args           = coll_args;
    is_root        = (comm->get_rank() == args.root);

    if (map_type_ == UCC_PT_MAP_TYPE_GLOBAL) {
        /* Buffers and global mem handles are pre-allocated in the constructor;
         * only the per-iteration counts change. */
        args.dst.info.count = generator->get_dst_count();
        if (is_root) {
            args.src.info.count = generator->get_src_count();
        }
        return UCC_OK;
    }

    st = UCC_OK;
    args.dst.info.count = generator->get_dst_count();
    if (is_root || root_shift_) {
        args.src.info.count = generator->get_src_count();
        UCCCHECK_GOTO(
            ucc_pt_alloc(&src_header,
                         generator->get_src_count() * dt_size,
                         args.src.info.mem_type),
            exit, st);
        args.src.info.buffer = src_header->addr;
    }
    if (!is_root || !UCC_IS_INPLACE(args) || root_shift_) {
        UCCCHECK_GOTO(ucc_pt_alloc(&dst_header,
                                   generator->get_dst_count() * dt_size,
                                   args.dst.info.mem_type),
                      free_src, st);
        args.dst.info.buffer = dst_header->addr;
    }
    return UCC_OK;
free_src:
    if ((is_root || root_shift_) && st == UCC_OK) {
        ucc_pt_free(src_header);
    }
    return st;
exit:
    return st;
}

float ucc_pt_coll_scatter::get_bw(float time_ms, int grsize,
                                  ucc_pt_test_args_t test_args)
{
    ucc_coll_args_t &args = test_args.coll_args;
    float            S    = args.dst.info.count * ucc_dt_size(args.dst.info.datatype);
    float            N    = grsize - 1;

    return (S * N) / time_ms / 1000.0;
}

void ucc_pt_coll_scatter::free_args(ucc_pt_test_args_t &test_args)
{
    ucc_coll_args_t &args    = test_args.coll_args;
    bool             is_root = (comm->get_rank() == args.root);

    if (map_type_ == UCC_PT_MAP_TYPE_GLOBAL) {
        return; /* buffers + global mem handles are ctor-allocated, freed in dtor */
    }
    if (!is_root || !UCC_IS_INPLACE(args) || root_shift_) {
        ucc_pt_free(dst_header);
    }
    if (is_root || root_shift_) {
        ucc_pt_free(src_header);
    }
}

ucc_pt_coll_scatter::~ucc_pt_coll_scatter()
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
    if (dst_header) {
        ucc_pt_free(dst_header);
    }
}
