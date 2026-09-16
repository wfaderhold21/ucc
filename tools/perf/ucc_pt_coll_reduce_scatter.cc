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

ucc_pt_coll_reduce_scatter::ucc_pt_coll_reduce_scatter(ucc_datatype_t dt,
                        ucc_memory_type mt, ucc_reduction_op_t op,
                        bool is_inplace, bool is_persistent,
                        ucc_pt_map_type_t map_type,
                        ucc_pt_comm *communicator,
                        ucc_pt_generator_base *generator)
                   : ucc_pt_coll(communicator, generator)
{
    has_inplace_   = true;
    has_reduction_ = true;
    has_range_     = true;
    has_bw_        = true;
    root_shift_    = 0;
    map_type_      = map_type;

    coll_args.mask              = 0;
    coll_args.flags             = 0;
    coll_args.coll_type         = UCC_COLL_TYPE_REDUCE_SCATTER;
    coll_args.op                = op;
    coll_args.src.info.datatype = dt;
    coll_args.src.info.mem_type = mt;
    coll_args.dst.info.datatype = dt;
    coll_args.dst.info.mem_type = mt;

    if (is_inplace) {
        coll_args.mask  = UCC_COLL_ARGS_FIELD_FLAGS;
        coll_args.flags = UCC_COLL_ARGS_FLAG_IN_PLACE;
    }

    if (map_type != UCC_PT_MAP_TYPE_NONE) {
        /* Onesided (full-mesh put to the internal scratch segment): the src
         * is read locally by the puts, the dst is reduced into place by the
         * executor, and the symmetric global work buffer carries the
         * completion signals. Only the MEM_MAPPED_BUFFERS flag and the work
         * buffer are needed beyond the buffers (I2/I3); the src is allocated
         * and registered in init_args(). */
        coll_args.mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
        coll_args.flags |= UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS;
        coll_args.global_work_buffer = comm->get_onesided_buf();
        coll_args.mask |= UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER;
    }

    if (is_persistent) {
        coll_args.mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
        coll_args.flags |= UCC_COLL_ARGS_FLAG_PERSISTENT;
    }
}

ucc_status_t ucc_pt_coll_reduce_scatter::init_args(ucc_pt_test_args_t &test_args)
{
    ucc_coll_args_t &args = test_args.coll_args;
    size_t          dt_size = ucc_dt_size(coll_args.src.info.datatype);
    ucc_status_t st;

    args = coll_args;
    src_header = nullptr;
    dst_header = nullptr;
    if (UCC_IS_INPLACE(args)) {
        args.src.info.count = 0;
        /* In-place dst contains the full source buffer. */
        args.dst.info.count = generator->get_src_count();
    } else {
        args.src.info.count = generator->get_src_count();
        args.dst.info.count = generator->get_dst_count();
    }

    UCCCHECK_GOTO(ucc_pt_alloc(&dst_header,
                               args.dst.info.count * dt_size,
                               args.dst.info.mem_type),
                  exit, st);
    args.dst.info.buffer = dst_header->addr;
    if (args.src.info.count != 0) {
        UCCCHECK_GOTO(ucc_pt_alloc(&src_header,
                                   generator->get_src_count() * dt_size,
                                   args.src.info.mem_type),
                      free_dst, st);
        args.src.info.buffer = src_header->addr;
        if (map_type_ != UCC_PT_MAP_TYPE_NONE) {
            /* Register the src so the one-sided puts read it through UCX's
             * registration cache (the algorithm passes src_memh == NULL and
             * relies on the registered region, I1). */
            ucc_context_h        ctx = comm->get_context();
            ucc_mem_map_t        segments[1];
            ucc_mem_map_params_t mem_map_params;
            size_t               src_memh_size;

            mem_map_params.n_segments = 1;
            mem_map_params.segments   = segments;
            mem_map_params.segments[0].address = args.src.info.buffer;
            mem_map_params.segments[0].len     = args.src.info.count * dt_size;
            UCCCHECK_GOTO(ucc_mem_map(ctx, UCC_MEM_MAP_MODE_EXPORT,
                                      &mem_map_params, &src_memh_size,
                                      &src_memh),
                          free_src, st);
            args.src_memh.local_memh = src_memh;
            args.mask |= UCC_COLL_ARGS_FIELD_MEM_MAP_SRC_MEMH;
        }
    }
    return UCC_OK;
free_src:
    ucc_pt_free(src_header);
    src_header = nullptr;
free_dst:
    ucc_pt_free(dst_header);
    dst_header = nullptr;
exit:
    return st;
}

void ucc_pt_coll_reduce_scatter::free_args(ucc_pt_test_args_t &test_args)
{
    if (map_type_ != UCC_PT_MAP_TYPE_NONE && src_memh) {
        ucc_mem_unmap(&src_memh);
    }
    if (dst_header) {
        ucc_pt_free(dst_header);
        dst_header = nullptr;
    }
    if (src_header) {
        ucc_pt_free(src_header);
        src_header = nullptr;
    }
}

float ucc_pt_coll_reduce_scatter::get_bw(float time_ms, int grsize,
                                         ucc_pt_test_args_t test_args)
{
    ucc_coll_args_t &args  = test_args.coll_args;
    float            N     = grsize;
    size_t           count = UCC_IS_INPLACE(args) ? args.dst.info.count :
                                                   args.src.info.count;
    float S                = count * ucc_dt_size(args.dst.info.datatype);

    return (S / time_ms) * ((N - 1) / N) / 1000.0;
}
