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
 * One-sided barrier is a no-data dissemination rendezvous: over
 * ceil(log2(size)) rounds each rank atomic-adds into its partner's copy of
 * slot (3+r) in the symmetric work buffer, then waits for its own copy to
 * receive the +1. No inplace, no reduction, no message range, no bandwidth
 * metric. The only onesided requirement is the symmetric global work buffer,
 * supplied from the perftest onesided segment (a symmetric context segment),
 * so the atomic signal path is exercisable here. Selection of the onesided
 * algorithm is opt-in via UCC_TL_UCP_TUNE (e.g. "barrier:0-inf:@onesided");
 * without it the two-sided knomial runs (which ignores the work buffer).
 */
ucc_pt_coll_barrier::ucc_pt_coll_barrier(ucc_pt_comm *communicator,
                                         ucc_pt_generator_base *generator) :
                                          ucc_pt_coll(communicator, generator)
{
    has_inplace_   = false;
    has_reduction_ = false;
    has_range_     = false;
    has_bw_        = false;
    root_shift_    = 0;

    coll_args                    = {};
    coll_args.coll_type          = UCC_COLL_TYPE_BARRIER;
    coll_args.global_work_buffer = comm->get_onesided_buf();
    coll_args.mask               = UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER;
}

ucc_status_t ucc_pt_coll_barrier::init_args(ucc_pt_test_args_t &test_args)
{
    ucc_coll_args_t &args = test_args.coll_args;

    args = coll_args;
    return UCC_OK;
}

void ucc_pt_coll_barrier::free_args(ucc_pt_test_args_t &test_args)
{
    return;
}
