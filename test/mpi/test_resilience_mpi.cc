/**
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 *
 * Standalone MPI test binary for Phase 1 resilience API.
 *
 * Tests:
 *  1. guard_violations      — single-rank exclusive context; state-machine guards.
 *  2. abort_recover_no_fail — all ranks abort+recover; expect 0 failed ranks.
 *  3. post_after_abort      — collective_post while ABORTING → UCC_ERR_ABORTED.
 *  4. simulated_failure     — rank 0 marks rank (size-1); BOR agreement verified.
 *
 * Each test function creates its own UCC context and team (if needed), runs
 * the scenario, and returns 0 on pass or 1 on fail.  The final exit code is
 * the OR of all test results so that CI can detect any failure.
 */
#include <mpi.h>
#include <iostream>
#include <string>
#include <ucc/api/ucc.h>

extern "C" {
#include "core/ucc_context.h"
}

/* -------------------------------------------------------------------------- */
/* Helpers                                                                     */
/* -------------------------------------------------------------------------- */

#define RES_CHECK(_call, _label)                                               \
    do {                                                                       \
        ucc_status_t _st = (_call);                                            \
        if (UCC_OK != _st) {                                                   \
            std::cerr << "[rank " << world_rank << "] FAIL " << _label        \
                      << ": " << #_call << " returned "                        \
                      << ucc_status_string(_st) << "\n";                       \
            return 1;                                                          \
        }                                                                      \
    } while (0)

#define RES_EXPECT(_expr, _label)                                              \
    do {                                                                       \
        if (!(_expr)) {                                                        \
            std::cerr << "[rank " << world_rank << "] FAIL " << _label        \
                      << ": assertion failed: " #_expr "\n";                  \
            return 1;                                                          \
        }                                                                      \
    } while (0)

static int world_rank;
static int world_size;

/* OOB allgather callbacks (backed by MPI) used for UCC context and team
   creation. */
static ucc_status_t oob_allgather(void *sbuf, void *rbuf, size_t msglen,
                                  void *coll_info, void **req)
{
    MPI_Comm    comm    = (MPI_Comm)(uintptr_t)coll_info;
    MPI_Request mpi_req;
    MPI_Iallgather(sbuf, msglen, MPI_BYTE, rbuf, msglen, MPI_BYTE, comm,
                   &mpi_req);
    *req = (void *)(uintptr_t)mpi_req;
    return UCC_OK;
}

static ucc_status_t oob_allgather_test(void *req)
{
    MPI_Request mpi_req = (MPI_Request)(uintptr_t)req;
    int         done;
    MPI_Test(&mpi_req, &done, MPI_STATUS_IGNORE);
    return done ? UCC_OK : UCC_INPROGRESS;
}

static ucc_status_t oob_allgather_free(void */*req*/)
{
    return UCC_OK;
}

/* Create a UCC global context backed by the given MPI communicator. */
static ucc_context_h create_global_ctx(ucc_lib_h lib, MPI_Comm comm)
{
    ucc_context_config_h  cfg;
    ucc_context_params_t  params = {};
    ucc_context_h         ctx    = nullptr;
    int                   rank, size;

    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    if (UCC_OK != ucc_context_config_read(lib, nullptr, &cfg)) return nullptr;

    params.mask                = UCC_CONTEXT_PARAM_FIELD_OOB;
    params.oob.allgather       = oob_allgather;
    params.oob.req_test        = oob_allgather_test;
    params.oob.req_free        = oob_allgather_free;
    params.oob.coll_info       = (void *)(uintptr_t)comm;
    params.oob.n_oob_eps       = size;
    params.oob.oob_ep          = rank;

    ucc_status_t st = ucc_context_create(lib, &params, cfg, &ctx);
    ucc_context_config_release(cfg);
    return (UCC_OK == st) ? ctx : nullptr;
}

/* Create a UCC team spanning the given MPI communicator. */
static ucc_team_h create_team(ucc_context_h ctx, MPI_Comm comm)
{
    ucc_team_params_t params;
    ucc_team_h        team;
    ucc_status_t      st;
    int               rank, size;

    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    params.mask               = UCC_TEAM_PARAM_FIELD_EP       |
                                UCC_TEAM_PARAM_FIELD_EP_RANGE |
                                UCC_TEAM_PARAM_FIELD_OOB;
    params.ep                 = rank;
    params.ep_range           = UCC_COLLECTIVE_EP_RANGE_CONTIG;
    params.oob.allgather      = oob_allgather;
    params.oob.req_test       = oob_allgather_test;
    params.oob.req_free       = oob_allgather_free;
    params.oob.coll_info      = (void *)(uintptr_t)comm;
    params.oob.n_oob_eps      = size;
    params.oob.oob_ep         = rank;

    st = ucc_team_create_post(&ctx, 1, &params, &team);
    if (UCC_OK != st) return nullptr;

    while (UCC_INPROGRESS == (st = ucc_team_create_test(team))) {
        ucc_context_progress(ctx);
    }
    return (UCC_OK == st) ? team : nullptr;
}

/* Destroy a UCC team (polls until done). */
static void destroy_team(ucc_team_h team, ucc_context_h ctx)
{
    ucc_status_t st;
    while (UCC_INPROGRESS == (st = ucc_team_destroy(team))) {
        ucc_context_progress(ctx);
    }
}

/* Poll abort_test for this rank's context until it converges. */
static ucc_status_t poll_abort_test(ucc_context_h ctx)
{
    ucc_status_t st;
    do {
        ucc_context_progress(ctx);
        st = ucc_context_abort_test(ctx);
    } while (st == UCC_INPROGRESS);
    return st;
}

/* -------------------------------------------------------------------------- */
/* Test 1: guard violations on a single-rank exclusive context.               */
/* -------------------------------------------------------------------------- */
static int test_guard_violations(ucc_lib_h lib)
{
    const char *label = "guard_violations";

    ucc_context_config_h  cfg;
    ucc_context_h         ctx = nullptr;
    ucc_context_params_t  params = {};

    RES_CHECK(ucc_context_config_read(lib, nullptr, &cfg), label);
    params.mask = UCC_CONTEXT_PARAM_FIELD_TYPE;
    params.type = UCC_CONTEXT_EXCLUSIVE;
    ucc_status_t st = ucc_context_create(lib, &params, cfg, &ctx);
    ucc_context_config_release(cfg);
    RES_CHECK(st, label);

    /* abort_test before abort */
    RES_EXPECT(ucc_context_abort_test(ctx) == UCC_ERR_INVALID_STATE, label);
    /* recover before abort */
    RES_EXPECT(ucc_context_recover(ctx) == UCC_ERR_INVALID_STATE, label);
    /* shrink before recover */
    RES_EXPECT(ucc_context_shrink(ctx, nullptr, nullptr, nullptr) ==
                   UCC_ERR_INVALID_STATE,
               label);
    /* abort with no service team (exclusive context, no OOB) */
    RES_EXPECT(ucc_context_abort(ctx) == UCC_ERR_INVALID_STATE, label);

    ucc_context_destroy(ctx);

    if (world_rank == 0) {
        std::cout << "[PASS] " << label << "\n";
    }
    return 0;
}

/* -------------------------------------------------------------------------- */
/* Test 2: abort + recover with no injected failure.                          */
/* -------------------------------------------------------------------------- */
static int test_abort_recover_no_fail(ucc_lib_h lib, MPI_Comm comm)
{
    const char *label = "abort_recover_no_fail";

    ucc_context_h ctx = create_global_ctx(lib, comm);
    RES_EXPECT(ctx != nullptr, label);

    RES_CHECK(ucc_context_abort(ctx), label);
    RES_CHECK(poll_abort_test(ctx), label);
    RES_CHECK(ucc_context_recover(ctx), label);

    ucc_context_attr_t attr;
    attr.mask = UCC_CONTEXT_ATTR_FIELD_FAILED_RANKS;
    RES_CHECK(ucc_context_get_attr(ctx, &attr), label);
    RES_EXPECT(attr.n_failed_ranks == 0, label);

    ucc_context_destroy(ctx);

    /* Aggregate pass/fail across ranks so all see the same result. */
    MPI_Barrier(comm);
    if (world_rank == 0) {
        std::cout << "[PASS] " << label << "\n";
    }
    return 0;
}

/* -------------------------------------------------------------------------- */
/* Test 3: collective_post after abort returns UCC_ERR_ABORTED.               */
/* -------------------------------------------------------------------------- */
static int test_post_after_abort(ucc_lib_h lib, MPI_Comm comm)
{
    const char *label = "post_after_abort";

    ucc_context_h ctx = create_global_ctx(lib, comm);
    RES_EXPECT(ctx != nullptr, label);

    ucc_team_h team = create_team(ctx, comm);
    RES_EXPECT(team != nullptr, label);

    /* Init a barrier — init is state-independent. */
    ucc_coll_args_t args = {};
    args.coll_type       = UCC_COLL_TYPE_BARRIER;
    ucc_coll_req_h req;
    RES_CHECK(ucc_collective_init(&args, &req, team), label);

    /* Abort transitions ctx to ABORTING (drains queue; nothing was posted). */
    RES_CHECK(ucc_context_abort(ctx), label);

    /* Post while ABORTING: must return UCC_ERR_ABORTED. */
    ucc_status_t post_st = ucc_collective_post(req);
    RES_EXPECT(post_st == UCC_ERR_ABORTED, label);

    /* Clean up the never-posted request. */
    ucc_collective_finalize(req);

    RES_CHECK(poll_abort_test(ctx), label);
    RES_CHECK(ucc_context_recover(ctx), label);

    destroy_team(team, ctx);
    ucc_context_destroy(ctx);

    MPI_Barrier(comm);
    if (world_rank == 0) {
        std::cout << "[PASS] " << label << "\n";
    }
    return 0;
}

/* -------------------------------------------------------------------------- */
/* Test 4: BOR agreement on simulated failure.                                */
/* Rank 0 marks rank (size-1) as failed.  After abort+recover every rank     */
/* must see exactly that one rank in the agreed failed set.                   */
/* -------------------------------------------------------------------------- */
static int test_simulated_failure(ucc_lib_h lib, MPI_Comm comm)
{
    const char *label = "simulated_failure";

    if (world_size < 2) {
        if (world_rank == 0) {
            std::cout << "[SKIP] " << label
                      << " (requires at least 2 ranks)\n";
        }
        return 0;
    }

    ucc_context_h ctx = create_global_ctx(lib, comm);
    RES_EXPECT(ctx != nullptr, label);

    /* Rank 0 observes that rank (size-1) is unreachable. */
    if (world_rank == 0) {
        ucc_context_mark_rank_failed(
            reinterpret_cast<ucc_context_t *>(ctx),
            static_cast<ucc_rank_t>(world_size - 1));
    }

    RES_CHECK(ucc_context_abort(ctx), label);
    RES_CHECK(poll_abort_test(ctx), label);
    RES_CHECK(ucc_context_recover(ctx), label);

    ucc_context_attr_t attr;
    attr.mask = UCC_CONTEXT_ATTR_FIELD_FAILED_RANKS;
    RES_CHECK(ucc_context_get_attr(ctx, &attr), label);

    RES_EXPECT(attr.n_failed_ranks == 1, label);
    RES_EXPECT(attr.failed_ranks[0] ==
                   static_cast<ucc_rank_t>(world_size - 1),
               label);

    ucc_context_destroy(ctx);

    MPI_Barrier(comm);
    if (world_rank == 0) {
        std::cout << "[PASS] " << label << "\n";
    }
    return 0;
}

/* -------------------------------------------------------------------------- */
/* main                                                                        */
/* -------------------------------------------------------------------------- */
int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    ucc_lib_config_h lib_config;
    ucc_lib_h        lib;
    int              failed = 0;

    ucc_lib_params_t lib_params = {
        .mask        = UCC_LIB_PARAM_FIELD_THREAD_MODE,
        .thread_mode = UCC_THREAD_SINGLE,
    };

    if (UCC_OK != ucc_lib_config_read(nullptr, nullptr, &lib_config)) {
        std::cerr << "ucc_lib_config_read failed\n";
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (UCC_OK != ucc_init(&lib_params, lib_config, &lib)) {
        std::cerr << "ucc_init failed\n";
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    ucc_lib_config_release(lib_config);

    if (world_rank == 0) {
        std::cout << "=== UCC Resilience MPI Tests ===\n";
    }

    failed |= test_guard_violations(lib);
    failed |= test_abort_recover_no_fail(lib, MPI_COMM_WORLD);
    failed |= test_post_after_abort(lib, MPI_COMM_WORLD);
    failed |= test_simulated_failure(lib, MPI_COMM_WORLD);

    /* Aggregate: if any rank failed, all ranks report failure. */
    int global_failed;
    MPI_Allreduce(&failed, &global_failed, 1, MPI_INT, MPI_MAX,
                  MPI_COMM_WORLD);

    if (world_rank == 0) {
        std::cout << (global_failed ? "\n[FAIL] Some tests failed.\n"
                                    : "\n[PASS] All tests passed.\n");
    }

    ucc_finalize(lib);
    MPI_Finalize();
    return global_failed;
}
