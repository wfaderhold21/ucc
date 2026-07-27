/**
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 *
 * Standalone MPI test binary for Phase 1 resilience API.
 *
 * Tests:
 *  1. guard_violations      — single-rank exclusive context; state-machine
 *                             guards return the correct error codes.
 *  2. abort_recover_no_fail — all ranks abort+recover; expect 0 failed ranks.
 *  3. post_after_abort      — collective_post while ABORTING → UCC_ERR_ABORTED.
 *  4. drain_inflight        — posted request is drained to UCC_ERR_ABORTED
 *                             when abort is called before any progress.
 *  5. simulated_failure     — rank 0 marks rank (size-1); BOR agreement
 *                             propagates it to all surviving ranks.
 *  6. shrink_allreduce      — full lifecycle: simulated failure → abort →
 *                             recover → shrink (MPI_Comm_split sub-comm) →
 *                             new team → allreduce SUM with data verification.
 *  7. shrink_multi_kill     — K>1 ranks fail; BOR reports K; shrink survivors
 *                             and verify a full alltoall exchange (data check).
 *  8. iterated_shrink       — shrink, then fail+shrink a SECOND time; proves a
 *                             shrunken context is itself resilient. Final
 *                             alltoall over the twice-shrunk team is verified.
 *  9. shrink_to_single      — kill all but rank 0; barrier + allreduce on the
 *                             degenerate single-survivor context.
 * 10. shrink_repeated_colls — 20 rounds of barrier+alltoall+allreduce on the
 *                             shrunken team; catches latent rebuild bugs that
 *                             only surface under sustained reuse.
 *
 * Each test creates its own UCC context and team (if needed), runs the
 * scenario, and returns 0 on pass or 1 on fail.  The final exit code is the
 * OR of all test results so that CI detects any single failure.
 */
#include <mpi.h>
#include <iostream>
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
    *req = (void *)(uintptr_t)mpi_req; // NOLINT(clang-analyzer-optin.mpi.MPI-Checker)
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
    while (UCC_INPROGRESS == ucc_team_destroy(team)) {
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

/* Drive an alltoall to completion on the given team/context and verify the
   received data.  Rank i sends value (i*1000 + j) to rank j; after the
   exchange rank j must hold (i*1000 + j) in slot i for every source i.
   Returns 0 on success, non-zero on error/mismatch.  All ranks in `comm`
   must call this collectively. */
static int alltoall_verify(ucc_team_h team, ucc_context_h ctx, MPI_Comm comm)
{
    int rank, size;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    int64_t *sbuf = (int64_t *)malloc((size_t)size * sizeof(int64_t));
    int64_t *rbuf = (int64_t *)malloc((size_t)size * sizeof(int64_t));
    if (!sbuf || !rbuf) {
        free(sbuf);
        free(rbuf);
        return 1;
    }
    for (int j = 0; j < size; j++) {
        sbuf[j] = (int64_t)rank * 1000 + j;
        rbuf[j] = -1;
    }

    ucc_coll_args_t args = {};
    args.coll_type         = UCC_COLL_TYPE_ALLTOALL;
    args.src.info.buffer   = sbuf;
    args.src.info.count     = (uint64_t)size;
    args.src.info.datatype  = UCC_DT_INT64;
    args.src.info.mem_type  = UCC_MEMORY_TYPE_HOST;
    args.dst.info.buffer   = rbuf;
    args.dst.info.count     = (uint64_t)size;
    args.dst.info.datatype  = UCC_DT_INT64;
    args.dst.info.mem_type  = UCC_MEMORY_TYPE_HOST;

    ucc_coll_req_h req;
    int rc = 0;
    if (UCC_OK != ucc_collective_init(&args, &req, team)) { rc = 1; goto out; }
    if (UCC_OK != ucc_collective_post(req))               { rc = 1; goto out; }

    ucc_status_t st;
    do {
        ucc_context_progress(ctx);
        st = ucc_collective_test(req);
    } while (st == UCC_INPROGRESS);
    ucc_collective_finalize(req);
    if (st != UCC_OK) { rc = 1; goto out; }

    /* rbuf[i] came from rank i and should equal (i*1000 + rank). */
    for (int i = 0; i < size; i++) {
        int64_t exp = (int64_t)i * 1000 + rank;
        if (rbuf[i] != exp) {
            std::cerr << "[rank " << world_rank << "] alltoall mismatch: slot "
                      << i << " got " << rbuf[i] << " expected " << exp << "\n";
            rc = 1;
        }
    }
out:
    free(sbuf);
    free(rbuf);
    return rc;
}

/* Drive an allreduce SUM to completion and verify the result.  Rank i
   contributes (i+1); the reduced value must be size*(size+1)/2 on every rank.
   Returns 0 on success.  All ranks in `comm` must call this collectively. */
static int allreduce_verify(ucc_team_h team, ucc_context_h ctx, MPI_Comm comm)
{
    int rank, size;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &size);

    int64_t sbuf     = (int64_t)rank + 1;
    int64_t rbuf     = -1;
    int64_t expected = (int64_t)size * (size + 1) / 2;

    ucc_coll_args_t args = {};
    args.coll_type        = UCC_COLL_TYPE_ALLREDUCE;
    args.op               = UCC_OP_SUM;
    args.src.info.buffer  = &sbuf;
    args.src.info.count    = 1;
    args.src.info.datatype = UCC_DT_INT64;
    args.src.info.mem_type = UCC_MEMORY_TYPE_HOST;
    args.dst.info.buffer  = &rbuf;
    args.dst.info.count    = 1;
    args.dst.info.datatype = UCC_DT_INT64;
    args.dst.info.mem_type = UCC_MEMORY_TYPE_HOST;

    ucc_coll_req_h req;
    if (UCC_OK != ucc_collective_init(&args, &req, team)) return 1;
    if (UCC_OK != ucc_collective_post(req))               return 1;
    ucc_status_t st;
    do {
        ucc_context_progress(ctx);
        st = ucc_collective_test(req);
    } while (st == UCC_INPROGRESS);
    ucc_collective_finalize(req);
    if (st != UCC_OK)       return 1;
    if (rbuf != expected) {
        std::cerr << "[rank " << world_rank << "] allreduce mismatch: got "
                  << rbuf << " expected " << expected << "\n";
        return 1;
    }
    return 0;
}

/* Drive a barrier to completion on the given team/context.  Returns 0 on
   success.  All ranks in the team must call this collectively. */
static int barrier_once(ucc_team_h team, ucc_context_h ctx)
{
    ucc_coll_args_t args = {};
    args.coll_type = UCC_COLL_TYPE_BARRIER;
    ucc_coll_req_h req;
    if (UCC_OK != ucc_collective_init(&args, &req, team)) return 1;
    if (UCC_OK != ucc_collective_post(req))               return 1;
    ucc_status_t st;
    do {
        ucc_context_progress(ctx);
        st = ucc_collective_test(req);
    } while (st == UCC_INPROGRESS);
    ucc_collective_finalize(req);
    return (st == UCC_OK) ? 0 : 1;
}

/* Survivor-side shrink.  Precondition: `ctx` is already in RECOVERED state
   (all ranks, including the simulated-failed ones, must have jointly driven
   abort → abort_test → recover *before* the failed ranks dropped out, since
   the abort BOR allreduce spans the full rank set).  Produces a new context
   over `sub_comm`; on success *new_ctx_out holds it and the old ctx is
   destroyed inside ucc_context_shrink.  Returns 0 on success. */
static int survivor_shrink(ucc_lib_h lib, ucc_context_h ctx,
                           MPI_Comm sub_comm, ucc_context_h *new_ctx_out)
{
    int sub_rank, sub_size;
    MPI_Comm_rank(sub_comm, &sub_rank);
    MPI_Comm_size(sub_comm, &sub_size);

    ucc_context_config_h cfg;
    if (UCC_OK != ucc_context_config_read(lib, nullptr, &cfg)) return 1;

    ucc_context_params_t p = {};
    p.mask            = UCC_CONTEXT_PARAM_FIELD_OOB;
    p.oob.allgather   = oob_allgather;
    p.oob.req_test    = oob_allgather_test;
    p.oob.req_free    = oob_allgather_free;
    p.oob.coll_info   = (void *)(uintptr_t)sub_comm;
    p.oob.n_oob_eps   = sub_size;
    p.oob.oob_ep      = sub_rank;

    ucc_status_t st = ucc_context_shrink(ctx, &p, cfg, new_ctx_out);
    ucc_context_config_release(cfg);
    return (st == UCC_OK) ? 0 : 1;
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
/* Test 4: request posted before abort is drained to UCC_ERR_ABORTED.        */
/*                                                                            */
/* This tests the progress-queue drain path, not just the post-guard.        */
/*                                                                            */
/* The barrier is initialized on every rank but only posted on               */
/* world_size - 1 ranks.  A barrier requires ALL team members before any     */
/* can complete, so withholding one participant guarantees that none of      */
/* the (world_size - 1) posted requests can ever advance past INPROGRESS —   */
/* even though ucc_progress_queue_enqueue performs an inline                 */
/* task->progress(task) call at enqueue time.  The non-posting rank simply   */
/* finalizes its initialized-but-never-posted request and proceeds to        */
/* symmetric teardown alongside everyone else.                               */
/*                                                                            */
/* All ranks complete the full abort → abort_test → recover →               */
/* destroy_team → destroy_context sequence without any early returns,        */
/* ensuring that collective state is consistent across every rank before    */
/* the test concludes.                                                      */
/* -------------------------------------------------------------------------- */
static int test_drain_inflight(ucc_lib_h lib, MPI_Comm comm)
{
    const char *label = "drain_inflight";

    if (world_size < 2) {
        if (world_rank == 0) {
            std::cout << "[SKIP] " << label
                      << " (requires at least 2 ranks)\n";
        }
        return 0;
    }

    ucc_context_h ctx = create_global_ctx(lib, comm);
    RES_EXPECT(ctx != nullptr, label);

    ucc_team_h team = create_team(ctx, comm);
    RES_EXPECT(team != nullptr, label);

    ucc_coll_args_t args = {};
    args.coll_type = UCC_COLL_TYPE_BARRIER;
    ucc_coll_req_h req;
    RES_CHECK(ucc_collective_init(&args, &req, team), label);

    /* Post the barrier on only world_size - 1 ranks.  The last rank
       withholds its post so the barrier is provably incomplete: none of
       the posted requests can complete without the missing participant. */
    const bool do_post = (world_rank < world_size - 1);
    if (do_post) {
        RES_CHECK(ucc_collective_post(req), label);
    }

    /* Abort immediately; the drain inside ucc_context_abort must mark every
       queued request UCC_ERR_ABORTED. */
    RES_CHECK(ucc_context_abort(ctx), label);

    if (do_post) {
        /* The posted request must now be ABORTED without requiring any
           further progress.  This is the key correctness assertion. */
        ucc_status_t req_st = ucc_collective_test(req);
        RES_EXPECT(req_st == UCC_ERR_ABORTED, label);
    }

    /* Every rank finalizes its request (posted or not). */
    ucc_collective_finalize(req);

    /* Symmetric teardown on every rank — no early returns while collective
       state has diverged. */
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
/* Test 5: BOR agreement on simulated failure.                                */
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
/* Test 6: shrink produces a functional context — allreduce with data check.  */
/*                                                                            */
/* Full lifecycle:                                                             */
/*   1. All ranks create a context and simulate rank (size-1) failing.       */
/*   2. All ranks participate in abort+recover (the BOR allreduce needs all  */
/*      ranks alive, including the "failed" one in this simulation).         */
/*   3. Survivors (ranks 0..size-2) call ucc_context_shrink with a new      */
/*      MPI sub-communicator as OOB and create a team.                       */
/*   4. Survivors run allreduce SUM: each contributes its sub-rank.          */
/*   5. The expected sum is verified — a passing state machine with a broken */
/*      context would produce a wrong answer or hang.                        */
/* -------------------------------------------------------------------------- */
static int test_shrink_allreduce(ucc_lib_h lib, MPI_Comm comm)
{
    const char *label = "shrink_allreduce";

    if (world_size < 2) {
        if (world_rank == 0) {
            std::cout << "[SKIP] " << label << " (requires at least 2 ranks)\n";
        }
        return 0;
    }

    const int  failed_rank  = world_size - 1;
    const bool is_survivor  = (world_rank < world_size - 1);

    ucc_context_h ctx = create_global_ctx(lib, comm);
    RES_EXPECT(ctx != nullptr, label);

    /* Rank 0 marks rank (size-1) as the failed rank. */
    if (world_rank == 0) {
        ucc_context_mark_rank_failed(
            reinterpret_cast<ucc_context_t *>(ctx),
            static_cast<ucc_rank_t>(failed_rank));
    }

    /* All ranks participate in the abort BOR allreduce. */
    RES_CHECK(ucc_context_abort(ctx), label);
    RES_CHECK(poll_abort_test(ctx), label);
    RES_CHECK(ucc_context_recover(ctx), label);

    /* Split comm into survivor group (color 1) and failed group
       (MPI_UNDEFINED so they get MPI_COMM_NULL). */
    MPI_Comm sub_comm;
    MPI_Comm_split(comm, is_survivor ? 1 : MPI_UNDEFINED,
                   world_rank, &sub_comm);

    if (!is_survivor) {
        /* "Failed" rank just cleans up and synchronizes. */
        ucc_context_destroy(ctx);
        MPI_Barrier(comm);
        return 0;
    }

    /* --- Survivors only from here --- */

    int sub_rank, sub_size;
    MPI_Comm_rank(sub_comm, &sub_rank);
    MPI_Comm_size(sub_comm, &sub_size);

    /* Shrink: create new context for survivors using the sub-communicator. */
    ucc_context_config_h cfg;
    RES_CHECK(ucc_context_config_read(lib, nullptr, &cfg), label);

    ucc_context_params_t new_params = {};
    new_params.mask              = UCC_CONTEXT_PARAM_FIELD_OOB;
    new_params.oob.allgather     = oob_allgather;
    new_params.oob.req_test      = oob_allgather_test;
    new_params.oob.req_free      = oob_allgather_free;
    new_params.oob.coll_info     = (void *)(uintptr_t)sub_comm;
    new_params.oob.n_oob_eps     = sub_size;
    new_params.oob.oob_ep        = sub_rank;

    ucc_context_h new_ctx;
    ucc_status_t  shrink_st = ucc_context_shrink(ctx, &new_params, cfg, &new_ctx);
    ucc_context_config_release(cfg);
    RES_CHECK(shrink_st, label);

    /* Create a team on the new (smaller) context. */
    ucc_team_h team = create_team(new_ctx, sub_comm);
    RES_EXPECT(team != nullptr, label);

    /* Run allreduce SUM: each survivor contributes its sub_rank (0, 1, ...).
       Expected result: 0 + 1 + ... + (sub_size - 1) = sub_size*(sub_size-1)/2.
       A correct answer proves the collective ran on the right set of ranks
       with the right data — not just that it completed without crashing. */
    int32_t sbuf        = (int32_t)sub_rank;
    int32_t rbuf        = 0;
    int32_t expected    = (int32_t)(sub_size * (sub_size - 1) / 2);

    ucc_coll_args_t args = {};
    args.coll_type            = UCC_COLL_TYPE_ALLREDUCE;
    args.op                   = UCC_OP_SUM;
    args.src.info.buffer      = &sbuf;
    args.src.info.count       = 1;
    args.src.info.datatype    = UCC_DT_INT32;
    args.src.info.mem_type    = UCC_MEMORY_TYPE_HOST;
    args.dst.info.buffer      = &rbuf;
    args.dst.info.count       = 1;
    args.dst.info.datatype    = UCC_DT_INT32;
    args.dst.info.mem_type    = UCC_MEMORY_TYPE_HOST;

    ucc_coll_req_h req;
    RES_CHECK(ucc_collective_init(&args, &req, team), label);
    RES_CHECK(ucc_collective_post(req), label);

    ucc_status_t coll_st;
    do {
        ucc_context_progress(new_ctx);
        coll_st = ucc_collective_test(req);
    } while (coll_st == UCC_INPROGRESS);
    RES_CHECK(coll_st, label);
    ucc_collective_finalize(req);

    if (rbuf != expected) {
        std::cerr << "[rank " << world_rank << "] FAIL " << label
                  << ": wrong allreduce result: got " << rbuf
                  << " expected " << expected << "\n";
        destroy_team(team, new_ctx);
        ucc_context_destroy(new_ctx);
        MPI_Comm_free(&sub_comm);
        MPI_Barrier(comm);
        return 1;
    }

    destroy_team(team, new_ctx);
    ucc_context_destroy(new_ctx);
    MPI_Comm_free(&sub_comm);

    /* Synchronize with the "failed" rank before declaring pass. */
    MPI_Barrier(comm);
    if (world_rank == 0) {
        std::cout << "[PASS] " << label << "\n";
    }
    return 0;
}

/* -------------------------------------------------------------------------- */
/* Test 7: multi-rank failure → shrink → alltoall correctness.                */
/*                                                                            */
/* Marks the last K ranks failed (K = 2 when size >= 4, else 1), verifies the */
/* BOR agreement reports exactly K failed ranks, shrinks the survivors, and   */
/* checks a full alltoall exchange on the reduced team returns correct data.  */
/* This is the multi-victim generalisation of test_shrink_allreduce and the   */
/* only alltoall data-correctness check in the suite.                         */
/* -------------------------------------------------------------------------- */
static int test_shrink_multi_kill(ucc_lib_h lib, MPI_Comm comm)
{
    const char *label = "shrink_multi_kill";

    if (world_size < 3) {
        if (world_rank == 0)
            std::cout << "[SKIP] " << label << " (requires at least 3 ranks)\n";
        return 0;
    }

    const int  kill        = (world_size >= 4) ? 2 : 1;
    const int  surviving   = world_size - kill;
    const bool is_survivor = (world_rank < surviving);

    ucc_context_h ctx = create_global_ctx(lib, comm);
    RES_EXPECT(ctx != nullptr, label);

    /* Rank 0 observes the last K ranks as unreachable. */
    if (world_rank == 0) {
        for (int r = surviving; r < world_size; r++) {
            ucc_context_mark_rank_failed(reinterpret_cast<ucc_context_t *>(ctx),
                                         static_cast<ucc_rank_t>(r));
        }
    }

    /* All ranks (survivors + soon-to-be-failed) join the abort BOR. */
    RES_CHECK(ucc_context_abort(ctx), label);
    RES_CHECK(poll_abort_test(ctx), label);
    RES_CHECK(ucc_context_recover(ctx), label);

    ucc_context_attr_t attr;
    attr.mask = UCC_CONTEXT_ATTR_FIELD_FAILED_RANKS;
    RES_CHECK(ucc_context_get_attr(ctx, &attr), label);
    RES_EXPECT(attr.n_failed_ranks == static_cast<uint32_t>(kill), label);

    MPI_Comm sub_comm;
    MPI_Comm_split(comm, is_survivor ? 0 : MPI_UNDEFINED, world_rank, &sub_comm);

    if (!is_survivor) {
        ucc_context_destroy(ctx);
        MPI_Barrier(comm);
        return 0;
    }

    ucc_context_h new_ctx;
    RES_EXPECT(survivor_shrink(lib, ctx, sub_comm, &new_ctx) == 0, label);

    ucc_team_h team = create_team(new_ctx, sub_comm);
    RES_EXPECT(team != nullptr, label);

    int vrc = alltoall_verify(team, new_ctx, sub_comm);
    RES_EXPECT(vrc == 0, label);

    destroy_team(team, new_ctx);
    ucc_context_destroy(new_ctx);
    MPI_Comm_free(&sub_comm);

    MPI_Barrier(comm);
    if (world_rank == 0) {
        std::cout << "[PASS] " << label << " (killed " << kill
                  << ", " << surviving << " survivors)\n";
    }
    return 0;
}

/* -------------------------------------------------------------------------- */
/* Test 8: iterated resilience — shrink, then fail + shrink AGAIN.            */
/*                                                                            */
/* Proves a shrunken context is itself a fully-functional resilient context: */
/* it can be aborted, recovered and shrunk a second time.  Round 1 drops the */
/* last world rank (size -> size-1); round 2 drops the last surviving rank    */
/* (size-1 -> size-2).  A final alltoall over the twice-shrunk team verifies  */
/* data correctness.  Requires >= 4 ranks so the final team has >= 2 members. */
/* -------------------------------------------------------------------------- */
static int test_iterated_shrink(ucc_lib_h lib, MPI_Comm comm)
{
    const char *label = "iterated_shrink";

    if (world_size < 4) {
        if (world_rank == 0)
            std::cout << "[SKIP] " << label << " (requires at least 4 ranks)\n";
        return 0;
    }

    /* ---- Round 1: full world, drop the last rank. ---- */
    const int  r1_failed    = world_size - 1;
    const bool r1_survivor  = (world_rank < r1_failed);

    ucc_context_h ctx = create_global_ctx(lib, comm);
    RES_EXPECT(ctx != nullptr, label);

    if (world_rank == 0) {
        ucc_context_mark_rank_failed(reinterpret_cast<ucc_context_t *>(ctx),
                                     static_cast<ucc_rank_t>(r1_failed));
    }
    RES_CHECK(ucc_context_abort(ctx), label);
    RES_CHECK(poll_abort_test(ctx), label);
    RES_CHECK(ucc_context_recover(ctx), label);

    MPI_Comm comm1;
    MPI_Comm_split(comm, r1_survivor ? 0 : MPI_UNDEFINED, world_rank, &comm1);

    if (!r1_survivor) {
        /* Round-1 victim: drop out and wait at the single global barrier. */
        ucc_context_destroy(ctx);
        MPI_Barrier(comm);
        return 0;
    }

    ucc_context_h ctx1;
    RES_EXPECT(survivor_shrink(lib, ctx, comm1, &ctx1) == 0, label);

    int r1_rank, r1_size;
    MPI_Comm_rank(comm1, &r1_rank);
    MPI_Comm_size(comm1, &r1_size);

    /* ---- Round 2: on the shrunken context, drop its last rank. ---- */
    const int  r2_failed   = r1_size - 1;
    const bool r2_survivor = (r1_rank < r2_failed);

    if (r1_rank == 0) {
        ucc_context_mark_rank_failed(reinterpret_cast<ucc_context_t *>(ctx1),
                                     static_cast<ucc_rank_t>(r2_failed));
    }
    RES_CHECK(ucc_context_abort(ctx1), label);
    RES_CHECK(poll_abort_test(ctx1), label);
    RES_CHECK(ucc_context_recover(ctx1), label);

    MPI_Comm comm2;
    MPI_Comm_split(comm1, r2_survivor ? 0 : MPI_UNDEFINED, r1_rank, &comm2);

    if (!r2_survivor) {
        ucc_context_destroy(ctx1);
        MPI_Comm_free(&comm1);
        MPI_Barrier(comm);
        return 0;
    }

    ucc_context_h ctx2;
    RES_EXPECT(survivor_shrink(lib, ctx1, comm2, &ctx2) == 0, label);

    ucc_team_h team = create_team(ctx2, comm2);
    RES_EXPECT(team != nullptr, label);

    int vrc = alltoall_verify(team, ctx2, comm2);
    RES_EXPECT(vrc == 0, label);

    destroy_team(team, ctx2);
    ucc_context_destroy(ctx2);
    MPI_Comm_free(&comm2);
    MPI_Comm_free(&comm1);

    MPI_Barrier(comm);
    if (world_rank == 0) {
        std::cout << "[PASS] " << label << " (" << world_size << " -> "
                  << (world_size - 1) << " -> " << (world_size - 2)
                  << " ranks)\n";
    }
    return 0;
}

/* -------------------------------------------------------------------------- */
/* Test 9: shrink to a single survivor.                                       */
/*                                                                            */
/* Kills all but rank 0 (kill_count = size-1).  The lone survivor shrinks to  */
/* a 1-rank context and must still run a barrier and an allreduce (which      */
/* reduces to its own contribution).  This is the degenerate lower bound of   */
/* the shrink path — the smallest possible surviving set.                     */
/* -------------------------------------------------------------------------- */
static int test_shrink_to_single(ucc_lib_h lib, MPI_Comm comm)
{
    const char *label = "shrink_to_single";

    if (world_size < 2) {
        if (world_rank == 0)
            std::cout << "[SKIP] " << label << " (requires at least 2 ranks)\n";
        return 0;
    }

    const bool is_survivor = (world_rank == 0);

    ucc_context_h ctx = create_global_ctx(lib, comm);
    RES_EXPECT(ctx != nullptr, label);

    if (world_rank == 0) {
        for (int r = 1; r < world_size; r++) {
            ucc_context_mark_rank_failed(reinterpret_cast<ucc_context_t *>(ctx),
                                         static_cast<ucc_rank_t>(r));
        }
    }

    RES_CHECK(ucc_context_abort(ctx), label);
    RES_CHECK(poll_abort_test(ctx), label);
    RES_CHECK(ucc_context_recover(ctx), label);

    ucc_context_attr_t attr;
    attr.mask = UCC_CONTEXT_ATTR_FIELD_FAILED_RANKS;
    RES_CHECK(ucc_context_get_attr(ctx, &attr), label);
    RES_EXPECT(attr.n_failed_ranks == static_cast<uint32_t>(world_size - 1),
               label);

    MPI_Comm sub_comm;
    MPI_Comm_split(comm, is_survivor ? 0 : MPI_UNDEFINED, world_rank, &sub_comm);

    if (!is_survivor) {
        ucc_context_destroy(ctx);
        MPI_Barrier(comm);
        return 0;
    }

    ucc_context_h new_ctx;
    RES_EXPECT(survivor_shrink(lib, ctx, sub_comm, &new_ctx) == 0, label);

    ucc_team_h team = create_team(new_ctx, sub_comm);
    RES_EXPECT(team != nullptr, label);

    RES_EXPECT(barrier_once(team, new_ctx) == 0, label);
    RES_EXPECT(allreduce_verify(team, new_ctx, sub_comm) == 0, label);

    destroy_team(team, new_ctx);
    ucc_context_destroy(new_ctx);
    MPI_Comm_free(&sub_comm);

    MPI_Barrier(comm);
    if (world_rank == 0) {
        std::cout << "[PASS] " << label << " (1 survivor)\n";
    }
    return 0;
}

/* -------------------------------------------------------------------------- */
/* Test 10: sustained collectives on a shrunken context.                      */
/*                                                                            */
/* After a single-rank failure and shrink, run several rounds of a mixed      */
/* collective workload (barrier + alltoall + allreduce) on the reduced team.  */
/* A one-shot post-shrink collective can pass while a latent teardown/rebuild */
/* bug only surfaces under repeated use; this exercises that path.            */
/* -------------------------------------------------------------------------- */
static int test_shrink_repeated_colls(ucc_lib_h lib, MPI_Comm comm)
{
    const char *label = "shrink_repeated_colls";
    const int   ROUNDS = 20;

    if (world_size < 2) {
        if (world_rank == 0)
            std::cout << "[SKIP] " << label << " (requires at least 2 ranks)\n";
        return 0;
    }

    const int  failed_rank = world_size - 1;
    const bool is_survivor = (world_rank < failed_rank);

    ucc_context_h ctx = create_global_ctx(lib, comm);
    RES_EXPECT(ctx != nullptr, label);

    if (world_rank == 0) {
        ucc_context_mark_rank_failed(reinterpret_cast<ucc_context_t *>(ctx),
                                     static_cast<ucc_rank_t>(failed_rank));
    }
    RES_CHECK(ucc_context_abort(ctx), label);
    RES_CHECK(poll_abort_test(ctx), label);
    RES_CHECK(ucc_context_recover(ctx), label);

    MPI_Comm sub_comm;
    MPI_Comm_split(comm, is_survivor ? 0 : MPI_UNDEFINED, world_rank, &sub_comm);

    if (!is_survivor) {
        ucc_context_destroy(ctx);
        MPI_Barrier(comm);
        return 0;
    }

    ucc_context_h new_ctx;
    RES_EXPECT(survivor_shrink(lib, ctx, sub_comm, &new_ctx) == 0, label);

    ucc_team_h team = create_team(new_ctx, sub_comm);
    RES_EXPECT(team != nullptr, label);

    for (int r = 0; r < ROUNDS; r++) {
        RES_EXPECT(barrier_once(team, new_ctx) == 0, label);
        RES_EXPECT(alltoall_verify(team, new_ctx, sub_comm) == 0, label);
        RES_EXPECT(allreduce_verify(team, new_ctx, sub_comm) == 0, label);
    }

    destroy_team(team, new_ctx);
    ucc_context_destroy(new_ctx);
    MPI_Comm_free(&sub_comm);

    MPI_Barrier(comm);
    if (world_rank == 0) {
        std::cout << "[PASS] " << label << " (" << ROUNDS
                  << " rounds on " << (world_size - 1) << "-rank team)\n";
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
    failed |= test_drain_inflight(lib, MPI_COMM_WORLD);
    failed |= test_simulated_failure(lib, MPI_COMM_WORLD);
    failed |= test_shrink_allreduce(lib, MPI_COMM_WORLD);
    failed |= test_shrink_multi_kill(lib, MPI_COMM_WORLD);
    failed |= test_iterated_shrink(lib, MPI_COMM_WORLD);
    failed |= test_shrink_to_single(lib, MPI_COMM_WORLD);
    failed |= test_shrink_repeated_colls(lib, MPI_COMM_WORLD);

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
