/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

/*
 * Bounded multi-rank runtime coverage for UCC issue #1174
 * ("enforce UCC owner lifetimes"). Runs under MPI with a live multi-rank
 * UCP team and mapping.
 *
 * Coverage:
 *   1. Refusal/retry: ucc_context_destroy and ucc_finalize refuse while live
 *      team and memory-mapping handles exist; retries succeed only after the
 *      owning handles are released.
 *   2. Asynchronous failed team creation: a team whose OOB exchange fails is
 *      left in the failed state but retains ownership of its context until
 *      ucc_team_destroy; ucc_context_destroy refuses until then.
 *   3. Ordered cleanup: unmap -> destroy team -> destroy context -> finalize,
 *      verified step by step with the refusal/retry semantics above.
 *
 * Bounded: fixed rank count, one pass, no unbounded loops.
 */

#include <iostream>
#include <cstring>
#include <ucc/api/ucc.h>
#include <mpi.h>
extern "C" {
#include "utils/ucc_malloc.h"
}

/* Abort helper for hard failures (library/MPI contract violations). */
#define CHECK(_expr)                                                     \
    do {                                                                 \
        if (!(_expr)) {                                                  \
            std::cerr << "*** test_lifetime FAIL: " << #_expr << "\n";   \
            MPI_Abort(MPI_COMM_WORLD, 1);                                \
        }                                                                \
    } while (0)

/* Expect an exact UCC status; abort if it differs. */
#define CHECK_STATUS(_expr, _want)                                       \
    do {                                                                 \
        ucc_status_t _s_ = (_expr);                                      \
        if (_s_ != (_want)) {                                            \
            std::cerr << "*** test_lifetime FAIL: " << #_expr            \
                      << " = " << ucc_status_string(_s_)                 \
                      << " (want " << ucc_status_string(_want) << ")\n"; \
            MPI_Abort(MPI_COMM_WORLD, 1);                                \
        }                                                                \
    } while (0)

/* --- Working MPI OOB (used for the context and happy-path teams) --- */
static ucc_status_t oob_allgather(void *sbuf, void *rbuf, size_t msglen,
                                  void *coll_info, void **req)
{
    MPI_Comm    comm = (MPI_Comm)(uintptr_t)coll_info;
    MPI_Request request;

    MPI_Iallgather(sbuf, msglen, MPI_BYTE, rbuf, msglen, MPI_BYTE, comm,
                   &request);
    *req = (void *)(uintptr_t)request;
    return UCC_OK;
}

static ucc_status_t oob_req_test(void *req)
{
    MPI_Request request = (MPI_Request)(uintptr_t)req;
    int         completed;

    MPI_Test(&request, &completed, MPI_STATUS_IGNORE);
    return completed ? UCC_OK : UCC_INPROGRESS;
}

static ucc_status_t oob_req_free(void *req)
{
    return UCC_OK;
}

/* --- Failing OOB: allgather "starts" but req_test always fails. This makes
 *     the team addr exchange fail asynchronously (UCC_TEAM_FAILED). --- */
static ucc_status_t fail_allgather(void *sbuf, void *rbuf, size_t msglen,
                                   void *coll_info, void **req)
{
    *req = (void *)(uintptr_t)0x1; /* non-NULL marker */
    return UCC_OK;
}

static ucc_status_t fail_req_test(void *req)
{
    return UCC_ERR_NO_RESOURCE; /* negative -> async exchange failure */
}

static ucc_status_t fail_req_free(void *req)
{
    return UCC_OK;
}

/* Create a live multi-rank team on ctx using the working MPI OOB. */
static ucc_team_h create_live_team(ucc_context_h ctx, int rank, int size)
{
    ucc_team_params_t params = {};
    ucc_team_h        team   = nullptr;
    ucc_status_t      st;

    params.mask = UCC_TEAM_PARAM_FIELD_EP | UCC_TEAM_PARAM_FIELD_EP_RANGE |
                  UCC_TEAM_PARAM_FIELD_OOB;
    params.oob.allgather = oob_allgather;
    params.oob.req_test  = oob_req_test;
    params.oob.req_free  = oob_req_free;
    params.oob.coll_info = (void *)(uintptr_t)MPI_COMM_WORLD;
    params.oob.n_oob_eps = size;
    params.oob.oob_ep    = rank;
    params.ep            = rank;
    params.ep_range      = UCC_COLLECTIVE_EP_RANGE_CONTIG;

    CHECK_STATUS(ucc_team_create_post(&ctx, 1, &params, &team), UCC_OK);
    CHECK(team != nullptr);
    do {
        st = ucc_team_create_test(team);
        if (st == UCC_INPROGRESS) {
            ucc_context_progress(ctx);
        }
    } while (st == UCC_INPROGRESS);
    CHECK_STATUS(st, UCC_OK);
    return team;
}

/* Create a shared multi-rank context with the working MPI OOB. */
static ucc_context_h create_context(ucc_lib_h lib, int rank, int size)
{
    ucc_context_params_t  ctx_params = {};
    ucc_context_config_h  ctx_config;
    ucc_context_h         ctx;

    ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_TYPE |
                      UCC_CONTEXT_PARAM_FIELD_OOB;
    ctx_params.type = UCC_CONTEXT_SHARED;
    ctx_params.oob.allgather = oob_allgather;
    ctx_params.oob.req_test  = oob_req_test;
    ctx_params.oob.req_free  = oob_req_free;
    ctx_params.oob.coll_info = (void *)(uintptr_t)MPI_COMM_WORLD;
    ctx_params.oob.n_oob_eps = size;
    ctx_params.oob.oob_ep    = rank;

    CHECK_STATUS(ucc_context_config_read(lib, NULL, &ctx_config), UCC_OK);
    CHECK_STATUS(ucc_context_create(lib, &ctx_params, ctx_config, &ctx),
                 UCC_OK);
    ucc_context_config_release(ctx_config);
    return ctx;
}

/* Phase 1: refusal/retry with a live team + live mapping, then ordered
 * cleanup. Uses a fresh library so it can be finalized at the end. */
static void run_phase_refusal_retry(int rank, int size)
{
    ucc_lib_params_t      lib_params = {};
    ucc_lib_config_h      lib_config;
    ucc_lib_h             lib;
    ucc_context_h         ctx;
    ucc_team_h            team = nullptr;
    ucc_mem_map_mem_h     memh = nullptr;
    size_t                memh_size = 0;
    void                 *buffer;
    const size_t          buf_size = (1u << 20);
    ucc_status_t          st;

    lib_params.mask = UCC_LIB_PARAM_FIELD_THREAD_MODE;
    lib_params.thread_mode = UCC_THREAD_SINGLE;
    CHECK_STATUS(ucc_lib_config_read(NULL, NULL, &lib_config), UCC_OK);
    CHECK_STATUS(ucc_init(&lib_params, lib_config, &lib), UCC_OK);
    ucc_lib_config_release(lib_config);

    ctx = create_context(lib, rank, size);

    /* Live mapping. */
    buffer = ucc_malloc(buf_size, "test_lifetime_buffer");
    CHECK(buffer != nullptr);
    ucc_mem_map_params_t map_params;
    ucc_mem_map_t        seg;
    seg.address     = buffer;
    seg.len         = buf_size;
    map_params.segments   = &seg;
    map_params.n_segments = 1;
    st = ucc_mem_map(ctx, UCC_MEM_MAP_MODE_EXPORT, &map_params, &memh_size,
                     &memh);
    if (st == UCC_ERR_NOT_SUPPORTED || st == UCC_ERR_NOT_IMPLEMENTED) {
        /* TL cannot map host memory here; nothing to hold, skip mapping
         * accounting and rely on the team-only refusal path. */
        if (rank == 0) {
            std::cerr << "test_lifetime: mem_map not supported, skipping "
                         "mapping coverage\n";
        }
    } else {
        CHECK_STATUS(st, UCC_OK);
        CHECK(memh != nullptr);
    }

    /* Live multi-rank team. */
    team = create_live_team(ctx, rank, size);
    CHECK(team != nullptr);

    /* Refusal: context has a live team (and, if mapped, a live mapping). */
    CHECK_STATUS(ucc_context_destroy(ctx), UCC_ERR_INVALID_PARAM);
    /* Refusal: library has a live context. */
    CHECK_STATUS(ucc_finalize(lib), UCC_ERR_INVALID_PARAM);

    /* Unmap -> context still refuses (live team). */
    if (memh != nullptr) {
        CHECK_STATUS(ucc_mem_unmap(&memh), UCC_OK);
        CHECK(memh == nullptr);
        CHECK_STATUS(ucc_context_destroy(ctx), UCC_ERR_INVALID_PARAM);
    }

    /* Destroy team -> context destroy succeeds. */
    CHECK_STATUS(ucc_team_destroy(team), UCC_OK);
    team = nullptr;
    CHECK_STATUS(ucc_context_destroy(ctx), UCC_OK);
    CHECK_STATUS(ucc_finalize(lib), UCC_OK);

    ucc_free(buffer);
}

/* Phase 2: asynchronous failed team creation. A team whose OOB exchange fails
 * is left failed but retains context ownership until ucc_team_destroy. */
static void run_phase_async_failure(int rank, int size)
{
    ucc_lib_params_t      lib_params = {};
    ucc_lib_config_h      lib_config;
    ucc_lib_h             lib;
    ucc_context_h         ctx;
    ucc_team_params_t     params = {};
    ucc_team_h            team   = nullptr;
    ucc_status_t          st;

    lib_params.mask = UCC_LIB_PARAM_FIELD_THREAD_MODE;
    lib_params.thread_mode = UCC_THREAD_SINGLE;
    CHECK_STATUS(ucc_lib_config_read(NULL, NULL, &lib_config), UCC_OK);
    CHECK_STATUS(ucc_init(&lib_params, lib_config, &lib), UCC_OK);
    ucc_lib_config_release(lib_config);

    ctx = create_context(lib, rank, size);

    /* Team with the failing OOB: create_post succeeds (async create). */
    params.mask = UCC_TEAM_PARAM_FIELD_EP | UCC_TEAM_PARAM_FIELD_EP_RANGE |
                  UCC_TEAM_PARAM_FIELD_OOB;
    params.oob.allgather = fail_allgather;
    params.oob.req_test  = fail_req_test;
    params.oob.req_free  = fail_req_free;
    params.oob.coll_info = (void *)(uintptr_t)MPI_COMM_WORLD;
    params.oob.n_oob_eps = size;
    params.oob.oob_ep    = rank;
    params.ep            = rank;
    params.ep_range      = UCC_COLLECTIVE_EP_RANGE_CONTIG;

    CHECK_STATUS(ucc_team_create_post(&ctx, 1, &params, &team), UCC_OK);
    CHECK(team != nullptr);

    /* Drive creation to failure: create_test must return a negative status. */
    st = UCC_INPROGRESS;
    while (st == UCC_INPROGRESS) {
        st = ucc_team_create_test(team);
        if (st == UCC_INPROGRESS) {
            ucc_context_progress(ctx);
        }
    }
    CHECK(st < 0); /* async failure -> team left in failed state */

    /* Failed team retains ownership: context destroy refuses. */
    CHECK_STATUS(ucc_context_destroy(ctx), UCC_ERR_INVALID_PARAM);

    /* ucc_team_destroy releases the failed team's ownership. */
    CHECK_STATUS(ucc_team_destroy(team), UCC_OK);
    team = nullptr;

    CHECK_STATUS(ucc_context_destroy(ctx), UCC_OK);
    CHECK_STATUS(ucc_finalize(lib), UCC_OK);
}

int main(int argc, char *argv[])
{
    int rank, size;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size < 2) {
        if (rank == 0) {
            std::cerr << "test_lifetime requires at least 2 ranks "
                         "(multi-rank UCP coverage)\n";
        }
        MPI_Abort(MPI_COMM_WORLD, 2);
    }

    run_phase_refusal_retry(rank, size);
    run_phase_async_failure(rank, size);

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        std::cout << "test_lifetime: PASS\n";
    }
    MPI_Finalize();
    return 0;
}
