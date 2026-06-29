/**
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 *
 * Resilience tests: abort / recover / shrink state machine AND functional
 * correctness.
 *
 * Test groups:
 *  1. Guard violations  — single exclusive context, no OOB / service team.
 *  2. Abort + recover   — fresh 4-process UccJob with global (OOB) context.
 *     2a. abort_recover_no_failure     — state machine round-trip, 0 failed.
 *     2b. post_after_abort             — post while ABORTING → UCC_ERR_ABORTED.
 *     2c. drain_inflight_on_abort      — posted request is drained to ABORTED.
 *     2d. simulated_failure_bor        — BOR agreement propagates one failure.
 *  3. Shrink correctness — full lifecycle: failure → abort → recover →
 *     shrink → new team → collective on surviving ranks.
 *     3a. barrier_completes_after_shrink     — barrier on shrunken team.
 *     3b. allreduce_data_correct_after_shrink — allreduce SUM with data check.
 */
#include "common/test_ucc.h"
#include "common/test.h"
#include "core/test_context.h"

extern "C" {
#include "core/ucc_context.h"
}

/* Thread-allgather helpers defined in test_ucc.cc; needed for context
   creation in threads (context creation is synchronous with the OOB). */
ucc_status_t thread_allgather_start(void *src, void *dst, size_t size,
                                    void *coll_info, void **request);
ucc_status_t thread_allgather_req_test(void *request);
ucc_status_t thread_allgather_req_free(void *request);

/* ========================================================================= */
/* Fixture 1: guard violations on a single exclusive context (no OOB).       */
/* Inherits test_context_get_attr which owns ctx_h and destroys it.          */
/* ========================================================================= */
class test_resilience_guards : public test_context_get_attr {};

/* Calling abort_test before abort: context is still ACTIVE. */
UCC_TEST_F(test_resilience_guards, abort_test_before_abort)
{
    EXPECT_EQ(UCC_ERR_INVALID_STATE, ucc_context_abort_test(ctx_h));
}

/* Calling recover before abort has completed. */
UCC_TEST_F(test_resilience_guards, recover_before_abort)
{
    EXPECT_EQ(UCC_ERR_INVALID_STATE, ucc_context_recover(ctx_h));
}

/* Calling shrink before context is recovered. */
UCC_TEST_F(test_resilience_guards, shrink_before_recover)
{
    EXPECT_EQ(UCC_ERR_INVALID_STATE,
              ucc_context_shrink(ctx_h, nullptr, nullptr, nullptr));
}

/* Abort on an exclusive context (no OOB -> no service_team). */
UCC_TEST_F(test_resilience_guards, abort_no_service_team)
{
    /* No OOB means no service team; abort guard returns INVALID_STATE
       without changing context state, so the destructor can destroy normally. */
    EXPECT_EQ(UCC_ERR_INVALID_STATE, ucc_context_abort(ctx_h));
}

/* ========================================================================= */
/* Fixture 2: multi-process abort / recover using a fresh UccJob per test.   */
/* ========================================================================= */
class test_resilience_multi : public ucc::test {
protected:
    static const int n_procs = 4;
    UccJob *         job;

    void SetUp() override
    {
        job = new UccJob(n_procs, UccJob::UCC_JOB_CTX_GLOBAL);
    }

    void TearDown() override { delete job; }

    void progress_all()
    {
        for (int i = 0; i < n_procs; i++) {
            ucc_context_progress(job->procs[i]->ctx_h);
        }
    }

    void abort_all()
    {
        for (int i = 0; i < n_procs; i++) {
            ASSERT_EQ(UCC_OK, ucc_context_abort(job->procs[i]->ctx_h));
        }
    }

    /* Poll abort_test on every process; progress all on each iteration so
       the service allreduce transport is driven even for processes that have
       already finished. */
    void wait_abort_all()
    {
        std::vector<bool> done(n_procs, false);
        int               remaining = n_procs;
        while (remaining > 0) {
            progress_all();
            for (int i = 0; i < n_procs; i++) {
                if (done[i]) continue;
                ucc_status_t st =
                    ucc_context_abort_test(job->procs[i]->ctx_h);
                if (st == UCC_OK) {
                    done[i] = true;
                    --remaining;
                } else {
                    ASSERT_EQ(UCC_INPROGRESS, st);
                }
            }
        }
    }

    void recover_all()
    {
        for (int i = 0; i < n_procs; i++) {
            ASSERT_EQ(UCC_OK, ucc_context_recover(job->procs[i]->ctx_h));
        }
    }
};

/* Abort + recover with no injected failure: every process must see 0 failed
   ranks after recover. */
UCC_TEST_F(test_resilience_multi, abort_recover_no_failure)
{
    abort_all();
    wait_abort_all();
    recover_all();

    for (int i = 0; i < n_procs; i++) {
        ucc_context_attr_t attr;
        attr.mask = UCC_CONTEXT_ATTR_FIELD_FAILED_RANKS;
        ASSERT_EQ(UCC_OK, ucc_context_get_attr(job->procs[i]->ctx_h, &attr));
        EXPECT_EQ(0u, attr.n_failed_ranks);
    }
}

/* After abort, ucc_collective_post must return UCC_ERR_ABORTED. */
UCC_TEST_F(test_resilience_multi, post_after_abort)
{
    auto team = job->create_team(n_procs);

    /* Init barrier requests before aborting (init is state-independent). */
    std::vector<ucc_coll_req_h> reqs(n_procs);
    for (int i = 0; i < n_procs; i++) {
        ucc_coll_args_t args = {};
        args.coll_type       = UCC_COLL_TYPE_BARRIER;
        ASSERT_EQ(UCC_OK,
                  ucc_collective_init(&args, &reqs[i],
                                      team->procs[i].team));
    }

    /* Abort all — context state transitions to ABORTING.
       The barriers were never posted so the drain is a no-op. */
    abort_all();

    /* Now attempt to post each init'd request; context is ABORTING. */
    for (int i = 0; i < n_procs; i++) {
        EXPECT_EQ(UCC_ERR_ABORTED, ucc_collective_post(reqs[i]));
    }

    /* Finalize the never-posted requests to free their memory. */
    for (int i = 0; i < n_procs; i++) {
        ucc_collective_finalize(reqs[i]);
    }

    wait_abort_all();
    recover_all();
}

/* Requests that have already been POSTED must be drained to UCC_ERR_ABORTED
   when abort is called.  This tests the progress-queue drain path, not just
   the post-guard.

   We post barriers for n_procs-1 ranks only (all but the last).  A barrier
   requires ALL team members to post before any can complete, so those
   (n_procs-1) requests are guaranteed to stay in the progress queues —
   they cannot complete without the final rank.  The drain path inside
   ucc_context_abort must mark every queued request UCC_ERR_ABORTED. */
UCC_TEST_F(test_resilience_multi, drain_inflight_on_abort)
{
    const int n_posted = n_procs - 1; /* ranks 0 .. n_procs-2 */
    auto team = job->create_team(n_procs);

    ucc_coll_args_t args = {};
    args.coll_type = UCC_COLL_TYPE_BARRIER;

    std::vector<ucc_coll_req_h> reqs(n_procs);
    for (int i = 0; i < n_procs; i++) {
        ASSERT_EQ(UCC_OK,
                  ucc_collective_init(&args, &reqs[i], team->procs[i].team));
    }

    /* Post for the first n_posted ranks.  The barrier is incomplete — the
       last rank never posts — so these requests stay queued as INPROGRESS. */
    for (int i = 0; i < n_posted; i++) {
        ASSERT_EQ(UCC_OK, ucc_collective_post(reqs[i]));
    }

    /* Abort immediately; the drain must mark every queued request ABORTED. */
    abort_all();

    for (int i = 0; i < n_posted; i++) {
        EXPECT_EQ(UCC_ERR_ABORTED, ucc_collective_test(reqs[i]))
            << "posted request for rank " << i
            << " was not drained to UCC_ERR_ABORTED";
    }
    for (int i = 0; i < n_procs; i++) {
        ucc_collective_finalize(reqs[i]);
    }

    wait_abort_all();
    recover_all();
}

/* Process 0 marks rank (n-1) as failed before the abort.
   After BOR agreement, every process must expose exactly that one rank. */
UCC_TEST_F(test_resilience_multi, simulated_failure_bor)
{
    const ucc_rank_t failed_rank = static_cast<ucc_rank_t>(n_procs - 1);

    /* Simulate process 0 detecting that rank (n-1) is unreachable. */
    ucc_context_mark_rank_failed(
        reinterpret_cast<ucc_context_t *>(job->procs[0]->ctx_h), failed_rank);

    abort_all();
    wait_abort_all();
    recover_all();

    for (int i = 0; i < n_procs; i++) {
        ucc_context_attr_t attr;
        attr.mask = UCC_CONTEXT_ATTR_FIELD_FAILED_RANKS;
        ASSERT_EQ(UCC_OK, ucc_context_get_attr(job->procs[i]->ctx_h, &attr));
        ASSERT_EQ(1u, attr.n_failed_ranks)
            << "process " << i << " expected 1 failed rank";
        EXPECT_EQ(failed_rank, attr.failed_ranks[0])
            << "process " << i << " wrong failed rank";
    }
}

/* ========================================================================= */
/* Fixture 3: shrink correctness.                                             */
/*                                                                            */
/* Tests the full lifecycle:                                                  */
/*   failure → abort → recover → shrink → new team → collective.             */
/*                                                                            */
/* ucc_context_shrink is synchronous with its internal OOB: all N surviving  */
/* procs must call it concurrently.  shrink_survivors() handles this by       */
/* spawning one thread per survivor, mirroring UccJob::create_context().     */
/* After shrink each proc's ctx_h is updated in-place; UccProcess's          */
/* destructor then correctly destroys the new context.                        */
/* ========================================================================= */
class test_resilience_shrink : public test_resilience_multi {
protected:
    /* Atomically shrink procs[0..n_survivors-1] to new (smaller) contexts.
       Each surviving proc's ctx_h is replaced with the new context handle.
       The old contexts are destroyed inside ucc_context_shrink. */
    void shrink_survivors(int n_survivors)
    {
        struct ShrinkTask {
            ucc_context_h   *ctx_inout;
            ucc_lib_h        lib;
            int              new_rank;
            int              n;
            ThreadAllgather *ta;
            ucc_status_t     st{UCC_ERR_NO_MESSAGE};
        };

        auto *ta = new ThreadAllgather(n_survivors);
        std::vector<ShrinkTask> tasks(n_survivors);
        for (int i = 0; i < n_survivors; i++) {
            tasks[i].ctx_inout = &job->procs[i]->ctx_h;
            tasks[i].lib       = job->procs[i]->lib_h;
            tasks[i].new_rank  = i;
            tasks[i].n         = n_survivors;
            tasks[i].ta        = ta;
        }

        std::vector<std::thread> threads;
        for (int i = 0; i < n_survivors; i++) {
            threads.emplace_back([i, n_survivors, &tasks]() {
                ShrinkTask &t = tasks[i];

                ucc_context_params_t params = {};
                params.mask              = UCC_CONTEXT_PARAM_FIELD_OOB |
                                           UCC_CONTEXT_PARAM_FIELD_TYPE;
                params.type              = UCC_CONTEXT_EXCLUSIVE;
                params.oob.allgather     = thread_allgather_start;
                params.oob.req_test      = thread_allgather_req_test;
                params.oob.req_free      = thread_allgather_req_free;
                params.oob.coll_info     = &t.ta->reqs[i];
                params.oob.n_oob_eps     = n_survivors;
                params.oob.oob_ep        = i;

                ucc_context_config_h cfg;
                if (UCC_OK != ucc_context_config_read(t.lib, nullptr, &cfg)) {
                    t.st = UCC_ERR_NO_MESSAGE;
                    return;
                }
                ucc_context_h new_ctx;
                t.st = ucc_context_shrink(*t.ctx_inout, &params, cfg, &new_ctx);
                ucc_context_config_release(cfg);
                if (t.st == UCC_OK) {
                    *t.ctx_inout = new_ctx;
                }
            });
        }
        for (auto &th : threads) th.join();
        delete ta;

        for (int i = 0; i < n_survivors; i++) {
            ASSERT_EQ(UCC_OK, tasks[i].st)
                << "ucc_context_shrink failed for rank " << i;
        }
    }

    /* Run allreduce SUM INT32 on a team where process i contributes (i+1).
       Expected result = n*(n+1)/2.  Using (i+1) keeps the expected value
       non-zero even for a single-survivor team. */
    void run_allreduce_check(UccTeam_h team)
    {
        int n = team->n_procs;
        std::vector<int32_t> sbufs(n), rbufs(n, -1);
        for (int i = 0; i < n; i++) sbufs[i] = (int32_t)(i + 1);
        const int32_t expected = (int32_t)(n * (n + 1) / 2);

        std::vector<ucc_coll_args_t> cargs(n);
        std::vector<ucc_coll_req_h>  reqs(n);
        for (int i = 0; i < n; i++) {
            cargs[i] = {};
            cargs[i].coll_type            = UCC_COLL_TYPE_ALLREDUCE;
            cargs[i].op                   = UCC_OP_SUM;
            cargs[i].src.info.buffer      = &sbufs[i];
            cargs[i].src.info.count       = 1;
            cargs[i].src.info.datatype    = UCC_DT_INT32;
            cargs[i].src.info.mem_type    = UCC_MEMORY_TYPE_HOST;
            cargs[i].dst.info.buffer      = &rbufs[i];
            cargs[i].dst.info.count       = 1;
            cargs[i].dst.info.datatype    = UCC_DT_INT32;
            cargs[i].dst.info.mem_type    = UCC_MEMORY_TYPE_HOST;
            ASSERT_EQ(UCC_OK, ucc_collective_init(&cargs[i], &reqs[i],
                                                  team->procs[i].team));
            ASSERT_EQ(UCC_OK, ucc_collective_post(reqs[i]));
        }

        bool all_done;
        do {
            team->progress();
            all_done = true;
            for (int i = 0; i < n; i++) {
                ucc_status_t s = ucc_collective_test(reqs[i]);
                if (s == UCC_INPROGRESS) {
                    all_done = false;
                    continue;
                }
                ASSERT_GE(s, 0) << "allreduce error at rank " << i << ": "
                                << ucc_status_string(s);
            }
        } while (!all_done);

        for (int i = 0; i < n; i++) {
            EXPECT_EQ(expected, rbufs[i])
                << "wrong allreduce result at rank " << i
                << " (expected " << expected << ", got " << rbufs[i] << ")";
            ucc_collective_finalize(reqs[i]);
        }
    }
};

/* A barrier posted on the shrunken (single-survivor) team must complete.
   This verifies that ucc_context_shrink produces a functional UCC context,
   not just that the state machine transitions completed without error.
   Note: shrinking to a single survivor avoids concurrent multi-threaded
   context creation in the gtest's shared-address-space "processes", which
   can cause UCX worker conflicts.  Multi-proc collective correctness on a
   shrunken team is covered by the MPI test (test_shrink_allreduce). */
UCC_TEST_F(test_resilience_shrink, barrier_completes_after_shrink)
{
    const int        n_survivors = 1; /* only rank 0 survives */
    const ucc_rank_t failed_rank = static_cast<ucc_rank_t>(n_procs - 1);

    ucc_context_mark_rank_failed(
        reinterpret_cast<ucc_context_t *>(job->procs[0]->ctx_h), failed_rank);

    abort_all();
    wait_abort_all();
    recover_all();

    shrink_survivors(n_survivors);

    auto team = job->create_team(n_survivors);

    ucc_coll_args_t args = {};
    args.coll_type = UCC_COLL_TYPE_BARRIER;
    UccReq req(team, &args);
    req.start();
    EXPECT_EQ(UCC_OK, req.wait())
        << "barrier failed on shrunken single-survivor team";
}

/* Allreduce SUM on the shrunken single-survivor context.  The survivor
   contributes value 1; expected result = 1.  A non-trivial value check
   confirms the collective path is live, not just that it returned UCC_OK. */
UCC_TEST_F(test_resilience_shrink, allreduce_data_correct_after_shrink)
{
    const int        n_survivors = 1; /* only rank 0 survives */
    const ucc_rank_t failed_rank = static_cast<ucc_rank_t>(n_procs - 1);

    ucc_context_mark_rank_failed(
        reinterpret_cast<ucc_context_t *>(job->procs[0]->ctx_h), failed_rank);

    abort_all();
    wait_abort_all();
    recover_all();

    shrink_survivors(n_survivors);

    auto team = job->create_team(n_survivors);
    run_allreduce_check(team);
}
