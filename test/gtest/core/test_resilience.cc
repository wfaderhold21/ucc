/**
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 *
 * Phase 1 resilience tests: abort / recover state machine.
 *
 * Test groups:
 *  1. Guard violations  — single exclusive context, no OOB / service team.
 *  2. Abort + recover   — fresh 4-process UccJob with global (OOB) context.
 *  3. Post after abort  — collective_post while ABORTING returns UCC_ERR_ABORTED.
 *  4. Simulated failure — BOR agreement propagates a locally observed failure.
 */
#include "common/test_ucc.h"
#include "common/test.h"
#include "core/test_context.h"

extern "C" {
#include "core/ucc_context.h"
}

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
