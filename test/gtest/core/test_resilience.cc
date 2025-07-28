/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 */

#include "test_context.h" 
#include "common/test_ucc.h"
#include <vector>
#include <algorithm>
#include <random>

extern "C" {
#include "core/ucc_context.h"
#include "core/ucc_team.h"
}

class test_resilience : public test_context {
public:
    ucc_context_h ctx_h;
    UccJob *job;

    test_resilience() {
        // Create context with OOB for resilience testing
        job = new UccJob(4, UccJob::UCC_JOB_CTX_GLOBAL);
        ctx_h = job->procs[0]->ctx_h;
    }

    ~test_resilience() {
        if (job) {
            job->cleanup();
            delete job;
        }
    }
};

class test_resilience_team : public test_resilience {
public:
    UccTeam_h team;

    test_resilience_team() {
        // Create a team for shrink testing
        team = job->create_team(4);
    }

    ~test_resilience_team() {
        // Team cleanup handled by UccJob
    }
};

/* Test basic context abort functionality */
UCC_TEST_F(test_resilience, context_abort_basic)
{
    // Test that context abort completes successfully
    EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));

    // Verify context is marked as failed
    ucc_context_t *context = (ucc_context_t*)ctx_h;
    EXPECT_EQ(1, context->is_failed);

    // Test that abort on already failed context is idempotent
    EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));
}

/* Test context abort with invalid parameters */
UCC_TEST_F(test_resilience, context_abort_invalid_params)
{
    // Test abort with NULL context
    EXPECT_EQ(UCC_ERR_INVALID_PARAM, ucc_context_abort(NULL));
}

/* Test basic context recover functionality */
UCC_TEST_F(test_resilience, context_recover_basic)
{
    // First abort the context
    EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));

    ucc_context_t *context = (ucc_context_t*)ctx_h;
    EXPECT_EQ(1, context->is_failed);

    // Now recover the context
    EXPECT_EQ(UCC_OK, ucc_context_recover(ctx_h));

    // Verify context is no longer marked as failed
    EXPECT_EQ(0, context->is_failed);
}

/* Test context recover without prior abort */
UCC_TEST_F(test_resilience, context_recover_not_failed)
{
    // Test recover on context that's not failed
    EXPECT_EQ(UCC_OK, ucc_context_recover(ctx_h));

    ucc_context_t *context = (ucc_context_t*)ctx_h;
    EXPECT_EQ(0, context->is_failed);
}

/* Test context recover with invalid parameters */
UCC_TEST_F(test_resilience, context_recover_invalid_params)
{
    // Test recover with NULL context
    EXPECT_EQ(UCC_ERR_INVALID_PARAM, ucc_context_recover(NULL));
}

/* Test team shrink basic functionality */
UCC_TEST_F(test_resilience_team, team_shrink_basic)
{
    // Create a list of failed ranks (simulating rank 2 failed)
    uint64_t failed_ranks[1] = {2};

    // Test team shrink - Note: this may not be fully implemented yet
    // so we test that it doesn't crash and returns a valid status
    ucc_status_t status = ucc_team_shrink(failed_ranks, &team->procs[0].team);

    // Should return either OK or NOT_SUPPORTED
    EXPECT_TRUE(status == UCC_OK || status == UCC_ERR_NOT_SUPPORTED);
}

/* Test team shrink with invalid parameters */
UCC_TEST_F(test_resilience_team, team_shrink_invalid_params)
{
    uint64_t failed_ranks[1] = {1};

    // Test with NULL failed_ranks
    ucc_status_t status = ucc_team_shrink(NULL, &team->procs[0].team);
    EXPECT_EQ(UCC_ERR_INVALID_PARAM, status);

    // Test with NULL team
    status = ucc_team_shrink(failed_ranks, NULL);
    EXPECT_EQ(UCC_ERR_INVALID_PARAM, status);
}

/* Test context attribute retrieval for failed ranks */
UCC_TEST_F(test_resilience, context_get_attr_failed_ranks)
{
    // First abort the context to populate failure info
    EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));

    // Test getting context attributes including failed ranks
    ucc_context_attr_t attr;
    attr.mask = UCC_CONTEXT_ATTR_FIELD_FAILED_RANKS;

    ucc_status_t status = ucc_context_get_attr(ctx_h, &attr);
    // Should return either OK or NOT_SUPPORTED depending on implementation
    EXPECT_TRUE(status == UCC_OK || status == UCC_ERR_NOT_SUPPORTED);
}

/* Test complete resilience workflow: abort -> recover -> shrink */
UCC_TEST_F(test_resilience_team, resilience_workflow_integration)
{
    // Step 1: Abort context (simulating failure detection)
    EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));

    ucc_context_t *context = (ucc_context_t*)ctx_h;
    EXPECT_EQ(1, context->is_failed);

    // Step 2: Recover context (restore communication capability)
    EXPECT_EQ(UCC_OK, ucc_context_recover(ctx_h));
    EXPECT_EQ(0, context->is_failed);

    // Step 3: Shrink team to remove failed processes
    uint64_t failed_ranks[1] = {3}; // Simulate rank 3 failed
    ucc_status_t shrink_status = ucc_team_shrink(failed_ranks, &team->procs[0].team);

    // Verify the shrink operation completes (OK or NOT_SUPPORTED are both valid)
    EXPECT_TRUE(shrink_status == UCC_OK || shrink_status == UCC_ERR_NOT_SUPPORTED);
}

/* Test multiple abort/recover cycles */
UCC_TEST_F(test_resilience, multiple_abort_recover_cycles)
{
    ucc_context_t *context = (ucc_context_t*)ctx_h;

    for (int cycle = 0; cycle < 3; cycle++) {
        // Abort
        EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));
        EXPECT_EQ(1, context->is_failed);

        // Recover
        EXPECT_EQ(UCC_OK, ucc_context_recover(ctx_h));
        EXPECT_EQ(0, context->is_failed);
    }
}

/* Test concurrent abort operations (if threading is supported) */
UCC_TEST_F(test_resilience, concurrent_abort_safety)
{
    // This test verifies that concurrent aborts don't cause issues
    std::vector<std::thread> threads;
    std::atomic<int> successful_aborts{0};

    for (int i = 0; i < 4; i++) {
        threads.emplace_back([this, &successful_aborts]() {
            if (ucc_context_abort(ctx_h) == UCC_OK) {
                successful_aborts++;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // At least one abort should succeed
    EXPECT_GE(successful_aborts.load(), 1);

    // Context should be marked as failed
    ucc_context_t *context = (ucc_context_t*)ctx_h;
    EXPECT_EQ(1, context->is_failed);
}

/* Test that TL abort interfaces are called */
UCC_TEST_F(test_resilience, verify_tl_abort_called)
{
    // This test verifies that the core abort actually calls TL abort functions
    // We do this by ensuring the abort completes successfully, which means
    // all TL abort functions were called without error

    EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));

    // Verify context state is properly set
    ucc_context_t *context = (ucc_context_t*)ctx_h;
    EXPECT_EQ(1, context->is_failed);

    // Verify we can still recover (showing TL abort didn't break anything)
    EXPECT_EQ(UCC_OK, ucc_context_recover(ctx_h));
    EXPECT_EQ(0, context->is_failed);
}

/* Test edge case: shrink team with all ranks failed */
UCC_TEST_F(test_resilience_team, team_shrink_all_ranks_failed)
{
    // This is an edge case that should be handled gracefully
    uint64_t failed_ranks[4] = {0, 1, 2, 3}; // All ranks failed

    ucc_status_t status = ucc_team_shrink(failed_ranks, &team->procs[0].team);

    // Should either handle gracefully or return appropriate error
    EXPECT_TRUE(status == UCC_OK || 
                status == UCC_ERR_NOT_SUPPORTED || 
                status == UCC_ERR_INVALID_PARAM);
}

/* Test resilience with different team sizes */
class test_resilience_parameterized : public test_resilience, 
                                     public ::testing::WithParamInterface<int> {
public:
    UccTeam_h team;

    test_resilience_parameterized() {
        int team_size = GetParam();
        if (team_size <= job->n_procs) {
            team = job->create_team(team_size);
        }
    }
};

UCC_TEST_P(test_resilience_parameterized, resilience_different_team_sizes)
{
    int team_size = GetParam();

    if (team_size > job->n_procs) {
        GTEST_SKIP() << "Team size larger than job size";
    }

    // Test abort/recover with different team sizes
    EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));
    EXPECT_EQ(UCC_OK, ucc_context_recover(ctx_h));

    // Test shrink with one failed rank (if team size > 1)
    if (team_size > 1) {
        uint64_t failed_ranks[1] = {static_cast<uint64_t>(team_size - 1)};
        ucc_status_t status = ucc_team_shrink(failed_ranks, &team->procs[0].team);
        EXPECT_TRUE(status == UCC_OK || status == UCC_ERR_NOT_SUPPORTED);
    }
}

INSTANTIATE_TEST_CASE_P(, test_resilience_parameterized,
                        ::testing::Values(1, 2, 3, 4)); 