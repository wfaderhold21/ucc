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
#include "core/ucc_resilience.h"
}

class test_hybrid_resilience : public test_context {
public:
    ucc_context_h ctx_h;
    UccJob *job;

    test_hybrid_resilience() {
        // Create context with OOB for resilience testing
        job = new UccJob(4, UccJob::UCC_JOB_CTX_GLOBAL);
        ctx_h = job->procs[0]->ctx_h;
    }

    ~test_hybrid_resilience() {
        if (job) {
            job->cleanup();
            delete job;
        }
    }
};

/* Test setting and getting failure detection method */
UCC_TEST_F(test_hybrid_resilience, set_get_failure_detection_method)
{
    uint32_t method;
    
    // Test getting default method (should be hybrid)
    EXPECT_EQ(UCC_OK, ucc_get_failure_detection_method(ctx_h, &method));
    EXPECT_EQ(UCC_FAILURE_DETECTION_HYBRID, method);
    
    // Test setting to sockets only
    EXPECT_EQ(UCC_OK, ucc_set_failure_detection_method(ctx_h, UCC_FAILURE_DETECTION_SOCKETS));
    EXPECT_EQ(UCC_OK, ucc_get_failure_detection_method(ctx_h, &method));
    EXPECT_EQ(UCC_FAILURE_DETECTION_SOCKETS, method);
    
    // Test setting to service team only
    EXPECT_EQ(UCC_OK, ucc_set_failure_detection_method(ctx_h, UCC_FAILURE_DETECTION_SERVICE));
    EXPECT_EQ(UCC_OK, ucc_get_failure_detection_method(ctx_h, &method));
    EXPECT_EQ(UCC_FAILURE_DETECTION_SERVICE, method);
    
    // Test setting back to hybrid
    EXPECT_EQ(UCC_OK, ucc_set_failure_detection_method(ctx_h, UCC_FAILURE_DETECTION_HYBRID));
    EXPECT_EQ(UCC_OK, ucc_get_failure_detection_method(ctx_h, &method));
    EXPECT_EQ(UCC_FAILURE_DETECTION_HYBRID, method);
}

/* Test setting invalid failure detection method */
UCC_TEST_F(test_hybrid_resilience, set_invalid_failure_detection_method)
{
    // Test setting invalid method
    EXPECT_EQ(UCC_ERR_INVALID_PARAM, ucc_set_failure_detection_method(ctx_h, 99));
    
    // Test setting with NULL context
    EXPECT_EQ(UCC_ERR_INVALID_PARAM, ucc_set_failure_detection_method(NULL, UCC_FAILURE_DETECTION_SOCKETS));
}

/* Test getting failure detection method with invalid parameters */
UCC_TEST_F(test_hybrid_resilience, get_failure_detection_method_invalid_params)
{
    uint32_t method;
    
    // Test getting with NULL context
    EXPECT_EQ(UCC_ERR_INVALID_PARAM, ucc_get_failure_detection_method(NULL, &method));
    
    // Test getting with NULL method pointer
    EXPECT_EQ(UCC_ERR_INVALID_PARAM, ucc_get_failure_detection_method(ctx_h, NULL));
}

/* Test context abort with different failure detection methods */
UCC_TEST_F(test_hybrid_resilience, context_abort_with_different_methods)
{
    ucc_context_t *context = (ucc_context_t*)ctx_h;
    
    // Test with hybrid method (default)
    EXPECT_EQ(UCC_OK, ucc_set_failure_detection_method(ctx_h, UCC_FAILURE_DETECTION_HYBRID));
    EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));
    EXPECT_EQ(1, context->is_failed);
    
    // Recover and test with sockets only
    EXPECT_EQ(UCC_OK, ucc_context_recover(ctx_h, &context->params.oob));
    EXPECT_EQ(0, context->is_failed);
    
    EXPECT_EQ(UCC_OK, ucc_set_failure_detection_method(ctx_h, UCC_FAILURE_DETECTION_SOCKETS));
    EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));
    EXPECT_EQ(1, context->is_failed);
    
    // Recover and test with service team only
    EXPECT_EQ(UCC_OK, ucc_context_recover(ctx_h, &context->params.oob));
    EXPECT_EQ(0, context->is_failed);
    
    EXPECT_EQ(UCC_OK, ucc_set_failure_detection_method(ctx_h, UCC_FAILURE_DETECTION_SERVICE));
    EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));
    EXPECT_EQ(1, context->is_failed);
}

/* Test that the failure detection method persists across abort/recover cycles */
UCC_TEST_F(test_hybrid_resilience, failure_detection_method_persistence)
{
    ucc_context_t *context = (ucc_context_t*)ctx_h;
    uint32_t method;
    
    // Set to sockets only
    EXPECT_EQ(UCC_OK, ucc_set_failure_detection_method(ctx_h, UCC_FAILURE_DETECTION_SOCKETS));
    
    // Abort and recover multiple times
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(UCC_OK, ucc_context_abort(ctx_h));
        EXPECT_EQ(1, context->is_failed);
        
        EXPECT_EQ(UCC_OK, ucc_context_recover(ctx_h, &context->params.oob));
        EXPECT_EQ(0, context->is_failed);
        
        // Verify method is still set
        EXPECT_EQ(UCC_OK, ucc_get_failure_detection_method(ctx_h, &method));
        EXPECT_EQ(UCC_FAILURE_DETECTION_SOCKETS, method);
    }
}

/* Test hybrid failure detection function directly */
UCC_TEST_F(test_hybrid_resilience, hybrid_failure_detection_function)
{
    uint64_t *alive_mask = NULL;
    uint64_t *failed_ranks = NULL;
    int num_failed = 0;
    
    // Test hybrid failure detection (should work even without failures)
    EXPECT_EQ(UCC_OK, ucc_hybrid_failure_detection(ctx_h, &alive_mask, &failed_ranks, &num_failed));
    
    // Should have no failures in normal operation
    EXPECT_EQ(0, num_failed);
    EXPECT_NE((uint64_t*)NULL, alive_mask);
    EXPECT_EQ((uint64_t*)NULL, failed_ranks);
    
    // Clean up
    if (alive_mask) {
        ucc_free(alive_mask);
    }
}

/* Test service team failure detection function directly */
UCC_TEST_F(test_hybrid_resilience, service_team_failure_detection_function)
{
    uint64_t *alive_mask = NULL;
    uint64_t *failed_ranks = NULL;
    int num_failed = 0;
    
    // Test service team failure detection
    ucc_status_t status = ucc_service_team_failure_detection(ctx_h, &alive_mask, &failed_ranks, &num_failed);
    
    // This may succeed or fail depending on whether service team is available
    // Both outcomes are valid
    if (status == UCC_OK) {
        EXPECT_NE((uint64_t*)NULL, alive_mask);
        // Should have no failures in normal operation
        EXPECT_EQ(0, num_failed);
    } else {
        // If service team is not available, should get NOT_SUPPORTED
        EXPECT_EQ(UCC_ERR_NOT_SUPPORTED, status);
    }
    
    // Clean up
    if (alive_mask) {
        ucc_free(alive_mask);
    }
    if (failed_ranks) {
        ucc_free(failed_ranks);
    }
}

/* Test that failure detection method is properly initialized */
UCC_TEST_F(test_hybrid_resilience, failure_detection_method_initialization)
{
    ucc_context_t *context = (ucc_context_t*)ctx_h;
    
    // The default method should be hybrid
    EXPECT_EQ(UCC_FAILURE_DETECTION_HYBRID, context->failure_detection_method);
}

/* Test edge case: multiple rapid method changes */
UCC_TEST_F(test_hybrid_resilience, rapid_method_changes)
{
    uint32_t method;
    
    // Rapidly change methods
    for (int i = 0; i < 10; i++) {
        uint32_t test_method = i % 3;
        EXPECT_EQ(UCC_OK, ucc_set_failure_detection_method(ctx_h, test_method));
        EXPECT_EQ(UCC_OK, ucc_get_failure_detection_method(ctx_h, &method));
        EXPECT_EQ(test_method, method);
    }
    
    // Final method should be 1 (sockets)
    EXPECT_EQ(UCC_OK, ucc_get_failure_detection_method(ctx_h, &method));
    EXPECT_EQ(1, method);
}
