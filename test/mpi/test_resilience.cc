/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 */

#include "test_mpi.h"
#include "mpi_util.h"
BEGIN_C_DECLS
#include "core/ucc_resilience.h"
END_C_DECLS
#include <iostream>

#if 0
class test_resilience_mpi : public UccTestMpi {
public:
    test_resilience_mpi() : UccTestMpi(0, NULL, UCC_THREAD_SINGLE, 0, false) {}
};

/* Test service team failure detection with MPI OOB */
UCC_TEST_F(test_resilience_mpi, service_team_failure_detection)
{
    uint64_t *alive_mask = NULL;
    uint64_t *failed_ranks = NULL;
    int num_failed = 0;
    
    // Test service team failure detection
    ucc_status_t status = ucc_service_team_failure_detection(ctx, &alive_mask, &failed_ranks, &num_failed);
    
    if (rank == 0) {
        std::cout << "Service team failure detection status: " << ucc_status_string(status) << std::endl;
        
        if (status == UCC_OK) {
            std::cout << "Number of failed processes: " << num_failed << std::endl;
            if (alive_mask) {
                std::cout << "Alive mask: ";
                for (int i = 0; i < size; i++) {
                    std::cout << alive_mask[i] << " ";
                }
                std::cout << std::endl;
            }
        }
    }
    
    // Clean up
    if (alive_mask) {
        ucc_free(alive_mask);
    }
    if (failed_ranks) {
        ucc_free(failed_ranks);
    }
    
    return UCC_OK;
}

/* Test hybrid failure detection with MPI OOB */
UCC_TEST_F(test_resilience_mpi, hybrid_failure_detection)
{
    uint64_t *alive_mask = NULL;
    uint64_t *failed_ranks = NULL;
    int num_failed = 0;
    
    // Test hybrid failure detection
    ucc_status_t status = ucc_hybrid_failure_detection(ctx, &alive_mask, &failed_ranks, &num_failed);
    
    if (rank == 0) {
        std::cout << "Hybrid failure detection status: " << ucc_status_string(status) << std::endl;
        
        if (status == UCC_OK) {
            std::cout << "Number of failed processes: " << num_failed << std::endl;
            if (alive_mask) {
                std::cout << "Alive mask: ";
                for (int i = 0; i < size; i++) {
                    std::cout << alive_mask[i] << " ";
                }
                std::cout << std::endl;
            }
        }
    }
    
    // Clean up
    if (alive_mask) {
        ucc_free(alive_mask);
    }
    if (failed_ranks) {
        ucc_free(failed_ranks);
    }
    
    return UCC_OK;
}
#endif