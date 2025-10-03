/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 */

#include <iostream>
#include <mpi.h>
#include <ucc/api/ucc.h>
#include "core/ucc_resilience.h"

int main(int argc, char *argv[])
{
    int rank, size;
    ucc_context_h ctx;
    ucc_context_params_t ctx_params;
    ucc_lib_config_h lib_config;
    ucc_lib_h lib;
    ucc_status_t status;
    
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    if (rank == 0) {
        std::cout << "Testing UCC resilience with " << size << " MPI processes" << std::endl;
    }
    
    // Initialize UCC library
    ucc_lib_params_t lib_params = {
        .mask = UCC_LIB_PARAM_FIELD_THREAD_MODE,
        .thread_mode = UCC_THREAD_SINGLE,
    };
    
    status = ucc_lib_config_read(NULL, NULL, &lib_config);
    if (status != UCC_OK) {
        std::cerr << "Failed to read UCC lib config" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    status = ucc_init(&lib_params, lib_config, &lib);
    if (status != UCC_OK) {
        std::cerr << "Failed to initialize UCC library" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    ucc_lib_config_release(lib_config);
    
    // Initialize context with OOB
    ucc_context_config_h ctx_config;
    status = ucc_context_config_read(lib, NULL, &ctx_config);
    if (status != UCC_OK) {
        std::cerr << "Failed to read context config" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    memset(&ctx_params, 0, sizeof(ctx_params));
    ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_OOB;
    ctx_params.oob.allgather = [](void *sbuf, void *rbuf, size_t msglen, void *coll_ctx, void **req) -> ucc_status_t {
        MPI_Request *mpi_req = new MPI_Request;
        int ret = MPI_Iallgather(sbuf, msglen, MPI_BYTE, rbuf, msglen, MPI_BYTE, MPI_COMM_WORLD, mpi_req);
        *req = mpi_req;
        return (ret == MPI_SUCCESS) ? UCC_OK : UCC_ERR_NO_MESSAGE;
    };
    ctx_params.oob.req_test = [](void *req) -> ucc_status_t {
        MPI_Request *mpi_req = (MPI_Request*)req;
        int flag;
        MPI_Test(mpi_req, &flag, MPI_STATUS_IGNORE);
        return flag ? UCC_OK : UCC_INPROGRESS;
    };
    ctx_params.oob.req_free = [](void *req) -> ucc_status_t {
        delete (MPI_Request*)req;
        return UCC_OK;
    };
    ctx_params.oob.coll_info = (void*)(uintptr_t)MPI_COMM_WORLD;
    ctx_params.oob.n_oob_eps = size;
    ctx_params.oob.oob_ep = rank;
    
    status = ucc_context_create(lib, &ctx_params, ctx_config, &ctx);
    if (status != UCC_OK) {
        std::cerr << "Failed to create UCC context" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    ucc_context_config_release(ctx_config);
    
    if (rank == 0) {
        std::cout << "UCC context created successfully" << std::endl;
    }
    
    // Test service team failure detection
    if (rank == 0) {
        std::cout << "\nTesting service team failure detection..." << std::endl;
    }
    
    uint64_t *alive_mask = NULL;
    uint64_t *failed_ranks = NULL;
    int num_failed = 0;
    
    status = ucc_service_team_failure_detection(ctx, &alive_mask, &failed_ranks, &num_failed);
    
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
    
    // Test hybrid failure detection
    if (rank == 0) {
        std::cout << "\nTesting hybrid failure detection..." << std::endl;
    }
    
    status = ucc_hybrid_failure_detection(ctx, &alive_mask, &failed_ranks, &num_failed);
    
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
    
    // Cleanup
    ucc_context_destroy(ctx);
    ucc_finalize(lib);
    
    if (rank == 0) {
        std::cout << "\nTest completed successfully!" << std::endl;
    }
    
    MPI_Finalize();
    return 0;
}
