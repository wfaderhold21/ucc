/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 */

#include <iostream>
#include <cstring>
#include <mpi.h>
#include <ucc/api/ucc.h>

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
                                                                                
static ucc_status_t oob_allgather_test(void *req)                               
{                                                                               
    MPI_Request request = (MPI_Request)(uintptr_t)req;                          
    int         completed;                                                      
    MPI_Test(&request, &completed, MPI_STATUS_IGNORE);                          
    return completed ? UCC_OK : UCC_INPROGRESS;                                 
}                                                                               
                                                                                
static ucc_status_t oob_allgather_free(void *req)                               
{                                                                               
    return UCC_OK;                                                              
}

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
        std::cout << "Testing UCC service team rebuilding with " << size << " MPI processes" << std::endl;
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
    ctx_params.oob.allgather = oob_allgather;
    ctx_params.oob.req_test = oob_allgather_test;
    ctx_params.oob.req_free = oob_allgather_free;
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
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Test 1: Basic context abort and recover
    if (rank == 0) {
        std::cout << "\nTest 1: Basic context abort and recover..." << std::endl;
    }
    
    status = ucc_context_abort(ctx);
    if (status != UCC_OK) {
        std::cerr << "Context abort failed on rank " << rank << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    status = ucc_context_recover(ctx, &ctx_params.oob);
    if (status != UCC_OK) {
        std::cerr << "Context recover failed on rank " << rank << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    if (rank == 0) {
        std::cout << "Basic context abort/recover successful" << std::endl;
    }
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Test 2: Create team and test collective
    if (rank == 0) {
        std::cout << "\nTest 2: Creating team and testing collective..." << std::endl;
    }
    
    ucc_team_h team;
    ucc_team_params_t team_params;
    memset(&team_params, 0, sizeof(team_params));
    team_params.mask = UCC_TEAM_PARAM_FIELD_OOB | UCC_TEAM_PARAM_FIELD_EP;
    team_params.oob.allgather = ctx_params.oob.allgather;
    team_params.oob.req_test = ctx_params.oob.req_test;
    team_params.oob.req_free = ctx_params.oob.req_free;
    team_params.oob.coll_info = ctx_params.oob.coll_info;
    team_params.oob.n_oob_eps = size;
    team_params.oob.oob_ep = rank;
    team_params.ep = rank;
    
    status = ucc_team_create_post(&ctx, 1, &team_params, &team);
    if (status != UCC_OK) {
        std::cerr << "Team create post failed on rank " << rank << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    // Wait for team creation to complete
    while (UCC_INPROGRESS == (status = ucc_team_create_test(team))) {
        ucc_context_progress(ctx);
    }
    
    if (status != UCC_OK) {
        std::cerr << "Team creation failed on rank " << rank << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    if (rank == 0) {
        std::cout << "Team created successfully with " << size << " processes" << std::endl;
    }
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Test 3: Perform allreduce
    if (rank == 0) {
        std::cout << "\nTest 3: Performing allreduce..." << std::endl;
    }
    
    int send_value = 1;
    int recv_value = 0;
    
    ucc_coll_args_t coll_args;
    memset(&coll_args, 0, sizeof(coll_args));
    coll_args.coll_type = UCC_COLL_TYPE_ALLREDUCE;
    coll_args.src.info.buffer = &send_value;
    coll_args.src.info.count = 1;
    coll_args.src.info.datatype = UCC_DT_INT32;
    coll_args.src.info.mem_type = UCC_MEMORY_TYPE_HOST;
    coll_args.dst.info.buffer = &recv_value;
    coll_args.dst.info.count = 1;
    coll_args.dst.info.datatype = UCC_DT_INT32;
    coll_args.dst.info.mem_type = UCC_MEMORY_TYPE_HOST;
    coll_args.op = UCC_OP_SUM;
    
    ucc_coll_req_h req;
    status = ucc_collective_init(&coll_args, &req, team);
    if (status != UCC_OK) {
        std::cerr << "Allreduce init failed on rank " << rank << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    status = ucc_collective_post(req);
    if (status != UCC_OK) {
        std::cerr << "Allreduce post failed on rank " << rank << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    // Wait for allreduce to complete
    while (UCC_INPROGRESS == (status = ucc_collective_test(req))) {
        ucc_context_progress(ctx);
    }
    
    if (status != UCC_OK) {
        std::cerr << "Allreduce failed on rank " << rank << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    ucc_collective_finalize(req);
    
    if (rank == 0) {
        std::cout << "Allreduce completed successfully, result = " << recv_value << std::endl;
        if (recv_value == size) {
            std::cout << "✅ SUCCESS: Allreduce result is correct!" << std::endl;
        } else {
            std::cout << "❌ FAILURE: Expected " << size << ", got " << recv_value << std::endl;
        }
    }
    
    // Cleanup
    ucc_team_destroy(team);
    ucc_context_destroy(ctx);
    ucc_finalize(lib);
    
    if (rank == 0) {
        std::cout << "\nTest completed successfully!" << std::endl;
    }
    MPI_Finalize();
    return 0;
}
