/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 */

#include <iostream>
#include <cstring>
#include <mpi.h>
#include <ucc/api/ucc.h>
#include <unistd.h>

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
        std::cout << "Testing UCC real failure simulation with " << size << " MPI processes" << std::endl;
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
    
    // Test 1: Create team and perform collective
    if (rank == 0) {
        std::cout << "\nTest 1: Creating team and performing allreduce..." << std::endl;
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
    
    // Perform initial allreduce
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
        std::cout << "Initial allreduce completed successfully, result = " << recv_value << std::endl;
    }
    MPI_Barrier(MPI_COMM_WORLD);
    
    // Test 2: Simulate real failure - rank 3 exits
    if (rank == 3) {
        if (rank == 3) {
            std::cout << "\nTest 2: Rank 3 simulating failure by exiting..." << std::endl;
        }
        // Simulate a real failure by exiting
        MPI_Barrier(MPI_COMM_WORLD);
        MPI_Finalize();
        exit(0);
    }
    
    // Ranks 0, 1, 2 continue
    if (rank == 0) {
        std::cout << "\nTest 2: Ranks 0, 1, 2 continuing after rank 3 failure..." << std::endl;
    }
    
    // Test 3: Call context abort to trigger failure detection and service team rebuild
    if (rank == 0) {
        std::cout << "\nTest 3: Calling context abort to detect failure and rebuild service team..." << std::endl;
    }
    
    status = ucc_context_abort(ctx);
    if (status != UCC_OK) {
        std::cerr << "Context abort failed on rank " << rank << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    if (rank == 0) {
        std::cout << "Context abort completed" << std::endl;
        std::cout << "Note: Context OOB parameters should be updated internally to n_oob_eps=3, oob_ep=0,1,2" << std::endl;
    }
    
    // Test 5: Create new MPI communicator for ranks 0-2
    if (rank == 0) {
        std::cout << "\nTest 5: Creating new MPI communicator for ranks 0-2..." << std::endl;
    }
    
    MPI_Comm new_comm;
    MPI_Group world_group, new_group;
    int ranks[3] = {0, 1, 2}; // Only ranks 0-2
    
    MPI_Comm_group(MPI_COMM_WORLD, &world_group);
    MPI_Group_incl(world_group, 3, ranks, &new_group);
    MPI_Comm_create_group(MPI_COMM_WORLD, new_group, 0, &new_comm);
    MPI_Comm_rank(new_comm, &rank);
    MPI_Comm_size(new_comm, &size);
    MPI_Barrier(new_comm);
    ctx_params.oob.coll_info = (void*)(uintptr_t)new_comm;
    ctx_params.oob.n_oob_eps = size;
    ctx_params.oob.oob_ep = rank;
    

    // Test 4: Call context recover
    if (rank == 0) {
        std::cout << "\nTest 4: Calling context recover..." << std::endl;
    }
    
    status = ucc_context_recover(ctx, &ctx_params.oob);
    if (status != UCC_OK) {
        std::cerr << "Context recover failed on rank " << rank << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    
    if (rank == 0) {
        std::cout << "Context recover completed" << std::endl;
    }
    
    MPI_Barrier(new_comm);
    sleep(2);
    
    if (rank < 3) {
        if (rank == 0) {
            std::cout << "New MPI communicator created successfully for ranks 0-2" << std::endl;
        }
        
        // Test 6: Reuse aborted context for ranks 0-2
        if (rank == 0) {
            std::cout << "\nTest 6: Reusing aborted context for ranks 0-2..." << std::endl;
        }
        
        // Use the original aborted context
        ucc_context_h new_ctx = ctx; // Reuse the aborted context
        
        if (rank == 0) {
            std::cout << "Reusing aborted UCC context for ranks 0-2" << std::endl;
        }
        MPI_Barrier(new_comm);
        
        // Test 7: Create new team on the aborted context for ranks 0-2
        if (rank == 0) {
            std::cout << "\nTest 7: Creating new team on aborted context for ranks 0-2..." << std::endl;
        }
        
        ucc_team_h new_team;
        ucc_team_params_t new_team_params;
        memset(&new_team_params, 0, sizeof(new_team_params));
        new_team_params.mask = UCC_TEAM_PARAM_FIELD_OOB | UCC_TEAM_PARAM_FIELD_EP;
        
        // Use the context's updated OOB parameters (after abort/recover)
        // The context should have updated n_oob_eps to 3 and oob_ep to 0,1,2
        // But we need to use the original MPI communicator since that's what the context expects
        new_team_params.oob.allgather = oob_allgather;
        new_team_params.oob.req_test = oob_allgather_test;
        new_team_params.oob.req_free = oob_allgather_free;
        new_team_params.oob.coll_info = (void*)(uintptr_t)new_comm; // Use original MPI communicator
        new_team_params.oob.n_oob_eps = size; // Should match context's updated n_oob_eps
        new_team_params.oob.oob_ep = rank; // Should match context's updated oob_ep
        new_team_params.ep = rank; // Our local rank
        
        if (rank == 0) {
            std::cout << "Team params: n_oob_eps=" << new_team_params.oob.n_oob_eps 
                      << ", oob_ep=" << new_team_params.oob.oob_ep 
                      << ", ep=" << new_team_params.ep << std::endl;
        }
        
        status = ucc_team_create_post(&ctx, 1, &new_team_params, &new_team);
        if (status != UCC_OK) {
            std::cerr << "New team create post failed on rank " << rank << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        
        // Wait for team creation to complete
        while (UCC_INPROGRESS == (status = ucc_team_create_test(new_team))) {
            ucc_context_progress(ctx);
        }
        
        if (status != UCC_OK) {
            std::cerr << "New team creation failed on rank " << rank << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        
        if (rank == 0) {
            std::cout << "New team created successfully with 3 processes (ranks 0-2)" << std::endl;
        }
        MPI_Barrier(new_comm);
        
        // Test 8: Perform allreduce on ranks 0-2
        if (rank == 0) {
            std::cout << "\nTest 8: Performing allreduce on ranks 0-2..." << std::endl;
        }
        
        int new_send_value = rank + 1; // Different value for each rank (1, 2, 3)
        int new_recv_value = 0;
        
        ucc_coll_args_t new_coll_args;
        memset(&new_coll_args, 0, sizeof(new_coll_args));
        new_coll_args.coll_type = UCC_COLL_TYPE_ALLREDUCE;
        new_coll_args.src.info.buffer = &new_send_value;
        new_coll_args.src.info.count = 1;
        new_coll_args.src.info.datatype = UCC_DT_INT32;
        new_coll_args.src.info.mem_type = UCC_MEMORY_TYPE_HOST;
        new_coll_args.dst.info.buffer = &new_recv_value;
        new_coll_args.dst.info.count = 1;
        new_coll_args.dst.info.datatype = UCC_DT_INT32;
        new_coll_args.dst.info.mem_type = UCC_MEMORY_TYPE_HOST;
        new_coll_args.op = UCC_OP_SUM;
        
        ucc_coll_req_h new_req;
        status = ucc_collective_init(&new_coll_args, &new_req, new_team);
        if (status != UCC_OK) {
            std::cerr << "New allreduce init failed on rank " << rank << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        
        status = ucc_collective_post(new_req);
        if (status != UCC_OK) {
            std::cerr << "New allreduce post failed on rank " << rank << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        
        // Wait for allreduce to complete
        while (UCC_INPROGRESS == (status = ucc_collective_test(new_req))) {
            ucc_context_progress(new_ctx);
        }
        
        if (status != UCC_OK) {
            std::cerr << "New allreduce failed on rank " << rank << std::endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        
        ucc_collective_finalize(new_req);
        
        if (rank == 0) {
            std::cout << "✅ SUCCESS: New allreduce completed successfully, result = " << new_recv_value << " (expected: 6)" << std::endl;
        }
        MPI_Barrier(new_comm);
        
        // Cleanup
        ucc_team_destroy(new_team);
        MPI_Comm_free(&new_comm);
    }
    
    // Cleanup original resources
    ucc_team_destroy(team);
    ucc_context_destroy(ctx);
    ucc_finalize(lib);
    
    if (rank == 0) {
        std::cout << "\nTest completed successfully!" << std::endl;
    }
    MPI_Finalize();
    
    return 0;
}
