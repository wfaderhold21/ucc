/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

/**
 * Test for the OpenCL MC/EC plugins with collective operations on OpenCL memory.
 *
 * Build: make test UCC_SRC=/path/to/ucc
 * Run:   UCC_PLUGIN_COMPONENT_PATH=. ./test_opencl_plugin
 */

#include <CL/cl.h>
#include <ucc/api/ucc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define TEST_SIZE   64
#define TEAM_SIZE   1   /* Single process test */

static int test_count = 0;
static int pass_count = 0;

#define TEST(name) \
    do { \
        test_count++; \
        printf("TEST: %s ... ", name); \
        fflush(stdout); \
    } while(0)

#define PASS() \
    do { \
        pass_count++; \
        printf("PASS\n"); \
    } while(0)

#define FAIL(msg) \
    do { \
        printf("FAIL: %s\n", msg); \
    } while(0)

#define CHECK(cond, msg) \
    do { \
        if (!(cond)) { \
            FAIL(msg); \
            return -1; \
        } \
    } while(0)

#define CHECK_UCC(status, msg) \
    do { \
        if ((status) != UCC_OK) { \
            printf("FAIL: %s (status=%d)\n", msg, (int)(status)); \
            return -1; \
        } \
    } while(0)

#define CHECK_CL(err, msg) \
    do { \
        if ((err) != CL_SUCCESS) { \
            printf("FAIL: %s (OpenCL error=%d)\n", msg, (int)(err)); \
            return -1; \
        } \
    } while(0)

/* OOB allgather for single process - just copy data */
static ucc_status_t oob_allgather(void *sbuf, void *rbuf, size_t msglen,
                                   void *coll_info, void **request)
{
    (void)coll_info;
    memcpy(rbuf, sbuf, msglen);
    *request = (void*)1; /* Non-null to indicate completion */
    return UCC_OK;
}

static ucc_status_t oob_allgather_test(void *request)
{
    (void)request;
    return UCC_OK;
}

static ucc_status_t oob_allgather_free(void *request)
{
    (void)request;
    return UCC_OK;
}

/* OpenCL globals */
static cl_context       cl_ctx   = NULL;
static cl_command_queue cl_queue = NULL;
static cl_device_id     cl_dev   = 0;

static int init_opencl(void)
{
    cl_platform_id platform;
    cl_uint        num_platforms, num_devices;
    cl_int         err;
    char           dev_name[128];

    err = clGetPlatformIDs(1, &platform, &num_platforms);
    if (err != CL_SUCCESS || num_platforms == 0) {
        printf("No OpenCL platforms found\n");
        return -1;
    }

    err = clGetDeviceIDs(platform, CL_DEVICE_TYPE_GPU, 1, &cl_dev, &num_devices);
    if (err != CL_SUCCESS || num_devices == 0) {
        /* Try any device */
        err = clGetDeviceIDs(platform, CL_DEVICE_TYPE_ALL, 1, &cl_dev, &num_devices);
        if (err != CL_SUCCESS || num_devices == 0) {
            printf("No OpenCL devices found\n");
            return -1;
        }
    }

    clGetDeviceInfo(cl_dev, CL_DEVICE_NAME, sizeof(dev_name), dev_name, NULL);
    printf("Using OpenCL device: %s\n", dev_name);

    cl_ctx = clCreateContext(NULL, 1, &cl_dev, NULL, NULL, &err);
    if (err != CL_SUCCESS) {
        printf("Failed to create OpenCL context\n");
        return -1;
    }

    cl_queue = clCreateCommandQueue(cl_ctx, cl_dev, 0, &err);
    if (err != CL_SUCCESS) {
        printf("Failed to create OpenCL command queue\n");
        clReleaseContext(cl_ctx);
        return -1;
    }

    return 0;
}

static void cleanup_opencl(void)
{
    if (cl_queue) {
        clReleaseCommandQueue(cl_queue);
        cl_queue = NULL;
    }
    if (cl_ctx) {
        clReleaseContext(cl_ctx);
        cl_ctx = NULL;
    }
}

int main(int argc, char **argv)
{
    ucc_lib_config_h     lib_config;
    ucc_lib_h            lib;
    ucc_lib_params_t     lib_params;
    ucc_context_config_h ctx_config;
    ucc_context_h        ctx;
    ucc_context_params_t ctx_params;
    ucc_team_h           team;
    ucc_team_params_t    team_params;
    ucc_coll_req_h       req;
    ucc_coll_args_t      coll_args;
    ucc_status_t         status;

    /* OpenCL buffers */
    cl_mem   cl_send_buf = NULL;
    cl_mem   cl_recv_buf = NULL;
    cl_int   cl_err;

    /* Host staging buffers */
    int     *host_send = NULL;
    int     *host_recv = NULL;
    size_t   buf_size  = TEST_SIZE * TEAM_SIZE * sizeof(int);
    int      i;

    /* Memory type for OpenCL - plugins get types >= UCC_MEMORY_TYPE_LAST */
    ucc_memory_type_t opencl_mem_type = (ucc_memory_type_t)(UCC_MEMORY_TYPE_LAST + 1);

    (void)argc;
    (void)argv;


    MPI_Init(&argc, &argv);
    printf("=== OpenCL Plugin Test with Alltoall on OpenCL Memory ===\n\n");

    /* Check environment */
    if (!getenv("UCC_PLUGIN_COMPONENT_PATH")) {
        printf("WARNING: UCC_PLUGIN_COMPONENT_PATH not set.\n");
        printf("         Set it to the directory containing libucc_mc_opencl.so\n");
        printf("         Example: UCC_PLUGIN_COMPONENT_PATH=. ./test_opencl_plugin\n\n");
    }

    /*
     * Test 1: Initialize OpenCL
     */
    TEST("OpenCL initialization");
    CHECK(init_opencl() == 0, "OpenCL initialization failed");
    PASS();

    /*
     * Test 2: Initialize UCC library
     */
    TEST("UCC library initialization");

    status = ucc_lib_config_read(NULL, NULL, &lib_config);
    CHECK_UCC(status, "ucc_lib_config_read failed");

    lib_params.mask        = UCC_LIB_PARAM_FIELD_THREAD_MODE;
    lib_params.thread_mode = UCC_THREAD_SINGLE;

    status = ucc_init(&lib_params, lib_config, &lib);
    CHECK_UCC(status, "ucc_init failed");

    ucc_lib_config_release(lib_config);
    PASS();

    /*
     * Test 3: Create context
     */
    TEST("UCC context creation");

    status = ucc_context_config_read(lib, NULL, &ctx_config);
    CHECK_UCC(status, "ucc_context_config_read failed");

    ctx_params.mask          = UCC_CONTEXT_PARAM_FIELD_OOB;
    ctx_params.oob.allgather = oob_allgather;
    ctx_params.oob.req_test  = oob_allgather_test;
    ctx_params.oob.req_free  = oob_allgather_free;
    ctx_params.oob.coll_info = NULL;
    ctx_params.oob.n_oob_eps = TEAM_SIZE;
    ctx_params.oob.oob_ep    = 0;

    status = ucc_context_create(lib, &ctx_params, ctx_config, &ctx);
    ucc_context_config_release(ctx_config);
    CHECK_UCC(status, "ucc_context_create failed");

    PASS();

    /*
     * Test 4: Create team
     */
    TEST("UCC team creation");

    team_params.mask          = UCC_TEAM_PARAM_FIELD_OOB |
                                UCC_TEAM_PARAM_FIELD_EP |
                                UCC_TEAM_PARAM_FIELD_EP_RANGE;
    team_params.oob.allgather = oob_allgather;
    team_params.oob.req_test  = oob_allgather_test;
    team_params.oob.req_free  = oob_allgather_free;
    team_params.oob.coll_info = NULL;
    team_params.oob.n_oob_eps = TEAM_SIZE;
    team_params.oob.oob_ep    = 0;
    team_params.ep            = 0;
    team_params.ep_range      = UCC_COLLECTIVE_EP_RANGE_CONTIG;

    status = ucc_team_create_post(&ctx, 1, &team_params, &team);
    CHECK_UCC(status, "ucc_team_create_post failed");

    /* Progress team creation */
    while ((status = ucc_team_create_test(team)) == UCC_INPROGRESS) {
        status = ucc_context_progress(ctx);
        if (status != UCC_OK && status != UCC_INPROGRESS) {
            CHECK_UCC(status, "ucc_context_progress failed during team create");
        }
    }
    CHECK_UCC(status, "ucc_team_create_test failed");

    PASS();

    /*
     * Test 5: Allocate OpenCL buffers and run Alltoall
     */
    TEST("Alltoall collective (OpenCL memory)");

    /* Allocate host staging buffers */
    host_send = (int *)malloc(buf_size);
    host_recv = (int *)malloc(buf_size);
    CHECK(host_send && host_recv, "host buffer allocation failed");

    /* Initialize send data */
    for (i = 0; i < TEST_SIZE * TEAM_SIZE; i++) {
        host_send[i] = i + 1;
        host_recv[i] = 0;
    }

    /* Create OpenCL buffers */
    cl_send_buf = clCreateBuffer(cl_ctx, CL_MEM_READ_WRITE, buf_size, NULL, &cl_err);
    CHECK_CL(cl_err, "clCreateBuffer for send buffer failed");

    cl_recv_buf = clCreateBuffer(cl_ctx, CL_MEM_READ_WRITE, buf_size, NULL, &cl_err);
    CHECK_CL(cl_err, "clCreateBuffer for recv buffer failed");

    /* Copy send data to OpenCL buffer */
    cl_err = clEnqueueWriteBuffer(cl_queue, cl_send_buf, CL_TRUE, 0, buf_size,
                                   host_send, 0, NULL, NULL);
    CHECK_CL(cl_err, "clEnqueueWriteBuffer failed");

    /* Initialize recv buffer to zeros */
    cl_err = clEnqueueWriteBuffer(cl_queue, cl_recv_buf, CL_TRUE, 0, buf_size,
                                   host_recv, 0, NULL, NULL);
    CHECK_CL(cl_err, "clEnqueueWriteBuffer for recv failed");

    clFinish(cl_queue);

    printf("\n    Using OpenCL memory type: %d\n    ", (int)opencl_mem_type);
    fflush(stdout);

    /* Setup alltoall args with OpenCL buffers */
    memset(&coll_args, 0, sizeof(coll_args));
    coll_args.mask              = UCC_COLL_ARGS_FIELD_FLAGS;
    coll_args.flags             = 0;
    coll_args.coll_type         = UCC_COLL_TYPE_ALLTOALL;
    coll_args.src.info.buffer   = (void *)cl_send_buf;  /* OpenCL cl_mem handle */
    coll_args.src.info.count    = TEST_SIZE;
    coll_args.src.info.datatype = UCC_DT_INT32;
    coll_args.src.info.mem_type = opencl_mem_type;      /* OpenCL memory type */
    coll_args.dst.info.buffer   = (void *)cl_recv_buf;
    coll_args.dst.info.count    = TEST_SIZE;
    coll_args.dst.info.datatype = UCC_DT_INT32;
    coll_args.dst.info.mem_type = opencl_mem_type;

    /* Post collective */
    status = ucc_collective_init(&coll_args, &req, team);
    CHECK_UCC(status, "ucc_collective_init failed");

    status = ucc_collective_post(req);
    CHECK_UCC(status, "ucc_collective_post failed");

    /* Progress until complete */
    while ((status = ucc_collective_test(req)) == UCC_INPROGRESS) {
        status = ucc_context_progress(ctx);
        if (status != UCC_OK && status != UCC_INPROGRESS) {
            CHECK_UCC(status, "ucc_context_progress failed");
        }
    }
    CHECK_UCC(status, "ucc_collective_test failed");

    status = ucc_collective_finalize(req);
    CHECK_UCC(status, "ucc_collective_finalize failed");

    /* Read back results from OpenCL buffer */
    cl_err = clEnqueueReadBuffer(cl_queue, cl_recv_buf, CL_TRUE, 0, buf_size,
                                  host_recv, 0, NULL, NULL);
    CHECK_CL(cl_err, "clEnqueueReadBuffer failed");

    clFinish(cl_queue);

    PASS();

    /*
     * Test 6: Verify alltoall results
     */
    TEST("Verify alltoall data correctness");

    printf("\n");

    /* Show sample of send buffer */
    printf("    Send buffer (first 8 elements): ");
    for (i = 0; i < 8 && i < TEST_SIZE * TEAM_SIZE; i++) {
        printf("%d ", host_send[i]);
    }
    printf("...\n");

    /* Show sample of recv buffer */
    printf("    Recv buffer (first 8 elements): ");
    for (i = 0; i < 8 && i < TEST_SIZE * TEAM_SIZE; i++) {
        printf("%d ", host_recv[i]);
    }
    printf("...\n");

    /* Verify all elements - for team size 1, alltoall is just a copy */
    int errors = 0;
    int first_error_idx = -1;
    for (i = 0; i < TEST_SIZE * TEAM_SIZE; i++) {
        if (host_recv[i] != host_send[i]) {
            if (first_error_idx < 0) {
                first_error_idx = i;
            }
            errors++;
        }
    }

    if (errors > 0) {
        printf("    VERIFICATION FAILED!\n");
        printf("    Total errors: %d out of %d elements\n", errors, TEST_SIZE * TEAM_SIZE);
        printf("    First error at index %d: expected %d, got %d\n",
               first_error_idx, host_send[first_error_idx], host_recv[first_error_idx]);
        clReleaseMemObject(cl_send_buf);
        clReleaseMemObject(cl_recv_buf);
        free(host_send);
        free(host_recv);
        FAIL("data verification failed");
        return -1;
    }

    printf("    Verified %d elements: ALL CORRECT\n    ", TEST_SIZE * TEAM_SIZE);

    PASS();

    /*
     * Test 7: Cleanup
     */
    TEST("Cleanup");

    clReleaseMemObject(cl_send_buf);
    clReleaseMemObject(cl_recv_buf);
    free(host_send);
    free(host_recv);

    status = ucc_team_destroy(team);
    CHECK_UCC(status, "ucc_team_destroy failed");

    status = ucc_context_destroy(ctx);
    CHECK_UCC(status, "ucc_context_destroy failed");

    status = ucc_finalize(lib);
    CHECK_UCC(status, "ucc_finalize failed");

    cleanup_opencl();

    PASS();

    /*
     * Summary
     */
    printf("\n=== Results ===\n");
    printf("Passed: %d/%d\n", pass_count, test_count);

    MPI_Finalize();
    if (pass_count == test_count) {
        printf("\nSUCCESS: All tests passed!\n");
        printf("\nThe alltoall collective ran on OpenCL memory buffers.\n");
        printf("The OpenCL MC plugin handled the memory operations.\n");
        return 0;
    } else {
        printf("\nFAILURE: Some tests failed.\n");
        return 1;
    }
}
