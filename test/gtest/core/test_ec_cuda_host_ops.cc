/**
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 *
 * 579 — ec_cuda_use_host_ops policy + config + host/GPU routing counters.
 *
 * The CUDA EC is a dlopen'd component (libucc_ec_cuda.so), so its symbols are
 * not linked into the test binary; they are resolved at runtime with
 * dlopen/dlsym (RTLD_GLOBAL), exactly as the UCC runtime discovers components.
 *
 * Covers:
 *   - config parses (defaults: USE_HOST_REDUCE=0, REDUCE_HOST_LIMIT=256);
 *   - the policy function is unit-testable (direct config mutation);
 *   - end-to-end: a small reduce is routed to the CPU executor (host
 *     counter) and a large one stays on the GPU (gpu counter), both compute
 *     the correct result.
 */

extern "C" {
#include <components/ec/ucc_ec.h>
#include <pthread.h>
#include <dlfcn.h>
#include <stdint.h>
#include <stddef.h>
}
#include <components/ec/cuda/ec_cuda.h>
#include <common/test.h>
#include <common/test_ucc.h>
#include <cuda_runtime.h>
#include <cstdlib>
#include <cstring>

typedef bool (*ec_cuda_use_host_ops_fn)(const ucc_ee_executor_task_args_t *);
typedef uint64_t (*count_fn)(void);

class test_ec_cuda_host_ops : public ucc::test {
public:
    virtual void SetUp() override
    {
        ucc::test::SetUp();
        ucc_constructor();
        ucc_ec_params_t ec_params = {
            .thread_mode = UCC_THREAD_SINGLE,
        };
        ucc_ec_init(&ec_params);
        if (UCC_OK != ucc_ec_available(UCC_EE_CUDA_STREAM)) {
            GTEST_SKIP();
        }

        /* Locate libucc_ec_cuda.so: UCC_COMPONENT_PATH or the build tree. */
        const char *cp = getenv("UCC_COMPONENT_PATH");
        std::string dir = cp ? cp
                             : std::string(GTEST_UCC_TOP_SRCDIR) +
                                   "/src/components/ec/cuda/.libs";
        char lib[4096];
        snprintf(lib, sizeof(lib), "%s/libucc_ec_cuda.so", dir.c_str());
        handle_ = dlopen(lib, RTLD_NOW | RTLD_GLOBAL);
        if (!handle_) {
            GTEST_SKIP() << "cannot dlopen " << lib << ": " << dlerror();
        }
        eccuda_ = (ucc_ec_cuda_t *)dlsym(handle_, "ucc_ec_cuda");
        use_host_ = (ec_cuda_use_host_ops_fn)dlsym(handle_,
                                                    "ec_cuda_use_host_ops");
        host_cnt_ = (count_fn)dlsym(handle_, "ucc_ec_cuda_host_reduce_count");
        gpu_cnt_  = (count_fn)dlsym(handle_, "ucc_ec_cuda_gpu_reduce_count");
        if (!eccuda_ || !use_host_ || !host_cnt_ || !gpu_cnt_) {
            GTEST_SKIP() << "dlsym failed: " << dlerror();
        }
    }

    virtual void TearDown() override
    {
        ucc_ec_finalize();
        ucc::test::TearDown();
        if (handle_) {
            dlclose(handle_);
            handle_ = NULL;
        }
    }

    /* The 579 config fields, at the offsets defined by
     * ucc_ec_cuda_config_t (ec_cuda_resources.h).  A layout drift changes
     * the defaults test's observed values, so the mismatch is caught
     * loudly. */
    int *cfg_use_host_reduce()
    {
        return (int *)((char *)eccuda_->super.config +
                       offsetof(ucc_ec_cuda_config_t, use_host_reduce));
    }

    int *cfg_reduce_host_limit()
    {
        return (int *)((char *)eccuda_->super.config +
                       offsetof(ucc_ec_cuda_config_t, reduce_host_limit));
    }

    ucc_ee_executor_t *get_executor()
    {
        ucc_ee_executor_params_t eparams;
        ucc_ee_executor_t        *exe = NULL;
        ucc_status_t              st;

        eparams.mask    = UCC_EE_EXECUTOR_PARAM_FIELD_TYPE;
        eparams.ee_type = UCC_EE_CUDA_STREAM;
        st = ucc_ee_executor_init(&eparams, &exe);
        return st == UCC_OK ? exe : NULL;
    }

    void                   *handle_ = NULL;
    ucc_ec_cuda_t          *eccuda_ = NULL;
    ec_cuda_use_host_ops_fn use_host_ = NULL;
    count_fn                host_cnt_ = NULL;
    count_fn                gpu_cnt_  = NULL;
};

static ucc_status_t
post_reduce(ucc_ee_executor_t *exe, size_t count, float *dst,
            const float *s1, const float *s2)
{
    ucc_ee_executor_task_args_t args;
    ucc_ee_executor_task_t     *task = NULL;
    ucc_status_t                status;

    memset(&args, 0, sizeof(args));
    args.task_type         = UCC_EE_EXECUTOR_TASK_REDUCE;
    args.reduce.dst         = dst;
    args.reduce.srcs[0]     = (void *)s1;
    args.reduce.srcs[1]     = (void *)s2;
    args.reduce.count       = count;
    args.reduce.alpha       = 1.0;
    args.reduce.dt          = UCC_DT_FLOAT32;
    args.reduce.op          = UCC_OP_SUM;
    args.reduce.n_srcs      = 2;

    status = ucc_ee_executor_task_post(exe, &args, &task);
    if (status != UCC_OK) {
        return status;
    }
    do {
        status = ucc_ee_executor_task_test(task);
    } while (status > 0); /* UCC_INPROGRESS = 1 */
    if (status != UCC_OK) {
        return status;
    }
    return ucc_ee_executor_task_finalize(task);
}

UCC_TEST_F(test_ec_cuda_host_ops, config_defaults)
{
    /* ucc_ec_init in SetUp parsed the config (env prefix UCC_).  Defaults:
     * policy off, 256-byte limit (UCS memunits: bare number = bytes). */
    EXPECT_EQ(*cfg_use_host_reduce(), 0);
    EXPECT_EQ(*cfg_reduce_host_limit(), 256);
}

UCC_TEST_F(test_ec_cuda_host_ops, policy_matrix)
{
    ucc_ee_executor_task_args_t a;

    /* policy disabled: never host, regardless of size/dt */
    *cfg_use_host_reduce()   = 0;
    *cfg_reduce_host_limit() = 1024;

    a.task_type    = UCC_EE_EXECUTOR_TASK_REDUCE;
    a.reduce.dt    = UCC_DT_FLOAT32;
    a.reduce.count = 4; /* 16 bytes */
    EXPECT_FALSE(use_host_(&a));

    /* enabled + small + f32: host */
    *cfg_use_host_reduce() = 1;
    EXPECT_TRUE(use_host_(&a));

    /* enabled + f32 complex (not host-supported): GPU */
    a.reduce.dt = UCC_DT_FLOAT32_COMPLEX;
    EXPECT_FALSE(use_host_(&a));

    /* enabled + bf16 (not host-supported): GPU */
    a.reduce.dt = UCC_DT_BFLOAT16;
    EXPECT_FALSE(use_host_(&a));

    /* enabled + f64 at exactly the limit: host; one element over: GPU */
    a.reduce.dt    = UCC_DT_FLOAT64;
    a.reduce.count = 128; /* 1024 bytes == limit -> host */
    EXPECT_TRUE(use_host_(&a));
    a.reduce.count = 129; /* 1032 bytes > limit -> GPU */
    EXPECT_FALSE(use_host_(&a));

    /* strided variant: same policy */
    a.task_type = UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED;
    a.reduce_strided.dt = UCC_DT_INT32;
    a.reduce_strided.count = 4; /* 16 bytes */
    EXPECT_TRUE(use_host_(&a));

    /* non-reduce types are never routed to host */
    a.task_type = UCC_EE_EXECUTOR_TASK_COPY;
    a.copy.len  = 4;
    EXPECT_FALSE(use_host_(&a));

    *cfg_use_host_reduce() = 0; /* restore default */
}
UCC_TEST_F(test_ec_cuda_host_ops, host_gpu_routing_counters)
{
    ucc_ee_executor_t *exe;
    ucc_status_t       status;
    size_t             n_small = 4;    /* 16 bytes <= 64 limit -> host */
    size_t             n_large = 1024; /* 4096 bytes > 64 limit -> gpu  */
    float             *dst_s, *s1s, *s2s;
    float             *dst_l, *s1l, *s2l, *dst_l_h;
    uint64_t            host_before, gpu_before;
    size_t              i;

    exe = get_executor();
    ASSERT_NE(exe, nullptr);
    status = ucc_ee_executor_start(exe, nullptr);
    ASSERT_EQ(status, UCC_OK);

    *cfg_use_host_reduce()   = 1;
    *cfg_reduce_host_limit() = 64;

    /* small reduce: host buffers (CPU executor reads/writes host memory) */
    dst_s = (float *)malloc(n_small * sizeof(float));
    s1s   = (float *)malloc(n_small * sizeof(float));
    s2s   = (float *)malloc(n_small * sizeof(float));
    ASSERT_NE(dst_s, nullptr);

    /* large reduce: device buffers (GPU reduce kernel); D2H/H2D staging
     * for host-backed device ops is 580, so the GPU path gets device
     * memory here. */
    float h_s1l[1024], h_s2l[1024];
    for (i = 0; i < n_large; i++) {
        h_s1l[i] = 1.0f;
        h_s2l[i] = 1.0f;
    }
    ASSERT_EQ(cudaSuccess, cudaMalloc((void **)&s1l, n_large * sizeof(float)));
    ASSERT_EQ(cudaSuccess, cudaMalloc((void **)&s2l, n_large * sizeof(float)));
    ASSERT_EQ(cudaSuccess, cudaMalloc((void **)&dst_l, n_large * sizeof(float)));
    ASSERT_EQ(cudaSuccess, cudaMemcpy(s1l, h_s1l, n_large * sizeof(float),
                                      cudaMemcpyHostToDevice));
    ASSERT_EQ(cudaSuccess, cudaMemcpy(s2l, h_s2l, n_large * sizeof(float),
                                      cudaMemcpyHostToDevice));
    dst_l_h = (float *)malloc(n_large * sizeof(float));
    ASSERT_NE(dst_l_h, nullptr);

    for (i = 0; i < n_small; i++) {
        s1s[i] = (float)(i + 1);
        s2s[i] = (float)(2 * (i + 1));
    }

    host_before = host_cnt_();
    gpu_before  = gpu_cnt_();

    /* small reduce -> routed to the CPU executor */
    status = post_reduce(exe, n_small, dst_s, s1s, s2s);
    ASSERT_EQ(status, UCC_OK);
    for (i = 0; i < n_small; i++) {
        EXPECT_FLOAT_EQ(dst_s[i], (float)(3 * (i + 1)));
    }

    /* large reduce -> stays on the GPU (device buffers) */
    status = post_reduce(exe, n_large, dst_l, s1l, s2l);
    ASSERT_EQ(status, UCC_OK);
    ASSERT_EQ(cudaSuccess, cudaMemcpy(dst_l_h, dst_l, n_large * sizeof(float),
                                      cudaMemcpyDeviceToHost));
    for (i = 0; i < n_large; i += 256) {
        EXPECT_FLOAT_EQ(dst_l_h[i], 2.0f);
    }

    EXPECT_EQ(host_cnt_() - host_before, 1u);
    EXPECT_EQ(gpu_cnt_() - gpu_before, 1u);

    status = ucc_ee_executor_stop(exe);
    EXPECT_EQ(status, UCC_OK);
    status = ucc_ee_executor_finalize(exe);
    EXPECT_EQ(status, UCC_OK);

    free(dst_s); free(s1s); free(s2s);
    free(dst_l_h);
    cudaFree(s1l); cudaFree(s2l); cudaFree(dst_l);
}
/*
 * 580 — device-buffer host-offload staging.
 *
 * When the host policy matches and the reduce buffers are device-resident,
 * the offload path stages the sources D2H, runs the reduce on the nested
 * CPU executor, and stages the result H2D (fenced by a CUDA event).  These
 * tests verify the result is bit-exact vs the reference and that the task
 * is routed to the host counter (not the GPU counter).
 */
static ucc_status_t
post_reduce_ext(ucc_ee_executor_t *exe, size_t count, void *dst,
                float **srcs, size_t n_srcs)
{
    ucc_ee_executor_task_args_t args;
    ucc_ee_executor_task_t     *task = NULL;
    ucc_status_t                status;

    memset(&args, 0, sizeof(args));
    args.task_type = UCC_EE_EXECUTOR_TASK_REDUCE;
    args.flags     = UCC_EEE_TASK_FLAG_REDUCE_SRCS_EXT;
    args.reduce.dst       = dst;
    args.reduce.srcs_ext  = (void **)srcs;
    args.reduce.count     = count;
    args.reduce.alpha     = 1.0;
    args.reduce.dt        = UCC_DT_FLOAT32;
    args.reduce.op        = UCC_OP_SUM;
    args.reduce.n_srcs    = (uint16_t)n_srcs;

    status = ucc_ee_executor_task_post(exe, &args, &task);
    if (status != UCC_OK) {
        return status;
    }
    do {
        status = ucc_ee_executor_task_test(task);
    } while (status > 0);
    if (status != UCC_OK) {
        return status;
    }
    return ucc_ee_executor_task_finalize(task);
}

UCC_TEST_F(test_ec_cuda_host_ops, device_reduce_staged_to_host)
{
    ucc_ee_executor_t *exe;
    ucc_status_t       status;
    const size_t       n = 8; /* 32 bytes <= 64 limit -> host */
    float             *s1, *s2, *dst, *dst_h;
    float              ref[8];
    uint64_t           host_before;
    size_t             i;

    exe = get_executor();
    ASSERT_NE(exe, nullptr);
    status = ucc_ee_executor_start(exe, nullptr);
    ASSERT_EQ(status, UCC_OK);

    *cfg_use_host_reduce()   = 1;
    *cfg_reduce_host_limit() = 64;

    ASSERT_EQ(cudaSuccess, cudaMalloc((void **)&s1, n * sizeof(float)));
    ASSERT_EQ(cudaSuccess, cudaMalloc((void **)&s2, n * sizeof(float)));
    ASSERT_EQ(cudaSuccess, cudaMalloc((void **)&dst, n * sizeof(float)));
    dst_h = (float *)malloc(n * sizeof(float));
    ASSERT_NE(dst_h, nullptr);

    /* s1[i] = 1*(i+1), s2[i] = 2*(i+1) -> dst[i] = 3*(i+1) */
    for (i = 0; i < n; i++) {
        ref[i] = (float)(i + 1);
    }
    ASSERT_EQ(cudaSuccess, cudaMemcpy(s1, ref, n * sizeof(float),
                                      cudaMemcpyHostToDevice));
    for (i = 0; i < n; i++) {
        ref[i] *= 2.0f; /* s2 = 2*(i+1) */
    }
    ASSERT_EQ(cudaSuccess, cudaMemcpy(s2, ref, n * sizeof(float),
                                      cudaMemcpyHostToDevice));

    host_before = host_cnt_();

    status = post_reduce(exe, n, dst, s1, s2);
    ASSERT_EQ(status, UCC_OK);
    ASSERT_EQ(cudaSuccess, cudaMemcpy(dst_h, dst, n * sizeof(float),
                                      cudaMemcpyDeviceToHost));
    for (i = 0; i < n; i++) {
        EXPECT_FLOAT_EQ(dst_h[i], 3.0f * (float)(i + 1));
    }

    /* small + device buffers: routed to the host counter via staging */
    EXPECT_EQ(host_cnt_() - host_before, 1u);

    status = ucc_ee_executor_stop(exe);
    EXPECT_EQ(status, UCC_OK);
    status = ucc_ee_executor_finalize(exe);
    EXPECT_EQ(status, UCC_OK);

    free(dst_h);
    cudaFree(s1); cudaFree(s2); cudaFree(dst);
}

UCC_TEST_F(test_ec_cuda_host_ops, device_reduce_staged_multi_src)
{
    ucc_ee_executor_t *exe;
    ucc_status_t       status;
    const size_t       n = 8; /* 32 bytes <= 64 limit -> host */
    float             *d_srcs[5] = {NULL};
    float             *dst, *dst_h, *ref;
    uint64_t           host_before;
    size_t             i;
    int                k;

    exe = get_executor();
    ASSERT_NE(exe, nullptr);
    status = ucc_ee_executor_start(exe, nullptr);
    ASSERT_EQ(status, UCC_OK);

    *cfg_use_host_reduce()   = 1;
    *cfg_reduce_host_limit() = 64;

    for (k = 0; k < 5; k++) {
        ASSERT_EQ(cudaSuccess,
                  cudaMalloc((void **)&d_srcs[k], n * sizeof(float)));
    }
    ASSERT_EQ(cudaSuccess, cudaMalloc((void **)&dst, n * sizeof(float)));
    dst_h = (float *)malloc(n * sizeof(float));
    ref   = (float *)malloc(n * sizeof(float));
    ASSERT_NE(dst_h, nullptr);
    ASSERT_NE(ref, nullptr);

    /* src_k[i] = (k+1)*(i+1); total = 15*(i+1) */
    for (k = 0; k < 5; k++) {
        for (i = 0; i < n; i++) {
            ref[i] = (float)(k + 1) * (i + 1);
        }
        ASSERT_EQ(cudaSuccess, cudaMemcpy(d_srcs[k], ref, n * sizeof(float),
                                          cudaMemcpyHostToDevice));
    }
    for (i = 0; i < n; i++) {
        ref[i] = 15.0f * (i + 1);
    }

    host_before = host_cnt_();

    status = post_reduce_ext(exe, n, dst, d_srcs, 5);
    ASSERT_EQ(status, UCC_OK);
    ASSERT_EQ(cudaSuccess, cudaMemcpy(dst_h, dst, n * sizeof(float),
                                      cudaMemcpyDeviceToHost));
    for (i = 0; i < n; i++) {
        EXPECT_FLOAT_EQ(dst_h[i], ref[i]);
    }

    EXPECT_EQ(host_cnt_() - host_before, 1u);

    status = ucc_ee_executor_stop(exe);
    EXPECT_EQ(status, UCC_OK);
    status = ucc_ee_executor_finalize(exe);
    EXPECT_EQ(status, UCC_OK);

    free(dst_h);
    free(ref);
    cudaFree(dst);
    for (k = 0; k < 5; k++) {
        cudaFree(d_srcs[k]);
    }
}

UCC_TEST_F(test_ec_cuda_host_ops, strided_reduce_staged_to_host)
{
    ucc_ee_executor_t *exe;
    ucc_status_t       status;
    size_t             n      = 128; /* 512 bytes <= 1024 limit -> host */
    size_t             n_src2 = 2;   /* 3 sources total */
    const size_t       stride  = 12; /* 3-float offset; 12 % 4 == 0 */
    const size_t       region  = (n_src2 - 1) * stride + n * sizeof(float);
    float             *src1, *src2, *dst, *dst_h, *fill;
    uint64_t           host_before;
    size_t             i;

    exe = get_executor();
    ASSERT_NE(exe, nullptr);
    status = ucc_ee_executor_start(exe, nullptr);
    ASSERT_EQ(status, UCC_OK);

    *cfg_use_host_reduce()   = 1;
    *cfg_reduce_host_limit() = 1024;

    ASSERT_EQ(cudaSuccess, cudaMalloc((void **)&src1, n * sizeof(float)));
    ASSERT_EQ(cudaSuccess, cudaMalloc((void **)&src2, region));
    ASSERT_EQ(cudaSuccess, cudaMalloc((void **)&dst, n * sizeof(float)));
    dst_h = (float *)malloc(n * sizeof(float));
    fill  = (float *)malloc(region / sizeof(float) + 1);
    ASSERT_NE(dst_h, nullptr);
    ASSERT_NE(fill, nullptr);

    /* Constant patterns: src1 = 1.0, every strided slice of src2 = 2.0,
     * so dst[i] = 1.0 + 2.0 + 2.0 = 5.0 regardless of the (overlapping)
     * 12-byte stride. */
    for (i = 0; i < n; i++) {
        fill[i] = 1.0f;
    }
    ASSERT_EQ(cudaSuccess, cudaMemcpy(src1, fill, n * sizeof(float),
                                      cudaMemcpyHostToDevice));
    for (i = 0; i < region / sizeof(float) + 1; i++) {
        fill[i] = 2.0f;
    }
    ASSERT_EQ(cudaSuccess, cudaMemcpy(src2, fill, region,
                                      cudaMemcpyHostToDevice));

    host_before = host_cnt_();

    {
        ucc_ee_executor_task_args_t args;
        ucc_ee_executor_task_t     *task = NULL;

        memset(&args, 0, sizeof(args));
        args.task_type          = UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED;
        args.reduce_strided.dst     = dst;
        args.reduce_strided.src1    = src1;
        args.reduce_strided.src2    = src2;
        args.reduce_strided.stride  = stride;
        args.reduce_strided.count   = n;
        args.reduce_strided.alpha   = 1.0;
        args.reduce_strided.dt      = UCC_DT_FLOAT32;
        args.reduce_strided.op      = UCC_OP_SUM;
        args.reduce_strided.n_src2  = (uint16_t)n_src2;

        status = ucc_ee_executor_task_post(exe, &args, &task);
        ASSERT_EQ(status, UCC_OK);
        do {
            status = ucc_ee_executor_task_test(task);
        } while (status > 0);
        ASSERT_EQ(status, UCC_OK);
        status = ucc_ee_executor_task_finalize(task);
        ASSERT_EQ(status, UCC_OK);
    }

    ASSERT_EQ(cudaSuccess, cudaMemcpy(dst_h, dst, n * sizeof(float),
                                      cudaMemcpyDeviceToHost));
    for (i = 0; i < n; i++) {
        EXPECT_FLOAT_EQ(dst_h[i], 5.0f);
    }

    EXPECT_EQ(host_cnt_() - host_before, 1u);

    status = ucc_ee_executor_stop(exe);
    EXPECT_EQ(status, UCC_OK);
    status = ucc_ee_executor_finalize(exe);
    EXPECT_EQ(status, UCC_OK);

    free(dst_h);
    free(fill);
    cudaFree(src1); cudaFree(src2); cudaFree(dst);
}
