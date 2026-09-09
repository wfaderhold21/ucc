/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 */

/*
 * Skeleton + host-fallback tests for the DOCA (BlueField DPU) execution
 * component (UCC_EE_DOCA), modeled on test_ec_cpu_threaded.cc.  The suite is
 * self-guarding: it skips when the DOCA component is not loaded (build without
 * --with-doca_urom, or libucc_ec_doca not on the component path), so a
 * DOCA-less tree still compiles and runs clean.
 *
 * 583 contract: the executor lifecycle (init / start / status / stop /
 * finalize) is a callable no-op stub.
 * 584 contract: the host-side DOCA UROM command channel attempts to connect
 * to the configured DPU at component init; when no DPU is reachable the
 * channel stays down and the component remains available.
 * 586 contract: with the DOCA command channel down, an eligible reduce
 * posted to the DOCA executor is served by the nested CPU executor (the
 * Milestone-B host-offload path) and produces correct results; a task type
 * the CPU path cannot serve reports UCC_ERR_NOT_SUPPORTED.
 */

extern "C" {
#include <components/ec/ucc_ec.h>
#include <pthread.h>
}
#include <common/test.h>
#include <common/test_ucc.h>
#include <cstdlib>
#include <cstring>

/* 4 srcs of (1+i),(2+i),(3+i),(4+i) => dst[i] = 10 + 4*i. */
static const int FALLBACK_COUNT = 256;
static float src0[FALLBACK_COUNT];
static float src1[FALLBACK_COUNT];
static float src2[FALLBACK_COUNT];
static float src3[FALLBACK_COUNT];
static float dst[FALLBACK_COUNT];

/*
 * Configure the DOCA component (584) to try a DPU command channel on a
 * device name that does not exist.  overwrite=0 so a user-set
 * UCC_EC_DOCA_DEVICE (e.g. a real device) still wins.  The channel connect
 * must fail cleanly (no DOCA SDK, no DPU, or no matching device) and the
 * host fallback (586) must still serve the reduce.
 */
__attribute__((constructor)) static void test_ec_doca_set_device()
{
    setenv("UCC_EC_DOCA_DEVICE", "mlx5_nonexistent_584", 0);
}

class test_ec_doca : public ucc::test {
    virtual void SetUp() override
    {
        ucc_ec_params_t ec_params = {
            .thread_mode = UCC_THREAD_SINGLE,
        };

        ucc::test::SetUp();
        ucc_constructor();
        ucc_ec_init(&ec_params);
        if (UCC_OK != ucc_ec_available(UCC_EE_DOCA)) {
            GTEST_SKIP();
        }
    }

    virtual void TearDown() override
    {
        ucc_ec_finalize();
        ucc::test::TearDown();
    }

public:
    ucc_status_t get_doca_executor(ucc_ee_executor_t **executor)
    {
        ucc_ee_executor_params_t eparams;

        eparams.mask    = UCC_EE_EXECUTOR_PARAM_FIELD_TYPE;
        eparams.ee_type = UCC_EE_DOCA;

        return ucc_ee_executor_init(&eparams, executor);
    }

    ucc_status_t put_doca_executor(ucc_ee_executor_t *executor)
    {
        return ucc_ee_executor_finalize(executor);
    }

    /* Post a 4-src f32 SUM of FALLBACK_COUNT into dst on the DOCA executor. */
    ucc_status_t post_sum(ucc_ee_executor_t *executor,
                          ucc_ee_executor_task_t **task)
    {
        ucc_ee_executor_task_args_t args;

        memset(&args, 0, sizeof(args));
        args.task_type      = UCC_EE_EXECUTOR_TASK_REDUCE;
        args.reduce.dst     = dst;
        args.reduce.count   = FALLBACK_COUNT;
        args.reduce.dt      = UCC_DT_FLOAT32;
        args.reduce.op      = UCC_OP_SUM;
        args.reduce.n_srcs  = 4;
        args.reduce.srcs[0] = src0;
        args.reduce.srcs[1] = src1;
        args.reduce.srcs[2] = src2;
        args.reduce.srcs[3] = src3;

        return ucc_ee_executor_task_post(executor, &args, task);
    }

    ucc_status_t wait_task(ucc_ee_executor_task_t *task)
    {
        ucc_status_t status;

        while (UCC_INPROGRESS == (status = ucc_ee_executor_task_test(task))) {
        }
        return status;
    }

    static int check_result()
    {
        int i;

        for (i = 0; i < FALLBACK_COUNT; i++) {
            if (dst[i] != 10.f + 4.f * (float)i) {
                return 0;
            }
        }
        return 1;
    }
};

static void fill_srcs()
{
    int i;

    for (i = 0; i < FALLBACK_COUNT; i++) {
        src0[i] = 1.f + i;
        src1[i] = 2.f + i;
        src2[i] = 3.f + i;
        src3[i] = 4.f + i;
    }
}

UCC_TEST_F(test_ec_doca, ec_doca_load)
{
    /* The component loads even though the DPU channel could not connect
     * (UCC_EC_DOCA_DEVICE points at a nonexistent device): 584 must fail
     * cleanly at init, not break availability. */
    ASSERT_EQ(UCC_OK, ucc_ec_available(UCC_EE_DOCA));
}

UCC_TEST_F(test_ec_doca, ec_doca_executor_init_finalize)
{
    ucc_ee_executor_t *executor;

    ASSERT_EQ(UCC_OK, get_doca_executor(&executor));
    ASSERT_EQ(UCC_OK, put_doca_executor(executor));
}

UCC_TEST_F(test_ec_doca, ec_doca_executor_start_stop)
{
    ucc_ee_executor_t *executor;
    ucc_status_t       status;

    ASSERT_EQ(UCC_OK, get_doca_executor(&executor));

    status = ucc_ee_executor_start(executor, nullptr);
    ASSERT_EQ(UCC_OK, status);

    status = ucc_ee_executor_status(executor);
    ASSERT_EQ(UCC_OK, status);

    status = ucc_ee_executor_stop(executor);
    ASSERT_EQ(UCC_OK, status);

    ASSERT_EQ(UCC_OK, put_doca_executor(executor));
}

/*
 * 584 + 586: with the DOCA command channel down (no reachable DPU), a reduce
 * posted to the DOCA executor completes on the nested CPU executor with
 * correct results.  This is the "USE_DOCA_REDUCE=1 still produces correct
 * results via the host path (no crash)" acceptance.
 */
UCC_TEST_F(test_ec_doca, ec_doca_host_fallback_reduce)
{
    ucc_ee_executor_t    *executor;
    ucc_ee_executor_task_t *task = NULL;
    ucc_status_t        status;

    ASSERT_EQ(UCC_OK, get_doca_executor(&executor));

    fill_srcs();
    memset(dst, 0, sizeof(dst));

    status = post_sum(executor, &task);
    ASSERT_EQ(UCC_OK, status);
    ASSERT_NE(nullptr, task);

    status = wait_task(task);
    ASSERT_EQ(UCC_OK, status);
    EXPECT_TRUE(check_result());

    ASSERT_EQ(UCC_OK, ucc_ee_executor_task_finalize(task));
    ASSERT_EQ(UCC_OK, put_doca_executor(executor));
}
/*
 * 585 config contract: DOCA_REDUCE_LIMIT (default 65536 bytes) is the
 * size gate for the DOCA reduce path.  The 4-src f32 reduce above uses
 * count=256 => total_len = 4*256*4 = 4096, well under the limit, and is
 * served (ec_doca_host_fallback_reduce).  Bumping count so the total
 * reduce length exceeds the default limit (4*16385*4 = 65540 > 65536)
 * makes the reduce ineligible: task_post must reject with
 * UCC_ERR_NOT_SUPPORTED rather than run a too-large reduce.  This brackets
 * the default limit and proves the 585 size-policy knob is wired.
 */
UCC_TEST_F(test_ec_doca, ec_doca_reduce_limit_gate)
{
    ucc_ee_executor_t         *executor;
    ucc_ee_executor_task_args_t task_args = {};
    ucc_ee_executor_task_t    *task       = NULL;
    ucc_status_t            status;

    /* 4 * 16385 * sizeof(float) = 65540 > 65536 (default limit). */
    const int OVER_LIMIT_COUNT = 16385;
    float      big_dst[4];
    float      big_src[4];

    ASSERT_EQ(UCC_OK, get_doca_executor(&executor));

    memset(&task_args, 0, sizeof(task_args));
    task_args.task_type      = UCC_EE_EXECUTOR_TASK_REDUCE;
    task_args.reduce.dst     = big_dst;
    task_args.reduce.count   = OVER_LIMIT_COUNT;
    task_args.reduce.dt      = UCC_DT_FLOAT32;
    task_args.reduce.op      = UCC_OP_SUM;
    task_args.reduce.n_srcs  = 4;
    task_args.reduce.srcs[0] = big_src;
    task_args.reduce.srcs[1] = big_src;
    task_args.reduce.srcs[2] = big_src;
    task_args.reduce.srcs[3] = big_src;

    status = ucc_ee_executor_task_post(executor, &task_args, &task);
    ASSERT_EQ(UCC_ERR_NOT_SUPPORTED, status);
    ASSERT_EQ(nullptr, task);

    ASSERT_EQ(UCC_OK, put_doca_executor(executor));
}


/*
 * A task type the CPU fallback path cannot serve (COPY, while the DPU channel
 * is absent) must report UCC_ERR_NOT_SUPPORTED rather than silently succeed.
 */
UCC_TEST_F(test_ec_doca, ec_doca_copy_not_supported)
{
    ucc_ee_executor_t        *executor;
    ucc_ee_executor_task_args_t task_args = {};
    ucc_ee_executor_task_t   *task        = NULL;

    ASSERT_EQ(UCC_OK, get_doca_executor(&executor));

    task_args.task_type = UCC_EE_EXECUTOR_TASK_COPY;
    task_args.copy.dst  = dst;
    task_args.copy.src  = src0;
    task_args.copy.len  = FALLBACK_COUNT * sizeof(float);

    ASSERT_EQ(UCC_ERR_NOT_SUPPORTED,
              ucc_ee_executor_task_post(executor, &task_args, &task));
    ASSERT_EQ(nullptr, task);

    ASSERT_EQ(UCC_OK, put_doca_executor(executor));
}
