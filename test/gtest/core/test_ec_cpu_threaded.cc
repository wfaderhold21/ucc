/**
 * Copyright (c) 2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 */

/*
 * Executor semantics + stress tests for the threaded CPU reduce pool
 * (UCC_EE_CPU_THREAD with UCC_EC_CPU_USE_THREADED_REDUCE=1), modeled on
 * test_ec_cuda.cc.  The suite is self-guarding: the whole threaded body
 * is excluded when the tree is built without --enable-ec-threaded-reduce,
 * and every test asserts the CPU-thread executor is present.  Async-only
 * tests additionally skip when the pool is not active
 * (UCC_EC_CPU_USE_THREADED_REDUCE unset or 0/false), so the same binary
 * covers both the synchronous and the threaded pool path.
 *
 * The large buffers are file-scope statics: the default pthread stack is
 * 8 MB and a 1M x 4 x 4 B reduce already exceeds it on the stack.  All
 * tests reduce the same deterministic src (filled once per test, before
 * any task is posted), so one shared src static suffices.
 */

extern "C" {
#include <components/ec/ucc_ec.h>
#include <pthread.h>
}
#include <common/test.h>
#include <common/test_ucc.h>
#include <cstdio>
#include <cstdlib>
#include <cctype>
#include <cstring>
/*
 * Mirrors the runtime USE_THREADED_REDUCE (UCC_CONFIG_TYPE_BOOL) parse:
 * the async tests only make sense when the pool is actually started.
 * A bare presence check (getenv) is wrong for the documented
 * "USE_THREADED_REDUCE=0" disable / A2-synchronous-fallback case, which
 * would otherwise run the async tests on the synchronous path and
 * spuriously fail (no task is ever observed INPROGRESS).
 */
static bool
threaded_reduce_enabled(void)
{
    const char *v = getenv("UCC_EC_CPU_USE_THREADED_REDUCE");
    char        buf[32];
    size_t      i;

    if (v == NULL || *v == '\0') {
        return false;
    }
    for (i = 0; v[i] && i < sizeof(buf) - 1; i++) {
        buf[i] = (char)tolower((unsigned char)v[i]);
    }
    buf[i] = '\0';
    return (strcmp(buf, "1") == 0 || strcmp(buf, "true") == 0 ||
            strcmp(buf, "on") == 0 || strcmp(buf, "yes") == 0);
}

#ifdef HAVE_EC_THREADED_REDUCE

#define BURST_COUNT    1000000
#define BURST_N_TASKS  16
#define COPY_LEN       (1 << 20)

#define STRESS_N_THREADS 8
#define STRESS_TASKS     32
#define STRESS_COUNT     1000000

/*
 * File-scope buffers (BSS, not stack).  src is shared: every test fills
 * it identically before posting, and no test runs concurrently with
 * another (gtest runs tests sequentially in one process).
 */
static float src[BURST_N_TASKS][BURST_COUNT];   /* 64 MB */
static float dst_single[BURST_COUNT];            /* 4 MB  */
static float dst_burst[BURST_N_TASKS][BURST_COUNT]; /* 64 MB */
static float dst_stress[STRESS_N_THREADS][STRESS_COUNT]; /* 32 MB */
static char  copy_src[COPY_LEN];
static char  copy_dst[COPY_LEN];

/* Reference for the f32 SUM used below: srcs are (1+i),(2+i),(3+i),
 * (4+i), so dst[i] = 10 + 4*i. */
static float
ref_sum_f32(size_t i)
{
    return 10.f + 4.f * (float)i;
}
static void
fill_f32(void)
{
    size_t i;

    for (i = 0; i < BURST_COUNT; i++) {
        src[0][i] = 1.f + i;
        src[1][i] = 2.f + i;
        src[2][i] = 3.f + i;
        src[3][i] = 4.f + i;
    }
}

static int
check_f32(const float *dst, size_t count)
{
    size_t i;

    for (i = 0; i < count; i++) {
        if (dst[i] != ref_sum_f32(i)) {
            return 0;
        }
    }
    return 1;
}

class test_ec_cpu_threaded : public ucc::test {
  protected:
    virtual void SetUp() override
    {
        ucc_ec_params_t ec_params = {
            .thread_mode = UCC_THREAD_MULTIPLE,
        };

        ucc::test::SetUp();
        ucc_constructor();
        if (UCC_OK != ucc_ec_init(&ec_params)) {
            GTEST_SKIP();
        }
        if (UCC_OK != ucc_ec_available(UCC_EE_CPU_THREAD)) {
            GTEST_SKIP();
        }
    }

    virtual void TearDown() override
    {
        ucc_ec_finalize();
        ucc::test::TearDown();
    }

    ucc_status_t get_cpu_executor(ucc_ee_executor_t **executor)
    {
        ucc_ee_executor_params_t eparams;

        eparams.mask    = UCC_EE_EXECUTOR_PARAM_FIELD_TYPE;
        eparams.ee_type = UCC_EE_CPU_THREAD;

        return ucc_ee_executor_init(&eparams, executor);
    }

    ucc_status_t put_cpu_executor(ucc_ee_executor_t *executor)
    {
        return ucc_ee_executor_finalize(executor);
    }

    /* Post a 4-src f32 SUM of BURST_COUNT into dst; returns the task. */
    ucc_status_t post_sum(ucc_ee_executor_t *executor, float *dst,
                          ucc_ee_executor_task_t **task)
    {
        ucc_ee_executor_task_args_t args;

        memset(&args, 0, sizeof(args));
        args.task_type      = UCC_EE_EXECUTOR_TASK_REDUCE;
        args.reduce.dst     = dst;
        args.reduce.count   = BURST_COUNT;
        args.reduce.dt      = UCC_DT_FLOAT32;
        args.reduce.op      = UCC_OP_SUM;
        args.reduce.n_srcs  = 4;
        args.reduce.srcs[0] = src[0];
        args.reduce.srcs[1] = src[1];
        args.reduce.srcs[2] = src[2];
        args.reduce.srcs[3] = src[3];

        return ucc_ee_executor_task_post(executor, &args, task);
    }

    ucc_status_t wait_task(ucc_ee_executor_task_t *task)
    {
        ucc_status_t status;

        while (UCC_INPROGRESS == (status = ucc_ee_executor_task_test(task))) {
        }
        return status;
    }
};

UCC_TEST_F(test_ec_cpu_threaded, ec_cpu_threaded_load)
{
    ASSERT_EQ(UCC_OK, ucc_ec_available(UCC_EE_CPU_THREAD));
}

UCC_TEST_F(test_ec_cpu_threaded, ec_cpu_threaded_executor_init_finalize)
{
    ucc_ee_executor_t *executor;

    ASSERT_EQ(UCC_OK, get_cpu_executor(&executor));
    ASSERT_EQ(UCC_OK, put_cpu_executor(executor));
}

UCC_TEST_F(test_ec_cpu_threaded, ec_cpu_threaded_executor_start_stop)
{
    ucc_ee_executor_t *executor;
    ucc_status_t       status;

    ASSERT_EQ(UCC_OK, get_cpu_executor(&executor));

    status = ucc_ee_executor_start(executor, nullptr);
    EXPECT_EQ(UCC_OK, status);
    if (status == UCC_OK) {
        ucc_ee_executor_task_t *task = NULL;

        fill_f32();
        /* A 1M reduce completes correctly through the executor
         * contract on both the synchronous path and the threaded pool
         * path (async semantics are asserted separately). */
        status = post_sum(executor, dst_single, &task);
        EXPECT_EQ(UCC_OK, status);
        if (UCC_OK == status) {
            status = wait_task(task);
            EXPECT_EQ(UCC_OK, status);
            if (UCC_OK == status) {
                EXPECT_TRUE(check_f32(dst_single, BURST_COUNT));
                EXPECT_EQ(UCC_OK, ucc_ee_executor_task_finalize(task));
            }
        }
        EXPECT_EQ(UCC_OK, ucc_ee_executor_stop(executor));
    }
    ASSERT_EQ(UCC_OK, put_cpu_executor(executor));
}

UCC_TEST_F(test_ec_cpu_threaded, ec_cpu_threaded_async_reduce)
{
    ucc_ee_executor_t *executor;

    if (!threaded_reduce_enabled()) {
        GTEST_SKIP();
    }

    ASSERT_EQ(UCC_OK, get_cpu_executor(&executor));
    ASSERT_EQ(UCC_OK, ucc_ee_executor_start(executor, nullptr));

    fill_f32();
    /*
     * Async contract: post returns UCC_OK with the task handed to a
     * pool worker; task_test polls the worker's release-stored status
     * until it is no longer INPROGRESS; finalize releases the task.
     */
    ucc_ee_executor_task_t *task = NULL;
    ucc_status_t            status = post_sum(executor, dst_single, &task);
    EXPECT_EQ(UCC_OK, status);
    if (UCC_OK == status) {
        status = wait_task(task);
        EXPECT_EQ(UCC_OK, status);
        if (UCC_OK == status) {
            EXPECT_TRUE(check_f32(dst_single, BURST_COUNT));
            EXPECT_EQ(UCC_OK, ucc_ee_executor_task_finalize(task));
        }
    }
    EXPECT_EQ(UCC_OK, ucc_ee_executor_stop(executor));
    ASSERT_EQ(UCC_OK, put_cpu_executor(executor));
}

UCC_TEST_F(test_ec_cpu_threaded, ec_cpu_threaded_async_copy)
{
    ucc_ee_executor_t *      executor;
    ucc_ee_executor_task_t * task  = NULL;
    ucc_ee_executor_task_args_t args;
    ucc_status_t               status;
    size_t                     i;

    if (!threaded_reduce_enabled()) {
        GTEST_SKIP();
    }

    ASSERT_EQ(UCC_OK, get_cpu_executor(&executor));
    ASSERT_EQ(UCC_OK, ucc_ee_executor_start(executor, nullptr));

    for (i = 0; i < COPY_LEN; i++) {
        copy_src[i] = (char)(i & 0xff);
    }
    memset(copy_dst, 0, COPY_LEN);

    memset(&args, 0, sizeof(args));
    args.task_type = UCC_EE_EXECUTOR_TASK_COPY;
    args.copy.src  = copy_src;
    args.copy.dst  = copy_dst;
    args.copy.len  = COPY_LEN;

    status = ucc_ee_executor_task_post(executor, &args, &task);
    EXPECT_EQ(UCC_OK, status);
    if (UCC_OK == status) {
        status = wait_task(task);
        EXPECT_EQ(UCC_OK, status);
        EXPECT_EQ(0, memcmp(copy_dst, copy_src, COPY_LEN));
        EXPECT_EQ(UCC_OK, ucc_ee_executor_task_finalize(task));
    }
    EXPECT_EQ(UCC_OK, ucc_ee_executor_stop(executor));
    ASSERT_EQ(UCC_OK, put_cpu_executor(executor));
}

UCC_TEST_F(test_ec_cpu_threaded, ec_cpu_threaded_unsupported_op)
{
    ucc_ee_executor_t *      executor;
    ucc_ee_executor_task_t * task = NULL;
    ucc_ee_executor_task_args_t args;
    ucc_status_t               status;

    ASSERT_EQ(UCC_OK, get_cpu_executor(&executor));
    ASSERT_EQ(UCC_OK, ucc_ee_executor_start(executor, nullptr));

    /* COPY_MULTI is not supported by the CPU executor on any path. */
    memset(&args, 0, sizeof(args));
    args.task_type = UCC_EE_EXECUTOR_TASK_COPY_MULTI;
    status         = ucc_ee_executor_task_post(executor, &args, &task);
    EXPECT_EQ(UCC_ERR_NOT_SUPPORTED, status);

    EXPECT_EQ(UCC_OK, ucc_ee_executor_stop(executor));
    ASSERT_EQ(UCC_OK, put_cpu_executor(executor));
}

UCC_TEST_F(test_ec_cpu_threaded, ec_cpu_threaded_async_burst)
{
    ucc_ee_executor_t *executor;
    size_t            i;
    int               saw_inprogress = 0;
    int               all_ok = 1;

    if (!threaded_reduce_enabled()) {
        GTEST_SKIP();
    }

    ASSERT_EQ(UCC_OK, get_cpu_executor(&executor));
    ASSERT_EQ(UCC_OK, ucc_ee_executor_start(executor, nullptr));

    fill_f32();
    ucc_ee_executor_task_t *task[BURST_N_TASKS];
    ucc_status_t            status;

    for (i = 0; i < BURST_N_TASKS; i++) {
        status = post_sum(executor, dst_burst[i], &task[i]);
        EXPECT_EQ(UCC_OK, status);
        if (UCC_OK != status) {
            all_ok = 0;
        }
    }

    /* BURST_N_TASKS in-flight tasks over the (default 4) pool workers:
     * at least one must be observed INPROGRESS while the others drain. */
    for (i = 0; i < BURST_N_TASKS; i++) {
        if (UCC_INPROGRESS == ucc_ee_executor_task_test(task[i])) {
            saw_inprogress = 1;
        }
    }

    for (i = 0; i < BURST_N_TASKS; i++) {
        status = wait_task(task[i]);
        if (UCC_OK != status) {
            all_ok = 0;
        }
        if (!check_f32(dst_burst[i], BURST_COUNT)) {
            all_ok = 0;
        }
        if (UCC_OK != ucc_ee_executor_task_finalize(task[i])) {
            all_ok = 0;
        }
    }

    EXPECT_TRUE(saw_inprogress);
    EXPECT_TRUE(all_ok);

    EXPECT_EQ(UCC_OK, ucc_ee_executor_stop(executor));
    ASSERT_EQ(UCC_OK, put_cpu_executor(executor));
}

/*
 * Multi-thread stress: N threads each create their own CPU executor
 * (per-thread progress, mirroring the one-executor-thread-per-comm
 * usage) and post K large reduces through the shared process-wide
 * worker pool.  Each thread is single-in-flight (post -> wait ->
 * finalize, then the next task) and reuses its own dst slot, so there
 * are no data dependencies between tasks; the worker pool and the
 * executor-task mpool (spin-locked under UCC_THREAD_MULTIPLE) are the
 * shared state under test.
 */
struct stress_arg {
    ucc_ee_executor_t *executor;
    int                tid;
    int                n_ok;
    int                n_bad;
};

static void *stress_worker(void *arg)
{
    struct stress_arg *a = (struct stress_arg *)arg;
    int                tid = a->tid;
    int                j;

    for (j = 0; j < STRESS_TASKS; j++) {
        ucc_ee_executor_task_args_t args;
        ucc_ee_executor_task_t *    task = NULL;
        ucc_status_t                status;

        memset(&args, 0, sizeof(args));
        args.task_type      = UCC_EE_EXECUTOR_TASK_REDUCE;
        args.reduce.dst     = dst_stress[tid];
        args.reduce.count   = STRESS_COUNT;
        args.reduce.dt      = UCC_DT_FLOAT32;
        args.reduce.op      = UCC_OP_SUM;
        args.reduce.n_srcs  = 4;
        args.reduce.srcs[0] = src[0];
        args.reduce.srcs[1] = src[1];
        args.reduce.srcs[2] = src[2];
        args.reduce.srcs[3] = src[3];

        status = ucc_ee_executor_task_post(a->executor, &args, &task);
        if (UCC_OK != status) {
            a->n_bad++;
            continue;
        }
        status = UCC_INPROGRESS;
        while (UCC_INPROGRESS == (status = ucc_ee_executor_task_test(task))) {
        }
        if (UCC_OK != status) {
            a->n_bad++;
            ucc_ee_executor_task_finalize(task);
            continue;
        }
        if (!check_f32(dst_stress[tid], STRESS_COUNT)) {
            a->n_bad++;
        } else {
            a->n_ok++;
        }
        if (UCC_OK != ucc_ee_executor_task_finalize(task)) {
            a->n_bad++;
        }
    }
    return NULL;
}

UCC_TEST_F(test_ec_cpu_threaded, ec_cpu_threaded_multithread_stress)
{
    pthread_t            th[STRESS_N_THREADS];
    ucc_ee_executor_t   *executor[STRESS_N_THREADS];
    struct stress_arg    a[STRESS_N_THREADS];
    int                  ok_total  = 0;
    int                  bad_total = 0;
    int                  i;

    if (!threaded_reduce_enabled()) {
        GTEST_SKIP();
    }

    fill_f32();
    for (i = 0; i < STRESS_N_THREADS; i++) {
        ASSERT_EQ(UCC_OK, get_cpu_executor(&executor[i]));
        ASSERT_EQ(UCC_OK, ucc_ee_executor_start(executor[i], nullptr));
        a[i].executor = executor[i];
        a[i].tid      = i;
        a[i].n_ok     = 0;
        a[i].n_bad    = 0;
    }

    for (i = 0; i < STRESS_N_THREADS; i++) {
        ASSERT_EQ(0, pthread_create(&th[i], nullptr, stress_worker, &a[i]));
    }
    for (i = 0; i < STRESS_N_THREADS; i++) {
        ASSERT_EQ(0, pthread_join(th[i], nullptr));
    }
    for (i = 0; i < STRESS_N_THREADS; i++) {
        ok_total  += a[i].n_ok;
        bad_total += a[i].n_bad;
        EXPECT_EQ(UCC_OK, ucc_ee_executor_stop(executor[i]));
        ASSERT_EQ(UCC_OK, put_cpu_executor(executor[i]));
    }

    EXPECT_EQ(STRESS_N_THREADS * STRESS_TASKS, ok_total);
    EXPECT_EQ(0, bad_total);
}

#else /* HAVE_EC_THREADED_REDUCE */

/* Built without --enable-ec-threaded-reduce: nothing to test. */
class test_ec_cpu_threaded : public ucc::test {
};

UCC_TEST_F(test_ec_cpu_threaded, skipped)
{
    GTEST_SKIP();
}

#endif /* HAVE_EC_THREADED_REDUCE */
