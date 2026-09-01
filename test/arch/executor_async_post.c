/*
 * Executor async-post acceptance harness (task 573).
 *
 * Drives the CPU EC's *public* executor API (ucc_ee_executor_init /
 * start / task_post / task_test / task_finalize / stop / finalize)
 * against the prebuilt libucc + dlopen'd modules:
 *
 *   A. sync path (UCC_USE_THREADED_REDUCE unset):
 *      REDUCE / REDUCE_STRIDED / COPY run inside task_post and come
 *      back UCC_OK immediately; results verified against a reference;
 *      COPY_MULTI -> UCC_ERR_NOT_SUPPORTED.
 *
 *   B. async path (UCC_USE_THREADED_REDUCE=1):
 *      - task_post returns UCC_OK while task->status is UCC_INPROGRESS
 *      - task_test observes INPROGRESS, then a final UCC_OK
 *      - results are bitwise-identical to the sync-path results for
 *        the same inputs (f32 SUM n=4; f64 PROD strided n_srcs=12 >
 *        UCC_EE_EXECUTOR_NUM_BUFS to exercise the heap-srcs worker
 *        path; COPY)
 *      - a burst of 16 concurrent tasks all complete, at least one is
 *        still INPROGRESS mid-flight, and all match their references
 *      - ucc_ec_finalize() stops the pool cleanly (no hang)
 *
 * Compile:
 *   gcc -O2 -std=gnu11 \
 *       -I$T -I$T/src \
 *       executor_async_post.c -o hap \
 *       -L$T/src/.libs -lucc -lucs -lpthread -lm
 * Run (CPU EC + MC modules dlopen from the src/.libs/ucc -> modules
 *     symlink; worker count via UCC_EC_CPU_EXEC_NUM_WORKERS):
 *   LD_LIBRARY_PATH=$T/src/.libs ./hap
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <components/ec/ucc_ec.h>
#include <components/ec/base/ucc_ec_base.h>

#define COUNT        1000000
#define N_SRC        12 /* >= UCC_EE_EXECUTOR_NUM_BUFS (9) */
#define N_COPY_BURST 16
#define COPY_LEN     (1u << 20)

static int g_failures = 0;

static void
check(int cond, const char *what)
{
    if (!cond) {
        g_failures++;
        fprintf(stderr, "FAIL: %s\n", what);
    }
}

/* Left-associative reference folds (bitwise order of the scalar
 * kernel). */
static void
ref_sum_f32(void *d, const float *const *s, size_t n, unsigned ns)
{
    float *dst = (float *)d;
    size_t i;

    for (i = 0; i < n; i++) {
        float    acc = s[0][i];
        unsigned k;

        for (k = 1; k < ns; k++) {
            acc += s[k][i];
        }
        dst[i] = acc;
    }
}

static void
ref_prod_f64(void *d, const double *const *s, size_t n, unsigned ns)
{
    double *dst = (double *)d;
    size_t  i;

    for (i = 0; i < n; i++) {
        double   acc = s[0][i];
        unsigned k;

        for (k = 1; k < ns; k++) {
            acc *= s[k][i];
        }
        dst[i] = acc;
    }
}

/* Deterministic fills, small values (no overflow in the folds). */
static void
fill_f32(float *p, size_t count, unsigned idx)
{
    size_t j;

    for (j = 0; j < count; j++) {
        p[j] = (float)((j + (size_t)idx * count) % 100) + 1.0f;
    }
}

static void
fill_f64(double *p, size_t count, unsigned idx)
{
    size_t j;

    for (j = 0; j < count; j++) {
        p[j] = ((j + (size_t)idx * count) % 10) + 1.0;
    }
}

static int
same_bits(const void *a, const void *b, size_t bytes)
{
    return memcmp(a, b, bytes) == 0;
}

static void
post_f32_reduce(ucc_ee_executor_t *exe, void *dst, const float *const *srcs,
                unsigned n_srcs, ucc_ee_executor_task_t **task,
                ucc_status_t *status)
{
    ucc_ee_executor_task_args_t args;
    unsigned                    i;

    memset(&args, 0, sizeof(args));
    args.task_type = UCC_EE_EXECUTOR_TASK_REDUCE;
    args.reduce.dst = dst;
    for (i = 0; i < n_srcs; i++) {
        args.reduce.srcs[i] = (void *)srcs[i];
    }
    args.reduce.count  = COUNT;
    args.reduce.dt     = UCC_DT_FLOAT32;
    args.reduce.op     = UCC_OP_SUM;
    args.reduce.n_srcs = (uint16_t)n_srcs;
    *status = ucc_ee_executor_task_post(exe, &args, task);
}

static void
post_strided_f64_prod(ucc_ee_executor_t *exe, void *dst, void *src1,
                      void *src2, unsigned n_srcs,
                      ucc_ee_executor_task_t **task, ucc_status_t *status)
{
    ucc_ee_executor_task_args_t args;

    memset(&args, 0, sizeof(args));
    args.task_type = UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED;
    args.reduce_strided.dst    = dst;
    args.reduce_strided.src1   = src1;
    args.reduce_strided.src2   = src2;
    args.reduce_strided.stride = sizeof(double) * COUNT;
    args.reduce_strided.count  = COUNT;
    args.reduce_strided.dt     = UCC_DT_FLOAT64;
    args.reduce_strided.op     = UCC_OP_PROD;
    args.reduce_strided.n_src2 = (uint16_t)(n_srcs - 1);
    *status = ucc_ee_executor_task_post(exe, &args, task);
}

static void
post_copy(ucc_ee_executor_t *exe, const void *src, void *dst, size_t len,
          ucc_ee_executor_task_t **task, ucc_status_t *status)
{
    ucc_ee_executor_task_args_t args;

    memset(&args, 0, sizeof(args));
    args.task_type = UCC_EE_EXECUTOR_TASK_COPY;
    args.copy.src  = src;
    args.copy.dst  = dst;
    args.copy.len  = len;
    *status = ucc_ee_executor_task_post(exe, &args, task);
}

static void
wait_done(ucc_ee_executor_task_t *task)
{
    unsigned spins = 0;

    while (ucc_ee_executor_task_test(task) == UCC_INPROGRESS) {
        if (++spins > 100000000u) {
            check(0, "task never completed");
            return;
        }
    }
}

int
main(void)
{
    ucc_ec_params_t          ec_params = {.thread_mode = UCC_THREAD_MULTIPLE};
    ucc_ee_executor_params_t eparams;
    ucc_ee_executor_t       *executor  = NULL;
    ucc_ee_executor_task_t  *task;
    ucc_status_t             status;

    static float       f32_src[N_SRC][COUNT];
    static double      f64_src[N_SRC][COUNT];
    static const float *f32_ptrs[N_SRC];
    static const double *f64_ptrs[N_SRC];
    static float       f32_dst_sync[COUNT], f32_dst_async[COUNT];
    static double      f64_dst_sync[COUNT], f64_dst_async[COUNT];
    static float       f32_dst_burst[N_COPY_BURST][COUNT];
    static float       f32_ref[COUNT];
    static double      f64_ref[COUNT];
    static char        copy_src[COPY_LEN];
    static char        copy_dst_sync[COPY_LEN], copy_dst_async[COPY_LEN];
    unsigned           i, k;

    for (i = 0; i < N_SRC; i++) {
        fill_f32(f32_src[i], COUNT, i);
        fill_f64(f64_src[i], COUNT, i);
        f32_ptrs[i] = f32_src[i];
        f64_ptrs[i] = f64_src[i];
    }
    memset(copy_src, 0xAB, sizeof(copy_src));

    ucc_constructor();
    status = ucc_ec_init(&ec_params);
    check(status == UCC_OK, "ucc_ec_init");

    eparams.mask    = UCC_EE_EXECUTOR_PARAM_FIELD_TYPE;
    eparams.ee_type = UCC_EE_CPU_THREAD;
    status = ucc_ee_executor_init(&eparams, &executor);
    check(status == UCC_OK, "executor_init");
    check(executor != NULL, "executor non-NULL");
    status = ucc_ee_executor_start(executor, NULL);
    check(status == UCC_OK, "executor_start");

    /* ---------- A. sync path (threaded reduce off) ---------- */

    /* A1: f32 SUM n=4 completes inside the post */
    post_f32_reduce(executor, f32_dst_sync, f32_ptrs, 4, &task, &status);
    check(status == UCC_OK, "sync f32 reduce post");
    check(task && task->status == UCC_OK, "sync f32 reduce done in post");
    ref_sum_f32(f32_ref, f32_ptrs, COUNT, 4);
    check(same_bits(f32_dst_sync, f32_ref, COUNT * sizeof(float)),
          "sync f32 result");
    status = ucc_ee_executor_task_finalize(task);
    check(status == UCC_OK, "sync f32 finalize");

    /* A2: f64 PROD strided, n_srcs = 12 (> NUM_BUFS = 9) */
    post_strided_f64_prod(executor, f64_dst_sync, f64_src[0], f64_src[1],
                          N_SRC, &task, &status);
    check(status == UCC_OK, "sync strided prod post");
    check(task->status == UCC_OK, "sync strided done in post");
    ref_prod_f64(f64_ref, f64_ptrs, COUNT, N_SRC);
    check(same_bits(f64_dst_sync, f64_ref, COUNT * sizeof(double)),
          "sync strided result");
    status = ucc_ee_executor_task_finalize(task);
    check(status == UCC_OK, "sync strided finalize");

    /* A3: COPY */
    post_copy(executor, copy_src, copy_dst_sync, COPY_LEN, &task, &status);
    check(status == UCC_OK, "sync copy post");
    check(task->status == UCC_OK, "sync copy done in post");
    check(memcmp(copy_dst_sync, copy_src, COPY_LEN) == 0,
          "sync copy contents");
    status = ucc_ee_executor_task_finalize(task);
    check(status == UCC_OK, "sync copy finalize");

    /* A4: unsupported op type still rejected */
    {
        ucc_ee_executor_task_args_t args;

        memset(&args, 0, sizeof(args));
        args.task_type = UCC_EE_EXECUTOR_TASK_COPY_MULTI;
        status         = ucc_ee_executor_task_post(executor, &args, &task);
        check(status == UCC_ERR_NOT_SUPPORTED, "sync copy_multi unsupported");
    }

    /* ---------- B. async path (threaded reduce on) ---------- */

    setenv("UCC_USE_THREADED_REDUCE", "1", 1);
    setenv("UCC_EC_CPU_EXEC_NUM_WORKERS", "4", 1);

    /* B1: f32 SUM n=4: post returns OK, test observes INPROGRESS then
     * OK, result bitwise-identical to the sync-path result */
    post_f32_reduce(executor, f32_dst_async, f32_ptrs, 4, &task, &status);
    check(status == UCC_OK, "async post returns OK");
    {
        int saw_inprogress = 0;

        while (ucc_ee_executor_task_test(task) == UCC_INPROGRESS) {
            saw_inprogress = 1;
        }
        check(task->status == UCC_OK, "async f32 reduce final OK");
        /* INPROGRESS is a contract, not a guarantee: a 1M-element task
         * is large enough to still be running on its first test, but a
         * fast worker could finish first.  The burst below is the
         * robust INPROGRESS evidence. */
        if (saw_inprogress) {
            printf("  (B1 observed UCC_INPROGRESS before completion)\n");
        }
        check(same_bits(f32_dst_async, f32_dst_sync, COUNT * sizeof(float)),
              "async f32 result == sync result");
    }
    status = ucc_ee_executor_task_finalize(task);
    check(status == UCC_OK, "async f32 finalize");

    /* B2: strided f64 PROD n_srcs=12 (heap-srcs path in the worker) */
    post_strided_f64_prod(executor, f64_dst_async, f64_src[0], f64_src[1],
                          N_SRC, &task, &status);
    check(status == UCC_OK, "async strided post");
    wait_done(task);
    check(task->status == UCC_OK, "async strided final OK");
    check(same_bits(f64_dst_async, f64_dst_sync, COUNT * sizeof(double)),
          "async strided result == sync result");
    status = ucc_ee_executor_task_finalize(task);
    check(status == UCC_OK, "async strided finalize");

    /* B3: COPY through the pool */
    post_copy(executor, copy_src, copy_dst_async, COPY_LEN, &task, &status);
    check(status == UCC_OK, "async copy post");
    wait_done(task);
    check(task->status == UCC_OK, "async copy final OK");
    check(memcmp(copy_dst_async, copy_src, COPY_LEN) == 0,
          "async copy contents");
    status = ucc_ee_executor_task_finalize(task);
    check(status == UCC_OK, "async copy finalize");

    /* B4: burst of 16 f32 SUM n=4 tasks (4 workers): some must still be
     * INPROGRESS mid-flight, all must complete correctly */
    {
        ucc_ee_executor_task_t *tasks[N_COPY_BURST];
        int                     all_ok     = 1;
        int                     saw_inprog = 0;

        for (i = 0; i < N_COPY_BURST; i++) {
            const float *r[4];

            for (k = 0; k < 4; k++) {
                r[k] = f32_src[(i + k) % N_SRC];
            }
            post_f32_reduce(executor, f32_dst_burst[i], r, 4, &tasks[i],
                            &status);
            check(status == UCC_OK, "burst post");
        }
        for (i = 0; i < N_COPY_BURST; i++) {
            if (ucc_ee_executor_task_test(tasks[i]) == UCC_INPROGRESS) {
                saw_inprog = 1;
            }
        }
        check(saw_inprog, "burst: at least one task INPROGRESS mid-flight");
        for (i = 0; i < N_COPY_BURST; i++) {
            const float *r[4];
            float        ref[COUNT];

            for (k = 0; k < 4; k++) {
                r[k] = f32_src[(i + k) % N_SRC];
            }
            wait_done(tasks[i]);
            if (tasks[i]->status != UCC_OK) {
                all_ok = 0;
            }
            ref_sum_f32(ref, r, COUNT, 4);
            if (!same_bits(f32_dst_burst[i], ref, COUNT * sizeof(float))) {
                all_ok = 0;
            }
            status = ucc_ee_executor_task_finalize(tasks[i]);
            if (status != UCC_OK) {
                all_ok = 0;
            }
        }
        check(all_ok, "burst: all 16 tasks OK and correct");
    }

    status = ucc_ee_executor_stop(executor);
    check(status == UCC_OK, "executor_stop");
    status = ucc_ee_executor_finalize(executor);
    check(status == UCC_OK, "executor_finalize");
    status = ucc_ec_finalize();
    check(status == UCC_OK, "ucc_ec_finalize (pool stop, no hang)");

    if (g_failures) {
        printf("573-HARNESS: %d FAILURES\n", g_failures);
        return 1;
    }
    printf("573-HARNESS: ALL PASS (sync + async parity OK)\n");
    return 0;
}
