/*
 * Milestone A gate benchmark (task 576) — A3 (overlap) + A4 (latency).
 *
 * ucc_perftest's executor loop is strictly single-in-flight and sequential
 * (post -> spin task_test until done -> finalize -> next), so it cannot
 * express A3's "sustained post->complete throughput" with concurrency.
 * This harness drives the CPU executor directly.  Task shape is f32 SUM,
 * count x N_SRC (count defaults to 1M).
 *
 *   mode 0  sync        sequential post (completes inside post): the
 *                        "single-thread synchronous rate" reference.
 *   mode 1  async1      single-flight async post -> poll -> finalize:
 *                        per-task latency distribution (A4 median).
 *   mode 2  pipeline W  keep W async tasks in flight (W =
 *                        EXEC_NUM_WORKERS); sustained post->complete
 *                        throughput (A3) vs mode 0's per-task rate.
 *   mode 4  a3fair W D  run the sync reference (mode 0) AND the pipeline
 *                        (mode 2) back to back in one process, both
 *                        rotating D dst buffers, so the working set is
 *                        identical (the DRAM-bound vs L3-resident effect
 *                        is the dominant term at 1M; a fair A3 must match
 *                        it).  Prints both per-task times.
 *   mode 3  multi P     P producer threads, each with its own executor
 *                        (created by main; the EC is init/finalized once
 *                        by main, mirroring the gtest stress), posting K
 *                        tasks each; sustained throughput (A3 cross-check).
 *
 * Every task's timed region is post .. task_test==UCC_OK (finalize
 * excluded), matching the "post->complete" wording of A3/A4.  Every
 * completed task's result is checked against the deterministic fill
 * before its task is finalized, so a wrong reduction fails the run.
 * Each in-flight task in pipeline/multi mode has its own dst, so no two
 * concurrent reductions share a buffer.  Buffers are file-scope BSS.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <math.h>
#include <pthread.h>

#include <components/ec/ucc_ec.h>
#include <components/ec/base/ucc_ec_base.h>

#define COUNT    1000000
#define N_SRC    4
#define ITERS    100
#define WARMUP   20
#define MAX_W    64
#define MULTI_P  8
#define MULTI_K  8
#define MULTI_N  262144 /* elements per multi task (1 MB) */

static int   g_failures = 0;
static size_t g_count   = COUNT; /* task element count */
static int   g_dsts     = 1;     /* dst buffers rotated by sync/pipeline */

static void
check(int cond, const char *what)
{
    if (!cond) {
        fprintf(stderr, "FAIL: %s\n", what);
        g_failures++;
    }
}

static double
now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

static void
fill_f32(float *p, size_t count, unsigned idx)
{
    size_t i;
    for (i = 0; i < count; i++) {
        p[i] = (float)((i + idx) % 97) - 48.f;
    }
}

static float
ref_f32(size_t i, unsigned n)
{
    float    s = 0.f;
    unsigned k;
    for (k = 0; k < n; k++) {
        s += (float)((i + k) % 97) - 48.f;
    }
    return s;
}

static int
check_buf(const float *dst, size_t count)
{
    size_t i;
    for (i = 0; i < count; i += 997) {
        float r = ref_f32(i, N_SRC);
        if (memcmp(&dst[i], &r, sizeof(float)) != 0) {
            return 0;
        }
    }
    return 1;
}

static void
post_reduce(ucc_ee_executor_t *exe, float *dst, const float *const *srcs,
            unsigned count, ucc_ee_executor_task_t **task,
            ucc_status_t *status)
{
    ucc_ee_executor_task_args_t args;
    unsigned                    i;

    memset(&args, 0, sizeof(args));
    args.task_type      = UCC_EE_EXECUTOR_TASK_REDUCE;
    args.reduce.dst     = dst;
    args.reduce.count   = count;
    args.reduce.dt      = UCC_DT_FLOAT32;
    args.reduce.op      = UCC_OP_SUM;
    args.reduce.n_srcs  = (uint16_t)N_SRC;
    for (i = 0; i < N_SRC; i++) {
        args.reduce.srcs[i] = (void *)srcs[i];
    }
    *status = ucc_ee_executor_task_post(exe, &args, task);
}

static ucc_status_t
poll_ok(ucc_ee_executor_task_t *task)
{
    ucc_status_t st;
    while ((st = ucc_ee_executor_task_test(task)) == UCC_INPROGRESS) {
    }
    return st;
}

static int
dbl_cmp(const void *pa, const void *pb)
{
    double a = *(const double *)pa, b = *(const double *)pb;
    return (a > b) - (a < b);
}

static void
stats(const double *a, size_t n, double *mn, double *md, double *mx)
{
    double *b = malloc(n * sizeof(*b));
    size_t   i;
    check(b != NULL, "stats malloc");
    if (!b) {
        return;
    }
    for (i = 0; i < n; i++) {
        b[i] = a[i];
    }
    qsort(b, n, sizeof(*b), dbl_cmp);
    *mn = b[0];
    *mx = b[n - 1];
    *md = (n % 2) ? b[n / 2] : (b[n / 2 - 1] + b[n / 2]) / 2.0;
    free(b);
}

/* file-scope buffers (BSS).  dst_rot[] is shared by the sync reference
 * and the pipeline so a fair A3 can give them the same working set;
 * each in-flight pipeline slot has a distinct dst so no two concurrent
 * reductions share a buffer. */
static float       src[N_SRC][COUNT];
static const float *srcs[N_SRC];
static float       dst_rot[MAX_W][COUNT];

static float       src_multi[N_SRC][MULTI_N];
static const float *srcs_multi[N_SRC];
static float       dst_multi[MULTI_P][MULTI_K][MULTI_N];

/* ---- mode 0: sync sequential ------------------------------------------- */
static double
run_sync(ucc_ee_executor_t *exe)
{
    ucc_ee_executor_task_t *task;
    ucc_status_t            status;
    double                  lat[ITERS], mn, md, mx, total = 0.0;
    int                     i;

    check(g_dsts >= 1 && g_dsts <= MAX_W, "sync dsts in range");
    /* Sequential single-in-flight: post, then poll to completion.
     * With USE=0 the reduce completes inside the post (status UCC_OK);
     * with USE=1 (mode 4, pool active) it goes through the pool and we
     * poll — either way this is the "caller blocked on the reduce"
     * reference, exactly what perftest's executor loop does. */
    for (i = 0; i < WARMUP + ITERS; i++) {
        float   *dst = dst_rot[i % g_dsts];
        double   t0 = now_us();
        post_reduce(exe, dst, srcs, (unsigned)g_count, &task, &status);
        check(status == UCC_OK, "sync post");
        check(poll_ok(task) == UCC_OK, "sync test ok");
        if (i >= WARMUP) {
            lat[i - WARMUP] = now_us() - t0;
            total += lat[i - WARMUP];
        }
        status = ucc_ee_executor_task_finalize(task);
        check(status == UCC_OK, "sync finalize");
    }
    check(check_buf(dst_rot[(WARMUP + ITERS - 1) % g_dsts], g_count),
          "sync results");
    stats(lat, ITERS, &mn, &md, &mx);
    printf("sync     per-task avg=%.2f med=%.2f min=%.2f max=%.2f us "
           "(n=%d, count=%zu, dsts=%d)\n",
           total / ITERS, md, mn, mx, ITERS, g_count, g_dsts);
    return total;
}

/* ---- mode 1: async single-flight latency (A4) -------------------------- */
static double
run_async1(ucc_ee_executor_t *exe)
{
    ucc_ee_executor_task_t *task;
    ucc_status_t            status;
    double                  lat[ITERS], mn, md, mx, total = 0.0;
    int                     i;

    for (i = 0; i < WARMUP + ITERS; i++) {
        float  *dst = dst_rot[i % g_dsts];
        double t0   = now_us();
        post_reduce(exe, dst, srcs, (unsigned)g_count, &task, &status);
        check(status == UCC_OK, "async post");
        check(poll_ok(task) == UCC_OK, "async test ok");
        if (i >= WARMUP) {
            lat[i - WARMUP] = now_us() - t0;
            total += lat[i - WARMUP];
        }
        status = ucc_ee_executor_task_finalize(task);
        check(status == UCC_OK, "async finalize");
    }
    check(check_buf(dst_rot[(WARMUP + ITERS - 1) % g_dsts], g_count),
          "async1 results");
    stats(lat, ITERS, &mn, &md, &mx);
    printf("async1   per-task avg=%.2f med=%.2f min=%.2f max=%.2f us "
           "(n=%d, count=%zu, dsts=%d)\n",
           total / ITERS, md, mn, mx, ITERS, g_count, g_dsts);
    return total;
}

/* ---- mode 2: pipelined W in flight (A3) -------------------------------- */
static double
run_pipeline(ucc_ee_executor_t *exe, int W)
{
    struct win {
        ucc_ee_executor_task_t *task;
        float                  *dst;
        double                  t_post;
    } win[MAX_W];
    ucc_status_t status;
    int          head = 0, tail = 0; /* [head..tail) occupied */
    int          posted = 0, done = 0;
    double       wall, mn, md, mx, total = 0.0;
    static double lat[ITERS];
    int          i;

    check(W >= 1 && W <= MAX_W, "pipeline W in range");
    check(g_dsts >= W, "pipeline dsts >= W (one per in-flight task)");
    wall = now_us();
    while (done < ITERS) {
        /* top up to W in flight; each slot has a private dst */
        while (tail - head < W && posted < ITERS) {
            post_reduce(exe, dst_rot[tail % g_dsts], srcs,
                        (unsigned)g_count, &win[tail % MAX_W].task,
                        &status);
            check(status == UCC_OK, "pipe post");
            win[tail % MAX_W].dst = dst_rot[tail % g_dsts];
            win[tail % MAX_W].t_post = now_us();
            tail++;
            posted++;
        }
        /* block on the oldest in-flight task (FIFO) */
        {
            struct win *w = &win[head % MAX_W];
            check(poll_ok(w->task) == UCC_OK, "pipe test ok");
            if (done >= WARMUP) {
                lat[done - WARMUP] = now_us() - w->t_post;
                total += lat[done - WARMUP];
            }
            check(check_buf(w->dst, g_count), "pipe results");
            status = ucc_ee_executor_task_finalize(w->task);
            check(status == UCC_OK, "pipe finalize");
            head++;
            done++;
        }
    }
    wall = now_us() - wall;
    stats(lat, ITERS - WARMUP, &mn, &md, &mx);
    printf("pipeline W=%d  count=%zu dsts=%d  %d tasks: wall=%.2f us -> "
           "%.2f tasks/ms; per-task post->ok avg=%.2f med=%.2f min=%.2f "
           "max=%.2f us\n",
           W, g_count, g_dsts, ITERS, wall, ITERS / (wall / 1000.0),
           total / (ITERS - WARMUP), md, mn, mx);
    return wall;
}

/* ---- mode 4: fair A3 (sync + pipeline, same working set, one proc) ---- */
static void
run_a3fair(ucc_ee_executor_t *exe, int W)
{
    run_sync(exe);
    run_pipeline(exe, W);
}

/* ---- mode 3: multi-producer (A3 cross-check) --------------------------- */
struct mprod_arg {
    ucc_ee_executor_t *executor;
    int                n_tasks;
    const float      **srcs;
    float            (*dsts)[MULTI_N]; /* [n_tasks][MULTI_N] */
};

static void *
mprod_worker(void *arg)
{
    struct mprod_arg *a = (struct mprod_arg *)arg;
    int               i;

    for (i = 0; i < a->n_tasks; i++) {
        ucc_ee_executor_task_t *task;
        ucc_status_t            status;
        post_reduce(a->executor, a->dsts[i], a->srcs, MULTI_N, &task,
                    &status);
        check(status == UCC_OK, "mprod post");
        check(poll_ok(task) == UCC_OK, "mprod test");
        check(check_buf(a->dsts[i], MULTI_N), "mprod results");
        status = ucc_ee_executor_task_finalize(task);
        check(status == UCC_OK, "mprod finalize");
    }
    return NULL;
}

int
main(int argc, char **argv)
{
    int mode = (argc > 1) ? atoi(argv[1]) : 0;
    ucc_ec_params_t          ec_params = {.thread_mode = UCC_THREAD_MULTIPLE};
    ucc_ee_executor_params_t eparams;
    ucc_ee_executor_t        *exe = NULL;
    ucc_status_t              status;
    unsigned                  i;

    if (mode == 0 || mode == 1) {
        /* argv: <mode> [count] [dsts] */
        if (argc > 2) {
            g_count = (size_t)atol(argv[2]);
        }
        if (argc > 3) {
            g_dsts = atoi(argv[3]);
        }
    } else if (mode == 2) {
        /* argv: <mode> W [count] [dsts]  (dsts must be >= W) */
        if (argc > 3) {
            g_count = (size_t)atol(argv[3]);
        }
        if (argc > 4) {
            g_dsts = atoi(argv[4]);
        } else if (argc > 2) {
            int W = atoi(argv[2]);
            if (g_dsts < W) {
                g_dsts = W;
            }
        }
    } else if (mode == 4) {
        /* argv: <mode> W D [count] */
        if (argc > 3) {
            g_dsts = atoi(argv[3]);
        }
        if (argc > 4) {
            g_count = (size_t)atol(argv[4]);
        }
    } else if (mode == 3) {
        /* argv: <mode> P [count] */
    } else {
        fprintf(stderr, "unknown mode %d\n", mode);
        return 2;
    }
    if (g_count > COUNT) {
        g_count = COUNT;
    }
    if (g_dsts < 1) {
        g_dsts = 1;
    }
    if (g_dsts > MAX_W) {
        g_dsts = MAX_W;
    }

    for (i = 0; i < N_SRC; i++) {
        fill_f32(src[i], COUNT, i);
        srcs[i] = src[i];
        fill_f32(src_multi[i], MULTI_N, i);
        srcs_multi[i] = src_multi[i];
    }

    ucc_constructor();
    status = ucc_ec_init(&ec_params);
    check(status == UCC_OK, "ucc_ec_init");

    if (mode == 3) {
        int              nthreads = (argc > 2) ? atoi(argv[2]) : MULTI_P;
        pthread_t        th[MULTI_P];
        struct mprod_arg args[MULTI_P];
        ucc_ee_executor_t *exes[MULTI_P];
        int              p;
        double           t0, t1;
        check(nthreads >= 1 && nthreads <= MULTI_P, "multi P in range");
        for (p = 0; p < nthreads; p++) {
            eparams.mask    = UCC_EE_EXECUTOR_PARAM_FIELD_TYPE;
            eparams.ee_type = UCC_EE_CPU_THREAD;
            status = ucc_ee_executor_init(&eparams, &exes[p]);
            check(status == UCC_OK, "multi exe init");
            status = ucc_ee_executor_start(exes[p], NULL);
            check(status == UCC_OK, "multi exe start");
        }
        t0 = now_us();
        for (p = 0; p < nthreads; p++) {
            args[p].executor = exes[p];
            args[p].n_tasks = MULTI_K;
            args[p].srcs = srcs_multi;
            args[p].dsts = dst_multi[p];
            pthread_create(&th[p], NULL, mprod_worker, &args[p]);
        }
        for (p = 0; p < nthreads; p++) {
            pthread_join(th[p], NULL);
        }
        t1 = now_us();
        for (p = 0; p < nthreads; p++) {
            ucc_ee_executor_stop(exes[p]);
            ucc_ee_executor_finalize(exes[p]);
        }
        printf("multi    P=%d K=%d count=%d: %d tasks in %.2f us -> "
               "%.2f tasks/ms (mean %.2f us/task)\n",
               nthreads, MULTI_K, MULTI_N, nthreads * MULTI_K, t1 - t0,
               nthreads * MULTI_K / ((t1 - t0) / 1000.0),
               (t1 - t0) / (nthreads * MULTI_K));
    } else {
        eparams.mask    = UCC_EE_EXECUTOR_PARAM_FIELD_TYPE;
        eparams.ee_type = UCC_EE_CPU_THREAD;
        status = ucc_ee_executor_init(&eparams, &exe);
        check(status == UCC_OK, "executor_init");
        status = ucc_ee_executor_start(exe, NULL);
        check(status == UCC_OK, "executor_start");

        if (mode == 0) {
            run_sync(exe);
        } else if (mode == 1) {
            run_async1(exe);
        } else if (mode == 2) {
            int W = (argc > 2) ? atoi(argv[2]) : 4;
            run_pipeline(exe, W);
        } else if (mode == 4) {
            int W = (argc > 2) ? atoi(argv[2]) : 4;
            run_a3fair(exe, W);
        } else {
            fprintf(stderr, "unknown mode %d\n", mode);
            return 2;
        }

        ucc_ee_executor_stop(exe);
        ucc_ee_executor_finalize(exe);
    }

    ucc_ec_finalize();

    if (g_failures) {
        printf("576-BENCH: %d FAILURES\n", g_failures);
        return 1;
    }
    printf("576-BENCH: ALL PASS\n");
    return 0;
}
