/*
 * 576 diagnostic — isolate kernel scaling and raw-thread parallel
 * scaling, independent of the executor/pool.
 *
 *   (1) direct ucc_ec_cpu_reduce timing at several sizes (n=4 f32 sum),
 *       single thread: the kernel's size scaling.
 *   (2) N raw pthreads, each looping a fixed-size reduction: how the
 *       kernel scales with concurrent threads (no pool, no queue).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#include "components/ec/cpu/ec_cpu.h"

static double
now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

#define NSRC 4

static void
do_reduce(size_t count, float *dst, const float *const *srcs)
{
    ucc_eee_task_reduce_t tr;
    memset(&tr, 0, sizeof(tr));
    tr.dst    = dst;
    tr.count  = count;
    tr.dt     = UCC_DT_FLOAT32;
    tr.op     = UCC_OP_SUM;
    tr.n_srcs = NSRC;
    tr.srcs[0] = (void *)srcs[0];
    tr.srcs[1] = (void *)srcs[1];
    tr.srcs[2] = (void *)srcs[2];
    tr.srcs[3] = (void *)srcs[3];
    ucc_ec_cpu_reduce(&tr, dst, (void *const *)srcs, 0);
}

static double
time_reduce(size_t count, size_t iters)
{
    static float      src[NSRC][1 << 20];
    static float      dst[1 << 20];
    const float     *srcs[NSRC];
    size_t           i;
    for (i = 0; i < NSRC; i++) {
        srcs[i] = src[i];
    }
    for (i = 0; i < NSRC * (size_t)(1 << 20); i++) {
        src[(i / (1 << 20)) % NSRC][i % (1 << 20)] = (float)(i % 97) - 48.f;
    }
    {
        size_t w;
        for (w = 0; w < 5; w++) {
            do_reduce(count, dst, srcs);
        }
    }
    {
        double t0 = now_us();
        for (i = 0; i < iters; i++) {
            do_reduce(count, dst, srcs);
        }
        return (now_us() - t0) / iters;
    }
}

struct parg {
    int    iters;
    size_t count;
    double per;
};

static void *
pworker(void *a)
{
    struct parg       *p = (struct parg *)a;
    static __thread float src[NSRC][1 << 20];
    static __thread float dst[1 << 20];
    const float       *srcs[NSRC];
    size_t             i, w;
    for (i = 0; i < NSRC; i++) {
        srcs[i] = src[i];
    }
    for (i = 0; i < NSRC * (size_t)(1 << 20); i++) {
        src[(i / (1 << 20)) % NSRC][i % (1 << 20)] = (float)(i % 97) - 48.f;
    }
    for (w = 0; w < 3; w++) {
        do_reduce(p->count, dst, srcs);
    }
    {
        double t0 = now_us();
        for (i = 0; i < p->iters; i++) {
            do_reduce(p->count, dst, srcs);
        }
        p->per = (now_us() - t0) / p->iters;
    }
    return NULL;
}

int
main(void)
{
    size_t sizes[] = {1024, 65536, 262144, 1048576};
    size_t i;
    printf("== (1) direct kernel, n=4 f32 sum, single thread ==\n");
    for (i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++) {
        double per = time_reduce(sizes[i], 50);
        double bw  = (double)(NSRC + 1) * sizes[i] * 4 / per / 1000.0;
        printf("  count=%-8zu per=%.2f us  bw=%.1f GB/s\n", sizes[i], per,
               bw);
    }

    printf("== (2) raw N threads, 1M n=4 f32 sum, 30 iters each ==\n");
    int    nt[] = {1, 2, 4, 8, 16, 32};
    size_t j;
    for (j = 0; j < sizeof(nt) / sizeof(nt[0]); j++) {
        int         n = nt[j];
        pthread_t   th[64];
        struct parg a[64];
        double      wall, per_max;
        int         k;
        for (k = 0; k < n; k++) {
            a[k].iters = 30;
            a[k].count = 1048576;
            a[k].per   = 0;
        }
        wall = now_us();
        for (k = 0; k < n; k++) {
            pthread_create(&th[k], NULL, pworker, &a[k]);
        }
        for (k = 0; k < n; k++) {
            pthread_join(th[k], NULL);
        }
        wall = (now_us() - wall) / 30.0;
        per_max = a[0].per;
        for (k = 1; k < n; k++) {
            if (a[k].per > per_max) {
                per_max = a[k].per;
            }
        }
        printf("  N=%-3d  wall/task=%.1f us  per-thread(max)=%.1f us\n", n,
               wall, per_max);
    }
    return 0;
}
