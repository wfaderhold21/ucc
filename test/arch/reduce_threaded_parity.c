/*
 * Parity + benchmark harness for ucc_ec_cpu_reduce_threaded.
 *
 * Links against the real libucc_ec_cpu so it exercises the committed
 * fork-join implementation (not a reimplementation).  Compares threaded
 * output bitwise against single-threaded ucc_ec_cpu_reduce across the
 * SIMD-supported (dt, op) matrix at odd/even/boundary counts, then
 * benchmarks 1M-element throughput scaling up to socket cores.
 *
 * Compile:
 *   gcc -O2 -mavx2 -std=gnu11 -I<ucc-src-root> \
 *       reduce_threaded_parity.c -o reduce_threaded_parity \
 *       -L<ucc-src-root>/src/.libs -L<ucc-src-root>/src/components/ec/cpu/.libs \
 *       -lucc_ec_cpu -lucc -lucs -lrt -lm -lpthread
 * Run with LD_LIBRARY_PATH pointing at both .libs dirs.
 */

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <pthread.h>
#include <sched.h>

#include "components/ec/cpu/ec_cpu.h"
#include "core/ucc_dt.h"
#include "utils/ucc_math_op.h"

#define MAX_SRCS 17 /* UCC_EE_EXECUTOR_NUM_BUFS + fallback headroom */
#define DTYPE_CNT 10
#define INT_OPS 10
#define FLOAT_OPS 4
#define CHUNK_ELEMS 4096 /* elements per chunk; >= THRESH so SIMD path */

typedef enum { DT_INT8, DT_INT16, DT_INT32, DT_INT64,
               DT_UINT8, DT_UINT16, DT_UINT32, DT_UINT64,
               DT_F32, DT_F64 } dtype_e;

static size_t dt_size(dtype_e d)
{
    switch (d) {
    case DT_INT8: case DT_UINT8:   return 1;
    case DT_INT16: case DT_UINT16: return 2;
    case DT_INT32: case DT_UINT32: case DT_F32: return 4;
    default:                       return 8; /* int64/uint64/f64 */
    }
}

static ucc_datatype_t dt_ucc(dtype_e d)
{
    switch (d) {
    case DT_INT8:  return UCC_DT_INT8;
    case DT_INT16: return UCC_DT_INT16;
    case DT_INT32: return UCC_DT_INT32;
    case DT_INT64: return UCC_DT_INT64;
    case DT_UINT8: return UCC_DT_UINT8;
    case DT_UINT16:return UCC_DT_UINT16;
    case DT_UINT32:return UCC_DT_UINT32;
    case DT_UINT64:return UCC_DT_UINT64;
    case DT_F32:   return UCC_DT_FLOAT32;
    default:       return UCC_DT_FLOAT64;
    }
}

/* deterministic fill; floats mix normals/±0/±Inf/NaN/denormals */
static void fill(dtype_e d, void *buf, size_t count, unsigned seed)
{
    size_t i;
    switch (d) {
    case DT_INT8: {  int8_t *p = buf;  for (i=0;i<count;i++) p[i] = (int8_t)((i*7+seed*11) & 0xFF); break; }
    case DT_INT16: { int16_t *p = buf; for (i=0;i<count;i++) p[i] = (int16_t)((i*31+seed*17) & 0xFFFF); break; }
    case DT_INT32: { int32_t *p = buf; for (i=0;i<count;i++) p[i] = (int32_t)(i*131+seed*19); break; }
    case DT_INT64: { int64_t *p = buf; for (i=0;i<count;i++) p[i] = (int64_t)(i*1000003ULL+seed*23ULL); break; }
    case DT_UINT8: { uint8_t *p = buf;  for (i=0;i<count;i++) p[i] = (uint8_t)((i*7+seed*29) & 0xFF); break; }
    case DT_UINT16:{ uint16_t *p = buf; for (i=0;i<count;i++) p[i] = (uint16_t)((i*31+seed*31) & 0xFFFF); break; }
    case DT_UINT32:{ uint32_t *p = buf; for (i=0;i<count;i++) p[i] = (uint32_t)(i*131+seed*37); break; }
    case DT_UINT64:{ uint64_t *p = buf; for (i=0;i<count;i++) p[i] = (uint64_t)(i*1000003ULL+seed*41ULL); break; }
    case DT_F32: { float *p = buf; for (i=0;i<count;i++) {
        switch ((i+seed) % 6) {
        case 0: p[i] = (float)(i * 0.5 + seed); break;
        case 1: p[i] = -0.0f; break;
        case 2: p[i] = INFINITY; break;
        case 3: p[i] = (i % 2) ? NAN : -INFINITY; break;
        case 4: p[i] = 1.0e-45f; break; /* denormal */
        default: p[i] = -(float)(i * 1.75 + seed * 2.5); break;
        } } break; }
    default: { double *p = buf; for (i=0;i<count;i++) {
        switch ((i+seed) % 6) {
        case 0: p[i] = (double)(i * 0.5 + seed); break;
        case 1: p[i] = -0.0; break;
        case 2: p[i] = INFINITY; break;
        case 3: p[i] = (i % 2) ? NAN : -INFINITY; break;
        case 4: p[i] = 1.0e-320; break; /* denormal */
        default: p[i] = -(double)(i * 1.75 + seed * 2.5); break;
        } } break; }
    }
}

typedef struct {
    const char *name;
    dtype_e d;
    size_t ops;
    ucc_reduction_op_t op[INT_OPS];
} dt_case_t;

static const dt_case_t cases[DTYPE_CNT] = {
    { "int8",  DT_INT8,  INT_OPS,  { UCC_OP_SUM, UCC_OP_PROD, UCC_OP_MIN, UCC_OP_MAX, UCC_OP_BAND, UCC_OP_BOR, UCC_OP_BXOR, UCC_OP_LAND, UCC_OP_LOR, UCC_OP_LXOR } },
    { "int16", DT_INT16, INT_OPS,  { UCC_OP_SUM, UCC_OP_PROD, UCC_OP_MIN, UCC_OP_MAX, UCC_OP_BAND, UCC_OP_BOR, UCC_OP_BXOR, UCC_OP_LAND, UCC_OP_LOR, UCC_OP_LXOR } },
    { "int32", DT_INT32, INT_OPS,  { UCC_OP_SUM, UCC_OP_PROD, UCC_OP_MIN, UCC_OP_MAX, UCC_OP_BAND, UCC_OP_BOR, UCC_OP_BXOR, UCC_OP_LAND, UCC_OP_LOR, UCC_OP_LXOR } },
    { "int64", DT_INT64, INT_OPS,  { UCC_OP_SUM, UCC_OP_PROD, UCC_OP_MIN, UCC_OP_MAX, UCC_OP_BAND, UCC_OP_BOR, UCC_OP_BXOR, UCC_OP_LAND, UCC_OP_LOR, UCC_OP_LXOR } },
    { "uint8", DT_UINT8, INT_OPS,  { UCC_OP_SUM, UCC_OP_PROD, UCC_OP_MIN, UCC_OP_MAX, UCC_OP_BAND, UCC_OP_BOR, UCC_OP_BXOR, UCC_OP_LAND, UCC_OP_LOR, UCC_OP_LXOR } },
    { "uint16",DT_UINT16,INT_OPS,  { UCC_OP_SUM, UCC_OP_PROD, UCC_OP_MIN, UCC_OP_MAX, UCC_OP_BAND, UCC_OP_BOR, UCC_OP_BXOR, UCC_OP_LAND, UCC_OP_LOR, UCC_OP_LXOR } },
    { "uint32",DT_UINT32,INT_OPS,  { UCC_OP_SUM, UCC_OP_PROD, UCC_OP_MIN, UCC_OP_MAX, UCC_OP_BAND, UCC_OP_BOR, UCC_OP_BXOR, UCC_OP_LAND, UCC_OP_LOR, UCC_OP_LXOR } },
    { "uint64",DT_UINT64,INT_OPS,  { UCC_OP_SUM, UCC_OP_PROD, UCC_OP_MIN, UCC_OP_MAX, UCC_OP_BAND, UCC_OP_BOR, UCC_OP_BXOR, UCC_OP_LAND, UCC_OP_LOR, UCC_OP_LXOR } },
    { "f32",   DT_F32,   FLOAT_OPS,{ UCC_OP_SUM, UCC_OP_PROD, UCC_OP_MIN, UCC_OP_MAX } },
    { "f64",   DT_F64,   FLOAT_OPS,{ UCC_OP_SUM, UCC_OP_PROD, UCC_OP_MIN, UCC_OP_MAX } },
};

static int failures = 0;

static void run_parity(const char *dname, dtype_e d, ucc_reduction_op_t op,
                       size_t count, unsigned n_srcs, int num_threads,
                       size_t chunk_size)
{
    size_t esz = dt_size(d);
    size_t bytes = count * esz;
    char *srcs[MAX_SRCS], *dst_ref, *dst_thr;
    void *src_ptrs[MAX_SRCS];
    unsigned s, i;
    ucc_eee_task_reduce_t task;
    ucc_status_t st_ref, st_thr;

    for (s = 0; s < n_srcs; s++) {
        srcs[s] = malloc(bytes);
        fill(d, srcs[s], count, s + 1);
        src_ptrs[s] = srcs[s];
    }
    dst_ref = malloc(bytes);
    dst_thr = malloc(bytes);

    task.count  = count;
    task.alpha  = 1.0;
    task.dt     = dt_ucc(d);
    task.op     = op;
    task.n_srcs = (uint16_t)n_srcs;

    /* single-threaded reference (SIMD path at this count) */
    st_ref = ucc_ec_cpu_reduce(&task, dst_ref, src_ptrs, 0);
    /* threaded fork-join */
    st_thr = ucc_ec_cpu_reduce_threaded(&task, dst_thr, src_ptrs, 0,
                                        num_threads, chunk_size);

    if (st_ref != UCC_OK || st_thr != UCC_OK ||
        memcmp(dst_ref, dst_thr, bytes) != 0) {
        failures++;
        printf("FAIL d=%s op=%d count=%zu n_srcs=%u nt=%d chunk=%zu "
               "(st_ref=%d st_thr=%d)\n",
               dname, (int)op, count, n_srcs, num_threads, chunk_size,
               (int)st_ref, (int)st_thr);
    }

    free(dst_thr);
    free(dst_ref);
    for (s = 0; s < n_srcs; s++) free(srcs[s]);
}

/* mean ns over iters of a single reduce call */
static double bench_ns(ucc_eee_task_reduce_t *task, void **src_ptrs,
                       void *dst, int num_threads, size_t chunk_size, int iters)
{
    struct timespec t0, t1;
    double best = 1e30;
    int k;
    for (k = 0; k < iters; k++) {
        clock_gettime(CLOCK_MONOTONIC, &t0);
        ucc_ec_cpu_reduce_threaded(task, dst, src_ptrs, 0, num_threads,
                                   chunk_size);
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double ns = (t1.tv_sec - t0.tv_sec) * 1e9 + (t1.tv_nsec - t0.tv_nsec);
        if (ns < best) best = ns;
    }
    return best;
}

int main(void)
{
    /* Pin the process to 8 distinct physical cores. Pool workers inherit
       this affinity mask, so the CFS scheduler spreads the resident
       workers deterministically across cores instead of floating them
       across SMT siblings. */
    {
        cpu_set_t cs;
        int p;
        CPU_ZERO(&cs);
        for (p = 0; p < 8; p++) {
            CPU_SET(p, &cs);
        }
        pthread_setaffinity_np(pthread_self(), 1024, &cs);
    }

    /* ---- parity: full dt×op matrix, odd/even/boundary counts ---- */
    size_t counts[] = { 1, 3, 31, 32, 4095, 4096, 4097, 8192, 8191,
                        1 << 20 }; /* 1M */
    int nts[] = { 1, 2, 3, 4, 8, 17 };
    int ci, ti, di, oi, si;

    printf("== parity: threaded == single-threaded (bitwise) ==\n");
    for (di = 0; di < DTYPE_CNT; di++) {
        const dt_case_t *c = &cases[di];
        for (oi = 0; oi < (int)c->ops; oi++) {
            ucc_reduction_op_t op = c->op[oi];
            for (ci = 0; ci < 9; ci++) { /* skip 1M here; it's in bench */
                size_t count = counts[ci];
                for (si = 0; si < 5; si++) {
                    unsigned n_srcs_list[] = { 2, 3, 8, 9, 17 };
                    for (ti = 0; ti < 6; ti++) {
                        run_parity(c->name, c->d, op, count, n_srcs_list[si],
                                   nts[ti], CHUNK_ELEMS);
                    }
                }
            }
            /* boundary: count == 2*chunk (even split), ±1, tiny num_threads */
            run_parity(c->name, c->d, op, 2 * CHUNK_ELEMS, 4, 8, CHUNK_ELEMS);
            run_parity(c->name, c->d, op, 2 * CHUNK_ELEMS - 1, 4, 8,
                       CHUNK_ELEMS);
            run_parity(c->name, c->d, op, 2 * CHUNK_ELEMS + 1, 4, 8,
                       CHUNK_ELEMS);
            run_parity(c->name, c->d, op, 5, 4, 8, CHUNK_ELEMS);
        }
    }

    /* ---- benchmark: int32 sum, n_srcs=4, counts spanning L3-resident ---- */
    const unsigned NSRC = 4;
    size_t bench_counts[] = { 1 << 20, 2 << 20, 4 << 20, 8 << 20 };
    int bc;
    for (bc = 0; bc < 4; bc++) {
        size_t COUNT = bench_counts[bc];
        size_t bytes = COUNT * 4;
        char *srcs[MAX_SRCS], *dst;
        void *src_ptrs[MAX_SRCS];
        unsigned s;
        for (s = 0; s < NSRC; s++) { srcs[s] = malloc(bytes); fill(DT_INT32, srcs[s], COUNT, s + 1); src_ptrs[s] = srcs[s]; }
        dst = malloc(bytes);

        ucc_eee_task_reduce_t task = {
            .count  = COUNT,
            .alpha  = 1.0,
            .dt     = UCC_DT_INT32,
            .op     = UCC_OP_SUM,
            .n_srcs = NSRC,
        };
        bench_ns(&task, src_ptrs, dst, 1, CHUNK_ELEMS, 8); /* warm */
        double base = bench_ns(&task, src_ptrs, dst, 1, CHUNK_ELEMS, 101);
        printf("== benchmark: %zu int32 SUM elems, n_srcs=%u ==\n", COUNT, NSRC);
        printf("  single-thread baseline: %.3f ms\n", base / 1e6);
        for (int nt = 2; nt <= 8; nt++) {
            bench_ns(&task, src_ptrs, dst, nt, CHUNK_ELEMS, 4); /* warm */
            double ns = bench_ns(&task, src_ptrs, dst, nt, CHUNK_ELEMS, 101);
            double speedup = base / ns;
            printf("  num_threads=%d: %.3f ms  speedup=%.2fx  scale=%.2f/N %s\n",
                   nt, ns / 1e6, speedup, speedup / nt,
                   (speedup / nt >= 0.6) ? "OK" : "");
        }
        free(dst);
        for (s = 0; s < NSRC; s++) free(srcs[s]);
    }

    if (failures) {
        printf("\nRESULT: %d parity failures\n", failures);
        return 1;
    }
    printf("\nRESULT: ALL PARITY PASS (bitwise identical)\n");
    return 0;
}
