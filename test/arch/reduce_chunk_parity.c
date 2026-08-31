/*
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

/**
 * ucc_ec_cpu_reduce_chunk parity harness (task 566).
 *
 * Drives the real library functions:
 *
 *   1. chunk over [0, count) is bitwise equal to ucc_ec_cpu_reduce(task)
 *      over the full range, for every (dt, op) the executor supports;
 *
 *   2. chunk over a sub-range [start, end) (odd counts included) is
 *      bitwise equal to an independent element-wise scalar reference over
 *      that sub-range, with flags/alpha passed through exactly as
 *      ucc_ec_cpu_reduce applies them:
 *        - int dt:    alpha applied only for SUM/AVG
 *        - float dt:  alpha applied for every op
 *
 *      The reference replicates the scalar op semantics of
 *      ec_cpu_reduce.c: sum/prod/band/bor/bxor/land/lor are left-
 *      associative folds, min/max/lxor use the tree form DO_OP__N
 *      (same as the library), and n_srcs > 8 first folds sources 0-7
 *      then left-folds the rest (same as the library).
 *
 * Build (from the repo root):
 *   gcc -O2 -Isrc -I. test/arch/reduce_chunk_parity.c \
 *       -Lsrc/components/ec/cpu/.libs -lucc_ec_cpu \
 *       -Lsrc/.libs -lucc -o /tmp/reduce_chunk_parity
 * Run:
 *   LD_LIBRARY_PATH=src/.libs:src/components/ec/cpu/.libs \
 *   /tmp/reduce_chunk_parity
 */

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "components/ec/cpu/ec_cpu.h"

/*
 * Scalar op replicas, verbatim from src/utils/ucc_math.h and
 * src/utils/ucc_math_op.h.
 */
#define DO_OP_MIN(_v1, _v2) (_v1 < _v2 ? _v1 : _v2)
#define DO_OP_MAX(_v1, _v2) (_v1 > _v2 ? _v1 : _v2)
#define DO_OP_LXOR(_v1, _v2) ((!_v1) != (!_v2))

#define DO_OP__3(_OP, _v1, _v2, _v3)      _OP(_OP(_v1, _v2), _v3)
#define DO_OP__4(_OP, _v1, _v2, _v3, _v4) _OP(_OP(_v1, _v2), _OP(_v3, _v4))
#define DO_OP__5(_OP, _v1, _v2, _v3, _v4, _v5)                                  \
    _OP(_OP(_v1, _v2), DO_OP__3(_OP, _v3, _v4, _v5))
#define DO_OP__6(_OP, _v1, _v2, _v3, _v4, _v5, _v6)                            \
    _OP(DO_OP__3(_OP, _v1, _v2, _v3), DO_OP__3(_OP, _v4, _v5, _v6))
#define DO_OP__7(_OP, _v1, _v2, _v3, _v4, _v5, _v6, _v7)                       \
    _OP(DO_OP__3(_OP, _v1, _v2, _v3), DO_OP__4(_OP, _v4, _v5, _v6, _v7))
#define DO_OP__8(_OP, _v1, _v2, _v3, _v4, _v5, _v6, _v7, _v8)                  \
    _OP(DO_OP__4(_OP, _v1, _v2, _v3, _v4), DO_OP__4(_OP, _v5, _v6, _v7, _v8))

/*
 * Element-wise left fold over [start, end) for sum/prod/band/bor/bxor/
 * land/lor.  Mirrors DO_DT_REDUCE_WITH_OP: for n_srcs > 8 the first eight
 * sources are left-folded, then the rest left-folded on top; a sequential
 * loop from source 0 is the identical left-associative chain.
 */
#define REF_FOLD(CTYPE, NAME, TOK)                                            \
    static void ref_##NAME(void *d_, const void *const *s_, size_t start,     \
                           size_t end, unsigned n_srcs)                       \
    {                                                                          \
        CTYPE *d = (CTYPE *)d_;                                               \
        const CTYPE *const *s = (const CTYPE *const *)s_;                     \
        size_t i;                                                             \
        unsigned j;                                                           \
        for (i = start; i < end; i++) {                                      \
            CTYPE r = s[0][i];                                                \
            for (j = 1; j < n_srcs; j++) {                                   \
                r = r TOK s[j][i];                                            \
            }                                                                 \
            d[i] = r;                                                         \
        }                                                                     \
    }

/* Element-wise tree fold over [start, end) for min/max/lxor. */
#define REF_TREE(CTYPE, NAME, OP)                                             \
    static void ref_##NAME(void *d_, const void *const *s_, size_t start,     \
                           size_t end, unsigned n_srcs)                       \
    {                                                                          \
        CTYPE *d = (CTYPE *)d_;                                               \
        const CTYPE *const *s = (const CTYPE *const *)s_;                     \
        size_t i;                                                             \
        unsigned j;                                                          \
        for (i = start; i < end; i++) {                                      \
            CTYPE r;                                                         \
            switch (n_srcs) {                                                 \
            case 2: r = OP(s[0][i], s[1][i]); break;                          \
            case 3: r = DO_OP__3(OP, s[0][i], s[1][i], s[2][i]); break;      \
            case 4: r = DO_OP__4(OP, s[0][i], s[1][i], s[2][i],              \
                                 s[3][i]); break;                             \
            case 5: r = DO_OP__5(OP, s[0][i], s[1][i], s[2][i],              \
                                 s[3][i], s[4][i]); break;                   \
            case 6: r = DO_OP__6(OP, s[0][i], s[1][i], s[2][i],              \
                                 s[3][i], s[4][i], s[5][i]); break;          \
            case 7: r = DO_OP__7(OP, s[0][i], s[1][i], s[2][i],              \
                                 s[3][i], s[4][i], s[5][i],                  \
                                 s[6][i]); break;                             \
            case 8: r = DO_OP__8(OP, s[0][i], s[1][i], s[2][i],              \
                                 s[3][i], s[4][i], s[5][i],                  \
                                 s[6][i], s[7][i]); break;                   \
            default:                                                          \
                r = DO_OP__8(OP, s[0][i], s[1][i], s[2][i], s[3][i],         \
                             s[4][i], s[5][i], s[6][i], s[7][i]);           \
                for (j = 8; j < n_srcs; j++) {                               \
                    r = OP(r, s[j][i]);                                      \
                }                                                            \
                break;                                                       \
            }                                                                 \
            d[i] = r;                                                         \
        }                                                                     \
    }

REF_FOLD(int8_t, int8_sum, +)
REF_FOLD(int16_t, int16_sum, +)
REF_FOLD(int32_t, int32_sum, +)
REF_FOLD(int64_t, int64_sum, +)
REF_FOLD(uint8_t, uint8_sum, +)
REF_FOLD(uint16_t, uint16_sum, +)
REF_FOLD(uint32_t, uint32_sum, +)
REF_FOLD(uint64_t, uint64_sum, +)
REF_FOLD(float, f32_sum, +)
REF_FOLD(double, f64_sum, +)

REF_FOLD(int8_t, int8_prod, *)
REF_FOLD(int16_t, int16_prod, *)
REF_FOLD(int32_t, int32_prod, *)
REF_FOLD(int64_t, int64_prod, *)
REF_FOLD(uint8_t, uint8_prod, *)
REF_FOLD(uint16_t, uint16_prod, *)
REF_FOLD(uint32_t, uint32_prod, *)
REF_FOLD(uint64_t, uint64_prod, *)
REF_FOLD(float, f32_prod, *)
REF_FOLD(double, f64_prod, *)

REF_FOLD(int8_t, int8_band, &)
REF_FOLD(int16_t, int16_band, &)
REF_FOLD(int32_t, int32_band, &)
REF_FOLD(int64_t, int64_band, &)
REF_FOLD(uint8_t, uint8_band, &)
REF_FOLD(uint16_t, uint16_band, &)
REF_FOLD(uint32_t, uint32_band, &)
REF_FOLD(uint64_t, uint64_band, &)

REF_FOLD(int8_t, int8_bor, |)
REF_FOLD(int16_t, int16_bor, |)
REF_FOLD(int32_t, int32_bor, |)
REF_FOLD(int64_t, int64_bor, |)
REF_FOLD(uint8_t, uint8_bor, |)
REF_FOLD(uint16_t, uint16_bor, |)
REF_FOLD(uint32_t, uint32_bor, |)
REF_FOLD(uint64_t, uint64_bor, |)

REF_FOLD(int8_t, int8_bxor, ^)
REF_FOLD(int16_t, int16_bxor, ^)
REF_FOLD(int32_t, int32_bxor, ^)
REF_FOLD(int64_t, int64_bxor, ^)
REF_FOLD(uint8_t, uint8_bxor, ^)
REF_FOLD(uint16_t, uint16_bxor, ^)
REF_FOLD(uint32_t, uint32_bxor, ^)
REF_FOLD(uint64_t, uint64_bxor, ^)

REF_FOLD(int8_t, int8_land, &&)
REF_FOLD(int16_t, int16_land, &&)
REF_FOLD(int32_t, int32_land, &&)
REF_FOLD(int64_t, int64_land, &&)
REF_FOLD(uint8_t, uint8_land, &&)
REF_FOLD(uint16_t, uint16_land, &&)
REF_FOLD(uint32_t, uint32_land, &&)
REF_FOLD(uint64_t, uint64_land, &&)

REF_FOLD(int8_t, int8_lor, ||)
REF_FOLD(int16_t, int16_lor, ||)
REF_FOLD(int32_t, int32_lor, ||)
REF_FOLD(int64_t, int64_lor, ||)
REF_FOLD(uint8_t, uint8_lor, ||)
REF_FOLD(uint16_t, uint16_lor, ||)
REF_FOLD(uint32_t, uint32_lor, ||)
REF_FOLD(uint64_t, uint64_lor, ||)

REF_TREE(int8_t, int8_min, DO_OP_MIN)
REF_TREE(int16_t, int16_min, DO_OP_MIN)
REF_TREE(int32_t, int32_min, DO_OP_MIN)
REF_TREE(int64_t, int64_min, DO_OP_MIN)
REF_TREE(uint8_t, uint8_min, DO_OP_MIN)
REF_TREE(uint16_t, uint16_min, DO_OP_MIN)
REF_TREE(uint32_t, uint32_min, DO_OP_MIN)
REF_TREE(uint64_t, uint64_min, DO_OP_MIN)
REF_TREE(float, f32_min, DO_OP_MIN)
REF_TREE(double, f64_min, DO_OP_MIN)

REF_TREE(int8_t, int8_max, DO_OP_MAX)
REF_TREE(int16_t, int16_max, DO_OP_MAX)
REF_TREE(int32_t, int32_max, DO_OP_MAX)
REF_TREE(int64_t, int64_max, DO_OP_MAX)
REF_TREE(uint8_t, uint8_max, DO_OP_MAX)
REF_TREE(uint16_t, uint16_max, DO_OP_MAX)
REF_TREE(uint32_t, uint32_max, DO_OP_MAX)
REF_TREE(uint64_t, uint64_max, DO_OP_MAX)
REF_TREE(float, f32_max, DO_OP_MAX)
REF_TREE(double, f64_max, DO_OP_MAX)

REF_TREE(int8_t, int8_lxor, DO_OP_LXOR)
REF_TREE(int16_t, int16_lxor, DO_OP_LXOR)
REF_TREE(int32_t, int32_lxor, DO_OP_LXOR)
REF_TREE(int64_t, int64_lxor, DO_OP_LXOR)
REF_TREE(uint8_t, uint8_lxor, DO_OP_LXOR)
REF_TREE(uint16_t, uint16_lxor, DO_OP_LXOR)
REF_TREE(uint32_t, uint32_lxor, DO_OP_LXOR)
REF_TREE(uint64_t, uint64_lxor, DO_OP_LXOR)

/*
 * Alpha pass-through, replicated from ec_cpu_reduce.c VEC_OP:
 * `d[i] = d[i] * alpha` (double multiply, converted back to CTYPE).
 * The caller gates alpha_on by the dtype rules (int dt: SUM/AVG only;
 * float dt: every op).
 */
#define REF_ALPHA(NAME, CTYPE)                                                 \
    static void ref_alpha_##NAME(void *d_, size_t start, size_t end,          \
                                  int alpha_on, double alpha)                 \
    {                                                                          \
        CTYPE *d = (CTYPE *)d_;                                               \
        size_t i;                                                            \
        for (i = start; i < end; i++) {                                      \
            if (alpha_on) {                                                   \
                d[i] = (CTYPE)((double)d[i] * alpha);                        \
            }                                                                 \
        }                                                                     \
    }

REF_ALPHA(int8, int8_t)
REF_ALPHA(int16, int16_t)
REF_ALPHA(int32, int32_t)
REF_ALPHA(int64, int64_t)
REF_ALPHA(uint8, uint8_t)
REF_ALPHA(uint16, uint16_t)
REF_ALPHA(uint32, uint32_t)
REF_ALPHA(uint64, uint64_t)
REF_ALPHA(f32, float)
REF_ALPHA(f64, double)

/* Deterministic data generation (same pattern as reduce_parity.c). */
#define GEN_INT(NAME, CTYPE, ISNEG)                                            \
    static void gen_int_##NAME(void *d_, unsigned n_srcs, size_t count)        \
    {                                                                          \
        CTYPE *srcs = (CTYPE *)d_;                                            \
        size_t i;                                                             \
        unsigned k;                                                           \
        for (k = 0; k < n_srcs; k++) {                                       \
            for (i = 0; i < count; i++) {                                     \
                unsigned x = (unsigned)(i * 7 + k * 131);                    \
                x = (x * 1103515245u + 12345u) & 0x7FFFFFFFu;                \
                srcs[k * count + i] =                                         \
                    ISNEG ? (CTYPE)((int)(x % 251) - 125)                    \
                          : (CTYPE)(x % 251);                                 \
            }                                                                 \
        }                                                                    \
    }

GEN_INT(int8, int8_t, 1)
GEN_INT(int16, int16_t, 1)
GEN_INT(int32, int32_t, 1)
GEN_INT(int64, int64_t, 1)
GEN_INT(uint8, uint8_t, 0)
GEN_INT(uint16, uint16_t, 0)
GEN_INT(uint32, uint32_t, 0)
GEN_INT(uint64, uint64_t, 0)

#define GEN_FLOAT(NAME, CTYPE)                                                  \
    static void gen_f##NAME(void *d_, unsigned n_srcs, size_t count)           \
    {                                                                          \
        CTYPE *srcs = (CTYPE *)d_;                                            \
        size_t i;                                                             \
        unsigned k;                                                           \
        for (k = 0; k < n_srcs; k++) {                                       \
            for (i = 0; i < count; i++) {                                     \
                unsigned x = (unsigned)(i * 7 + k * 131);                    \
                x = (x * 1103515245u + 12345u) & 0x7FFFFFFFu;                \
                int tag = (int)(x % 16);                                      \
                if (tag == 0) {                                                \
                    srcs[k * count + i] = -0.0f;                             \
                } else if (tag == 1) {                                         \
                    srcs[k * count + i] =                                     \
                        (CTYPE)(1e-40f *                                     \
                                 (0.5f + (float)(x % 100) / 100.0f));        \
                } else if (tag == 2) {                                         \
                    srcs[k * count + i] =                                     \
                        (CTYPE)((float)(x % 1000) / 1000.0f);                \
                } else if (tag == 3) {                                         \
                    srcs[k * count + i] =                                     \
                        (CTYPE)((float)((x % 997) - 498) * 1e-30f);          \
                } else if (tag == 4) {                                         \
                    srcs[k * count + i] =                                     \
                        (x % 2 == 0) ? (CTYPE)1e30f : (CTYPE)-1e30f;         \
                } else {                                                       \
                    srcs[k * count + i] =                                     \
                        (CTYPE)((float)((x % 2000) - 1000) / 1000.0f);       \
                }                                                            \
            }                                                                \
        }                                                                   \
    }

GEN_FLOAT(32, float)
GEN_FLOAT(64, double)
static unsigned long checks, fails;

static void check_eq(const char *what, const void *a, const void *b,
                     size_t bytes)
{
    checks++;
    if (memcmp(a, b, bytes) != 0) {
        fails++;
        fprintf(stderr, "FAIL: %s\n", what);
    }
}

struct opdef {
    const char *name;
    ucc_reduction_op_t op;
    int op_sum, op_avg;
    void (*ref)(void *, const void *const *, size_t, size_t, unsigned);
};

#define INT_OPS(DTSUFFIX)                                                       \
    static const struct opdef ops_##DTSUFFIX[] = {                              \
        {"sum", UCC_OP_SUM, 1, 0, ref_##DTSUFFIX##_sum},                       \
        {"avg", UCC_OP_AVG, 0, 1, ref_##DTSUFFIX##_sum},                       \
        {"prod", UCC_OP_PROD, 0, 0, ref_##DTSUFFIX##_prod},                    \
        {"min", UCC_OP_MIN, 0, 0, ref_##DTSUFFIX##_min},                       \
        {"max", UCC_OP_MAX, 0, 0, ref_##DTSUFFIX##_max},                       \
        {"band", UCC_OP_BAND, 0, 0, ref_##DTSUFFIX##_band},                    \
        {"bor", UCC_OP_BOR, 0, 0, ref_##DTSUFFIX##_bor},                       \
        {"bxor", UCC_OP_BXOR, 0, 0, ref_##DTSUFFIX##_bxor},                    \
        {"land", UCC_OP_LAND, 0, 0, ref_##DTSUFFIX##_land},                    \
        {"lor", UCC_OP_LOR, 0, 0, ref_##DTSUFFIX##_lor},                       \
        {"lxor", UCC_OP_LXOR, 0, 0, ref_##DTSUFFIX##_lxor},                    \
    };

INT_OPS(int8)
INT_OPS(int16)
INT_OPS(int32)
INT_OPS(int64)
INT_OPS(uint8)
INT_OPS(uint16)
INT_OPS(uint32)
INT_OPS(uint64)

#define FLOAT_OPS(DTSUFFIX)                                                       \
    static const struct opdef ops_##DTSUFFIX[] = {                                \
        {"sum", UCC_OP_SUM, 1, 0, ref_##DTSUFFIX##_sum},                         \
        {"avg", UCC_OP_AVG, 0, 1, ref_##DTSUFFIX##_sum},                         \
        {"prod", UCC_OP_PROD, 0, 0, ref_##DTSUFFIX##_prod},                      \
        {"min", UCC_OP_MIN, 0, 0, ref_##DTSUFFIX##_min},                         \
        {"max", UCC_OP_MAX, 0, 0, ref_##DTSUFFIX##_max},                         \
    };

FLOAT_OPS(f32)
FLOAT_OPS(f64)

/*
 * One (dt, op) case: full-range chunk vs ucc_ec_cpu_reduce, and
 * sub-ranges (odd counts included) vs the element-wise scalar reference,
 * across n_srcs {2,3,4,8,9}, counts {1,2,3,7,8,31,32,33,64,129} and
 * alpha modes {no flag, alpha=0.5, alpha=-1.5}.
 */
static void run_case(const char *dt_name, const struct opdef *o,
                     ucc_datatype_t dt, int is_int,
                     void (*gen)(void *, unsigned, size_t), size_t elem_sz,
                     void (*ref_a)(void *, size_t, size_t, int, double))
{
    static const size_t counts[] = {1, 2, 3, 7, 8, 31, 32, 33, 64, 129};
    static const unsigned nss[] = {2, 3, 4, 8, 9};
    static const double alphas[] = {1.0, 0.5, -1.5};
    static const size_t rng[][2] = {
        {0, 1},   {0, 3},   {1, 7},   {0, 5},  {2, 8},   {1, 8},
        {0, 31},  {31, 32}, {32, 64}, {3, 5},  {37, 113},
    };
    size_t ci;
    unsigned ki, ai, r, tr;

    for (ki = 0; ki < sizeof(nss) / sizeof(nss[0]); ki++) {
        unsigned n_srcs = nss[ki];
        for (ci = 0; ci < sizeof(counts) / sizeof(counts[0]); ci++) {
            size_t count = counts[ci];
            void *srcs = calloc(n_srcs * count, elem_sz);
            void *dst_full = calloc(count, elem_sz);
            void *dst_chunk = calloc(count, elem_sz);
            void *dst_ref = calloc(count, elem_sz);
            ucc_eee_task_reduce_t task;

            gen(srcs, n_srcs, count);
            void *src_ptrs[16];
            unsigned sp;
            for (sp = 0; sp < n_srcs; sp++) {
                src_ptrs[sp] = (char *)srcs + sp * count * elem_sz;
            }
            task.dst = NULL;
            task.count = count;
            task.dt = dt;
            task.op = o->op;
            task.n_srcs = n_srcs;

            for (ai = 0; ai < sizeof(alphas) / sizeof(alphas[0]); ai++) {
                double alpha = alphas[ai];
                uint16_t flags;
                int sumavg = o->op_sum || o->op_avg;
                int alpha_on = ai > 0 &&
                               ((is_int && sumavg) || !is_int);

                if (ai == 0) {
                    flags = 0;
                } else {
                    flags = UCC_EEE_TASK_FLAG_REDUCE_WITH_ALPHA;
                    task.alpha = alpha;
                }

                /* check 1: chunk over [0, count) == full reduce */
                if (ucc_ec_cpu_reduce(&task, dst_full,
                                       src_ptrs, flags) != UCC_OK) {
                    fails++;
                    fprintf(stderr,
                            "FAIL: %s %s ucc_ec_cpu_reduce n_srcs=%u "
                            "count=%zu alpha=%g\n",
                            dt_name, o->name, n_srcs, count, alpha);
                    break;
                }
                if (ucc_ec_cpu_reduce_chunk(src_ptrs, dst_chunk, o->op, dt,
                                            count, n_srcs, flags, 0, count,
                                            alpha) != UCC_OK) {
                    fails++;
                    fprintf(stderr,
                            "FAIL: %s %s chunk-full n_srcs=%u count=%zu "
                            "alpha=%g\n",
                            dt_name, o->name, n_srcs, count, alpha);
                    break;
                }
                {
                    char what[192];
                    snprintf(what, sizeof(what),
                             "%s %s chunk[0,count) vs full n_srcs=%u "
                             "count=%zu alpha=%g",
                             dt_name, o->name, n_srcs, count, alpha);
                    check_eq(what, dst_full, dst_chunk, count * elem_sz);
                }

                /* check 2: sub-ranges vs scalar reference */
                for (r = 0; r < sizeof(rng) / sizeof(rng[0]); r++) {
                    size_t s = rng[r][0], e = rng[r][1];
                    char what[192];
                    if (!(s < e && e <= count)) {
                        continue;
                    }
                    o->ref(dst_ref, (const void *const *)src_ptrs, s, e, n_srcs);
                    ref_a(dst_ref, s, e, alpha_on, alpha);
                    if (ucc_ec_cpu_reduce_chunk(src_ptrs, dst_chunk, o->op,
                                                dt, count, n_srcs, flags, s,
                                                e, alpha) != UCC_OK) {
                        fails++;
                        fprintf(stderr,
                                "FAIL: %s %s chunk[%zu,%zu) n_srcs=%u "
                                "count=%zu alpha=%g\n",
                                dt_name, o->name, s, e, n_srcs, count, alpha);
                        continue;
                    }
                    snprintf(what, sizeof(what),
                             "%s %s chunk[%zu,%zu) vs ref n_srcs=%u count=%zu "
                             "alpha=%g",
                             dt_name, o->name, s, e, n_srcs, count, alpha);
                    check_eq(what, (char *)dst_ref + s * elem_sz,
                             (char *)dst_chunk + s * elem_sz,
                             (e - s) * elem_sz);
                }
                /* odd tails: (count-5, count), (count-3, count) */
                for (tr = 0; tr < 2; tr++) {
                    size_t s = count - (tr ? 3 : 5), e = count;
                    char what[192];
                    if (s >= e) {
                        continue;
                    }
                    o->ref(dst_ref, (const void *const *)src_ptrs, s, e, n_srcs);
                    ref_a(dst_ref, s, e, alpha_on, alpha);
                    if (ucc_ec_cpu_reduce_chunk(src_ptrs, dst_chunk, o->op,
                                                dt, count, n_srcs, flags, s,
                                                e, alpha) != UCC_OK) {
                        fails++;
                        fprintf(stderr,
                                "FAIL: %s %s chunk[%zu,%zu) n_srcs=%u "
                                "count=%zu alpha=%g\n",
                                dt_name, o->name, s, e, n_srcs, count, alpha);
                        continue;
                    }
                    snprintf(what, sizeof(what),
                             "%s %s chunk[%zu,%zu) vs ref n_srcs=%u "
                             "count=%zu alpha=%g",
                             dt_name, o->name, s, e, n_srcs, count, alpha);
                    check_eq(what, (char *)dst_ref + s * elem_sz,
                             (char *)dst_chunk + s * elem_sz,
                             (e - s) * elem_sz);
                }
            }
            free(srcs);
            free(dst_full);
            free(dst_chunk);
            free(dst_ref);
        }
    }
}

int main(void)
{
    static const struct {
        const char *name;
        ucc_datatype_t dt;
        int is_int;
        size_t sz;
        void (*gen)(void *, unsigned, size_t);
        void (*ref_a)(void *, size_t, size_t, int, double);
        const struct opdef *ops;
        size_t nops;
    } dts[] = {
        {"int8", UCC_DT_INT8, 1, 1, gen_int_int8, ref_alpha_int8,
         ops_int8, sizeof(ops_int8) / sizeof(ops_int8[0])},
        {"int16", UCC_DT_INT16, 1, 2, gen_int_int16, ref_alpha_int16,
         ops_int16, sizeof(ops_int16) / sizeof(ops_int16[0])},
        {"int32", UCC_DT_INT32, 1, 4, gen_int_int32, ref_alpha_int32,
         ops_int32, sizeof(ops_int32) / sizeof(ops_int32[0])},
        {"int64", UCC_DT_INT64, 1, 8, gen_int_int64, ref_alpha_int64,
         ops_int64, sizeof(ops_int64) / sizeof(ops_int64[0])},
        {"uint8", UCC_DT_UINT8, 1, 1, gen_int_uint8, ref_alpha_uint8,
         ops_uint8, sizeof(ops_uint8) / sizeof(ops_uint8[0])},
        {"uint16", UCC_DT_UINT16, 1, 2, gen_int_uint16, ref_alpha_uint16,
         ops_uint16, sizeof(ops_uint16) / sizeof(ops_uint16[0])},
        {"uint32", UCC_DT_UINT32, 1, 4, gen_int_uint32, ref_alpha_uint32,
         ops_uint32, sizeof(ops_uint32) / sizeof(ops_uint32[0])},
        {"uint64", UCC_DT_UINT64, 1, 8, gen_int_uint64, ref_alpha_uint64,
         ops_uint64, sizeof(ops_uint64) / sizeof(ops_uint64[0])},
        {"f32", UCC_DT_FLOAT32, 0, 4, gen_f32, ref_alpha_f32, ops_f32,
         sizeof(ops_f32) / sizeof(ops_f32[0])},
        {"f64", UCC_DT_FLOAT64, 0, 8, gen_f64, ref_alpha_f64, ops_f64,
         sizeof(ops_f64) / sizeof(ops_f64[0])},
    };
    size_t di, oi;

    for (di = 0; di < sizeof(dts) / sizeof(dts[0]); di++) {
        for (oi = 0; oi < dts[di].nops; oi++) {
            run_case(dts[di].name, &dts[di].ops[oi], dts[di].dt,
                     dts[di].is_int, dts[di].gen, dts[di].sz,
                     dts[di].ref_a);
        }
    }

    /* guard checks on ucc_ec_cpu_reduce_chunk */
    {
        int8_t a[4] = {1, 2, 3, 4}, b[4] = {5, 6, 7, 8}, d[4] = {0, 0, 0, 0};
        if (ucc_ec_cpu_reduce_chunk((void *const *)d, d, UCC_OP_SUM,
                                    UCC_DT_INT8, 4, 2, 0, 3, 1, 1.0) !=
            UCC_ERR_INVALID_PARAM) {
            fails++;
            fprintf(stderr, "FAIL: chunk start>end not rejected\n");
        }
        if (ucc_ec_cpu_reduce_chunk((void *const *)d, d, UCC_OP_SUM,
                                    UCC_DT_INT8, 4, 2, 0, 0, 8, 1.0) !=
            UCC_ERR_INVALID_PARAM) {
            fails++;
            fprintf(stderr, "FAIL: chunk end>count not rejected\n");
        }
        if (ucc_ec_cpu_reduce_chunk((void *const *)d, d, UCC_OP_SUM,
                                    UCC_DT_INT8, 4, 0, 0, 0, 4, 1.0) !=
            UCC_ERR_INVALID_PARAM) {
            fails++;
            fprintf(stderr, "FAIL: chunk n_srcs=0 not rejected\n");
        }
        (void)a;
        (void)b;
    }

    printf("checks: %lu, fails: %lu\n", (unsigned long)checks,
           (unsigned long)fails);
    return fails ? 1 : 0;
}
