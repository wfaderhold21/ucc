/*
 * Parity harness: AVX2 reduce kernels vs the scalar ec_cpu reference.
 *
 * The scalar reference semantics are replicated exactly from
 * src/components/ec/cpu/ec_cpu_reduce.c + src/utils/ucc_math_op.h:
 *   - sum/prod/band/bor/bxor/land/lor: left-associative DO_OP_*_N
 *   - min/max/lxor: tree-structured DO_OP__N (NOT left-fold; matters for NaN)
 *   - n_srcs > 8: 8-way first, then left-fold from source 8.
 *
 * Compile: gcc -O2 -mavx2 reduce_parity.c -o reduce_parity
 */

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* compile: gcc -O2 -mavx2 -I<ucc-src-root> reduce_parity.c -o reduce_parity */
#include "utils/arch/x86_64/reduce_avx2.h"
#include <math.h>

/* ------------------------------------------------------------------ */
/* Exact scalar op replicas (ucc_math.h + ucc_math_op.h)               */
/* ------------------------------------------------------------------ */
#define DO_OP__3(_OP, _v1, _v2, _v3)      _OP(_OP(_v1, _v2), _v3)
#define DO_OP__4(_OP, _v1, _v2, _v3, _v4) \
    _OP(_OP(_v1, _v2), _OP(_v3, _v4))
#define DO_OP__5(_OP, _v1, _v2, _v3, _v4, _v5) \
    _OP(_OP(_v1, _v2), DO_OP__3(_OP, _v3, _v4, _v5))
#define DO_OP__6(_OP, _v1, _v2, _v3, _v4, _v5, _v6) \
    _OP(DO_OP__3(_OP, _v1, _v2, _v3), DO_OP__3(_OP, _v4, _v5, _v6))
#define DO_OP__7(_OP, _v1, _v2, _v3, _v4, _v5, _v6, _v7) \
    _OP(DO_OP__3(_OP, _v1, _v2, _v3), DO_OP__4(_OP, _v4, _v5, _v6, _v7))
#define DO_OP__8(_OP, _v1, _v2, _v3, _v4, _v5, _v6, _v7, _v8) \
    _OP(DO_OP__4(_OP, _v1, _v2, _v3, _v4), DO_OP__4(_OP, _v5, _v6, _v7, _v8))

#define DO_OP_MIN(_v1, _v2) (_v1 < _v2 ? _v1 : _v2)
#define DO_OP_MAX(_v1, _v2) (_v1 > _v2 ? _v1 : _v2)
#define DO_OP_LXOR(_v1, _v2) ((!_v1) != (!_v2))

/* FOLD reference: left-assoc token chain (sum/prod/band/bor/bxor/land/lor) */
#define REF_FOLD(CTYPE, NAME, TOK)                                          \
static void ref_##CTYPE##_##NAME(CTYPE *d, const CTYPE * const *s,          \
                                 size_t count, unsigned n_srcs)             \
{                                                                           \
    size_t i; unsigned j;                                                   \
    for (i = 0; i < count; i++) {                                           \
        switch (n_srcs) {                                                   \
        case 2:  d[i] = s[0][i] TOK s[1][i]; break;                        \
        case 3:  d[i] = s[0][i] TOK s[1][i] TOK s[2][i]; break;            \
        case 4:  d[i] = s[0][i] TOK s[1][i] TOK s[2][i] TOK s[3][i];       \
                 break;                                                     \
        case 5:  d[i] = s[0][i] TOK s[1][i] TOK s[2][i] TOK s[3][i]       \
                        TOK s[4][i]; break;                                 \
        case 6:  d[i] = s[0][i] TOK s[1][i] TOK s[2][i] TOK s[3][i]       \
                        TOK s[4][i] TOK s[5][i]; break;                    \
        case 7:  d[i] = s[0][i] TOK s[1][i] TOK s[2][i] TOK s[3][i]       \
                        TOK s[4][i] TOK s[5][i] TOK s[6][i]; break;        \
        case 8:  d[i] = s[0][i] TOK s[1][i] TOK s[2][i] TOK s[3][i]       \
                        TOK s[4][i] TOK s[5][i] TOK s[6][i] TOK s[7][i];   \
                 break;                                                     \
        default: {                                                          \
            CTYPE t = s[0][i] TOK s[1][i] TOK s[2][i] TOK s[3][i]          \
                      TOK s[4][i] TOK s[5][i] TOK s[6][i] TOK s[7][i];     \
            for (j = 8; j < n_srcs; j++) t = t TOK s[j][i];               \
            d[i] = t;                                                       \
        } break;                                                            \
        }                                                                   \
    }                                                                       \
}

/* TREE reference: DO_OP__N tree (min/max/lxor) */
#define REF_TREE(CTYPE, NAME, BIN)                                          \
static void ref_##CTYPE##_##NAME(CTYPE *d, const CTYPE * const *s,          \
                                 size_t count, unsigned n_srcs)             \
{                                                                           \
    size_t i; unsigned j;                                                   \
    for (i = 0; i < count; i++) {                                           \
        switch (n_srcs) {                                                   \
        case 2:  d[i] = BIN(s[0][i], s[1][i]); break;                      \
        case 3:  d[i] = DO_OP__3(BIN, s[0][i], s[1][i], s[2][i]); break;  \
        case 4:  d[i] = DO_OP__4(BIN, s[0][i], s[1][i], s[2][i],          \
                                 s[3][i]); break;                           \
        case 5:  d[i] = DO_OP__5(BIN, s[0][i], s[1][i], s[2][i],          \
                                 s[3][i], s[4][i]); break;                 \
        case 6:  d[i] = DO_OP__6(BIN, s[0][i], s[1][i], s[2][i],          \
                                 s[3][i], s[4][i], s[5][i]); break;        \
        case 7:  d[i] = DO_OP__7(BIN, s[0][i], s[1][i], s[2][i],          \
                                 s[3][i], s[4][i], s[5][i], s[6][i]);      \
                 break;                                                     \
        case 8:  d[i] = DO_OP__8(BIN, s[0][i], s[1][i], s[2][i],          \
                                 s[3][i], s[4][i], s[5][i], s[6][i],        \
                                 s[7][i]); break;                           \
        default: {                                                          \
            CTYPE t = DO_OP__8(BIN, s[0][i], s[1][i], s[2][i],            \
                               s[3][i], s[4][i], s[5][i], s[6][i],         \
                               s[7][i]);                                    \
            for (j = 8; j < n_srcs; j++) t = BIN(t, s[j][i]);             \
            d[i] = t;                                                       \
        } break;                                                            \
        }                                                                   \
    }                                                                       \
}

/* ------------------------------------------------------------------ */
/* Reference instantiations                                          */
/* ------------------------------------------------------------------ */
REF_FOLD(int8_t, sum, +)
REF_FOLD(int8_t, prod, *)
REF_TREE(int8_t, min, DO_OP_MIN)
REF_TREE(int8_t, max, DO_OP_MAX)
REF_FOLD(int8_t, band, &)
REF_FOLD(int8_t, bor, |)
REF_FOLD(int8_t, bxor, ^)
REF_FOLD(int8_t, land, &&)
REF_FOLD(int8_t, lor, ||)
REF_TREE(int8_t, lxor, DO_OP_LXOR)
REF_FOLD(int16_t, sum, +)
REF_FOLD(int16_t, prod, *)
REF_TREE(int16_t, min, DO_OP_MIN)
REF_TREE(int16_t, max, DO_OP_MAX)
REF_FOLD(int16_t, band, &)
REF_FOLD(int16_t, bor, |)
REF_FOLD(int16_t, bxor, ^)
REF_FOLD(int16_t, land, &&)
REF_FOLD(int16_t, lor, ||)
REF_TREE(int16_t, lxor, DO_OP_LXOR)
REF_FOLD(int32_t, sum, +)
REF_FOLD(int32_t, prod, *)
REF_TREE(int32_t, min, DO_OP_MIN)
REF_TREE(int32_t, max, DO_OP_MAX)
REF_FOLD(int32_t, band, &)
REF_FOLD(int32_t, bor, |)
REF_FOLD(int32_t, bxor, ^)
REF_FOLD(int32_t, land, &&)
REF_FOLD(int32_t, lor, ||)
REF_TREE(int32_t, lxor, DO_OP_LXOR)
REF_FOLD(int64_t, sum, +)
REF_FOLD(int64_t, prod, *)
REF_TREE(int64_t, min, DO_OP_MIN)
REF_TREE(int64_t, max, DO_OP_MAX)
REF_FOLD(int64_t, band, &)
REF_FOLD(int64_t, bor, |)
REF_FOLD(int64_t, bxor, ^)
REF_FOLD(int64_t, land, &&)
REF_FOLD(int64_t, lor, ||)
REF_TREE(int64_t, lxor, DO_OP_LXOR)
REF_FOLD(uint8_t, sum, +)
REF_FOLD(uint8_t, prod, *)
REF_TREE(uint8_t, min, DO_OP_MIN)
REF_TREE(uint8_t, max, DO_OP_MAX)
REF_FOLD(uint8_t, band, &)
REF_FOLD(uint8_t, bor, |)
REF_FOLD(uint8_t, bxor, ^)
REF_FOLD(uint8_t, land, &&)
REF_FOLD(uint8_t, lor, ||)
REF_TREE(uint8_t, lxor, DO_OP_LXOR)
REF_FOLD(uint16_t, sum, +)
REF_FOLD(uint16_t, prod, *)
REF_TREE(uint16_t, min, DO_OP_MIN)
REF_TREE(uint16_t, max, DO_OP_MAX)
REF_FOLD(uint16_t, band, &)
REF_FOLD(uint16_t, bor, |)
REF_FOLD(uint16_t, bxor, ^)
REF_FOLD(uint16_t, land, &&)
REF_FOLD(uint16_t, lor, ||)
REF_TREE(uint16_t, lxor, DO_OP_LXOR)
REF_FOLD(uint32_t, sum, +)
REF_FOLD(uint32_t, prod, *)
REF_TREE(uint32_t, min, DO_OP_MIN)
REF_TREE(uint32_t, max, DO_OP_MAX)
REF_FOLD(uint32_t, band, &)
REF_FOLD(uint32_t, bor, |)
REF_FOLD(uint32_t, bxor, ^)
REF_FOLD(uint32_t, land, &&)
REF_FOLD(uint32_t, lor, ||)
REF_TREE(uint32_t, lxor, DO_OP_LXOR)
REF_FOLD(uint64_t, sum, +)
REF_FOLD(uint64_t, prod, *)
REF_TREE(uint64_t, min, DO_OP_MIN)
REF_TREE(uint64_t, max, DO_OP_MAX)
REF_FOLD(uint64_t, band, &)
REF_FOLD(uint64_t, bor, |)
REF_FOLD(uint64_t, bxor, ^)
REF_FOLD(uint64_t, land, &&)
REF_FOLD(uint64_t, lor, ||)
REF_TREE(uint64_t, lxor, DO_OP_LXOR)
REF_FOLD(float, sum, +)
REF_FOLD(float, prod, *)
REF_TREE(float, min, DO_OP_MIN)
REF_TREE(float, max, DO_OP_MAX)
REF_FOLD(double, sum, +)
REF_FOLD(double, prod, *)
REF_TREE(double, min, DO_OP_MIN)
REF_TREE(double, max, DO_OP_MAX)

/* ------------------------------------------------------------------ */
/* Test driver                                                         */
/* ------------------------------------------------------------------ */
#define DEF_TEST(CTYPE, DT, OP, FLOAT)                                        \
static int test_##DT##_##OP(void)                                             \
{                                                                             \
    static const unsigned counts[] = {1, 2, 3, 7, 8, 31, 32, 33, 64, 129};   \
    static const unsigned ns[]    = {2, 3, 4, 8, 9, 17};                      \
    unsigned ci, ni;                                                          \
    int failures = 0;                                                         \
    for (ci = 0; ci < sizeof(counts)/sizeof(counts[0]); ci++) {              \
        for (ni = 0; ni < sizeof(ns)/sizeof(ns[0]); ni++) {                  \
            size_t count = counts[ci]; unsigned n_srcs = ns[ni];             \
            CTYPE **srcs = malloc(n_srcs * sizeof(CTYPE*));                  \
            const CTYPE **sp = malloc(n_srcs * sizeof(CTYPE*));              \
            CTYPE *dst_ref  = malloc(count * sizeof(CTYPE));                \
            CTYPE *dst_simd = malloc(count * sizeof(CTYPE));                \
            unsigned k; size_t i;                                             \
            for (k = 0; k < n_srcs; k++) {                                   \
                srcs[k] = malloc(count * sizeof(CTYPE));                    \
                sp[k] = srcs[k];                                             \
                for (i = 0; i < count; i++) {                               \
                    unsigned x = (unsigned)(i * 7 + k * 131 + ci * 17 + ni);\
                    x = (x * 1103515245u + 12345u) & 0x7FFFFFFFu;            \
                    if (FLOAT) {                                             \
                        unsigned m = (i + k * 3) % 7;                                  \
                        if (m == 0) srcs[k][i] = (CTYPE)((int)(x & 0xFFFF) - 32768) / 100.0; \
                        else if (m == 1) srcs[k][i] = 0.0;                   \
                        else if (m == 2) srcs[k][i] = -0.0;                 \
                        else if (m == 3) srcs[k][i] = (CTYPE)INFINITY;       \
                        else if (m == 4) srcs[k][i] = (CTYPE)-INFINITY;      \
                        else if (m == 5) srcs[k][i] = (CTYPE)NAN;            \
                        else srcs[k][i] = (CTYPE)((int)(x % 2048) - 1024) * 1e-30; \
                    } else {                                                 \
                        srcs[k][i] = (CTYPE)(x % 251);                      \
                    }                                                        \
                }                                                            \
            }                                                                \
            ref_##CTYPE##_##OP(dst_ref, sp, count, n_srcs);                 \
            ucc_arch_reduce_avx2_##DT##_##OP(dst_simd, sp, count, n_srcs);  \
            for (i = 0; i < count; i++) {                                    \
                int eq = (memcmp(&dst_ref[i], &dst_simd[i],                 \
                                 sizeof(CTYPE)) == 0);                       \
                if (!eq) {                                                   \
                    failures++;                                              \
                    if (failures <= 10)                                       \
                        printf("MISMATCH " #DT " " #OP                        \
                               " count=%zu n_srcs=%u i=%zu\n",               \
                               count, n_srcs, i);                            \
                }                                                            \
            }                                                                \
            for (k = 0; k < n_srcs; k++) free(srcs[k]);                     \
            free(srcs); free(sp); free(dst_ref); free(dst_simd);            \
        }                                                                    \
    }                                                                        \
    return failures;                                                         \
}


DEF_TEST(int8_t, int8, sum, 0)
DEF_TEST(int8_t, int8, prod, 0)
DEF_TEST(int8_t, int8, min, 0)
DEF_TEST(int8_t, int8, max, 0)
DEF_TEST(int8_t, int8, band, 0)
DEF_TEST(int8_t, int8, bor, 0)
DEF_TEST(int8_t, int8, bxor, 0)
DEF_TEST(int8_t, int8, land, 0)
DEF_TEST(int8_t, int8, lor, 0)
DEF_TEST(int8_t, int8, lxor, 0)
DEF_TEST(int16_t, int16, sum, 0)
DEF_TEST(int16_t, int16, prod, 0)
DEF_TEST(int16_t, int16, min, 0)
DEF_TEST(int16_t, int16, max, 0)
DEF_TEST(int16_t, int16, band, 0)
DEF_TEST(int16_t, int16, bor, 0)
DEF_TEST(int16_t, int16, bxor, 0)
DEF_TEST(int16_t, int16, land, 0)
DEF_TEST(int16_t, int16, lor, 0)
DEF_TEST(int16_t, int16, lxor, 0)
DEF_TEST(int32_t, int32, sum, 0)
DEF_TEST(int32_t, int32, prod, 0)
DEF_TEST(int32_t, int32, min, 0)
DEF_TEST(int32_t, int32, max, 0)
DEF_TEST(int32_t, int32, band, 0)
DEF_TEST(int32_t, int32, bor, 0)
DEF_TEST(int32_t, int32, bxor, 0)
DEF_TEST(int32_t, int32, land, 0)
DEF_TEST(int32_t, int32, lor, 0)
DEF_TEST(int32_t, int32, lxor, 0)
DEF_TEST(int64_t, int64, sum, 0)
DEF_TEST(int64_t, int64, prod, 0)
DEF_TEST(int64_t, int64, min, 0)
DEF_TEST(int64_t, int64, max, 0)
DEF_TEST(int64_t, int64, band, 0)
DEF_TEST(int64_t, int64, bor, 0)
DEF_TEST(int64_t, int64, bxor, 0)
DEF_TEST(int64_t, int64, land, 0)
DEF_TEST(int64_t, int64, lor, 0)
DEF_TEST(int64_t, int64, lxor, 0)
DEF_TEST(uint8_t, uint8, sum, 0)
DEF_TEST(uint8_t, uint8, prod, 0)
DEF_TEST(uint8_t, uint8, min, 0)
DEF_TEST(uint8_t, uint8, max, 0)
DEF_TEST(uint8_t, uint8, band, 0)
DEF_TEST(uint8_t, uint8, bor, 0)
DEF_TEST(uint8_t, uint8, bxor, 0)
DEF_TEST(uint8_t, uint8, land, 0)
DEF_TEST(uint8_t, uint8, lor, 0)
DEF_TEST(uint8_t, uint8, lxor, 0)
DEF_TEST(uint16_t, uint16, sum, 0)
DEF_TEST(uint16_t, uint16, prod, 0)
DEF_TEST(uint16_t, uint16, min, 0)
DEF_TEST(uint16_t, uint16, max, 0)
DEF_TEST(uint16_t, uint16, band, 0)
DEF_TEST(uint16_t, uint16, bor, 0)
DEF_TEST(uint16_t, uint16, bxor, 0)
DEF_TEST(uint16_t, uint16, land, 0)
DEF_TEST(uint16_t, uint16, lor, 0)
DEF_TEST(uint16_t, uint16, lxor, 0)
DEF_TEST(uint32_t, uint32, sum, 0)
DEF_TEST(uint32_t, uint32, prod, 0)
DEF_TEST(uint32_t, uint32, min, 0)
DEF_TEST(uint32_t, uint32, max, 0)
DEF_TEST(uint32_t, uint32, band, 0)
DEF_TEST(uint32_t, uint32, bor, 0)
DEF_TEST(uint32_t, uint32, bxor, 0)
DEF_TEST(uint32_t, uint32, land, 0)
DEF_TEST(uint32_t, uint32, lor, 0)
DEF_TEST(uint32_t, uint32, lxor, 0)
DEF_TEST(uint64_t, uint64, sum, 0)
DEF_TEST(uint64_t, uint64, prod, 0)
DEF_TEST(uint64_t, uint64, min, 0)
DEF_TEST(uint64_t, uint64, max, 0)
DEF_TEST(uint64_t, uint64, band, 0)
DEF_TEST(uint64_t, uint64, bor, 0)
DEF_TEST(uint64_t, uint64, bxor, 0)
DEF_TEST(uint64_t, uint64, land, 0)
DEF_TEST(uint64_t, uint64, lor, 0)
DEF_TEST(uint64_t, uint64, lxor, 0)
DEF_TEST(float, float32, sum, 1)
DEF_TEST(float, float32, prod, 1)
DEF_TEST(float, float32, min, 1)
DEF_TEST(float, float32, max, 1)
DEF_TEST(double, float64, sum, 1)
DEF_TEST(double, float64, prod, 1)
DEF_TEST(double, float64, min, 1)
DEF_TEST(double, float64, max, 1)
int main(void) {
    if (!ucc_arch_avx2_supported()) { printf("SKIP: CPU lacks AVX2\n"); return 0; }
    int failures = 0;
    failures += test_int8_sum();
    failures += test_int8_prod();
    failures += test_int8_min();
    failures += test_int8_max();
    failures += test_int8_band();
    failures += test_int8_bor();
    failures += test_int8_bxor();
    failures += test_int8_land();
    failures += test_int8_lor();
    failures += test_int8_lxor();
    failures += test_int16_sum();
    failures += test_int16_prod();
    failures += test_int16_min();
    failures += test_int16_max();
    failures += test_int16_band();
    failures += test_int16_bor();
    failures += test_int16_bxor();
    failures += test_int16_land();
    failures += test_int16_lor();
    failures += test_int16_lxor();
    failures += test_int32_sum();
    failures += test_int32_prod();
    failures += test_int32_min();
    failures += test_int32_max();
    failures += test_int32_band();
    failures += test_int32_bor();
    failures += test_int32_bxor();
    failures += test_int32_land();
    failures += test_int32_lor();
    failures += test_int32_lxor();
    failures += test_int64_sum();
    failures += test_int64_prod();
    failures += test_int64_min();
    failures += test_int64_max();
    failures += test_int64_band();
    failures += test_int64_bor();
    failures += test_int64_bxor();
    failures += test_int64_land();
    failures += test_int64_lor();
    failures += test_int64_lxor();
    failures += test_uint8_sum();
    failures += test_uint8_prod();
    failures += test_uint8_min();
    failures += test_uint8_max();
    failures += test_uint8_band();
    failures += test_uint8_bor();
    failures += test_uint8_bxor();
    failures += test_uint8_land();
    failures += test_uint8_lor();
    failures += test_uint8_lxor();
    failures += test_uint16_sum();
    failures += test_uint16_prod();
    failures += test_uint16_min();
    failures += test_uint16_max();
    failures += test_uint16_band();
    failures += test_uint16_bor();
    failures += test_uint16_bxor();
    failures += test_uint16_land();
    failures += test_uint16_lor();
    failures += test_uint16_lxor();
    failures += test_uint32_sum();
    failures += test_uint32_prod();
    failures += test_uint32_min();
    failures += test_uint32_max();
    failures += test_uint32_band();
    failures += test_uint32_bor();
    failures += test_uint32_bxor();
    failures += test_uint32_land();
    failures += test_uint32_lor();
    failures += test_uint32_lxor();
    failures += test_uint64_sum();
    failures += test_uint64_prod();
    failures += test_uint64_min();
    failures += test_uint64_max();
    failures += test_uint64_band();
    failures += test_uint64_bor();
    failures += test_uint64_bxor();
    failures += test_uint64_land();
    failures += test_uint64_lor();
    failures += test_uint64_lxor();
    failures += test_float32_sum();
    failures += test_float32_prod();
    failures += test_float32_min();
    failures += test_float32_max();
    failures += test_float64_sum();
    failures += test_float64_prod();
    failures += test_float64_min();
    failures += test_float64_max();
    if (failures == 0) printf("ALL PASS\n");
    else printf("FAILURES=%d\n", failures);
    return failures ? 1 : 0;
}
