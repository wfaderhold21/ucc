/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_ARCH_X86_64_REDUCE_AVX2_H_
#define UCC_ARCH_X86_64_REDUCE_AVX2_H_

#if defined(__x86_64__)

#include <immintrin.h>
#include <stddef.h>
#include <stdint.h>

/*
 * AVX2 SIMD reduce kernels for the CPU (ec_cpu) reduce path.
 *
 * Each kernel is a pure reduce over `srcs[0..n_srcs-1]` into `dst`:
 *
 *   dst[i] = srcs[0][i] OP srcs[1][i] OP ... OP srcs[n_srcs-1][i]
 *
 * The reduction is a sequential per-lane left-fold in source order.  This
 * matches the scalar reference (src/components/ec/cpu/ec_cpu_reduce.c)
 * exactly:
 *
 *   - sum/prod/band/bor/bxor/land/lor/lxor are left-associative in the scalar
 *     DO_OP_*_N macros, so a left-fold is bitwise identical (integer) or
 *     IEEE-identical (float sum/prod) for every n_srcs.
 *   - min/max/lxor are computed by the scalar as a balanced tree, but the
 *     operations are associative (min/max commutative+idempotent, lxor is
 *     truthiness XOR), so the per-lane left-fold yields the same value.
 *
 * alpha (AVG / REDUCE_WITH_ALPHA) is NOT applied here; the dispatcher applies
 * it after the call, mirroring the scalar path.
 *
 * Kernels are named per (dtype, op) and carry no dispatch table; a central
 * reduce_simd.h maps UCC_DT_* / UCC_OP_* to the kernel.  Functions are emitted
 * with target("avx2") so the surrounding TU need not be compiled with -mavx2.
 */

#ifdef __GNUC__
static inline int ucc_arch_avx2_supported(void)
{
    return __builtin_cpu_supports("avx2");
}
#endif

/* ------------------------------------------------------------------ */
/* Shared multi-step helpers                                          */
/* ------------------------------------------------------------------ */

/* int8 product: widen each byte to 16-bit, mullo, keep low byte, pack. */
static inline __attribute__((target("avx2"))) __m256i ucc_arch_avx2_mul_8bit(__m256i a, __m256i b)
{
    __m128i al = _mm256_castsi256_si128(a);
    __m128i ah = _mm256_extractf128_si256(a, 1);
    __m128i bl = _mm256_castsi256_si128(b);
    __m128i bh = _mm256_extractf128_si256(b, 1);
    __m256i pl = _mm256_mullo_epi16(_mm256_cvtepi8_epi16(al),
                                    _mm256_cvtepi8_epi16(bl));
    __m256i ph = _mm256_mullo_epi16(_mm256_cvtepi8_epi16(ah),
                                    _mm256_cvtepi8_epi16(bh));
    pl = _mm256_and_si256(pl, _mm256_set1_epi16(0xFF));
    ph = _mm256_and_si256(ph, _mm256_set1_epi16(0xFF));
    /* per 128-bit lane: pack 8 int16 -> 8 int8 (low bytes, 0..255, exact) */
    return _mm256_set_m128i(
        _mm_packus_epi16(_mm256_castsi256_si128(ph),
                         _mm256_extractf128_si256(ph, 1)),
        _mm_packus_epi16(_mm256_castsi256_si128(pl),
                         _mm256_extractf128_si256(pl, 1)));
}

/* int64 product: 32-bit cross-term decomposition, low 64 bits exact. */
static inline __attribute__((target("avx2"))) __m256i ucc_arch_avx2_mul_64bit(__m256i a, __m256i b)
{
    __m256i mask  = _mm256_set1_epi64x(0xFFFFFFFF);
    __m256i a_lo  = _mm256_and_si256(a, mask);
    __m256i b_lo  = _mm256_and_si256(b, mask);
    __m256i a_hi  = _mm256_srli_epi64(a, 32);
    __m256i b_hi  = _mm256_srli_epi64(b, 32);
    __m256i lo_lo = _mm256_mul_epu32(a_lo, b_lo);
    __m256i lo_hi = _mm256_mul_epu32(a_lo, b_hi);
    __m256i hi_lo = _mm256_mul_epu32(a_hi, b_lo);
    __m256i cross = _mm256_slli_epi64(_mm256_add_epi64(lo_hi, hi_lo), 32);
    return _mm256_add_epi64(cross, lo_lo);
}

/* signed int64 min/max via cmpgt + blendv */
static inline __attribute__((target("avx2"))) __m256i ucc_arch_avx2_min_64bit(__m256i a, __m256i b)
{
    __m256i mask = _mm256_cmpgt_epi64(a, b); /* a > b */
    return _mm256_blendv_epi8(a, b, mask);    /* a>b -> b, else a */
}

static inline __attribute__((target("avx2"))) __m256i ucc_arch_avx2_max_64bit(__m256i a, __m256i b)
{
    __m256i mask = _mm256_cmpgt_epi64(a, b); /* a > b */
    return _mm256_blendv_epi8(b, a, mask);    /* a>b -> a, else b */
}

/* unsigned int64 min/max: flip sign bit, then signed compare */
static inline __attribute__((target("avx2"))) __m256i ucc_arch_avx2_min_64bit_u(__m256i a, __m256i b)
{
    __m256i flip = _mm256_set1_epi64x((int64_t)0x8000000000000000ULL);
    __m256i mask = _mm256_cmpgt_epi64(_mm256_xor_si256(a, flip),
                                      _mm256_xor_si256(b, flip));
    return _mm256_blendv_epi8(a, b, mask);
}

static inline __attribute__((target("avx2"))) __m256i ucc_arch_avx2_max_64bit_u(__m256i a, __m256i b)
{
    __m256i flip = _mm256_set1_epi64x((int64_t)0x8000000000000000ULL);
    __m256i mask = _mm256_cmpgt_epi64(_mm256_xor_si256(a, flip),
                                      _mm256_xor_si256(b, flip));
    return _mm256_blendv_epi8(b, a, mask);
}

/* per-lane truthiness: all-ones iff nonzero */
#define UCC_RED_AVX2_TRUTHY_8BIT(v)                                            \
    _mm256_andnot_si256(_mm256_cmpeq_epi8((v), _mm256_setzero_si256()),       \
                        _mm256_set1_epi8(-1))
#define UCC_RED_AVX2_TRUTHY_16BIT(v)                                           \
    _mm256_andnot_si256(_mm256_cmpeq_epi16((v), _mm256_setzero_si256()),      \
                        _mm256_set1_epi16(-1))
#define UCC_RED_AVX2_TRUTHY_32BIT(v)                                           \
    _mm256_andnot_si256(_mm256_cmpeq_epi32((v), _mm256_setzero_si256()),      \
                        _mm256_set1_epi32(-1))
#define UCC_RED_AVX2_TRUTHY_64BIT(v)                                           \
    _mm256_andnot_si256(_mm256_cmpeq_epi64((v), _mm256_setzero_si256()),      \
                        _mm256_set1_epi64x(-1))

/* ------------------------------------------------------------------ */
/* Per-dtype vector ops (acc, v) -> acc OP v                            */
/* ------------------------------------------------------------------ */

/* INT8 */
#define UCC_RED_AVX2_INT8_VEC    __m256i
#define UCC_RED_AVX2_INT8_CTYPE  int8_t
#define UCC_RED_AVX2_INT8_LANES  32
#define UCC_RED_AVX2_INT8_LOAD(p)   _mm256_loadu_si256((const __m256i *)(p))
#define UCC_RED_AVX2_INT8_STORE(p,v) _mm256_storeu_si256((__m256i *)(p), (v))
#define UCC_RED_AVX2_INT8_SUM(a,v)  _mm256_add_epi8((a), (v))
#define UCC_RED_AVX2_INT8_PROD(a,v) ucc_arch_avx2_mul_8bit((a), (v))
#define UCC_RED_AVX2_INT8_MIN(a,v)  _mm256_min_epi8((a), (v))
#define UCC_RED_AVX2_INT8_MAX(a,v)  _mm256_max_epi8((a), (v))
#define UCC_RED_AVX2_INT8_BAND(a,v) _mm256_and_si256((a), (v))
#define UCC_RED_AVX2_INT8_BOR(a,v)  _mm256_or_si256((a), (v))
#define UCC_RED_AVX2_INT8_BXOR(a,v) _mm256_xor_si256((a), (v))
#define UCC_RED_AVX2_INT8_LAND(a,v)                                         \
    _mm256_and_si256(_mm256_and_si256(UCC_RED_AVX2_TRUTHY_8BIT(a),          \
                                      UCC_RED_AVX2_TRUTHY_8BIT(v)),         \
                     _mm256_set1_epi8(1))
#define UCC_RED_AVX2_INT8_LOR(a,v)                                          \
    _mm256_and_si256(_mm256_or_si256(UCC_RED_AVX2_TRUTHY_8BIT(a),          \
                                     UCC_RED_AVX2_TRUTHY_8BIT(v)),         \
                     _mm256_set1_epi8(1))
#define UCC_RED_AVX2_INT8_LXOR(a,v)                                         \
    _mm256_xor_si256(_mm256_and_si256(UCC_RED_AVX2_TRUTHY_8BIT(a),         \
                                      _mm256_set1_epi8(1)),                \
                     _mm256_and_si256(UCC_RED_AVX2_TRUTHY_8BIT(v),         \
                                      _mm256_set1_epi8(1)))

/* UINT8 */
#define UCC_RED_AVX2_UINT8_VEC    __m256i
#define UCC_RED_AVX2_UINT8_CTYPE  uint8_t
#define UCC_RED_AVX2_UINT8_LANES  32
#define UCC_RED_AVX2_UINT8_LOAD(p)   _mm256_loadu_si256((const __m256i *)(p))
#define UCC_RED_AVX2_UINT8_STORE(p,v) _mm256_storeu_si256((__m256i *)(p), (v))
#define UCC_RED_AVX2_UINT8_SUM(a,v)  _mm256_add_epi8((a), (v))
#define UCC_RED_AVX2_UINT8_PROD(a,v) ucc_arch_avx2_mul_8bit((a), (v))
#define UCC_RED_AVX2_UINT8_MIN(a,v)  _mm256_min_epu8((a), (v))
#define UCC_RED_AVX2_UINT8_MAX(a,v)  _mm256_max_epu8((a), (v))
#define UCC_RED_AVX2_UINT8_BAND(a,v) _mm256_and_si256((a), (v))
#define UCC_RED_AVX2_UINT8_BOR(a,v)  _mm256_or_si256((a), (v))
#define UCC_RED_AVX2_UINT8_BXOR(a,v) _mm256_xor_si256((a), (v))
#define UCC_RED_AVX2_UINT8_LAND(a,v) UCC_RED_AVX2_INT8_LAND(a,v)
#define UCC_RED_AVX2_UINT8_LOR(a,v)  UCC_RED_AVX2_INT8_LOR(a,v)
#define UCC_RED_AVX2_UINT8_LXOR(a,v) UCC_RED_AVX2_INT8_LXOR(a,v)

/* INT16 */
#define UCC_RED_AVX2_INT16_VEC    __m256i
#define UCC_RED_AVX2_INT16_CTYPE  int16_t
#define UCC_RED_AVX2_INT16_LANES  16
#define UCC_RED_AVX2_INT16_LOAD(p)   _mm256_loadu_si256((const __m256i *)(p))
#define UCC_RED_AVX2_INT16_STORE(p,v) _mm256_storeu_si256((__m256i *)(p), (v))
#define UCC_RED_AVX2_INT16_SUM(a,v)  _mm256_add_epi16((a), (v))
#define UCC_RED_AVX2_INT16_PROD(a,v) _mm256_mullo_epi16((a), (v))
#define UCC_RED_AVX2_INT16_MIN(a,v)  _mm256_min_epi16((a), (v))
#define UCC_RED_AVX2_INT16_MAX(a,v)  _mm256_max_epi16((a), (v))
#define UCC_RED_AVX2_INT16_BAND(a,v) _mm256_and_si256((a), (v))
#define UCC_RED_AVX2_INT16_BOR(a,v)  _mm256_or_si256((a), (v))
#define UCC_RED_AVX2_INT16_BXOR(a,v) _mm256_xor_si256((a), (v))
#define UCC_RED_AVX2_INT16_LAND(a,v)                                         \
    _mm256_and_si256(_mm256_and_si256(UCC_RED_AVX2_TRUTHY_16BIT(a),         \
                                      UCC_RED_AVX2_TRUTHY_16BIT(v)),        \
                     _mm256_set1_epi16(1))
#define UCC_RED_AVX2_INT16_LOR(a,v)                                          \
    _mm256_and_si256(_mm256_or_si256(UCC_RED_AVX2_TRUTHY_16BIT(a),         \
                                     UCC_RED_AVX2_TRUTHY_16BIT(v)),        \
                     _mm256_set1_epi16(1))
#define UCC_RED_AVX2_INT16_LXOR(a,v)                                         \
    _mm256_xor_si256(_mm256_and_si256(UCC_RED_AVX2_TRUTHY_16BIT(a),        \
                                      _mm256_set1_epi16(1)),               \
                     _mm256_and_si256(UCC_RED_AVX2_TRUTHY_16BIT(v),        \
                                      _mm256_set1_epi16(1)))

/* UINT16 */
#define UCC_RED_AVX2_UINT16_VEC    __m256i
#define UCC_RED_AVX2_UINT16_CTYPE  uint16_t
#define UCC_RED_AVX2_UINT16_LANES  16
#define UCC_RED_AVX2_UINT16_LOAD(p)   _mm256_loadu_si256((const __m256i *)(p))
#define UCC_RED_AVX2_UINT16_STORE(p,v) _mm256_storeu_si256((__m256i *)(p), (v))
#define UCC_RED_AVX2_UINT16_SUM(a,v)  _mm256_add_epi16((a), (v))
#define UCC_RED_AVX2_UINT16_PROD(a,v) _mm256_mullo_epi16((a), (v))
#define UCC_RED_AVX2_UINT16_MIN(a,v)  _mm256_min_epu16((a), (v))
#define UCC_RED_AVX2_UINT16_MAX(a,v)  _mm256_max_epu16((a), (v))
#define UCC_RED_AVX2_UINT16_BAND(a,v) _mm256_and_si256((a), (v))
#define UCC_RED_AVX2_UINT16_BOR(a,v)  _mm256_or_si256((a), (v))
#define UCC_RED_AVX2_UINT16_BXOR(a,v) _mm256_xor_si256((a), (v))
#define UCC_RED_AVX2_UINT16_LAND(a,v) UCC_RED_AVX2_INT16_LAND(a,v)
#define UCC_RED_AVX2_UINT16_LOR(a,v)  UCC_RED_AVX2_INT16_LOR(a,v)
#define UCC_RED_AVX2_UINT16_LXOR(a,v) UCC_RED_AVX2_INT16_LXOR(a,v)

/* INT32 */
#define UCC_RED_AVX2_INT32_VEC    __m256i
#define UCC_RED_AVX2_INT32_CTYPE  int32_t
#define UCC_RED_AVX2_INT32_LANES  8
#define UCC_RED_AVX2_INT32_LOAD(p)   _mm256_loadu_si256((const __m256i *)(p))
#define UCC_RED_AVX2_INT32_STORE(p,v) _mm256_storeu_si256((__m256i *)(p), (v))
#define UCC_RED_AVX2_INT32_SUM(a,v)  _mm256_add_epi32((a), (v))
#define UCC_RED_AVX2_INT32_PROD(a,v) _mm256_mullo_epi32((a), (v))
#define UCC_RED_AVX2_INT32_MIN(a,v)  _mm256_min_epi32((a), (v))
#define UCC_RED_AVX2_INT32_MAX(a,v)  _mm256_max_epi32((a), (v))
#define UCC_RED_AVX2_INT32_BAND(a,v) _mm256_and_si256((a), (v))
#define UCC_RED_AVX2_INT32_BOR(a,v)  _mm256_or_si256((a), (v))
#define UCC_RED_AVX2_INT32_BXOR(a,v) _mm256_xor_si256((a), (v))
#define UCC_RED_AVX2_INT32_LAND(a,v)                                         \
    _mm256_and_si256(_mm256_and_si256(UCC_RED_AVX2_TRUTHY_32BIT(a),         \
                                      UCC_RED_AVX2_TRUTHY_32BIT(v)),        \
                     _mm256_set1_epi32(1))
#define UCC_RED_AVX2_INT32_LOR(a,v)                                          \
    _mm256_and_si256(_mm256_or_si256(UCC_RED_AVX2_TRUTHY_32BIT(a),         \
                                     UCC_RED_AVX2_TRUTHY_32BIT(v)),        \
                     _mm256_set1_epi32(1))
#define UCC_RED_AVX2_INT32_LXOR(a,v)                                         \
    _mm256_xor_si256(_mm256_and_si256(UCC_RED_AVX2_TRUTHY_32BIT(a),        \
                                      _mm256_set1_epi32(1)),               \
                     _mm256_and_si256(UCC_RED_AVX2_TRUTHY_32BIT(v),        \
                                      _mm256_set1_epi32(1)))

/* UINT32 */
#define UCC_RED_AVX2_UINT32_VEC    __m256i
#define UCC_RED_AVX2_UINT32_CTYPE  uint32_t
#define UCC_RED_AVX2_UINT32_LANES  8
#define UCC_RED_AVX2_UINT32_LOAD(p)   _mm256_loadu_si256((const __m256i *)(p))
#define UCC_RED_AVX2_UINT32_STORE(p,v) _mm256_storeu_si256((__m256i *)(p), (v))
#define UCC_RED_AVX2_UINT32_SUM(a,v)  _mm256_add_epi32((a), (v))
#define UCC_RED_AVX2_UINT32_PROD(a,v) _mm256_mullo_epi32((a), (v))
#define UCC_RED_AVX2_UINT32_MIN(a,v)  _mm256_min_epu32((a), (v))
#define UCC_RED_AVX2_UINT32_MAX(a,v)  _mm256_max_epu32((a), (v))
#define UCC_RED_AVX2_UINT32_BAND(a,v) _mm256_and_si256((a), (v))
#define UCC_RED_AVX2_UINT32_BOR(a,v)  _mm256_or_si256((a), (v))
#define UCC_RED_AVX2_UINT32_BXOR(a,v) _mm256_xor_si256((a), (v))
#define UCC_RED_AVX2_UINT32_LAND(a,v) UCC_RED_AVX2_INT32_LAND(a,v)
#define UCC_RED_AVX2_UINT32_LOR(a,v)  UCC_RED_AVX2_INT32_LOR(a,v)
#define UCC_RED_AVX2_UINT32_LXOR(a,v) UCC_RED_AVX2_INT32_LXOR(a,v)

/* INT64 */
#define UCC_RED_AVX2_INT64_VEC    __m256i
#define UCC_RED_AVX2_INT64_CTYPE  int64_t
#define UCC_RED_AVX2_INT64_LANES  4
#define UCC_RED_AVX2_INT64_LOAD(p)   _mm256_loadu_si256((const __m256i *)(p))
#define UCC_RED_AVX2_INT64_STORE(p,v) _mm256_storeu_si256((__m256i *)(p), (v))
#define UCC_RED_AVX2_INT64_SUM(a,v)  _mm256_add_epi64((a), (v))
#define UCC_RED_AVX2_INT64_PROD(a,v) ucc_arch_avx2_mul_64bit((a), (v))
#define UCC_RED_AVX2_INT64_MIN(a,v)  ucc_arch_avx2_min_64bit((a), (v))
#define UCC_RED_AVX2_INT64_MAX(a,v)  ucc_arch_avx2_max_64bit((a), (v))
#define UCC_RED_AVX2_INT64_BAND(a,v) _mm256_and_si256((a), (v))
#define UCC_RED_AVX2_INT64_BOR(a,v)  _mm256_or_si256((a), (v))
#define UCC_RED_AVX2_INT64_BXOR(a,v) _mm256_xor_si256((a), (v))
#define UCC_RED_AVX2_INT64_LAND(a,v)                                         \
    _mm256_and_si256(_mm256_and_si256(UCC_RED_AVX2_TRUTHY_64BIT(a),         \
                                      UCC_RED_AVX2_TRUTHY_64BIT(v)),        \
                     _mm256_set1_epi64x(1))
#define UCC_RED_AVX2_INT64_LOR(a,v)                                          \
    _mm256_and_si256(_mm256_or_si256(UCC_RED_AVX2_TRUTHY_64BIT(a),         \
                                     UCC_RED_AVX2_TRUTHY_64BIT(v)),        \
                     _mm256_set1_epi64x(1))
#define UCC_RED_AVX2_INT64_LXOR(a,v)                                         \
    _mm256_xor_si256(_mm256_and_si256(UCC_RED_AVX2_TRUTHY_64BIT(a),        \
                                      _mm256_set1_epi64x(1)),               \
                     _mm256_and_si256(UCC_RED_AVX2_TRUTHY_64BIT(v),        \
                                      _mm256_set1_epi64x(1)))

/* UINT64 */
#define UCC_RED_AVX2_UINT64_VEC    __m256i
#define UCC_RED_AVX2_UINT64_CTYPE  uint64_t
#define UCC_RED_AVX2_UINT64_LANES  4
#define UCC_RED_AVX2_UINT64_LOAD(p)   _mm256_loadu_si256((const __m256i *)(p))
#define UCC_RED_AVX2_UINT64_STORE(p,v) _mm256_storeu_si256((__m256i *)(p), (v))
#define UCC_RED_AVX2_UINT64_SUM(a,v)  _mm256_add_epi64((a), (v))
#define UCC_RED_AVX2_UINT64_PROD(a,v) ucc_arch_avx2_mul_64bit((a), (v))
#define UCC_RED_AVX2_UINT64_MIN(a,v)  ucc_arch_avx2_min_64bit_u((a), (v))
#define UCC_RED_AVX2_UINT64_MAX(a,v)  ucc_arch_avx2_max_64bit_u((a), (v))
#define UCC_RED_AVX2_UINT64_BAND(a,v) _mm256_and_si256((a), (v))
#define UCC_RED_AVX2_UINT64_BOR(a,v)  _mm256_or_si256((a), (v))
#define UCC_RED_AVX2_UINT64_BXOR(a,v) _mm256_xor_si256((a), (v))
#define UCC_RED_AVX2_UINT64_LAND(a,v) UCC_RED_AVX2_INT64_LAND(a,v)
#define UCC_RED_AVX2_UINT64_LOR(a,v)  UCC_RED_AVX2_INT64_LOR(a,v)
#define UCC_RED_AVX2_UINT64_LXOR(a,v) UCC_RED_AVX2_INT64_LXOR(a,v)

/* FLOAT32 */
#define UCC_RED_AVX2_FLOAT32_VEC    __m256
#define UCC_RED_AVX2_FLOAT32_CTYPE  float
#define UCC_RED_AVX2_FLOAT32_LANES  8
#define UCC_RED_AVX2_FLOAT32_LOAD(p)   _mm256_loadu_ps((p))
#define UCC_RED_AVX2_FLOAT32_STORE(p,v) _mm256_storeu_ps((p), (v))
#define UCC_RED_AVX2_FLOAT32_SUM(a,v) _mm256_add_ps((a), (v))
#define UCC_RED_AVX2_FLOAT32_PROD(a,v) _mm256_mul_ps((a), (v))
#define UCC_RED_AVX2_FLOAT32_MIN(a,v) _mm256_min_ps((a), (v))
#define UCC_RED_AVX2_FLOAT32_MAX(a,v) _mm256_max_ps((a), (v))

/* FLOAT64 */
#define UCC_RED_AVX2_FLOAT64_VEC    __m256d
#define UCC_RED_AVX2_FLOAT64_CTYPE  double
#define UCC_RED_AVX2_FLOAT64_LANES  4
#define UCC_RED_AVX2_FLOAT64_LOAD(p)   _mm256_loadu_pd((p))
#define UCC_RED_AVX2_FLOAT64_STORE(p,v) _mm256_storeu_pd((p), (v))
#define UCC_RED_AVX2_FLOAT64_SUM(a,v) _mm256_add_pd((a), (v))
#define UCC_RED_AVX2_FLOAT64_PROD(a,v) _mm256_mul_pd((a), (v))
#define UCC_RED_AVX2_FLOAT64_MIN(a,v) _mm256_min_pd((a), (v))
#define UCC_RED_AVX2_FLOAT64_MAX(a,v) _mm256_max_pd((a), (v))

/* ------------------------------------------------------------------ */
/* Scalar tail ops (acc, val) -> acc OP val, exact scalar semantics    */
/* ------------------------------------------------------------------ */
/* shared for signed/unsigned; min/max/truthiness do not care about sign */
#define UCC_RED_AVX2_S_SUM(acc,val)   ((acc) + (val))
#define UCC_RED_AVX2_S_PROD(acc,val)  ((acc) * (val))
#define UCC_RED_AVX2_S_MIN(acc,val)   ((acc) < (val) ? (acc) : (val))
#define UCC_RED_AVX2_S_MAX(acc,val)   ((acc) > (val) ? (acc) : (val))
#define UCC_RED_AVX2_S_BAND(acc,val)  ((acc) & (val))
#define UCC_RED_AVX2_S_BOR(acc,val)   ((acc) | (val))
#define UCC_RED_AVX2_S_BXOR(acc,val)  ((acc) ^ (val))
#define UCC_RED_AVX2_S_LAND(acc,val)  ((acc) && (val))
#define UCC_RED_AVX2_S_LOR(acc,val)   ((acc) || (val))
#define UCC_RED_AVX2_S_LXOR(acc,val)  ((!(acc)) != (!(val)))
#define UCC_RED_AVX2_DEF_FOLD(CTYPE, DT, UTAG, OP, UOP)                       \
    static inline __attribute__((target("avx2"))) void                    \
    ucc_arch_reduce_avx2_##DT##_##OP(void *dst,                           \
                                     const void * const *srcs,            \
                                     size_t count, unsigned n_srcs)       \
    {                                                                      \
        const CTYPE **restrict s = (const CTYPE **)srcs;                 \
        CTYPE *restrict d = (CTYPE *)dst;                                \
        const unsigned lanes = UCC_RED_AVX2_##UTAG##_LANES;               \
        size_t i;                                                          \
        for (i = 0; i + lanes <= count; i += lanes) {                     \
            UCC_RED_AVX2_##UTAG##_VEC acc =                                \
                UCC_RED_AVX2_##UTAG##_LOAD(&s[0][i]);                     \
            unsigned j;                                                    \
            for (j = 1; j < n_srcs; j++) {                                 \
                UCC_RED_AVX2_##UTAG##_VEC v =                              \
                    UCC_RED_AVX2_##UTAG##_LOAD(&s[j][i]);                 \
                acc = UCC_RED_AVX2_##UTAG##_##UOP(acc, v);                 \
            }                                                              \
            UCC_RED_AVX2_##UTAG##_STORE(&d[i], acc);                      \
        }                                                                  \
        for (; i < count; i++) {                                           \
            CTYPE acc = s[0][i];                                          \
            unsigned j;                                                    \
            for (j = 1; j < n_srcs; j++) {                                 \
                acc = UCC_RED_AVX2_S_##UOP(acc, s[j][i]);                  \
            }                                                              \
            d[i] = acc;                                                    \
        }                                                                  \
    }


/* TREE reduction helpers: exact DO_OP__N shape (min/max/lxor)         */
#define UCC_RED_AVX2_TREE2(UTAG, UOP, a, b)     UCC_RED_AVX2_##UTAG##_##UOP(a, b)
#define UCC_RED_AVX2_TREE3(UTAG, UOP, a, b, c)     UCC_RED_AVX2_##UTAG##_##UOP(UCC_RED_AVX2_##UTAG##_##UOP(a, b), c)
#define UCC_RED_AVX2_TREE4(UTAG, UOP, a, b, c, d) \
    UCC_RED_AVX2_##UTAG##_##UOP(UCC_RED_AVX2_##UTAG##_##UOP(a, b), \
                                UCC_RED_AVX2_##UTAG##_##UOP(c, d))
#define UCC_RED_AVX2_TREE5(UTAG, UOP, a, b, c, d, e) \
    UCC_RED_AVX2_##UTAG##_##UOP(UCC_RED_AVX2_##UTAG##_##UOP(a, b), \
                                UCC_RED_AVX2_TREE3(UTAG, UOP, c, d, e))
#define UCC_RED_AVX2_TREE6(UTAG, UOP, a, b, c, d, e, f) \
    UCC_RED_AVX2_##UTAG##_##UOP(UCC_RED_AVX2_TREE3(UTAG, UOP, a, b, c), \
                                UCC_RED_AVX2_TREE3(UTAG, UOP, d, e, f))
#define UCC_RED_AVX2_TREE7(UTAG, UOP, a, b, c, d, e, f, g) \
    UCC_RED_AVX2_##UTAG##_##UOP(UCC_RED_AVX2_TREE3(UTAG, UOP, a, b, c), \
                                UCC_RED_AVX2_TREE4(UTAG, UOP, d, e, f, g))
#define UCC_RED_AVX2_TREE8(UTAG, UOP, a, b, c, d, e, f, g, h) \
    UCC_RED_AVX2_##UTAG##_##UOP(UCC_RED_AVX2_TREE4(UTAG, UOP, a, b, c, d), \
                                UCC_RED_AVX2_TREE4(UTAG, UOP, e, f, g, h))

/* scalar tree helpers */
#define UCC_RED_AVX2_STREE2(UOP, a, b)     UCC_RED_AVX2_S_##UOP(a, b)
#define UCC_RED_AVX2_STREE3(UOP, a, b, c)     UCC_RED_AVX2_S_##UOP(UCC_RED_AVX2_S_##UOP(a, b), c)
#define UCC_RED_AVX2_STREE4(UOP, a, b, c, d)     UCC_RED_AVX2_S_##UOP(UCC_RED_AVX2_S_##UOP(a, b), \
                         UCC_RED_AVX2_S_##UOP(c, d))
#define UCC_RED_AVX2_STREE5(UOP, a, b, c, d, e)     UCC_RED_AVX2_S_##UOP(UCC_RED_AVX2_S_##UOP(a, b), \
                         UCC_RED_AVX2_STREE3(UOP, c, d, e))
#define UCC_RED_AVX2_STREE6(UOP, a, b, c, d, e, f)     UCC_RED_AVX2_S_##UOP(UCC_RED_AVX2_STREE3(UOP, a, b, c), \
                         UCC_RED_AVX2_STREE3(UOP, d, e, f))
#define UCC_RED_AVX2_STREE7(UOP, a, b, c, d, e, f, g)     UCC_RED_AVX2_S_##UOP(UCC_RED_AVX2_STREE3(UOP, a, b, c), \
                         UCC_RED_AVX2_STREE4(UOP, d, e, f, g))
#define UCC_RED_AVX2_STREE8(UOP, a, b, c, d, e, f, g, h)     UCC_RED_AVX2_S_##UOP(UCC_RED_AVX2_STREE4(UOP, a, b, c, d), \
                         UCC_RED_AVX2_STREE4(UOP, e, f, g, h))

/* TREE kernel: exact DO_OP__N tree, then fold from source 8           */
#define UCC_RED_AVX2_DEF_TREE(CTYPE, DT, UTAG, OP, UOP)                       \
    static inline __attribute__((target("avx2"))) void                    \
    ucc_arch_reduce_avx2_##DT##_##OP(void *dst,                           \
                                     const void * const *srcs,            \
                                     size_t count, unsigned n_srcs)       \
    {                                                                      \
        const CTYPE **restrict s = (const CTYPE **)srcs;                 \
        CTYPE *restrict d = (CTYPE *)dst;                                \
        const unsigned lanes = UCC_RED_AVX2_##UTAG##_LANES;               \
        size_t i;                                                          \
        for (i = 0; i + lanes <= count; i += lanes) {                     \
            unsigned k = n_srcs < 8 ? n_srcs : 8;                        \
            UCC_RED_AVX2_##UTAG##_VEC v[8] = {0};                            \
            unsigned j;                                                    \
            for (j = 0; j < k; j++) v[j] =                                 \
                UCC_RED_AVX2_##UTAG##_LOAD(&s[j][i]);                    \
            UCC_RED_AVX2_##UTAG##_VEC acc = v[0];                        \
            switch (k) {                                                   \
            case 2: acc = UCC_RED_AVX2_TREE2(UTAG, UOP, v[0], v[1]);     \
                    break;                                                 \
            case 3: acc = UCC_RED_AVX2_TREE3(UTAG, UOP, v[0], v[1],      \
                                             v[2]); break;                 \
            case 4: acc = UCC_RED_AVX2_TREE4(UTAG, UOP, v[0], v[1],      \
                                             v[2], v[3]); break;           \
            case 5: acc = UCC_RED_AVX2_TREE5(UTAG, UOP, v[0], v[1],      \
                                             v[2], v[3], v[4]); break;     \
            case 6: acc = UCC_RED_AVX2_TREE6(UTAG, UOP, v[0], v[1],      \
                                             v[2], v[3], v[4], v[5]);      \
                    break;                                                 \
            case 7: acc = UCC_RED_AVX2_TREE7(UTAG, UOP, v[0], v[1],      \
                                             v[2], v[3], v[4], v[5],      \
                                             v[6]); break;                 \
            case 8: acc = UCC_RED_AVX2_TREE8(UTAG, UOP, v[0], v[1],      \
                                             v[2], v[3], v[4], v[5],      \
                                             v[6], v[7]); break;           \
            }                                                              \
            for (j = k; j < n_srcs; j++) {                                 \
                acc = UCC_RED_AVX2_##UTAG##_##UOP(                         \
                    acc, UCC_RED_AVX2_##UTAG##_LOAD(&s[j][i]));          \
            }                                                              \
            UCC_RED_AVX2_##UTAG##_STORE(&d[i], acc);                      \
        }                                                                  \
        for (; i < count; i++) {                                           \
            unsigned k = n_srcs < 8 ? n_srcs : 8;                        \
            CTYPE v[8] = {0};                                                \
            unsigned j;                                                    \
            for (j = 0; j < k; j++) v[j] = s[j][i];                       \
            CTYPE acc = v[0];                                         \
            switch (k) {                                                   \
            case 2: acc = UCC_RED_AVX2_STREE2(UOP, v[0], v[1]); break;   \
            case 3: acc = UCC_RED_AVX2_STREE3(UOP, v[0], v[1], v[2]);     \
                    break;                                                 \
            case 4: acc = UCC_RED_AVX2_STREE4(UOP, v[0], v[1], v[2],     \
                                              v[3]); break;                \
            case 5: acc = UCC_RED_AVX2_STREE5(UOP, v[0], v[1], v[2],     \
                                              v[3], v[4]); break;          \
            case 6: acc = UCC_RED_AVX2_STREE6(UOP, v[0], v[1], v[2],     \
                                              v[3], v[4], v[5]); break;    \
            case 7: acc = UCC_RED_AVX2_STREE7(UOP, v[0], v[1], v[2],     \
                                              v[3], v[4], v[5], v[6]);     \
                    break;                                                 \
            case 8: acc = UCC_RED_AVX2_STREE8(UOP, v[0], v[1], v[2],     \
                                              v[3], v[4], v[5], v[6],      \
                                              v[7]); break;                \
            }                                                              \
            for (j = k; j < n_srcs; j++) {                                 \
                acc = UCC_RED_AVX2_S_##UOP(acc, s[j][i]);                 \
            }                                                              \
            d[i] = acc;                                                    \
        }                                                                  \
    }

/* dispatcher */
#define UCC_RED_AVX2_DEF_REDUCE(CTYPE, DT, UTAG, OP, UOP, STRTG)            \
    UCC_RED_AVX2_DEF_##STRTG(CTYPE, DT, UTAG, OP, UOP)

/* ------------------------------------------------------------------ */
/* Instantiations: per (dtype, op)                                     */
/* ------------------------------------------------------------------ */

/* ------------------------------------------------------------------ */
/* Instantiations: per (dtype, op)                                     */
/* ------------------------------------------------------------------ */

/* ------------------------------------------------------------------ */
/* Instantiations: per (dtype, op)                                     */
/* ------------------------------------------------------------------ */

/* ------------------------------------------------------------------ */
/* Instantiations: per (dtype, op)                                     */
/* ------------------------------------------------------------------ */
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, sum, SUM, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, prod, PROD, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, min, MIN, TREE)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, max, MAX, TREE)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, band, BAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, bor, BOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, bxor, BXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, land, LAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, lor, LOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, lxor, LXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, sum, SUM, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, prod, PROD, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, min, MIN, TREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, max, MAX, TREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, band, BAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, bor, BOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, bxor, BXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, land, LAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, lor, LOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, lxor, LXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, sum, SUM, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, prod, PROD, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, min, MIN, TREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, max, MAX, TREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, band, BAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, bor, BOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, bxor, BXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, land, LAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, lor, LOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, lxor, LXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, sum, SUM, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, prod, PROD, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, min, MIN, TREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, max, MAX, TREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, band, BAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, bor, BOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, bxor, BXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, land, LAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, lor, LOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, lxor, LXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, sum, SUM, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, prod, PROD, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, min, MIN, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, max, MAX, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, band, BAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, bor, BOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, bxor, BXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, land, LAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, lor, LOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, lxor, LXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, sum, SUM, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, prod, PROD, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, min, MIN, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, max, MAX, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, band, BAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, bor, BOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, bxor, BXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, land, LAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, lor, LOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, lxor, LXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, sum, SUM, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, prod, PROD, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, min, MIN, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, max, MAX, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, band, BAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, bor, BOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, bxor, BXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, land, LAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, lor, LOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, lxor, LXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, sum, SUM, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, prod, PROD, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, min, MIN, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, max, MAX, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, band, BAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, bor, BOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, bxor, BXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, land, LAND, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, lor, LOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, lxor, LXOR, FOLD)
UCC_RED_AVX2_DEF_REDUCE(float, float32, FLOAT32, sum, SUM, FOLD)
UCC_RED_AVX2_DEF_REDUCE(float, float32, FLOAT32, prod, PROD, FOLD)
UCC_RED_AVX2_DEF_REDUCE(float, float32, FLOAT32, min, MIN, TREE)
UCC_RED_AVX2_DEF_REDUCE(float, float32, FLOAT32, max, MAX, TREE)
UCC_RED_AVX2_DEF_REDUCE(double, float64, FLOAT64, sum, SUM, FOLD)
UCC_RED_AVX2_DEF_REDUCE(double, float64, FLOAT64, prod, PROD, FOLD)
UCC_RED_AVX2_DEF_REDUCE(double, float64, FLOAT64, min, MIN, TREE)
UCC_RED_AVX2_DEF_REDUCE(double, float64, FLOAT64, max, MAX, TREE)

#endif /* defined(__x86_64__) */
#endif /* UCC_ARCH_X86_64_REDUCE_AVX2_H_ */
