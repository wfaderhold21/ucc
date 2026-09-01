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
 * Each kernel dispatches on n_srcs and emits the per-lane reduction
 * statically unrolled, with one named vector local per source (no
 * array, no spills).  Two association strategies:
 *
 *   TREE (sum/prod/band/bor/bxor/land/lor/lxor): the exact
 *   left-associative chain of the scalar reference
 *   (src/components/ec/cpu/ec_cpu_reduce.c):
 *     n_srcs <= 8:   ((s0 OP s1) OP s2) ... OP s(n_srcs-1)
 *     n_srcs  > 8:   the 8-wide left chain, then s8..s(n_srcs-1) folded
 *                    in left-associatively
 *   -> bitwise identical to the scalar for every dtype/op/n_srcs.
 *
 *   BTREE (min/max only): a balanced tree.  min/max are exact,
 *   associative and commutative (IEEE min/max pick the non-NaN operand
 *   regardless of association), so the tree is bitwise identical to the
 *   scalar's DO_OP__N shape too, with ~log2(n_srcs) dependency depth
 *   instead of n_srcs-1.
 *
 * The eight source base pointers are hoisted to locals (p0..p7) so the
 * vector loads use only the loop index (a runtime pointer array per
 * iteration forces dependent loads and blocks codegen).
 *
 * All independent source loads are issued up front, giving the compiler
 * the ILP to overlap the OP chain (a runtime inner fold loop prevents
 * the outer element loop from being unrolled, leaving the chain
 * latency-bound at n_srcs >= 4).
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
/*
 * Wrapping (mod 2^n) integer adds.  The SSE/AVX integer add instructions
 * saturate, but C integer arithmetic wraps, and the scalar reduce
 * reference (DO_OP_SUM) is C.  Widen to a width where the sum cannot
 * overflow (8->16, 16->32), add there exactly, then truncate to the
 * operand width.  Shared by the signed and unsigned 8/16-bit SUM/AVG
 * kernels (same bit patterns).
 */
static inline __attribute__((target("avx2"))) __m256i ucc_arch_avx2_add_epi8_wrap(__m256i a, __m256i b)
{
    const __m128i shuf =
        _mm_setr_epi8(0, 2, 4, 6, 8, 10, 12, 14,
                      0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80);
    const __m128i m8 = _mm_set1_epi16(0xFF);

    __m128i al = _mm256_castsi256_si128(a);
    __m128i ah = _mm256_extractf128_si256(a, 1);
    __m128i bl = _mm256_castsi256_si128(b);
    __m128i bh = _mm256_extractf128_si256(b, 1);

    /* each 128-bit half holds 16 int8 lanes; cvt handles 8 at a time */
    __m128i t  = _mm_add_epi16(_mm_cvtepi8_epi16(al), _mm_cvtepi8_epi16(bl));
    __m128i tu = _mm_add_epi16(_mm_cvtepi8_epi16(_mm_srli_si128(al, 8)),
                                _mm_cvtepi8_epi16(_mm_srli_si128(bl, 8)));
    __m128i rl  = _mm_shuffle_epi8(_mm_and_si128(t,  m8), shuf);
    __m128i rlu = _mm_shuffle_epi8(_mm_and_si128(tu, m8), shuf);

    t  = _mm_add_epi16(_mm_cvtepi8_epi16(ah), _mm_cvtepi8_epi16(bh));
    tu = _mm_add_epi16(_mm_cvtepi8_epi16(_mm_srli_si128(ah, 8)),
                        _mm_cvtepi8_epi16(_mm_srli_si128(bh, 8)));
    __m128i rh  = _mm_shuffle_epi8(_mm_and_si128(t,  m8), shuf);
    __m128i rhu = _mm_shuffle_epi8(_mm_and_si128(tu, m8), shuf);

    /* rl holds lanes 0-7 in bytes 0-7; shift rlu into bytes 8-15 */
    return _mm256_set_m128i(
        _mm_or_si128(rh, _mm_slli_si128(rhu, 8)),
        _mm_or_si128(rl, _mm_slli_si128(rlu, 8)));
}

static inline __attribute__((target("avx2"))) __m256i ucc_arch_avx2_add_epi16_wrap(__m256i a, __m256i b)
{
    const __m128i shuf =
        _mm_setr_epi8(0, 1, 4, 5, 8, 9, 12, 13,
                      0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80);
    const __m128i m16 = _mm_set1_epi32(0xFFFF);

    __m128i al = _mm256_castsi256_si128(a);
    __m128i ah = _mm256_extractf128_si256(a, 1);
    __m128i bl = _mm256_castsi256_si128(b);
    __m128i bh = _mm256_extractf128_si256(b, 1);

    /* each 128-bit half holds 8 int16 lanes; cvt handles 4 at a time */
    __m128i t  = _mm_add_epi32(_mm_cvtepi16_epi32(al), _mm_cvtepi16_epi32(bl));
    __m128i tu = _mm_add_epi32(_mm_cvtepi16_epi32(_mm_srli_si128(al, 8)),
                                _mm_cvtepi16_epi32(_mm_srli_si128(bl, 8)));
    __m128i rl  = _mm_shuffle_epi8(_mm_and_si128(t,  m16), shuf);
    __m128i rlu = _mm_shuffle_epi8(_mm_and_si128(tu, m16), shuf);

    t  = _mm_add_epi32(_mm_cvtepi16_epi32(ah), _mm_cvtepi16_epi32(bh));
    tu = _mm_add_epi32(_mm_cvtepi16_epi32(_mm_srli_si128(ah, 8)),
                        _mm_cvtepi16_epi32(_mm_srli_si128(bh, 8)));
    __m128i rh  = _mm_shuffle_epi8(_mm_and_si128(t,  m16), shuf);
    __m128i rhu = _mm_shuffle_epi8(_mm_and_si128(tu, m16), shuf);

    /* rl holds lanes 0-3 (8 bytes) in bytes 0-7; shift rlu into 8-15 */
    return _mm256_set_m128i(
        _mm_or_si128(rh, _mm_slli_si128(rhu, 8)),
        _mm_or_si128(rl, _mm_slli_si128(rlu, 8)));
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
#define UCC_RED_AVX2_INT8_SUM(a,v)  ucc_arch_avx2_add_epi8_wrap((a), (v))
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
#define UCC_RED_AVX2_UINT8_SUM(a,v)  ucc_arch_avx2_add_epi8_wrap((a), (v))
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
#define UCC_RED_AVX2_INT16_SUM(a,v)  ucc_arch_avx2_add_epi16_wrap((a), (v))
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
#define UCC_RED_AVX2_UINT16_SUM(a,v)  ucc_arch_avx2_add_epi16_wrap((a), (v))
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
/* ------------------------------------------------------------------ */
/* TREE kernel generator: per-n_srcs statically unrolled left-fold     */
/* ------------------------------------------------------------------ */
#define UCC_RED_AVX2_DEF_TREE(CTYPE, DT, UTAG, OP, UOP)                       \
    static inline __attribute__((target("avx2"))) void                    \
    ucc_arch_reduce_avx2_##DT##_##OP(void *dst,                           \
                                     const void * const *srcs,            \
                                     size_t count, unsigned n_srcs)       \
    {                                                                      \
        const CTYPE **restrict s = (const CTYPE **)srcs;                 \
        CTYPE *restrict d = (CTYPE *)dst;                                \
        const CTYPE *restrict p0 = s[0];                     \
        const CTYPE *restrict p1 = s[1];                     \
        const CTYPE *restrict p2 = s[2];                     \
        const CTYPE *restrict p3 = s[3];                     \
        const CTYPE *restrict p4 = s[4];                     \
        const CTYPE *restrict p5 = s[5];                     \
        const CTYPE *restrict p6 = s[6];                     \
        const CTYPE *restrict p7 = s[7];                     \
        const unsigned lanes = UCC_RED_AVX2_##UTAG##_LANES;               \
        size_t i = 0;                                                     \
        if (n_srcs <= 8) {                                                 \
            switch (n_srcs) {                                              \
            case 1:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i],                     \
                                                UCC_RED_AVX2_##UTAG##_LOAD(&p0[i])); \
                }                                                          \
                break;                                                     \
            case 2:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i],                     \
                                                UCC_RED_AVX2_##UTAG##_##UOP(v0, v1)); \
                }                                                          \
                break;                                                     \
            case 3:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v1);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v2);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i], v0);               \
                }                                                          \
                break;                                                     \
            case 4:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v3 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v1);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v2);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v3);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i], v0);               \
                }                                                          \
                break;                                                     \
            case 5:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v3 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v4 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p4[i]);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v1);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v2);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v3);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v4);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i], v0);               \
                }                                                          \
                break;                                                     \
            case 6:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v3 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v4 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p4[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v5 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p5[i]);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v1);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v2);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v3);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v4);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v5);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i], v0);               \
                }                                                          \
                break;                                                     \
            case 7:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v3 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v4 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p4[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v5 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p5[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v6 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p6[i]);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v1);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v2);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v3);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v4);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v5);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v6);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i], v0);               \
                }                                                          \
                break;                                                     \
            case 8:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v3 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v4 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p4[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v5 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p5[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v6 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p6[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v7 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p7[i]);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v1);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v2);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v3);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v4);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v5);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v6);             \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v7);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i], v0);               \
                }                                                          \
                break;                                                     \
            }                                                              \
        } else {                                                           \
            for (i = 0; i + lanes <= count; i += lanes) {                 \
                UCC_RED_AVX2_##UTAG##_VEC v0 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v1 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v2 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v3 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v4 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p4[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v5 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p5[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v6 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p6[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v7 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p7[i]);                 \
                v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v1);                 \
                v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v2);                 \
                v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v3);                 \
                v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v4);                 \
                v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v5);                 \
                v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v6);                 \
                v0 = UCC_RED_AVX2_##UTAG##_##UOP(v0, v7);                 \
                unsigned j;                                                \
                for (j = 8; j < n_srcs; j++) {                            \
                    v0 = UCC_RED_AVX2_##UTAG##_##UOP(                     \
                        v0, UCC_RED_AVX2_##UTAG##_LOAD(&s[j][i]));        \
                }                                                          \
                UCC_RED_AVX2_##UTAG##_STORE(&d[i], v0);                    \
            }                                                              \
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

/* ------------------------------------------------------------------ */
/* BTREE kernel generator (min/max only): balanced-tree association    */
/* ------------------------------------------------------------------ */
/* min/max are exact, associative and commutative, and IEEE min/max     */
/* pick the non-NaN operand regardless of association, so a balanced    */
/* tree yields bitwise the same result as the scalar reference's       */
/* DO_OP__N shape.  The pairing halves the dependency depth at every   */
/* n_srcs (~log2 vs n_srcs-1 for the left fold), exposing ILP at      */
/* n_srcs >= 4; the n_srcs > 8 tail left-folds s8..s(n_srcs-1) into   */
/* the tree result (matches the scalar DO_OP__8(...) OP s8 OP ...).   */
#define UCC_RED_AVX2_DEF_BTREE(CTYPE, DT, UTAG, OP, UOP)                    \
    static inline __attribute__((target("avx2"))) void                    \
    ucc_arch_reduce_avx2_##DT##_##OP(void *dst,                           \
                                     const void * const *srcs,            \
                                     size_t count, unsigned n_srcs)       \
    {                                                                      \
        const CTYPE **restrict s = (const CTYPE **)srcs;                 \
        CTYPE *restrict d = (CTYPE *)dst;                                \
        const CTYPE *restrict p0 = s[0];                     \
        const CTYPE *restrict p1 = s[1];                     \
        const CTYPE *restrict p2 = s[2];                     \
        const CTYPE *restrict p3 = s[3];                     \
        const CTYPE *restrict p4 = s[4];                     \
        const CTYPE *restrict p5 = s[5];                     \
        const CTYPE *restrict p6 = s[6];                     \
        const CTYPE *restrict p7 = s[7];                     \
        const unsigned lanes = UCC_RED_AVX2_##UTAG##_LANES;               \
        size_t i = 0;                                                      \
        if (n_srcs <= 8) {                                                 \
            switch (n_srcs) {                                              \
            case 1:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i],                     \
                                                UCC_RED_AVX2_##UTAG##_LOAD(&p0[i])); \
                }                                                          \
                break;                                                     \
            case 2:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i],                     \
                                                UCC_RED_AVX2_##UTAG##_##UOP(v0, v1)); \
                }                                                          \
                break;                                                     \
            case 3:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i],                     \
                        UCC_RED_AVX2_##UTAG##_##UOP(                      \
                            UCC_RED_AVX2_##UTAG##_##UOP(v0, v1), v2));    \
                }                                                          \
                break;                                                     \
            case 4:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v3 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i],                     \
                        UCC_RED_AVX2_##UTAG##_##UOP(                      \
                            UCC_RED_AVX2_##UTAG##_##UOP(v0, v1),          \
                            UCC_RED_AVX2_##UTAG##_##UOP(v2, v3)));        \
                }                                                          \
                break;                                                     \
            case 5:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v3 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v4 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p4[i]);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i],                     \
                        UCC_RED_AVX2_##UTAG##_##UOP(                      \
                            UCC_RED_AVX2_##UTAG##_##UOP(v0, v1),          \
                            UCC_RED_AVX2_##UTAG##_##UOP(                  \
                                UCC_RED_AVX2_##UTAG##_##UOP(v2, v3),      \
                                v4)));                                     \
                }                                                          \
                break;                                                     \
            case 6:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v3 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v4 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p4[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v5 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p5[i]);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i],                     \
                        UCC_RED_AVX2_##UTAG##_##UOP(                      \
                            UCC_RED_AVX2_##UTAG##_##UOP(                  \
                                UCC_RED_AVX2_##UTAG##_##UOP(v0, v1),      \
                                UCC_RED_AVX2_##UTAG##_##UOP(v2, v3)),     \
                            UCC_RED_AVX2_##UTAG##_##UOP(v4, v5)));        \
                }                                                          \
                break;                                                     \
            case 7:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v3 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v4 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p4[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v5 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p5[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v6 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p6[i]);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i],                     \
                        UCC_RED_AVX2_##UTAG##_##UOP(                      \
                            UCC_RED_AVX2_##UTAG##_##UOP(                  \
                                UCC_RED_AVX2_##UTAG##_##UOP(v0, v1),      \
                                UCC_RED_AVX2_##UTAG##_##UOP(v2, v3)),     \
                            UCC_RED_AVX2_##UTAG##_##UOP(                  \
                                UCC_RED_AVX2_##UTAG##_##UOP(v4, v5),      \
                                v6)));                                     \
                }                                                          \
                break;                                                     \
            case 8:                                                        \
                for (i = 0; i + lanes <= count; i += lanes) {             \
                    UCC_RED_AVX2_##UTAG##_VEC v0 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v1 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v2 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v3 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v4 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p4[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v5 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p5[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v6 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p6[i]);             \
                    UCC_RED_AVX2_##UTAG##_VEC v7 =                         \
                        UCC_RED_AVX2_##UTAG##_LOAD(&p7[i]);             \
                    UCC_RED_AVX2_##UTAG##_STORE(&d[i],                     \
                        UCC_RED_AVX2_##UTAG##_##UOP(                      \
                            UCC_RED_AVX2_##UTAG##_##UOP(                  \
                                UCC_RED_AVX2_##UTAG##_##UOP(v0, v1),      \
                                UCC_RED_AVX2_##UTAG##_##UOP(v2, v3)),     \
                            UCC_RED_AVX2_##UTAG##_##UOP(                  \
                                UCC_RED_AVX2_##UTAG##_##UOP(v4, v5),      \
                                UCC_RED_AVX2_##UTAG##_##UOP(v6, v7))));   \
                }                                                          \
                break;                                                     \
            }                                                              \
        } else {                                                           \
            for (i = 0; i + lanes <= count; i += lanes) {                 \
                UCC_RED_AVX2_##UTAG##_VEC v0 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p0[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v1 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p1[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v2 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p2[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v3 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p3[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v4 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p4[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v5 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p5[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v6 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p6[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC v7 =                             \
                    UCC_RED_AVX2_##UTAG##_LOAD(&p7[i]);                 \
                UCC_RED_AVX2_##UTAG##_VEC acc =                            \
                    UCC_RED_AVX2_##UTAG##_##UOP(                          \
                        UCC_RED_AVX2_##UTAG##_##UOP(                      \
                            UCC_RED_AVX2_##UTAG##_##UOP(v0, v1),          \
                            UCC_RED_AVX2_##UTAG##_##UOP(v2, v3)),         \
                        UCC_RED_AVX2_##UTAG##_##UOP(                      \
                            UCC_RED_AVX2_##UTAG##_##UOP(v4, v5),          \
                            UCC_RED_AVX2_##UTAG##_##UOP(v6, v7)));        \
                unsigned j;                                                \
                for (j = 8; j < n_srcs; j++) {                            \
                    acc = UCC_RED_AVX2_##UTAG##_##UOP(                    \
                        acc, UCC_RED_AVX2_##UTAG##_LOAD(&s[j][i]));       \
                }                                                          \
                UCC_RED_AVX2_##UTAG##_STORE(&d[i], acc);                    \
            }                                                              \
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

/* dispatcher */
#define UCC_RED_AVX2_DEF_REDUCE(CTYPE, DT, UTAG, OP, UOP, STRTG)            \
    UCC_RED_AVX2_DEF_##STRTG(CTYPE, DT, UTAG, OP, UOP)

/* ------------------------------------------------------------------ */
/* Instantiations: per (dtype, op)                                     */
/* ------------------------------------------------------------------ */
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, sum, SUM, TREE)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, prod, PROD, TREE)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, min, MIN, BTREE)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, max, MAX, BTREE)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, band, BAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, bor, BOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, bxor, BXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, land, LAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, lor, LOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int8_t, int8, INT8, lxor, LXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, sum, SUM, TREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, prod, PROD, TREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, min, MIN, BTREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, max, MAX, BTREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, band, BAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, bor, BOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, bxor, BXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, land, LAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, lor, LOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int16_t, int16, INT16, lxor, LXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, sum, SUM, TREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, prod, PROD, TREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, min, MIN, BTREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, max, MAX, BTREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, band, BAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, bor, BOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, bxor, BXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, land, LAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, lor, LOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int32_t, int32, INT32, lxor, LXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, sum, SUM, TREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, prod, PROD, TREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, min, MIN, BTREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, max, MAX, BTREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, band, BAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, bor, BOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, bxor, BXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, land, LAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, lor, LOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(int64_t, int64, INT64, lxor, LXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, sum, SUM, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, prod, PROD, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, min, MIN, BTREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, max, MAX, BTREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, band, BAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, bor, BOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, bxor, BXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, land, LAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, lor, LOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint8_t, uint8, UINT8, lxor, LXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, sum, SUM, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, prod, PROD, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, min, MIN, BTREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, max, MAX, BTREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, band, BAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, bor, BOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, bxor, BXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, land, LAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, lor, LOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint16_t, uint16, UINT16, lxor, LXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, sum, SUM, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, prod, PROD, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, min, MIN, BTREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, max, MAX, BTREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, band, BAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, bor, BOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, bxor, BXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, land, LAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, lor, LOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint32_t, uint32, UINT32, lxor, LXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, sum, SUM, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, prod, PROD, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, min, MIN, BTREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, max, MAX, BTREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, band, BAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, bor, BOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, bxor, BXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, land, LAND, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, lor, LOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(uint64_t, uint64, UINT64, lxor, LXOR, TREE)
UCC_RED_AVX2_DEF_REDUCE(float, float32, FLOAT32, sum, SUM, TREE)
UCC_RED_AVX2_DEF_REDUCE(float, float32, FLOAT32, prod, PROD, TREE)
UCC_RED_AVX2_DEF_REDUCE(float, float32, FLOAT32, min, MIN, BTREE)
UCC_RED_AVX2_DEF_REDUCE(float, float32, FLOAT32, max, MAX, BTREE)
UCC_RED_AVX2_DEF_REDUCE(double, float64, FLOAT64, sum, SUM, TREE)
UCC_RED_AVX2_DEF_REDUCE(double, float64, FLOAT64, prod, PROD, TREE)
UCC_RED_AVX2_DEF_REDUCE(double, float64, FLOAT64, min, MIN, BTREE)
UCC_RED_AVX2_DEF_REDUCE(double, float64, FLOAT64, max, MAX, BTREE)

#endif /* defined(__x86_64__) */
#endif /* UCC_ARCH_X86_64_REDUCE_AVX2_H_ */
