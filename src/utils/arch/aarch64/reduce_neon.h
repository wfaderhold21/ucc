/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_ARCH_AARCH64_REDUCE_NEON_H_
#define UCC_ARCH_AARCH64_REDUCE_NEON_H_

#if defined(__aarch64__)

#include <arm_neon.h>
#include <stddef.h>
#include <stdint.h>

/*
 * NEON SIMD reduce kernels for the CPU (ec_cpu) reduce path.
 *
 * AArch64-only mirror of src/utils/arch/x86_64/reduce_avx2.h.
 * Each kernel is a pure reduce over srcs[0..n_srcs-1] into dst:
 *
 *   dst[i] = srcs[0][i] OP srcs[1][i] OP ... OP srcs[n_srcs-1][i]
 *
 * The reduction is a sequential per-lane left-fold in source order for
 * sum/prod/band/bor/bxor/land/lor/lxor, matching the scalar reference
 * (src/components/ec/cpu/ec_cpu_reduce.c) bitwise-identically for
 * integers and IEEE-identically for floats.
 *
 * min/max/lxor use the exact DO_OP__N tree structure from the scalar
 * DO_OP_MIN/MAX/LXOR_N macros, preserving the tree reduction shape that
 * matters for float NaN behaviour (tree != left-fold).
 *
 * alpha (AVG / REDUCE_WITH_ALPHA) is NOT applied here; the dispatcher
 * applies it after the call, mirroring the scalar path.
 *
 * Lane counts (128-bit NEON): int8/uint8=16, int16/uint16=8,
 * int32/uint32/float32=4, int64/uint64/float64=2.
 *
 * Verification note: no aarch64 runtime is available in this harness;
 * the header is verified by cross-compilation only (clang
 * -target aarch64-linux-gnu -ffreestanding -O2 -Wall -c).
 */

/* NEON is the baseline on aarch64 */
static inline int ucc_arch_neon_supported(void)
{
    return 1;
}

/* ------------------------------------------------------------------ */
/* Shared multi-step helpers                                          */
/* ------------------------------------------------------------------ */

/*
 * Truncating folds.  NEON's narrowing ops shift (vmovn_s16 = >> 1), they
 * do NOT take the low byte.  Shift the value into the upper byte (exact
 * for values < 2^8 / < 2^16) and narrow-shift it back down:
 *   (x << n) >> n  ==  x,  truncated to the operand width.
 */
/* int8/uint8 product: widen each byte to 16-bit, mullo, keep low byte,
 * fold back to 8-bit.  Operates on two int8x16_t (16 lanes each).
 */
static inline int8x16_t ucc_arch_neon_mul_8bit(int8x16_t a, int8x16_t b)
{
    int16x8_t al = vmovl_s8(vget_low_s8(a));
    int16x8_t ah = vmovl_s8(vget_high_s8(a));
    int16x8_t bl = vmovl_s8(vget_low_s8(b));
    int16x8_t bh = vmovl_s8(vget_high_s8(b));

    /* low byte of the 16-bit products, folded back to 8-bit */
    uint16x8_t pl = vreinterpretq_u16_s16(
        vandq_s16(vmulq_s16(al, bl), vdupq_n_s16(0xFF)));
    uint16x8_t ph = vreinterpretq_u16_s16(
        vandq_s16(vmulq_s16(ah, bh), vdupq_n_s16(0xFF)));
    uint8x8_t pl8 = vshrn_n_u16(vshlq_n_u16(pl, 8), 8);
    uint8x8_t ph8 = vshrn_n_u16(vshlq_n_u16(ph, 8), 8);

    return vcombine_s8(vreinterpret_s8_u8(ph8), vreinterpret_s8_u8(pl8));
}

/* Wrapping (mod 2^n) 8-bit add: widen to 16-bit (exact), add, fold. */
static inline int8x16_t ucc_arch_neon_add_epi8_wrap(int8x16_t a, int8x16_t b)
{
    int16x8_t t_lo = vaddq_s16(vmovl_s8(vget_low_s8(a)),
                                vmovl_s8(vget_low_s8(b)));
    int16x8_t t_hi = vaddq_s16(vmovl_s8(vget_high_s8(a)),
                                vmovl_s8(vget_high_s8(b)));
    uint16x8_t m_lo = vreinterpretq_u16_s16(vandq_s16(t_lo, vdupq_n_s16(0xFF)));
    uint16x8_t m_hi = vreinterpretq_u16_s16(vandq_s16(t_hi, vdupq_n_s16(0xFF)));
    uint8x8_t p_lo = vshrn_n_u16(vshlq_n_u16(m_lo, 8), 8);
    uint8x8_t p_hi = vshrn_n_u16(vshlq_n_u16(m_hi, 8), 8);
    return vcombine_s8(vreinterpret_s8_u8(p_hi), vreinterpret_s8_u8(p_lo));
}

/* Wrapping (mod 2^n) 16-bit add: widen to 32-bit (exact), add, fold. */
static inline int16x8_t ucc_arch_neon_add_epi16_wrap(int16x8_t a, int16x8_t b)
{
    int32x4_t t_lo = vaddq_s32(vmovl_s16(vget_low_s16(a)),
                                vmovl_s16(vget_low_s16(b)));
    int32x4_t t_hi = vaddq_s32(vmovl_s16(vget_high_s16(a)),
                                vmovl_s16(vget_high_s16(b)));
    uint32x4_t m_lo = vreinterpretq_u32_s32(vandq_s32(t_lo, vdupq_n_s32(0xFFFF)));
    uint32x4_t m_hi = vreinterpretq_u32_s32(vandq_s32(t_hi, vdupq_n_s32(0xFFFF)));
    uint16x4_t p_lo = vshrn_n_u32(vshlq_n_u32(m_lo, 16), 16);
    uint16x4_t p_hi = vshrn_n_u32(vshlq_n_u32(m_hi, 16), 16);
    return vcombine_s16(vreinterpret_s16_u16(p_hi), vreinterpret_s16_u16(p_lo));
}

/* int64/uint64 product: 32-bit cross-term decomposition, low 64 bits
 * exact.  NEON lacks vmulq_s64, so we split each int64x2_t into two
 * int32x2_t halves, recombine to int32x4_t, multiply, widen back.
 *
 * a = a_hi << 32 | a_lo,  b = b_hi << 32 | b_lo
 * a*b = lo_lo + ((lo_hi + hi_lo) << 32)
 * where lo_lo = a_lo * b_lo (32*32->64), lo_hi = a_lo * b_hi,
 * hi_lo = a_hi * b_lo.
 */
static inline int64x2_t ucc_arch_neon_mul_64bit(int64x2_t a, int64x2_t b)
{
    /* Split into low/high 32-bit halves, rebuild as int32x4_t */
    int32x4_t a32 = vreinterpretq_s32_s64(a);
    int32x4_t b32 = vreinterpretq_s32_s64(b);
    int32x2_t a_lo = vget_low_s32(a32);
    int32x2_t a_hi = vget_high_s32(a32);
    int32x2_t b_lo = vget_low_s32(b32);
    int32x2_t b_hi = vget_high_s32(b32);

    /* 32x32->64 cross-lane multiply: vmull_s32(a[0],b[0]) -> int64x2_t */
    int64x2_t lo_lo = vmull_s32(a_lo, b_lo);
    int64x2_t lo_hi = vmull_s32(a_lo, b_hi);
    int64x2_t hi_lo = vmull_s32(a_hi, b_lo);

    /* Cross term = (lo_hi + hi_lo) << 32 */
    int64x2_t cross = vshlq_n_s64(vaddq_s64(lo_hi, hi_lo), 32);

    return vaddq_s64(cross, lo_lo);
}

/* signed int64 min: vcgtq_s64(a,b) mask + vbslq_s64 */
static inline int64x2_t ucc_arch_neon_min_64bit(int64x2_t a, int64x2_t b)
{
    int64x2_t mask = vcgtq_s64(a, b); /* a > b  -> all-ones where true */
    return vbslq_s64(mask, b, a);     /* a>b -> b, else a */
}

/* signed int64 max */
static inline int64x2_t ucc_arch_neon_max_64bit(int64x2_t a, int64x2_t b)
{
    int64x2_t mask = vcgtq_s64(a, b);
    return vbslq_s64(mask, a, b);
}

/* unsigned int64 min/max: flip sign bit, then signed compare */
static inline int64x2_t ucc_arch_neon_min_64bit_u(int64x2_t a, int64x2_t b)
{
    int64x2_t flip = vdupq_n_s64((int64_t)0x8000000000000000ULL);
    int64x2_t mask = vcgtq_s64(veorq_s64(a, flip), veorq_s64(b, flip));
    return vbslq_s64(mask, b, a);
}

static inline int64x2_t ucc_arch_neon_max_64bit_u(int64x2_t a, int64x2_t b)
{
    int64x2_t flip = vdupq_n_s64((int64_t)0x8000000000000000ULL);
    int64x2_t mask = vcgtq_s64(veorq_s64(a, flip), veorq_s64(b, flip));
    return vbslq_s64(mask, a, b);
}

/* per-lane truthiness: all-ones iff nonzero.
 * NEON: vceqq(a, 0) -> 0 where non-zero; invert with veorq(-1).
 * Then AND with dup(1) to produce 0 or 1 per lane.
 */
#define UCC_RED_NEON_TRUTHY_8BIT(v)                                            \
    veorq_s8(vceqq_s8((v), vdupq_n_s8(0)), vdupq_n_s8(-1))
#define UCC_RED_NEON_TRUTHY_16BIT(v)                                           \
    veorq_s16(vceqq_s16((v), vdupq_n_s16(0)), vdupq_n_s16(-1))
#define UCC_RED_NEON_TRUTHY_32BIT(v)                                           \
    veorq_s32(vceqq_s32((v), vdupq_n_s32(0)), vdupq_n_s32(-1))
#define UCC_RED_NEON_TRUTHY_64BIT(v)                                           \
    veorq_s64(vceqq_s64((v), vdupq_n_s64(0)), vdupq_n_s64(-1))

/* ------------------------------------------------------------------ */
/* Per-dtype vector ops (acc, v) -> acc OP v                            */
/* ------------------------------------------------------------------ */

/* INT8 */
#define UCC_RED_NEON_INT8_VEC    int8x16_t
#define UCC_RED_NEON_INT8_CTYPE  int8_t
#define UCC_RED_NEON_INT8_LANES  16
#define UCC_RED_NEON_INT8_LOAD(p)        vld1q_s8((const int8_t *)(p))
#define UCC_RED_NEON_INT8_STORE(p, v)    vst1q_s8((int8_t *)(p), (v))
#define UCC_RED_NEON_INT8_SUM(a, v)      ucc_arch_neon_add_epi8_wrap((a), (v))
#define UCC_RED_NEON_INT8_PROD(a, v)     ucc_arch_neon_mul_8bit((a), (v))
#define UCC_RED_NEON_INT8_MIN(a, v)      vminq_s8((a), (v))
#define UCC_RED_NEON_INT8_MAX(a, v)      vmaxq_s8((a), (v))
#define UCC_RED_NEON_INT8_BAND(a, v)     vandq_s8((a), (v))
#define UCC_RED_NEON_INT8_BOR(a, v)      vorrq_s8((a), (v))
#define UCC_RED_NEON_INT8_BXOR(a, v)     veorq_s8((a), (v))
#define UCC_RED_NEON_INT8_LAND(a, v)                                     \
    vandq_s8(vandq_s8(UCC_RED_NEON_TRUTHY_8BIT(a),                        \
                      UCC_RED_NEON_TRUTHY_8BIT(v)),                       \
             vdupq_n_s8(1))
#define UCC_RED_NEON_INT8_LOR(a, v)                                        \
    vandq_s8(vorrq_s8(UCC_RED_NEON_TRUTHY_8BIT(a),                        \
                      UCC_RED_NEON_TRUTHY_8BIT(v)),                       \
             vdupq_n_s8(1))
#define UCC_RED_NEON_INT8_LXOR(a, v)                                       \
    veorq_s8(vandq_s8(UCC_RED_NEON_TRUTHY_8BIT(a),                        \
                      vdupq_n_s8(1)),                                     \
             vandq_s8(UCC_RED_NEON_TRUTHY_8BIT(v),                        \
                      vdupq_n_s8(1)))

/* UINT8 */
#define UCC_RED_NEON_UINT8_VEC    uint8x16_t
#define UCC_RED_NEON_UINT8_CTYPE  uint8_t
#define UCC_RED_NEON_UINT8_LANES  16
#define UCC_RED_NEON_UINT8_LOAD(p)          vld1q_u8((const uint8_t *)(p))
#define UCC_RED_NEON_UINT8_STORE(p, v)      vst1q_u8((uint8_t *)(p), (v))
#define UCC_RED_NEON_UINT8_SUM(a, v)        ucc_arch_neon_add_epi8_wrap((a), (v))
#define UCC_RED_NEON_UINT8_PROD(a, v)                                       \
    vreinterpretq_u8_s8(ucc_arch_neon_mul_8bit(                            \
        vreinterpretq_s8_u8((a)), vreinterpretq_s8_u8((v))))
#define UCC_RED_NEON_UINT8_MIN(a, v)        vminq_u8((a), (v))
#define UCC_RED_NEON_UINT8_MAX(a, v)        vmaxq_u8((a), (v))
#define UCC_RED_NEON_UINT8_BAND(a, v)       vandq_u8((a), (v))
#define UCC_RED_NEON_UINT8_BOR(a, v)        vorrq_u8((a), (v))
#define UCC_RED_NEON_UINT8_BXOR(a, v)       veorq_u8((a), (v))
#define UCC_RED_NEON_UINT8_LAND(a, v)       UCC_RED_NEON_INT8_LAND(a, v)
#define UCC_RED_NEON_UINT8_LOR(a, v)        UCC_RED_NEON_INT8_LOR(a, v)
#define UCC_RED_NEON_UINT8_LXOR(a, v)       UCC_RED_NEON_INT8_LXOR(a, v)

/* INT16 */
#define UCC_RED_NEON_INT16_VEC    int16x8_t
#define UCC_RED_NEON_INT16_CTYPE  int16_t
#define UCC_RED_NEON_INT16_LANES  8
#define UCC_RED_NEON_INT16_LOAD(p)        vld1q_s16((const int16_t *)(p))
#define UCC_RED_NEON_INT16_STORE(p, v)    vst1q_s16((int16_t *)(p), (v))
#define UCC_RED_NEON_INT16_SUM(a, v)      ucc_arch_neon_add_epi16_wrap((a), (v))
#define UCC_RED_NEON_INT16_PROD(a, v)     vmulq_s16((a), (v))
#define UCC_RED_NEON_INT16_MIN(a, v)      vminq_s16((a), (v))
#define UCC_RED_NEON_INT16_MAX(a, v)      vmaxq_s16((a), (v))
#define UCC_RED_NEON_INT16_BAND(a, v)     vandq_s16((a), (v))
#define UCC_RED_NEON_INT16_BOR(a, v)      vorrq_s16((a), (v))
#define UCC_RED_NEON_INT16_BXOR(a, v)     veorq_s16((a), (v))
#define UCC_RED_NEON_INT16_LAND(a, v)                                      \
    vandq_s16(vandq_s16(UCC_RED_NEON_TRUTHY_16BIT(a),                       \
                        UCC_RED_NEON_TRUTHY_16BIT(v)),                      \
              vdupq_n_s16(1))
#define UCC_RED_NEON_INT16_LOR(a, v)                                         \
    vandq_s16(vorrq_s16(UCC_RED_NEON_TRUTHY_16BIT(a),                       \
                        UCC_RED_NEON_TRUTHY_16BIT(v)),                      \
              vdupq_n_s16(1))
#define UCC_RED_NEON_INT16_LXOR(a, v)                                        \
    veorq_s16(vandq_s16(UCC_RED_NEON_TRUTHY_16BIT(a),                       \
                        vdupq_n_s16(1)),                                     \
              vandq_s16(UCC_RED_NEON_TRUTHY_16BIT(v),                       \
                        vdupq_n_s16(1)))

/* UINT16 */
#define UCC_RED_NEON_UINT16_VEC    uint16x8_t
#define UCC_RED_NEON_UINT16_CTYPE  uint16_t
#define UCC_RED_NEON_UINT16_LANES  8
#define UCC_RED_NEON_UINT16_LOAD(p)         vld1q_u16((const uint16_t *)(p))
#define UCC_RED_NEON_UINT16_STORE(p, v)     vst1q_u16((uint16_t *)(p), (v))
#define UCC_RED_NEON_UINT16_SUM(a, v)       ucc_arch_neon_add_epi16_wrap((a), (v))
#define UCC_RED_NEON_UINT16_PROD(a, v)      vmulq_u16((a), (v))
#define UCC_RED_NEON_UINT16_MIN(a, v)       vminq_u16((a), (v))
#define UCC_RED_NEON_UINT16_MAX(a, v)       vmaxq_u16((a), (v))
#define UCC_RED_NEON_UINT16_BAND(a, v)      vandq_u16((a), (v))
#define UCC_RED_NEON_UINT16_BOR(a, v)       vorrq_u16((a), (v))
#define UCC_RED_NEON_UINT16_BXOR(a, v)      veorq_u16((a), (v))
#define UCC_RED_NEON_UINT16_LAND(a, v)      UCC_RED_NEON_INT16_LAND(a, v)
#define UCC_RED_NEON_UINT16_LOR(a, v)       UCC_RED_NEON_INT16_LOR(a, v)
#define UCC_RED_NEON_UINT16_LXOR(a, v)      UCC_RED_NEON_INT16_LXOR(a, v)

/* INT32 */
#define UCC_RED_NEON_INT32_VEC    int32x4_t
#define UCC_RED_NEON_INT32_CTYPE  int32_t
#define UCC_RED_NEON_INT32_LANES  4
#define UCC_RED_NEON_INT32_LOAD(p)        vld1q_s32((const int32_t *)(p))
#define UCC_RED_NEON_INT32_STORE(p, v)    vst1q_s32((int32_t *)(p), (v))
#define UCC_RED_NEON_INT32_SUM(a, v)      vaddq_s32((a), (v))
#define UCC_RED_NEON_INT32_PROD(a, v)     vmulq_s32((a), (v))
#define UCC_RED_NEON_INT32_MIN(a, v)      vminq_s32((a), (v))
#define UCC_RED_NEON_INT32_MAX(a, v)      vmaxq_s32((a), (v))
#define UCC_RED_NEON_INT32_BAND(a, v)     vandq_s32((a), (v))
#define UCC_RED_NEON_INT32_BOR(a, v)      vorrq_s32((a), (v))
#define UCC_RED_NEON_INT32_BXOR(a, v)     veorq_s32((a), (v))
#define UCC_RED_NEON_INT32_LAND(a, v)                                      \
    vandq_s32(vandq_s32(UCC_RED_NEON_TRUTHY_32BIT(a),                       \
                        UCC_RED_NEON_TRUTHY_32BIT(v)),                      \
              vdupq_n_s32(1))
#define UCC_RED_NEON_INT32_LOR(a, v)                                         \
    vandq_s32(vorrq_s32(UCC_RED_NEON_TRUTHY_32BIT(a),                       \
                        UCC_RED_NEON_TRUTHY_32BIT(v)),                      \
              vdupq_n_s32(1))
#define UCC_RED_NEON_INT32_LXOR(a, v)                                        \
    veorq_s32(vandq_s32(UCC_RED_NEON_TRUTHY_32BIT(a),                       \
                        vdupq_n_s32(1)),                                     \
              vandq_s32(UCC_RED_NEON_TRUTHY_32BIT(v),                       \
                        vdupq_n_s32(1)))

/* UINT32 */
#define UCC_RED_NEON_UINT32_VEC    uint32x4_t
#define UCC_RED_NEON_UINT32_CTYPE  uint32_t
#define UCC_RED_NEON_UINT32_LANES  4
#define UCC_RED_NEON_UINT32_LOAD(p)         vld1q_u32((const uint32_t *)(p))
#define UCC_RED_NEON_UINT32_STORE(p, v)     vst1q_u32((uint32_t *)(p), (v))
#define UCC_RED_NEON_UINT32_SUM(a, v)       vaddq_u32((a), (v))
#define UCC_RED_NEON_UINT32_PROD(a, v)      vmulq_u32((a), (v))
#define UCC_RED_NEON_UINT32_MIN(a, v)       vminq_u32((a), (v))
#define UCC_RED_NEON_UINT32_MAX(a, v)       vmaxq_u32((a), (v))
#define UCC_RED_NEON_UINT32_BAND(a, v)      vandq_u32((a), (v))
#define UCC_RED_NEON_UINT32_BOR(a, v)       vorrq_u32((a), (v))
#define UCC_RED_NEON_UINT32_BXOR(a, v)      veorq_u32((a), (v))
#define UCC_RED_NEON_UINT32_LAND(a, v)      UCC_RED_NEON_INT32_LAND(a, v)
#define UCC_RED_NEON_UINT32_LOR(a, v)       UCC_RED_NEON_INT32_LOR(a, v)
#define UCC_RED_NEON_UINT32_LXOR(a, v)      UCC_RED_NEON_INT32_LXOR(a, v)

/* INT64 */
#define UCC_RED_NEON_INT64_VEC    int64x2_t
#define UCC_RED_NEON_INT64_CTYPE  int64_t
#define UCC_RED_NEON_INT64_LANES  2
#define UCC_RED_NEON_INT64_LOAD(p)        vld1q_s64((const int64_t *)(p))
#define UCC_RED_NEON_INT64_STORE(p, v)    vst1q_s64((int64_t *)(p), (v))
#define UCC_RED_NEON_INT64_SUM(a, v)      vaddq_s64((a), (v))
#define UCC_RED_NEON_INT64_PROD(a, v)     ucc_arch_neon_mul_64bit((a), (v))
#define UCC_RED_NEON_INT64_MIN(a, v)      ucc_arch_neon_min_64bit((a), (v))
#define UCC_RED_NEON_INT64_MAX(a, v)      ucc_arch_neon_max_64bit((a), (v))
#define UCC_RED_NEON_INT64_BAND(a, v)     vandq_s64((a), (v))
#define UCC_RED_NEON_INT64_BOR(a, v)      vorrq_s64((a), (v))
#define UCC_RED_NEON_INT64_BXOR(a, v)     veorq_s64((a), (v))
#define UCC_RED_NEON_INT64_LAND(a, v)                                      \
    vandq_s64(vandq_s64(UCC_RED_NEON_TRUTHY_64BIT(a),                       \
                        UCC_RED_NEON_TRUTHY_64BIT(v)),                      \
              vdupq_n_s64(1))
#define UCC_RED_NEON_INT64_LOR(a, v)                                         \
    vandq_s64(vorrq_s64(UCC_RED_NEON_TRUTHY_64BIT(a),                       \
                        UCC_RED_NEON_TRUTHY_64BIT(v)),                      \
              vdupq_n_s64(1))
#define UCC_RED_NEON_INT64_LXOR(a, v)                                        \
    veorq_s64(vandq_s64(UCC_RED_NEON_TRUTHY_64BIT(a),                       \
                        vdupq_n_s64(1)),                                     \
              vandq_s64(UCC_RED_NEON_TRUTHY_64BIT(v),                       \
                        vdupq_n_s64(1)))

/* UINT64 */
#define UCC_RED_NEON_UINT64_VEC    uint64x2_t
#define UCC_RED_NEON_UINT64_CTYPE  uint64_t
#define UCC_RED_NEON_UINT64_LANES  2
#define UCC_RED_NEON_UINT64_LOAD(p)         vld1q_u64((const uint64_t *)(p))
#define UCC_RED_NEON_UINT64_STORE(p, v)     vst1q_u64((uint64_t *)(p), (v))
#define UCC_RED_NEON_UINT64_SUM(a, v)       vaddq_u64((a), (v))
#define UCC_RED_NEON_UINT64_PROD(a, v)      ucc_arch_neon_mul_64bit(         \
        vreinterpretq_s64_u64((a)), vreinterpretq_s64_u64((v)))
#define UCC_RED_NEON_UINT64_MIN(a, v)       ucc_arch_neon_min_64bit_u(       \
        vreinterpretq_s64_u64((a)), vreinterpretq_s64_u64((v)))
#define UCC_RED_NEON_UINT64_MAX(a, v)       ucc_arch_neon_max_64bit_u(       \
        vreinterpretq_s64_u64((a)), vreinterpretq_s64_u64((v)))
#define UCC_RED_NEON_UINT64_BAND(a, v)      vandq_u64((a), (v))
#define UCC_RED_NEON_UINT64_BOR(a, v)       vorrq_u64((a), (v))
#define UCC_RED_NEON_UINT64_BXOR(a, v)      veorq_u64((a), (v))
#define UCC_RED_NEON_UINT64_LAND(a, v)      UCC_RED_NEON_INT64_LAND(a, v)
#define UCC_RED_NEON_UINT64_LOR(a, v)       UCC_RED_NEON_INT64_LOR(a, v)
#define UCC_RED_NEON_UINT64_LXOR(a, v)      UCC_RED_NEON_INT64_LXOR(a, v)

/* FLOAT32 */
#define UCC_RED_NEON_FLOAT32_VEC    float32x4_t
#define UCC_RED_NEON_FLOAT32_CTYPE  float
#define UCC_RED_NEON_FLOAT32_LANES  4
#define UCC_RED_NEON_FLOAT32_LOAD(p)        vld1q_f32((const float *)(p))
#define UCC_RED_NEON_FLOAT32_STORE(p, v)    vst1q_f32((float *)(p), (v))
#define UCC_RED_NEON_FLOAT32_SUM(a, v)      vaddq_f32((a), (v))
#define UCC_RED_NEON_FLOAT32_PROD(a, v)     vmulq_f32((a), (v))
#define UCC_RED_NEON_FLOAT32_MIN(a, v)      vminq_f32((a), (v))
#define UCC_RED_NEON_FLOAT32_MAX(a, v)      vmaxq_f32((a), (v))

/* FLOAT64 */
#define UCC_RED_NEON_FLOAT64_VEC    float64x2_t
#define UCC_RED_NEON_FLOAT64_CTYPE  double
#define UCC_RED_NEON_FLOAT64_LANES  2
#define UCC_RED_NEON_FLOAT64_LOAD(p)        vld1q_f64((const double *)(p))
#define UCC_RED_NEON_FLOAT64_STORE(p, v)    vst1q_f64((double *)(p), (v))
#define UCC_RED_NEON_FLOAT64_SUM(a, v)      vaddq_f64((a), (v))
#define UCC_RED_NEON_FLOAT64_PROD(a, v)     vmulq_f64((a), (v))
#define UCC_RED_NEON_FLOAT64_MIN(a, v)      vminq_f64((a), (v))
#define UCC_RED_NEON_FLOAT64_MAX(a, v)      vmaxq_f64((a), (v))

/* ------------------------------------------------------------------ */
/* Scalar tail ops (acc, val) -> acc OP val, exact scalar semantics   */
/* ------------------------------------------------------------------ */
/* Shared for signed/unsigned; min/max/truthiness do not care about sign */
#define UCC_RED_NEON_S_SUM(acc, val)      ((acc) + (val))
#define UCC_RED_NEON_S_PROD(acc, val)     ((acc) * (val))
#define UCC_RED_NEON_S_MIN(acc, val)      ((acc) < (val) ? (acc) : (val))
#define UCC_RED_NEON_S_MAX(acc, val)      ((acc) > (val) ? (acc) : (val))
#define UCC_RED_NEON_S_BAND(acc, val)     ((acc) & (val))
#define UCC_RED_NEON_S_BOR(acc, val)      ((acc) | (val))
#define UCC_RED_NEON_S_BXOR(acc, val)     ((acc) ^ (val))
#define UCC_RED_NEON_S_LAND(acc, val)     ((acc) && (val))
#define UCC_RED_NEON_S_LOR(acc, val)      ((acc) || (val))
#define UCC_RED_NEON_S_LXOR(acc, val)     ((!(acc)) != (!(val)))

/* ------------------------------------------------------------------ */
/* FOLD kernel generator (sum/prod/band/bor/bxor/land/lor/lxor)       */
/* ------------------------------------------------------------------ */
#define UCC_RED_NEON_DEF_FOLD(CTYPE, DT, UTAG, OP, UOP)                   \
    static inline void                                                   \
    ucc_arch_reduce_neon_##DT##_##OP(void *dst,                         \
                                     const void * const *srcs,         \
                                     size_t count, unsigned n_srcs)     \
    {                                                                    \
        const CTYPE **restrict s = (const CTYPE **)srcs;               \
        CTYPE *restrict d = (CTYPE *)dst;                              \
        const unsigned lanes = UCC_RED_NEON_##UTAG##_LANES;            \
        size_t i;                                                        \
        for (i = 0; i + lanes <= count; i += lanes) {                  \
            UCC_RED_NEON_##UTAG##_VEC acc =                               \
                UCC_RED_NEON_##UTAG##_LOAD(&s[0][i]);                  \
            unsigned j;                                                  \
            for (j = 1; j < n_srcs; j++) {                               \
                UCC_RED_NEON_##UTAG##_VEC v =                             \
                    UCC_RED_NEON_##UTAG##_LOAD(&s[j][i]);              \
                acc = UCC_RED_NEON_##UTAG##_##UOP(acc, v);               \
            }                                                            \
            UCC_RED_NEON_##UTAG##_STORE(&d[i], acc);                   \
        }                                                                \
        for (; i < count; i++) {                                         \
            CTYPE acc = s[0][i];                                         \
            unsigned j;                                                  \
            for (j = 1; j < n_srcs; j++) {                               \
                acc = UCC_RED_NEON_S_##UOP(acc, s[j][i]);                \
            }                                                            \
            d[i] = acc;                                                  \
        }                                                                \
    }

/* ------------------------------------------------------------------ */
/* TREE reduction helpers: exact DO_OP__N shape (min/max/lxor)        */
/* ------------------------------------------------------------------ */
#define UCC_RED_NEON_TREE2(UTAG, UOP, a, b)        UCC_RED_NEON_##UTAG##_##UOP(a, b)
#define UCC_RED_NEON_TREE3(UTAG, UOP, a, b, c)     UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(a, b), c)
#define UCC_RED_NEON_TREE4(UTAG, UOP, a, b, c, d) \
    UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(a, b), \
                                UCC_RED_NEON_##UTAG##_##UOP(c, d))
#define UCC_RED_NEON_TREE5(UTAG, UOP, a, b, c, d, e) \
    UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(a, b), \
                                UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(c, d), e))
#define UCC_RED_NEON_TREE6(UTAG, UOP, a, b, c, d, e, f) \
    UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(a, b), \
                                                            UCC_RED_NEON_##UTAG##_##UOP(c, d)), \
                                UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(d, e), f))
#define UCC_RED_NEON_TREE7(UTAG, UOP, a, b, c, d, e, f, g) \
    UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(a, b), \
                                                            UCC_RED_NEON_##UTAG##_##UOP(c, d)), \
                                UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(d, e), \
                                                            UCC_RED_NEON_##UTAG##_##UOP(f, g)))
#define UCC_RED_NEON_TREE8(UTAG, UOP, a, b, c, d, e, f, g, h) \
    UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(a, b), \
                                                            UCC_RED_NEON_##UTAG##_##UOP(c, d)), \
                                UCC_RED_NEON_##UTAG##_##UOP(UCC_RED_NEON_##UTAG##_##UOP(e, f), \
                                                            UCC_RED_NEON_##UTAG##_##UOP(g, h)))

/* scalar tree helpers */
#define UCC_RED_NEON_STREE2(UOP, a, b)     UCC_RED_NEON_S_##UOP(a, b)
#define UCC_RED_NEON_STREE3(UOP, a, b, c)     UCC_RED_NEON_S_##UOP(UCC_RED_NEON_S_##UOP(a, b), c)
#define UCC_RED_NEON_STREE4(UOP, a, b, c, d)     UCC_RED_NEON_S_##UOP(UCC_RED_NEON_S_##UOP(a, b), \
                         UCC_RED_NEON_S_##UOP(c, d))
#define UCC_RED_NEON_STREE5(UOP, a, b, c, d, e)     UCC_RED_NEON_S_##UOP(UCC_RED_NEON_S_##UOP(a, b), \
                         UCC_RED_NEON_STREE3(UOP, c, d, e))
#define UCC_RED_NEON_STREE6(UOP, a, b, c, d, e, f)     UCC_RED_NEON_S_##UOP(UCC_RED_NEON_STREE3(UOP, a, b, c), \
                         UCC_RED_NEON_STREE3(UOP, d, e, f))
#define UCC_RED_NEON_STREE7(UOP, a, b, c, d, e, f, g)     UCC_RED_NEON_S_##UOP(UCC_RED_NEON_STREE3(UOP, a, b, c), \
                         UCC_RED_NEON_STREE4(UOP, d, e, f, g))
#define UCC_RED_NEON_STREE8(UOP, a, b, c, d, e, f, g, h)     UCC_RED_NEON_S_##UOP(UCC_RED_NEON_STREE4(UOP, a, b, c, d), \
                         UCC_RED_NEON_STREE4(UOP, e, f, g, h))

/* TREE kernel: exact DO_OP__N tree, then fold from source 8           */
#define UCC_RED_NEON_DEF_TREE(CTYPE, DT, UTAG, OP, UOP)                   \
    static inline void                                                   \
    ucc_arch_reduce_neon_##DT##_##OP(void *dst,                         \
                                     const void * const *srcs,         \
                                     size_t count, unsigned n_srcs)     \
    {                                                                    \
        const CTYPE **restrict s = (const CTYPE **)srcs;               \
        CTYPE *restrict d = (CTYPE *)dst;                              \
        const unsigned lanes = UCC_RED_NEON_##UTAG##_LANES;            \
        size_t i;                                                        \
        for (i = 0; i + lanes <= count; i += lanes) {                  \
            unsigned k = n_srcs < 8 ? n_srcs : 8;                      \
            UCC_RED_NEON_##UTAG##_VEC v[8];                            \
            unsigned j;                                                  \
            for (j = 0; j < k; j++) v[j] =                             \
                UCC_RED_NEON_##UTAG##_LOAD(&s[j][i]);                  \
            UCC_RED_NEON_##UTAG##_VEC acc = v[0];                        \
            switch (k) {                                                 \
            case 2: acc = UCC_RED_NEON_TREE2(UTAG, UOP, v[0], v[1]); \
                    break;                                                 \
            case 3: acc = UCC_RED_NEON_TREE3(UTAG, UOP, v[0], v[1], \
                                             v[2]); break;                 \
            case 4: acc = UCC_RED_NEON_TREE4(UTAG, UOP, v[0], v[1], \
                                             v[2], v[3]); break;           \
            case 5: acc = UCC_RED_NEON_TREE5(UTAG, UOP, v[0], v[1], \
                                             v[2], v[3], v[4]); break;     \
            case 6: acc = UCC_RED_NEON_TREE6(UTAG, UOP, v[0], v[1], \
                                             v[2], v[3], v[4], v[5]);      \
                    break;                                                 \
            case 7: acc = UCC_RED_NEON_TREE7(UTAG, UOP, v[0], v[1], \
                                             v[2], v[3], v[4], v[5],      \
                                             v[6]); break;                 \
            case 8: acc = UCC_RED_NEON_TREE8(UTAG, UOP, v[0], v[1], \
                                             v[2], v[3], v[4], v[5],      \
                                             v[6], v[7]); break;           \
            }                                                              \
            for (j = k; j < n_srcs; j++) {                               \
                acc = UCC_RED_NEON_##UTAG##_##UOP(                       \
                    acc, UCC_RED_NEON_##UTAG##_LOAD(&s[j][i]));          \
            }                                                              \
            UCC_RED_NEON_##UTAG##_STORE(&d[i], acc);                   \
        }                                                                \
        for (; i < count; i++) {                                         \
            unsigned k = n_srcs < 8 ? n_srcs : 8;                      \
            CTYPE v[8];                                                    \
            unsigned j;                                                  \
            for (j = 0; j < k; j++) v[j] = s[j][i];                     \
            CTYPE acc = v[0];                                         \
            switch (k) {                                                   \
            case 2: acc = UCC_RED_NEON_STREE2(UOP, v[0], v[1]); break; \
            case 3: acc = UCC_RED_NEON_STREE3(UOP, v[0], v[1], v[2]); \
                    break;                                                   \
            case 4: acc = UCC_RED_NEON_STREE4(UOP, v[0], v[1], v[2], \
                                              v[3]); break;                \
            case 5: acc = UCC_RED_NEON_STREE5(UOP, v[0], v[1], v[2], \
                                              v[3], v[4]); break;          \
            case 6: acc = UCC_RED_NEON_STREE6(UOP, v[0], v[1], v[2], \
                                              v[3], v[4], v[5]); break;    \
            case 7: acc = UCC_RED_NEON_STREE7(UOP, v[0], v[1], v[2], \
                                              v[3], v[4], v[5], v[6]); \
                    break;                                                   \
            case 8: acc = UCC_RED_NEON_STREE8(UOP, v[0], v[1], v[2], \
                                              v[3], v[4], v[5], v[6],      \
                                              v[7]); break;                \
            }                                                              \
            for (j = k; j < n_srcs; j++) {                               \
                acc = UCC_RED_NEON_S_##UOP(acc, s[j][i]);                \
            }                                                              \
            d[i] = acc;                                                    \
        }                                                                \
    }

/* dispatcher */
#define UCC_RED_NEON_DEF_REDUCE(CTYPE, DT, UTAG, OP, UOP, STRTG)            \
    UCC_RED_NEON_DEF_##STRTG(CTYPE, DT, UTAG, OP, UOP)

/* ------------------------------------------------------------------ */
/* Instantiations: per (dtype, op)                                     */
/* ------------------------------------------------------------------ */

/* INT8 */
UCC_RED_NEON_DEF_REDUCE(int8_t, int8, INT8, sum, SUM, FOLD)
UCC_RED_NEON_DEF_REDUCE(int8_t, int8, INT8, prod, PROD, FOLD)
UCC_RED_NEON_DEF_REDUCE(int8_t, int8, INT8, min, MIN, TREE)
UCC_RED_NEON_DEF_REDUCE(int8_t, int8, INT8, max, MAX, TREE)
UCC_RED_NEON_DEF_REDUCE(int8_t, int8, INT8, band, BAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(int8_t, int8, INT8, bor, BOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int8_t, int8, INT8, bxor, BXOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int8_t, int8, INT8, land, LAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(int8_t, int8, INT8, lor, LOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int8_t, int8, INT8, lxor, LXOR, FOLD)
/* INT16 */
UCC_RED_NEON_DEF_REDUCE(int16_t, int16, INT16, sum, SUM, FOLD)
UCC_RED_NEON_DEF_REDUCE(int16_t, int16, INT16, prod, PROD, FOLD)
UCC_RED_NEON_DEF_REDUCE(int16_t, int16, INT16, min, MIN, TREE)
UCC_RED_NEON_DEF_REDUCE(int16_t, int16, INT16, max, MAX, TREE)
UCC_RED_NEON_DEF_REDUCE(int16_t, int16, INT16, band, BAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(int16_t, int16, INT16, bor, BOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int16_t, int16, INT16, bxor, BXOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int16_t, int16, INT16, land, LAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(int16_t, int16, INT16, lor, LOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int16_t, int16, INT16, lxor, LXOR, FOLD)
/* INT32 */
UCC_RED_NEON_DEF_REDUCE(int32_t, int32, INT32, sum, SUM, FOLD)
UCC_RED_NEON_DEF_REDUCE(int32_t, int32, INT32, prod, PROD, FOLD)
UCC_RED_NEON_DEF_REDUCE(int32_t, int32, INT32, min, MIN, TREE)
UCC_RED_NEON_DEF_REDUCE(int32_t, int32, INT32, max, MAX, TREE)
UCC_RED_NEON_DEF_REDUCE(int32_t, int32, INT32, band, BAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(int32_t, int32, INT32, bor, BOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int32_t, int32, INT32, bxor, BXOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int32_t, int32, INT32, land, LAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(int32_t, int32, INT32, lor, LOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int32_t, int32, INT32, lxor, LXOR, FOLD)
/* INT64 */
UCC_RED_NEON_DEF_REDUCE(int64_t, int64, INT64, sum, SUM, FOLD)
UCC_RED_NEON_DEF_REDUCE(int64_t, int64, INT64, prod, PROD, FOLD)
UCC_RED_NEON_DEF_REDUCE(int64_t, int64, INT64, min, MIN, TREE)
UCC_RED_NEON_DEF_REDUCE(int64_t, int64, INT64, max, MAX, TREE)
UCC_RED_NEON_DEF_REDUCE(int64_t, int64, INT64, band, BAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(int64_t, int64, INT64, bor, BOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int64_t, int64, INT64, bxor, BXOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int64_t, int64, INT64, land, LAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(int64_t, int64, INT64, lor, LOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(int64_t, int64, INT64, lxor, LXOR, FOLD)
/* UINT8 */
UCC_RED_NEON_DEF_REDUCE(uint8_t, uint8, UINT8, sum, SUM, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint8_t, uint8, UINT8, prod, PROD, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint8_t, uint8, UINT8, min, MIN, TREE)
UCC_RED_NEON_DEF_REDUCE(uint8_t, uint8, UINT8, max, MAX, TREE)
UCC_RED_NEON_DEF_REDUCE(uint8_t, uint8, UINT8, band, BAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint8_t, uint8, UINT8, bor, BOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint8_t, uint8, UINT8, bxor, BXOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint8_t, uint8, UINT8, land, LAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint8_t, uint8, UINT8, lor, LOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint8_t, uint8, UINT8, lxor, LXOR, FOLD)
/* UINT16 */
UCC_RED_NEON_DEF_REDUCE(uint16_t, uint16, UINT16, sum, SUM, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint16_t, uint16, UINT16, prod, PROD, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint16_t, uint16, UINT16, min, MIN, TREE)
UCC_RED_NEON_DEF_REDUCE(uint16_t, uint16, UINT16, max, MAX, TREE)
UCC_RED_NEON_DEF_REDUCE(uint16_t, uint16, UINT16, band, BAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint16_t, uint16, UINT16, bor, BOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint16_t, uint16, UINT16, bxor, BXOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint16_t, uint16, UINT16, land, LAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint16_t, uint16, UINT16, lor, LOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint16_t, uint16, UINT16, lxor, LXOR, FOLD)
/* UINT32 */
UCC_RED_NEON_DEF_REDUCE(uint32_t, uint32, UINT32, sum, SUM, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint32_t, uint32, UINT32, prod, PROD, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint32_t, uint32, UINT32, min, MIN, TREE)
UCC_RED_NEON_DEF_REDUCE(uint32_t, uint32, UINT32, max, MAX, TREE)
UCC_RED_NEON_DEF_REDUCE(uint32_t, uint32, UINT32, band, BAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint32_t, uint32, UINT32, bor, BOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint32_t, uint32, UINT32, bxor, BXOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint32_t, uint32, UINT32, land, LAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint32_t, uint32, UINT32, lor, LOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint32_t, uint32, UINT32, lxor, LXOR, FOLD)
/* UINT64 */
UCC_RED_NEON_DEF_REDUCE(uint64_t, uint64, UINT64, sum, SUM, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint64_t, uint64, UINT64, prod, PROD, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint64_t, uint64, UINT64, min, MIN, TREE)
UCC_RED_NEON_DEF_REDUCE(uint64_t, uint64, UINT64, max, MAX, TREE)
UCC_RED_NEON_DEF_REDUCE(uint64_t, uint64, UINT64, band, BAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint64_t, uint64, UINT64, bor, BOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint64_t, uint64, UINT64, bxor, BXOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint64_t, uint64, UINT64, land, LAND, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint64_t, uint64, UINT64, lor, LOR, FOLD)
UCC_RED_NEON_DEF_REDUCE(uint64_t, uint64, UINT64, lxor, LXOR, FOLD)
/* FLOAT32 */
UCC_RED_NEON_DEF_REDUCE(float, float32, FLOAT32, sum, SUM, FOLD)
UCC_RED_NEON_DEF_REDUCE(float, float32, FLOAT32, prod, PROD, FOLD)
UCC_RED_NEON_DEF_REDUCE(float, float32, FLOAT32, min, MIN, TREE)
UCC_RED_NEON_DEF_REDUCE(float, float32, FLOAT32, max, MAX, TREE)
/* FLOAT64 */
UCC_RED_NEON_DEF_REDUCE(double, float64, FLOAT64, sum, SUM, FOLD)
UCC_RED_NEON_DEF_REDUCE(double, float64, FLOAT64, prod, PROD, FOLD)
UCC_RED_NEON_DEF_REDUCE(double, float64, FLOAT64, min, MIN, TREE)
UCC_RED_NEON_DEF_REDUCE(double, float64, FLOAT64, max, MAX, TREE)

#endif /* defined(__aarch64__) */
#endif /* UCC_ARCH_AARCH64_REDUCE_NEON_H_ */
