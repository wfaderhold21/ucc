/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_ARCH_REDUCE_SIMD_H_
#define UCC_ARCH_REDUCE_SIMD_H_

#include <stddef.h>
#include <stdint.h>
#include "utils/ucc_math_op.h"

/* minimum element count for SIMD dispatch (one full vector) */
#define UCC_ARCH_REDUCE_THRESH 32

#if defined(__x86_64__)
#  include "x86_64/reduce_avx2.h"
#  ifdef __GNUC__
#    define UCC_ARCH_REDUCE_SIMD_SUPPORTED() ucc_arch_avx2_supported()
#  else
#    define UCC_ARCH_REDUCE_SIMD_SUPPORTED() 0
#  endif
#elif defined(__aarch64__)
#  include "aarch64/reduce_neon.h"
#  define UCC_ARCH_REDUCE_SIMD_SUPPORTED() 1
#endif

#if defined(__x86_64__)
static inline int ucc_arch_reduce_supported_avx2(ucc_datatype_t dt, ucc_reduction_op_t op)
{
    switch (dt) {
    case UCC_DT_INT8:
    case UCC_DT_INT16:
    case UCC_DT_INT32:
    case UCC_DT_INT64:
    case UCC_DT_UINT8:
    case UCC_DT_UINT16:
    case UCC_DT_UINT32:
    case UCC_DT_UINT64:
        switch (op) {
        case UCC_OP_SUM:
        case UCC_OP_PROD:
        case UCC_OP_MIN:
        case UCC_OP_MAX:
        case UCC_OP_BAND:
        case UCC_OP_BOR:
        case UCC_OP_BXOR:
        case UCC_OP_LAND:
        case UCC_OP_LOR:
        case UCC_OP_LXOR:
            return 1;
        default: return 0;
        }
    case UCC_DT_FLOAT32:
    case UCC_DT_FLOAT64:
        switch (op) {
        case UCC_OP_SUM:
        case UCC_OP_PROD:
        case UCC_OP_MIN:
        case UCC_OP_MAX:
            return 1;
        default: return 0;
        }
    default: return 0;
    }
}

static inline void ucc_arch_reduce_avx2(void *dst, const void * const *srcs,
                                size_t count, unsigned n_srcs,
                                ucc_datatype_t dt, ucc_reduction_op_t op)
{
    switch (dt) {
    case UCC_DT_INT8:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_avx2_int8_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_avx2_int8_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_avx2_int8_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_avx2_int8_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_avx2_int8_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_avx2_int8_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_avx2_int8_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_avx2_int8_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_avx2_int8_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_avx2_int8_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_INT16:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_avx2_int16_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_avx2_int16_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_avx2_int16_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_avx2_int16_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_avx2_int16_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_avx2_int16_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_avx2_int16_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_avx2_int16_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_avx2_int16_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_avx2_int16_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_INT32:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_avx2_int32_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_avx2_int32_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_avx2_int32_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_avx2_int32_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_avx2_int32_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_avx2_int32_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_avx2_int32_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_avx2_int32_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_avx2_int32_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_avx2_int32_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_INT64:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_avx2_int64_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_avx2_int64_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_avx2_int64_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_avx2_int64_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_avx2_int64_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_avx2_int64_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_avx2_int64_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_avx2_int64_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_avx2_int64_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_avx2_int64_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_UINT8:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_avx2_uint8_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_avx2_uint8_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_avx2_uint8_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_avx2_uint8_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_avx2_uint8_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_avx2_uint8_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_avx2_uint8_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_avx2_uint8_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_avx2_uint8_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_avx2_uint8_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_UINT16:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_avx2_uint16_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_avx2_uint16_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_avx2_uint16_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_avx2_uint16_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_avx2_uint16_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_avx2_uint16_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_avx2_uint16_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_avx2_uint16_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_avx2_uint16_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_avx2_uint16_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_UINT32:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_avx2_uint32_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_avx2_uint32_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_avx2_uint32_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_avx2_uint32_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_avx2_uint32_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_avx2_uint32_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_avx2_uint32_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_avx2_uint32_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_avx2_uint32_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_avx2_uint32_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_UINT64:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_avx2_uint64_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_avx2_uint64_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_avx2_uint64_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_avx2_uint64_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_avx2_uint64_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_avx2_uint64_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_avx2_uint64_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_avx2_uint64_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_avx2_uint64_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_avx2_uint64_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_FLOAT32:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_avx2_float32_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_avx2_float32_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_avx2_float32_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_avx2_float32_max(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_FLOAT64:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_avx2_float64_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_avx2_float64_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_avx2_float64_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_avx2_float64_max(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    default: break;
    }
}
#elif defined(__aarch64__)
static inline int ucc_arch_reduce_supported_neon(ucc_datatype_t dt, ucc_reduction_op_t op)
{
    switch (dt) {
    case UCC_DT_INT8:
    case UCC_DT_INT16:
    case UCC_DT_INT32:
    case UCC_DT_INT64:
    case UCC_DT_UINT8:
    case UCC_DT_UINT16:
    case UCC_DT_UINT32:
    case UCC_DT_UINT64:
        switch (op) {
        case UCC_OP_SUM:
        case UCC_OP_PROD:
        case UCC_OP_MIN:
        case UCC_OP_MAX:
        case UCC_OP_BAND:
        case UCC_OP_BOR:
        case UCC_OP_BXOR:
        case UCC_OP_LAND:
        case UCC_OP_LOR:
        case UCC_OP_LXOR:
            return 1;
        default: return 0;
        }
    case UCC_DT_FLOAT32:
    case UCC_DT_FLOAT64:
        switch (op) {
        case UCC_OP_SUM:
        case UCC_OP_PROD:
        case UCC_OP_MIN:
        case UCC_OP_MAX:
            return 1;
        default: return 0;
        }
    default: return 0;
    }
}

static inline void ucc_arch_reduce_neon(void *dst, const void * const *srcs,
                                size_t count, unsigned n_srcs,
                                ucc_datatype_t dt, ucc_reduction_op_t op)
{
    switch (dt) {
    case UCC_DT_INT8:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_neon_int8_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_neon_int8_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_neon_int8_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_neon_int8_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_neon_int8_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_neon_int8_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_neon_int8_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_neon_int8_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_neon_int8_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_neon_int8_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_INT16:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_neon_int16_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_neon_int16_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_neon_int16_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_neon_int16_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_neon_int16_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_neon_int16_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_neon_int16_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_neon_int16_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_neon_int16_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_neon_int16_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_INT32:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_neon_int32_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_neon_int32_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_neon_int32_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_neon_int32_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_neon_int32_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_neon_int32_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_neon_int32_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_neon_int32_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_neon_int32_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_neon_int32_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_INT64:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_neon_int64_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_neon_int64_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_neon_int64_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_neon_int64_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_neon_int64_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_neon_int64_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_neon_int64_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_neon_int64_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_neon_int64_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_neon_int64_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_UINT8:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_neon_uint8_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_neon_uint8_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_neon_uint8_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_neon_uint8_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_neon_uint8_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_neon_uint8_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_neon_uint8_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_neon_uint8_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_neon_uint8_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_neon_uint8_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_UINT16:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_neon_uint16_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_neon_uint16_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_neon_uint16_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_neon_uint16_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_neon_uint16_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_neon_uint16_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_neon_uint16_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_neon_uint16_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_neon_uint16_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_neon_uint16_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_UINT32:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_neon_uint32_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_neon_uint32_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_neon_uint32_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_neon_uint32_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_neon_uint32_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_neon_uint32_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_neon_uint32_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_neon_uint32_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_neon_uint32_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_neon_uint32_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_UINT64:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_neon_uint64_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_neon_uint64_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_neon_uint64_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_neon_uint64_max(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BAND:
            ucc_arch_reduce_neon_uint64_band(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BOR:
            ucc_arch_reduce_neon_uint64_bor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_BXOR:
            ucc_arch_reduce_neon_uint64_bxor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LAND:
            ucc_arch_reduce_neon_uint64_land(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LOR:
            ucc_arch_reduce_neon_uint64_lor(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_LXOR:
            ucc_arch_reduce_neon_uint64_lxor(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_FLOAT32:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_neon_float32_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_neon_float32_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_neon_float32_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_neon_float32_max(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    case UCC_DT_FLOAT64:
        switch (op) {
        case UCC_OP_SUM:
            ucc_arch_reduce_neon_float64_sum(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_PROD:
            ucc_arch_reduce_neon_float64_prod(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MIN:
            ucc_arch_reduce_neon_float64_min(dst, srcs, count, n_srcs);
            break;
        case UCC_OP_MAX:
            ucc_arch_reduce_neon_float64_max(dst, srcs, count, n_srcs);
            break;
        default: break;
        }
        break;
    default: break;
    }
}
#endif

#if defined(__x86_64__) || defined(__aarch64__)
static inline int ucc_arch_reduce_supported(ucc_datatype_t dt,
                                           ucc_reduction_op_t op)
{
#if defined(__x86_64__)
    if (!UCC_ARCH_REDUCE_SIMD_SUPPORTED()) {
        return 0;
    }
    return ucc_arch_reduce_supported_avx2(dt, op);
#else
    return ucc_arch_reduce_supported_neon(dt, op);
#endif
}

static inline void ucc_arch_reduce(void *dst, const void * const *srcs,
                                   size_t count, unsigned n_srcs,
                                   ucc_datatype_t dt, ucc_reduction_op_t op)
{
#if defined(__x86_64__)
    ucc_arch_reduce_avx2(dst, srcs, count, n_srcs, dt, op);
#else
    ucc_arch_reduce_neon(dst, srcs, count, n_srcs, dt, op);
#endif
}
#endif

#endif /* UCC_ARCH_REDUCE_SIMD_H_ */
