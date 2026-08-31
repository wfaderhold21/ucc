/**
 * Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "utils/arch/reduce_simd.h"
#include "utils/ucc_math_op.h"
#include "ec_cpu.h"
#include <complex.h>

#define DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, OP)                  \
    do {                                                                       \
        size_t _i, _j;                                                         \
        type  _tmp;                                                            \
        size_t __count = _count;                                               \
        switch (_n_srcs) {                                                     \
        case 1:                                                                \
            for (_i = 0; _i < __count; _i++) {                                 \
                d[_i] = s[0][_i];                                              \
            }                                                                  \
            break;                                                             \
        case 2:                                                                \
            for (_i = 0; _i < __count; _i++) {                                 \
                d[_i] = OP##_2(s[0][_i], s[1][_i]);                            \
            }                                                                  \
            break;                                                             \
        case 3:                                                                \
            for (_i = 0; _i < __count; _i++) {                                 \
                d[_i] = OP##_3(s[0][_i], s[1][_i], s[2][_i]);                  \
            }                                                                  \
            break;                                                             \
        case 4:                                                                \
            for (_i = 0; _i < __count; _i++) {                                 \
                d[_i] = OP##_4(s[0][_i], s[1][_i], s[2][_i], s[3][_i]);        \
            }                                                                  \
            break;                                                             \
        case 5:                                                                \
            for (_i = 0; _i < __count; _i++) {                                 \
                d[_i] =                                                        \
                    OP##_5(s[0][_i], s[1][_i], s[2][_i], s[3][_i], s[4][_i]);  \
            }                                                                  \
            break;                                                             \
        case 6:                                                                \
            for (_i = 0; _i < __count; _i++) {                                 \
                d[_i] = OP##_6(s[0][_i], s[1][_i], s[2][_i], s[3][_i],         \
                               s[4][_i], s[5][_i]);                            \
            }                                                                  \
            break;                                                             \
        case 7:                                                                \
            for (_i = 0; _i < __count; _i++) {                                 \
                d[_i] = OP##_7(s[0][_i], s[1][_i], s[2][_i], s[3][_i],         \
                               s[4][_i], s[5][_i], s[6][_i]);                  \
            }                                                                  \
            break;                                                             \
        case 8:                                                                \
            for (_i = 0; _i < __count; _i++) {                                 \
                d[_i] = OP##_8(s[0][_i], s[1][_i], s[2][_i], s[3][_i],         \
                               s[4][_i], s[5][_i], s[6][_i], s[7][_i]);        \
            }                                                                  \
            break;                                                             \
        default:                                                               \
            for (_i = 0; _i < __count; _i++) {                                 \
                _tmp = OP##_8(s[0][_i], s[1][_i], s[2][_i], s[3][_i],          \
                              s[4][_i], s[5][_i], s[6][_i], s[7][_i]);         \
                for (_j = 8; _j < _n_srcs; _j++) {                             \
                    _tmp = OP##_2(_tmp, s[_j][_i]);                            \
                }                                                              \
                d[_i] = _tmp;                                                  \
            }                                                                  \
            break;                                                             \
        }                                                                      \
    } while (0)

#define VEC_OP(_d, _count, _alpha)                                             \
    do {                                                                       \
        size_t _i;                                                             \
        for (_i = 0; _i < _count; _i++) {                                      \
            _d[_i] = _d[_i] * _alpha;                                          \
        }                                                                      \
    } while (0)

#define DO_DT_REDUCE_INT(type, _srcs, _dst, _op, _count, _n_srcs)              \
    do {                                                                       \
        const type **restrict s = (const type **)_srcs;                        \
        type *restrict        d = (type * ) _dst;                              \
        switch (_op) {                                                         \
        case UCC_OP_AVG:                                                       \
        case UCC_OP_SUM:                                                       \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_SUM);      \
            if (flags & UCC_EEE_TASK_FLAG_REDUCE_WITH_ALPHA) {                 \
                VEC_OP(d, _count, task->alpha);                                \
            }                                                                  \
            break;                                                             \
        case UCC_OP_MIN:                                                       \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_MIN);      \
            break;                                                             \
        case UCC_OP_MAX:                                                       \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_MAX);      \
            break;                                                             \
        case UCC_OP_PROD:                                                      \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_PROD);     \
            break;                                                             \
        case UCC_OP_LAND:                                                      \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_LAND);     \
            break;                                                             \
        case UCC_OP_BAND:                                                      \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_BAND);     \
            break;                                                             \
        case UCC_OP_LOR:                                                       \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_LOR);      \
            break;                                                             \
        case UCC_OP_BOR:                                                       \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_BOR);      \
            break;                                                             \
        case UCC_OP_LXOR:                                                      \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_LXOR);     \
            break;                                                             \
        case UCC_OP_BXOR:                                                      \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_BXOR);     \
            break;                                                             \
        default:                                                               \
            ec_error(&ucc_ec_cpu.super,                                        \
                     "int dtype does not support "                             \
                     "requested reduce op: %s",                                \
                     ucc_reduction_op_str(_op));                               \
            return UCC_ERR_NOT_SUPPORTED;                                      \
        }                                                                      \
    } while (0)

#define DO_DT_REDUCE_WITH_OP_BFLOAT16(_srcs, _dst, _count, _n_srcs, _OP,       \
                                      _alpha)                                  \
    do {                                                                       \
        float     _tmp;                                                        \
        size_t    _i, _j;                                                      \
        int16_t **_s = (int16_t **)_srcs;                                      \
        int16_t * _d = (int16_t *)_dst;                                        \
        if (_n_srcs == 1) {                                                    \
            for (_i = 0; _i < _count; _i++) {                                  \
                _tmp = bfloat16tofloat32(&_s[0][_i]);                          \
                float32tobfloat16(_tmp * _alpha, &_d[_i]);                     \
            }                                                                  \
        } else {                                                               \
            for (_i = 0; _i < _count; _i++) {                                  \
                _tmp = _OP(bfloat16tofloat32(&_s[0][_i]),                      \
                           bfloat16tofloat32(&_s[1][_i]));                     \
                for (_j = 2; _j < _n_srcs; _j++) {                             \
                    _tmp = _OP(_tmp, bfloat16tofloat32(&_s[_j][_i]));         \
                }                                                              \
                float32tobfloat16(_tmp * _alpha, &_d[_i]);                     \
            }                                                                  \
        }                                                                      \
    } while (0)

#define DO_DT_REDUCE_BFLOAT16(_srcs, _dst, _op, _count, _n_srcs)               \
    do {                                                                       \
        float _a = (flags & UCC_EEE_TASK_FLAG_REDUCE_WITH_ALPHA) ? task->alpha \
                                                                 : 1.0f;       \
        switch (_op) {                                                         \
        case UCC_OP_AVG:                                                       \
        case UCC_OP_SUM:                                                       \
            DO_DT_REDUCE_WITH_OP_BFLOAT16(_srcs, _dst, _count, _n_srcs,        \
                                          DO_OP_SUM, _a);                      \
            break;                                                             \
        case UCC_OP_PROD:                                                      \
            DO_DT_REDUCE_WITH_OP_BFLOAT16(_srcs, _dst, _count, _n_srcs,        \
                                          DO_OP_PROD, _a);                     \
            break;                                                             \
        case UCC_OP_MIN:                                                       \
            DO_DT_REDUCE_WITH_OP_BFLOAT16(_srcs, _dst, _count, _n_srcs,        \
                                          DO_OP_MIN, _a);                      \
            break;                                                             \
        case UCC_OP_MAX:                                                       \
            DO_DT_REDUCE_WITH_OP_BFLOAT16(_srcs, _dst, _count, _n_srcs,        \
                                          DO_OP_MAX, _a);                      \
            break;                                                             \
        default:                                                               \
            ec_error(&ucc_ec_cpu.super,                                        \
                     "bfloat16 dtype does not support "                        \
                     "requested reduce op: %s",                                \
                     ucc_reduction_op_str(_op));                               \
            return UCC_ERR_NOT_SUPPORTED;                                      \
        }                                                                      \
    } while (0)

#define DO_DT_REDUCE_FLOAT(type, _srcs, _dst, _op, _count, _n_srcs)            \
    do {                                                                       \
        const type **restrict s = (const type **)_srcs;                        \
        type *restrict        d = (type *) _dst;                               \
        switch (_op) {                                                         \
        case UCC_OP_AVG:                                                       \
        case UCC_OP_SUM:                                                       \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_SUM);      \
            break;                                                             \
        case UCC_OP_PROD:                                                      \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_PROD);     \
            break;                                                             \
        case UCC_OP_MIN:                                                       \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_MIN);      \
            break;                                                             \
        case UCC_OP_MAX:                                                       \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_MAX);      \
            break;                                                             \
        default:                                                               \
            ec_error(&ucc_ec_cpu.super,                                        \
                     "float dtype does not support "                           \
                     "requested reduce op: %s",                                \
                     ucc_reduction_op_str(_op));                               \
            return UCC_ERR_NOT_SUPPORTED;                                      \
        }                                                                      \
        if (flags & UCC_EEE_TASK_FLAG_REDUCE_WITH_ALPHA) {                     \
            VEC_OP(d, _count, task->alpha);                                    \
        }                                                                      \
    } while (0)

#define DO_DT_REDUCE_FLOAT_COMPLEX(type, _srcs, _dst, _op, _count, _n_srcs)    \
    do {                                                                       \
        const type **restrict s = (const type **)_srcs;                        \
        type *restrict        d = (type *) _dst;                               \
        switch (_op) {                                                         \
        case UCC_OP_AVG:                                                       \
        case UCC_OP_SUM:                                                       \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_SUM);      \
            break;                                                             \
        case UCC_OP_PROD:                                                      \
            DO_DT_REDUCE_WITH_OP(type, s, d, _count, _n_srcs, DO_OP_PROD);     \
            break;                                                             \
        default:                                                               \
            ec_error(&ucc_ec_cpu.super,                                        \
                     "float complex dtype does not support "                   \
                     "requested reduce op: %s",                                \
                     ucc_reduction_op_str(_op));                               \
            return UCC_ERR_NOT_SUPPORTED;                                      \
        }                                                                      \
        if (flags & UCC_EEE_TASK_FLAG_REDUCE_WITH_ALPHA) {                     \
            VEC_OP(d, _count, task->alpha);          \
        }                                                                      \
    } while (0)

ucc_status_t ucc_ec_cpu_reduce(ucc_eee_task_reduce_t *task, void * restrict dst,
                               void * const * restrict srcs, uint16_t flags)
{
    /* SIMD fast path: large counts on a supported (dt, op).  Kernels are a
     * pure reduce; alpha is applied below exactly as the scalar macros do.
     * Scalar macros remain the reference and handle unsupported ops. */
#if defined(__x86_64__) || defined(__aarch64__)
    if ((task->count >= UCC_ARCH_REDUCE_THRESH) &&
        ucc_arch_reduce_supported(task->dt, task->op)) {
        ucc_arch_reduce(dst, (const void * const *)srcs, task->count,
                        task->n_srcs, task->dt, task->op);
        if (flags & UCC_EEE_TASK_FLAG_REDUCE_WITH_ALPHA) {
            switch (task->dt) {
            case UCC_DT_INT8:
                if ((task->op == UCC_OP_SUM) || (task->op == UCC_OP_AVG)) {
                    VEC_OP(((int8_t *)dst), task->count, task->alpha);
                }
                break;
            case UCC_DT_INT16:
                if ((task->op == UCC_OP_SUM) || (task->op == UCC_OP_AVG)) {
                    VEC_OP(((int16_t *)dst), task->count, task->alpha);
                }
                break;
            case UCC_DT_INT32:
                if ((task->op == UCC_OP_SUM) || (task->op == UCC_OP_AVG)) {
                    VEC_OP(((int32_t *)dst), task->count, task->alpha);
                }
                break;
            case UCC_DT_INT64:
                if ((task->op == UCC_OP_SUM) || (task->op == UCC_OP_AVG)) {
                    VEC_OP(((int64_t *)dst), task->count, task->alpha);
                }
                break;
            case UCC_DT_UINT8:
                if ((task->op == UCC_OP_SUM) || (task->op == UCC_OP_AVG)) {
                    VEC_OP(((uint8_t *)dst), task->count, task->alpha);
                }
                break;
            case UCC_DT_UINT16:
                if ((task->op == UCC_OP_SUM) || (task->op == UCC_OP_AVG)) {
                    VEC_OP(((uint16_t *)dst), task->count, task->alpha);
                }
                break;
            case UCC_DT_UINT32:
                if ((task->op == UCC_OP_SUM) || (task->op == UCC_OP_AVG)) {
                    VEC_OP(((uint32_t *)dst), task->count, task->alpha);
                }
                break;
            case UCC_DT_UINT64:
                if ((task->op == UCC_OP_SUM) || (task->op == UCC_OP_AVG)) {
                    VEC_OP(((uint64_t *)dst), task->count, task->alpha);
                }
                break;
            case UCC_DT_FLOAT32:
                /* float scalar applies alpha after every op */
                VEC_OP(((float *)dst), task->count, task->alpha);
                break;
            case UCC_DT_FLOAT64:
                VEC_OP(((double *)dst), task->count, task->alpha);
                break;
            default:
                break;
            }
        }
        return UCC_OK;
    }
#endif

    switch (task->dt) {
    case UCC_DT_INT8:
        DO_DT_REDUCE_INT(int8_t, srcs, dst, task->op, task->count,
                         task->n_srcs);
        break;
    case UCC_DT_INT16:
        DO_DT_REDUCE_INT(int16_t, srcs, dst, task->op, task->count,
                         task->n_srcs);
        break;
    case UCC_DT_INT32:
        DO_DT_REDUCE_INT(int32_t, srcs, dst, task->op, task->count,
                         task->n_srcs);
        break;
    case UCC_DT_INT64:
        DO_DT_REDUCE_INT(int64_t, srcs, dst, task->op, task->count,
                         task->n_srcs);
        break;
    case UCC_DT_UINT8:
        DO_DT_REDUCE_INT(uint8_t, srcs, dst, task->op, task->count,
                         task->n_srcs);
        break;
    case UCC_DT_UINT16:
        DO_DT_REDUCE_INT(uint16_t, srcs, dst, task->op, task->count,
                         task->n_srcs);
        break;
    case UCC_DT_UINT32:
        DO_DT_REDUCE_INT(uint32_t, srcs, dst, task->op, task->count,
                         task->n_srcs);
        break;
    case UCC_DT_UINT64:
        DO_DT_REDUCE_INT(uint64_t, srcs, dst, task->op, task->count,
                         task->n_srcs);
        break;
    case UCC_DT_FLOAT32:
#if SIZEOF_FLOAT == 4
        DO_DT_REDUCE_FLOAT(float, srcs, dst, task->op, task->count,
                           task->n_srcs);
        break;
#else
        return UCC_ERR_NOT_SUPPORTED;
#endif
    case UCC_DT_FLOAT64:
#if SIZEOF_DOUBLE == 8
        DO_DT_REDUCE_FLOAT(double, srcs, dst, task->op, task->count,
                           task->n_srcs);
        break;
#else
        return UCC_ERR_NOT_SUPPORTED;
#endif
    case UCC_DT_FLOAT128:
#if SIZEOF_LONG_DOUBLE == 16
        DO_DT_REDUCE_FLOAT(long double, srcs, dst, task->op, task->count,
                           task->n_srcs);
        break;
#else
        return UCC_ERR_NOT_SUPPORTED;
#endif
    case UCC_DT_BFLOAT16:
        DO_DT_REDUCE_BFLOAT16(srcs, dst, task->op, task->count,
                              task->n_srcs);
        break;
    case UCC_DT_FLOAT32_COMPLEX:
#if SIZEOF_FLOAT__COMPLEX == 8
        DO_DT_REDUCE_FLOAT_COMPLEX(float complex, srcs, dst, task->op,
                                   task->count, task->n_srcs);
        break;
#else
        return UCC_ERR_NOT_SUPPORTED;
#endif
    case UCC_DT_FLOAT64_COMPLEX:
#if SIZEOF_DOUBLE__COMPLEX == 16
        DO_DT_REDUCE_FLOAT_COMPLEX(double complex, srcs, dst, task->op,
                                   task->count, task->n_srcs);
        break;
#else
        return UCC_ERR_NOT_SUPPORTED;
#endif
    case UCC_DT_FLOAT128_COMPLEX:
#if SIZEOF_LONG_DOUBLE__COMPLEX == 32
        DO_DT_REDUCE_FLOAT_COMPLEX(long double complex, srcs, dst,
                                   task->op, task->count, task->n_srcs);
        break;
#else
        return UCC_ERR_NOT_SUPPORTED;
#endif
    default:
        ec_error(&ucc_ec_cpu.super, "unsupported reduction type (%s)",
                 ucc_datatype_str(task->dt));
        return UCC_ERR_NOT_SUPPORTED;
    }

    return UCC_OK;
}

#ifdef HAVE_EC_THREADED_REDUCE

#include <pthread.h>

#include "core/ucc_dt.h"
#include "utils/ucc_malloc.h"

typedef struct ucc_ec_cpu_reduce_thread_args {
    void * const * restrict srcs;
    void * restrict dst;
    ucc_reduction_op_t op;
    ucc_datatype_t dt;
    size_t count;
    uint16_t n_srcs;
    uint16_t flags;
    size_t start_idx;
    size_t end_idx;
    double alpha;
    ucc_status_t status;
} ucc_ec_cpu_reduce_thread_args_t;

/*
 * Reduces the element range [start_idx, end_idx) of the "n_srcs" source
 * buffers (each "count" elements of "dt") into the corresponding range
 * of "dst". Every source (and dst) is offset by
 * start_idx * ucc_dt_size(dt); "alpha" is passed through to
 * ucc_ec_cpu_reduce and is applied exactly as ucc_ec_cpu_reduce applies
 * it: only when "flags" has UCC_EEE_TASK_FLAG_REDUCE_WITH_ALPHA set.
 */
ucc_status_t ucc_ec_cpu_reduce_chunk(void * const * restrict srcs,
                                      void * restrict dst,
                                      ucc_reduction_op_t op,
                                      ucc_datatype_t dt,
                                      size_t count,
                                      uint16_t n_srcs,
                                      uint16_t flags,
                                      size_t start_idx,
                                      size_t end_idx,
                                      double alpha)
{
    void *chunk_srcs[UCC_EE_EXECUTOR_NUM_BUFS];
    ucc_eee_task_reduce_t chunk_task;
    size_t dt_size = ucc_dt_size(dt);
    size_t i;

    if (start_idx > end_idx || end_idx > count || n_srcs == 0 ||
        n_srcs > UCC_EE_EXECUTOR_NUM_BUFS) {
        return UCC_ERR_INVALID_PARAM;
    }

    for (i = 0; i < n_srcs; i++) {
        chunk_srcs[i] = (char *)srcs[i] + start_idx * dt_size;
    }

    chunk_task.dst = (char *)dst + start_idx * dt_size;
    chunk_task.count = end_idx - start_idx;
    chunk_task.dt = dt;
    chunk_task.op = op;
    chunk_task.n_srcs = n_srcs;
    chunk_task.alpha = alpha;

    return ucc_ec_cpu_reduce(&chunk_task, chunk_task.dst, chunk_srcs, flags);
}

/*
 * Persistent worker set dedicated to ucc_ec_cpu_reduce_threaded.
 *
 * A per-call pthread_create/join costs ~10us per thread (clone + exit
 * round-trip, plus a 256KB stack mmap), which dominates fork-join at
 * high N and caps scaling below 0.6/N.  Instead, a lazily created set
 * of UCC_EC_CPU_REDUCE_MAX_WORKERS threads -- each on its own
 * preallocated 256KB static stack -- parks on a condvar between calls,
 * so a call only signals / unparks / reparks (~2us per thread).
 *
 * Contract: workers are process-lifetime and at most one batch is
 * active per process -- the caller blocks for the whole batch, and
 * workers never nest ucc_ec_cpu_reduce_threaded (they run
 * ucc_ec_cpu_reduce_chunk), so two batches can never overlap.
 * num_threads > UCC_EC_CPU_REDUCE_MAX_WORKERS keeps the fork-join path
 * for the excess threads.
 */
#define UCC_EC_CPU_REDUCE_MAX_WORKERS 8
#define UCC_EC_CPU_REDUCE_WORKER_STACK (256 * 1024)

static pthread_t     ucc_ec_cpu_reduce_workers[UCC_EC_CPU_REDUCE_MAX_WORKERS];
static char          ucc_ec_cpu_reduce_worker_stacks
                     [UCC_EC_CPU_REDUCE_MAX_WORKERS][UCC_EC_CPU_REDUCE_WORKER_STACK];
static pthread_mutex_t ucc_ec_cpu_reduce_mu   = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  ucc_ec_cpu_reduce_wake = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  ucc_ec_cpu_reduce_job  = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  ucc_ec_cpu_reduce_done = PTHREAD_COND_INITIALIZER;
static int             ucc_ec_cpu_reduce_parked;      /* parked on wake       */
static int             ucc_ec_cpu_reduce_active_n;    /* jobs set this batch  */
static int             ucc_ec_cpu_reduce_done_count;  /* done in this batch   */
static int             ucc_ec_cpu_reduce_pool_size;   /* 0 = uninitialized    */
static ucc_ec_cpu_reduce_thread_args_t ucc_ec_cpu_reduce_jobs
                                        [UCC_EC_CPU_REDUCE_MAX_WORKERS];


static void *ucc_ec_cpu_reduce_pool_worker(void *arg)
{
    int idx = (int)(intptr_t)arg;

    for (;;) {
        /* Park: wait for a batch; signal the pool that we are available.
           Park notifications go to "wake" only; the batch wake comes on
           "job", so a re-parking worker can never wake a parked worker. */
        pthread_mutex_lock(&ucc_ec_cpu_reduce_mu);
        ucc_ec_cpu_reduce_parked++;
        pthread_cond_signal(&ucc_ec_cpu_reduce_wake);
        pthread_cond_wait(&ucc_ec_cpu_reduce_job, &ucc_ec_cpu_reduce_mu);
        ucc_ec_cpu_reduce_parked--;
        /* The broadcast wakes every parked worker; only idx < active_n
           has a job this batch -- the rest re-park without running. */
        if (idx >= ucc_ec_cpu_reduce_active_n) {
            pthread_mutex_unlock(&ucc_ec_cpu_reduce_mu);
            continue;
        }
        pthread_mutex_unlock(&ucc_ec_cpu_reduce_mu);
        /* Run our chunk, then notify completion */
        ucc_ec_cpu_reduce_jobs[idx].status = ucc_ec_cpu_reduce_chunk(
            ucc_ec_cpu_reduce_jobs[idx].srcs,
            ucc_ec_cpu_reduce_jobs[idx].dst,
            ucc_ec_cpu_reduce_jobs[idx].op,
            ucc_ec_cpu_reduce_jobs[idx].dt,
            ucc_ec_cpu_reduce_jobs[idx].count,
            ucc_ec_cpu_reduce_jobs[idx].n_srcs,
            ucc_ec_cpu_reduce_jobs[idx].flags,
            ucc_ec_cpu_reduce_jobs[idx].start_idx,
            ucc_ec_cpu_reduce_jobs[idx].end_idx,
            ucc_ec_cpu_reduce_jobs[idx].alpha);

        pthread_mutex_lock(&ucc_ec_cpu_reduce_mu);
        ucc_ec_cpu_reduce_done_count++;
        pthread_cond_signal(&ucc_ec_cpu_reduce_done);
        pthread_mutex_unlock(&ucc_ec_cpu_reduce_mu);
    }
    return NULL;
}

/* Lazily create the worker set; returns the number of live workers. */
static int ucc_ec_cpu_reduce_pool_get(void)
{
    int size;

    pthread_mutex_lock(&ucc_ec_cpu_reduce_mu);
    if (ucc_ec_cpu_reduce_pool_size == 0) {
        int i;

        for (i = 0; i < UCC_EC_CPU_REDUCE_MAX_WORKERS; i++) {
            pthread_attr_t attr;
            int ret;

            pthread_attr_init(&attr);
            pthread_attr_setstack(&attr, ucc_ec_cpu_reduce_worker_stacks[i],
                                  UCC_EC_CPU_REDUCE_WORKER_STACK);
            ret = pthread_create(&ucc_ec_cpu_reduce_workers[i], &attr,
                                 ucc_ec_cpu_reduce_pool_worker,
                                 (void *)(intptr_t)i);
            pthread_attr_destroy(&attr);
            if (ret != 0) {
                ucc_ec_cpu_reduce_pool_size = i;
                break;
            }
        }
        if (i == UCC_EC_CPU_REDUCE_MAX_WORKERS) {
            ucc_ec_cpu_reduce_pool_size = UCC_EC_CPU_REDUCE_MAX_WORKERS;
        }
    }
    size = ucc_ec_cpu_reduce_pool_size;
    /* Release before waiting: workers need the mutex to park */
    pthread_mutex_unlock(&ucc_ec_cpu_reduce_mu);

    pthread_mutex_lock(&ucc_ec_cpu_reduce_mu);
    /* Wait until all live workers have parked */
    while (ucc_ec_cpu_reduce_parked < size) {
        pthread_cond_wait(&ucc_ec_cpu_reduce_wake, &ucc_ec_cpu_reduce_mu);
    }
    pthread_mutex_unlock(&ucc_ec_cpu_reduce_mu);

    return size;
}

static void *ucc_ec_cpu_reduce_thread_worker(void *arg)
{
    ucc_ec_cpu_reduce_thread_args_t *thread_args =
        (ucc_ec_cpu_reduce_thread_args_t *)arg;

    thread_args->status = ucc_ec_cpu_reduce_chunk(
        thread_args->srcs, thread_args->dst, thread_args->op,
        thread_args->dt, thread_args->count, thread_args->n_srcs,
        thread_args->flags, thread_args->start_idx, thread_args->end_idx,
        thread_args->alpha);

    return NULL;
}

/*
 * Splits the reduction of "task" across "num_threads" threads, each
 * reducing a contiguous chunk via ucc_ec_cpu_reduce_chunk. Falls back to
 * ucc_ec_cpu_reduce when threading is not worthwhile (num_threads <= 1
 * or count < 2*chunk_size) or when the chunk path cannot express "n_srcs"
 * (> UCC_EE_EXECUTOR_NUM_BUFS sources).
 */
ucc_status_t ucc_ec_cpu_reduce_threaded(ucc_eee_task_reduce_t *task,
                                        void * restrict dst,
                                        void * const * restrict srcs,
                                        uint16_t flags,
                                        int num_threads,
                                        size_t chunk_size)
{
    size_t chunk_count, remaining;
    int i, n_p, n_f, pool_size;
    ucc_status_t status = UCC_OK;

    if (num_threads <= 1 || task->count < chunk_size * 2 ||
        task->n_srcs > UCC_EE_EXECUTOR_NUM_BUFS) {
        /* Fall back to single-threaded reduction */
        return ucc_ec_cpu_reduce(task, dst, srcs, flags);
    }

    chunk_count = task->count / num_threads;
    remaining = task->count % num_threads;

    /*
     * The first min(num_threads, pool) chunks run on the persistent
     * worker set (signal / unpark, no per-call clone); any excess
     * (num_threads > pool) keeps the fork-join path.
     */
    pool_size = ucc_ec_cpu_reduce_pool_get();
    n_p = num_threads < UCC_EC_CPU_REDUCE_MAX_WORKERS ? num_threads
                                                       : UCC_EC_CPU_REDUCE_MAX_WORKERS;
    n_p = n_p < pool_size ? n_p : pool_size;
    n_f = num_threads - n_p;

    if (n_p > 0) {
        for (i = 0; i < n_p; i++) {
            ucc_ec_cpu_reduce_jobs[i].srcs = srcs;
            ucc_ec_cpu_reduce_jobs[i].dst = dst;
            ucc_ec_cpu_reduce_jobs[i].op = task->op;
            ucc_ec_cpu_reduce_jobs[i].dt = task->dt;
            ucc_ec_cpu_reduce_jobs[i].count = task->count;
            ucc_ec_cpu_reduce_jobs[i].n_srcs = task->n_srcs;
            ucc_ec_cpu_reduce_jobs[i].flags = flags;
            ucc_ec_cpu_reduce_jobs[i].alpha = task->alpha;

            /* Calculate chunk boundaries */
            ucc_ec_cpu_reduce_jobs[i].start_idx =
                i * chunk_count + (i < remaining ? i : remaining);
            ucc_ec_cpu_reduce_jobs[i].end_idx =
                ucc_ec_cpu_reduce_jobs[i].start_idx + chunk_count +
                (i < remaining ? 1 : 0);
        }

        pthread_mutex_lock(&ucc_ec_cpu_reduce_mu);
        while (ucc_ec_cpu_reduce_parked < n_p) {
            pthread_cond_wait(&ucc_ec_cpu_reduce_wake, &ucc_ec_cpu_reduce_mu);
        }
        ucc_ec_cpu_reduce_active_n = n_p;
        ucc_ec_cpu_reduce_done_count = 0;
        pthread_cond_broadcast(&ucc_ec_cpu_reduce_job);
        pthread_mutex_unlock(&ucc_ec_cpu_reduce_mu);
    }

    if (n_f > 0) {
        pthread_t *                threads;
        ucc_ec_cpu_reduce_thread_args_t *thread_args;
        int                          j, ret;

        threads = ucc_malloc(num_threads * sizeof(pthread_t), "reduce_threads");
        thread_args =
            ucc_malloc(num_threads * sizeof(ucc_ec_cpu_reduce_thread_args_t),
                       "reduce_thread_args");
        if (!threads || !thread_args) {
            ucc_free(threads);
            ucc_free(thread_args);
            return UCC_ERR_NO_MEMORY;
        }

        for (i = n_p; i < num_threads; i++) {
            thread_args[i].srcs = srcs;
            thread_args[i].dst = dst;
            thread_args[i].op = task->op;
            thread_args[i].dt = task->dt;
            thread_args[i].count = task->count;
            thread_args[i].n_srcs = task->n_srcs;
            thread_args[i].flags = flags;
            thread_args[i].alpha = task->alpha;

            /* Calculate chunk boundaries */
            thread_args[i].start_idx =
                i * chunk_count + (i < remaining ? i : remaining);
            thread_args[i].end_idx =
                thread_args[i].start_idx + chunk_count +
                (i < remaining ? 1 : 0);
        }

        for (j = n_p; j < num_threads; j++) {
            pthread_attr_t attr;

            pthread_attr_init(&attr);
            pthread_attr_setstacksize(&attr, UCC_EC_CPU_REDUCE_WORKER_STACK);
            ret = pthread_create(&threads[j], &attr,
                                 ucc_ec_cpu_reduce_thread_worker,
                                 &thread_args[j]);
            pthread_attr_destroy(&attr);
            if (ret != 0) {
                int k;
                for (k = n_p; k < j; k++) {
                    pthread_join(threads[k], NULL);
                }
                ucc_free(threads);
                ucc_free(thread_args);
                return UCC_ERR_NO_RESOURCE;
            }
        }

        /* Wait for the fork-join threads to complete */
        for (i = n_p; i < num_threads; i++) {
            pthread_join(threads[i], NULL);
            if (thread_args[i].status != UCC_OK) {
                status = thread_args[i].status;
            }
        }
        ucc_free(threads);
        ucc_free(thread_args);
    }

    if (n_p > 0) {
        /* Wait for the persistent workers to complete */
        pthread_mutex_lock(&ucc_ec_cpu_reduce_mu);
        while (ucc_ec_cpu_reduce_done_count < n_p) {
            pthread_cond_wait(&ucc_ec_cpu_reduce_done, &ucc_ec_cpu_reduce_mu);
        }
        pthread_mutex_unlock(&ucc_ec_cpu_reduce_mu);

        for (i = 0; i < n_p; i++) {
            if (ucc_ec_cpu_reduce_jobs[i].status != UCC_OK) {
                status = ucc_ec_cpu_reduce_jobs[i].status;
            }
        }
    }

    return status;
}

#endif /* HAVE_EC_THREADED_REDUCE */
