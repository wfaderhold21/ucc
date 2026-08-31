/* Native correctness check for the AVX2 wrapping 8/16-bit SUM kernels
 * (int8/int16/uint8/uint16).  The SIMD reduce must be bitwise-identical to
 * the scalar C reference: widen-and-add must match wrapping integer math.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>

#include "../../src/ucc/api/ucc.h"
#include "../../src/utils/arch/reduce_simd.h"

static int n_mismatch;

#define CHECK_VEC(DT, CTYPE, UCC_DT, N, K)                                     \
    do {                                                                       \
        CTYPE **srcs;                                                        \
        CTYPE *res, *ref;                                                      \
        size_t i;                                                              \
        srcs = (CTYPE **)malloc((size_t)N * sizeof(CTYPE *));                \
        for (i = 0; i < (size_t)N; i++) {                                     \
            srcs[i] = (CTYPE *)malloc((K) * sizeof(CTYPE));            \
            res = (CTYPE *)malloc((K) * sizeof(CTYPE));                        \
            ref = (CTYPE *)malloc((K) * sizeof(CTYPE));                        \
            for (size_t j = 0; j < (K); j++) {                                \
                srcs[i][j] = (CTYPE)((i * 7919 + 13 * (int32_t)j) % 1000);    \
            }                                                                  \
        }                                                                      \
        ucc_arch_reduce_avx2(res, (const void * const *)srcs, K, N, UCC_DT,   \
                             UCC_OP_SUM);                                      \
        for (i = 0; i < (K); i++) {                                            \
            CTYPE acc = srcs[0][i];                                            \
            for (size_t j = 1; j < N; j++) {                                   \
                acc = (CTYPE)(acc + srcs[j][i]);                               \
            }                                                                  \
            ref[i] = acc;                                                      \
        }                                                                      \
        for (i = 0; i < (K); i++) {                                            \
            if (memcmp(&res[i], &ref[i], sizeof(CTYPE))) {                     \
                if (n_mismatch < 4) {                                          \
                    printf("%s: i=%zu res=%d ref=%d\n", #DT, i,               \
                           (int)(intptr_t)res[i], (int)(intptr_t)ref[i]);     \
                }                                                              \
                n_mismatch++;                                                  \
            }                                                                  \
        }                                                                      \
        for (i = 0; i < (size_t)N; i++) free(srcs[i]);                         \
        free(srcs);                                                          \
        free(res);                                                             \
        free(ref);                                                             \
    } while (0)

int main(void)
{
    if (!ucc_arch_avx2_supported()) {
        printf("avx2 not supported\n");
        return 77;
    }
    CHECK_VEC(INT8, int8_t, UCC_DT_INT8, 3, 1000);
    CHECK_VEC(INT8, int8_t, UCC_DT_INT8, 9, 1000);
    CHECK_VEC(INT16, int16_t, UCC_DT_INT16, 3, 1000);
    CHECK_VEC(INT16, int16_t, UCC_DT_INT16, 9, 1000);
    CHECK_VEC(UINT8, uint8_t, UCC_DT_UINT8, 3, 1000);
    CHECK_VEC(UINT8, uint8_t, UCC_DT_UINT8, 9, 1000);
    CHECK_VEC(UINT16, uint16_t, UCC_DT_UINT16, 3, 1000);
    CHECK_VEC(UINT16, uint16_t, UCC_DT_UINT16, 9, 1000);

    /* regression: int32 sum still exact */
    {
        int32_t s0[64], s1[64], s2[64];
        int32_t *r = (int32_t *)malloc(64 * sizeof(int32_t));
        int32_t *f = (int32_t *)malloc(64 * sizeof(int32_t));
        for (size_t i = 0; i < 64; i++) {
            s0[i] = (int32_t)(i * 97 + 5);
            s1[i] = (int32_t)(i * 31 - 40000);
            s2[i] = (int32_t)(i * 13 + 7);
        }
        ucc_arch_reduce_avx2(r, (const void * const *[]){s0, s1, s2}, 64,
                             3, UCC_DT_INT32, UCC_OP_SUM);
        for (size_t i = 0; i < 64; i++) {
            f[i] = s0[i] + s1[i] + s2[i];
            if (r[i] != f[i]) {
                printf("int32 i=%zu r=%d f=%d\n", i, r[i], f[i]);
                n_mismatch++;
            }
        }
    }

    printf(n_mismatch ? "FAIL: %d mismatches\n" : "PASS: all wrap-sum checks\n",
           n_mismatch);
    return n_mismatch ? 1 : 0;
}
