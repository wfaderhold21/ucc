/* Repro: harness case sequence — (8192, n2, nt8) then (8192, n3, nt1..8). */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <stdint.h>

#include "config.h"
#include "components/ec/cpu/ec_cpu.h"

static void fill8(int8_t *p, size_t count, unsigned seed)
{
    for (size_t i = 0; i < count; i++)
        p[i] = (int8_t)((i * 7 + seed * 11) & 0xFF);
}

static void run_case(size_t count, int n_srcs, int nt)
{
    int8_t **srcs = malloc(n_srcs * sizeof(*srcs));
    int8_t *dst_ref = malloc(count), *dst_thr = malloc(count);
    void **ptrs = malloc(n_srcs * sizeof(*ptrs));
    ucc_eee_task_reduce_t task;
    int s, bad = 0;

    for (s = 0; s < n_srcs; s++) {
        srcs[s] = malloc(count);
        fill8(srcs[s], count, s + 1);
        ptrs[s] = srcs[s];
    }
    task.count  = count;
    task.alpha  = 1.0;
    task.dt     = UCC_DT_INT8;
    task.op     = UCC_OP_SUM;
    task.n_srcs = (uint16_t)n_srcs;

    ucc_status_t st_ref = ucc_ec_cpu_reduce(&task, dst_ref, ptrs, 0);
    ucc_status_t st_thr =
        ucc_ec_cpu_reduce_threaded(&task, dst_thr, ptrs, 0, nt, 4096);

    for (size_t i = 0; i < count; i++)
        if (dst_ref[i] != dst_thr[i])
            bad++;
    printf("count=%zu n=%d nt=%d st_ref=%d st_thr=%d mismatch=%zu\n",
           count, n_srcs, nt, (int)st_ref, (int)st_thr, (size_t)bad);

    for (s = 0; s < n_srcs; s++)
        free(srcs[s]);
    free(srcs);
    free(dst_ref);
    free(dst_thr);
    free(ptrs);
}

int main(void)
{
    run_case(8192, 2, 8);   /* first pool batch */
    run_case(8192, 3, 1);   /* harness's first failing case */
    run_case(8192, 3, 2);
    run_case(8192, 3, 3);
    run_case(8192, 3, 8);
    run_case(8192, 8, 2);
    return 0;
}
