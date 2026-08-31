/* Minimal repro: persistent-pool threaded reduce, run twice. */
#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#include "config.h"
extern "C" {
#include "components/ec/cpu/ec_cpu.h"
}

#define NTEST 3

int main(void)
{
    size_t count = 8192;
    int n_srcs = 2, nt = 8;
    int32_t *srcs[NTEST];
    int32_t *dst = malloc(count * sizeof(int32_t));
    ucc_eee_task_reduce_t task;
    void *ptrs[NTEST];
    int r, s, it;

    for (s = 0; s < n_srcs; s++) {
        srcs[s] = malloc(count * sizeof(int32_t));
        for (int i = 0; i < (int)count; i++)
            srcs[s][i] = i * (s + 1) + 1;
        ptrs[s] = srcs[s];
    }

    task.count  = count;
    task.alpha  = 1.0;
    task.dt     = UCC_DT_INT32;
    task.op     = UCC_OP_SUM;
    task.n_srcs = (uint16_t)n_srcs;

    for (it = 0; it < 3; it++) {
        memset(dst, 0, count * sizeof(int32_t));
        ucc_status_t st = ucc_ec_cpu_reduce_threaded(
            &task, dst, ptrs, 0, nt, 4096);
        printf("iter %d st=%d dst[0]=%d dst[4095]=%d dst[4096]=%d dst[%zu]=%d\n",
               it, (int)st, dst[0], dst[4095], dst[4096], count - 1,
               dst[count - 1]);
    }
    return 0;
}
