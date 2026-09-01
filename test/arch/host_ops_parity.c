/*
 * 578 semantic-parity check for the shared host-capability helpers
 * (ucc_ec_host_ops.h).  Compiles the header standalone (the same way a CUDA
 * TU would) and verifies ucc_ec_host_total_reduce_len() /
 * ucc_ec_host_dt_supported() return exactly what the original statics in
 * ec_rocm_executor_interruptible.c returned, across the (dt, op) matrix.
 *
 * Not a Makefile target — built ad hoc:
 *   gcc -I src -I src/. -I . -DHAVE_CONFIG_H host_ops_parity.c \
 *       -o host_ops_parity src/libucc.la ... (link libucc for ucc_dt_size)
 */
#include <stdio.h>
#include <stdlib.h>
#include "components/ec/base/ucc_ec_host_ops.h"

static int n_fail = 0;

static void check_eq(const char *what, long got, long want)
{
    if (got != want) {
        printf("FAIL %s: got %ld want %ld\n", what, got, want);
        n_fail++;
    } else {
        printf("ok   %s = %ld\n", what, got);
    }
}

int main(void)
{
    ucc_ee_executor_task_args_t a;

    /* REDUCE, f32, count 1000 -> 1000 * 4 = 4000 bytes */
    a.task_type = UCC_EE_EXECUTOR_TASK_REDUCE;
    a.reduce.dt = UCC_DT_FLOAT32;
    a.reduce.count = 1000;
    check_eq("reduce f32 x1000 len", ucc_ec_host_total_reduce_len(&a), 4000);
    check_eq("reduce f32 dt_supported", ucc_ec_host_dt_supported(&a), 1);

    /* REDUCE, f64, count 3 -> 24 bytes */
    a.reduce.dt = UCC_DT_FLOAT64;
    a.reduce.count = 3;
    check_eq("reduce f64 x3 len", ucc_ec_host_total_reduce_len(&a), 24);
    check_eq("reduce f64 dt_supported", ucc_ec_host_dt_supported(&a), 1);

    /* REDUCE_STRIDED, int64, count 7 -> 56 bytes */
    a.task_type = UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED;
    a.reduce_strided.dt = UCC_DT_INT64;
    a.reduce_strided.count = 7;
    check_eq("strided i64 x7 len", ucc_ec_host_total_reduce_len(&a), 56);
    check_eq("strided i64 dt_supported", ucc_ec_host_dt_supported(&a), 1);

    /* accelerator-only types must be reported as NOT host-supported */
    a.task_type = UCC_EE_EXECUTOR_TASK_REDUCE;
    a.reduce.dt = UCC_DT_BFLOAT16;
    a.reduce.count = 1;
    check_eq("reduce bf16 dt_supported", ucc_ec_host_dt_supported(&a), 0);

    a.reduce.dt = UCC_DT_FLOAT16;
    check_eq("reduce fp16 dt_supported", ucc_ec_host_dt_supported(&a), 0);

    a.reduce.dt = UCC_DT_FLOAT32_COMPLEX;
    check_eq("reduce c32 dt_supported", ucc_ec_host_dt_supported(&a), 0);

    a.reduce.dt = UCC_DT_FLOAT64_COMPLEX;
    check_eq("reduce c64 dt_supported", ucc_ec_host_dt_supported(&a), 0);

    if (n_fail) {
        printf("HOST_OPS_PARITY: FAIL (%d)\n", n_fail);
        return 1;
    }
    printf("HOST_OPS_PARITY: ALL PASS\n");
    return 0;
}
