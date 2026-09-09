/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

/*
 * Host <-> DPU reduce command wire format for the DOCA (BlueField DPU)
 * execution component (task 584).
 *
 * The host side (this component) packs a reduce into a struct ecc_doca_cmd and
 * sends it to the DPU worker over the DOCA command channel.  The DPU-side
 * worker (deferred to hardware; reuses
 * contrib/doca_urom_ucc_plugin/dpu/worker_ucc.c) parses the same struct, runs
 * the reduction on the ARM cores, and sends back a struct ecc_doca_cmd_reply.
 *
 * The two halves are built from the same source tree but are separate binaries
 * (host x86 and DPU ARM), so this header is self-contained: it uses fixed-width
 * types and no UCC/CUDA/DOCA types, and it must be ABI-stable across the two
 * builds.  Keep it that way.
 */

#ifndef UCC_EC_DOCA_COMMAND_H_
#define UCC_EC_DOCA_COMMAND_H_

#include <stdint.h>
#include <stddef.h>

/* Must equal UCC_EE_EXECUTOR_NUM_BUFS (src/components/ec/base/ucc_ec_base.h). */
#define ECC_DOCA_MAX_SRC_BUFS 9

/* Command types carried in struct ecc_doca_cmd::cmd_type. */
enum ecc_doca_cmd_type {
    ECC_DOCA_CMD_REDUCE = 1, /* reduce n_srcs buffers into dst */
};

/* Host -> DPU: a single reduce.  op/dt carry the numeric values of
 * ucc_reduction_op_t / ucc_datatype_t so the DPU side needs no UCC types. */
struct ecc_doca_cmd_reduce {
    uint64_t cmd_seq; /* monotonic host command id, echoed in the reply */
    uint32_t op;      /* ucc_reduction_op_t value */
    uint32_t dt;      /* ucc_datatype_t value */
    uint64_t count;   /* elements per source buffer */
    uint16_t n_srcs;  /* number of source buffers (<= ECC_DOCA_MAX_SRC_BUFS) */
    uint16_t flags;   /* ucc_eee_task_flags value (alpha / ext-srcs) */
    uint64_t dst;     /* destination address (DPU-visible) */
    uint64_t srcs[ECC_DOCA_MAX_SRC_BUFS]; /* source addresses (DPU-visible)  */
    size_t   len;     /* total bytes moved, for the DPU to size the work */
};

struct ecc_doca_cmd {
    uint32_t cmd_type;
    union {
        struct ecc_doca_cmd_reduce reduce;
    };
};

/* DPU -> host: completion of one command. */
struct ecc_doca_cmd_reply {
    uint64_t cmd_seq; /* matches the command it completes */
    int32_t  status;  /* ucc_status_t value */
};

#endif /* UCC_EC_DOCA_COMMAND_H_ */
