/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_EC_DOCA_H_
#define UCC_EC_DOCA_H_

#include "components/ec/base/ucc_ec_base.h"
#include "components/ec/ucc_ec_log.h"
#include "utils/ucc_mpool.h"
#include "ec_doca_channel.h"

typedef struct ucc_ec_doca_config {
    ucc_ec_config_t super;
    /*
     * DOCA (BlueField DPU) offload configuration.  USE_DOCA_REDUCE is a
     * *global* option (see ucc_global_opts); these tune the DPU-side command
     * channel and worker (584-585).  The host-fallback size gate reuses
     * the Milestone-B DOCA_REDUCE_LIMIT idea so a too-large reduce stays on
     * the accelerator instead of the DPU round-trip.
     */
    unsigned long reduce_limit;   /* max reduce bytes routed to the DPU/offload */
    unsigned long dpu_num_workers;/* DPU worker count (584 command channel)   */
    int           dpu_affinity;   /* DPU worker core affinity (0 = none)       */
    char          *device;        /* DPU IB device for the 584 channel        */
} ucc_ec_doca_config_t;

/*
 * Executor lifecycle state.  The skeleton (task 583) only moves an executor
 * between INITIALIZED / STARTED / STOPPED through the no-op start/stop stubs;
 * the DOCA command channel (task 584) will introduce in-flight states for the
 * DPU-side reduction.
 */
typedef enum ucc_ec_doca_executor_state {
    UCC_EC_DOCA_EXECUTOR_INITIALIZED = 0,
    UCC_EC_DOCA_EXECUTOR_STARTED,
    UCC_EC_DOCA_EXECUTOR_STOPPED,
} ucc_ec_doca_executor_state_t;

typedef struct ucc_ec_doca_executor {
    ucc_ee_executor_t             super;
    ucc_ec_doca_executor_state_t  state;
    /*
     * Nested CPU executor: the Milestone-B host-offload path.  A DOCA reduce
     * that cannot run on the DPU (the command channel is not connected in the
     * 583/586 skeleton; or is too large / unsupported) is delegated to this
     * executor (task 586).  Mirrors the CUDA executor's cpu_executor.
     */
    ucc_ee_executor_t             *cpu_executor;
} ucc_ec_doca_executor_t;

/*
 * A completed (host-fallback) task wrapper.  The CPU reduce already finished
 * at post time (mirrors the CUDA host-offload, which runs the CPU task to
 * completion at post); the wrapper just reports completion to task_test and
 * is released by task_finalize.
 */
typedef struct ucc_ec_doca_task {
    ucc_ee_executor_task_t  super;
    int                     finished;
} ucc_ec_doca_task_t;
typedef struct ucc_ec_doca {
    ucc_ec_base_t       super;
    ucc_thread_mode_t   thread_mode;
    ucc_mpool_t         executors;       /* pooled ucc_ec_doca_executor_t  */
    ucc_mpool_t         task_wrappers;   /* pooled ucc_ec_doca_task_t      */
    int                 channel_connected; /* 584: DOCA UROM channel up  */
} ucc_ec_doca_t;

#define EC_DOCA_CONFIG \
    (ucc_derived_of(ucc_ec_doca.super.config, ucc_ec_doca_config_t))

extern ucc_ec_doca_t ucc_ec_doca;

#endif
