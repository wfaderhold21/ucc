/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

/*
 * Host-side DOCA UROM command channel for the DOCA (BlueField DPU) execution
 * component.  This file is the 584 "command channel": it owns the UROM
 * transport that talks to a BlueField DPU (open device -> progress engine ->
 * service -> worker, exactly the host-side sequence used by the collective
 * CL_DOCA_UROM component) and the host-side send of a reduce command.
 *
 * The transport is loaded with dlopen() and every DOCA symbol is resolved with
 * dlsym() at connect time.  This keeps libucc_ec_doca.so itself free of any
 * DOCA link dependency, so the component stays dlopen-able (and therefore
 * available for the 586 host fallback) on a host that has no DOCA SDK or no
 * DPU.  When the transport is absent or the DPU cannot be brought up, the
 * channel simply reports it as unavailable and the reduce falls back to the
 * nested CPU executor.
 *
 * The DPU-side execution of the reduce command (the worker that runs
 * ucc_ec_cpu_reduce_threaded on the ARM cores) is NOT implemented here: it
 * requires a BlueField DPU and reuses the plugin queue/command infrastructure
 * in contrib/doca_urom_ucc_plugin/dpu/worker_ucc.c.  The command payload
 * format is shared with the DPU worker via ec_doca_command.h, so the two
 * halves agree on the wire format even though only the host half exists in
 * this build.
 */

#ifndef UCC_EC_DOCA_CHANNEL_H_
#define UCC_EC_DOCA_CHANNEL_H_

#include <dlfcn.h>
#include <stdint.h>
#include <stddef.h>

/*
 * The host-side UROM call sequence (open device, create the progress engine
 * and service, start the worker, send a command) is identical to the
 * collective CL_DOCA_UROM component
 * (src/components/cl/doca_urom/cl_doca_urom_common.c).  All DOCA types here
 * are intentionally opaque: the transport never dereferences them, only the
 * dlsym'ed function pointers do.
 */
struct doca_pe;
struct doca_dev;
struct doca_devinfo;
struct doca_urom_service;
struct doca_urom_worker;
struct doca_ctx;

/*
 * doca_ctx_states is an anonymous DOCA enum; it cannot be forward-declared,
 * so the transport passes it around inside this opaque one-word struct.
 */
struct doca_ctx_state {
    int state;
};
typedef struct doca_ctx_state doca_ctx_state_t;

/* DOCA progress engine. */
typedef int (*ecc_doca_pe_create_fn)(struct doca_pe **);
typedef int (*ecc_doca_pe_destroy_fn)(struct doca_pe *);
typedef int (*ecc_doca_pe_progress_fn)(struct doca_pe *);

/* DOCA device. */
typedef int (*ecc_doca_devinfo_create_list_fn)(struct doca_devinfo ***,
                                               uint32_t *);
typedef int (*ecc_doca_devinfo_get_ibdev_name_fn)(struct doca_devinfo *,
                                                  char *, size_t);
typedef int (*ecc_doca_devinfo_destroy_list_fn)(struct doca_devinfo **);
typedef int (*ecc_doca_dev_open_fn)(const struct doca_devinfo *,
                                    struct doca_dev **);
typedef int (*ecc_doca_dev_close_fn)(struct doca_dev *);

/* DOCA UROM service / worker. */
typedef int (*ecc_doca_urom_service_create_fn)(struct doca_urom_service **);
typedef int (*ecc_doca_urom_service_destroy_fn)(struct doca_urom_service *);
typedef int (*ecc_doca_urom_service_set_dev_fn)(struct doca_urom_service *,
                                                struct doca_dev *);
typedef int (*ecc_doca_urom_service_set_max_workers_fn)(
    struct doca_urom_service *, uint64_t);
typedef int (*ecc_doca_urom_worker_create_fn)(struct doca_urom_worker **);
typedef int (*ecc_doca_urom_worker_destroy_fn)(struct doca_urom_worker *);
typedef int (*ecc_doca_urom_worker_set_service_fn)(struct doca_urom_worker *,
                                                   struct doca_urom_service *);
typedef int (*ecc_doca_urom_worker_set_id_fn)(struct doca_urom_worker *,
                                              uint64_t);
typedef int (*ecc_doca_urom_worker_set_max_inflight_tasks_fn)(
    struct doca_urom_worker *, uint64_t);
typedef int (*ecc_doca_urom_worker_set_plugins_fn)(struct doca_urom_worker *,
                                                   uint64_t);
/* Context bridging: doca_urom_*_as_ctx() are macros in doca_urom.h that
 * return the embedded struct doca_ctx *, so their address is taken here. */
typedef int (*ecc_doca_pe_connect_ctx_fn)(struct doca_pe *, struct doca_ctx *);
typedef int (*ecc_doca_ctx_start_fn)(struct doca_ctx *);
typedef int (*ecc_doca_ctx_stop_fn)(struct doca_ctx *);
typedef int (*ecc_doca_ctx_get_state_fn)(struct doca_ctx *, doca_ctx_state_t *);

/*
 * The DPU worker id is chosen by the host (it matches the DPU-side worker
 * rank).  The UROM worker id used here is 0: a single reduce worker.
 */
#define ECC_DOCA_WORKER_ID 0u

/* doca_error_t codes (doca_error.h); only the ones the transport needs. */
#define ECC_DOCA_SUCCESS 0
#define ECC_DOCA_ERROR_INVALID_VALUE 3
#define ECC_DOCA_ERROR_NOT_SUPPORTED 4
#define ECC_DOCA_ERROR_IN_PROGRESS 26

/* doca_ctx_states values (doca_ctx.h). */
#define ECC_DOCA_CTX_STATE_IDLE 0
#define ECC_DOCA_CTX_STATE_STARTING 2
#define ECC_DOCA_CTX_STATE_RUNNING 3

struct ecc_doca_urom_transport {
    /* Resolved DOCA symbols (NULL when the transport is not loaded). */
    void                              *handle;
    ecc_doca_pe_create_fn              pe_create;
    ecc_doca_pe_destroy_fn             pe_destroy;
    ecc_doca_pe_progress_fn            pe_progress;
    ecc_doca_devinfo_create_list_fn    devinfo_create_list;
    ecc_doca_devinfo_get_ibdev_name_fn devinfo_get_ibdev_name;
    ecc_doca_devinfo_destroy_list_fn   devinfo_destroy_list;
    ecc_doca_dev_open_fn               dev_open;
    ecc_doca_dev_close_fn              dev_close;
    ecc_doca_urom_service_create_fn    service_create;
    ecc_doca_urom_service_destroy_fn   service_destroy;
    ecc_doca_urom_service_set_dev_fn   service_set_dev;
    ecc_doca_urom_service_set_max_workers_fn
                                       service_set_max_workers;
    ecc_doca_urom_worker_create_fn     worker_create;
    ecc_doca_urom_worker_destroy_fn    worker_destroy;
    ecc_doca_urom_worker_set_service_fn
                                       worker_set_service;
    ecc_doca_urom_worker_set_id_fn     worker_set_id;
    ecc_doca_urom_worker_set_max_inflight_tasks_fn
                                       worker_set_max_inflight_tasks;
    ecc_doca_urom_worker_set_plugins_fn
                                       worker_set_plugins;
    ecc_doca_pe_connect_ctx_fn         pe_connect_ctx;
    ecc_doca_ctx_start_fn              ctx_start;
    ecc_doca_ctx_stop_fn               ctx_stop;
    ecc_doca_ctx_get_state_fn          ctx_get_state;

    /* Live channel objects (all NULL until a connect succeeds). */
    struct doca_dev          *dev;
    struct doca_pe           *pe;
    struct doca_urom_service *service;
    struct doca_urom_worker  *worker;
};

extern struct ecc_doca_urom_transport ecc_doca_urom;

/*
 * Bring up the host-side UROM channel on `dev_name`.  Returns 0 on success
 * (transport loaded, device opened, PE + service created, the worker started
 * to RUNNING) and a nonzero DOCA error code otherwise.  Safe to call once
 * from the component.
 */
int ecc_doca_channel_connect(struct ecc_doca_urom_transport *tr,
                             const char *dev_name);

/* Tear the channel back down, in reverse. */
void ecc_doca_channel_disconnect(struct ecc_doca_urom_transport *tr);

/* Drive the progress engine (DPU work completes out-of-band). */
int ecc_doca_channel_progress(struct ecc_doca_urom_transport *tr);

#endif /* UCC_EC_DOCA_CHANNEL_H_ */
