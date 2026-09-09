/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

/*
 * Host-side DOCA UROM transport for the DOCA execution component (task 584).
 * See ec_doca_channel.h for the design: the DOCA symbols are resolved at
 * connect time with dlopen/dlsym so libucc_ec_doca.so carries no DOCA link
 * dependency and the component stays available for the host fallback on a
 * DOCA-less host.
 *
 * The bring-up sequence mirrors the collective CL_DOCA_UROM host side
 * (src/components/cl/doca_urom/cl_doca_urom_common.c): open device ->
 * create PE -> create + start service -> create + start worker.  The
 * worker's reduce-plugin selection and the task-submission / notification
 * path are the 585/587 seams: they need the DOCA UROM task API (header not
 * resolvable here without the SDK) and the DPU-side worker that executes the
 * reduce, so they are left as marked seams and the reduce falls back to the
 * nested CPU executor until then.
 */

#include "ec_doca_channel.h"

#include <dlfcn.h>
#include <stdint.h>
#include <string.h>
#include <limits.h>

/*
 * The DOCA UROM context objects embed their struct doca_ctx at a fixed
 * offset; doca_urom_service_as_ctx()/doca_urom_worker_as_ctx() in
 * doca_urom.h return that embedded pointer.  The transport cannot include
 * doca_urom.h (it would pull in DOCA link deps), so it reproduces the
 * pointer arithmetic with this offset.  DOCA keeps the embedded ctx at the
 * start of the public struct for both types.
 */
#define ECC_DOCA_CTX_EMBED_OFFSET 0u
#define ECC_DOCA_WORKER_MAX_INFLIGHT 16u
#define ECC_DOCA_CONNECT_MAX_ITERS 1000000u

static struct doca_ctx *ecc_doca_service_as_ctx(struct doca_urom_service *s)
{
    return (struct doca_ctx *)(((char *)s) + ECC_DOCA_CTX_EMBED_OFFSET);
}

static struct doca_ctx *ecc_doca_worker_as_ctx(struct doca_urom_worker *w)
{
    return (struct doca_ctx *)(((char *)w) + ECC_DOCA_CTX_EMBED_OFFSET);
}

struct ecc_doca_urom_transport ecc_doca_urom;

static const char *ecc_doca_transport_libs[] = {
    "libdoca_urom.so.3", "libdoca_urom.so.2", "libdoca_urom.so.1",
    "libdoca_urom.so"
};

static int ecc_doca_transport_load(struct ecc_doca_urom_transport *tr)
{
    size_t i;

    for (i = 0; i < sizeof(ecc_doca_transport_libs) /
                      sizeof(ecc_doca_transport_libs[0]);
         i++) {
        tr->handle = dlopen(ecc_doca_transport_libs[i], RTLD_NOW | RTLD_LOCAL);
        if (tr->handle) {
            break;
        }
    }
    if (!tr->handle) {
        return -1;
    }

#define ECC_DOCA_SYM(name, field) do {                                        \
        *(void **)&tr->field = dlsym(tr->handle, name);                       \
        if (!tr->field) {                                                     \
            return -1;                                                        \
        }                                                                     \
    } while (0)

    ECC_DOCA_SYM("doca_pe_create", pe_create);
    ECC_DOCA_SYM("doca_pe_destroy", pe_destroy);
    ECC_DOCA_SYM("doca_pe_progress", pe_progress);
    ECC_DOCA_SYM("doca_devinfo_create_list", devinfo_create_list);
    ECC_DOCA_SYM("doca_devinfo_get_ibdev_name", devinfo_get_ibdev_name);
    ECC_DOCA_SYM("doca_devinfo_destroy_list", devinfo_destroy_list);
    ECC_DOCA_SYM("doca_dev_open", dev_open);
    ECC_DOCA_SYM("doca_dev_close", dev_close);
    ECC_DOCA_SYM("doca_urom_service_create", service_create);
    ECC_DOCA_SYM("doca_urom_service_destroy", service_destroy);
    ECC_DOCA_SYM("doca_urom_service_set_dev", service_set_dev);
    ECC_DOCA_SYM("doca_urom_service_set_max_workers",
                 service_set_max_workers);
    ECC_DOCA_SYM("doca_urom_worker_create", worker_create);
    ECC_DOCA_SYM("doca_urom_worker_destroy", worker_destroy);
    ECC_DOCA_SYM("doca_urom_worker_set_service", worker_set_service);
    ECC_DOCA_SYM("doca_urom_worker_set_id", worker_set_id);
    ECC_DOCA_SYM("doca_urom_worker_set_max_inflight_tasks",
                 worker_set_max_inflight_tasks);
    ECC_DOCA_SYM("doca_urom_worker_set_plugins", worker_set_plugins);
    ECC_DOCA_SYM("doca_pe_connect_ctx", pe_connect_ctx);
    ECC_DOCA_SYM("doca_ctx_start", ctx_start);
    ECC_DOCA_SYM("doca_ctx_stop", ctx_stop);
    ECC_DOCA_SYM("doca_ctx_get_state", ctx_get_state);
#undef ECC_DOCA_SYM

    return 0;
}

static void ecc_doca_transport_unload(struct ecc_doca_urom_transport *tr)
{
    if (tr->handle) {
        dlclose(tr->handle);
        tr->handle = NULL;
    }
}

/*
 * Open the first DOCA device whose IB device name matches `dev_name`.
 * Mirrors ucc_cl_doca_urom_open_doca_device_with_ibdev_name.
 */
static int ecc_doca_open_device(struct ecc_doca_urom_transport *tr,
                                const char *dev_name)
{
    struct doca_devinfo **dev_list = NULL;
    uint32_t              nb_devs  = 0;
    char                  buf[64]  = {0};
    size_t                 i;
    int                    ret;

    ret = tr->devinfo_create_list(&dev_list, &nb_devs);
    if (ret != ECC_DOCA_SUCCESS) {
        return ret;
    }
    for (i = 0; i < nb_devs; i++) {
        ret = tr->devinfo_get_ibdev_name(dev_list[i], buf,
                                         sizeof(buf));
        if (ret == ECC_DOCA_SUCCESS && strcmp(buf, dev_name) == 0) {
            ret = tr->dev_open(dev_list[i], &tr->dev);
            if (ret == ECC_DOCA_SUCCESS) {
                tr->devinfo_destroy_list(dev_list);
                return ECC_DOCA_SUCCESS;
            }
            break;
        }
    }
    tr->devinfo_destroy_list(dev_list);
    return ECC_DOCA_ERROR_NOT_SUPPORTED;
}

int ecc_doca_channel_connect(struct ecc_doca_urom_transport *tr,
                             const char *dev_name)
{
    doca_ctx_state_t   st;
    uint64_t           iters;
    int                ret;

    if (!dev_name || !*dev_name) {
        return ECC_DOCA_ERROR_INVALID_VALUE;
    }

    memset(tr, 0, sizeof(*tr));
    if (ecc_doca_transport_load(tr) != 0) {
        return ECC_DOCA_ERROR_NOT_SUPPORTED; /* no DOCA UROM transport */
    }

    ret = ecc_doca_open_device(tr, dev_name);
    if (ret != ECC_DOCA_SUCCESS) {
        ecc_doca_transport_unload(tr);
        return ret;
    }

    ret = tr->pe_create(&tr->pe);
    if (ret != ECC_DOCA_SUCCESS) {
        goto dev_close;
    }

    ret = tr->service_create(&tr->service);
    if (ret != ECC_DOCA_SUCCESS) {
        goto pe_destroy;
    }
    ret = tr->pe_connect_ctx(tr->pe, ecc_doca_service_as_ctx(tr->service));
    if (ret != ECC_DOCA_SUCCESS) {
        goto service_destroy;
    }
    ret = tr->service_set_max_workers(tr->service, 1);
    if (ret != ECC_DOCA_SUCCESS) {
        goto service_destroy;
    }
    ret = tr->service_set_dev(tr->service, tr->dev);
    if (ret != ECC_DOCA_SUCCESS) {
        goto service_destroy;
    }
    ret = tr->ctx_start(ecc_doca_service_as_ctx(tr->service));
    if (ret != ECC_DOCA_SUCCESS) {
        goto service_destroy;
    }
    ret = tr->ctx_get_state(ecc_doca_service_as_ctx(tr->service), &st);
    if (ret != ECC_DOCA_SUCCESS || st.state != ECC_DOCA_CTX_STATE_RUNNING) {
        goto service_stop;
    }

    ret = tr->worker_create(&tr->worker);
    if (ret != ECC_DOCA_SUCCESS) {
        goto service_stop;
    }
    ret = tr->worker_set_service(tr->worker, tr->service);
    if (ret != ECC_DOCA_SUCCESS) {
        goto worker_destroy;
    }
    ret = tr->pe_connect_ctx(tr->pe, ecc_doca_worker_as_ctx(tr->worker));
    if (ret != ECC_DOCA_SUCCESS) {
        goto worker_destroy;
    }
    ret = tr->worker_set_id(tr->worker, ECC_DOCA_WORKER_ID);
    if (ret != ECC_DOCA_SUCCESS) {
        goto worker_destroy;
    }
    ret = tr->worker_set_max_inflight_tasks(tr->worker,
                                            ECC_DOCA_WORKER_MAX_INFLIGHT);
    if (ret != ECC_DOCA_SUCCESS) {
        goto worker_destroy;
    }
    /*
     * 585/587 seam: select the DPU-side reduce plugin via
     * doca_urom_worker_set_plugins() (the reduce worker plugin is the
     * counterpart of contrib/doca_urom_ucc_plugin/dpu/worker_ucc.c).  The
     * plugin id is discovered from doca_urom_service_get_plugins_list();
     * left as a seam because it requires the DPU-side plugin to be present
     * and the DOCA UROM task API, both of which need a BlueField DPU.
     */
    ret = tr->ctx_start(ecc_doca_worker_as_ctx(tr->worker));
    if (ret != ECC_DOCA_SUCCESS && ret != ECC_DOCA_ERROR_IN_PROGRESS) {
        goto worker_destroy;
    }
    for (iters = 0; iters < ECC_DOCA_CONNECT_MAX_ITERS; iters++) {
        tr->pe_progress(tr->pe);
        ret = tr->ctx_get_state(ecc_doca_worker_as_ctx(tr->worker), &st);
        if (ret != ECC_DOCA_SUCCESS) {
            break;
        }
        if (st.state == ECC_DOCA_CTX_STATE_RUNNING) {
            break;
        }
    }
    if (ret != ECC_DOCA_SUCCESS || st.state != ECC_DOCA_CTX_STATE_RUNNING) {
        goto worker_stop;
    }
    return ECC_DOCA_SUCCESS;

worker_stop:
    tr->ctx_stop(ecc_doca_worker_as_ctx(tr->worker));
worker_destroy:
    tr->worker_destroy(tr->worker);
    tr->worker = NULL;
service_stop:
    tr->ctx_stop(ecc_doca_service_as_ctx(tr->service));
service_destroy:
    tr->service_destroy(tr->service);
    tr->service = NULL;
pe_destroy:
    tr->pe_destroy(tr->pe);
    tr->pe = NULL;
dev_close:
    tr->dev_close(tr->dev);
    tr->dev = NULL;
    ecc_doca_transport_unload(tr);
    return ret;
}

void ecc_doca_channel_disconnect(struct ecc_doca_urom_transport *tr)
{
    if (tr->worker) {
        tr->ctx_stop(ecc_doca_worker_as_ctx(tr->worker));
        tr->worker_destroy(tr->worker);
        tr->worker = NULL;
    }
    if (tr->service) {
        tr->ctx_stop(ecc_doca_service_as_ctx(tr->service));
        tr->service_destroy(tr->service);
        tr->service = NULL;
    }
    if (tr->pe) {
        tr->pe_destroy(tr->pe);
        tr->pe = NULL;
    }
    if (tr->dev) {
        tr->dev_close(tr->dev);
        tr->dev = NULL;
    }
    ecc_doca_transport_unload(tr);
}

int ecc_doca_channel_progress(struct ecc_doca_urom_transport *tr)
{
    if (tr->pe_progress && tr->pe) {
        return tr->pe_progress(tr->pe);
    }
    return ECC_DOCA_SUCCESS;
}
