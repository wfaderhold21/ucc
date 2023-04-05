/**
 * Copyright (c) 2020-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "cl_urom.h"
#include "utils/ucc_malloc.h"
#include "components/tl/ucc_tl.h"
#include "core/ucc_global_opts.h"
#include "utils/ucc_math.h"

#include <urom/api/urom.h>

static int service_connect(ucc_cl_urom_lib_t *urom_lib, urom_service_params_t *service_params, urom_service_h *service)
{
    urom_status_t status;

    status = urom_service_connect(service_params, service);
    if (status != UROM_OK) {
        cl_error(&urom_lib->super, "urom_service_connect() returned error: %s\n",
                urom_status_string(status));
        return -1;
    };

    return 0;
}


static int device_connect(ucc_cl_urom_lib_t *urom_lib, char *dev_name, urom_service_h *service)
{
    urom_service_params_t service_params = {0};
    urom_status_t status;
    urom_device_t *dev;
    struct urom_device *device_list;
    int num_devices;
    int ret;

    status = urom_get_device_list(&device_list, &num_devices);
    if (status != UROM_OK) {
        cl_error(&urom_lib->super, "urom_get_device_list() returned error: %s\n",
                urom_status_string(status));
        return -1;
    };

    dev = device_list;
    while (dev) {
        if (dev_name) {
            if (!strcmp(dev_name, dev->name)) {
                break;
            }
        } else {
            break;
        }
        dev = dev->next;
    }
    if (!dev) {
        cl_error(urom_lib, "No matching device:\n");
        return -1;
    }

    service_params.flags = UROM_SERVICE_PARAM_DEVICE;
    service_params.device = dev;
    ret = service_connect(urom_lib, &service_params, service);
    if (ret) {
        status = urom_free_device_list(device_list);
        return -1;
    }

    status = urom_free_device_list(device_list);
    if (status != UROM_OK) {
        cl_error(&urom_lib->super, "urom_free_device_list() returned error: %s\n",
                urom_status_string(status));
        return -1;
    }

    return 0;
}

/* NOLINTNEXTLINE  TODO params is not used*/
UCC_CLASS_INIT_FUNC(ucc_cl_urom_lib_t, const ucc_base_lib_params_t *params,
                    const ucc_base_config_t *config)
{
    const ucc_cl_urom_lib_config_t *cl_config =
        ucc_derived_of(config, ucc_cl_urom_lib_config_t);
	char * device = NULL;
    int                  ret;

    UCC_CLASS_CALL_SUPER_INIT(ucc_cl_lib_t, &ucc_cl_urom.super, &cl_config->super);
    memcpy(&self->cfg, cl_config, sizeof(*cl_config));

    cl_debug(&self->super, "initialized lib object: %p", self);
    memset(&self->urom_ctx, 0, sizeof(urom_ctx_t));

    /* how to know service here? */
    ret = device_connect(self, device, &self->urom_ctx.urom_service);
    if (ret) {
        cl_error(&self->super, "failed to connect to urom");
        return UCC_ERR_NO_RESOURCE;
    }

    self->urom_ctx.urom_worker_addr = ucc_calloc(1, UROM_WORKER_ADDR_MAX_LEN, "urom worker addr");
    if (!self->urom_ctx.urom_worker_addr) {
        cl_error(&self->super, "failed to allocate %d bytes", UROM_WORKER_ADDR_MAX_LEN);
        return UCC_ERR_NO_MEMORY;
    }

    self->tl_ucp_index = -1;
    return UCC_OK;
}

UCC_CLASS_CLEANUP_FUNC(ucc_cl_urom_lib_t)
{
    urom_status_t urom_status;
    cl_debug(&self->super, "finalizing lib object: %p", self);
    urom_status = urom_worker_disconnect(self->urom_ctx.urom_worker);
    if (urom_status != UROM_OK) {
        cl_error(self, "Failed to disconnect to UROM Worker");
    }

    urom_status = urom_worker_destroy(self->urom_ctx.urom_service, self->urom_ctx.worker_id);
    if (urom_status != UROM_OK) {
        cl_error(self, "Failed to destroy UROM Worker");
    }

    urom_status = urom_service_disconnect(self->urom_ctx.urom_service);
    if (urom_status != UROM_OK) {
        cl_error(self, "Failed to disconnect from UROM service");
    }
}

UCC_CLASS_DEFINE(ucc_cl_urom_lib_t, ucc_cl_lib_t);
static inline ucc_status_t check_tl_lib_attr(const ucc_base_lib_t *lib,
                                             ucc_tl_iface_t *      tl_iface,
                                             ucc_cl_lib_attr_t *   attr)
{
    ucc_tl_lib_attr_t tl_attr;
    ucc_status_t      status;

    memset(&tl_attr, 0, sizeof(tl_attr));
    status = tl_iface->lib.get_attr(NULL, &tl_attr.super);
    if (UCC_OK != status) {
        cl_error(lib, "failed to query tl %s lib attributes",
                 tl_iface->super.name);
        return status;
    }
    attr->super.attr.thread_mode =
        ucc_min(attr->super.attr.thread_mode, tl_attr.super.attr.thread_mode);
    attr->super.attr.coll_types |= tl_attr.super.attr.coll_types;
    attr->super.flags |= tl_attr.super.flags;
    return UCC_OK;
}
ucc_status_t ucc_cl_urom_get_lib_attr(const ucc_base_lib_t *lib,
                                      ucc_base_lib_attr_t  *base_attr)
{
    ucc_cl_lib_attr_t  *attr     = ucc_derived_of(base_attr, ucc_cl_lib_attr_t);
    ucc_cl_urom_lib_t *cl_lib   = ucc_derived_of(lib, ucc_cl_urom_lib_t);
    ucc_config_names_list_t *tls = &cl_lib->super.tls;
    ucc_tl_iface_t          *tl_iface;
    int                      i;
    ucc_status_t             status;

    attr->tls                = &cl_lib->super.tls.array;
    if (cl_lib->super.tls.requested) {
        status = ucc_config_names_array_dup(&cl_lib->super.tls_forced,
                                            &cl_lib->super.tls.array);
        if (UCC_OK != status) {
            return status;
        }
    }
    attr->tls_forced             = &cl_lib->super.tls_forced;
    attr->super.attr.thread_mode = UCC_THREAD_MULTIPLE;
    attr->super.attr.coll_types  = 0;
    attr->super.flags            = 0;

    ucc_assert(tls->array.count >= 1);
    for (i = 0; i < tls->array.count; i++) {
        /* Check TLs provided in CL_BASIC_TLS. Not all of them could be
           available, check for NULL. */
        tl_iface =
            ucc_derived_of(ucc_get_component(&ucc_global_config.tl_framework,
                                             tls->array.names[i]),
                           ucc_tl_iface_t);
        if (!tl_iface) {
            cl_warn(lib, "tl %s is not available", tls->array.names[i]);
            continue;
        }
        if (UCC_OK != (status = check_tl_lib_attr(lib, tl_iface, attr))) {
            return status;
        }
    }
    return UCC_OK;
}

ucc_status_t ucc_cl_urom_get_lib_properties(ucc_base_lib_properties_t *prop)
{
    prop->default_team_size = 2;
    prop->min_team_size     = 2;
    prop->max_team_size     = UCC_RANK_MAX;
    return UCC_OK;
}
