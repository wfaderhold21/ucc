/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ucc_ec_plugin.h"
#include "utils/ucc_malloc.h"
#include "utils/ucc_log.h"
#include <pthread.h>
#include <string.h>

/* Plugin registry */
static ucc_ec_plugin_entry_t       *ec_plugin_list  = NULL;
static pthread_mutex_t              ec_plugin_mutex = PTHREAD_MUTEX_INITIALIZER;
static ucc_ee_type_t                next_ee_type    = UCC_EE_LAST + 1;

/* External ops arrays from ucc_ec.c - we'll need to extend these */
extern const ucc_ec_ops_t          *ec_ops[];
extern const ucc_ee_executor_ops_t *executor_ops[];

ucc_status_t ucc_ec_plugin_register(const ucc_ec_plugin_desc_t *plugin_desc,
                                    ucc_ee_type_t *ee_type)
{
    ucc_ec_plugin_entry_t *entry;
    ucc_ee_type_t          et;

    if (!plugin_desc || !ee_type) {
        ucc_error("invalid plugin descriptor or ee_type pointer");
        return UCC_ERR_INVALID_PARAM;
    }

    if (!plugin_desc->name) {
        ucc_error("plugin must have a name");
        return UCC_ERR_INVALID_PARAM;
    }

    if (!plugin_desc->ops.create_event || !plugin_desc->ops.destroy_event) {
        ucc_error(
            "plugin must provide create_event and destroy_event operations");
        return UCC_ERR_INVALID_PARAM;
    }

    pthread_mutex_lock(&ec_plugin_mutex);

    /* Check if plugin with same name already exists */
    entry = ec_plugin_list;
    while (entry) {
        if (strcmp(entry->desc.name, plugin_desc->name) == 0) {
            ucc_warn("EC plugin '%s' already registered", plugin_desc->name);
            pthread_mutex_unlock(&ec_plugin_mutex);
            return UCC_ERR_ALREADY_EXISTS;
        }
        entry = entry->next;
    }

    /* Allocate new entry */
    entry = (ucc_ec_plugin_entry_t *)ucc_malloc(
        sizeof(ucc_ec_plugin_entry_t), "ec_plugin_entry");
    if (!entry) {
        ucc_error("failed to allocate memory for EC plugin entry");
        pthread_mutex_unlock(&ec_plugin_mutex);
        return UCC_ERR_NO_MEMORY;
    }

    /* Assign new execution engine type */
    et = next_ee_type++;

    /* Deep copy descriptor */
    memcpy(&entry->desc, plugin_desc, sizeof(ucc_ec_plugin_desc_t));

    /* Duplicate strings */
    if (plugin_desc->name) {
        entry->desc.name = ucc_strdup(plugin_desc->name, "ec_plugin_name");
    }
    if (plugin_desc->version) {
        entry->desc.version = ucc_strdup(
            plugin_desc->version, "ec_plugin_version");
    }

    entry->ee_type = et;
    entry->ref_cnt = 0;

    /* Add to list */
    entry->next    = ec_plugin_list;
    ec_plugin_list = entry;

    *ee_type       = et;

    ucc_info(
        "EC plugin '%s' registered with ee_type=%d", plugin_desc->name, et);

    pthread_mutex_unlock(&ec_plugin_mutex);

    return UCC_OK;
}

ucc_status_t ucc_ec_plugin_unregister(ucc_ee_type_t ee_type)
{
    ucc_ec_plugin_entry_t *entry, *prev;

    if (ee_type < UCC_EE_LAST) {
        ucc_error(
            "cannot unregister builtin execution engine type %d", ee_type);
        return UCC_ERR_INVALID_PARAM;
    }

    pthread_mutex_lock(&ec_plugin_mutex);

    prev  = NULL;
    entry = ec_plugin_list;

    while (entry) {
        if (entry->ee_type == ee_type) {
            if (entry->ref_cnt > 0) {
                ucc_warn(
                    "EC plugin ee_type=%d still has %d references",
                    ee_type,
                    entry->ref_cnt);
                pthread_mutex_unlock(&ec_plugin_mutex);
                return UCC_ERR_IN_PROGRESS;
            }

            /* Remove from list */
            if (prev) {
                prev->next = entry->next;
            } else {
                ec_plugin_list = entry->next;
            }

            /* Free strings */
            if (entry->desc.name) {
                ucc_free((void *)entry->desc.name);
            }
            if (entry->desc.version) {
                ucc_free((void *)entry->desc.version);
            }

            ucc_free(entry);

            ucc_info("EC plugin ee_type=%d unregistered", ee_type);

            pthread_mutex_unlock(&ec_plugin_mutex);
            return UCC_OK;
        }
        prev  = entry;
        entry = entry->next;
    }

    pthread_mutex_unlock(&ec_plugin_mutex);

    ucc_error("EC plugin ee_type=%d not found", ee_type);
    return UCC_ERR_NOT_FOUND;
}

ucc_status_t ucc_ec_plugin_query(
    ucc_ee_type_t ee_type, const ucc_ec_plugin_desc_t **plugin_desc)
{
    ucc_ec_plugin_entry_t *entry;

    if (!plugin_desc) {
        return UCC_ERR_INVALID_PARAM;
    }

    if (ee_type < UCC_EE_LAST) {
        return UCC_ERR_NOT_FOUND;
    }

    pthread_mutex_lock(&ec_plugin_mutex);

    entry = ec_plugin_list;
    while (entry) {
        if (entry->ee_type == ee_type) {
            *plugin_desc = &entry->desc;
            pthread_mutex_unlock(&ec_plugin_mutex);
            return UCC_OK;
        }
        entry = entry->next;
    }

    pthread_mutex_unlock(&ec_plugin_mutex);

    return UCC_ERR_NOT_FOUND;
}

const char *ucc_ec_plugin_get_name(ucc_ee_type_t ee_type)
{
    const ucc_ec_plugin_desc_t *desc;

    if (ucc_ec_plugin_query(ee_type, &desc) == UCC_OK) {
        return desc->name;
    }

    return NULL;
}

int ucc_ec_is_plugin(ucc_ee_type_t ee_type)
{
    return (ee_type >= UCC_EE_LAST) ? 1 : 0;
}

/* Internal helper to get plugin entry by ee_type */
ucc_ec_plugin_entry_t *ucc_ec_plugin_get_entry(ucc_ee_type_t ee_type)
{
    ucc_ec_plugin_entry_t *entry;

    if (ee_type < UCC_EE_LAST) {
        return NULL;
    }

    entry = ec_plugin_list;
    while (entry) {
        if (entry->ee_type == ee_type) {
            return entry;
        }
        entry = entry->next;
    }

    return NULL;
}

