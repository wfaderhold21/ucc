/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ucc_mc_plugin.h"
#include "utils/ucc_malloc.h"
#include "utils/ucc_log.h"
#include <stdlib.h>
#include <string.h>

/* Plugin registry - only modified during initialization (single-threaded),
 * read-only afterwards. No locking needed. */
static ucc_mc_plugin_entry_t *mc_plugin_list   = NULL;
static ucc_memory_type_t      next_memory_type = UCC_MEMORY_TYPE_LAST + 1;

/* External ops array from ucc_mc.c - we'll need to extend this */
extern const ucc_mc_ops_t    *mc_ops[];

ucc_status_t ucc_mc_plugin_register(const ucc_mc_plugin_desc_t *plugin_desc,
                                    ucc_memory_type_t *memory_type)
{
    ucc_mc_plugin_entry_t *entry;
    ucc_memory_type_t      mt;

    if (!plugin_desc || !memory_type) {
        ucc_error("invalid plugin descriptor or memory_type pointer");
        return UCC_ERR_INVALID_PARAM;
    }

    if (!plugin_desc->name) {
        ucc_error("plugin must have a name");
        return UCC_ERR_INVALID_PARAM;
    }

    if (!plugin_desc->ops.mem_alloc || !plugin_desc->ops.mem_free) {
        ucc_error("plugin must provide mem_alloc and mem_free operations");
        return UCC_ERR_INVALID_PARAM;
    }

    /* Check if plugin with same name already exists */
    entry = mc_plugin_list;
    while (entry) {
        if (strcmp(entry->desc.name, plugin_desc->name) == 0) {
            ucc_warn("MC plugin '%s' already registered", plugin_desc->name);
            return UCC_ERR_NO_RESOURCE;
        }
        entry = entry->next;
    }

    /* Allocate new entry */
    entry = (ucc_mc_plugin_entry_t *)ucc_malloc(
        sizeof(ucc_mc_plugin_entry_t), "mc_plugin_entry");
    if (!entry) {
        ucc_error("failed to allocate memory for MC plugin entry");
        return UCC_ERR_NO_MEMORY;
    }

    /* Assign new memory type */
    mt = next_memory_type++;

    /* Deep copy descriptor */
    memcpy(&entry->desc, plugin_desc, sizeof(ucc_mc_plugin_desc_t));

    /* Duplicate strings */
    if (plugin_desc->name) {
        entry->desc.name = strdup(plugin_desc->name);
    }
    if (plugin_desc->version) {
        entry->desc.version = strdup(plugin_desc->version);
    }

    entry->memory_type       = mt;
    entry->ref_cnt           = 0;
    entry->is_autodiscovered = 0; /* Programmatic registration */

    /* Add to list */
    entry->next              = mc_plugin_list;
    mc_plugin_list           = entry;
    *memory_type             = mt;

    ucc_info(
        "MC plugin '%s' registered with memory_type=%d", plugin_desc->name, mt);

    return UCC_OK;
}

/**
 * Register an autodiscovered plugin component (loaded from .so)
 * This is called during ucc_mc_init() for components loaded from plugin path
 */
ucc_status_t ucc_mc_plugin_register_autodiscovered(
    ucc_mc_base_t *mc, ucc_memory_type_t *memory_type)
{
    ucc_mc_plugin_entry_t *entry;
    ucc_memory_type_t      mt;

    if (!mc || !memory_type) {
        ucc_error("invalid parameters for autodiscovered plugin registration");
        return UCC_ERR_INVALID_PARAM;
    }

    if (!mc->ops.mem_alloc || !mc->ops.mem_free) {
        ucc_error("plugin must provide mem_alloc and mem_free operations");
        return UCC_ERR_INVALID_PARAM;
    }

    /* Check if plugin with same name already exists */
    entry = mc_plugin_list;
    while (entry) {
        if (strcmp(entry->mc->super.name, mc->super.name) == 0) {
            ucc_warn("MC plugin '%s' already registered", mc->super.name);
            return UCC_ERR_NO_RESOURCE;
        }
        entry = entry->next;
    }

    /* Allocate new entry */
    entry = (ucc_mc_plugin_entry_t *)ucc_malloc(
        sizeof(ucc_mc_plugin_entry_t), "mc_plugin_entry");
    if (!entry) {
        ucc_error("failed to allocate memory for MC plugin entry");
        return UCC_ERR_NO_MEMORY;
    }

    /* Assign new memory type */
    mt                       = next_memory_type++;

    /* Store reference to the loaded component */
    entry->mc                = mc;
    entry->memory_type       = mt;
    entry->ref_cnt           = 0;
    entry->is_autodiscovered = 1; /* Loaded from .so file */

    /* Add to list */
    entry->next              = mc_plugin_list;
    mc_plugin_list           = entry;
    *memory_type             = mt;

    return UCC_OK;
}

ucc_status_t ucc_mc_plugin_unregister(ucc_memory_type_t memory_type)
{
    ucc_mc_plugin_entry_t *entry, *prev;

    if (memory_type < UCC_MEMORY_TYPE_LAST) {
        ucc_error("cannot unregister builtin memory type %d", memory_type);
        return UCC_ERR_INVALID_PARAM;
    }

    prev  = NULL;
    entry = mc_plugin_list;

    while (entry) {
        if (entry->memory_type == memory_type) {
            if (entry->ref_cnt > 0) {
                ucc_warn(
                    "MC plugin memory_type=%d still has %d references",
                    memory_type,
                    entry->ref_cnt);
                return UCC_ERR_NO_RESOURCE;
            }

            /* Remove from list */
            if (prev) {
                prev->next = entry->next;
            } else {
                mc_plugin_list = entry->next;
            }

            /* Free strings (allocated with strdup) */
            if (entry->desc.name) {
                free((void *)entry->desc.name);
            }
            if (entry->desc.version) {
                free((void *)entry->desc.version);
            }

            ucc_free(entry);

            ucc_info("MC plugin memory_type=%d unregistered", memory_type);

            return UCC_OK;
        }
        prev  = entry;
        entry = entry->next;
    }

    ucc_error("MC plugin memory_type=%d not found", memory_type);
    return UCC_ERR_NOT_FOUND;
}

ucc_status_t ucc_mc_plugin_query(
    ucc_memory_type_t memory_type, const ucc_mc_plugin_desc_t **plugin_desc)
{
    ucc_mc_plugin_entry_t *entry;

    if (!plugin_desc) {
        return UCC_ERR_INVALID_PARAM;
    }

    if (memory_type < UCC_MEMORY_TYPE_LAST) {
        return UCC_ERR_NOT_FOUND;
    }

    entry = mc_plugin_list;
    while (entry) {
        if (entry->memory_type == memory_type) {
            *plugin_desc = &entry->desc;
            return UCC_OK;
        }
        entry = entry->next;
    }

    return UCC_ERR_NOT_FOUND;
}

/* Get operations table from plugin entry */
const ucc_mc_ops_t *ucc_mc_plugin_get_ops(ucc_mc_plugin_entry_t *entry)
{
    if (!entry) {
        return NULL;
    }

    if (entry->is_autodiscovered) {
        /* Component loaded from .so file */
        return &entry->mc->ops;
    } else {
        /* Programmatically registered plugin */
        return &entry->desc.ops;
    }
}

const char *ucc_mc_plugin_get_name(ucc_memory_type_t memory_type)
{
    ucc_mc_plugin_entry_t *entry;

    entry = ucc_mc_plugin_get_entry(memory_type);
    if (!entry) {
        return NULL;
    }

    if (entry->is_autodiscovered) {
        return entry->mc->super.name;
    } else {
        return entry->desc.name;
    }
}

int ucc_mc_is_plugin(ucc_memory_type_t memory_type)
{
    return (memory_type >= UCC_MEMORY_TYPE_LAST) ? 1 : 0;
}

/* Internal helper to get plugin entry by memory type */
ucc_mc_plugin_entry_t *ucc_mc_plugin_get_entry(ucc_memory_type_t memory_type)
{
    ucc_mc_plugin_entry_t *entry;

    if (memory_type < UCC_MEMORY_TYPE_LAST) {
        return NULL;
    }

    entry = mc_plugin_list;
    while (entry) {
        if (entry->memory_type == memory_type) {
            return entry;
        }
        entry = entry->next;
    }

    return NULL;
}

ucc_status_t ucc_mc_plugin_iterate(ucc_mc_plugin_iter_cb_t callback, void *ctx)
{
    ucc_mc_plugin_entry_t *entry;
    ucc_status_t           status;

    if (!callback) {
        return UCC_ERR_INVALID_PARAM;
    }

    entry = mc_plugin_list;
    while (entry) {
        status = callback(entry, ctx);
        if (status != UCC_OK) {
            return status;
        }
        entry = entry->next;
    }

    return UCC_OK;
}

