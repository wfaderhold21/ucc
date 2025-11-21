/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_MC_PLUGIN_H_
#define UCC_MC_PLUGIN_H_

#include "base/ucc_mc_base.h"

/* Note: ucc_mc_plugin_desc_t is defined in ucc/api/ucc.h for public API */

/**
 * Internal plugin registry entry
 */
typedef struct ucc_mc_plugin_entry {
    union {
        ucc_mc_plugin_desc_t desc; /* For programmatic registration */
        ucc_mc_base_t       *mc;   /* For autodiscovered plugins (.so) */
    };
    ucc_memory_type_t           memory_type; /* Dynamically assigned */
    uint32_t                    ref_cnt;
    int                         is_autodiscovered; /* 1 if loaded from .so */
    struct ucc_mc_plugin_entry *next;
} ucc_mc_plugin_entry_t;

/* Note: ucc_mc_plugin_register() and ucc_mc_plugin_unregister()
 * are internal functions for plugin management */

/**
 * Register an autodiscovered plugin component (internal use)
 * Called during ucc_mc_init() for components loaded from .so files
 *
 * @param [in]  mc          Loaded component
 * @param [out] memory_type Assigned memory type
 *
 * @return UCC_OK on success
 */
ucc_status_t ucc_mc_plugin_register_autodiscovered(
    ucc_mc_base_t *mc, ucc_memory_type_t *memory_type);

/**
 * Query plugin information by memory type
 *
 * @param [in]  memory_type  Memory type to query
 * @param [out] plugin_desc  Plugin descriptor (read-only)
 *
 * @return UCC_OK on success, UCC_ERR_NOT_FOUND if not a plugin
 */
ucc_status_t ucc_mc_plugin_query(
    ucc_memory_type_t memory_type, const ucc_mc_plugin_desc_t **plugin_desc);

/**
 * Get plugin name by memory type
 *
 * @param [in] memory_type  Memory type
 *
 * @return Plugin name or NULL if not found
 */
const char            *ucc_mc_plugin_get_name(ucc_memory_type_t memory_type);

/**
 * Check if memory type is a plugin
 *
 * @param [in] memory_type  Memory type to check
 *
 * @return 1 if plugin, 0 if builtin
 */
int                    ucc_mc_is_plugin(ucc_memory_type_t memory_type);

/**
 * Get plugin entry by memory type (internal use)
 *
 * @param [in] memory_type  Memory type
 *
 * @return Plugin entry or NULL if not found
 */
ucc_mc_plugin_entry_t *ucc_mc_plugin_get_entry(ucc_memory_type_t memory_type);

/**
 * Get operations table from plugin entry (internal use)
 *
 * @param [in] entry  Plugin entry
 *
 * @return Operations table or NULL
 */
const ucc_mc_ops_t    *ucc_mc_plugin_get_ops(ucc_mc_plugin_entry_t *entry);

/**
 * Iterate through all registered plugins with a callback function
 *
 * @param [in] callback  Callback function called for each plugin
 * @param [in] ctx       User context passed to callback
 *
 * @return UCC_OK on success
 */
typedef ucc_status_t (*ucc_mc_plugin_iter_cb_t)(
    ucc_mc_plugin_entry_t *plugin, void *ctx);
ucc_status_t ucc_mc_plugin_iterate(ucc_mc_plugin_iter_cb_t callback, void *ctx);

#endif /* UCC_MC_PLUGIN_H_ */

