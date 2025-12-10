/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_EC_PLUGIN_H_
#define UCC_EC_PLUGIN_H_

#include "ucc/api/ucc.h"
#include "base/ucc_ec_base.h"

/**
 * EC Plugin descriptor for programmatic registration
 */
typedef struct ucc_ec_plugin_desc {
    const char           *name;         /**< Plugin name */
    const char           *version;      /**< Plugin version string */
    ucc_ec_ops_t          ops;          /**< Execution context operations */
    ucc_ee_executor_ops_t executor_ops; /**< Executor operations */
} ucc_ec_plugin_desc_t;

/**
 * Internal plugin registry entry
 */
typedef struct ucc_ec_plugin_entry {
    ucc_ec_plugin_desc_t        desc;
    ucc_ee_type_t               ee_type; /* Dynamically assigned */
    uint32_t                    ref_cnt;
    struct ucc_ec_plugin_entry *next;
} ucc_ec_plugin_entry_t;

/* Note: ucc_ec_plugin_register() and ucc_ec_plugin_unregister()
 * are declared in ucc/api/ucc.h as public API */

/**
 * Query plugin information by execution engine type
 *
 * @param [in]  ee_type      Execution engine type to query
 * @param [out] plugin_desc  Plugin descriptor (read-only)
 *
 * @return UCC_OK on success, UCC_ERR_NOT_FOUND if not a plugin
 */
ucc_status_t ucc_ec_plugin_query(
    ucc_ee_type_t ee_type, const ucc_ec_plugin_desc_t **plugin_desc);

/**
 * Get plugin name by execution engine type
 *
 * @param [in] ee_type  Execution engine type
 *
 * @return Plugin name or NULL if not found
 */
const char            *ucc_ec_plugin_get_name(ucc_ee_type_t ee_type);

/**
 * Check if execution engine type is a plugin
 *
 * @param [in] ee_type  Execution engine type to check
 *
 * @return 1 if plugin, 0 if builtin
 */
int                    ucc_ec_is_plugin(ucc_ee_type_t ee_type);

/**
 * Get plugin entry by execution engine type (internal use)
 *
 * @param [in] ee_type  Execution engine type
 *
 * @return Plugin entry or NULL if not found
 */
ucc_ec_plugin_entry_t *ucc_ec_plugin_get_entry(ucc_ee_type_t ee_type);

#endif /* UCC_EC_PLUGIN_H_ */

