/**
 * Copyright (c) 2020-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 */

#ifndef UCC_MC_H_
#define UCC_MC_H_

#include "ucc/api/ucc.h"
#include "components/mc/base/ucc_mc_base.h"
#include "core/ucc_dt.h"
#include "utils/ucc_math.h"

ucc_status_t ucc_mc_init(const ucc_mc_params_t *mc_params);

ucc_status_t ucc_mc_finalize();

ucc_status_t ucc_mc_available(ucc_memory_type_t mem_type);

/**
 * Query for memory attributes.
 * @param [in]        ptr       Memory pointer to query.
 * @param [in,out]    mem_attr  Memory attributes.
 */
ucc_status_t ucc_mc_get_mem_attr(const void *ptr, ucc_mem_attr_t *mem_attr);

ucc_status_t ucc_mc_get_attr(ucc_mc_attr_t *attr, ucc_memory_type_t mem_type);

ucc_status_t ucc_mc_alloc(ucc_mc_buffer_header_t **h_ptr, size_t len,
                          ucc_memory_type_t mem_type);

ucc_status_t ucc_mc_free(ucc_mc_buffer_header_t *h_ptr);

ucc_status_t ucc_mc_flush(ucc_memory_type_t mem_type);

ucc_status_t ucc_mc_memcpy(void *dst, const void *src, size_t len,
                           ucc_memory_type_t dst_mem,
                           ucc_memory_type_t src_mem);

ucc_status_t ucc_mc_memset(void *ptr, int value, size_t size,
                           ucc_memory_type_t mem_type);

/**
 * Get the corresponding execution engine type for a memory type.
 * For built-in types, returns the standard EE type.
 * For plugin types, queries the plugin for its preferred EE type.
 *
 * @param [in]  mem_type  Memory type
 * @param [out] ee_type   Corresponding execution engine type
 *
 * @return UCC_OK if found, UCC_ERR_NOT_FOUND if no EC available
 */
ucc_status_t ucc_mc_get_execution_engine_type(ucc_memory_type_t mem_type,
                                              ucc_ee_type_t *ee_type);

#endif
