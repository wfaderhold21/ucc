/**
 * Copyright (c) 2020, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_CL_UROM_H_
#define UCC_CL_UROM_H_
#include "components/cl/ucc_cl.h"
#include "components/cl/ucc_cl_log.h"
#include "components/tl/ucc_tl.h"
#include "coll_score/ucc_coll_score.h"
#include "utils/ucc_mpool.h"

#include <urom/api/urom.h>
#include <urom/api/urom_ucc.h>

#ifndef UCC_CL_UROM_DEFAULT_SCORE
#define UCC_CL_UROM_DEFAULT_SCORE 20
#endif

typedef struct ucc_cl_urom_iface {
    ucc_cl_iface_t super;
} ucc_cl_urom_iface_t;
/* Extern iface should follow the pattern: ucc_cl_<cl_name> */
extern ucc_cl_urom_iface_t ucc_cl_urom;

typedef struct pass_info {
//    size_t src_size;
//    size_t dst_size;
    uint64_t xgvmi_flag;
    size_t src_xgvmi_size;
    size_t dst_xgvmi_size;
//    uint64_t src_addr;
//    uint64_t dst_addr;
    char rkeys[];
} pass_info_t;

typedef struct mem_cache {
    void *base_src_va;
    size_t src_va_len;
    void *base_dst_va;
    size_t dst_va_len;
    ucp_mem_h src_memh;
    ucp_mem_h dst_memh;
    void *src_xgvmi_memh;
    void *dst_xgvmi_memh;
    size_t src_packed_xgvmi_len;
    size_t dst_packed_xgvmi_len;
    void *packed_src_key;
    void *packed_dst_key;
    size_t packed_src_key_len;
    size_t packed_dst_key_len;
    pass_info_t *pass_info;
    size_t pass_info_size;
} mem_cache_t;

typedef struct ucc_cl_urom_lib_config {
    ucc_cl_lib_config_t super;
    /*
     * FIXME:
     * what do we need:
     *  buffer size
     *  number of buffers
     */
    uint32_t num_buffers;
    uint32_t xgvmi_buffer_size;
    uint32_t use_xgvmi;
    uint32_t use_xgvmi_cache;
} ucc_cl_urom_lib_config_t;

typedef struct ucc_cl_urom_context_config {
    ucc_cl_context_config_t super;
} ucc_cl_urom_context_config_t;

typedef struct urom_ctx {
    urom_service_h urom_service;
    urom_worker_h  urom_worker;
    void          *urom_worker_addr;
    size_t         urom_worker_len;
    uint64_t       worker_id;
    int            pass_dc_exist;
} urom_ctx_t;

typedef struct xgvmi_info {
    ucp_mem_h xgvmi_memh;
    void     *packed_mkey;
    uint64_t  packed_mkey_len;
    void     *packed_xgvmi_memh;
    uint64_t  packed_xgvmi_len;
    void     *xgvmi_buffer;
    size_t    xgvmi_size;
} xgvmi_info_t;

typedef struct ucc_cl_urom_lib {
    ucc_cl_lib_t             super;
    ucc_cl_urom_lib_config_t cfg;
    urom_ctx_t               urom_ctx;
    int                      tl_ucp_index; //FIXME: make this better
} ucc_cl_urom_lib_t;
UCC_CLASS_DECLARE(ucc_cl_urom_lib_t, const ucc_base_lib_params_t *,
                  const ucc_base_config_t *);

typedef struct ucc_cl_urom_context {
    ucc_cl_context_t super;
    urom_domain_h    urom_domain;
    void            *urom_ucc_ctx_h;
    ucc_mpool_t      sched_mp;
    xgvmi_info_t     xgvmi;
    int              xgvmi_enabled;
    int              req_mc;
    void            *old_dest;
    void            *old_src;
    ucc_rank_t       ctx_rank;     //FIXME: this is not right
} ucc_cl_urom_context_t;
UCC_CLASS_DECLARE(ucc_cl_urom_context_t, const ucc_base_context_params_t *,
                  const ucc_base_config_t *);

typedef struct ucc_cl_urom_team {
    ucc_cl_team_t     super;
    int               team_posted;
    ucc_team_h      **teams;
    unsigned          n_teams;
    mem_cache_t       cache_el[32]; //FIXME: make a linked list
    ucc_coll_score_t *score;
    ucc_score_map_t  *score_map;
} ucc_cl_urom_team_t;
UCC_CLASS_DECLARE(ucc_cl_urom_team_t, ucc_base_context_t *,
                  const ucc_base_team_params_t *);

ucc_status_t ucc_cl_urom_coll_init(ucc_base_coll_args_t *coll_args,
                                   ucc_base_team_t      *team,
                                   ucc_coll_task_t     **task);

#define UCC_CL_UROM_TEAM_CTX(_team)                                            \
    (ucc_derived_of((_team)->super.super.context, ucc_cl_urom_context_t))

#endif
