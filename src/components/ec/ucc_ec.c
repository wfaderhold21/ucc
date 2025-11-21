/**
 * Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include <pthread.h>
#include "config.h"
#include "base/ucc_ec_base.h"
#include "ucc_ec.h"
#include "ucc_ec_plugin.h"
#include "core/ucc_global_opts.h"
#include "utils/ucc_malloc.h"
#include "utils/ucc_log.h"

static const ucc_ec_ops_t          *ec_ops[UCC_EE_LAST];
static const ucc_ee_executor_ops_t *executor_ops[UCC_EE_LAST];
static pthread_mutex_t ucc_ec_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Helper functions to get ec_ops and executor_ops including plugins */
static inline const ucc_ec_ops_t   *ucc_ec_get_ops(ucc_ee_type_t ee_type)
{
    if (ee_type < UCC_EE_LAST) {
        return ec_ops[ee_type];
    } else {
        /* Check plugin registry */
        ucc_ec_plugin_entry_t *plugin = ucc_ec_plugin_get_entry(ee_type);
        return plugin ? &plugin->desc.ops : NULL;
    }
}

static inline const ucc_ee_executor_ops_t *ucc_ec_get_executor_ops(
    ucc_ee_type_t ee_type)
{
    if (ee_type < UCC_EE_LAST) {
        return executor_ops[ee_type];
    } else {
        /* Check plugin registry */
        ucc_ec_plugin_entry_t *plugin = ucc_ec_plugin_get_entry(ee_type);
        return plugin ? &plugin->desc.executor_ops : NULL;
    }
}

#define UCC_CHECK_EC_AVAILABLE(ee)                                             \
    do {                                                                       \
        const ucc_ec_ops_t *ops = ucc_ec_get_ops(ee);                          \
        if (NULL == ops) {                                                     \
            if (ee < UCC_EE_LAST) {                                            \
                ucc_error("execution engine type %d not initialized", ee);     \
            } else {                                                           \
                ucc_error("EC plugin with ee_type %d not registered", ee);     \
            }                                                                  \
            return UCC_ERR_NOT_SUPPORTED;                                      \
        }                                                                      \
    } while (0)

ucc_status_t ucc_ec_init(const ucc_ec_params_t *ec_params)
{
    int            i, n_ecs;
    ucc_ec_base_t *ec;
    ucc_status_t   status;
    ucc_ec_attr_t  attr;

    pthread_mutex_lock(&ucc_ec_mutex);
    memset(ec_ops, 0, UCC_EE_LAST * sizeof(ucc_ec_ops_t *));
    n_ecs = ucc_global_config.ec_framework.n_components;
    for (i = 0; i < n_ecs; i++) {
        ec = ucc_derived_of(ucc_global_config.ec_framework.components[i],
                            ucc_ec_base_t);
        if (ec->ref_cnt == 0) {
            ec->config = ucc_malloc(ec->config_table.size);
            if (!ec->config) {
                ucc_error("failed to allocate %zd bytes for ec config",
                          ec->config_table.size);
                continue;
            }
            status = ucc_config_parser_fill_opts(
                ec->config, &ec->config_table, "UCC_", 1);
            if (UCC_OK != status) {
                ucc_debug("failed to parse config for EC component: %s (%d)",
                          ec->super.name, status);
                ucc_free(ec->config);
                continue;
            }
            status = ec->init(ec_params);
            if (UCC_OK != status) {
                ucc_debug("ec_init failed for component: %s, skipping (%d)",
                          ec->super.name, status);
                ucc_config_parser_release_opts(ec->config,
                                               ec->config_table.table);
                ucc_free(ec->config);
                continue;
            }
            ucc_debug("ec %s initialized", ec->super.name);
        } else {
            attr.field_mask = UCC_EC_ATTR_FIELD_THREAD_MODE;
            status = ec->get_attr(&attr);
            if (status != UCC_OK) {
                pthread_mutex_unlock(&ucc_ec_mutex);
                return status;
            }
            if (attr.thread_mode < ec_params->thread_mode) {
                ucc_info("ec %s was allready initilized with "
                         "different thread mode: current tm %d, provided tm %d",
                         ec->super.name, attr.thread_mode,
                         ec_params->thread_mode);
            }
        }
        ec->ref_cnt++;
        ec_ops[ec->type] = &ec->ops;
        executor_ops[ec->type] = &ec->executor_ops;
    }
    pthread_mutex_unlock(&ucc_ec_mutex);

    return UCC_OK;
}

ucc_status_t ucc_ec_available(ucc_ee_type_t ee_type)
{
    const ucc_ec_ops_t *ops = ucc_ec_get_ops(ee_type);
    
    if (NULL == ops) {
        return UCC_ERR_NOT_FOUND;
    }

    return UCC_OK;
}

ucc_status_t ucc_ec_get_attr(ucc_ec_attr_t *attr)
{
    if (attr->field_mask & UCC_EC_ATTR_FILED_MAX_EXECUTORS_BUFS) {
        attr->max_ee_bufs = UCC_EE_EXECUTOR_NUM_BUFS;
    }

    return UCC_OK;
}

ucc_status_t ucc_ec_finalize()
{
    ucc_ee_type_t  et;
    ucc_ec_base_t *ec;

    pthread_mutex_lock(&ucc_ec_mutex);
    for (et = UCC_EE_FIRST; et < UCC_EE_LAST; et++) {
        if (NULL != ec_ops[et]) {
            ec = ucc_container_of(ec_ops[et], ucc_ec_base_t, ops);
            ec->ref_cnt--;
            if (ec->ref_cnt == 0) {
                ec->finalize();
                ucc_config_parser_release_opts(ec->config,
                                               ec->config_table.table);
                ucc_free(ec->config);
                ec_ops[et] = NULL;
            }
        }
    }
    pthread_mutex_unlock(&ucc_ec_mutex);

    return UCC_OK;
}

ucc_status_t ucc_ec_create_event(void **event, ucc_ee_type_t ee_type)
{
    const ucc_ec_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(ee_type);
    ops = ucc_ec_get_ops(ee_type);
    return ops->create_event(event);
}

ucc_status_t ucc_ec_destroy_event(void *event, ucc_ee_type_t ee_type)
{
    const ucc_ec_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(ee_type);
    ops = ucc_ec_get_ops(ee_type);
    return ops->destroy_event(event);
}

ucc_status_t ucc_ec_event_post(void *ee_context, void *event,
                               ucc_ee_type_t ee_type)
{
    const ucc_ec_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(ee_type);
    ops = ucc_ec_get_ops(ee_type);
    return ops->event_post(ee_context, event);
}

ucc_status_t ucc_ec_event_test(void *event, ucc_ee_type_t ee_type)
{
    const ucc_ec_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(ee_type);
    ops = ucc_ec_get_ops(ee_type);
    return ops->event_test(event);
}

ucc_status_t ucc_ee_executor_init(const ucc_ee_executor_params_t *params,
                                  ucc_ee_executor_t **executor)
{
    const ucc_ee_executor_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(params->ee_type);
    ops = ucc_ec_get_executor_ops(params->ee_type);
    return ops->init(params, executor);
}

ucc_status_t ucc_ee_executor_status(const ucc_ee_executor_t *executor)
{
    const ucc_ee_executor_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(executor->ee_type);
    ops = ucc_ec_get_executor_ops(executor->ee_type);
    return ops->status(executor);
}

ucc_status_t ucc_ee_executor_start(ucc_ee_executor_t *executor,
                                   void *ee_context)
{
    const ucc_ee_executor_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(executor->ee_type);
    ops = ucc_ec_get_executor_ops(executor->ee_type);
    return ops->start(executor, ee_context);
}

ucc_status_t ucc_ee_executor_stop(ucc_ee_executor_t *executor)
{
    const ucc_ee_executor_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(executor->ee_type);
    ops = ucc_ec_get_executor_ops(executor->ee_type);
    return ops->stop(executor);
}

ucc_status_t ucc_ee_executor_finalize(ucc_ee_executor_t *executor)
{
    const ucc_ee_executor_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(executor->ee_type);
    ops = ucc_ec_get_executor_ops(executor->ee_type);
    return ops->finalize(executor);
}

ucc_status_t ucc_ee_executor_task_post(ucc_ee_executor_t *executor,
                                       const ucc_ee_executor_task_args_t *task_args,
                                       ucc_ee_executor_task_t **task)
{
    const ucc_ee_executor_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(executor->ee_type);
    ops = ucc_ec_get_executor_ops(executor->ee_type);
    return ops->task_post(executor, task_args, task);
}

ucc_status_t ucc_ee_executor_task_test(const ucc_ee_executor_task_t *task)
{
    const ucc_ee_executor_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(task->eee->ee_type);
    ops = ucc_ec_get_executor_ops(task->eee->ee_type);
    return ops->task_test(task);
}

ucc_status_t ucc_ee_executor_task_finalize(ucc_ee_executor_task_t *task)
{
    const ucc_ee_executor_ops_t *ops;
    UCC_CHECK_EC_AVAILABLE(task->eee->ee_type);
    ops = ucc_ec_get_executor_ops(task->eee->ee_type);
    return ops->task_finalize(task);
}
