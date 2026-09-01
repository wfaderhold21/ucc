/**
 * Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "ec_cpu_thread_pool.h"
#include "components/ec/ucc_ec_log.h"

typedef struct ucc_ec_cpu_pool_node {
    ucc_lf_queue_elem_t lf_elem; /* must be first: enqueued into the lf-queue */
    ucc_ee_executor_task_t *task;
} ucc_ec_cpu_pool_node_t;

static void ucc_ec_cpu_pool_node_obj_init(ucc_mpool_t *mp, void *obj,
                                          void *chunk)
{
    ucc_ec_cpu_pool_node_t *node = (ucc_ec_cpu_pool_node_t *)obj;

    ucc_lf_queue_init_elem(&node->lf_elem);
    node->task = NULL;
}

static ucc_mpool_ops_t ucc_ec_cpu_pool_node_ops = {
    .chunk_alloc   = ucc_mpool_hugetlb_malloc,
    .chunk_release = ucc_mpool_hugetlb_free,
    .obj_init      = ucc_ec_cpu_pool_node_obj_init,
    .obj_cleanup   = NULL,
};

ucc_status_t ucc_ec_cpu_thread_pool_init(ucc_ec_cpu_thread_pool_t *pool,
                                         int max_tasks)
{
    ucc_status_t status;

    ucc_lf_queue_init(&pool->queue);

    /* Pool nodes carry executor tasks; a node is allocated per in-flight
     * task and released when the worker has dequeued it. */
    status = ucc_mpool_init(&pool->pool_nodes, 0, sizeof(ucc_ec_cpu_pool_node_t),
                            0, UCC_CACHE_LINE_SIZE, 16, max_tasks,
                            &ucc_ec_cpu_pool_node_ops, UCC_THREAD_MULTIPLE,
                            "ec cpu pool nodes");
    return status;
}

void ucc_ec_cpu_thread_pool_finalize(ucc_ec_cpu_thread_pool_t *pool)
{
    /* The pool owner is responsible for draining the queue before this. */
    ucc_lf_queue_destroy(&pool->queue);
    ucc_mpool_cleanup(&pool->pool_nodes, 1);
}

ucc_status_t ucc_ec_cpu_thread_pool_enqueue(ucc_ec_cpu_thread_pool_t *pool,
                                            ucc_ee_executor_task_t *task)
{
    ucc_ec_cpu_pool_node_t *node = ucc_mpool_get(&pool->pool_nodes);

    if (ucc_unlikely(!node)) {
        return UCC_ERR_NO_MEMORY;
    }
    node->task = task;
    ucc_lf_queue_enqueue(&pool->queue, &node->lf_elem);
    return UCC_OK;
}

ucc_ee_executor_task_t *
ucc_ec_cpu_thread_pool_dequeue(ucc_ec_cpu_thread_pool_t *pool)
{
    ucc_lf_queue_elem_t *elem;
    ucc_ec_cpu_pool_node_t *node;
    ucc_ee_executor_task_t *task;

    /*
     * The lf-queue is multi-consumer safe: the fast-path slots are claimed
     * via CAS and the overflow lists are guarded by per-pool locks, so any
     * pool worker may dequeue concurrently.
     */
    elem = ucc_lf_queue_dequeue(&pool->queue, 1);
    if (!elem) {
        return NULL;
    }
    node = ucc_container_of(elem, ucc_ec_cpu_pool_node_t, lf_elem);
    task = node->task;
    ucc_mpool_put(node);
    return task;
}
