/**
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#ifndef UCC_RESILIENCE_H_
#define UCC_RESILIENCE_H_

#include "ucc/api/ucc.h"
#include "ucc_context.h"
#include <pthread.h>

/* Failure detection method constants */
#define UCC_FAILURE_DETECTION_HYBRID      0  /*!< Try service team first, fallback to sockets */
#define UCC_FAILURE_DETECTION_SOCKETS     1  /*!< Use socket-based detection only */
#define UCC_FAILURE_DETECTION_SERVICE     2  /*!< Use service team-based detection only */

/* Heartbeat server state */
typedef struct ucc_heartbeat_server {
    int                server_socket;
    uint16_t           port;
    pthread_t          thread;
    volatile int       running;
    ucc_context_t     *context;
} ucc_heartbeat_server_t;

/**
 * @brief Start heartbeat server for failure detection
 *
 * @param context       UCC context
 * @param server        Output heartbeat server handle
 *
 * @return UCC_OK on success, error code otherwise
 */
ucc_status_t ucc_heartbeat_server_start(ucc_context_t *context,
                                       ucc_heartbeat_server_t **server);

/**
 * @brief Stop heartbeat server
 *
 * @param server        Heartbeat server handle
 *
 * @return UCC_OK on success, error code otherwise
 */
ucc_status_t ucc_heartbeat_server_stop(ucc_heartbeat_server_t *server);

/**
 * @brief Detect failed processes using socket-based connectivity tests
 *
 * @param context       UCC context containing OOB information
 * @param alive_mask    Output array indicating which processes are alive (1) or failed (0)
 * @param failed_ranks  Output array of failed process ranks
 * @param num_failed    Output number of failed processes
 *
 * @return UCC_OK on success, error code otherwise
 */
ucc_status_t ucc_detect_failed_processes(ucc_context_t *context,
                                        uint64_t **alive_mask,
                                        uint64_t **failed_ranks,
                                        int *num_failed);

/**
 * @brief Detect failed processes using service team-based OOB communication
 *
 * @param context       UCC context with service team
 * @param alive_mask    Output array indicating which processes are alive (1) or failed (0)
 * @param failed_ranks  Output array of failed process ranks
 * @param num_failed    Output number of failed processes
 *
 * @return UCC_OK on success, error code otherwise
 */
ucc_status_t ucc_service_team_failure_detection(ucc_context_t *context,
                                               uint64_t **alive_mask,
                                               uint64_t **failed_ranks,
                                               int *num_failed);

/**
 * @brief Hybrid failure detection: try service team OOB first, fallback to sockets
 *
 * @param context       UCC context
 * @param alive_mask    Output array indicating which processes are alive (1) or failed (0)
 * @param failed_ranks  Output array of failed process ranks
 * @param num_failed    Output number of failed processes
 *
 * @return UCC_OK on success, error code otherwise
 */
ucc_status_t ucc_hybrid_failure_detection(ucc_context_t *context,
                                         uint64_t **alive_mask,
                                         uint64_t **failed_ranks,
                                         int *num_failed);

/**
 * @brief Set the failure detection method for a context
 *
 * @param context       UCC context
 * @param method        Failure detection method (see UCC_FAILURE_DETECTION_* constants)
 *
 * @return UCC_OK on success, error code otherwise
 */
ucc_status_t ucc_set_failure_detection_method(ucc_context_t *context, uint32_t method);

/**
 * @brief Get the current failure detection method for a context
 *
 * @param context       UCC context
 * @param method        Output: current failure detection method
 *
 * @return UCC_OK on success, error code otherwise
 */
ucc_status_t ucc_get_failure_detection_method(ucc_context_t *context, uint32_t *method);

/**
 * @brief Test if a specific process is alive using socket connection
 *
 * @param hostname      Hostname or IP address of target process
 * @param port          Port number of target process heartbeat server
 * @param timeout_sec   Timeout in seconds for connection attempt
 *
 * @return 1 if process is alive, 0 if failed/unreachable
 */
int ucc_test_process_connectivity(const char *hostname, uint16_t port, int timeout_sec);

/**
 * @brief Test if a specific process is alive using process ID (fallback method)
 *
 * @param process_id    Process identifier to test
 * @param timeout_sec   Timeout in seconds for connection attempt
 *
 * @return 1 if process is alive, 0 if failed/unreachable
 */
int ucc_test_process_alive(int process_id, int timeout_sec);

#endif /* UCC_RESILIENCE_H_ */