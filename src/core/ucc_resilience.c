/**
 * Copyright (c) 2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "config.h"
#include "ucc_resilience.h"
#include "utils/ucc_malloc.h"
#include "utils/ucc_log.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>
#include <time.h> /* For clock_gettime */
#include <pthread.h>
#include <arpa/inet.h>

#define UCC_HEARTBEAT_BASE_PORT 45000

/* Heartbeat server thread function */
static void* ucc_heartbeat_server_thread(void* arg)
{
    ucc_heartbeat_server_t *server = (ucc_heartbeat_server_t*)arg;
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int client_sock;
    char response = 'A'; /* 'A' for Alive */
    
    ucc_debug("heartbeat server started on port %d", server->port);
    
    while (server->running) {
        /* Accept incoming connections with timeout */
        fd_set readfds;
        struct timeval timeout;
        
        FD_ZERO(&readfds);
        FD_SET(server->server_socket, &readfds);
        timeout.tv_sec = 1;  /* 1 second timeout */
        timeout.tv_usec = 0;
        
        int activity = select(server->server_socket + 1, &readfds, NULL, NULL, &timeout);
        
        if (activity < 0) {
            if (errno != EINTR) {
                ucc_debug("heartbeat server select error: %s", strerror(errno));
            }
            continue;
        }
        
        if (activity == 0) {
            /* Timeout - continue loop to check running flag */
            continue;
        }
        
        if (FD_ISSET(server->server_socket, &readfds)) {
            client_sock = accept(server->server_socket, 
                               (struct sockaddr*)&client_addr, &client_len);
            if (client_sock < 0) {
                if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    ucc_debug("heartbeat server accept error: %s", strerror(errno));
                }
                continue;
            }
            
            /* Send alive response and close connection */
            send(client_sock, &response, 1, 0);
            close(client_sock);
            
            ucc_debug("heartbeat response sent to %s", 
                     inet_ntoa(client_addr.sin_addr));
        }
    }
    
    ucc_debug("heartbeat server thread exiting");
    return NULL;
}

ucc_status_t ucc_heartbeat_server_start(ucc_context_t *context,
                                       ucc_heartbeat_server_t **server)
{
    ucc_heartbeat_server_t *srv;
    struct sockaddr_in server_addr;
    int opt = 1;
    uint16_t port;
    
    if (!context || !server) {
        return UCC_ERR_INVALID_PARAM;
    }
    
    srv = ucc_malloc(sizeof(*srv), "heartbeat_server");
    if (!srv) {
        ucc_error("failed to allocate heartbeat server");
        return UCC_ERR_NO_MEMORY;
    }
    
    /* Calculate port based on process rank */
    if (context->params.mask & UCC_CONTEXT_PARAM_FIELD_OOB) {
        port = UCC_HEARTBEAT_BASE_PORT + context->params.oob.oob_ep;
    } else {
        port = UCC_HEARTBEAT_BASE_PORT + (getpid() % 1000);
    }
    
    /* Create server socket */
    srv->server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (srv->server_socket < 0) {
        ucc_error("failed to create heartbeat server socket: %s", strerror(errno));
        ucc_free(srv);
        return UCC_ERR_NO_RESOURCE;
    }
    
    /* Set socket options */
    if (setsockopt(srv->server_socket, SOL_SOCKET, SO_REUSEADDR, 
                   &opt, sizeof(opt)) < 0) {
        ucc_debug("failed to set SO_REUSEADDR: %s", strerror(errno));
    }
    
    /* Bind to port */
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);
    
    if (bind(srv->server_socket, (struct sockaddr*)&server_addr, 
             sizeof(server_addr)) < 0) {
        ucc_error("failed to bind heartbeat server to port %d: %s", 
                 port, strerror(errno));
        close(srv->server_socket);
        ucc_free(srv);
        return UCC_ERR_NO_RESOURCE;
    }
    
    /* Listen for connections */
    if (listen(srv->server_socket, 10) < 0) {
        ucc_error("failed to listen on heartbeat server: %s", strerror(errno));
        close(srv->server_socket);
        ucc_free(srv);
        return UCC_ERR_NO_RESOURCE;
    }
    
    /* Initialize server state */
    srv->port = port;
    srv->running = 1;
    srv->context = context;
    
    /* Start server thread */
    if (pthread_create(&srv->thread, NULL, ucc_heartbeat_server_thread, srv) != 0) {
        ucc_error("failed to create heartbeat server thread: %s", strerror(errno));
        close(srv->server_socket);
        ucc_free(srv);
        return UCC_ERR_NO_RESOURCE;
    }
    
    ucc_info("heartbeat server started on port %d", port);
    *server = srv;
    return UCC_OK;
}

ucc_status_t ucc_heartbeat_server_stop(ucc_heartbeat_server_t *server)
{
    if (!server) {
        return UCC_ERR_INVALID_PARAM;
    }
    
    /* Signal server thread to stop */
    server->running = 0;
    
    /* Wait for thread to finish */
    pthread_join(server->thread, NULL);
    
    /* Close server socket */
    close(server->server_socket);
    
    ucc_info("heartbeat server on port %d stopped", server->port);
    
    /* Free server structure */
    ucc_free(server);
    
    return UCC_OK;
}

int ucc_test_process_connectivity(const char *hostname, uint16_t port, int timeout_sec)
{
    int sock;
    struct sockaddr_in addr;
    struct timeval timeout;
    char response;
    int result = 0;
    
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        ucc_debug("failed to create socket: %s", strerror(errno));
        return 0;
    }
    
    /* Set socket timeout for connection attempt */
    timeout.tv_sec = timeout_sec;
    timeout.tv_usec = 0;
    
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        ucc_debug("failed to set socket timeout: %s", strerror(errno));
        close(sock);
        return 0;
    }
    
    if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
        ucc_debug("failed to set socket send timeout: %s", strerror(errno));
        close(sock);
        return 0;
    }
    
    /* Set up address structure */
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, hostname, &addr.sin_addr) <= 0) {
        ucc_debug("invalid hostname: %s", hostname);
        close(sock);
        return 0;
    }
    
    /* Attempt connection */
    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
        /* Connection successful, try to read response */
        if (recv(sock, &response, 1, 0) > 0) {
            if (response == 'A') {
                result = 1; /* Process is alive */
                ucc_debug("received alive response from %s:%d", hostname, port);
            }
        }
    } else {
        ucc_debug("failed to connect to %s:%d: %s", hostname, port, strerror(errno));
    }
    
    close(sock);
    return result;
}

int ucc_test_process_alive(int process_id, int timeout_sec)
{
    /* For this initial implementation, we'll use a simplified approach:
     * Try to check if process is still running using /proc filesystem
     * In production, this would be replaced with actual socket connectivity test */
    
    char proc_path[256];
    snprintf(proc_path, sizeof(proc_path), "/proc/%d", process_id);
    
    if (access(proc_path, F_OK) == 0) {
        ucc_debug("process %d appears to be alive", process_id);
        return 1;
    } else {
        ucc_debug("process %d appears to have failed", process_id);
        return 0;
    }
}

ucc_status_t ucc_detect_failed_processes(ucc_context_t *context,
                                        uint64_t **alive_mask,
                                        uint64_t **failed_ranks,
                                        int *num_failed)
{
    size_t n_eps;
    uint64_t *mask = NULL;
    uint64_t *failed = NULL;
    int failed_count = 0;
    int i;
    
    if (!context || !alive_mask || !failed_ranks || !num_failed) {
        return UCC_ERR_INVALID_PARAM;
    }
    
    *alive_mask = NULL;
    *failed_ranks = NULL;
    *num_failed = 0;
    
    if (!(context->params.mask & UCC_CONTEXT_PARAM_FIELD_OOB)) {
        ucc_debug("no OOB information available, skipping failure detection");
        return UCC_OK;
    }
    
    n_eps = context->params.oob.n_oob_eps;
    
    /* Allocate alive mask */
    mask = ucc_calloc(n_eps, sizeof(uint64_t), "alive_mask");
    if (!mask) {
        ucc_error("failed to allocate alive_mask");
        return UCC_ERR_NO_MEMORY;
    }
    
    /* Mark this process as alive */
    mask[context->params.oob.oob_ep] = 1;
    
    ucc_debug("attempting coordinated failure detection for %zu processes", n_eps);
    
    /* SOLUTION 1: Staggered Testing to Reduce Race Conditions */
    /* Each process waits a different amount based on its rank to spread out testing */
    usleep(context->params.oob.oob_ep * 10000); /* 10ms per rank offset */
    
    /* SOLUTION 2: Multiple Tests with Consensus */
    /* Test each process multiple times and use majority voting */
    const int num_tests = 3;
    const int test_interval_ms = 100; /* 100ms between tests */
    
    for (i = 0; i < n_eps; i++) {
        if (i == context->params.oob.oob_ep) {
            /* Skip self - already marked as alive */
            continue;
        }
        
        /* Perform multiple connectivity tests using actual socket connections */
        int alive_count = 0;
        int test_idx;
        uint16_t target_port = UCC_HEARTBEAT_BASE_PORT + i;
        
        for (test_idx = 0; test_idx < num_tests; test_idx++) {
            /* Use actual socket connectivity test */
            if (ucc_test_process_connectivity("127.0.0.1", target_port, 1)) {
                alive_count++;
            }
            
            /* Small delay between tests to avoid resource conflicts */
            if (test_idx < num_tests - 1) {
                usleep(test_interval_ms * 1000);
            }
        }
        
        /* Use majority consensus: process is alive if >50% of tests succeed */
        if (alive_count > num_tests / 2) {
            mask[i] = 1;
            ucc_debug("process %d: %d/%d tests passed - marked as alive", 
                     i, alive_count, num_tests);
        } else {
            mask[i] = 0;
            failed_count++;
            ucc_debug("process %d: %d/%d tests passed - marked as failed", 
                     i, alive_count, num_tests);
        }
    }
    
    /* SOLUTION 3: Add timestamp for result validity */
    struct timespec detection_time;
    clock_gettime(CLOCK_MONOTONIC, &detection_time);
    
    ucc_info("coordinated failure detection completed at %ld.%ld: %d failed processes detected", 
             detection_time.tv_sec, detection_time.tv_nsec / 1000000, failed_count);
    
    /* Build failed ranks list if there are failures */
    if (failed_count > 0) {
        failed = ucc_malloc(failed_count * sizeof(uint64_t), "failed_ranks");
        if (!failed) {
            ucc_error("failed to allocate failed_ranks");
            ucc_free(mask);
            return UCC_ERR_NO_MEMORY;
        }
        
        int failed_idx = 0;
        for (i = 0; i < n_eps; i++) {
            if (mask[i] == 0) {
                failed[failed_idx++] = i;
            }
        }
    }
    
    *alive_mask = mask;
    *failed_ranks = failed;
    *num_failed = failed_count;
    
    return UCC_OK;
}