/**
 * Standalone lifetime enforcement smoke test.
 * Verifies: context_create→finalize refusal→context_destroy→finalize success.
 * Also verifies double-context accounting.
 *
 * Build:
 *   gcc -std=gnu11 -I src -I src/ucc/api -I build/src \
 *       -L src/.libs -lucc -o lifetime_smoke lifetime_smoke.c
 * Run:
 *   LD_LIBRARY_PATH=src/.libs ./lifetime_smoke
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ucc/api/ucc.h"

#define CHECK(expr, msg) do {                                              \
    ucc_status_t _s = (expr);                                             \
    if (_s != UCC_OK) {                                                    \
        fprintf(stderr, "FAIL: %s: %s (%d)\n", msg,                       \
                ucc_status_string(_s), _s);                                \
        exit(1);                                                           \
    }                                                                      \
} while(0)

#define CHECK_REFUSED(expr, msg) do {                                      \
    ucc_status_t _s = (expr);                                             \
    if (_s != UCC_ERR_INVALID_PARAM) {                                     \
        fprintf(stderr, "FAIL: %s should refuse but got %s (%d)\n", msg,  \
                ucc_status_string(_s), _s);                                \
        exit(1);                                                           \
    }                                                                      \
    printf("PASS: %s correctly refused (UCC_ERR_INVALID_PARAM)\n", msg);   \
} while(0)

int main(void)
{
    ucc_lib_params_t      lib_params;
    ucc_lib_config_h      lib_config;
    ucc_lib_h             lib;
    ucc_context_params_t  ctx_params;
    ucc_context_config_h  ctx_config;
    ucc_context_h         ctx_a, ctx_b;
    ucc_status_t          status;

    memset(&lib_params, 0, sizeof(lib_params));
    lib_params.mask = UCC_LIB_PARAM_FIELD_THREAD_MODE;
    lib_params.thread_mode = UCC_THREAD_SINGLE;

    status = ucc_lib_config_read(NULL, NULL, &lib_config);
    CHECK(status, "ucc_lib_config_read");

    printf("--- Phase 1: Init and create two contexts ---\n");
    status = ucc_init(&lib_params, lib_config, &lib);
    CHECK(status, "ucc_init");
    ucc_lib_config_release(lib_config);
    memset(&ctx_params, 0, sizeof(ctx_params));
    ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_TYPE;
    ctx_params.type = UCC_CONTEXT_EXCLUSIVE;
    status = ucc_context_config_read(lib, NULL, &ctx_config);
    CHECK(status, "ucc_context_config_read");

    status = ucc_context_create(lib, &ctx_params, ctx_config, &ctx_a);
    CHECK(status, "ucc_context_create A");
    printf("  context_a=%p\n", (void*)ctx_a);

    status = ucc_context_create(lib, &ctx_params, ctx_config, &ctx_b);
    CHECK(status, "ucc_context_create B");
    printf("  context_b=%p\n", (void*)ctx_b);
    ucc_context_config_release(ctx_config);

    printf("\n--- Phase 2: Finalize refused (2 live contexts) ---\n");
    CHECK_REFUSED(ucc_finalize(lib), "ucc_finalize (2 contexts live)");

    printf("\n--- Phase 3: Destroy context A, finalize refused (1 live) ---\n");
    status = ucc_context_destroy(ctx_a);
    CHECK(status, "ucc_context_destroy A");
    printf("  context A destroyed\n");

    CHECK_REFUSED(ucc_finalize(lib), "ucc_finalize (1 context live)");

    printf("\n--- Phase 4: Destroy context B, finalize succeeds ---\n");
    status = ucc_context_destroy(ctx_b);
    CHECK(status, "ucc_context_destroy B");
    printf("  context B destroyed\n");

    status = ucc_finalize(lib);
    CHECK(status, "ucc_finalize (all contexts destroyed)");
    printf("  library finalized\n");

    printf("\n=== ALL TESTS PASSED ===\n");
    return 0;
}