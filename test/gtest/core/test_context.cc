/**
 * Copyright (c) 2020, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 */
#include "test_context.h"
#include "../common/test_ucc.h"
#include <vector>
#include <algorithm>
#include <random>

test_context::test_context()
{
    EXPECT_EQ(UCC_OK, ucc_context_config_read(lib_h, NULL, &ctx_config));
}

test_context::~test_context()
{
    ucc_context_config_release(ctx_config);
}

UCC_TEST_F(test_context, create_destroy)
{
    ucc_context_params_t ctx_params;
    ucc_context_h        ctx_h;
    ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_TYPE;
    ctx_params.type = UCC_CONTEXT_EXCLUSIVE;
    EXPECT_EQ(UCC_OK, ucc_context_create(lib_h, &ctx_params, ctx_config, &ctx_h));
    EXPECT_EQ(UCC_OK, ucc_context_destroy(ctx_h));
}

UCC_TEST_F(test_context, finalize_refuses_live_context_then_retries)
{
    ucc_context_params_t ctx_params;
    ucc_context_h        ctx_h;

    ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_TYPE;
    ctx_params.type = UCC_CONTEXT_EXCLUSIVE;
    ASSERT_EQ(UCC_OK,
              ucc_context_create(lib_h, &ctx_params, ctx_config, &ctx_h));
    EXPECT_EQ(UCC_ERR_INVALID_PARAM, ucc_finalize(lib_h));
    EXPECT_EQ(UCC_OK, ucc_context_destroy(ctx_h));
    /* The fixture destructor verifies that retrying finalize succeeds. */
}

UCC_TEST_F(test_context, context_refuses_live_team_then_retries)
{
    ucc_context_params_t ctx_params;
    ucc_team_params_t    team_params = {};
    ucc_context_h        ctx_h;
    ucc_team_h           team = nullptr;

    ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_TYPE;
    ctx_params.type = UCC_CONTEXT_EXCLUSIVE;
    ASSERT_EQ(UCC_OK,
              ucc_context_create(lib_h, &ctx_params, ctx_config, &ctx_h));
    team_params.mask = UCC_TEAM_PARAM_FIELD_TEAM_SIZE |
                       UCC_TEAM_PARAM_FIELD_EP |
                       UCC_TEAM_PARAM_FIELD_EP_RANGE;
    team_params.team_size = 1;
    team_params.ep = 0;
    team_params.ep_range = UCC_COLLECTIVE_EP_RANGE_CONTIG;
    ASSERT_EQ(UCC_OK, ucc_team_create_post(&ctx_h, 1, &team_params, &team));
    ucc_status_t status;
    while ((status = ucc_team_create_test(team)) == UCC_INPROGRESS) {
        ucc_context_progress(ctx_h);
    }
    ASSERT_EQ(UCC_OK, status);
    EXPECT_EQ(UCC_ERR_INVALID_PARAM, ucc_context_destroy(ctx_h));
    EXPECT_EQ(UCC_OK, ucc_context_progress(ctx_h));
    EXPECT_EQ(UCC_OK, ucc_team_destroy(team));
    EXPECT_EQ(UCC_OK, ucc_context_destroy(ctx_h));
}

UCC_TEST_F(test_context, synchronous_team_create_failure_has_no_owner)
{
    ucc_context_params_t ctx_params;
    ucc_team_params_t    team_params = {};
    ucc_context_h        ctx_h;
    ucc_team_h           team = reinterpret_cast<ucc_team_h>(1);

    ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_TYPE;
    ctx_params.type = UCC_CONTEXT_EXCLUSIVE;
    ASSERT_EQ(UCC_OK,
              ucc_context_create(lib_h, &ctx_params, ctx_config, &ctx_h));
    team_params.mask = UCC_TEAM_PARAM_FIELD_TEAM_SIZE |
                       UCC_TEAM_PARAM_FIELD_EP |
                       UCC_TEAM_PARAM_FIELD_EP_RANGE;
    team_params.team_size = 2;
    team_params.ep = 0;
    team_params.ep_range = UCC_COLLECTIVE_EP_RANGE_CONTIG;
    EXPECT_EQ(UCC_ERR_NO_RESOURCE,
              ucc_team_create_post(&ctx_h, 1, &team_params, &team));
    EXPECT_EQ(nullptr, team);
    EXPECT_EQ(UCC_OK, ucc_context_destroy(ctx_h));
}

UCC_TEST_F(test_context, configured_thread_modes_owner_accounting)
{
    ucc_lib_config_h     config;
    ucc_lib_params_t     params = {};
    ucc_context_config_h context_config;
    ucc_context_params_t context_params = {};
    ucc_lib_h            lib;
    ucc_context_h        context;

    const ucc_thread_mode_t modes[] = {UCC_THREAD_SINGLE, UCC_THREAD_FUNNELED,
                                       UCC_THREAD_MULTIPLE};

    for (auto mode : modes) {
        ASSERT_EQ(UCC_OK, ucc_lib_config_read(NULL, NULL, &config));
        params.mask = UCC_LIB_PARAM_FIELD_THREAD_MODE;
        params.thread_mode = mode;
        ASSERT_EQ(UCC_OK, ucc_init(&params, config, &lib));
        ucc_lib_config_release(config);
        ASSERT_EQ(UCC_OK, ucc_context_config_read(lib, NULL, &context_config));
        context_params.mask = UCC_CONTEXT_PARAM_FIELD_TYPE;
        context_params.type = UCC_CONTEXT_SHARED;
        ASSERT_EQ(UCC_OK, ucc_context_create(lib, &context_params,
                                             context_config, &context));
        ucc_context_config_release(context_config);
        EXPECT_EQ(UCC_ERR_INVALID_PARAM, ucc_finalize(lib));
        EXPECT_EQ(UCC_OK, ucc_context_destroy(context));
        EXPECT_EQ(UCC_OK, ucc_finalize(lib));
    }
}

UCC_TEST_F(test_context, init_multiple)
{
    const int                  n_ctxs = 8;
    ucc_context_params_t       ctx_params;
    ucc_context_h              ctx_h;
    std::vector<ucc_context_h> ctxs;
    ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_TYPE;
    ctx_params.type = UCC_CONTEXT_EXCLUSIVE;
    for (int i = 0; i < n_ctxs; i++) {
        EXPECT_EQ(UCC_OK, ucc_context_create(lib_h, &ctx_params, ctx_config, &ctx_h));
        ctxs.push_back(ctx_h);
    }

    std::shuffle(ctxs.begin(), ctxs.end(), std::default_random_engine());
    for (auto ctx_h : ctxs) {
        EXPECT_EQ(UCC_OK, ucc_context_destroy(ctx_h));
    }
}

test_context_get_attr::test_context_get_attr()
{
    ucc_context_params_t ctx_params;
    ctx_params.mask = UCC_CONTEXT_PARAM_FIELD_TYPE;
    ctx_params.type = UCC_CONTEXT_EXCLUSIVE;
    EXPECT_EQ(UCC_OK,
              ucc_context_create(lib_h, &ctx_params, ctx_config, &ctx_h));
}

test_context_get_attr::~test_context_get_attr()
{
    EXPECT_EQ(UCC_OK, ucc_context_destroy(ctx_h));
}

UCC_TEST_F(test_context_get_attr, addr_len)
{
    ucc_context_attr_t attr;
    attr.mask = UCC_CONTEXT_ATTR_FIELD_CTX_ADDR_LEN;
    EXPECT_EQ(UCC_OK, ucc_context_get_attr(ctx_h, &attr));
}

UCC_TEST_F(test_context_get_attr, addr)
{
    ucc_context_attr_t attr;
    attr.mask = UCC_CONTEXT_ATTR_FIELD_CTX_ADDR;
    EXPECT_EQ(UCC_OK, ucc_context_get_attr(ctx_h, &attr));
    EXPECT_EQ(true, ((attr.ctx_addr_len == 0) || (NULL != attr.ctx_addr)));
}

UCC_TEST_F(test_context_get_attr, work_buffer_size)
{
    ucc_context_attr_t attr;
    attr.mask = UCC_CONTEXT_ATTR_FIELD_WORK_BUFFER_SIZE;
    EXPECT_EQ(UCC_OK, ucc_context_get_attr(ctx_h, &attr));
    EXPECT_EQ(5, attr.global_work_buffer_size);
}

UCC_TEST_F(test_context, global)
{
    /* Create and cleanup several Jobs (ucc contextss) with OOB */
    UccJob job1(1, UccJob::UCC_JOB_CTX_GLOBAL);
    job1.cleanup();

    UccJob job3(3, UccJob::UCC_JOB_CTX_GLOBAL);
    job3.cleanup();

    UccJob job11(11, UccJob::UCC_JOB_CTX_GLOBAL);
    job11.cleanup();

    UccJob job16(16, UccJob::UCC_JOB_CTX_GLOBAL);
    job16.cleanup();

}
