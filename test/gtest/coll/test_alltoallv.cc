/**
 * Copyright (c) 2021-2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "common/test_ucc.h"
#include "utils/ucc_math.h"

using Param_0 = std::tuple<int, ucc_memory_type_t, gtest_ucc_inplace_t, ucc_datatype_t>;
using Param_1 = std::tuple<ucc_memory_type_t, gtest_ucc_inplace_t, ucc_datatype_t>;

template <class T>
class test_alltoallv : public UccCollArgs, public ucc::test {
public:
    uint64_t coll_mask;
    uint64_t coll_flags;

    test_alltoallv() : coll_mask(0), coll_flags(0) {}
    void data_init(int nprocs, ucc_datatype_t dtype, size_t count,
                   UccCollCtxVec &ctxs, UccTeam_h team,
                   bool persistent = false) {
        data_init_common(nprocs, dtype, count, ctxs, team, persistent);
    }
    void data_init(int nprocs, ucc_datatype_t dtype, size_t count,
                   UccCollCtxVec &ctxs, bool persistent = false) {
        data_init_common(nprocs, dtype, count, ctxs, nullptr, persistent);
    }
    void data_init_common(int nprocs, ucc_datatype_t dtype, size_t count,
                          UccCollCtxVec &ctxs, UccTeam_h team,
                          bool persistent) {
        int buf_count;
        bool is_onesided = (NULL != team);
        ctxs.resize(nprocs);

        for (auto r = 0; r < nprocs; r++) {
            ucc_coll_args_t *coll = (ucc_coll_args_t*)
                    calloc(1, sizeof(ucc_coll_args_t));

            ctxs[r] = (gtest_ucc_coll_ctx_t*)calloc(1, sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args = coll;

            coll->coll_type = UCC_COLL_TYPE_ALLTOALLV;
            coll->mask = coll_mask;
            coll->flags = coll_flags;

            coll->src.info_v.mem_type = mem_type;
            coll->src.info_v.counts = (ucc_count_t*)malloc(sizeof(T) * nprocs);
            coll->src.info_v.datatype = dtype;
            coll->src.info_v.displacements = (ucc_aint_t*)malloc(sizeof(T) * nprocs);

            coll->dst.info_v.mem_type = mem_type;
            coll->dst.info_v.counts = (ucc_count_t*)malloc(sizeof(T) * nprocs);
            coll->dst.info_v.datatype = dtype;
            coll->dst.info_v.displacements = (ucc_aint_t*)malloc(sizeof(T) * nprocs);

            if (!is_onesided) {
                buf_count = 0;
                for (int i = 0; i < nprocs; i++) {
                    int rank_count = (nprocs + r - i) * count;
                    ((T*)coll->src.info_v.counts)[i] = rank_count;
                    ((T*)coll->src.info_v.displacements)[i] = buf_count;
                    buf_count += rank_count;
                }
                /* Force at least 1 zero count for bigger coverage of corner cases */
                ((T*)coll->src.info_v.counts)[(r + 1) % nprocs] = 0;
            } else {
                /* The one-sided algorithm places peer i's data at the
                 * sender's own src displacement s_disp[i] in the peer's dst
                 * segment, and the peer validates at its own d_disp[i].
                 * I1 therefore requires d_disp[i] == s_disp[i] for every i.
                 * We satisfy that with a uniform layout: identical counts
                 * and displacements on every rank. */
                int urank_count = (int)(2 * count + 1);
                buf_count = 0;
                for (int i = 0; i < nprocs; i++) {
                    ((T*)coll->src.info_v.counts)[i] = urank_count;
                    ((T*)coll->src.info_v.displacements)[i] = buf_count;
                    buf_count += urank_count;
                    ((T*)coll->dst.info_v.counts)[i] = urank_count;
                    ((T*)coll->dst.info_v.displacements)[i] = buf_count;
                }
            }

            ctxs[r]->init_buf = ucc_malloc(buf_count * ucc_dt_size(dtype), "init buf");
            EXPECT_NE(ctxs[r]->init_buf, nullptr);
            for (int i = 0; i < nprocs; i++) {
                alltoallx_init_buf(r, i, (uint8_t*)ctxs[r]->init_buf +
                               ((T*)coll->src.info_v.displacements)[i] * ucc_dt_size(dtype),
                               ((T*)coll->src.info_v.counts)[i] * ucc_dt_size(dtype));
            }

            if (is_onesided) {
                /* Segment buffers: buf 0 = src, 1 = dst, 2 = work buffer. */
                void *sbuf     = team->procs[r].p->onesided_buf[0];
                void *rbuf     = team->procs[r].p->onesided_buf[1];
                long *work_buf = (long *)team->procs[r].p->onesided_buf[2];
                ctxs[r]->rbuf_size = buf_count * ucc_dt_size(dtype);
                coll->mask  |= UCC_COLL_ARGS_FIELD_FLAGS |
                               UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER;
                coll->flags |= UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS;
                coll->src.info_v.buffer    = sbuf;
                coll->src.info_v.mem_type  = UCC_MEMORY_TYPE_HOST;
                coll->dst.info_v.buffer    = rbuf;
                coll->dst.info_v.mem_type  = UCC_MEMORY_TYPE_HOST;
                coll->global_work_buffer   = work_buf;
                UCC_CHECK(ucc_mc_memcpy(sbuf, ctxs[r]->init_buf,
                                        buf_count * ucc_dt_size(dtype),
                                        UCC_MEMORY_TYPE_HOST,
                                        UCC_MEMORY_TYPE_HOST));
            } else {
                UCC_CHECK(ucc_mc_alloc(&ctxs[r]->src_mc_header,
                                       buf_count * ucc_dt_size(dtype), mem_type));
                coll->src.info_v.buffer = ctxs[r]->src_mc_header->addr;
                UCC_CHECK(ucc_mc_memcpy(coll->src.info_v.buffer, ctxs[r]->init_buf,
                                        buf_count * ucc_dt_size(dtype), mem_type,
                                        UCC_MEMORY_TYPE_HOST));

                /* TODO: inplace support */

                buf_count = 0;
                for (int i = 0; i < nprocs; i++) {
                    int rank_count = (nprocs - r + i) * count;
                    ((T*)coll->dst.info_v.counts)[i] = rank_count;
                    ((T*)coll->dst.info_v.displacements)[i] = buf_count;
                    buf_count += rank_count;
                }
                ((T*)coll->dst.info_v.counts)[(r - 1 + nprocs) % nprocs] = 0;
                ctxs[r]->rbuf_size = buf_count * ucc_dt_size(dtype);
                UCC_CHECK(ucc_mc_alloc(&ctxs[r]->dst_mc_header,
                                       buf_count * ucc_dt_size(dtype), mem_type));
                coll->dst.info_v.buffer = ctxs[r]->dst_mc_header->addr;
            }
            if (persistent) {
                coll->mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
                coll->flags |= UCC_COLL_ARGS_FLAG_PERSISTENT;
            }
        }
    }
    void reset(UccCollCtxVec ctxs)
    {
        for (auto r = 0; r < ctxs.size(); r++) {
            ucc_coll_args_t *coll = ctxs[r]->args;
            clear_buffer(coll->dst.info_v.buffer, ctxs[r]->rbuf_size, mem_type,
                         0);
        }
    }
    bool data_validate(UccCollCtxVec ctxs)
    {
        bool                   ret = true;
        std::vector<uint8_t *> dsts(ctxs.size());

        if (UCC_MEMORY_TYPE_HOST != mem_type) {
            for (int r = 0; r < ctxs.size(); r++) {
                dsts[r] = (uint8_t *) ucc_malloc(ctxs[r]->rbuf_size, "dsts buf");
                EXPECT_NE(dsts[r], nullptr);
                UCC_CHECK(ucc_mc_memcpy(dsts[r], ctxs[r]->args->dst.info_v.buffer,
                                        ctxs[r]->rbuf_size, UCC_MEMORY_TYPE_HOST,
                                        mem_type));
            }
        } else {
            for (int r = 0; r < ctxs.size(); r++) {
                dsts[r] = (uint8_t *)(ctxs[r]->args->dst.info_v.buffer);
            }
        }
        for (int r = 0; r < ctxs.size(); r++) {
            ucc_coll_args_t* coll = ctxs[r]->args;
            for (int i = 0; i < ctxs.size(); i++) {
                size_t rank_size = ucc_dt_size(coll->dst.info_v.datatype) *
                        (size_t)((T*)coll->dst.info_v.counts)[i];
                size_t rank_offs = ucc_dt_size(coll->dst.info_v.datatype) *
                        (size_t)((T*)coll->dst.info_v.displacements)[i];
                if (0 != alltoallx_validate_buf(r, i, (uint8_t*)dsts[r] +
                                                rank_offs, rank_size)) {
                    ret = false;
                    break;
                }
            }
        }
        if (UCC_MEMORY_TYPE_HOST != mem_type) {
            for (int r = 0; r < ctxs.size(); r++) {
                ucc_free(dsts[r]);
            }
        }
        return ret;
    }
    void data_fini(UccCollCtxVec ctxs)
    {
        for (gtest_ucc_coll_ctx_t* ctx : ctxs) {
            ucc_coll_args_t* coll = ctx->args;
            UCC_CHECK(ucc_mc_free(ctx->src_mc_header));
            free(coll->src.info_v.counts);
            free(coll->src.info_v.displacements);
            UCC_CHECK(ucc_mc_free(ctx->dst_mc_header));
            free(coll->dst.info_v.counts);
            free(coll->dst.info_v.displacements);
            ucc_free(ctx->init_buf);
            free(coll);
            free(ctx);
        }
        ctxs.clear();
    }
    void data_fini_onesided(UccCollCtxVec ctxs)
    {
        /* Segment-mode buffers are owned by the job, not the ctx. */
        for (gtest_ucc_coll_ctx_t *ctx : ctxs) {
            ucc_coll_args_t *coll = ctx->args;
            free(coll->src.info_v.counts);
            free(coll->src.info_v.displacements);
            free(coll->dst.info_v.counts);
            free(coll->dst.info_v.displacements);
            ucc_free(ctx->init_buf);
            free(coll);
            free(ctx);
        }
        ctxs.clear();
    }
};

class test_alltoallv_0 : public test_alltoallv <uint64_t>,
        public ::testing::WithParamInterface<Param_0> {};

UCC_TEST_P(test_alltoallv_0, single)
{
    const int            team_id  = std::get<0>(GetParam());
    ucc_memory_type_t    mem_type = std::get<1>(GetParam());
    gtest_ucc_inplace_t  inplace  = std::get<2>(GetParam());
    const ucc_datatype_t dtype    = std::get<3>(GetParam());
    UccTeam_h            team     = UccJob::getStaticTeams()[team_id];
    int                  size     = team->procs.size();
    UccCollCtxVec        ctxs;

    coll_mask = UCC_COLL_ARGS_FIELD_FLAGS;
    coll_flags = UCC_COLL_ARGS_FLAG_COUNT_64BIT |
                 UCC_COLL_ARGS_FLAG_DISPLACEMENTS_64BIT;
    set_inplace(inplace);
    SET_MEM_TYPE(mem_type);

    data_init(size, dtype, 1, ctxs, false);
    UccReq    req(team, ctxs);
    req.start();
    req.wait();

    EXPECT_EQ(true, data_validate(ctxs));
    data_fini(ctxs);
}

UCC_TEST_P(test_alltoallv_0, single_onesided)
{
    const int            team_id        = std::get<0>(GetParam());
    ucc_memory_type_t    mem_type       = std::get<1>(GetParam());
    gtest_ucc_inplace_t  inplace        = std::get<2>(GetParam());
    const ucc_datatype_t dtype          = std::get<3>(GetParam());
    UccTeam_h            reference_team = UccJob::getStaticTeams()[team_id];
    int                  size           = reference_team->procs.size();
    ucc_job_env_t        env            = {{"UCC_TL_UCP_TUNE",
                                            "alltoallv:0-inf:@onesided"}};
    bool                 is_contig = true;
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);
    UccTeam_h            team;
    std::vector<int>     reference_ranks;
    UccCollCtxVec        ctxs;

    ASSERT_EQ(UCC_MEMORY_TYPE_HOST, mem_type);
    ASSERT_EQ(TEST_NO_INPLACE, inplace);

    for (auto i = 0; i < reference_team->n_procs; i++) {
        int rank = reference_team->procs[i].p->job_rank;
        reference_ranks.push_back(rank);
        if (is_contig && i > 0 &&
            (rank - reference_ranks[i - 1] > 1 ||
             reference_ranks[i - 1] - rank > 1)) {
            is_contig = false;
        }
    }
    team = job.create_team(reference_ranks, true, is_contig, true);
    set_inplace(inplace);
    SET_MEM_TYPE(mem_type);

    coll_mask  = UCC_COLL_ARGS_FIELD_FLAGS;
    coll_flags = UCC_COLL_ARGS_FLAG_COUNT_64BIT |
                 UCC_COLL_ARGS_FLAG_DISPLACEMENTS_64BIT;
    data_init(size, dtype, 1, ctxs, team, false);
    UccReq req(team, ctxs);
    req.start();
    req.wait();
    EXPECT_EQ(true, data_validate(ctxs));
    data_fini_onesided(ctxs);
}

UCC_TEST_P(test_alltoallv_0, multiple_onesided)
{
    /* Two alltoallv one-sided collectives back-to-back on the same team, no
     * barrier between: the I7 counter-reuse regression case. */
    const int            team_id        = std::get<0>(GetParam());
    ucc_memory_type_t    mem_type       = std::get<1>(GetParam());
    gtest_ucc_inplace_t  inplace        = std::get<2>(GetParam());
    const ucc_datatype_t dtype          = std::get<3>(GetParam());
    UccTeam_h            reference_team = UccJob::getStaticTeams()[team_id];
    int                  size           = reference_team->procs.size();
    ucc_job_env_t        env            = {{"UCC_TL_UCP_TUNE",
                                            "alltoallv:0-inf:@onesided"}};
    bool                 is_contig = true;
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);
    UccTeam_h            team;
    std::vector<int>     reference_ranks;
    UccCollCtxVec        ctxs;

    ASSERT_EQ(UCC_MEMORY_TYPE_HOST, mem_type);
    ASSERT_EQ(TEST_NO_INPLACE, inplace);

    for (auto i = 0; i < reference_team->n_procs; i++) {
        int rank = reference_team->procs[i].p->job_rank;
        reference_ranks.push_back(rank);
        if (is_contig && i > 0 &&
            (rank - reference_ranks[i - 1] > 1 ||
             reference_ranks[i - 1] - rank > 1)) {
            is_contig = false;
        }
    }
    team = job.create_team(reference_ranks, true, is_contig, true);
    set_inplace(inplace);
    SET_MEM_TYPE(mem_type);

    coll_mask  = UCC_COLL_ARGS_FIELD_FLAGS;
    coll_flags = UCC_COLL_ARGS_FLAG_COUNT_64BIT |
                 UCC_COLL_ARGS_FLAG_DISPLACEMENTS_64BIT;
    data_init(size, dtype, 1, ctxs, team, false);
    UccReq req1(team, ctxs);
    req1.start();
    req1.wait();
    EXPECT_EQ(true, data_validate(ctxs));
    UccReq req2(team, ctxs);
    req2.start();
    req2.wait();
    EXPECT_EQ(true, data_validate(ctxs));
    data_fini_onesided(ctxs);
}

UCC_TEST_P(test_alltoallv_0, single_persistent)
{
    const int            team_id  = std::get<0>(GetParam());
    ucc_memory_type_t    mem_type = std::get<1>(GetParam());
    gtest_ucc_inplace_t  inplace  = std::get<2>(GetParam());
    const ucc_datatype_t dtype    = std::get<3>(GetParam());
    UccTeam_h            team     = UccJob::getStaticTeams()[team_id];
    int                  size     = team->procs.size();
    const int            n_calls  = 3;
    UccCollCtxVec        ctxs;

    coll_mask = UCC_COLL_ARGS_FIELD_FLAGS;
    coll_flags =
        UCC_COLL_ARGS_FLAG_COUNT_64BIT | UCC_COLL_ARGS_FLAG_DISPLACEMENTS_64BIT;
    set_inplace(inplace);
    SET_MEM_TYPE(mem_type);

    data_init(size, dtype, 1, ctxs, true);
    UccReq req(team, ctxs);

    for (auto i = 0; i < n_calls; i++) {
        req.start();
        req.wait();
        EXPECT_EQ(true, data_validate(ctxs));
        reset(ctxs);
    }
    data_fini(ctxs);
}

class test_alltoallv_1 : public test_alltoallv <uint32_t>,
        public ::testing::WithParamInterface<Param_0> {};

UCC_TEST_P(test_alltoallv_1, single)
{
    const int            team_id  = std::get<0>(GetParam());
    ucc_memory_type_t    mem_type = std::get<1>(GetParam());
    gtest_ucc_inplace_t  inplace  = std::get<2>(GetParam());
    const ucc_datatype_t dtype    = std::get<3>(GetParam());
    UccTeam_h            team     = UccJob::getStaticTeams()[team_id];
    int                  size     = team->procs.size();
    UccCollCtxVec        ctxs;

    set_inplace(inplace);
    SET_MEM_TYPE(mem_type);

    data_init(size, dtype, 1, ctxs, false);
    UccReq    req(team, ctxs);
    req.start();
    req.wait();

    EXPECT_EQ(true, data_validate(ctxs));
    data_fini(ctxs);
}

INSTANTIATE_TEST_CASE_P(
        64, test_alltoallv_0,
        ::testing::Combine(
            ::testing::Range(1, UccJob::nStaticTeams), // team_ids
#ifdef HAVE_CUDA
            ::testing::Values(UCC_MEMORY_TYPE_HOST, UCC_MEMORY_TYPE_CUDA,
                              UCC_MEMORY_TYPE_CUDA_MANAGED),
#else
            ::testing::Values(UCC_MEMORY_TYPE_HOST),
#endif
            ::testing::Values(/*TEST_INPLACE,*/ TEST_NO_INPLACE),
            PREDEFINED_DTYPES)); // dtype


INSTANTIATE_TEST_CASE_P(
        32, test_alltoallv_1,
        ::testing::Combine(
            ::testing::Range(1, UccJob::nStaticTeams), // team_ids
#ifdef HAVE_CUDA
            ::testing::Values(UCC_MEMORY_TYPE_HOST, UCC_MEMORY_TYPE_CUDA,
                              UCC_MEMORY_TYPE_CUDA_MANAGED),
#else
            ::testing::Values(UCC_MEMORY_TYPE_HOST),
#endif
            ::testing::Values(/*TEST_INPLACE,*/ TEST_NO_INPLACE),
            PREDEFINED_DTYPES)); // dtype

class test_alltoallv_2 : public test_alltoallv<uint64_t>,
        public ::testing::WithParamInterface<Param_1> {};

class test_alltoallv_3 : public test_alltoallv<uint32_t>,
        public ::testing::WithParamInterface<Param_1> {};

class test_alltoallv_alg : public test_alltoallv<uint32_t>,
        public ::testing::WithParamInterface<Param_1> {};

UCC_TEST_P(test_alltoallv_alg, hybrid)
{
    int                  n_procs  = 15;
    ucc_memory_type_t    mem_type = std::get<0>(GetParam());
    gtest_ucc_inplace_t  inplace  = std::get<1>(GetParam());
    const ucc_datatype_t dtype    = std::get<2>(GetParam());

    ASSERT_NE(inplace, TEST_INPLACE);
    ucc_job_env_t env     = {{"UCC_CL_BASIC_TUNE", "inf"},
                             {"UCC_TL_UCP_TUNE", "alltoallv:@hybrid:inf"}};
    UccJob        job(n_procs, UccJob::UCC_JOB_CTX_GLOBAL, env);
    UccTeam_h     team    = job.create_team(n_procs);
    UccCollCtxVec ctxs;

    SET_MEM_TYPE(mem_type);
    data_init(n_procs, dtype, 16, ctxs, false);
    UccReq req(team, ctxs);
    req.start();
    req.wait();

    EXPECT_EQ(true, data_validate(ctxs));
    data_fini(ctxs);
}

UCC_TEST_P(test_alltoallv_2, multiple)
{
    ucc_memory_type_t           mem_type = std::get<0>(GetParam());
    gtest_ucc_inplace_t         inplace  = std::get<1>(GetParam());
    const ucc_datatype_t        dtype    = std::get<2>(GetParam());
    std::vector<UccReq>         reqs;
    std::vector<UccCollCtxVec>  ctxs;

    coll_mask = UCC_COLL_ARGS_FIELD_FLAGS;
    coll_flags = UCC_COLL_ARGS_FLAG_COUNT_64BIT |
                 UCC_COLL_ARGS_FLAG_DISPLACEMENTS_64BIT;

    for (int tid = 0; tid < UccJob::nStaticTeams; tid++) {
        UccTeam_h       team = UccJob::getStaticTeams()[tid];
        int             size = team->procs.size();
        UccCollCtxVec   ctx;

        this->set_inplace(inplace);
        SET_MEM_TYPE(mem_type);

        data_init(size, dtype, 1, ctx, false);
        reqs.push_back(UccReq(team, ctx));
        ctxs.push_back(ctx);
    }
    UccReq::startall(reqs);
    UccReq::waitall(reqs);

    for (auto ctx : ctxs) {
        EXPECT_EQ(true, data_validate(ctx));
        data_fini(ctx);
    }
}

UCC_TEST_P(test_alltoallv_3, multiple)
{
    ucc_memory_type_t           mem_type = std::get<0>(GetParam());
    gtest_ucc_inplace_t         inplace  = std::get<1>(GetParam());
    const ucc_datatype_t        dtype    = std::get<2>(GetParam());
    std::vector<UccReq>         reqs;
    std::vector<UccCollCtxVec>  ctxs;

    for (int tid = 0; tid < UccJob::nStaticTeams; tid++) {
        UccTeam_h       team = UccJob::getStaticTeams()[tid];
        int             size = team->procs.size();
        UccCollCtxVec   ctx;

        this->set_inplace(inplace);
        SET_MEM_TYPE(mem_type);

        data_init(size, dtype, 1, ctx, false);
        reqs.push_back(UccReq(team, ctx));
        ctxs.push_back(ctx);
    }
    UccReq::startall(reqs);
    UccReq::waitall(reqs);

    for (auto ctx : ctxs) {
        EXPECT_EQ(true, data_validate(ctx));
        data_fini(ctx);
    }
}

INSTANTIATE_TEST_CASE_P(
        alltoallv_algs, test_alltoallv_alg,
        ::testing::Combine(
            ::testing::Values(UCC_MEMORY_TYPE_HOST),
            ::testing::Values(TEST_NO_INPLACE),
            PREDEFINED_DTYPES));

INSTANTIATE_TEST_CASE_P(
        64, test_alltoallv_2,
        ::testing::Combine(
#ifdef HAVE_CUDA
            ::testing::Values(UCC_MEMORY_TYPE_HOST, UCC_MEMORY_TYPE_CUDA,
                              UCC_MEMORY_TYPE_CUDA_MANAGED),
#else
            ::testing::Values(UCC_MEMORY_TYPE_HOST),
#endif
            ::testing::Values(/*TEST_INPLACE,*/ TEST_NO_INPLACE),
            PREDEFINED_DTYPES)); // dtype

INSTANTIATE_TEST_CASE_P(
        32, test_alltoallv_3,
        ::testing::Combine(
#ifdef HAVE_CUDA
            ::testing::Values(UCC_MEMORY_TYPE_HOST, UCC_MEMORY_TYPE_CUDA,
                              UCC_MEMORY_TYPE_CUDA_MANAGED),
#else
            ::testing::Values(UCC_MEMORY_TYPE_HOST),
#endif
            ::testing::Values(/*TEST_INPLACE,*/ TEST_NO_INPLACE),
            PREDEFINED_DTYPES)); // dtype
