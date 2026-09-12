/**
 * Copyright (c) 2021-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "common/test_ucc.h"
#include "utils/ucc_math.h"
#include "utils/ucc_log.h"
#include "utils/ucc_coll_utils.h"

using Param_0 = std::tuple<int, ucc_datatype_t, ucc_memory_type_t, int, gtest_ucc_inplace_t, bool>;
using Param_1 = std::tuple<ucc_datatype_t, ucc_memory_type_t, int, gtest_ucc_inplace_t, bool>;
using Param_2 = std::tuple<ucc_datatype_t, ucc_memory_type_t, int, gtest_ucc_inplace_t, std::string, bool>;

size_t noncontig_padding = 1; // # elements worth of space in between each rank's contribution to the dst buf

class test_allgatherv : public UccCollArgs, public ucc::test
{
public:
    void  data_init(int nprocs, ucc_datatype_t dtype, size_t count,
                    UccCollCtxVec &ctxs, bool persistent) {
        ctxs.resize(nprocs);
        for (auto r = 0; r < nprocs; r++) {
            int *counts;
            int *displs;
            size_t my_count = (nprocs - r) * count;
            size_t disp_counter = 0;
            ucc_coll_args_t *coll = (ucc_coll_args_t*)calloc(1, sizeof(ucc_coll_args_t));

            ctxs[r] = (gtest_ucc_coll_ctx_t*)calloc(1, sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args = coll;

            counts = (int*)malloc(sizeof(int) * nprocs);
            displs = (int*)malloc(sizeof(int) * nprocs);

            if (is_contig) {
                for (int i = 0; i < nprocs; i++) {
                    counts[i] = (nprocs - i) * count;
                    displs[i] = disp_counter;
                    disp_counter += counts[i];
                }
                coll->flags = UCC_COLL_ARGS_FLAG_CONTIG_DST_BUFFER;
            } else {
                for (int i = 0; i < nprocs; i++) {
                    counts[i] = (nprocs - i) * count;
                    displs[i] = disp_counter;
                    disp_counter += counts[i] + noncontig_padding; // Add noncontig_padding elemnts of space between the bufs
                }
            }
            coll->mask = UCC_COLL_ARGS_FIELD_FLAGS;
            coll->coll_type = UCC_COLL_TYPE_ALLGATHERV;

            coll->src.info.mem_type = mem_type;
            coll->src.info.count = my_count;
            coll->src.info.datatype = dtype;

            coll->dst.info_v.mem_type = mem_type;
            coll->dst.info_v.counts   = (ucc_count_t*)counts;
            coll->dst.info_v.displacements = (ucc_aint_t*)displs;
            coll->dst.info_v.datatype = dtype;

            ctxs[r]->init_buf = ucc_malloc(ucc_dt_size(dtype) * my_count, "init buf");
            EXPECT_NE(ctxs[r]->init_buf, nullptr);
            for (int i = 0; i < (ucc_dt_size(dtype) * my_count); i++) {
                uint8_t *sbuf = (uint8_t*)ctxs[r]->init_buf;
                sbuf[i] = r;
            }

            ctxs[r]->rbuf_size = ucc_dt_size(dtype) * disp_counter;
            UCC_CHECK(ucc_mc_alloc(&ctxs[r]->dst_mc_header, ctxs[r]->rbuf_size,
                                   mem_type));
            coll->dst.info_v.buffer = ctxs[r]->dst_mc_header->addr;
            if (TEST_INPLACE == inplace) {
                coll->mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
                coll->flags |= UCC_COLL_ARGS_FLAG_IN_PLACE;
                UCC_CHECK(ucc_mc_memcpy((void*)((ptrdiff_t)coll->dst.info_v.buffer +
                                        displs[r] * ucc_dt_size(dtype)),
                                        ctxs[r]->init_buf, ucc_dt_size(dtype) * my_count,
                                        mem_type, UCC_MEMORY_TYPE_HOST));
            } else {
                UCC_CHECK(ucc_mc_alloc(&ctxs[r]->src_mc_header,
                                       ucc_dt_size(dtype) * my_count,
                                       mem_type));
                coll->src.info.buffer = ctxs[r]->src_mc_header->addr;
                UCC_CHECK(ucc_mc_memcpy(coll->src.info.buffer, ctxs[r]->init_buf,
                                        ucc_dt_size(dtype) * my_count, mem_type,
                                        UCC_MEMORY_TYPE_HOST));
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
            ucc_coll_args_t *coll     = ctxs[r]->args;
            size_t           my_count = coll->src.info.count;
            ucc_datatype_t   dtype    = coll->dst.info_v.datatype;
            int *            displs   = (int *)coll->dst.info_v.displacements;

            clear_buffer(coll->dst.info_v.buffer, ctxs[r]->rbuf_size, mem_type,
                         0);
            if (TEST_INPLACE == inplace) {
                UCC_CHECK(ucc_mc_memcpy(
                    (void *)((ptrdiff_t)coll->dst.info_v.buffer +
                             displs[r] * ucc_dt_size(dtype)),
                    ctxs[r]->init_buf, ucc_dt_size(dtype) * my_count, mem_type,
                    UCC_MEMORY_TYPE_HOST));
            }
        }
    }
    void data_fini(UccCollCtxVec ctxs)
    {
        for (gtest_ucc_coll_ctx_t* ctx : ctxs) {
            ucc_coll_args_t* coll = ctx->args;
            if (coll->src.info.buffer) { /* no inplace */
                UCC_CHECK(ucc_mc_free(ctx->src_mc_header));
            }
            UCC_CHECK(ucc_mc_free(ctx->dst_mc_header));
            free(coll->dst.info_v.displacements);
            free(coll->dst.info_v.counts);
            ucc_free(ctx->init_buf);
            free(coll);
            free(ctx);
        }
        ctxs.clear();
    }
    bool data_validate(UccCollCtxVec ctxs)
    {
        bool                   ret = true;
        std::vector<uint8_t *> dsts(ctxs.size());

        if (UCC_MEMORY_TYPE_HOST != mem_type) {
            for (int r = 0; r < ctxs.size(); r++) {
                dsts[r] = (uint8_t *) ucc_malloc(ctxs[r]->rbuf_size, "ctxs buf");
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

        for (int i = 0; i < ctxs.size(); i++) {
            size_t rank_size = 0;
            uint8_t *rbuf = dsts[i];
            int is_contig = UCC_COLL_IS_DST_CONTIG(ctxs[i]->args);
            for (int r = 0; r < ctxs.size(); r++) {
                rbuf += rank_size;
                rank_size = ucc_dt_size((ctxs[r])->args->src.info.datatype) *
                        (ctxs[r])->args->src.info.count;
                for (int j = 0; j < rank_size; j++) {
                    if (r != rbuf[j]) {
                        ret = false;
                        break;
                    }
                }
                if (!is_contig) {
                    rbuf += noncontig_padding * ucc_dt_size((ctxs[r])->args->src.info.datatype);
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
};

class test_allgatherv_0 : public test_allgatherv,
        public ::testing::WithParamInterface<Param_0> {};

UCC_TEST_P(test_allgatherv_0, single)
{
    const int                 team_id  = std::get<0>(GetParam());
    const ucc_datatype_t      dtype    = std::get<1>(GetParam());
    const ucc_memory_type_t   mem_type = std::get<2>(GetParam());
    const int                 count    = std::get<3>(GetParam());
    const gtest_ucc_inplace_t inplace  = std::get<4>(GetParam());
    const bool                contig   = std::get<5>(GetParam());
    UccTeam_h                 team     = UccJob::getStaticTeams()[team_id];
    int                       size     = team->procs.size();
    UccCollCtxVec             ctxs;

    set_inplace(inplace);
    set_contig(contig);
    SET_MEM_TYPE(mem_type);

    data_init(size, dtype, count, ctxs, false);
    UccReq    req(team, ctxs);
    req.start();
    req.wait();
    EXPECT_EQ(true, data_validate(ctxs));;
    data_fini(ctxs);
}

UCC_TEST_P(test_allgatherv_0, single_persistent)
{
    const int                 team_id = std::get<0>(GetParam());
    const ucc_datatype_t      dtype   = std::get<1>(GetParam());
    const ucc_memory_type_t   mem_type = std::get<2>(GetParam());
    const int                 count    = std::get<3>(GetParam());
    const gtest_ucc_inplace_t inplace  = std::get<4>(GetParam());
    const bool                contig   = std::get<5>(GetParam());
    UccTeam_h                 team     = UccJob::getStaticTeams()[team_id];
    int                       size     = team->procs.size();
    const int                 n_calls  = 3;
    UccCollCtxVec             ctxs;

    set_inplace(inplace);
    set_contig(contig);
    SET_MEM_TYPE(mem_type);

    data_init(size, dtype, count, ctxs, true);
    UccReq req(team, ctxs);

    for (auto i = 0; i < n_calls; i++) {
        req.start();
        req.wait();
        EXPECT_EQ(true, data_validate(ctxs));
        reset(ctxs);
    }
    data_fini(ctxs);
}

INSTANTIATE_TEST_CASE_P(
    , test_allgatherv_0,
    ::testing::Combine(
        ::testing::Range(1, UccJob::nStaticTeams), // team_ids
        PREDEFINED_DTYPES,
#ifdef HAVE_CUDA
        ::testing::Values(UCC_MEMORY_TYPE_HOST, UCC_MEMORY_TYPE_CUDA,
                          UCC_MEMORY_TYPE_CUDA_MANAGED), // mem type
#else
        ::testing::Values(UCC_MEMORY_TYPE_HOST),
#endif
        ::testing::Values(1,3,8192), // count
        ::testing::Values(TEST_INPLACE, TEST_NO_INPLACE), // inplace
        ::testing::Bool() // contig dst buf displacements
        ));

class test_allgatherv_1 : public test_allgatherv,
        public ::testing::WithParamInterface<Param_1> {};

UCC_TEST_P(test_allgatherv_1, multiple)
{
    const ucc_datatype_t       dtype    = std::get<0>(GetParam());
    const ucc_memory_type_t    mem_type = std::get<1>(GetParam());
    const int                  count    = std::get<2>(GetParam());
    const gtest_ucc_inplace_t  inplace  = std::get<3>(GetParam());
    const bool                 contig   = std::get<4>(GetParam());
    std::vector<UccReq>        reqs;
    std::vector<UccCollCtxVec> ctxs;

    for (int tid = 0; tid < UccJob::nStaticTeams; tid++) {
        UccTeam_h       team = UccJob::getStaticTeams()[tid];
        int             size = team->procs.size();
        UccCollCtxVec   ctx;

        this->set_inplace(inplace);
        this->set_contig(contig);
        SET_MEM_TYPE(mem_type);

        data_init(size, dtype, count, ctx, false);
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
    , test_allgatherv_1,
    ::testing::Combine(
        PREDEFINED_DTYPES,
#ifdef HAVE_CUDA
        ::testing::Values(UCC_MEMORY_TYPE_HOST, UCC_MEMORY_TYPE_CUDA,
                          UCC_MEMORY_TYPE_CUDA_MANAGED),
#else
        ::testing::Values(UCC_MEMORY_TYPE_HOST),
#endif
        ::testing::Values(1,3,8192), // count
        ::testing::Values(TEST_INPLACE, TEST_NO_INPLACE),
        ::testing::Bool()) // dst buf contig
    );

class test_allgatherv_alg : public test_allgatherv,
        public ::testing::WithParamInterface<Param_2> {};

UCC_TEST_P(test_allgatherv_alg, alg)
{
    const ucc_datatype_t      dtype    = std::get<0>(GetParam());
    const ucc_memory_type_t   mem_type = std::get<1>(GetParam());
    const int                 count    = std::get<2>(GetParam());
    const gtest_ucc_inplace_t inplace  = std::get<3>(GetParam());
    const bool                contig   = std::get<5>(GetParam());
    int                       n_procs  = 5;
    char                      tune[32];

    sprintf(tune, "allgatherv:@%s:inf", std::get<4>(GetParam()).c_str());
    ucc_job_env_t env     = {{"UCC_CL_BASIC_TUNE", "inf"},
                             {"UCC_TL_UCP_TUNE", tune}};
    UccJob        job(n_procs, UccJob::UCC_JOB_CTX_GLOBAL, env);
    UccTeam_h     team    = job.create_team(n_procs);
    UccCollCtxVec ctxs;

    set_inplace(inplace);
    set_contig(contig);
    SET_MEM_TYPE(mem_type);

    data_init(n_procs, dtype, count, ctxs, false);
    UccReq    req(team, ctxs);
    req.start();
    req.wait();
    EXPECT_EQ(true, data_validate(ctxs));
    data_fini(ctxs);
}

INSTANTIATE_TEST_CASE_P(
    , test_allgatherv_alg,
    ::testing::Combine(
        PREDEFINED_DTYPES,
#ifdef HAVE_CUDA
        ::testing::Values(UCC_MEMORY_TYPE_HOST, UCC_MEMORY_TYPE_CUDA,
                          UCC_MEMORY_TYPE_CUDA_MANAGED),
#else
        ::testing::Values(UCC_MEMORY_TYPE_HOST),
#endif
        ::testing::Values(1,3,8192), // count
        ::testing::Values(TEST_INPLACE, TEST_NO_INPLACE),
        ::testing::Values("knomial", "ring"),
        ::testing::Bool()), // dst buf contig
        [](const testing::TestParamInfo<test_allgatherv_alg::ParamType>& info) {
            std::string name;
            name += ucc_datatype_str(std::get<0>(info.param));
            name += std::string("_") + std::string(ucc_mem_type_str(std::get<1>(info.param)));
            name += std::string("_count_")+std::to_string(std::get<2>(info.param));
            name += std::string("_inplace_")+std::to_string(std::get<3>(info.param));
            name += std::string("_contig_")+std::to_string(std::get<5>(info.param));
            name += std::string("_")+std::get<4>(info.param);
            return name;
        }
    );
/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * One-sided allgatherv (put variant, plan 5.2) gtests.
 *
 * Rank r's src block holds count[r] elements (pattern ((i + r) % 256) per
 * byte). After the allgatherv, rank r's dst block p sits at offset
 * displs[p]*dt and must hold rank p's pattern for count[p] elements. The
 * gaps between blocks (the noncontig_padding) are never touched by the puts.
 */

#include "common/test_ucc.h"
#include "utils/ucc_math.h"
#include "utils/ucc_log.h"
#include "utils/ucc_coll_utils.h"

using AvoParam = std::tuple<int, ucc_datatype_t>;

static const int ALLGATHERV_ONESIDED_SLOT = 0;

class test_allgatherv_onesided : public ucc::test,
                                 public ::testing::WithParamInterface<AvoParam>
{
  public:
    /* Each rank r's src block holds count[r] = (nprocs - r) * base_count
     * elements, pattern ((i + r) % 256) per byte. The dst is laid out with
     * per-rank count[p] and cumulative displs[p] (uniform across ranks). */
    void os_data_init(UccTeam_h team, size_t base_count, ucc_datatype_t dtype,
                      UccCollCtxVec &ctxs)
    {
        int    nprocs = team->procs.size();
        size_t dt     = ucc_dt_size(dtype);

        ctxs.resize(nprocs);
        for (int r = 0; r < nprocs; r++) {
            ucc_coll_args_t *coll =
                (ucc_coll_args_t *)calloc(1, sizeof(ucc_coll_args_t));
            int *counts = (int *)malloc(sizeof(int) * nprocs);
            int *displs = (int *)malloc(sizeof(int) * nprocs);
            size_t disp_counter = 0;
            for (int i = 0; i < nprocs; i++) {
                counts[i] = (nprocs - i) * base_count;
                displs[i] = disp_counter;
                disp_counter += counts[i];
            }
            ctxs[r] =
                (gtest_ucc_coll_ctx_t *)calloc(1, sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args = coll;

            coll->coll_type = UCC_COLL_TYPE_ALLGATHERV;
            coll->mask      = UCC_COLL_ARGS_FIELD_FLAGS |
                              UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER;
            coll->flags = UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS;

            coll->src.info.mem_type  = UCC_MEMORY_TYPE_HOST;
            coll->src.info.datatype  = dtype;
            coll->src.info.count     = (ucc_count_t)counts[r];
            coll->src.info.buffer    = team->procs[r].p->onesided_buf[0];

            for (size_t i = 0; i < (size_t)counts[r] * dt; i++) {
                ((uint8_t *)team->procs[r].p->onesided_buf[0])[i] =
                    (uint8_t)((i + r) % 256);
            }

            coll->dst.info_v.mem_type    = UCC_MEMORY_TYPE_HOST;
            coll->dst.info_v.datatype    = dtype;
            coll->dst.info_v.counts      = (ucc_count_t *)counts;
            coll->dst.info_v.displacements =
                (ucc_aint_t *)displs;
            coll->dst.info_v.buffer   = team->procs[r].p->onesided_buf[1];

            coll->global_work_buffer = team->procs[r].p->onesided_buf[2];
            ctxs[r]->rbuf_size       = disp_counter * dt;

            clear_buffer(team->procs[r].p->onesided_buf[1], disp_counter * dt,
                         UCC_MEMORY_TYPE_HOST, 0);
        }
    }

    void os_data_fini(UccCollCtxVec ctxs)
    {
        for (gtest_ucc_coll_ctx_t *ctx : ctxs) {
            ucc_coll_args_t *coll = (ucc_coll_args_t *)ctx->args;
            free(coll->dst.info_v.counts);
            free(coll->dst.info_v.displacements);
            free(coll);
            free(ctx);
        }
        ctxs.clear();
    }

    /* Rank r's dst block p (at displs[p]*dt, count[p] elems) must hold rank
     * p's pattern ((i + p) % 256). The gaps between blocks are zeroed by
     * init and untouched by the puts, so they must remain 0. */
    bool os_data_validate(UccTeam_h team, UccCollCtxVec ctxs, size_t base_count,
                          ucc_datatype_t dtype)
    {
        int    nprocs = team->procs.size();
        size_t dt     = ucc_dt_size(dtype);

        for (int r = 0; r < nprocs; r++) {
            ucc_coll_args_t *coll = (ucc_coll_args_t *)ctxs[r]->args;
            int *            displs   = (int *)coll->dst.info_v.displacements;
            int *            counts   = (int *)coll->dst.info_v.counts;
            uint8_t         *dsts     = (uint8_t *)team->procs[r].p->onesided_buf[1];
            for (int p = 0; p < nprocs; p++) {
                for (size_t i = 0; i < (size_t)counts[p] * dt; i++) {
                    if ((uint8_t)((i + p) % 256) !=
                        dsts[(size_t)displs[p] * dt + i]) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
};

/*
 * Single one-sided allgatherv across sizes {1,2,3,4,8,15,16} and a datatype
 * sweep, with per-rank variable block sizes. Size 1 is skipped: the UCP TL
 * has no size-1 teams.
 */
UCC_TEST_P(test_allgatherv_onesided, single_onesided)
{
    const int            size  = std::get<0>(GetParam());
    const ucc_datatype_t dtype = std::get<1>(GetParam());
    const size_t         base_count = 512; /* per-rank block = (size - r) * base_count */
    ucc_job_env_t        env   = {{"UCC_TL_UCP_TUNE",
                                   "allgatherv:0-inf:@onesided"}};
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams.";
    }

    UccTeam_h     team = job.create_team(size, true, true, true);
    UccCollCtxVec ctxs;

    os_data_init(team, base_count, dtype, ctxs);
    UccReq req(team, ctxs);
    ASSERT_EQ(UCC_OK, req.status);
    req.start();
    ucc_status_t st = req.wait();
    EXPECT_EQ(UCC_OK, st);

    EXPECT_TRUE(os_data_validate(team, ctxs, base_count, dtype))
        << "onesided allgatherv data mismatch, size=" << size;

    /* Every rank's local slot 0 advanced by exactly size this round
     * (size-1 remote signals + 1 self-increment). A two-sided fallback never
     * touches the work buffer, so this also proves the one-sided algorithm
     * was selected. */
    for (int r = 0; r < size; r++) {
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        EXPECT_EQ((long)size, slot[ALLGATHERV_ONESIDED_SLOT])
            << "onesided allgatherv slot, rank=" << r << " size=" << size;
    }

    os_data_fini(ctxs);
}

/*
 * Two one-sided allgathervs back-to-back on the same team with no barrier
 * between: the I7 counter-reuse regression. Every rank's slot 0 must reach
 * 2*size after the second round, and the data must still validate each round.
 */
UCC_TEST_P(test_allgatherv_onesided, multiple_onesided)
{
    const int            size  = std::get<0>(GetParam());
    const ucc_datatype_t dtype = std::get<1>(GetParam());
    const size_t         base_count = 512;
    ucc_job_env_t        env   = {{"UCC_TL_UCP_TUNE",
                                   "allgatherv:0-inf:@onesided"}};
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams.";
    }

    UccTeam_h     team = job.create_team(size, true, true, true);
    UccCollCtxVec ctxs;

    os_data_init(team, base_count, dtype, ctxs);

    for (int call = 0; call < 2; call++) {
        UccReq req(team, ctxs);
        ASSERT_EQ(UCC_OK, req.status);
        req.start();
        ucc_status_t st = req.wait();
        EXPECT_EQ(UCC_OK, st);
        EXPECT_TRUE(os_data_validate(team, ctxs, base_count, dtype))
            << "onesided allgatherv back-to-back data mismatch, call=" << call;
        for (int r = 0; r < size; r++) {
            long *slot = (long *)team->procs[r].p->onesided_buf[2];
            EXPECT_EQ((long)((call + 1) * size), slot[ALLGATHERV_ONESIDED_SLOT])
                << "onesided allgatherv back-to-back slot, rank=" << r
                << " call=" << call;
        }
        /* Re-arm the dst so the second allgatherv's puts overwrite zeroed
         * (not stale) data, keeping the validation meaningful. */
        ucc_coll_args_t *coll = (ucc_coll_args_t *)ctxs[0]->args;
        int *counts = (int *)coll->dst.info_v.counts;
        size_t total = 0;
        for (int r2 = 0; r2 < size; r2++) {
            total += (size_t)counts[r2];
        }
        for (int r = 0; r < size; r++) {
            clear_buffer(team->procs[r].p->onesided_buf[1],
                         total * ucc_dt_size(dtype), UCC_MEMORY_TYPE_HOST, 0);
        }
    }

    os_data_fini(ctxs);
}

INSTANTIATE_TEST_CASE_P(
    , test_allgatherv_onesided,
    ::testing::Combine(
        ::testing::Values(1, 2, 3, 4, 8, 15, 16), // size
        ::testing::Values(UCC_DT_INT8, UCC_DT_INT32, UCC_DT_INT64,
                          UCC_DT_UINT8, UCC_DT_FLOAT64))); // dtype
