/**
 * Copyright (c) 2022-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "core/test_mc_reduce.h"
#include "common/test_ucc.h"
#include "utils/ucc_math.h"

#include <array>

template <typename T>
class test_reduce_scatter : public UccCollArgs, public testing::Test {
  public:
    virtual void TestBody(){};
    void data_init(int nprocs, ucc_datatype_t dt, size_t count,
                   UccCollCtxVec &ctxs, bool persistent)
    {
        size_t rcount;
        ctxs.resize(nprocs);
        if (count < nprocs) {
            count = nprocs;
        }
        count  = count - (count % nprocs);
        rcount = count / nprocs;
        if (TEST_INPLACE == inplace) {
            rcount = count;
        }

        for (int r = 0; r < nprocs; r++) {
            ucc_coll_args_t *coll =
                (ucc_coll_args_t *)calloc(1, sizeof(ucc_coll_args_t));

            ctxs[r] =
                (gtest_ucc_coll_ctx_t *)calloc(1, sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args = coll;

            coll->mask      = 0;
            coll->coll_type = UCC_COLL_TYPE_REDUCE_SCATTER;
            coll->op        = T::redop;

            ctxs[r]->init_buf = ucc_malloc(ucc_dt_size(dt) * count, "init buf");
            EXPECT_NE(ctxs[r]->init_buf, nullptr);
            for (int i = 0; i < count; i++) {
                typename T::type *ptr;
                ptr = (typename T::type *)ctxs[r]->init_buf;
                /* need to limit the init value so that "prod" operation
                   would not grow too large. We have teams up to 16 procs
                   in gtest, this would result in prod ~2**48 */
                ptr[i] = (typename T::type)((i + r + 1) % 8);
            }

            UCC_CHECK(ucc_mc_alloc(&ctxs[r]->dst_mc_header,
                                   ucc_dt_size(dt) * rcount, mem_type));
            coll->dst.info.buffer = ctxs[r]->dst_mc_header->addr;
            coll->src.info.buffer = NULL;
            if (TEST_INPLACE == inplace) {
                coll->mask |= UCC_COLL_ARGS_FIELD_FLAGS;
                coll->flags |= UCC_COLL_ARGS_FLAG_IN_PLACE;
                UCC_CHECK(ucc_mc_memcpy(
                    coll->dst.info.buffer, ctxs[r]->init_buf,
                    ucc_dt_size(dt) * rcount, mem_type, UCC_MEMORY_TYPE_HOST));
                coll->src.info.mem_type = UCC_MEMORY_TYPE_UNKNOWN;
                coll->src.info.count    = SIZE_MAX;
                coll->src.info.datatype = (ucc_datatype_t)-1;
            } else {
                UCC_CHECK(ucc_mc_alloc(&ctxs[r]->src_mc_header,
                                       ucc_dt_size(dt) * count, mem_type));
                coll->src.info.buffer = ctxs[r]->src_mc_header->addr;
                UCC_CHECK(ucc_mc_memcpy(
                    coll->src.info.buffer, ctxs[r]->init_buf,
                    ucc_dt_size(dt) * count, mem_type, UCC_MEMORY_TYPE_HOST));
                coll->src.info.mem_type = mem_type;
                coll->src.info.count    = (ucc_count_t)count;
                coll->src.info.datatype = dt;
            }
            coll->dst.info.mem_type = mem_type;
            coll->dst.info.count    = (ucc_count_t)rcount;
            coll->dst.info.datatype = dt;
            if (persistent) {
                coll->mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
                coll->flags |= UCC_COLL_ARGS_FLAG_PERSISTENT;
            }
        }
    }
    void data_fini(UccCollCtxVec ctxs)
    {
        for (gtest_ucc_coll_ctx_t *ctx : ctxs) {
            ucc_coll_args_t *coll = ctx->args;
            if (coll->src.info.buffer) { /* no inplace */
                UCC_CHECK(ucc_mc_free(ctx->src_mc_header));
            }
            UCC_CHECK(ucc_mc_free(ctx->dst_mc_header));
            ucc_free(ctx->init_buf);
            free(coll);
            free(ctx);
        }
        ctxs.clear();
    }
    void reset(UccCollCtxVec ctxs)
    {
        for (auto r = 0; r < ctxs.size(); r++) {
            ucc_coll_args_t *coll  = ctxs[r]->args;
            size_t           count = coll->dst.info.count;
            ucc_datatype_t   dtype = coll->dst.info.datatype;
            clear_buffer(coll->dst.info.buffer, count * ucc_dt_size(dtype),
                         mem_type, 0);

            if (TEST_INPLACE == inplace) {
                UCC_CHECK(ucc_mc_memcpy(coll->dst.info.buffer,
                                        ctxs[r]->init_buf,
                                        ucc_dt_size(dtype) * count, mem_type,
                                        UCC_MEMORY_TYPE_HOST));
            }
        }
    }
    bool data_validate(UccCollCtxVec ctxs)
    {
        size_t            total_count, rcount, offset;
        typename T::type *dst, *dst_p;

        if (TEST_INPLACE != inplace) {
            total_count = (ctxs[0])->args->src.info.count;
            rcount      = (ctxs[0])->args->dst.info.count;
        } else {
            total_count = (ctxs[0])->args->dst.info.count;
            rcount      = total_count / ctxs.size();
        }

        ucc_assert(rcount * ctxs.size() == total_count);

        dst = (typename T::type *)ucc_malloc(
            total_count * sizeof(typename T::type), "dst buf");
        dst_p = dst;
        for (int r = 0; r < ctxs.size(); r++) {
            offset = (TEST_INPLACE == inplace)
                         ? rcount * r * sizeof(typename T::type)
                         : 0;
            UCC_CHECK(ucc_mc_memcpy(
                dst_p, PTR_OFFSET(ctxs[r]->args->dst.info.buffer, offset),
                rcount * sizeof(typename T::type), UCC_MEMORY_TYPE_HOST,
                mem_type));
            dst_p += rcount;
        }

        for (int i = 0; i < total_count; i++) {
            typename T::type res =
                ((typename T::type *)((ctxs[0])->init_buf))[i];
            for (int r = 1; r < ctxs.size(); r++) {
                res = T::do_op(res,
                               ((typename T::type *)((ctxs[r])->init_buf))[i]);
            }
            if (T::redop == UCC_OP_AVG) {
                res = res / (typename T::type)ctxs.size();
            }
            T::assert_equal(res, dst[i]);
        }
        ucc_free(dst);

        return true;
    }
};

template<typename T>
class test_reduce_scatter_host : public test_reduce_scatter<T> {};

template<typename T>
class test_reduce_scatter_cuda : public test_reduce_scatter<T> {};

TYPED_TEST_CASE(test_reduce_scatter_host, CollReduceTypeOpsHost);
TYPED_TEST_CASE(test_reduce_scatter_cuda, CollReduceTypeOpsCuda);

#define TEST_DECLARE(_mem_type, _inplace, _repeat, _persistent)                \
    {                                                                          \
        std::array<int, 1> counts{123};                                        \
        CHECK_TYPE_OP_SKIP(TypeParam::dt, TypeParam::redop, _mem_type);        \
        for (int tid = 0; tid < UccJob::nStaticTeams; tid++) {                 \
            for (int count : counts) {                                         \
                UccTeam_h     team = UccJob::getStaticTeams()[tid];            \
                int           size = team->procs.size();                       \
                UccCollCtxVec ctxs;                                            \
                SET_MEM_TYPE(_mem_type);                                       \
                this->set_inplace(_inplace);                                   \
                this->data_init(size, TypeParam::dt, count, ctxs, _persistent);\
                UccReq req(team, ctxs);                                        \
                CHECK_REQ_NOT_SUPPORTED_SKIP(req, this->data_fini(ctxs));      \
                for (auto i = 0; i < _repeat; i++) {                           \
                    req.start();                                               \
                    req.wait();                                                \
                    EXPECT_EQ(true, this->data_validate(ctxs));                \
                    this->reset(ctxs);                                         \
                }                                                              \
                this->data_fini(ctxs);                                         \
            }                                                                  \
        }                                                                      \
    }

TYPED_TEST(test_reduce_scatter_host, single)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_NO_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatter_host, single_persistent)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_NO_INPLACE, 3, 1);
}

TYPED_TEST(test_reduce_scatter_host, single_inplace)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatter_host, single_persistent_inplace)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_INPLACE, 3, 1);
}

#ifdef HAVE_CUDA
TYPED_TEST(test_reduce_scatter_cuda, single)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_NO_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatter_cuda, single_persistent)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_NO_INPLACE, 3, 1);
}

TYPED_TEST(test_reduce_scatter_cuda, single_inplace)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatter_cuda, single_persistent_inplace)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_INPLACE, 3, 1);
}
TYPED_TEST(test_reduce_scatter_cuda, single_managed)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_NO_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatter_cuda, single_persistent_managed)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_NO_INPLACE, 3, 1);
}

TYPED_TEST(test_reduce_scatter_cuda, single_inplace_managed)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatter_cuda, single_persistent_inplace_managed)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_INPLACE, 3, 1);
}
#endif

#define TEST_DECLARE_MULTIPLE(_mem_type, _inplace)                             \
    {                                                                          \
        std::array<int, 3> counts{4, 256, 65536};                              \
        CHECK_TYPE_OP_SKIP(TypeParam::dt, TypeParam::redop, _mem_type);        \
        for (int count : counts) {                                             \
            std::vector<UccReq>        reqs;                                   \
            std::vector<UccCollCtxVec> ctxs;                                   \
            for (int tid = 0; tid < UccJob::nStaticTeams; tid++) {             \
                UccTeam_h     team = UccJob::getStaticTeams()[tid];            \
                int           size = team->procs.size();                       \
                UccCollCtxVec ctx;                                             \
                this->set_inplace(_inplace);                                   \
                SET_MEM_TYPE(_mem_type);                                       \
                this->data_init(size, TypeParam::dt, count, ctx, false);       \
                ctxs.push_back(ctx);                                           \
                reqs.push_back(UccReq(team, ctx));                             \
                CHECK_REQ_NOT_SUPPORTED_SKIP(reqs.back(),                      \
                                             DATA_FINI_ALL(this, ctxs));       \
            }                                                                  \
            UccReq::startall(reqs);                                            \
            UccReq::waitall(reqs);                                             \
            for (auto ctx : ctxs) {                                            \
                EXPECT_EQ(true, this->data_validate(ctx));                     \
            }                                                                  \
            DATA_FINI_ALL(this, ctxs);                                         \
        }                                                                      \
    }

TYPED_TEST(test_reduce_scatter_host, multiple)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_HOST, TEST_NO_INPLACE);
}

TYPED_TEST(test_reduce_scatter_host, multiple_inplace)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_HOST, TEST_INPLACE);
}

#ifdef HAVE_CUDA
TYPED_TEST(test_reduce_scatter_cuda, multiple)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA, TEST_NO_INPLACE);
}

TYPED_TEST(test_reduce_scatter_cuda, multiple_inplace)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA, TEST_INPLACE);
}
TYPED_TEST(test_reduce_scatter_cuda, multiple_managed)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_NO_INPLACE);
}

TYPED_TEST(test_reduce_scatter_cuda, multiple_inplace_managed)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_INPLACE);
}
#endif

using Param_0 = std::tuple<ucc_job_env_t>;
class test_reduce_scatter_alg
    : public ucc::test,
      public ::testing::WithParamInterface<Param_0> {
};

UCC_TEST_P(test_reduce_scatter_alg,)
{
    test_reduce_scatter<TypeOpPair<UCC_DT_INT32, sum>> rs_test;
    int                                                n_procs = 15;
    const ucc_job_env_t     env   = std::get<0>(GetParam());
    UccJob                  job(n_procs, UccJob::UCC_JOB_CTX_GLOBAL, env);
    UccTeam_h               team   = job.create_team(n_procs);
    int                     repeat = 3;
    UccCollCtxVec           ctxs;
    std::vector<ucc_memory_type_t> mt = {UCC_MEMORY_TYPE_HOST};

    if (UCC_OK == ucc_mc_available(UCC_MEMORY_TYPE_CUDA)) {
        mt.push_back(UCC_MEMORY_TYPE_CUDA);
    }
    if (UCC_OK == ucc_mc_available(UCC_MEMORY_TYPE_CUDA_MANAGED)) {
        mt.push_back(UCC_MEMORY_TYPE_CUDA_MANAGED);
    }

    for (auto count : {65536, 123567}) {
        for (auto inplace : {TEST_NO_INPLACE, TEST_INPLACE}) {
            for (auto m : mt) {
                rs_test.set_mem_type(m);
                rs_test.set_inplace(inplace);
                rs_test.data_init(n_procs, UCC_DT_INT32, count, ctxs, true);
                UccReq req(team, ctxs);

                for (auto i = 0; i < repeat; i++) {
                    req.start();
                    req.wait();
                    EXPECT_EQ(true, rs_test.data_validate(ctxs));
                    rs_test.reset(ctxs);
                }
                rs_test.data_fini(ctxs);
            }
        }
    }
}

ucc_job_env_t ring_unidir_env = {{"name", "ring_unidirectional"},
                                 {"UCC_CL_BASIC_TUNE", "inf"},
                                 {"UCC_TL_UCP_TUNE", "reduce_scatter:@ring:inf"},
                                 {"UCC_TL_UCP_REDUCE_SCATTER_RING_BIDIRECTIONAL", "n"}};

ucc_job_env_t ring_bidir_env = {{"name", "ring_bidirectional"},
                                {"UCC_CL_BASIC_TUNE", "inf"},
                                {"UCC_TL_UCP_TUNE", "reduce_scatter:@ring:inf"},
                                {"UCC_TL_UCP_REDUCE_SCATTER_RING_BIDIRECTIONAL", "y"}};

ucc_job_env_t knomial = {{"name", "knomial"},
                         {"UCC_CL_BASIC_TUNE", "inf"},
                         {"UCC_TL_UCP_TUNE", "reduce_scatter:@knomial:inf"}};

INSTANTIATE_TEST_CASE_P(
    , test_reduce_scatter_alg,
        ::testing::Combine(
            ::testing::Values(ring_unidir_env, ring_bidir_env, knomial)),
    [](const testing::TestParamInfo<Param_0>& info) {
        const ucc_job_env_t env   = std::get<0>(info.param);
        return  env[0].second;});

/*
 * One-sided reduce_scatter (plan 6.2). Each rank is the only issuer of RMA
 * toward its own output: for every peer p != rank it put_signals its own
 * block p (src at offset p*rcount) into peer p's internal symmetric scratch
 * segment at offset rank*rcount and signals p's slot 0; the rank's own block
 * is the gap no peer writes, so it is copied in before the combine. Once
 * every block is in scratch, the local combine reduces the size contiguous
 * blocks into dst. Every rank's local slot 0 advances by exactly `size` per
 * round, so a two-sided fallback (ring/knomial) would leave slot 0 at 0 and
 * the slot assertion below fails -- it also proves the algorithm was
 * selected. The algorithm is forced via
 * UCC_TL_UCP_TUNE="reduce_scatter:0-inf:@onesided"; the scratch segment is
 * enabled via UCC_TL_UCP_ONESIDED_SCRATCH_SIZE (it defaults to 0 = disabled,
 * in which case the core falls back to ring). The scratch is a host segment
 * and the local combine writes dst in place, so dst is host (src may be any
 * mapped type); the values are small non-negative integers and the op is
 * SUM so the expectation is exact.
 */
static const int REDUCE_SCATTER_ONESIDED_SLOT = 0;

using RsoParam = std::tuple<int, ucc_datatype_t>;

class test_reduce_scatter_onesided
    : public ucc::test, public ::testing::WithParamInterface<RsoParam>
{
  public:
    /* Build one reduce_scatter's per-rank args on the onesided segments:
     * src = onesided_buf[0] (count elements), dst = onesided_buf[1] (rcount
     * elements, host), work buffer = onesided_buf[2]. Rank r's src holds
     * rcount blocks of rcount elements; block p (elements i) holds
     * (i + p + r) % 8, so the expected dst of rank r (block r) is the sum
     * over every rank q of (i + r + q) % 8. */
    void os_data_init(UccTeam_h team, size_t count, ucc_datatype_t dtype,
                      UccCollCtxVec &ctxs)
    {
        int        nprocs = team->procs.size();
        size_t     dt     = ucc_dt_size(dtype);
        size_t     rcount = count / (size_t)nprocs;

        ctxs.resize(nprocs);
        for (int r = 0; r < nprocs; r++) {
            ucc_coll_args_t *coll =
                (ucc_coll_args_t *)calloc(1, sizeof(ucc_coll_args_t));
            ctxs[r] =
                (gtest_ucc_coll_ctx_t *)calloc(1, sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args = coll;

            coll->coll_type = UCC_COLL_TYPE_REDUCE_SCATTER;
            coll->op        = UCC_OP_SUM;
            coll->mask      = UCC_COLL_ARGS_FIELD_FLAGS |
                              UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER;
            coll->flags     = UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS;

            coll->src.info.mem_type  = UCC_MEMORY_TYPE_HOST;
            coll->src.info.datatype  = dtype;
            coll->src.info.count     = (ucc_count_t)count;
            coll->src.info.buffer    = team->procs[r].p->onesided_buf[0];
            /* INT32 only (see INSTANTIATE): fill each of the rcount blocks of
             * rcount elements; element i of block p holds (i + p + r) % 8. */
            int32_t *src = (int32_t *)team->procs[r].p->onesided_buf[0];
            for (int p = 0; p < nprocs; p++) {
                for (size_t i = 0; i < rcount; i++) {
                    src[p * rcount + i] = (int32_t)((i + p + r) % 8);
                }
            }

            coll->dst.info.mem_type  = UCC_MEMORY_TYPE_HOST;
            coll->dst.info.datatype  = dtype;
            coll->dst.info.count     = (ucc_count_t)rcount;
            coll->dst.info.buffer    = team->procs[r].p->onesided_buf[1];

            coll->global_work_buffer = team->procs[r].p->onesided_buf[2];
            ctxs[r]->rbuf_size       = rcount * dt;
            clear_buffer(team->procs[r].p->onesided_buf[1], rcount * dt,
                         UCC_MEMORY_TYPE_HOST, 0);
        }
    }

    void os_data_fini(UccCollCtxVec ctxs)
    {
        for (gtest_ucc_coll_ctx_t *ctx : ctxs) {
            free(ctx->args);
            free(ctx);
        }
        ctxs.clear();
    }

    /* Rank r's dst must hold, for its block r: sum over every rank q of
     * (i + r + q) % 8 (the SUM reduce of every rank's r-th block). */
    bool os_data_validate(UccTeam_h team, size_t count, ucc_datatype_t dtype)
    {
        int    nprocs = team->procs.size();
        size_t rcount = count / (size_t)nprocs;

        for (int r = 0; r < nprocs; r++) {
            int32_t *dst = (int32_t *)team->procs[r].p->onesided_buf[1];
            for (size_t i = 0; i < rcount; i++) {
                int res = 0;
                for (int q = 0; q < nprocs; q++) {
                    res += (i + r + q) % 8;
                }
                if (dst[i] != (int32_t)res) {
                    return false;
                }
            }
        }
        return true;
    }

    /* Zero the dst segment so a repeat round reduces fresh (not stale) data. */
    void os_reset_dst(UccTeam_h team, size_t count, ucc_datatype_t dtype)
    {
        size_t dt     = ucc_dt_size(dtype);
        size_t rcount = count / (size_t)team->procs.size();
        for (int r = 0; r < (int)team->procs.size(); r++) {
            clear_buffer(team->procs[r].p->onesided_buf[1], rcount * dt,
                         UCC_MEMORY_TYPE_HOST, 0);
        }
    }
};

UCC_TEST_P(test_reduce_scatter_onesided, single_onesided)
{
    const int            size  = std::get<0>(GetParam());
    const ucc_datatype_t dtype = std::get<1>(GetParam());
    const size_t         count = 4608; /* = 48*96; divisible by every size below */
    ucc_job_env_t        env = {{"UCC_TL_UCP_TUNE",
                                 "reduce_scatter:0-inf:@onesided"},
                                {"UCC_TL_UCP_ONESIDED_SCRATCH_SIZE",
                                 "4194304"}};
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams; the self TL "
                       "handles reduce_scatter as a no-op.";
    }

    UccTeam_h     team = job.create_team(size, true, true, true);
    UccCollCtxVec ctxs;

    os_data_init(team, count, dtype, ctxs);
    UccReq req(team, ctxs);
    ASSERT_EQ(UCC_OK, req.status);
    req.start();
    ucc_status_t st = req.wait();
    EXPECT_EQ(UCC_OK, st);

    EXPECT_TRUE(os_data_validate(team, count, dtype))
        << "onesided reduce_scatter data mismatch, size=" << size;

    /* Every rank's local slot 0 advanced by exactly `size` (size-1 remote
     * signals + 1 self-increment). A two-sided fallback never touches the
     * work buffer, so slot 0 would still be 0. */
    for (int r = 0; r < size; r++) {
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        EXPECT_EQ((long)size, slot[REDUCE_SCATTER_ONESIDED_SLOT])
            << "onesided reduce_scatter slot, size=" << size << " rank=" << r;
    }

    os_data_fini(ctxs);
}

/*
 * Two one-sided reduce_scatters back-to-back on the same team with no barrier
 * between: the I7 counter-reuse and scratch-refcount regression. Every rank's
 * slot 0 must reach 2*size after the second round, and the data must still
 * validate each round.
 */
UCC_TEST_P(test_reduce_scatter_onesided, multiple_onesided)
{
    const int            size  = std::get<0>(GetParam());
    const ucc_datatype_t dtype = std::get<1>(GetParam());
    const size_t         count = 4608; /* = 48*96; divisible by every size below */
    ucc_job_env_t        env = {{"UCC_TL_UCP_TUNE",
                                 "reduce_scatter:0-inf:@onesided"},
                                {"UCC_TL_UCP_ONESIDED_SCRATCH_SIZE",
                                 "4194304"}};
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams.";
    }

    UccTeam_h     team = job.create_team(size, true, true, true);
    UccCollCtxVec ctxs;

    os_data_init(team, count, dtype, ctxs);

    for (int call = 0; call < 2; call++) {
        UccReq req(team, ctxs);
        ASSERT_EQ(UCC_OK, req.status);
        req.start();
        ucc_status_t st = req.wait();
        EXPECT_EQ(UCC_OK, st);
        EXPECT_TRUE(os_data_validate(team, count, dtype))
            << "onesided reduce_scatter back-to-back data mismatch, call="
            << call;
        for (int r = 0; r < size; r++) {
            long *slot = (long *)team->procs[r].p->onesided_buf[2];
            EXPECT_EQ((long)(call + 1) * size,
                      slot[REDUCE_SCATTER_ONESIDED_SLOT])
                << "onesided reduce_scatter back-to-back slot, size=" << size
                << " rank=" << r << " call=" << call;
        }
        /* Zero the dst so the second round's combine overwrites clean data. */
        os_reset_dst(team, count, dtype);
    }

    os_data_fini(ctxs);
}

INSTANTIATE_TEST_CASE_P(
    , test_reduce_scatter_onesided,
    ::testing::Combine(
        ::testing::Values(2, 3, 4, 8, 16), // size
        ::testing::Values(UCC_DT_INT32))); // dtype
