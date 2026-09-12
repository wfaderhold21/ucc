/**
 * Copyright (c) 2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 */

#include "core/test_mc_reduce.h"
#include "common/test_ucc.h"
#include "utils/ucc_math.h"
#include "utils/ucc_coll_utils.h"
#include <random>
#include <algorithm>
#include <array>

template <typename T>
class test_reduce_scatterv : public UccCollArgs, public testing::Test {
  public:
    virtual void TestBody(){};
    /* generates uniform random array of counts that sum
       up to total_count */
    std::vector<uint32_t> generate_counts(int nprocs, size_t total)
    {
        std::default_random_engine eng;
        std::vector<uint32_t>      counts, tmp;
        eng.seed(123);
        std::uniform_int_distribution<int> urd(1, total - 1);

        for (int i = 0; i < nprocs - 1; i++) {
            tmp.push_back(urd(eng));
        }
        tmp.push_back(0);
        tmp.push_back(total);
        std::sort(tmp.begin(), tmp.end());

        for (int i = 1; i < tmp.size(); i++) {
            counts.push_back(tmp[i] - tmp[i - 1]);
            total -= counts.back();
        }
        ucc_assert(total == 0);
        return counts;
    }

    void data_init(int nprocs, ucc_datatype_t dt, size_t total_count,
                   UccCollCtxVec &ctxs, bool persistent)

    {
        size_t rcount = total_count;

        ctxs.resize(nprocs);
        auto counts = generate_counts(nprocs, total_count);

        for (int r = 0; r < nprocs; r++) {
            if (TEST_INPLACE != inplace) {
                rcount = counts[r];
            }
            ucc_coll_args_t *coll =
                (ucc_coll_args_t *)calloc(1, sizeof(ucc_coll_args_t));

            ctxs[r] =
                (gtest_ucc_coll_ctx_t *)calloc(1, sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args = coll;

            coll->mask      = 0;
            coll->coll_type = UCC_COLL_TYPE_REDUCE_SCATTERV;
            coll->op        = T::redop;

            ctxs[r]->init_buf =
                ucc_malloc(ucc_dt_size(dt) * total_count, "init buf");
            EXPECT_NE(ctxs[r]->init_buf, nullptr);
            for (int i = 0; i < total_count; i++) {
                typename T::type *ptr;
                ptr = (typename T::type *)ctxs[r]->init_buf;
                /* need to limit the init value so that "prod" operation
                   would not grow too large. We have teams up to 16 procs
                   in gtest, this would result in prod ~2**48 */
                ptr[i] = (typename T::type)((i + r + 1) % 8);
            }

            UCC_CHECK(ucc_mc_alloc(&ctxs[r]->dst_mc_header,
                                   ucc_dt_size(dt) * rcount, mem_type));
            coll->dst.info_v.buffer = ctxs[r]->dst_mc_header->addr;
            coll->src.info.buffer   = NULL;
            if (TEST_INPLACE == inplace) {
                coll->mask |= UCC_COLL_ARGS_FIELD_FLAGS;
                coll->flags |= UCC_COLL_ARGS_FLAG_IN_PLACE;
                UCC_CHECK(ucc_mc_memcpy(
                    coll->dst.info_v.buffer, ctxs[r]->init_buf,
                    ucc_dt_size(dt) * rcount, mem_type, UCC_MEMORY_TYPE_HOST));
                coll->src.info.mem_type = UCC_MEMORY_TYPE_UNKNOWN;
                coll->src.info.count    = SIZE_MAX;
                coll->src.info.datatype = (ucc_datatype_t)-1;
            } else {
                UCC_CHECK(ucc_mc_alloc(&ctxs[r]->src_mc_header,
                                       ucc_dt_size(dt) * total_count,
                                       mem_type));
                coll->src.info.buffer = ctxs[r]->src_mc_header->addr;
                UCC_CHECK(ucc_mc_memcpy(coll->src.info.buffer,
                                        ctxs[r]->init_buf,
                                        ucc_dt_size(dt) * total_count, mem_type,
                                        UCC_MEMORY_TYPE_HOST));
                coll->src.info.mem_type = mem_type;
                coll->src.info.count    = (ucc_count_t)total_count;
                coll->src.info.datatype = dt;
            }
            coll->dst.info_v.mem_type = mem_type;
            coll->dst.info_v.counts =
                (ucc_count_t *)ucc_malloc(nprocs * sizeof(uint32_t), "counts");
            memcpy(coll->dst.info_v.counts, counts.data(),
                   sizeof(uint32_t) * nprocs);
            coll->dst.info_v.datatype = dt;
            if (persistent) {
                coll->mask |= UCC_COLL_ARGS_FIELD_FLAGS;
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
            ucc_free(coll->dst.info_v.counts);
            free(coll);
            free(ctx);
        }
        ctxs.clear();
    }
    size_t get_total_count(UccCollCtxVec ctxs)
    {
        ucc_coll_args_t *coll = ctxs[0]->args;

        return ucc_coll_args_get_total_count(coll, coll->dst.info_v.counts,
                                             ctxs.size());
    }

    void reset(UccCollCtxVec ctxs)
    {
        size_t    total_count = get_total_count(ctxs);
        uint32_t *counts      = (uint32_t *)ctxs[0]->args->dst.info_v.counts;

        for (auto r = 0; r < ctxs.size(); r++) {
            ucc_coll_args_t *coll  = ctxs[r]->args;
            ucc_datatype_t   dtype = coll->dst.info_v.datatype;
            size_t rcount = (TEST_INPLACE == inplace) ? total_count : counts[r];

            clear_buffer(coll->dst.info_v.buffer, rcount * ucc_dt_size(dtype),
                         mem_type, 0);

            if (TEST_INPLACE == inplace) {
                UCC_CHECK(ucc_mc_memcpy(coll->dst.info_v.buffer,
                                        ctxs[r]->init_buf,
                                        ucc_dt_size(dtype) * total_count,
                                        mem_type, UCC_MEMORY_TYPE_HOST));
            }
        }
    }

    size_t check_offset(UccCollCtxVec ctxs, int rank)
    {
        uint32_t *counts = (uint32_t *)ctxs[0]->args->dst.info_v.counts;
        size_t    offset = 0;

        if (TEST_INPLACE == inplace) {
            for (int i = 0; i < rank; i++) {
                offset += counts[i];
            }
        }

        return offset * sizeof(typename T::type);
    }

    bool data_validate(UccCollCtxVec ctxs)
    {
        size_t            total_count = get_total_count(ctxs);
        uint32_t *        counts = (uint32_t *)ctxs[0]->args->dst.info_v.counts;
        size_t            rcount;
        typename T::type *dst, *dst_p;

        dst = (typename T::type *)ucc_malloc(
            total_count * sizeof(typename T::type), "dst buf");
        dst_p = dst;
        for (int r = 0; r < ctxs.size(); r++) {
            rcount = counts[r];
            UCC_CHECK(ucc_mc_memcpy(dst_p,
                                    PTR_OFFSET(ctxs[r]->args->dst.info_v.buffer,
                                               check_offset(ctxs, r)),
                                    rcount * sizeof(typename T::type),
                                    UCC_MEMORY_TYPE_HOST, mem_type));
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
class test_reduce_scatterv_host : public test_reduce_scatterv<T> {};

template<typename T>
class test_reduce_scatterv_cuda : public test_reduce_scatterv<T> {};

TYPED_TEST_CASE(test_reduce_scatterv_host, CollReduceTypeOpsHost);
TYPED_TEST_CASE(test_reduce_scatterv_cuda, CollReduceTypeOpsCuda);

#define TEST_DECLARE(_mem_type, _inplace, _repeat, _persistent)                \
    {                                                                          \
        std::array<int, 3> counts{4, 123, 65536};                              \
        CHECK_TYPE_OP_SKIP(TypeParam::dt, TypeParam::redop, _mem_type);        \
        for (int tid = 0; tid < UccJob::nStaticTeams; tid++) {                 \
            for (int count : counts) {                                         \
                UccTeam_h     team = UccJob::getStaticTeams()[tid];            \
                int           size = team->procs.size();                       \
                UccCollCtxVec ctxs;                                            \
                SET_MEM_TYPE(_mem_type);                                       \
                this->set_inplace(_inplace);                                   \
                this->data_init(size, TypeParam::dt, count, ctxs,              \
                                _persistent);                                  \
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

TYPED_TEST(test_reduce_scatterv_host, single)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_NO_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatterv_host, single_persistent)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_NO_INPLACE, 3, 1);
}

TYPED_TEST(test_reduce_scatterv_host, single_inplace)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatterv_host, single_persistent_inplace)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_INPLACE, 3, 1);
}

#ifdef HAVE_CUDA
TYPED_TEST(test_reduce_scatterv_cuda, single)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_NO_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatterv_cuda, single_persistent)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_NO_INPLACE, 3, 1);
}

TYPED_TEST(test_reduce_scatterv_cuda, single_inplace)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatterv_cuda, single_persistent_inplace)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_INPLACE, 3, 1);
}
TYPED_TEST(test_reduce_scatterv_cuda, single_managed)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_NO_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatterv_cuda, single_persistent_managed)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_NO_INPLACE, 3, 1);
}

TYPED_TEST(test_reduce_scatterv_cuda, single_inplace_managed)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_scatterv_cuda, single_persistent_inplace_managed)
{
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_INPLACE, 3, 1);
}
#endif

#define TEST_DECLARE_MULTIPLE(_mem_type, _inplace)                             \
    {                                                                          \
        std::array<int, 3> counts{4, 123, 65536};                              \
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

TYPED_TEST(test_reduce_scatterv_host, multiple)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_HOST, TEST_NO_INPLACE);
}

TYPED_TEST(test_reduce_scatterv_host, multiple_inplace)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_HOST, TEST_INPLACE);
}

#ifdef HAVE_CUDA
TYPED_TEST(test_reduce_scatterv_cuda, multiple)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA, TEST_NO_INPLACE);
}

TYPED_TEST(test_reduce_scatterv_cuda, multiple_inplace)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA, TEST_INPLACE);
}
TYPED_TEST(test_reduce_scatterv_cuda, multiple_managed)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_NO_INPLACE);
}

TYPED_TEST(test_reduce_scatterv_cuda, multiple_inplace_managed)
{
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_INPLACE);
}
#endif

class test_reduce_scatterv_alg
    : public ucc::test,
      public ::testing::WithParamInterface<std::string> {
};

UCC_TEST_P(test_reduce_scatterv_alg, ring)
{
    test_reduce_scatterv<TypeOpPair<UCC_DT_INT32, sum>> rsv_test;
    int                                                 n_procs = 15;
    std::string                                         bidir   = GetParam();
    ucc_job_env_t env = {{"UCC_CL_BASIC_TUNE", "inf"},
                         {"UCC_TL_UCP_TUNE", "reduce_scatterv:@ring:inf"},
                         {"REDUCE_SCATTERV_RING_BIDIRECTIONAL",
                          bidir == "bidirectional" ? "y" : "n"}};
    UccJob        job(n_procs, UccJob::UCC_JOB_CTX_GLOBAL, env);
    UccTeam_h     team   = job.create_team(n_procs);
    int           repeat = 3;
    UccCollCtxVec ctxs;
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
                rsv_test.set_mem_type(m);
                rsv_test.set_inplace(inplace);
                rsv_test.data_init(n_procs, UCC_DT_INT32, count, ctxs, true);
                UccReq req(team, ctxs);

                for (auto i = 0; i < repeat; i++) {
                    req.start();
                    req.wait();
                    EXPECT_EQ(true, rsv_test.data_validate(ctxs));
                    rsv_test.reset(ctxs);
                }
                rsv_test.data_fini(ctxs);
            }
        }
    }
}
INSTANTIATE_TEST_CASE_P(, test_reduce_scatterv_alg,
                        ::testing::Values("bidirectional", "unidirectional"));

/*
 * One-sided reduce_scatterv (plan 6.3). Mirrors the one-sided reduce_scatter
 * test but with variable per-rank block sizes, so the per-block src/dst
 * offsets and the size*max_count scratch region are exercised. Rank r's src
 * holds blocks of counts[r]=8+16r elements; block p (element i) holds
 * (i + p + r) % 8, so rank r's dst (counts[r] elements) must hold
 * sum over every rank q of (i + r + q) % 8 (the SUM of every rank's r-th
 * block).
 */
static const int REDUCE_SCATTERV_ONESIDED_SLOT = 0;

/* Distinct, positive per-rank counts; size up to 16 keeps max_count small. */
static size_t osv_count_at(int nprocs, int r)
{
    (void)nprocs;
    return (size_t)(8 + 16 * r);
}

class test_reduce_scatterv_onesided
    : public ::testing::TestWithParam<std::tuple<int, ucc_datatype_t>> {
  public:
    void os_data_init(UccTeam_h team, ucc_datatype_t dtype, UccCollCtxVec &ctxs)
    {
        int    nprocs = team->procs.size();
        size_t dt     = ucc_dt_size(dtype);
        size_t total  = 0, max_count = 0;

        for (int r = 0; r < nprocs; r++) {
            total += osv_count_at(nprocs, r);
            max_count = std::max(max_count, osv_count_at(nprocs, r));
        }

        ctxs.resize(nprocs);
        for (int r = 0; r < nprocs; r++) {
            ucc_coll_args_t *coll =
                (ucc_coll_args_t *)calloc(1, sizeof(ucc_coll_args_t));
            ctxs[r] =
                (gtest_ucc_coll_ctx_t *)calloc(1, sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args = coll;

            coll->coll_type = UCC_COLL_TYPE_REDUCE_SCATTERV;
            coll->op        = UCC_OP_SUM;
            coll->mask      = UCC_COLL_ARGS_FIELD_FLAGS |
                              UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER;
            coll->flags     = UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS;

            coll->src.info.mem_type  = UCC_MEMORY_TYPE_HOST;
            coll->src.info.datatype  = dtype;
            coll->src.info.count     = (ucc_count_t)total;
            coll->src.info.buffer    = team->procs[r].p->onesided_buf[0];
            /* INT32 only (see INSTANTIATE): fill each block p of
             * counts[p] elements; element i holds (i + p + r) % 8. */
            int32_t *src = (int32_t *)team->procs[r].p->onesided_buf[0];
            size_t   off = 0;
            for (int p = 0; p < nprocs; p++) {
                size_t cnt = osv_count_at(nprocs, p);
                for (size_t i = 0; i < cnt; i++) {
                    src[off + i] = (int32_t)((i + p + r) % 8);
                }
                off += cnt;
            }

            coll->dst.info_v.mem_type  = UCC_MEMORY_TYPE_HOST;
            coll->dst.info_v.datatype  = dtype;
            coll->dst.info_v.counts =
                (ucc_count_t *)ucc_malloc(nprocs * sizeof(uint32_t), "counts");
            for (int p = 0; p < nprocs; p++) {
                ((uint32_t *)coll->dst.info_v.counts)[p] =
                    (uint32_t)osv_count_at(nprocs, p);
            }
            coll->dst.info_v.buffer = team->procs[r].p->onesided_buf[1];

            coll->global_work_buffer = team->procs[r].p->onesided_buf[2];

            size_t rcount = osv_count_at(nprocs, r);
            ctxs[r]->rbuf_size = rcount * dt;
            clear_buffer(team->procs[r].p->onesided_buf[1], rcount * dt,
                         UCC_MEMORY_TYPE_HOST, 0);
        }
    }

    void os_data_fini(UccCollCtxVec ctxs)
    {
        for (gtest_ucc_coll_ctx_t *ctx : ctxs) {
            ucc_coll_args_t *coll = ctx->args;
            ucc_free(coll->dst.info_v.counts);
            free(coll);
            free(ctx);
        }
        ctxs.clear();
    }

    /* Rank r's dst (counts[r] elements) must hold, for element i:
     * sum over every rank q of (i + r + q) % 8 (the SUM of every rank's
     * r-th block, which is counts[r] elements wide). */
    bool os_data_validate(UccTeam_h team, ucc_datatype_t dtype)
    {
        int nprocs = team->procs.size();
        for (int r = 0; r < nprocs; r++) {
            size_t    rcount = osv_count_at(nprocs, r);
            int32_t *dst    = (int32_t *)team->procs[r].p->onesided_buf[1];
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

    /* Zero each rank's dst segment (counts[r] elements) so a repeat round
     * reduces fresh (not stale) data. */
    void os_reset_dst(UccTeam_h team, ucc_datatype_t dtype)
    {
        int    nprocs = team->procs.size();
        size_t dt     = ucc_dt_size(dtype);
        for (int r = 0; r < nprocs; r++) {
            clear_buffer(team->procs[r].p->onesided_buf[1],
                         osv_count_at(nprocs, r) * dt, UCC_MEMORY_TYPE_HOST, 0);
        }
    }
};

UCC_TEST_P(test_reduce_scatterv_onesided, single_onesided)
{
    const int            size  = std::get<0>(GetParam());
    const ucc_datatype_t dtype = std::get<1>(GetParam());
    ucc_job_env_t        env = {{"UCC_TL_UCP_TUNE",
                                 "reduce_scatterv:0-inf:@onesided"},
                                {"UCC_TL_UCP_ONESIDED_SCRATCH_SIZE",
                                 "4194304"}};
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams; the self TL "
                       "handles reduce_scatterv as a no-op.";
    }

    UccTeam_h     team = job.create_team(size, true, true, true);
    UccCollCtxVec ctxs;

    os_data_init(team, dtype, ctxs);
    UccReq req(team, ctxs);
    ASSERT_EQ(UCC_OK, req.status);
    req.start();
    ucc_status_t st = req.wait();
    EXPECT_EQ(UCC_OK, st);

    EXPECT_TRUE(os_data_validate(team, dtype))
        << "onesided reduce_scatterv data mismatch, size=" << size;

    /* Every rank's local slot 0 advanced by exactly `size` (size-1 remote
     * signals + 1 self-increment). A two-sided fallback never touches the
     * work buffer, so slot 0 would still be 0. */
    for (int r = 0; r < size; r++) {
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        EXPECT_EQ((long)size, slot[REDUCE_SCATTERV_ONESIDED_SLOT])
            << "onesided reduce_scatterv slot, size=" << size << " rank=" << r;
    }

    os_data_fini(ctxs);
}

/*
 * Two one-sided reduce_scattervs back-to-back on the same team with no
 * barrier between: the I7 counter-reuse and scratch-refcount regression.
 * Every rank's slot 0 must reach 2*size after the second round, and the data
 * must still validate each round.
 */
UCC_TEST_P(test_reduce_scatterv_onesided, multiple_onesided)
{
    const int            size  = std::get<0>(GetParam());
    const ucc_datatype_t dtype = std::get<1>(GetParam());
    ucc_job_env_t        env = {{"UCC_TL_UCP_TUNE",
                                 "reduce_scatterv:0-inf:@onesided"},
                                {"UCC_TL_UCP_ONESIDED_SCRATCH_SIZE",
                                 "4194304"}};
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams.";
    }

    UccTeam_h     team = job.create_team(size, true, true, true);
    UccCollCtxVec ctxs;

    os_data_init(team, dtype, ctxs);

    for (int call = 0; call < 2; call++) {
        UccReq req(team, ctxs);
        ASSERT_EQ(UCC_OK, req.status);
        req.start();
        ucc_status_t st = req.wait();
        EXPECT_EQ(UCC_OK, st);
        EXPECT_TRUE(os_data_validate(team, dtype))
            << "onesided reduce_scatterv back-to-back data mismatch, call="
            << call;
        for (int r = 0; r < size; r++) {
            long *slot = (long *)team->procs[r].p->onesided_buf[2];
            EXPECT_EQ((long)(call + 1) * size,
                      slot[REDUCE_SCATTERV_ONESIDED_SLOT])
                << "onesided reduce_scatterv back-to-back slot, size=" << size
                << " rank=" << r << " call=" << call;
        }
        os_reset_dst(team, dtype);
    }

    os_data_fini(ctxs);
}

INSTANTIATE_TEST_CASE_P(
    , test_reduce_scatterv_onesided,
    ::testing::Combine(
        ::testing::Values(2, 3, 4, 8, 16), // size
        ::testing::Values(UCC_DT_INT32))); // dtype
