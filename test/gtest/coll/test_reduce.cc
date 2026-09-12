/**
 * Copyright (c) 2022-2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 *
 * See file LICENSE for terms.
 */

#include "core/test_mc_reduce.h"
#include "common/test_ucc.h"
#include "utils/ucc_math.h"

#include <algorithm>
#include <array>

template<typename T>
class test_reduce : public UccCollArgs, public testing::Test {
  private:
    int root = 0;
  public:
    void data_init(int nprocs, ucc_datatype_t dt, size_t count,
                   UccCollCtxVec &ctxs, bool persistent)
    {
        ctxs.resize(nprocs);
        for (int r = 0; r < nprocs; r++) {
            ucc_coll_args_t *coll = (ucc_coll_args_t*)
                    calloc(1, sizeof(ucc_coll_args_t));

            ctxs[r]           = (gtest_ucc_coll_ctx_t*)calloc(1,
                                sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args     = coll;
            ctxs[r]->init_buf = ucc_malloc(ucc_dt_size(dt) * count,
                                                        "init buf");
            EXPECT_NE(ctxs[r]->init_buf, nullptr);
            for (int i = 0; i < count; i++) {
                typename T::type * ptr;
                ptr = (typename T::type *)ctxs[r]->init_buf;
                /* need to limit the init value so that "prod" operation
                   would not grow too large. We have teams up to 16 procs
                   in gtest, this would result in prod ~2**48 */
                /* bFloat16 will be assigned with the floats matching the
                   uint16_t bit pattern*/
                ptr[i] = (typename T::type)((i + r + 1) % 8);
            }

            coll->coll_type = UCC_COLL_TYPE_REDUCE;
            coll->op        = T::redop;
            coll->root      = root;
            if (r != root || !inplace) {
                coll->src.info.mem_type = mem_type;
                coll->src.info.count    = (ucc_count_t)count;
                coll->src.info.datatype = dt;
                UCC_CHECK(ucc_mc_alloc(&ctxs[r]->src_mc_header,
                                       ucc_dt_size(dt) * count, mem_type));
                coll->src.info.buffer = ctxs[r]->src_mc_header->addr;
                UCC_CHECK(ucc_mc_memcpy(coll->src.info.buffer,
                                        ctxs[r]->init_buf,
                                        ucc_dt_size(dt) * count, mem_type,
                                        UCC_MEMORY_TYPE_HOST));
            }
            if (r == root) {
                coll->dst.info.mem_type = mem_type;
                coll->dst.info.count = (ucc_count_t)count;
                coll->dst.info.datatype = dt;
                UCC_CHECK(ucc_mc_alloc(&ctxs[r]->dst_mc_header,
                                       ucc_dt_size(dt) * count, mem_type));
                coll->dst.info.buffer = ctxs[r]->dst_mc_header->addr;
                if (inplace) {
                    UCC_CHECK(ucc_mc_memcpy(coll->dst.info.buffer,
                              ctxs[r]->init_buf, ucc_dt_size(dt) * count,
                              mem_type, UCC_MEMORY_TYPE_HOST));
                }
            }
            if (inplace) {
                coll->mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
                coll->flags |= UCC_COLL_ARGS_FLAG_IN_PLACE;
            }
            if (persistent) {
                coll->mask  |= UCC_COLL_ARGS_FIELD_FLAGS;
                coll->flags |= UCC_COLL_ARGS_FLAG_PERSISTENT;
            }
        }
    }
    void data_fini(UccCollCtxVec ctxs) {
    	for (auto r = 0; r < ctxs.size(); r++) {
            ucc_coll_args_t* coll = ctxs[r]->args;
            if (r == root) {
                UCC_CHECK(ucc_mc_free(ctxs[r]->dst_mc_header));
            }
            if (r != root || !inplace) {
            	UCC_CHECK(ucc_mc_free(ctxs[r]->src_mc_header));
            }
            ucc_free(ctxs[r]->init_buf);
            free(coll);
            free(ctxs[r]);
        }
        ctxs.clear();
    }
    void reset(UccCollCtxVec ctxs)
    {
        ucc_coll_args_t *coll  = ctxs[root]->args;
        size_t           count = coll->dst.info.count;
        ucc_datatype_t   dtype = coll->dst.info.datatype;
        clear_buffer(coll->dst.info.buffer, count * ucc_dt_size(dtype),
                     mem_type, 0);
		if (TEST_INPLACE == inplace) {
			UCC_CHECK(ucc_mc_memcpy(coll->dst.info.buffer,
                  ctxs[root]->init_buf,
                  ucc_dt_size(dtype) * count, mem_type, UCC_MEMORY_TYPE_HOST));
		}
    }
    bool data_validate(UccCollCtxVec ctxs)
    {
        size_t count = (ctxs[0])->args->src.info.count;
        typename T::type * dsts;

        if (UCC_MEMORY_TYPE_HOST != mem_type) {
            dsts = (typename T::type *)
                    ucc_malloc(count * sizeof(typename T::type), "dsts buf");
            EXPECT_NE(dsts, nullptr);
            UCC_CHECK(ucc_mc_memcpy(dsts, ctxs[root]->args->dst.info.buffer,
                                    count * sizeof(typename T::type),
                                    UCC_MEMORY_TYPE_HOST, mem_type));
        } else {
            dsts = (typename T::type *)ctxs[root]->args->dst.info.buffer;
        }
        for (int i = 0; i < count; i++) {
            typename T::type res =
                    ((typename T::type *)((ctxs[0])->init_buf))[i];
            for (int r = 1; r < ctxs.size(); r++) {
                res = T::do_op(res,
                              ((typename T::type *)((ctxs[r])->init_buf))[i]);
            }
            if (T::redop == UCC_OP_AVG) {
                if (T::dt == UCC_DT_BFLOAT16){
                    float32tobfloat16(bfloat16tofloat32(&res) / (float)ctxs.size(),
                    &res);
                } else {
                    res = res / (typename T::type)ctxs.size();
                }
            }
            T::assert_equal(res, dsts[i]);
        }
        if (UCC_MEMORY_TYPE_HOST != mem_type) {
            ucc_free(dsts);
        }
        return true;
    }
};

template<typename T>
class test_reduce_host : public test_reduce<T> {};

template<typename T>
class test_reduce_cuda : public test_reduce<T> {};

TYPED_TEST_CASE(test_reduce_host, CollReduceTypeOpsHost);
TYPED_TEST_CASE(test_reduce_cuda, CollReduceTypeOpsCuda);

#define TEST_DECLARE(_mem_type, _inplace, _repeat, _persistent)                \
    {                                                                          \
        std::array<int, 3> counts{4, 256, 65536};                              \
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

TYPED_TEST(test_reduce_host, single) {
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_NO_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_host, single_persistent) {
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_NO_INPLACE, 3, 1);
}

TYPED_TEST(test_reduce_host, single_inplace) {
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_host, single_persistent_inplace) {
    TEST_DECLARE(UCC_MEMORY_TYPE_HOST, TEST_INPLACE, 3, 1);
}

#ifdef HAVE_CUDA
TYPED_TEST(test_reduce_cuda, single) {
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_NO_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_cuda, single_persistent) {
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_NO_INPLACE, 3, 1);
}
TYPED_TEST(test_reduce_cuda, single_inplace) {
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_cuda, single_persistent_inplace) {
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA, TEST_INPLACE, 3, 1);
}
TYPED_TEST(test_reduce_cuda, single_managed) {
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_NO_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_cuda, single_persistent_managed) {
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_NO_INPLACE, 3, 1);
}
TYPED_TEST(test_reduce_cuda, single_inplace_managed) {
    TEST_DECLARE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_INPLACE, 1, 0);
}

TYPED_TEST(test_reduce_cuda, single_persistent_inplace_managed) {
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
                reqs.push_back(UccReq(team, ctx));                             \
                CHECK_REQ_NOT_SUPPORTED_SKIP(reqs.back(),                      \
                                             DATA_FINI_ALL(this, ctxs));       \
                ctxs.push_back(ctx);                                           \
            }                                                                  \
            UccReq::startall(reqs);                                            \
            UccReq::waitall(reqs);                                             \
            for (auto ctx : ctxs) {                                            \
                EXPECT_EQ(true, this->data_validate(ctx));                     \
                this->data_fini(ctx);                                          \
            }                                                                  \
        }                                                                      \
    }

TYPED_TEST(test_reduce_host, multiple) {
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_HOST, TEST_NO_INPLACE);
}

TYPED_TEST(test_reduce_host, multiple_inplace) {
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_HOST, TEST_INPLACE);
}

#ifdef HAVE_CUDA
TYPED_TEST(test_reduce_cuda, multiple) {
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA, TEST_NO_INPLACE);
}
TYPED_TEST(test_reduce_cuda, multiple_inplace) {
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA, TEST_INPLACE);
}
TYPED_TEST(test_reduce_cuda, multiple_managed) {
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_NO_INPLACE);
}
TYPED_TEST(test_reduce_cuda, multiple_inplace_managed) {
    TEST_DECLARE_MULTIPLE(UCC_MEMORY_TYPE_CUDA_MANAGED, TEST_INPLACE);
}
#endif

template <typename T> class test_reduce_avg_order : public test_reduce<T> {
};

template <typename T> class test_reduce_dbt : public test_reduce<T> {
};

template <typename T> class test_reduce_2step : public test_reduce<T> {
};

template <typename T> class test_reduce_srg : public test_reduce<T> {
};

#define TEST_DECLARE_WITH_ENV(_env, _n_procs, _persistent)                     \
    {                                                                          \
        UccJob        job(_n_procs, UccJob::UCC_JOB_CTX_GLOBAL, _env);         \
        UccTeam_h     team   = job.create_team(_n_procs);                      \
        int           repeat = _persistent ? 3 : 1;                            \
        UccCollCtxVec ctxs;                                                    \
        std::vector<ucc_memory_type_t> mt = {UCC_MEMORY_TYPE_HOST};            \
        if (UCC_OK == ucc_mc_available(UCC_MEMORY_TYPE_CUDA)) {                \
            mt.push_back(UCC_MEMORY_TYPE_CUDA);                                \
        }                                                                      \
        if (UCC_OK == ucc_mc_available(UCC_MEMORY_TYPE_CUDA_MANAGED)) {        \
            mt.push_back(UCC_MEMORY_TYPE_CUDA_MANAGED);                        \
        }                                                                      \
        for (auto count : {5, 256, 65536}) {                                   \
            for (auto inplace : {TEST_NO_INPLACE, TEST_INPLACE}) {             \
                for (auto m : mt) {                                            \
                    CHECK_TYPE_OP_SKIP(TypeParam::dt, TypeParam::redop, m);    \
                    SET_MEM_TYPE(m);                                           \
                    this->set_inplace(inplace);                                \
                    this->data_init(_n_procs, TypeParam::dt, count, ctxs,      \
                                    _persistent);                              \
                    UccReq req(team, ctxs);                                    \
                    CHECK_REQ_NOT_SUPPORTED_SKIP(req, this->data_fini(ctxs));  \
                    for (auto i = 0; i < repeat; i++) {                        \
                        req.start();                                           \
                        req.wait();                                            \
                        EXPECT_EQ(true, this->data_validate(ctxs));            \
                        this->reset(ctxs);                                     \
                    }                                                          \
                    this->data_fini(ctxs);                                     \
                }                                                              \
            }                                                                  \
        }                                                                      \
    }

TYPED_TEST_CASE(test_reduce_avg_order, CollReduceTypeOpsAvg);
TYPED_TEST_CASE(test_reduce_dbt, CollReduceTypeOpsHost);
TYPED_TEST_CASE(test_reduce_2step, CollReduceTypeOpsHost);
TYPED_TEST_CASE(test_reduce_srg, CollReduceTypeOpsHost);

ucc_job_env_t post_op_env      = {{"UCC_TL_UCP_REDUCE_AVG_PRE_OP", "0"}};
ucc_job_env_t reduce_dbt_env   = {{"UCC_TL_UCP_TUNE", "reduce:@dbt:0-inf:inf"},
                                  {"UCC_CLS", "basic"}};
ucc_job_env_t reduce_2step_env = {{"UCC_CL_HIER_TUNE", "reduce:@2step:0-inf:inf"},
                                  {"UCC_CLS", "all"}};
ucc_job_env_t reduce_srg_env   = {{"UCC_TL_UCP_TUNE", "reduce:@srg:0-inf:inf"},
                                  {"UCC_CLS", "basic"}};
TYPED_TEST(test_reduce_avg_order, avg_post_op) {
    TEST_DECLARE_WITH_ENV(post_op_env, 15, true);
}

TYPED_TEST(test_reduce_dbt, reduce_dbt_shift) {
    TEST_DECLARE_WITH_ENV(reduce_dbt_env, 15, true);
}

TYPED_TEST(test_reduce_dbt, reduce_dbt_mirror) {
    TEST_DECLARE_WITH_ENV(reduce_dbt_env, 16, true);
}

TYPED_TEST(test_reduce_2step, 2step) {
    TEST_DECLARE_WITH_ENV(reduce_2step_env, 16, false);
}

TYPED_TEST(test_reduce_srg, srg) {
    TEST_DECLARE_WITH_ENV(reduce_srg_env, 15, false);
}

/*
 * One-sided reduce (plan 6.4): a knomial tree of put+signal. Root rank's dst
 * holds the SUM over every rank of the matching element. The slot assertion
 * proves the one-sided alg was selected (a two-sided fallback never touches
 * the work buffer) and that the root's level-0 slot advanced by exactly its
 * number of children (per-level, per-rank slot base, I7).
 */
using RdoParam = std::tuple<int, ucc_datatype_t>;

class test_reduce_onesided
    : public ucc::test, public ::testing::WithParamInterface<RdoParam>
{
  public:
    /* Build one reduce's per-rank args on the onesided segments:
     * src = onesided_buf[0] (count elements, host), dst = onesided_buf[1]
     * (count elements, host, only the root's holds the result), work buffer =
     * onesided_buf[2]. Element i of rank r's src holds (i + r + 1) % 8, so the
     * expected root dst[i] is the sum over every rank q of (i + q + 1) % 8. */
    void os_data_init(UccTeam_h team, size_t count, ucc_datatype_t dtype,
                      UccCollCtxVec &ctxs)
    {
        int    nprocs = team->procs.size();
        size_t dt     = ucc_dt_size(dtype);

        ctxs.resize(nprocs);
        for (int r = 0; r < nprocs; r++) {
            ucc_coll_args_t *coll =
                (ucc_coll_args_t *)calloc(1, sizeof(ucc_coll_args_t));
            ctxs[r] =
                (gtest_ucc_coll_ctx_t *)calloc(1, sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args = coll;

            coll->coll_type = UCC_COLL_TYPE_REDUCE;
            coll->op        = UCC_OP_SUM;
            coll->root      = nprocs / 2; /* non-trivial root */
            coll->mask      = UCC_COLL_ARGS_FIELD_FLAGS |
                              UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER;
            coll->flags     = UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS;

            coll->src.info.mem_type  = UCC_MEMORY_TYPE_HOST;
            coll->src.info.datatype  = dtype;
            coll->src.info.count     = (ucc_count_t)count;
            coll->src.info.buffer    = team->procs[r].p->onesided_buf[0];
            int32_t *src = (int32_t *)team->procs[r].p->onesided_buf[0];
            for (size_t i = 0; i < count; i++) {
                src[i] = (int32_t)((i + r + 1) % 8);
            }

            coll->dst.info.mem_type  = UCC_MEMORY_TYPE_HOST;
            coll->dst.info.datatype  = dtype;
            coll->dst.info.count     = (ucc_count_t)count;
            coll->dst.info.buffer    = team->procs[r].p->onesided_buf[1];

            coll->global_work_buffer = team->procs[r].p->onesided_buf[2];
            ctxs[r]->rbuf_size       = count * dt;
            clear_buffer(team->procs[r].p->onesided_buf[1], count * dt,
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

    /* The root's dst must hold, for every element i: sum over every rank q of
     * (i + q + 1) % 8 (the SUM reduce over every rank's i-th element). */
    bool os_data_validate(UccTeam_h team, size_t count, ucc_datatype_t dtype)
    {
        int    nprocs = team->procs.size();
        int    root   = nprocs / 2;
        int32_t *dst  = (int32_t *)team->procs[root].p->onesided_buf[1];

        for (size_t i = 0; i < count; i++) {
            int res = 0;
            for (int q = 0; q < nprocs; q++) {
                res += (i + q + 1) % 8;
            }
            if (dst[i] != (int32_t)res) {
                return false;
            }
        }
        return true;
    }

    /* Zero the dst so a repeat round reduces fresh (not stale) data. */
    void os_reset_dst(UccTeam_h team, size_t count, ucc_datatype_t dtype)
    {
        size_t dt = ucc_dt_size(dtype);
        clear_buffer(team->procs[team->procs.size() / 2].p->onesided_buf[1],
                     count * dt, UCC_MEMORY_TYPE_HOST, 0);
    }
};

UCC_TEST_P(test_reduce_onesided, single_onesided)
{
    const int            size  = std::get<0>(GetParam());
    const ucc_datatype_t dtype = std::get<1>(GetParam());
    const size_t         count = 4608; /* 18432 bytes < 1MB segment */
    ucc_job_env_t        env = {{"UCC_TL_UCP_TUNE", "reduce:0-inf:@onesided"},
                                {"UCC_TL_UCP_ONESIDED_SCRATCH_SIZE",
                                 "4194304"}};
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams; the self TL "
                       "handles reduce as a no-op.";
    }

    UccTeam_h     team = job.create_team(size, true, true, true);
    UccCollCtxVec ctxs;
    int           root = size / 2;

    os_data_init(team, count, dtype, ctxs);
    UccReq req(team, ctxs);
    ASSERT_EQ(UCC_OK, req.status);
    req.start();
    ucc_status_t st = req.wait();
    EXPECT_EQ(UCC_OK, st);

    EXPECT_TRUE(os_data_validate(team, count, dtype))
        << "onesided reduce data mismatch, size=" << size;

    /* The root's level-0 slot advanced by exactly its level-0 children count.
     * slot_base = 3 + ceil(log2(size)); the root is a level-0 parent with
     * min(radix-1, size-1) children. A two-sided fallback leaves the work
     * buffer untouched, so the slot would still be 0. */
    int    radix      = std::min(4, size);
    long   increment  = std::min(radix - 1, size - 1);
    int    slot_base  = 3 + (int)ucc_ilog2_ceil(size);
    long  *slot       = (long *)team->procs[root].p->onesided_buf[2];
    EXPECT_EQ((long)increment, slot[slot_base])
        << "onesided reduce slot, size=" << size << " root=" << root;

    os_data_fini(ctxs);
}

/*
 * Two one-sided reduces back-to-back on the same team with no barrier
 * between: the I7 counter-reuse and scratch-refcount regression. The root's
 * level-0 slot must reach 2*increment after the second round, and the data
 * must still validate each round.
 */
UCC_TEST_P(test_reduce_onesided, multiple_onesided)
{
    const int            size  = std::get<0>(GetParam());
    const ucc_datatype_t dtype = std::get<1>(GetParam());
    const size_t         count = 4608; /* 18432 bytes < 1MB segment */
    ucc_job_env_t        env = {{"UCC_TL_UCP_TUNE", "reduce:0-inf:@onesided"},
                                {"UCC_TL_UCP_ONESIDED_SCRATCH_SIZE",
                                 "4194304"}};
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams.";
    }

    UccTeam_h     team = job.create_team(size, true, true, true);
    UccCollCtxVec ctxs;
    int           root = size / 2;

    os_data_init(team, count, dtype, ctxs);

    int    radix     = std::min(4, size);
    long   increment = std::min(radix - 1, size - 1);
    int    slot_base = 3 + (int)ucc_ilog2_ceil(size);

    for (int call = 0; call < 2; call++) {
        UccReq req(team, ctxs);
        ASSERT_EQ(UCC_OK, req.status);
        req.start();
        ucc_status_t st = req.wait();
        EXPECT_EQ(UCC_OK, st);
        EXPECT_TRUE(os_data_validate(team, count, dtype))
            << "onesided reduce back-to-back data mismatch, call=" << call;
        long *slot = (long *)team->procs[root].p->onesided_buf[2];
        EXPECT_EQ((long)(call + 1) * increment, slot[slot_base])
            << "onesided reduce back-to-back slot, size=" << size
            << " root=" << root << " call=" << call;
        /* Zero the dst so the second round's combine overwrites clean data. */
        os_reset_dst(team, count, dtype);
    }

    os_data_fini(ctxs);
}

INSTANTIATE_TEST_CASE_P(
    , test_reduce_onesided,
    ::testing::Combine(
        ::testing::Values(2, 3, 4, 8, 16), // size
        ::testing::Values(UCC_DT_INT32))); // dtype
