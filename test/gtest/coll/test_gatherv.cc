/**
 * Copyright (c) 2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 */

#include "common/test_ucc.h"
#include "utils/ucc_math.h"

using Param_0 = std::tuple<int, ucc_datatype_t, ucc_memory_type_t, int, int,
                           gtest_ucc_inplace_t>;
using Param_1 = std::tuple<ucc_datatype_t, ucc_memory_type_t, int, int,
                           gtest_ucc_inplace_t>;

class test_gatherv : public UccCollArgs, public ucc::test {
  private:
    int root;

  public:
    void data_init(int nprocs, ucc_datatype_t dtype, size_t count,
                   UccCollCtxVec &ctxs, bool persistent)
    {
        ucc_coll_args_t *coll;
        int             *counts, *displs;
        size_t           my_count, all_counts;
        ctxs.resize(nprocs);
        for (auto r = 0; r < nprocs; r++) {
            coll = (ucc_coll_args_t *)calloc(1, sizeof(ucc_coll_args_t));
            my_count = (nprocs - r) * count;
            ctxs[r] =
                (gtest_ucc_coll_ctx_t *)calloc(1, sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args           = coll;
            coll->mask              = 0;
            coll->flags             = 0;
            coll->coll_type         = UCC_COLL_TYPE_GATHERV;
            coll->root              = root;
            coll->src.info.mem_type = mem_type;
            coll->src.info.count    = (ucc_count_t)my_count;
            coll->src.info.datatype = dtype;

            ctxs[r]->init_buf =
                ucc_malloc(ucc_dt_size(dtype) * my_count, "init buf");
            ASSERT_NE(ctxs[r]->init_buf, nullptr);
            for (int i = 0; i < my_count * ucc_dt_size(dtype); i++) {
                uint8_t *sbuf = (uint8_t *)ctxs[r]->init_buf;
                sbuf[i]       = ((i + r) % 256);
            }

            if (r == root) {
                all_counts = 0;
                counts = (int*)malloc(sizeof(int) * nprocs);
                ASSERT_NE(counts, nullptr);
                displs = (int*)malloc(sizeof(int) * nprocs);
                ASSERT_NE(displs, nullptr);

                for (int i = 0; i < nprocs; i++) {
                    counts[i] = (nprocs - i) * count;
                    displs[i] = all_counts;
                    all_counts += counts[i];
                }

                coll->dst.info_v.mem_type      = mem_type;
                coll->dst.info_v.counts        = (ucc_count_t *)counts;
                coll->dst.info_v.displacements = (ucc_aint_t *)displs;
                coll->dst.info_v.datatype      = dtype;

                ctxs[r]->rbuf_size = ucc_dt_size(dtype) * all_counts;
                UCC_CHECK(ucc_mc_alloc(&ctxs[r]->dst_mc_header,
                                       ctxs[r]->rbuf_size, mem_type));
                coll->dst.info_v.buffer = ctxs[r]->dst_mc_header->addr;
                if (inplace) {
                    UCC_CHECK(ucc_mc_memcpy(
                        (void *)((ptrdiff_t)coll->dst.info_v.buffer +
                                 displs[r] * ucc_dt_size(dtype)),
                        ctxs[r]->init_buf,
                        ucc_dt_size(dtype) * my_count, mem_type,
                        UCC_MEMORY_TYPE_HOST));
                }
            }
            if (r != root || !inplace) {
                UCC_CHECK(ucc_mc_alloc(&ctxs[r]->src_mc_header,
                                       ucc_dt_size(dtype) * my_count,
                                       mem_type));
                coll->src.info.buffer = ctxs[r]->src_mc_header->addr;
                UCC_CHECK(ucc_mc_memcpy(coll->src.info.buffer,
                                        ctxs[r]->init_buf,
                                        ucc_dt_size(dtype) * my_count,
                                        mem_type, UCC_MEMORY_TYPE_HOST));
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
    void data_fini(UccCollCtxVec ctxs)
    {
        for (auto r = 0; r < ctxs.size(); r++) {
            ucc_coll_args_t *coll = ctxs[r]->args;
            if (r == root) {
                UCC_CHECK(ucc_mc_free(ctxs[r]->dst_mc_header));
                free(coll->dst.info_v.counts);
                free(coll->dst.info_v.displacements);
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
        ucc_coll_args_t *coll     = ctxs[root]->args;
        size_t           my_count = coll->src.info.count;
        ucc_datatype_t   dtype    = coll->dst.info_v.datatype;
        int *            displs   = (int *)coll->dst.info_v.displacements;

        clear_buffer(coll->dst.info_v.buffer, ctxs[root]->rbuf_size,
                     mem_type, 0);
        if (TEST_INPLACE == inplace) {
            UCC_CHECK(ucc_mc_memcpy(
                         (void *)((ptrdiff_t)coll->dst.info_v.buffer +
                         displs[root] * ucc_dt_size(dtype)),
                         ctxs[root]->init_buf, ucc_dt_size(dtype) * my_count,
                         mem_type, UCC_MEMORY_TYPE_HOST));
        }
    }
    bool data_validate(UccCollCtxVec ctxs)
    {
        bool   ret      = true;
        int    root     = ctxs[0]->args->root;
        int   *displs   = (int*)ctxs[root]->args->dst.info_v.displacements;
        size_t dt_size  = ucc_dt_size(ctxs[root]->args->src.info.datatype);
        ucc_count_t my_count;
        uint8_t    *dsts;

        if (UCC_MEMORY_TYPE_HOST != mem_type) {
            dsts = (uint8_t *)ucc_malloc(ctxs[root]->rbuf_size, "dsts buf");
            ucc_assert(dsts != nullptr);
            UCC_CHECK(ucc_mc_memcpy(dsts, ctxs[root]->args->dst.info_v.buffer,
                                    ctxs[root]->rbuf_size,
                                    UCC_MEMORY_TYPE_HOST, mem_type));
        } else {
            dsts = (uint8_t *)ctxs[root]->args->dst.info_v.buffer;
        }

        for (int r = 0; r < ctxs.size(); r++) {
            my_count = ctxs[r]->args->src.info.count;
            for (int i = 0; i < my_count * dt_size; i++) {
                if ((uint8_t)((i + r) % 256) !=
                    dsts[(displs[r] * dt_size + i)]) {
                    ret = false;
                    break;
                }
            }
        }

        if (UCC_MEMORY_TYPE_HOST != mem_type) {
            ucc_free(dsts);
        }
        return ret;
    }
    void set_root(int _root)
    {
        root = _root;
    }
};

class test_gatherv_0 : public test_gatherv,
                      public ::testing::WithParamInterface<Param_0> {
};

UCC_TEST_P(test_gatherv_0, single)
{
    const int                 team_id  = std::get<0>(GetParam());
    const ucc_datatype_t      dtype    = std::get<1>(GetParam());
    const ucc_memory_type_t   mem_type = std::get<2>(GetParam());
    const int                 count    = std::get<3>(GetParam());
    const int                 root     = std::get<4>(GetParam());
    const gtest_ucc_inplace_t inplace  = std::get<5>(GetParam());
    UccTeam_h                 team     = UccJob::getStaticTeams()[team_id];
    int                       size     = team->procs.size();
    UccCollCtxVec             ctxs;

    if (size <= root) {
        GTEST_SKIP();
    }

    set_inplace(inplace);
    SET_MEM_TYPE(mem_type);
    set_root(root);

    data_init(size, dtype, count, ctxs, false);
    UccReq req(team, ctxs);
    req.start();
    req.wait();
    EXPECT_EQ(true, data_validate(ctxs));
    data_fini(ctxs);
}

UCC_TEST_P(test_gatherv_0, single_persistent)
{
    const int                 team_id  = std::get<0>(GetParam());
    const ucc_datatype_t      dtype    = std::get<1>(GetParam());
    const ucc_memory_type_t   mem_type = std::get<2>(GetParam());
    const int                 count    = std::get<3>(GetParam());
    const int                 root     = std::get<4>(GetParam());
    const gtest_ucc_inplace_t inplace  = std::get<5>(GetParam());
    UccTeam_h                 team     = UccJob::getStaticTeams()[team_id];
    int                       size     = team->procs.size();
    const int                 n_calls  = 3;
    UccCollCtxVec             ctxs;

    if (size <= root) {
        GTEST_SKIP();
    }

    set_inplace(inplace);
    SET_MEM_TYPE(mem_type);
    set_root(root);

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
    , test_gatherv_0,
    ::testing::Combine(::testing::Range(1, UccJob::nStaticTeams), // team_ids
                       PREDEFINED_DTYPES,
#ifdef HAVE_CUDA
                       ::testing::Values(UCC_MEMORY_TYPE_HOST,
                                         UCC_MEMORY_TYPE_CUDA,
                                         UCC_MEMORY_TYPE_CUDA_MANAGED),
#else
                       ::testing::Values(UCC_MEMORY_TYPE_HOST),
#endif
                       ::testing::Values(1, 3, 8192), // count
                       ::testing::Values(0, 1),       // root
                       ::testing::Values(TEST_INPLACE, TEST_NO_INPLACE)));

class test_gatherv_1 : public test_gatherv,
                      public ::testing::WithParamInterface<Param_1> {
};

UCC_TEST_P(test_gatherv_1, multiple_host)
{
    const ucc_datatype_t       dtype    = std::get<0>(GetParam());
    const ucc_memory_type_t    mem_type = std::get<1>(GetParam());
    const int                  count    = std::get<2>(GetParam());
    const int                  root     = std::get<3>(GetParam());
    const gtest_ucc_inplace_t  inplace  = std::get<4>(GetParam());
    std::vector<UccReq>        reqs;
    std::vector<UccCollCtxVec> ctxs;

    for (int tid = 0; tid < UccJob::nStaticTeams; tid++) {
        UccTeam_h     team = UccJob::getStaticTeams()[tid];
        int           size = team->procs.size();
        UccCollCtxVec ctx;

        if (size <= root) {
            /* skip invalid */
            continue;
        }

        this->set_inplace(inplace);
        SET_MEM_TYPE(mem_type);
        set_root(root);

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
    , test_gatherv_1,
    ::testing::Combine(PREDEFINED_DTYPES,
#ifdef HAVE_CUDA
                       ::testing::Values(UCC_MEMORY_TYPE_HOST,
                                         UCC_MEMORY_TYPE_CUDA,
                                         UCC_MEMORY_TYPE_CUDA_MANAGED),
#else
                       ::testing::Values(UCC_MEMORY_TYPE_HOST),
#endif
                       ::testing::Values(1, 3, 8192), // count
                       ::testing::Values(0, 1),       // root
                       ::testing::Values(TEST_INPLACE, TEST_NO_INPLACE)));
/*
 * One-sided gatherv: root-driven get (plan 5.3). Only the root knows the
 * destination counts/displacements, so a non-root cannot compute where to put
 * its block -- instead every non-root signals "my src is ready" with a
 * +1 on the ROOT's copy of slot 2 in the symmetric work buffer, and the root
 * then issues one get per peer into dst + displs[p], and local-copies its own
 * block into dst + displs[root]. A get completes when the data is in the local
 * destination (I5), so the root needs no further flush/signal.
 *
 * It is the first data-moving GET algorithm, so the gtest asserts the variable
 * per-rank blocks land at the root at the correct displs (not just slot
 * counters), and that slot 2 (root) advances by size-1 (the linear two-sided
 * algorithm never touches the work buffer, so a fallback would leave it at 0).
 *
 * Segment buffers: buf 0 = src, buf 1 = dst, buf 2 = work buffer (the same
 * 1 MiB symmetric segments every rank registers, satisfying I1). Every rank
 * registers src and dst, so the root can get from each peer's src and a get
 * target resolves by symmetric offset. The algorithm is forced via
 * UCC_TL_UCP_TUNE="gatherv:0-inf:@onesided".
 */
static const int GATHERV_ONESIDED_SLOT = 2;

using GvOsideParam = std::tuple<int, int, ucc_datatype_t>;

class test_gatherv_onesided : public ucc::test,
                              public ::testing::WithParamInterface<GvOsideParam>
{
  public:
    int root = 0;

    /* Build one gatherv's per-rank args on the team's onesided segments.
     * Per-rank counts vary as (nprocs - r) * count with displs the prefix
     * sums, so the blocks have distinct sizes (true v-case). src block r is
     * filled with pattern ((i + r) % 256). The root's counts/displs arrays are
     * the RMA metadata the root uses to issue its gets. */
    void os_data_init(UccTeam_h team, size_t count, ucc_datatype_t dtype,
                      UccCollCtxVec &ctxs)
    {
        int    nprocs = team->procs.size();
        size_t dt     = ucc_dt_size(dtype);
        size_t total  = 0;

        for (int r = 0; r < nprocs; r++) {
            total += (size_t)(nprocs - r) * count;
        }

        ctxs.resize(nprocs);
        for (int r = 0; r < nprocs; r++) {
            ucc_coll_args_t *coll =
                (ucc_coll_args_t *)calloc(1, sizeof(ucc_coll_args_t));
            ctxs[r] =
                (gtest_ucc_coll_ctx_t *)calloc(1, sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args = coll;

            coll->coll_type = UCC_COLL_TYPE_GATHERV;
            coll->mask      = UCC_COLL_ARGS_FIELD_FLAGS |
                              UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER;
            coll->flags     = UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS;
            coll->root      = root;

            size_t my_count = (size_t)(nprocs - r) * count;

            coll->src.info.mem_type  = UCC_MEMORY_TYPE_HOST;
            coll->src.info.datatype  = dtype;
            coll->src.info.count     = (ucc_count_t)my_count;
            coll->src.info.buffer    = team->procs[r].p->onesided_buf[0];

            /* dst is the full aggregated buffer on every rank (I1): the root
             * reads dst + displs; the other ranks' copies are unused. */
            coll->dst.info_v.mem_type      = UCC_MEMORY_TYPE_HOST;
            coll->dst.info_v.datatype      = dtype;
            coll->dst.info_v.buffer        = team->procs[r].p->onesided_buf[1];

            coll->global_work_buffer = team->procs[r].p->onesided_buf[2];
            ctxs[r]->rbuf_size       = total * dt;

            if (r == root) {
                int *counts = (int *)malloc(sizeof(int) * nprocs);
                int *displs = (int *)malloc(sizeof(int) * nprocs);
                size_t acc = 0;
                for (int i = 0; i < nprocs; i++) {
                    counts[i] = (nprocs - i) * (int)count;
                    displs[i] = (int)acc;
                    acc += (size_t)(nprocs - i) * count;
                }
                coll->dst.info_v.counts        = (ucc_count_t *)counts;
                coll->dst.info_v.displacements = (ucc_aint_t *)displs;
            }

            for (size_t i = 0; i < my_count * dt; i++) {
                ((uint8_t *)team->procs[r].p->onesided_buf[0])[i] = ((i + r) % 256);
            }
            clear_buffer(team->procs[r].p->onesided_buf[1], total * dt,
                         UCC_MEMORY_TYPE_HOST, 0);
        }
    }

    void os_data_fini(UccCollCtxVec ctxs)
    {
        for (gtest_ucc_coll_ctx_t *ctx : ctxs) {
            ucc_coll_args_t *coll = (ucc_coll_args_t *)ctx->args;
            if (coll->root == root) {
                free(coll->dst.info_v.counts);
                free(coll->dst.info_v.displacements);
            }
            free(coll);
            free(ctx);
        }
        ctxs.clear();
    }

    /* The root's dst segment must hold rank r's (nprocs - r)*count block at
     * displs[r] with pattern ((i + r) % 256). */
    bool os_data_validate(UccTeam_h team, UccCollCtxVec ctxs, size_t count,
                          ucc_datatype_t dtype)
    {
        int     nprocs = team->procs.size();
        int     root   = ctxs[0]->args->root;
        size_t  dt     = ucc_dt_size(dtype);
        int    *counts = (int *)ctxs[root]->args->dst.info_v.counts;
        int    *displs = (int *)ctxs[root]->args->dst.info_v.displacements;
        uint8_t *dsts  = (uint8_t *)team->procs[root].p->onesided_buf[1];

        for (int r = 0; r < nprocs; r++) {
            for (size_t i = 0; i < (size_t)counts[r] * dt; i++) {
                if ((uint8_t)((i + r) % 256) !=
                    dsts[(size_t)displs[r] * dt + i]) {
                    return false;
                }
            }
        }
        return true;
    }
};

/*
 * Single one-sided gatherv across sizes {1,2,3,4,8,15,16}, roots {0,1,2}, and a
 * datatype sweep. The data assertion proves each variable block landed at its
 * displacement; the slot assertion proves the one-sided (get) algorithm ran.
 * Size 1 is skipped: the UCP TL has no size-1 teams.
 */
UCC_TEST_P(test_gatherv_onesided, single_onesided)
{
    const int            size = std::get<0>(GetParam());
    const int            root = std::get<1>(GetParam());
    const ucc_datatype_t dtype = std::get<2>(GetParam());
    const size_t         count = 512; /* per-rank block factor (fits 1 MiB seg) */
    ucc_job_env_t        env   = {{"UCC_TL_UCP_TUNE", "gatherv:0-inf:@onesided"}};
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);
    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams; the self TL "
                        "handles gatherv as a no-op and the work buffer is "
                        "never exercised.";
    }
    if (root >= size) {
        GTEST_SKIP() << "root " << root << " >= team size " << size;
    }

    UccTeam_h     team = job.create_team(size, true, true, true);
    UccCollCtxVec ctxs;
    this->root         = root;

    os_data_init(team, count, dtype, ctxs);
    UccReq req(team, ctxs);
    ASSERT_EQ(UCC_OK, req.status);
    req.start();
    ucc_status_t st = req.wait();
    EXPECT_EQ(UCC_OK, st);

    EXPECT_TRUE(os_data_validate(team, ctxs, count, dtype))
        << "onesided gatherv data mismatch, size=" << size << " root=" << root;

    /* The root's local slot 2 advanced by exactly (size-1): one +1 from each
     * non-root. A two-sided fallback never touches the work buffer, so this
     * also proves the one-sided algorithm was selected. */
    long *root_slot = (long *)team->procs[root].p->onesided_buf[2];
    EXPECT_EQ((long)(size - 1), root_slot[GATHERV_ONESIDED_SLOT])
        << "onesided gatherv root slot, size=" << size << " root=" << root;
    /* Non-roots' local slot 2 is untouched: their atomic targets the root's. */
    for (int r = 0; r < size; r++) {
        if (r == root) {
            continue;
        }
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        EXPECT_EQ(0L, slot[GATHERV_ONESIDED_SLOT])
            << "onesided gatherv non-root slot should be 0, rank=" << r;
    }

    os_data_fini(ctxs);
}

/*
 * Two one-sided gathervs back-to-back on the same team with no barrier between:
 * the I7 counter-reuse regression. The root's slot 2 must reach 2*(size-1) and
 * the variable blocks must still validate after each round.
 */
UCC_TEST_P(test_gatherv_onesided, multiple_onesided)
{
    const int            size = std::get<0>(GetParam());
    const int            root = std::get<1>(GetParam());
    const ucc_datatype_t dtype = std::get<2>(GetParam());
    const size_t         count = 512; /* per-rank factor; 4096 overflows 1 MiB seg */
    ucc_job_env_t        env   = {{"UCC_TL_UCP_TUNE", "gatherv:0-inf:@onesided"}};
    UccJob               job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams.";
    }
    if (root >= size) {
        GTEST_SKIP() << "root " << root << " >= team size " << size;
    }

    UccTeam_h     team = job.create_team(size, true, true, true);
    UccCollCtxVec ctxs;
    this->root         = root;

    os_data_init(team, count, dtype, ctxs);

    long *root_slot = (long *)team->procs[root].p->onesided_buf[2];

    for (int call = 0; call < 2; call++) {
        UccReq req(team, ctxs);
        ASSERT_EQ(UCC_OK, req.status);
        req.start();
        ucc_status_t st = req.wait();
        EXPECT_EQ(UCC_OK, st);
        EXPECT_TRUE(os_data_validate(team, ctxs, count, dtype))
            << "onesided gatherv back-to-back data mismatch, call=" << call;
        EXPECT_EQ((long)((call + 1) * (size - 1)), root_slot[GATHERV_ONESIDED_SLOT])
            << "onesided gatherv back-to-back root slot, call=" << call;
    }

    os_data_fini(ctxs);
}

INSTANTIATE_TEST_CASE_P(
    , test_gatherv_onesided,
    ::testing::Combine(
        ::testing::Values(1, 2, 3, 4, 8, 15, 16), // size
        ::testing::Values(0, 1, 2),                // root
        ::testing::Values(UCC_DT_INT8, UCC_DT_INT32, UCC_DT_INT64,
                          UCC_DT_UINT8, UCC_DT_FLOAT64))); // dtype
