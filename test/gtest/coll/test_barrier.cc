
/**
 * Copyright (c) 2021, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 *
 * One-sided barrier (dissemination). A pure-signal rendezvous with no data
 * movement: over `rounds = ceil(log2(size))` rounds, each round r has every
 * rank atomically add 1 to its partner (rank + 2^r) % size's copy of slot
 * (3 + r) in the symmetric work buffer, then wait for its own copy of that
 * slot to receive the +1. The partner-dependency branches into a spanning
 * tree, so no rank completes before the slowest has arrived (a true barrier).
 *
 * The slot-value assertion is the correctness/selection signal: the two-sided
 * (knomial) barrier never touches the work buffer, so a fallback to knomial
 * leaves slots [3 .. 3+rounds) at 0 and fails the assertion. The one-sided
 * algorithm is forced via UCC_TL_UCP_TUNE="barrier:0-inf:@onesided".
 */
#include "common/test_ucc.h"

class test_barrier : public ucc::test
{
public:
    ucc_coll_args_t coll;
    test_barrier() {
        coll.mask      = 0;
        coll.coll_type = UCC_COLL_TYPE_BARRIER;
    }
};

UCC_TEST_F(test_barrier, single_2proc)
{
    UccTeam_h team = UccJob::getStaticJob()->create_team(2);
    UccReq    req(team, &coll);
    req.start();
    req.wait();
}

UCC_TEST_F(test_barrier, single_max_procs)
{
    UccTeam_h team = UccJob::getStaticTeams().back();
    UccReq    req(team, &coll);
    req.start();
    req.wait();
}

UCC_TEST_F(test_barrier, multiple)
{
    std::vector<UccReq> reqs;
    for (auto &team : UccJob::getStaticTeams()) {
        reqs.push_back(UccReq(team, &coll));
    }
    UccReq::startall(reqs);
    UccReq::waitall(reqs);
}

/* First slot reserved for the one-sided barrier's dissemination rounds
 * (I7 slot layout: [3 .. 3+ceil(log2(N)))). */
static const int BARRIER_ONESIDED_BASE_SLOT = 3;

class test_barrier_onesided : public ucc::test,
                              public ::testing::WithParamInterface<std::tuple<int>>
{
public:
    /* Run one one-sided barrier round on `team`. */
    void run_round(UccTeam_h team)
    {
        int             size = (int)team->procs.size();
        UccCollCtxVec   ctxs(size, nullptr);
        ucc_status_t    st;

        for (int r = 0; r < size; r++) {
            ucc_coll_args_t *coll = (ucc_coll_args_t*)
                    calloc(1, sizeof(ucc_coll_args_t));
            ctxs[r] = (gtest_ucc_coll_ctx_t*)calloc(1, sizeof(gtest_ucc_coll_ctx_t));
            ctxs[r]->args = coll;

            coll->mask               = UCC_COLL_ARGS_FIELD_GLOBAL_WORK_BUFFER;
            coll->coll_type          = UCC_COLL_TYPE_BARRIER;
            coll->global_work_buffer = team->procs[r].p->onesided_buf[2];
        }

        UccReq req(team, ctxs);
        ASSERT_EQ(UCC_OK, req.status);
        req.start();
        st = req.wait();
        EXPECT_EQ(UCC_OK, st);

        for (int r = 0; r < size; r++) {
            free(ctxs[r]->args);
            free(ctxs[r]);
        }
    }

    static int barrier_rounds(int size)
    {
        int rounds = 0;
        for (int c = 1; c < size; c <<= 1) {
            rounds++;
        }
        return rounds;
    }
};

/*
 * Single one-sided barrier across the onesided team sizes. Every rank's local
 * slots [3 .. 3+rounds) advance by exactly 1 each (one +1 received per round).
 * Size 1 is skipped: the UCP TL does not support size-1 teams, so the self TL
 * handles the barrier as a no-op and the work buffer is never exercised.
 */
UCC_TEST_P(test_barrier_onesided, single_onesided)
{
    int          size   = std::get<0>(GetParam());
    ucc_job_env_t env   = {{"UCC_TL_UCP_TUNE", "barrier:0-inf:@onesided"}};
    UccJob       job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);
    UccTeam_h    team   = job.create_team(size, true, true, true);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams; the self TL "
                        "handles the barrier as a no-op and never exercises "
                        "the work buffer";
    }

    this->run_round(team);

    /* Each dissemination slot advanced by exactly 1. */
    int rounds = test_barrier_onesided::barrier_rounds(size);
    for (int r = 0; r < size; r++) {
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        for (int rd = 0; rd < rounds; rd++) {
            EXPECT_EQ(1L, slot[BARRIER_ONESIDED_BASE_SLOT + rd])
                << "onesided barrier slot, size=" << size << " rank=" << r
                << " round=" << rd;
        }
    }
}

/*
 * Two one-sided barriers back-to-back on the same team, no barrier between:
 * the I7 counter-reuse case. Each dissemination slot must advance by exactly
 * 1 per barrier (first barrier -> 1, second -> 2).
 */
UCC_TEST_P(test_barrier_onesided, multiple_onesided)
{
    int          size   = std::get<0>(GetParam());
    ucc_job_env_t env   = {{"UCC_TL_UCP_TUNE", "barrier:0-inf:@onesided"}};
    UccJob       job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);
    UccTeam_h    team   = job.create_team(size, true, true, true);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams; the self TL "
                        "handles the barrier as a no-op and never exercises "
                        "the work buffer";
    }

    int rounds = test_barrier_onesided::barrier_rounds(size);

    this->run_round(team); /* barrier 1: each slot -> 1 */
    for (int r = 0; r < size; r++) {
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        for (int rd = 0; rd < rounds; rd++) {
            EXPECT_EQ(1L, slot[BARRIER_ONESIDED_BASE_SLOT + rd])
                << "onesided barrier 1 slot, size=" << size << " rank=" << r
                << " round=" << rd;
        }
    }

    this->run_round(team); /* barrier 2: each slot -> 2 */
    for (int r = 0; r < size; r++) {
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        for (int rd = 0; rd < rounds; rd++) {
            EXPECT_EQ(2L, slot[BARRIER_ONESIDED_BASE_SLOT + rd])
                << "onesided back-to-back barrier slot, size=" << size
                << " rank=" << r << " round=" << rd;
        }
    }
}

INSTANTIATE_TEST_CASE_P(
    , test_barrier_onesided,
    ::testing::Values(
        std::make_tuple(1),   /* skipped: UCP TL has no size-1 teams */
        std::make_tuple(2),
        std::make_tuple(3),
        std::make_tuple(4),
        std::make_tuple(8),
        std::make_tuple(15),
        std::make_tuple(16)));
