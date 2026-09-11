/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 *
 * One-sided fanout. Fanout is a pure-signal collective with no data movement:
 * the root atomically adds 1 to every rank's copy of work-buffer slot 0,
 * including its own. After a round, every rank's local slot 0 is base + 1.
 * The gtest observes this by reading the per-rank work buffer after completion.
 *
 * The slot-value assertion also proves the one-sided algorithm was selected:
 * the two-sided (knomial) fanout never touches the work buffer, so a fallback
 * to knomial would leave the slot at 0 and fail the assertion. Forcing the
 * one-sided algorithm is done via UCC_TL_UCP_TUNE="fanout:0-inf:@onesided".
 *
 * Fanout uses slot 0, the same slot as the put-family (alltoallv, allgather,
 * scatter, gather). That is safe because both advance EVERY rank's local slot
 * 0 by exactly 1 per round (fanout via the root's signal + self-signal), so the
 * per-rank slot base (I7) stays in lockstep. (Fanin, by contrast, uses slot 1
 * because its delta is not uniform across ranks.)
 */

#include "common/test_ucc.h"

class test_fanout : public ucc::test,
                   public ::testing::WithParamInterface<std::tuple<int>>
{
public:
    /* Run one fanout round on `team` (fixed root 0). */
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
            coll->coll_type          = UCC_COLL_TYPE_FANOUT;
            coll->root               = 0;
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
};

/*
 * Single fanout across the onesided team sizes. Every rank's local slot 0
 * advances by exactly 1 (the root via its self-signal, each non-root via the
 * root's signal to it). Size 1 is skipped: the UCP TL does not support
 * size-1 teams, so the self TL handles fanout as a no-op and the work buffer
 * is never exercised.
 */
UCC_TEST_P(test_fanout, single_onesided)
{
    int          size   = std::get<0>(GetParam());
    ucc_job_env_t env   = {{"UCC_TL_UCP_TUNE", "fanout:0-inf:@onesided"}};
    UccJob       job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);
    UccTeam_h    team   = job.create_team(size, true, true, true);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams; the self TL "
                        "handles fanout as a no-op and never exercises the "
                        "work buffer";
    }

    this->run_round(team);

    /* Every rank received exactly one +1 on its local slot 0. */
    for (int r = 0; r < size; r++) {
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        EXPECT_EQ(1L, slot[0]) << "fanout slot, size=" << size << " rank=" << r;
    }
}

/*
 * Two fanout rounds back-to-back on the same team, no barrier between: the I7
 * counter-reuse case. Every rank's slot 0 must advance by exactly 1 each round
 * (round 1 -> 1, round 2 -> 2).
 */
UCC_TEST_P(test_fanout, multiple_onesided)
{
    int          size   = std::get<0>(GetParam());
    ucc_job_env_t env   = {{"UCC_TL_UCP_TUNE", "fanout:0-inf:@onesided"}};
    UccJob       job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);
    UccTeam_h    team   = job.create_team(size, true, true, true);

    if (size == 1) {
        GTEST_SKIP() << "UCP TL does not support size-1 teams; the self TL "
                        "handles fanout as a no-op and never exercises the "
                        "work buffer";
    }

    this->run_round(team); /* round 1: every slot -> 1 */
    for (int r = 0; r < size; r++) {
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        EXPECT_EQ(1L, slot[0]) << "fanout round 1 slot, size=" << size
                               << " rank=" << r;
    }

    this->run_round(team); /* round 2: every slot -> 2 */

    for (int r = 0; r < size; r++) {
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        EXPECT_EQ(2L, slot[0]) << "fanout back-to-back slot, size=" << size
                               << " rank=" << r;
    }
}

INSTANTIATE_TEST_CASE_P(
    , test_fanout,
    ::testing::Values(
        std::make_tuple(1),   /* skipped: UCP TL has no size-1 teams */
        std::make_tuple(2),
        std::make_tuple(3),
        std::make_tuple(4),
        std::make_tuple(8),
        std::make_tuple(15),
        std::make_tuple(16)));
