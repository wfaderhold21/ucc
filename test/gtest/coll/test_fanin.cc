/**
 * Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * See file LICENSE for terms.
 *
 * One-sided fanin. Fanin is a pure-signal collective with no data movement:
 * every non-root rank atomically adds 1 to the ROOT's copy of work-buffer slot
 * 1; the root completes once it has received size-1 signals. The gtest
 * observes this by reading the per-rank work buffer after completion.
 *
 * The slot-value assertion also proves the one-sided algorithm was selected:
 * the two-sided (knomial) fanin never touches the work buffer, so a fallback
 * to knomial would leave the slot at 0 and fail the assertion. Forcing the
 * one-sided algorithm is done via UCC_TL_UCP_TUNE="fanin:0-inf:@onesided".
 *
 * Fanin uses slot 1 (not slot 0): in fanin the root's local slot advances by
 * (size-1) per round while a non-root's never changes, whereas the put-family
 * (slot 0) advances by exactly 1 on every rank. Sharing slot 0 would break the
 * per-rank base bookkeeping (I7).
 *
 * The work buffer (onesided_buf[2]) is per-process and is zero-initialized at
 * context creation; only the root's rank is a non-zero slot after a round. The
 * team's slot base (per-team) is therefore consistent as long as a team uses a
 * single root, which is the case here (root 0).
 */

#include "common/test_ucc.h"

class test_fanin : public ucc::test,
                  public ::testing::WithParamInterface<std::tuple<int>>
{
public:
    /* Run one fanin round on `team` (fixed root 0) and return. The caller
     * asserts on the work-buffer slot values; this method only drives the
     * collective. */
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
            coll->coll_type          = UCC_COLL_TYPE_FANIN;
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
 * Single fanin across the onesided team sizes. The slot assertion confirms
 * every non-root signaled the root and the root received exactly size-1 of
 * them (and that the one-sided algorithm was selected).
 */
UCC_TEST_P(test_fanin, single_onesided)
{
    int          size   = std::get<0>(GetParam());
    ucc_job_env_t env   = {{"UCC_TL_UCP_TUNE", "fanin:0-inf:@onesided"}};
    UccJob       job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);
    UccTeam_h    team   = job.create_team(size, true, true, true);

    if (size == 1) {
        GTEST_SKIP() << "size 1 has no non-root; nothing to signal";
    }

    this->run_round(team);

    /* Root received one +1 per non-root; non-roots' slot is untouched. */
    long *root_slot = (long *)team->procs[0].p->onesided_buf[2];
    EXPECT_EQ((long)(size - 1), root_slot[1])
            << "fanin root slot, size=" << size;
    for (int r = 1; r < size; r++) {
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        EXPECT_EQ(0L, slot[1]) << "fanin non-root slot, size=" << size
                               << " rank=" << r;
    }
}

/*
 * Two fanin rounds back-to-back on the same team, no barrier between: the I7
 * counter-reuse case. The root's slot must advance by (size-1) each round
 * (round 1 -> size-1, round 2 -> 2*(size-1)); non-roots stay at 0.
 */
UCC_TEST_P(test_fanin, multiple_onesided)
{
    int          size   = std::get<0>(GetParam());
    ucc_job_env_t env   = {{"UCC_TL_UCP_TUNE", "fanin:0-inf:@onesided"}};
    UccJob       job(size, UccJob::UCC_JOB_CTX_GLOBAL_ONESIDED, env);
    UccTeam_h    team   = job.create_team(size, true, true, true);

    if (size == 1) {
        GTEST_SKIP() << "size 1 has no non-root; nothing to signal";
    }

    this->run_round(team); /* round 1: root slot -> size-1 */
    EXPECT_EQ((long)(size - 1),
              ((long *)team->procs[0].p->onesided_buf[2])[1]);

    this->run_round(team); /* round 2: root slot -> 2*(size-1) */

    long *root_slot = (long *)team->procs[0].p->onesided_buf[2];
    EXPECT_EQ(2L * (size - 1), root_slot[1])
            << "fanin back-to-back root slot, size=" << size;
    for (int r = 1; r < size; r++) {
        long *slot = (long *)team->procs[r].p->onesided_buf[2];
        EXPECT_EQ(0L, slot[1]) << "fanin back-to-back non-root slot, rank=" << r;
    }
}

INSTANTIATE_TEST_CASE_P(
    , test_fanin,
    ::testing::Values(
        std::make_tuple(1),   /* degenerate: skipped */
        std::make_tuple(2),
        std::make_tuple(3),
        std::make_tuple(4),
        std::make_tuple(8),
        std::make_tuple(15),
        std::make_tuple(16)));
