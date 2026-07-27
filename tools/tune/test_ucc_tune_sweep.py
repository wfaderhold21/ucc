#!/usr/bin/env python3
"""Unit tests for ucc_tune_sweep (no binaries required; measure() is mocked)."""

import unittest
from unittest.mock import MagicMock, call, patch

from ucc_tune_runner import RunResult, RunSpec, SingleRunSample
from ucc_tune_space import AlgInfo
from ucc_tune_sweep import (
    SizeDecision,
    SweepResult,
    SweepSpec,
    TuneRange,
    _compute_team_bands,
    _fmt_bytes,
    _forced_alg_env,
    _mem_type_for_tune,
    coalesce_ranges,
    sweep_cell,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_result(median_us: float, cv: float = 0.01) -> RunResult:
    s = SingleRunSample(count=1024, size_bytes=4096,
                        avg_us=median_us, min_us=median_us * 0.95,
                        max_us=median_us * 1.05)
    return RunResult(
        spec=MagicMock(),
        samples=[s],
        median_us=median_us,
        iqr_us=0.0,
        cv=cv,
        clean_count=1,
        dropped_count=0,
        failed_count=0,
        variance_warning=cv > 0.10,
    )


def _base_spec(**kwargs) -> SweepSpec:
    defaults: dict = dict(
        component="tl/ucp",
        collective="allreduce",
        mem_type="host",
        team_size=8,
        msg_sizes_bytes=[1024, 65536, 1 << 20],
        alg_list=[
            AlgInfo(id=0, name="knomial", desc=""),
            AlgInfo(id=1, name="sra_knomial", desc=""),
            AlgInfo(id=2, name="ring", desc=""),
        ],
        datatype="float32",
        n_reps=3,
        n_iter=100,
        n_warmup=10,
        mpi_launcher=["mpirun", "-np", "8"],
    )
    defaults.update(kwargs)
    return SweepSpec(**defaults)


# ---------------------------------------------------------------------------
# _fmt_bytes
# ---------------------------------------------------------------------------

class TestFmtBytes(unittest.TestCase):
    def test_bytes(self):
        self.assertEqual(_fmt_bytes(8), "8")
        self.assertEqual(_fmt_bytes(1023), "1023")

    def test_k(self):
        self.assertEqual(_fmt_bytes(1024), "1k")
        self.assertEqual(_fmt_bytes(4096), "4k")

    def test_M(self):
        self.assertEqual(_fmt_bytes(1 << 20), "1M")
        self.assertEqual(_fmt_bytes(64 << 20), "64M")

    def test_G(self):
        self.assertEqual(_fmt_bytes(1 << 30), "1G")

    def test_non_power_of_two(self):
        # 3072 = 3 * 1024 = 3k
        self.assertEqual(_fmt_bytes(3072), "3k")

    def test_unaligned(self):
        self.assertEqual(_fmt_bytes(1025), "1025")


# ---------------------------------------------------------------------------
# _mem_type_for_tune
# ---------------------------------------------------------------------------

class TestMemTypeForTune(unittest.TestCase):
    def test_host(self):
        self.assertEqual(_mem_type_for_tune("host"), "host")

    def test_cuda(self):
        self.assertEqual(_mem_type_for_tune("cuda"), "cuda")

    def test_cuda_mng_maps_to_cuda_managed(self):
        self.assertEqual(_mem_type_for_tune("cuda-mng"), "cuda-managed")

    def test_rocm(self):
        self.assertEqual(_mem_type_for_tune("rocm"), "rocm")

    def test_unknown_raises(self):
        with self.assertRaises(ValueError):
            _mem_type_for_tune("nvme")


# ---------------------------------------------------------------------------
# TuneRange.tune_token
# ---------------------------------------------------------------------------

class TestTuneToken(unittest.TestCase):
    def _range(self, start, end, alg="sra_knomial", alg_id=1, knobs=None):
        return TuneRange(
            start_bytes=start,
            end_bytes=end,
            alg_name=alg,
            alg_id=alg_id,
            knob_overrides=knobs or {},
        )

    def test_finite_range(self):
        tok = self._range(0, 4096).tune_token("allreduce", "host", 8)
        self.assertEqual(tok, "allreduce:0-4k:host:[8-inf]:inf:@sra_knomial")

    def test_infinite_end(self):
        tok = self._range(4096, None).tune_token("allreduce", "cuda", 16)
        self.assertEqual(tok, "allreduce:4k-inf:cuda:[16-inf]:inf:@sra_knomial")

    def test_zero_start_1M_end(self):
        tok = self._range(0, 1 << 20, alg="knomial").tune_token("bcast", "host", 4)
        self.assertEqual(tok, "bcast:0-1M:host:[4-inf]:inf:@knomial")

    def test_gigabyte_boundary(self):
        tok = self._range(1 << 30, None).tune_token("allreduce", "host", 2)
        self.assertEqual(tok, "allreduce:1G-inf:host:[2-inf]:inf:@sra_knomial")


# ---------------------------------------------------------------------------
# _forced_alg_env
# ---------------------------------------------------------------------------

class TestForcedAlgEnv(unittest.TestCase):
    def test_tl_ucp_tune_var_set(self):
        spec = _base_spec()
        env = _forced_alg_env(spec, "sra_knomial")
        self.assertIn("UCC_TL_UCP_TUNE", env)

    def test_tune_value_format(self):
        spec = _base_spec()
        env = _forced_alg_env(spec, "sra_knomial")
        val = env["UCC_TL_UCP_TUNE"]
        self.assertTrue(val.startswith("allreduce:"))
        self.assertIn(":host:", val)
        self.assertIn(":inf:@sra_knomial", val)
        self.assertIn("0-inf", val)

    def test_competition_vars_present(self):
        spec = _base_spec()
        env = _forced_alg_env(spec, "knomial")
        self.assertEqual(env.get("UCC_TLS"), "ucp")
        self.assertEqual(env.get("UCC_CLS"), "basic")

    def test_cuda_mem_type_in_tune_string(self):
        spec = _base_spec(mem_type="cuda")
        env = _forced_alg_env(spec, "ring")
        val = env["UCC_TL_UCP_TUNE"]
        self.assertIn(":cuda:", val)

    def test_cuda_mng_mem_type_uses_cuda_managed(self):
        spec = _base_spec(mem_type="cuda-mng")
        env = _forced_alg_env(spec, "ring")
        val = env["UCC_TL_UCP_TUNE"]
        self.assertIn(":cuda-managed:", val)


# ---------------------------------------------------------------------------
# coalesce_ranges
# ---------------------------------------------------------------------------

class TestCoalesceRanges(unittest.TestCase):
    def _dec(self, size, override, alg="knomial", alg_id=0, knobs=None):
        return SizeDecision(
            size_bytes=size, should_override=override,
            winner_name=alg, winner_id=alg_id,
            winner_median_us=10.0, default_median_us=12.0, margin=0.17,
            knob_overrides=knobs or {},
        )

    def test_all_same_alg(self):
        sizes = [1024, 65536, 1 << 20]
        decs = [self._dec(s, True) for s in sizes]
        ranges = coalesce_ranges(decs, sizes)
        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0].start_bytes, 0)
        self.assertIsNone(ranges[0].end_bytes)
        self.assertEqual(ranges[0].alg_name, "knomial")

    def test_two_algs_split(self):
        sizes = [1024, 65536, 1 << 20]
        decs = [
            self._dec(1024, True, "knomial", 0),
            self._dec(65536, True, "sra_knomial", 1),
            self._dec(1 << 20, True, "sra_knomial", 1),
        ]
        ranges = coalesce_ranges(decs, sizes)
        self.assertEqual(len(ranges), 2)
        self.assertEqual(ranges[0].alg_name, "knomial")
        self.assertEqual(ranges[0].start_bytes, 0)
        self.assertEqual(ranges[0].end_bytes, 65536)
        self.assertEqual(ranges[1].alg_name, "sra_knomial")
        self.assertEqual(ranges[1].start_bytes, 65536)
        self.assertIsNone(ranges[1].end_bytes)

    def test_no_override_breaks_merge(self):
        # same alg at 1k and 1M, but no-override at 64k — must NOT merge
        sizes = [1024, 65536, 1 << 20]
        decs = [
            self._dec(1024,     True,  "knomial", 0),
            self._dec(65536,    False, "knomial", 0),  # no-override gap
            self._dec(1 << 20,  True,  "knomial", 0),
        ]
        ranges = coalesce_ranges(decs, sizes)
        self.assertEqual(len(ranges), 2)
        # First range covers only 1k (next size is 64k which is the gap)
        self.assertEqual(ranges[0].start_bytes, 0)
        self.assertEqual(ranges[0].end_bytes, 65536)
        # Second range starts at 1M
        self.assertEqual(ranges[1].start_bytes, 1 << 20)
        self.assertIsNone(ranges[1].end_bytes)

    def test_all_no_override(self):
        sizes = [1024, 65536]
        decs = [self._dec(s, False) for s in sizes]
        ranges = coalesce_ranges(decs, sizes)
        self.assertEqual(ranges, [])

    def test_different_knobs_split(self):
        sizes = [1024, 65536]
        decs = [
            self._dec(1024,  True, "sra_knomial", 1, {"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "2"}),
            self._dec(65536, True, "sra_knomial", 1, {"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "4"}),
        ]
        ranges = coalesce_ranges(decs, sizes)
        self.assertEqual(len(ranges), 2)
        self.assertEqual(ranges[0].knob_overrides["UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX"], "2")
        self.assertEqual(ranges[1].knob_overrides["UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX"], "4")

    def test_same_knobs_merge(self):
        sizes = [1024, 65536]
        knobs = {"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "4"}
        decs = [self._dec(s, True, "sra_knomial", 1, knobs) for s in sizes]
        ranges = coalesce_ranges(decs, sizes)
        self.assertEqual(len(ranges), 1)

    def test_first_size_not_in_decisions(self):
        # First measured size (1024) has no decision; range starts at 65536.
        sizes = [1024, 65536, 1 << 20]
        decs = [
            self._dec(65536,   True, "knomial", 0),
            self._dec(1 << 20, True, "knomial", 0),
        ]
        ranges = coalesce_ranges(decs, sizes)
        self.assertEqual(len(ranges), 1)
        # 65536 is NOT all_sizes[0] (1024 is), so start_bytes = 65536
        self.assertEqual(ranges[0].start_bytes, 65536)

    def test_empty_decisions(self):
        self.assertEqual(coalesce_ranges([], [1024, 65536]), [])

    def test_empty_sizes(self):
        self.assertEqual(coalesce_ranges([], []), [])

    def test_single_override(self):
        sizes = [4096]
        decs = [self._dec(4096, True)]
        ranges = coalesce_ranges(decs, sizes)
        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0].start_bytes, 0)
        self.assertIsNone(ranges[0].end_bytes)


# ---------------------------------------------------------------------------
# sweep_cell (measure() mocked)
# ---------------------------------------------------------------------------

class TestSweepCell(unittest.TestCase):
    def _spec(self, **kw):
        return _base_spec(**kw)

    @patch("ucc_tune_sweep.measure")
    def test_winner_becomes_tune_range(self, mock_measure):
        # knomial=10us, sra_knomial=8us (winner), ring=12us; default=10us.
        # margin = (10-8)/10 = 0.20 > 0.05 → should_override for all sizes.
        def side_effect(rs):
            for alg, us in [("@knomial", 10.0), ("@sra_knomial", 8.0),
                             ("@ring", 12.0)]:
                if alg in (rs.extra_env.get("UCC_TL_UCP_TUNE", "")):
                    return _make_result(us)
            return _make_result(10.0)  # default

        mock_measure.side_effect = side_effect
        spec = self._spec(
            msg_sizes_bytes=[4096],
            alg_list=[
                AlgInfo(0, "knomial", ""),
                AlgInfo(1, "sra_knomial", ""),
                AlgInfo(2, "ring", ""),
            ],
        )
        result = sweep_cell(spec)
        self.assertEqual(len(result.size_decisions), 1)
        sd = result.size_decisions[0]
        self.assertEqual(sd.winner_name, "sra_knomial")
        self.assertTrue(sd.should_override)
        self.assertEqual(len(result.tune_ranges), 1)
        self.assertEqual(result.tune_ranges[0].alg_name, "sra_knomial")

    @patch("ucc_tune_sweep.measure")
    def test_no_override_when_within_margin(self, mock_measure):
        # winner=9.6us, default=10us, margin=4% < threshold 5% → no override.
        def side_effect(rs):
            if "@knomial" in rs.extra_env.get("UCC_TL_UCP_TUNE", ""):
                return _make_result(9.6)
            return _make_result(10.0)

        mock_measure.side_effect = side_effect
        spec = self._spec(
            msg_sizes_bytes=[4096],
            alg_list=[AlgInfo(0, "knomial", "")],
            margin_threshold=0.05,
        )
        result = sweep_cell(spec)
        self.assertEqual(len(result.size_decisions), 1)
        self.assertFalse(result.size_decisions[0].should_override)
        self.assertEqual(result.tune_ranges, [])

    @patch("ucc_tune_sweep.measure")
    def test_failed_algs_skipped(self, mock_measure):
        # knomial always fails (RuntimeError); sra_knomial works.
        call_count = [0]

        def side_effect(rs):
            call_count[0] += 1
            if "@knomial" in rs.extra_env.get("UCC_TL_UCP_TUNE", ""):
                raise RuntimeError("not available")
            return _make_result(10.0)

        mock_measure.side_effect = side_effect
        spec = self._spec(
            msg_sizes_bytes=[4096],
            alg_list=[AlgInfo(0, "knomial", ""), AlgInfo(1, "sra_knomial", "")],
        )
        result = sweep_cell(spec)
        self.assertEqual(len(result.size_decisions), 1)
        self.assertEqual(result.size_decisions[0].winner_name, "sra_knomial")

    @patch("ucc_tune_sweep.measure")
    def test_all_algs_fail_size_skipped(self, mock_measure):
        mock_measure.side_effect = RuntimeError("no alg works")
        spec = self._spec(
            msg_sizes_bytes=[4096],
            alg_list=[AlgInfo(0, "knomial", "")],
        )
        result = sweep_cell(spec)
        self.assertEqual(len(result.size_decisions), 0)
        self.assertEqual(len(result.warnings), 1)

    @patch("ucc_tune_sweep.measure")
    def test_multiple_sizes_coalesced(self, mock_measure):
        # All three sizes: sra_knomial wins with 20% margin → 1 merged range.
        def side_effect(rs):
            if "@sra_knomial" in rs.extra_env.get("UCC_TL_UCP_TUNE", ""):
                return _make_result(8.0)
            if "UCC_TL_UCP_TUNE" in rs.extra_env:
                return _make_result(12.0)
            return _make_result(10.0)  # default

        mock_measure.side_effect = side_effect
        spec = self._spec(
            msg_sizes_bytes=[1024, 65536, 1 << 20],
            alg_list=[AlgInfo(0, "knomial", ""), AlgInfo(1, "sra_knomial", "")],
        )
        result = sweep_cell(spec)
        self.assertEqual(len(result.tune_ranges), 1)
        self.assertEqual(result.tune_ranges[0].start_bytes, 0)
        self.assertIsNone(result.tune_ranges[0].end_bytes)

    @patch("ucc_tune_sweep.measure")
    def test_split_ranges_at_crossover(self, mock_measure):
        # knomial wins at 1k, sra_knomial wins at 64k and 1M.
        def side_effect(rs):
            tune = rs.extra_env.get("UCC_TL_UCP_TUNE", "")
            if "@knomial" in tune:
                return _make_result(5.0 if rs.count * 4 <= 1024 else 15.0)
            if "@sra_knomial" in tune:
                return _make_result(15.0 if rs.count * 4 <= 1024 else 5.0)
            return _make_result(12.0)  # default

        mock_measure.side_effect = side_effect
        spec = self._spec(
            msg_sizes_bytes=[1024, 65536, 1 << 20],
            alg_list=[AlgInfo(0, "knomial", ""), AlgInfo(1, "sra_knomial", "")],
        )
        result = sweep_cell(spec)
        self.assertEqual(len(result.tune_ranges), 2)
        alg_names = [r.alg_name for r in result.tune_ranges]
        self.assertEqual(alg_names, ["knomial", "sra_knomial"])
        self.assertEqual(result.tune_ranges[0].end_bytes, 65536)
        self.assertIsNone(result.tune_ranges[1].end_bytes)

    @patch("ucc_tune_sweep.measure")
    def test_empty_alg_list(self, mock_measure):
        spec = _base_spec(alg_list=[])
        result = sweep_cell(spec)
        self.assertEqual(result.tune_ranges, [])
        self.assertEqual(result.size_decisions, [])
        mock_measure.assert_not_called()

    @patch("ucc_tune_sweep.measure")
    def test_variance_warning_recorded(self, mock_measure):
        mock_measure.return_value = _make_result(10.0, cv=0.20)  # high CV
        spec = self._spec(
            msg_sizes_bytes=[4096],
            alg_list=[AlgInfo(0, "knomial", "")],
        )
        result = sweep_cell(spec)
        self.assertTrue(any("CV" in w for w in result.warnings))

    @patch("ucc_tune_sweep.measure")
    def test_tune_token_format(self, mock_measure):
        def side_effect(rs):
            if "@sra_knomial" in rs.extra_env.get("UCC_TL_UCP_TUNE", ""):
                return _make_result(8.0)
            return _make_result(10.0)

        mock_measure.side_effect = side_effect
        spec = self._spec(
            msg_sizes_bytes=[4096],
            alg_list=[AlgInfo(1, "sra_knomial", "")],
            team_size=8,
            mem_type="host",
        )
        result = sweep_cell(spec)
        self.assertEqual(len(result.tune_ranges), 1)
        tok = result.tune_ranges[0].tune_token("allreduce", "host", 8)
        self.assertEqual(tok, "allreduce:0-inf:host:[8-inf]:inf:@sra_knomial")

    @patch("ucc_tune_sweep.measure")
    def test_knob_sweep_updates_decision(self, mock_measure):
        # sra_knomial wins the alg sweep. Then during knob sweep (radix),
        # radix=4 beats the baseline. Verify knob_overrides is set.
        def side_effect(rs):
            tune = rs.extra_env.get("UCC_TL_UCP_TUNE", "")
            radix_env = rs.extra_env.get("UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX", "")
            if "@knomial" in tune and "@sra" not in tune:
                return _make_result(12.0)
            if "@sra_knomial" in tune:
                # Knob sweep: radix=4 is best
                if radix_env == "4":
                    return _make_result(7.0)
                return _make_result(9.0)
            return _make_result(11.0)  # default

        mock_measure.side_effect = side_effect
        spec = self._spec(
            msg_sizes_bytes=[65536],
            alg_list=[AlgInfo(0, "knomial", ""), AlgInfo(1, "sra_knomial", "")],
        )
        result = sweep_cell(spec)
        self.assertEqual(len(result.size_decisions), 1)
        sd = result.size_decisions[0]
        self.assertEqual(sd.winner_name, "sra_knomial")
        # The best knob (radix=4) should be in knob_overrides
        self.assertIn("UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX",
                      sd.knob_overrides,
                      "Expected knob override to be set after knob sweep")
        self.assertEqual(sd.knob_overrides["UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX"], "4")


# ---------------------------------------------------------------------------
# _compute_team_bands (Task 1)
# ---------------------------------------------------------------------------

class TestComputeTeamBands(unittest.TestCase):
    def test_single_size(self):
        bands = _compute_team_bands([8])
        self.assertEqual(bands, {8: (8, None)})

    def test_three_sizes_non_overlapping(self):
        bands = _compute_team_bands([8, 64, 512])
        self.assertEqual(bands[8], (8, 63))
        self.assertEqual(bands[64], (64, 511))
        self.assertEqual(bands[512], (512, None))

    def test_unsorted_duplicate_input(self):
        bands = _compute_team_bands([512, 8, 8, 64])
        self.assertEqual(len(bands), 3)
        self.assertEqual(bands[8], (8, 63))
        self.assertEqual(bands[64], (64, 511))
        self.assertEqual(bands[512], (512, None))

    def test_adjacent_sizes(self):
        bands = _compute_team_bands([8, 9])
        self.assertEqual(bands[8], (8, 8))
        self.assertEqual(bands[9], (9, None))


# ---------------------------------------------------------------------------
# TuneRange.tune_token with bands (Task 1)
# ---------------------------------------------------------------------------

class TestTuneTokenBands(unittest.TestCase):
    def _range(self, start, end, alg="sra_knomial", alg_id=1, knobs=None):
        return TuneRange(
            start_bytes=start, end_bytes=end,
            alg_name=alg, alg_id=alg_id,
            knob_overrides=knobs or {},
        )

    def test_single_band_inf(self):
        tok = self._range(0, None).tune_token("allreduce", "host", 8, None)
        self.assertEqual(tok, "allreduce:0-inf:host:[8-inf]:inf:@sra_knomial")

    def test_finite_band(self):
        tok = self._range(0, 4096).tune_token("allreduce", "host", 8, 63)
        self.assertEqual(tok, "allreduce:0-4k:host:[8-63]:inf:@sra_knomial")

    def test_middle_band(self):
        tok = self._range(4096, 1 << 20).tune_token("allreduce", "host", 64, 511)
        self.assertEqual(tok, "allreduce:4k-1M:host:[64-511]:inf:@sra_knomial")


if __name__ == "__main__":
    unittest.main()
