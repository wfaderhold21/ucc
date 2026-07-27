#!/usr/bin/env python3
"""
Unit tests for ucc_offline_tune emission, validation, and orchestration.
No real binaries or hardware required — measure() and sweep_cell() are mocked.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from ucc_tune_fingerprint import Fingerprint, _ucc_version, _ucx_version
from ucc_tune_runner import RunResult, RunSpec, SingleRunSample
from ucc_tune_space import AlgInfo
from ucc_tune_sweep import SizeDecision, SweepResult, SweepSpec, TuneRange
from ucc_offline_tune import (
    ValidationPoint,
    _build_conf_lines,
    _build_sh_lines,
    _collect_knob_overrides,
    _collect_tune_tokens,
    _representative_sizes,
    emit_conf,
    run_tuning,
    validate,
    write_summary,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fp(**kw) -> Fingerprint:
    defaults = dict(
        ucc_version="1.4.0",
        ucx_version="1.17.0",
        cpu_model="AMD EPYC 9654",
        gpu_model="NVIDIA H100 80GB HBM3",
        gpu_driver="535.104.05",
        cuda_version="12.2",
        hostname="node01",
        timestamp="2026-06-23T00:00:00+00:00",
        hash="abc123",
    )
    defaults.update(kw)
    return Fingerprint(**defaults)


def _make_run_result(median_us: float) -> RunResult:
    s = SingleRunSample(count=1024, size_bytes=4096,
                        avg_us=median_us, min_us=median_us * 0.95,
                        max_us=median_us * 1.05)
    return RunResult(
        spec=MagicMock(),
        samples=[s],
        median_us=median_us,
        iqr_us=0.0,
        cv=0.01,
        clean_count=1,
        dropped_count=0,
        failed_count=0,
        variance_warning=False,
    )


def _make_sweep_result(
    component="tl/ucp",
    collective="allreduce",
    mem_type="host",
    team_size=8,
    tune_ranges=None,
    size_decisions=None,
    warnings=None,
) -> SweepResult:
    algs = [AlgInfo(0, "knomial", ""), AlgInfo(1, "sra_knomial", "")]
    spec = SweepSpec(
        component=component,
        collective=collective,
        mem_type=mem_type,
        team_size=team_size,
        msg_sizes_bytes=[1024, 65536, 1 << 20],
        alg_list=algs,
        datatype="float32",
        n_reps=3,
        n_iter=100,
        n_warmup=10,
        mpi_launcher=["mpirun", "-np", str(team_size)],
    )
    return SweepResult(
        spec=spec,
        size_decisions=size_decisions or [],
        tune_ranges=tune_ranges or [],
        warnings=warnings or [],
    )


def _tr(start, end, alg="sra_knomial", alg_id=1, knobs=None) -> TuneRange:
    return TuneRange(
        start_bytes=start, end_bytes=end,
        alg_name=alg, alg_id=alg_id,
        knob_overrides=knobs or {},
    )


# ---------------------------------------------------------------------------
# _collect_tune_tokens
# ---------------------------------------------------------------------------

class TestCollectTuneTokens(unittest.TestCase):
    def test_single_range(self):
        r = _make_sweep_result(tune_ranges=[_tr(0, None)])
        tokens = _collect_tune_tokens([r])
        self.assertIn("UCC_TL_UCP_TUNE", tokens)
        self.assertEqual(len(tokens["UCC_TL_UCP_TUNE"]), 1)
        self.assertIn("@sra_knomial", tokens["UCC_TL_UCP_TUNE"][0])

    def test_multiple_ranges_same_component(self):
        ranges = [_tr(0, 65536, "knomial", 0), _tr(65536, None, "sra_knomial", 1)]
        r = _make_sweep_result(tune_ranges=ranges)
        tokens = _collect_tune_tokens([r])
        self.assertEqual(len(tokens["UCC_TL_UCP_TUNE"]), 2)

    def test_empty_tune_ranges_excluded(self):
        r = _make_sweep_result(tune_ranges=[])
        tokens = _collect_tune_tokens([r])
        self.assertNotIn("UCC_TL_UCP_TUNE", tokens)

    def test_different_components_separate_vars(self):
        r1 = _make_sweep_result(component="tl/ucp", tune_ranges=[_tr(0, None)])
        r2 = _make_sweep_result(component="tl/cuda",  tune_ranges=[_tr(0, None)])
        tokens = _collect_tune_tokens([r1, r2])
        self.assertIn("UCC_TL_UCP_TUNE", tokens)
        self.assertIn("UCC_TL_CUDA_TUNE", tokens)

    def test_token_contains_mem_type(self):
        r = _make_sweep_result(mem_type="cuda", tune_ranges=[_tr(0, None)])
        tokens = _collect_tune_tokens([r])
        tok = tokens["UCC_TL_UCP_TUNE"][0]
        self.assertIn(":cuda:", tok)

    def test_token_contains_team_size(self):
        r = _make_sweep_result(team_size=16, tune_ranges=[_tr(0, None)])
        tokens = _collect_tune_tokens([r])
        tok = tokens["UCC_TL_UCP_TUNE"][0]
        self.assertIn("[16-inf]", tok)


# ---------------------------------------------------------------------------
# _collect_knob_overrides
# ---------------------------------------------------------------------------

class TestCollectKnobOverrides(unittest.TestCase):
    def test_single_knob(self):
        r = _make_sweep_result(tune_ranges=[
            _tr(0, None, knobs={"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "4"})
        ])
        knob_env, warnings = _collect_knob_overrides([r])
        self.assertEqual(knob_env["UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX"], "4")
        self.assertEqual(warnings, [])

    def test_conflict_picks_largest_span(self):
        # Range 1: [0, 65536) → radix=2  (span 65536)
        # Range 2: [65536, ∞) → radix=4  (span ≈ 2^62)
        # Largest span is range 2, so radix=4 should win.
        r = _make_sweep_result(tune_ranges=[
            _tr(0,     65536, knobs={"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "2"}),
            _tr(65536, None,  knobs={"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "4"}),
        ])
        knob_env, warnings = _collect_knob_overrides([r])
        self.assertEqual(knob_env["UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX"], "4")
        self.assertEqual(len(warnings), 1)
        self.assertIn("conflict", warnings[0].lower())

    def test_no_conflict_no_warning(self):
        r = _make_sweep_result(tune_ranges=[
            _tr(0, 65536, knobs={"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "4"}),
            _tr(65536, None, knobs={"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "4"}),
        ])
        knob_env, warnings = _collect_knob_overrides([r])
        self.assertEqual(warnings, [])
        self.assertEqual(knob_env["UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX"], "4")

    def test_no_knobs_returns_empty(self):
        r = _make_sweep_result(tune_ranges=[_tr(0, None)])
        knob_env, warnings = _collect_knob_overrides([r])
        self.assertEqual(knob_env, {})
        self.assertEqual(warnings, [])


# ---------------------------------------------------------------------------
# _build_conf_lines / _build_sh_lines
# ---------------------------------------------------------------------------

class TestBuildConfLines(unittest.TestCase):
    def setUp(self):
        self.fp = _fp()
        self.tokens = {"UCC_TL_UCP_TUNE": ["allreduce:0-64k:host:[8-inf]:inf:@knomial",
                                            "allreduce:64k-inf:host:[8-inf]:inf:@sra_knomial"]}
        self.knobs = {"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "4"}

    def test_conf_has_tune_var(self):
        lines = _build_conf_lines(self.tokens, self.knobs, self.fp)
        joined = "\n".join(lines)
        self.assertIn("UCC_TL_UCP_TUNE=", joined)

    def test_conf_tokens_hash_separated(self):
        lines = _build_conf_lines(self.tokens, self.knobs, self.fp)
        tune_line = next(l for l in lines if l.startswith("UCC_TL_UCP_TUNE="))
        # Both tokens should be present, '#'-separated.
        self.assertIn("#", tune_line)
        self.assertIn("@knomial", tune_line)
        self.assertIn("@sra_knomial", tune_line)

    def test_conf_has_knob(self):
        lines = _build_conf_lines(self.tokens, self.knobs, self.fp)
        joined = "\n".join(lines)
        self.assertIn("UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX=4", joined)

    def test_conf_has_fingerprint_hash(self):
        lines = _build_conf_lines(self.tokens, self.knobs, self.fp)
        joined = "\n".join(lines)
        self.assertIn(self.fp.hash, joined)

    def test_conf_has_correctness_warning(self):
        lines = _build_conf_lines(self.tokens, self.knobs, self.fp)
        joined = "\n".join(lines)
        self.assertIn("Correctness", joined)


class TestBuildShLines(unittest.TestCase):
    def setUp(self):
        self.fp = _fp()
        self.tokens = {"UCC_TL_UCP_TUNE": ["allreduce:0-inf:host:[8-inf]:inf:@sra_knomial"]}
        self.knobs = {}

    def test_sh_starts_with_shebang(self):
        lines = _build_sh_lines(self.tokens, self.knobs, self.fp)
        self.assertTrue(lines[0].startswith("#!/"))

    def test_sh_exports_tune_var(self):
        lines = _build_sh_lines(self.tokens, self.knobs, self.fp)
        joined = "\n".join(lines)
        self.assertIn("export UCC_TL_UCP_TUNE=", joined)

    def test_sh_exports_knob(self):
        knobs = {"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "4"}
        lines = _build_sh_lines(self.tokens, knobs, self.fp)
        joined = "\n".join(lines)
        self.assertIn("export UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX='4'", joined)


# ---------------------------------------------------------------------------
# emit_conf (writes files to a temp dir)
# ---------------------------------------------------------------------------

class TestEmitConf(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.fp = _fp()

    def _emit(self, results):
        return emit_conf(Path(self.tmpdir), results, self.fp)

    def test_conf_file_created(self):
        r = _make_sweep_result(tune_ranges=[_tr(0, None)])
        paths = self._emit([r])
        self.assertTrue(paths["conf"].exists())

    def test_sh_file_created(self):
        r = _make_sweep_result(tune_ranges=[_tr(0, None)])
        paths = self._emit([r])
        self.assertTrue(paths["sh"].exists())

    def test_fingerprint_json_created(self):
        paths = self._emit([])
        self.assertTrue(paths["fingerprint"].exists())
        data = json.loads(paths["fingerprint"].read_text())
        self.assertEqual(data["ucc_version"], self.fp.ucc_version)

    def test_results_json_created(self):
        r = _make_sweep_result(tune_ranges=[_tr(0, None)])
        paths = self._emit([r])
        self.assertTrue(paths["results"].exists())
        data = json.loads(paths["results"].read_text())
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["collective"], "allreduce")

    def test_results_json_has_tune_token(self):
        r = _make_sweep_result(
            collective="allreduce",
            mem_type="host",
            team_size=8,
            tune_ranges=[_tr(0, None, "sra_knomial", 1)],
        )
        paths = self._emit([r])
        data = json.loads(paths["results"].read_text())
        tok = data[0]["tune_ranges"][0]["tune_token"]
        self.assertEqual(tok, "allreduce:0-inf:host:[8-inf]:inf:@sra_knomial")

    def test_empty_results_produces_valid_conf(self):
        paths = self._emit([])
        content = paths["conf"].read_text()
        # No TUNE= lines when there are no results.
        self.assertNotIn("_TUNE=", content)

    def test_knob_conflict_file_created(self):
        r = _make_sweep_result(tune_ranges=[
            _tr(0,     65536, knobs={"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "2"}),
            _tr(65536, None,  knobs={"UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX": "4"}),
        ])
        self._emit([r])
        conflict_file = Path(self.tmpdir) / "knob_conflicts.txt"
        self.assertTrue(conflict_file.exists())


# ---------------------------------------------------------------------------
# _representative_sizes
# ---------------------------------------------------------------------------

class TestRepresentativeSizes(unittest.TestCase):
    def test_picks_middle_of_range(self):
        sizes = [1024, 65536, 1 << 20, 1 << 24]
        ranges = [_tr(0, 1 << 20), _tr(1 << 20, None)]
        reps = _representative_sizes(ranges, sizes)
        # Range [0, 1M) contains sizes [1024, 65536]. Middle of 2 = index 1 → 65536.
        self.assertEqual(reps[0], 65536)
        # Range [1M, inf) contains [1M, 16M]. Middle of 2 = index 1 → 16M.
        self.assertEqual(reps[1], 1 << 24)

    def test_empty_ranges_returns_empty(self):
        self.assertEqual(_representative_sizes([], [1024, 65536]), [])

    def test_single_size_in_range(self):
        sizes = [4096]
        ranges = [_tr(0, None)]
        reps = _representative_sizes(ranges, sizes)
        self.assertEqual(reps, [4096])


# ---------------------------------------------------------------------------
# validate (measure() mocked)
# ---------------------------------------------------------------------------

class TestValidate(unittest.TestCase):
    @patch("ucc_offline_tune.measure")
    def test_pass_when_tuned_faster(self, mock_measure):
        # Tuned = 8us, default = 10us → speedup=20% > 5% margin.
        call_count = [0]

        def side_effect(rs):
            call_count[0] += 1
            # Alternate: tuned call (has TUNE in extra_env), default (no TUNE).
            if "UCC_TL_UCP_TUNE" in rs.extra_env:
                return _make_run_result(8.0)
            return _make_run_result(10.0)

        mock_measure.side_effect = side_effect
        r = _make_sweep_result(
            tune_ranges=[_tr(0, None)],
            size_decisions=[
                SizeDecision(65536, True, "sra_knomial", 1, 8.0, 10.0, 0.20, {})
            ],
        )
        points = validate([r], _collect_tune_tokens([r]), {}, margin_threshold=0.05,
                          n_reps=2, n_iter=50, n_warmup=5)
        self.assertEqual(len(points), 1)
        self.assertTrue(points[0].passed)
        self.assertAlmostEqual(points[0].speedup, 0.20, places=2)

    @patch("ucc_offline_tune.measure")
    def test_fail_when_tuned_slower(self, mock_measure):
        def side_effect(rs):
            if "UCC_TL_UCP_TUNE" in rs.extra_env:
                return _make_run_result(12.0)   # tuned is slower
            return _make_run_result(10.0)

        mock_measure.side_effect = side_effect
        r = _make_sweep_result(
            tune_ranges=[_tr(0, None)],
            size_decisions=[
                SizeDecision(65536, True, "sra_knomial", 1, 12.0, 10.0, -0.20, {})
            ],
        )
        points = validate([r], _collect_tune_tokens([r]), {}, margin_threshold=0.05,
                          n_reps=2, n_iter=50, n_warmup=5)
        self.assertEqual(len(points), 1)
        self.assertFalse(points[0].passed)

    @patch("ucc_offline_tune.measure")
    def test_no_points_for_empty_tune_ranges(self, mock_measure):
        r = _make_sweep_result(tune_ranges=[])
        points = validate([r], {}, {}, n_reps=2, n_iter=50, n_warmup=5)
        self.assertEqual(points, [])
        mock_measure.assert_not_called()

    @patch("ucc_offline_tune.measure")
    def test_failed_measure_skipped(self, mock_measure):
        mock_measure.side_effect = RuntimeError("no binary")
        r = _make_sweep_result(
            tune_ranges=[_tr(0, None)],
            size_decisions=[SizeDecision(65536, True, "sra_knomial", 1, 8.0, 10.0, 0.2, {})],
        )
        points = validate([r], _collect_tune_tokens([r]), {}, n_reps=2, n_iter=50, n_warmup=5)
        self.assertEqual(points, [])


# ---------------------------------------------------------------------------
# run_tuning (sweep_cell and run_ucc_info_algs mocked)
# ---------------------------------------------------------------------------

class TestRunTuning(unittest.TestCase):
    @patch("ucc_offline_tune.run_ucc_info_algs")
    @patch("ucc_offline_tune.sweep_cell")
    def test_returns_one_result_per_cell(self, mock_sweep, mock_info):
        mock_info.return_value = {
            "tl/ucp": {"allreduce": [AlgInfo(0, "knomial", "")]}
        }
        mock_sweep.return_value = _make_sweep_result()

        results, skipped = run_tuning(
            component_collective_pairs=[("tl/ucp", "allreduce")],
            mem_types=["host"],
            team_sizes=[8],
            msg_sizes_bytes=[1024, 65536],
            n_reps=3,
            n_iter=100,
            n_warmup=10,
            mpi_launcher=["mpirun", "-np", "8"],
        )
        self.assertEqual(len(results), 1)
        mock_sweep.assert_called_once()

    @patch("ucc_offline_tune.run_ucc_info_algs")
    @patch("ucc_offline_tune.sweep_cell")
    def test_skipped_when_no_algs(self, mock_sweep, mock_info):
        mock_info.return_value = {}   # no components
        results, skipped = run_tuning(
            component_collective_pairs=[("tl/ucp", "allreduce")],
            mem_types=["host"],
            team_sizes=[8],
            msg_sizes_bytes=[1024],
            n_reps=3,
            n_iter=100,
            n_warmup=10,
        )
        self.assertEqual(results, [])
        self.assertEqual(len(skipped), 1)
        self.assertIn("no algorithms found", skipped[0])
        mock_sweep.assert_not_called()

    @patch("ucc_offline_tune.run_ucc_info_algs")
    @patch("ucc_offline_tune.sweep_cell")
    def test_multiple_cells_all_swept(self, mock_sweep, mock_info):
        mock_info.return_value = {
            "tl/ucp": {
                "allreduce": [AlgInfo(0, "knomial", "")],
                "bcast":     [AlgInfo(0, "knomial", "")],
            }
        }
        mock_sweep.return_value = _make_sweep_result()

        results, _ = run_tuning(
            component_collective_pairs=[
                ("tl/ucp", "allreduce"),
                ("tl/ucp", "bcast"),
            ],
            mem_types=["host", "cuda"],
            team_sizes=[8],
            msg_sizes_bytes=[1024],
            n_reps=3,
            n_iter=100,
            n_warmup=10,
        )
        # 2 collectives × 2 mem_types × 1 team_size = 4 cells
        self.assertEqual(mock_sweep.call_count, 4)
        self.assertEqual(len(results), 4)


# ---------------------------------------------------------------------------
# Fingerprint parsing (mock subprocess)
# ---------------------------------------------------------------------------

class TestFingerprintParsing(unittest.TestCase):
    def test_ucc_version_parsed(self):
        out = "# UCC version=1.4.0 revision abcdef1234\n# Configured with: ...\n"
        with patch("ucc_tune_fingerprint._run", return_value=out):
            ver = _ucc_version("ucc_info")
        self.assertEqual(ver, "1.4.0")

    def test_ucc_version_unknown_on_bad_output(self):
        with patch("ucc_tune_fingerprint._run", return_value="garbage"):
            ver = _ucc_version("ucc_info")
        self.assertEqual(ver, "unknown")

    def test_ucx_version_parsed(self):
        out = "# UCX version=1.17.0 (Release)\n"
        with patch("ucc_tune_fingerprint._run", return_value=out):
            ver = _ucx_version("ucx_info")
        self.assertEqual(ver, "1.17.0")


# ---------------------------------------------------------------------------
# write_summary
# ---------------------------------------------------------------------------

class TestWriteSummary(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_summary_file_created(self):
        r = _make_sweep_result(tune_ranges=[_tr(0, None)])
        path = write_summary(
            Path(self.tmpdir), [r], [], _fp(), []
        )
        self.assertTrue(path.exists())

    def test_summary_contains_collective(self):
        r = _make_sweep_result(collective="allreduce", tune_ranges=[_tr(0, None)])
        path = write_summary(Path(self.tmpdir), [r], [], _fp(), [])
        content = path.read_text()
        self.assertIn("allreduce", content)

    def test_summary_records_skipped(self):
        path = write_summary(Path(self.tmpdir), [], [], _fp(),
                             ["tl/ucp/allreduce: no algorithms found"])
        content = path.read_text()
        self.assertIn("no algorithms found", content)

    def test_summary_validation_pass(self):
        vp = ValidationPoint(
            collective="allreduce", mem_type="host",
            size_bytes=65536, tuned_median_us=8.0, default_median_us=10.0,
            speedup=0.20, passed=True,
        )
        path = write_summary(Path(self.tmpdir), [], [vp], _fp(), [])
        content = path.read_text()
        self.assertIn("PASS", content)
        self.assertIn("20.0%", content)

    def test_summary_correctness_warning_always_present(self):
        path = write_summary(
            Path(self.tmpdir), [],
            [ValidationPoint("ar", "host", 4096, 8.0, 10.0, 0.2, True)],
            _fp(), []
        )
        content = path.read_text()
        self.assertIn("Correctness was NOT validated", content)


if __name__ == "__main__":
    unittest.main()
