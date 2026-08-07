#!/usr/bin/env python3
"""Unit tests for ucc_tune_runner (no perftest binary required)."""

import statistics
import unittest
from unittest.mock import MagicMock, patch

from ucc_tune_runner import (
    RunSpec,
    SingleRunSample,
    _build_cmd,
    _parse_output,
    _tukey_clean,
    bind_launcher_team_size,
    measure,
    measure_paired,
)


class TestLauncherBinding(unittest.TestCase):
    def test_mpirun_default_is_rebound_to_requested_eight(self):
        self.assertEqual(bind_launcher_team_size(["mpirun", "-np", "1"], 8),
                         (["mpirun", "-np", "8"], 8))

    def test_srun_and_template(self):
        self.assertEqual(
            bind_launcher_team_size(["srun", "--ntasks={team_size}", "--exclusive"], 64),
            (["srun", "--ntasks=64", "--exclusive"], 64))

    def test_ambiguous_and_unsupported_fail_closed(self):
        for launcher in (["mpirun", "-np", "8", "-n", "8"],
                         ["jsrun", "-n", "8"], ["srun", "--exclusive"]):
            with self.subTest(launcher=launcher), self.assertRaises(ValueError):
                bind_launcher_team_size(launcher, 8)

    @patch("ucc_tune_runner.subprocess.run")
    def test_deliberate_requested_executed_mismatch_never_launches(self, run):
        from ucc_tune_runner import _run_once
        with self.assertRaisesRegex(RuntimeError, "team-size mismatch"):
            _run_once(RunSpec("allreduce", requested_team_size=8,
                              executed_team_size=4))
        run.assert_not_called()


class TestBuildCmd(unittest.TestCase):
    def _spec(self, collective="allreduce", **kwargs):
        return RunSpec(collective=collective, **kwargs)

    def test_single_size(self):
        cmd = _build_cmd(self._spec(count=2048))
        b_idx = cmd.index("-b")
        e_idx = cmd.index("-e")
        self.assertEqual(cmd[b_idx + 1], "2048")
        self.assertEqual(cmd[e_idx + 1], "2048", "min must equal max for single-size run")

    def test_reduction_op_included_for_allreduce(self):
        cmd = _build_cmd(self._spec(reduction_op="sum"))
        self.assertIn("-o", cmd)
        self.assertEqual(cmd[cmd.index("-o") + 1], "sum")

    def test_reduction_op_excluded_for_bcast(self):
        cmd = _build_cmd(RunSpec(collective="bcast"))
        self.assertNotIn("-o", cmd)

    def test_persistent_flag(self):
        cmd = _build_cmd(self._spec(persistent=True))
        self.assertIn("-p", cmd)

    def test_no_persistent_flag(self):
        cmd = _build_cmd(self._spec(persistent=False))
        self.assertNotIn("-p", cmd)

    def test_collective_in_cmd(self):
        cmd = _build_cmd(RunSpec(collective="bcast"))
        self.assertIn("-c", cmd)
        self.assertEqual(cmd[cmd.index("-c") + 1], "bcast")

    def test_unknown_collective_raises(self):
        with self.assertRaises(ValueError):
            _build_cmd(RunSpec(collective="foobar"))

    def test_unknown_mem_type_raises(self):
        with self.assertRaises(ValueError):
            _build_cmd(RunSpec(collective="allreduce", mem_type="nvme"))

    def test_iter_and_warmup(self):
        cmd = _build_cmd(self._spec(n_iter=500, n_warmup=50))
        self.assertEqual(cmd[cmd.index("-n") + 1], "500")
        self.assertEqual(cmd[cmd.index("-w") + 1], "50")


class TestParseOutput(unittest.TestCase):
    def _make_output(self, count, size, avg, mn, mx):
        header = (
            "                        allreduce\n"
            "Count       Size        Time, us\n"
            "                        avg         min         max\n"
        )
        data = (
            f"{count:>12}{size:>12}"
            f"{avg:>12.2f}{mn:>12.2f}{mx:>12.2f}\n"
        )
        footer = "Total time: 45.6 ms\n"
        return header + data + footer

    def test_parses_normal_line(self):
        out = self._make_output(1024, 4096, 12.34, 11.00, 14.56)
        s = _parse_output(out, "allreduce")
        self.assertIsNotNone(s)
        self.assertEqual(s.count, 1024)
        self.assertEqual(s.size_bytes, 4096)
        self.assertAlmostEqual(s.avg_us, 12.34, places=1)
        self.assertAlmostEqual(s.min_us, 11.00, places=1)
        self.assertAlmostEqual(s.max_us, 14.56, places=1)

    def test_parses_na_line(self):
        # barrier prints N/A for count and size
        out = "         N/A         N/A       12.34       11.00       14.56\n"
        s = _parse_output(out, "barrier")
        self.assertIsNotNone(s)
        self.assertEqual(s.count, 0)
        self.assertEqual(s.size_bytes, 0)
        self.assertAlmostEqual(s.avg_us, 12.34, places=1)

    def test_returns_none_on_empty(self):
        self.assertIsNone(_parse_output("", "allreduce"))

    def test_returns_none_on_header_only(self):
        out = "Count       Size        Time, us\n                        avg         min         max\n"
        self.assertIsNone(_parse_output(out, "allreduce"))

    def test_skips_total_time_line(self):
        out = self._make_output(512, 2048, 5.0, 4.5, 5.5)
        s = _parse_output(out, "allreduce")
        # Should not mistake "Total time: 45.6 ms" for a data line.
        self.assertIsNotNone(s)
        self.assertEqual(s.count, 512)


class TestTukeyClean(unittest.TestCase):
    def test_removes_high_outlier(self):
        vals = [10.0, 10.1, 10.2, 10.0, 9.9, 10.1, 100.0]  # 100 is outlier
        clean, dropped = _tukey_clean(vals, k=1.5)
        self.assertEqual(dropped, 1)
        self.assertNotIn(100.0, clean)

    def test_removes_low_outlier(self):
        vals = [10.0, 10.1, 10.2, 10.0, 9.9, 10.1, 0.1]
        clean, dropped = _tukey_clean(vals, k=1.5)
        self.assertEqual(dropped, 1)
        self.assertNotIn(0.1, clean)

    def test_keeps_clean_set(self):
        vals = [10.0, 10.1, 10.2, 9.9, 10.0]
        clean, dropped = _tukey_clean(vals, k=1.5)
        self.assertEqual(dropped, 0)
        self.assertEqual(sorted(clean), sorted(vals))

    def test_small_list_passthrough(self):
        vals = [10.0, 11.0, 12.0]
        clean, dropped = _tukey_clean(vals, k=1.5)
        self.assertEqual(dropped, 0)
        self.assertEqual(clean, vals)

    def test_uniform_values_no_drop(self):
        vals = [10.0] * 10
        clean, dropped = _tukey_clean(vals, k=1.5)
        self.assertEqual(dropped, 0)


class TestMeasure(unittest.TestCase):
    def _make_sample(self, avg_us):
        return SingleRunSample(count=1024, size_bytes=4096,
                               avg_us=avg_us, min_us=avg_us * 0.9,
                               max_us=avg_us * 1.1)

    def _spec(self, collective="allreduce", n_reps=7, **kwargs):
        return RunSpec(
            collective=collective,
            n_reps=n_reps,
            mpi_launcher=["mpirun", "-np", "1"],
            **kwargs,
        )

    @patch("ucc_tune_runner._run_once")
    def test_median_computed_correctly(self, mock_run):
        times = [10.0, 10.2, 10.1, 10.3, 10.0, 10.1, 10.2]
        mock_run.side_effect = [self._make_sample(t) for t in times]
        result = measure(self._spec())
        self.assertAlmostEqual(result.median_us, statistics.median(times), places=1)
        self.assertEqual(result.failed_count, 0)

    @patch("ucc_tune_runner._run_once")
    def test_outlier_dropped(self, mock_run):
        times = [10.0, 10.2, 10.1, 10.3, 10.0, 10.1, 500.0]
        mock_run.side_effect = [self._make_sample(t) for t in times]
        result = measure(self._spec())
        self.assertGreater(result.dropped_count, 0)
        self.assertLess(result.median_us, 50.0)

    @patch("ucc_tune_runner._run_once")
    def test_failed_reps_counted(self, mock_run):
        samples = [self._make_sample(10.0)] * 5 + [None, None]
        mock_run.side_effect = samples
        result = measure(self._spec())
        self.assertEqual(result.failed_count, 2)
        self.assertEqual(len(result.samples), 5)

    @patch("ucc_tune_runner._run_once")
    def test_raises_when_too_few_succeed(self, mock_run):
        mock_run.return_value = None
        with self.assertRaises(RuntimeError):
            measure(self._spec(n_reps=3))

    @patch("ucc_tune_runner._run_once")
    def test_variance_warning_raised(self, mock_run):
        # Spread values with CV well above threshold but no single Tukey outlier.
        # IQR fences = [q1-1.5*iqr, q3+1.5*iqr] = [9-6, 13+6] = [3, 19], all in.
        # CV = stdev/median ≈ 2.16/11 ≈ 0.196 >> 0.05.
        times = [8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0]
        mock_run.side_effect = [self._make_sample(t) for t in times]
        result = measure(self._spec(cv_warn_threshold=0.05))
        self.assertTrue(result.variance_warning)

    @patch("ucc_tune_runner._run_once")
    def test_no_variance_warning_for_stable(self, mock_run):
        times = [10.0, 10.05, 9.98, 10.02, 10.01, 10.03, 9.99]
        mock_run.side_effect = [self._make_sample(t) for t in times]
        result = measure(self._spec(cv_warn_threshold=0.10))
        self.assertFalse(result.variance_warning)

    @patch("ucc_tune_runner._run_once")
    def test_extra_env_forwarded(self, mock_run):
        mock_run.return_value = self._make_sample(10.0)
        spec = self._spec(
            n_reps=2,
            extra_env={"UCC_TLS": "ucp", "UCC_TL_UCP_TUNE": "allreduce:0-inf:host:inf:@1"},
        )
        measure(spec)
        # Verify the spec that was used contains extra_env.
        called_spec = mock_run.call_args_list[0][0][0]
        self.assertEqual(called_spec.extra_env["UCC_TLS"], "ucp")


class TestMeasurePaired(unittest.TestCase):
    @staticmethod
    def _sample(us, count=1024):
        return SingleRunSample(count, count * 4, us, us, us)

    @patch("ucc_tune_runner._run_once")
    def test_balanced_orders_and_ten_complete_pairs(self, run_once):
        run_once.side_effect = lambda spec: self._sample(
            80 if spec.extra_env.get("ARM") == "A" else 100)
        result = measure_paired(RunSpec("allreduce", extra_env={"ARM": "D"}),
                                RunSpec("allreduce", extra_env={"ARM": "A"}), seed=7)
        orders = {sample.pair_id: sample.order for sample in result.samples}
        self.assertEqual(result.complete_pairs, 10)
        self.assertLessEqual(abs(list(orders.values()).count("AB")
                                 - list(orders.values()).count("BA")), 1)
        self.assertEqual(result.evidence.decision.value, "WIN")

    @patch("ucc_tune_runner._run_once")
    def test_failure_is_retained_and_falls_back(self, run_once):
        run_once.side_effect = [None, self._sample(80)] + [self._sample(100), self._sample(80)] * 13
        result = measure_paired(RunSpec("allreduce"), RunSpec("allreduce"))
        self.assertTrue(any(not sample.ok for sample in result.samples))
        self.assertEqual(result.evidence.decision.value, "DEFAULT")
        self.assertLessEqual(result.attempts, 14)

    @patch("ucc_tune_runner._run_once")
    def test_high_variance_uses_bounded_second_batch(self, run_once):
        calls = {"D": 0, "A": 0}
        def next_sample(spec):
            arm = spec.extra_env["ARM"]
            pair = calls[arm]
            calls[arm] += 1
            default = 50 if pair % 2 == 0 else 150
            return self._sample(default if arm == "D" else default * .8)
        run_once.side_effect = next_sample
        result = measure_paired(RunSpec("allreduce", extra_env={"ARM": "D"}),
                                RunSpec("allreduce", extra_env={"ARM": "A"}))
        self.assertEqual(result.complete_pairs, 20)
        self.assertLessEqual(result.attempts, 28)
        self.assertEqual(result.evidence.decision.value, "DEFAULT")

    def test_unsafe_limits_rejected(self):
        with self.assertRaises(ValueError):
            measure_paired(RunSpec("allreduce"), RunSpec("allreduce"), min_pairs=9)
        with self.assertRaises(ValueError):
            measure_paired(RunSpec("allreduce"), RunSpec("allreduce"), max_pairs=21)


if __name__ == "__main__":
    unittest.main()
