#!/usr/bin/env python3
"""
ucc_tune_sweep.py — coordinate-descent algorithm sweep and range coalescing.

Implements Stages 2 and 3 of the offline tuner for one (component, collective,
mem_type, team_size) cell:

  Stage 2.1  Per msg-size: sweep all candidate algorithms with default knobs.
             Pick winner per size.  Measure UCC default for margin comparison.
  Stage 2.2  Per coalesced algorithm range: sweep secondary knobs (radix /
             pipeline / posts) one dimension at a time using the first size in
             the range as the representative.  Only for sizes where the winner
             exceeds the margin threshold vs. the default.
  Stage 3    Coalesce adjacent same-(alg, knobs) decisions into byte ranges.
             Adjacent sizes with the same algorithm but separated by a
             no-override gap are kept in separate ranges.

Output is a SweepResult containing a list of TuneRange objects whose
tune_token() method emits the TUNE string token for that range.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Optional

from ucc_tune_runner import RunResult, RunSpec, measure
from ucc_tune_space import (
    AlgInfo,
    Knob,
    bytes_to_count,
    competition_env,
    knobs_for,
    tune_env_var,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mem-type string mapping
# Perftest CLI strings → UCC TUNE grammar strings (from ucc_mc_base.c)
# ---------------------------------------------------------------------------

_PERFTEST_TO_TUNE_MEM: dict[str, str] = {
    "host":     "host",
    "cuda":     "cuda",
    "cuda-mng": "cuda-managed",
    "rocm":     "rocm",
}


def _mem_type_for_tune(perftest_mem_type: str) -> str:
    """Convert a perftest CLI mem_type string to the form used in TUNE tokens."""
    mt = _PERFTEST_TO_TUNE_MEM.get(perftest_mem_type)
    if mt is None:
        raise ValueError(
            f"Unknown perftest mem_type {perftest_mem_type!r}. "
            f"Valid: {sorted(_PERFTEST_TO_TUNE_MEM)}"
        )
    return mt


# ---------------------------------------------------------------------------
# Byte-size formatter for TUNE msg_range
# ---------------------------------------------------------------------------

def _fmt_bytes(n: int) -> str:
    """Format a byte count as a compact memunits string (k/M/G or plain)."""
    for suffix, factor in (("G", 1 << 30), ("M", 1 << 20), ("k", 1 << 10)):
        if n >= factor and n % factor == 0:
            return f"{n // factor}{suffix}"
    return str(n)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class SweepSpec:
    """
    Parameters for sweeping one (component, collective, mem_type, team_size) cell.
    """
    component: str              # e.g. "tl/ucp"
    collective: str             # e.g. "allreduce"
    mem_type: str               # perftest CLI form: "host", "cuda", "cuda-mng"
    team_size: int              # actual team size, used in TUNE token [ts-inf]
    msg_sizes_bytes: list       # list[int] from msg_size_grid(); in bytes
    alg_list: list              # list[AlgInfo] from parse_ucc_info_algs()
    all_team_sizes: list = ()   # full team-sizes list for band computation

    datatype: str = "float32"
    reduction_op: str = "sum"
    n_reps: int = 7
    n_iter: int = 1000
    n_warmup: int = 100
    persistent: bool = True
    margin_threshold: float = 0.05   # only override when speedup > 5%
    mpi_launcher: list = dataclasses.field(
        default_factory=lambda: ["mpirun", "-np", "1"]
    )
    perftest_path: str = "ucc_perftest"
    timeout_s: int = 120


@dataclasses.dataclass
class SizeDecision:
    """
    Tuning decision for one msg-size point within a cell.

    should_override=False means the UCC default is within margin_threshold of
    the best measured algorithm; no TUNE token is needed for this size.
    """
    size_bytes: int
    should_override: bool
    winner_name: str
    winner_id: int
    winner_median_us: float
    default_median_us: Optional[float]   # None if default measurement failed
    margin: float                        # (default - winner) / default
    knob_overrides: dict                 # env_var → str; filled in pass 2


@dataclasses.dataclass
class TuneRange:
    """A coalesced msg-size range with a fixed algorithm and knob configuration."""
    start_bytes: int
    end_bytes: Optional[int]   # None → "inf"
    alg_name: str
    alg_id: int
    knob_overrides: dict       # env_var → str (companion env vars, not in token)

    def tune_token(
        self,
        collective: str,
        mem_type_tune: str,
        team_low: int,
        team_high: Optional[int] = None,
    ) -> str:
        """
        Return the '#'-ready TUNE token for this range.

        team_high=None means inf (single team size or largest band).

        Example: "allreduce:0-128k:cuda:[8-63]:inf:@sra_knomial"
        """
        start = _fmt_bytes(self.start_bytes)
        end = "inf" if self.end_bytes is None else _fmt_bytes(self.end_bytes)
        high_str = "inf" if team_high is None else str(team_high)
        return (
            f"{collective}:{start}-{end}:{mem_type_tune}"
            f":[{team_low}-{high_str}]:inf:@{self.alg_name}"
        )


def _compute_team_bands(
    team_sizes: list,  # list[int]
) -> dict[int, tuple[int, int | None]]:
    """
    Compute non-overlapping half-open band for each team size.

    Returns a mapping {team_size: (low, high|None)} where `high` is the
    exclusive upper bound (None means inf).  Input is sorted and deduped
    before processing.

    Examples:
        [8]       → {8: (8, None)}           i.e. [8-inf]
        [8, 64]  → {8: (8, 63), 64: (64, None)}  i.e. [8-63], [64-inf]
        [8, 9]   → {8: (8, 8), 9: (9, None)}     i.e. [8-8], [9-inf]
    """
    sizes = sorted(set(team_sizes))
    bands: dict[int, tuple[int, int | None]] = {}
    for i, ts in enumerate(sizes):
        if i + 1 < len(sizes):
            bands[ts] = (ts, sizes[i + 1] - 1)
        else:
            bands[ts] = (ts, None)
    return bands


@dataclasses.dataclass
class SweepResult:
    """Complete tuning result for one (component, collective, mem_type, team_size) cell."""
    spec: SweepSpec
    size_decisions: list       # list[SizeDecision]
    tune_ranges: list          # list[TuneRange]
    warnings: list             # list[str] (variance, failed algs, etc.)


# ---------------------------------------------------------------------------
# Environment builders
# ---------------------------------------------------------------------------

def _forced_alg_env(spec: SweepSpec, alg_name: str) -> dict:
    """
    Build extra_env that forces a specific algorithm for all sizes and mem_types
    while isolating the target component (competition_env).
    """
    comp_env = competition_env(spec.component)
    tune_var = tune_env_var(spec.component)
    mt = _mem_type_for_tune(spec.mem_type)
    tune_val = f"{spec.collective}:0-inf:{mt}:[1-inf]:inf:@{alg_name}"
    return {**comp_env, tune_var: tune_val}


def _default_env(spec: SweepSpec) -> dict:
    """
    Build extra_env for measuring UCC's own default selection (no TUNE override).
    Only competition_env is applied so no competing component wins.
    """
    return competition_env(spec.component)


# ---------------------------------------------------------------------------
# RunSpec factory
# ---------------------------------------------------------------------------

def _run_spec_for(spec: SweepSpec, size_bytes: int, extra_env: dict) -> RunSpec:
    return RunSpec(
        collective=spec.collective,
        mem_type=spec.mem_type,
        count=bytes_to_count(size_bytes, spec.datatype),
        datatype=spec.datatype,
        reduction_op=spec.reduction_op,
        n_reps=spec.n_reps,
        n_iter=spec.n_iter,
        n_warmup=spec.n_warmup,
        persistent=spec.persistent,
        extra_env=extra_env,
        mpi_launcher=list(spec.mpi_launcher),
        perftest_path=spec.perftest_path,
        timeout_s=spec.timeout_s,
    )


def _measure_safe(
    spec: SweepSpec, size_bytes: int, extra_env: dict, label: str
) -> Optional[RunResult]:
    """Run measure() and return None (logging a warning) on failure."""
    rs = _run_spec_for(spec, size_bytes, extra_env)
    try:
        return measure(rs)
    except RuntimeError as exc:
        logger.warning("Measurement failed [%s size=%d]: %s", label, size_bytes, exc)
        return None


# ---------------------------------------------------------------------------
# Stage 2.1 — algorithm sweep at one size
# ---------------------------------------------------------------------------

def _sweep_algs_at_size(
    spec: SweepSpec, size_bytes: int
) -> dict:   # alg_name → RunResult
    """
    Measure every algorithm in spec.alg_list at a single message size.
    Failed algorithms are omitted from the returned dict.
    """
    results: dict = {}
    for alg in spec.alg_list:
        env = _forced_alg_env(spec, alg.name)
        label = f"{spec.component}/{spec.collective}/@{alg.name}"
        result = _measure_safe(spec, size_bytes, env, label)
        if result is not None:
            results[alg.name] = result
    return results


# ---------------------------------------------------------------------------
# Stage 2.2 — knob sweep for one (algorithm, size)
# ---------------------------------------------------------------------------

def _sweep_knobs_at_size(
    spec: SweepSpec,
    size_bytes: int,
    alg_name: str,
    ks: list,  # list[Knob]
) -> dict:  # env_var → best_value str
    """
    Coordinate-descent knob sweep: sweep each knob dimension in order,
    locking in the best value before moving to the next.

    Returns a dict of env_var overrides (only knobs that improve over default).
    Already-optimal (default) knobs are omitted to keep configs minimal.
    """
    current_knob_overrides: dict = {}

    for knob in ks:
        env = {**_forced_alg_env(spec, alg_name), **current_knob_overrides}
        label = f"knob={knob.env_var} baseline"
        baseline = _measure_safe(spec, size_bytes, env, label)
        baseline_us = baseline.median_us if baseline is not None else None

        best_val: Optional[str] = None
        best_us = baseline_us

        for candidate in knob.candidates:
            test_env = {**env, knob.env_var: candidate}
            result = _measure_safe(spec, size_bytes, test_env,
                                   f"knob={knob.env_var}={candidate}")
            if result is None:
                continue
            if best_us is None or result.median_us < best_us:
                best_us = result.median_us
                best_val = candidate

        if best_val is not None and best_val != knob.default:
            logger.info(
                "Knob %s: best=%r (%.2f us) vs default=%r (%.2f us)",
                knob.env_var, best_val, best_us,
                knob.default, baseline_us if baseline_us else float("nan"),
            )
            current_knob_overrides[knob.env_var] = best_val

    return current_knob_overrides


# ---------------------------------------------------------------------------
# Stage 3 — coalescing
# ---------------------------------------------------------------------------

def _decision_key(d: SizeDecision) -> tuple:
    """Stable key for merging adjacent same-policy decisions."""
    return (d.winner_name, d.winner_id, tuple(sorted(d.knob_overrides.items())))


def coalesce_ranges(
    size_decisions: list,  # list[SizeDecision]; must be sorted ascending by size_bytes
    all_sizes: list,       # list[int]; the full original grid (sorted ascending)
) -> list:  # list[TuneRange]
    """
    Merge adjacent override decisions into TuneRange objects.

    Rules:
    - Only should_override=True decisions contribute to ranges.
    - A no-override size between two override-same-alg sizes breaks the merge;
      they become separate ranges because the gap means we assert nothing there.
    - start_bytes: 0 if the range opens at the first element of all_sizes,
      otherwise the first measured size in the group.
    - end_bytes: the first measured size of the next group/gap, or None (inf)
      if the range extends to the last element of all_sizes.
    """
    if not all_sizes:
        return []

    size_to_dec: dict[int, SizeDecision] = {d.size_bytes: d for d in size_decisions}
    ranges: list[TuneRange] = []
    current_group: list[SizeDecision] = []

    for i, sz in enumerate(all_sizes):
        dec = size_to_dec.get(sz)
        is_override = dec is not None and dec.should_override

        if not is_override:
            if current_group:
                ranges.append(_group_to_range(current_group, all_sizes))
                current_group = []
            continue

        # is_override=True from here
        if current_group:
            if _decision_key(dec) == _decision_key(current_group[-1]):
                current_group.append(dec)
            else:
                ranges.append(_group_to_range(current_group, all_sizes))
                current_group = [dec]
        else:
            current_group = [dec]

    if current_group:
        ranges.append(_group_to_range(current_group, all_sizes))

    return ranges


def _group_to_range(group: list, all_sizes: list) -> TuneRange:
    """Build a TuneRange from a non-empty group of same-policy SizeDecisions."""
    first, last = group[0], group[-1]

    # start_bytes: 0 when this group opens at the first measured size overall.
    start_bytes = 0 if first.size_bytes == all_sizes[0] else first.size_bytes

    # end_bytes: first size in all_sizes after last, or None (inf).
    last_idx = all_sizes.index(last.size_bytes)
    end_bytes: Optional[int] = (
        all_sizes[last_idx + 1] if last_idx + 1 < len(all_sizes) else None
    )

    return TuneRange(
        start_bytes=start_bytes,
        end_bytes=end_bytes,
        alg_name=first.winner_name,
        alg_id=first.winner_id,
        knob_overrides=dict(first.knob_overrides),
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def sweep_cell(spec: SweepSpec) -> SweepResult:
    """
    Sweep one (component, collective, mem_type, team_size) cell.

    Returns a SweepResult with the coalesced TuneRanges and all intermediate
    data needed for logging and validation.
    """
    if not spec.alg_list:
        logger.warning(
            "No algorithms for %s/%s — returning empty result",
            spec.component, spec.collective,
        )
        return SweepResult(spec=spec, size_decisions=[], tune_ranges=[], warnings=[])

    warnings: list[str] = []
    size_decisions: list[SizeDecision] = []

    # ------------------------------------------------------------------
    # Pass 1: algorithm sweep per size (default knobs)
    # ------------------------------------------------------------------
    for size in spec.msg_sizes_bytes:
        logger.info(
            "[%s/%s mem=%s ts=%d] sweeping algs at size=%s",
            spec.component, spec.collective, spec.mem_type,
            spec.team_size, _fmt_bytes(size),
        )

        alg_results = _sweep_algs_at_size(spec, size)
        if not alg_results:
            w = (f"All algorithms failed at {_fmt_bytes(size)} — "
                 f"size skipped for {spec.component}/{spec.collective}")
            logger.warning(w)
            warnings.append(w)
            continue

        default_result = _measure_safe(
            spec, size, _default_env(spec),
            f"{spec.component}/{spec.collective}/default",
        )
        default_us: Optional[float] = (
            default_result.median_us if default_result is not None else None
        )

        # Record variance warnings for noisy measurements.
        for alg_name, res in alg_results.items():
            if res.variance_warning:
                w = (f"High CV ({res.cv * 100:.1f}%) for {alg_name} at "
                     f"{_fmt_bytes(size)} — result may be unreliable")
                warnings.append(w)

        winner_name, winner_result = min(
            alg_results.items(), key=lambda kv: kv[1].median_us
        )
        winner_alg = next(a for a in spec.alg_list if a.name == winner_name)

        if default_us is not None:
            margin = (default_us - winner_result.median_us) / default_us
            should_override = margin > spec.margin_threshold
        else:
            # Cannot compare — treat as needing override to be conservative.
            margin = 0.0
            should_override = True

        size_decisions.append(SizeDecision(
            size_bytes=size,
            should_override=should_override,
            winner_name=winner_name,
            winner_id=winner_alg.id,
            winner_median_us=winner_result.median_us,
            default_median_us=default_us,
            margin=margin,
            knob_overrides={},  # filled in pass 2
        ))

        logger.info(
            "  winner=@%s (%.2f us)  default=%.2f us  margin=%.1f%%  override=%s",
            winner_name,
            winner_result.median_us,
            default_us if default_us else float("nan"),
            margin * 100,
            should_override,
        )

    # ------------------------------------------------------------------
    # Preliminary coalesce (algorithm only, no knobs yet).
    # ------------------------------------------------------------------
    prelim_ranges = coalesce_ranges(size_decisions, spec.msg_sizes_bytes)

    # ------------------------------------------------------------------
    # Pass 2: knob sweep per preliminary range.
    # ------------------------------------------------------------------
    for prange in prelim_ranges:
        ks = knobs_for(spec.component, spec.collective, prange.alg_name)
        if not ks:
            continue

        # Representative size: first measured size in this range.
        rep_size = next(
            (d.size_bytes for d in size_decisions
             if d.should_override and d.winner_name == prange.alg_name
             and d.size_bytes >= prange.start_bytes
             and (prange.end_bytes is None or d.size_bytes < prange.end_bytes)),
            None,
        )
        if rep_size is None:
            continue

        logger.info(
            "[%s/%s] sweeping knobs for @%s at %s",
            spec.component, spec.collective,
            prange.alg_name, _fmt_bytes(rep_size),
        )
        best_knobs = _sweep_knobs_at_size(spec, rep_size, prange.alg_name, ks)

        # Propagate best knobs to every SizeDecision in this range.
        if best_knobs:
            for dec in size_decisions:
                if (dec.should_override
                        and dec.winner_name == prange.alg_name
                        and dec.size_bytes >= prange.start_bytes
                        and (prange.end_bytes is None
                             or dec.size_bytes < prange.end_bytes)):
                    dec.knob_overrides = dict(best_knobs)

    # ------------------------------------------------------------------
    # Final coalesce (with knob overrides now set).
    # ------------------------------------------------------------------
    tune_ranges = coalesce_ranges(size_decisions, spec.msg_sizes_bytes)

    return SweepResult(
        spec=spec,
        size_decisions=size_decisions,
        tune_ranges=tune_ranges,
        warnings=warnings,
    )
