#!/usr/bin/env python3
"""
ucc_offline_tune.py — top-level driver for the UCC offline algorithm tuner.

Implements the full Stages 0–4 of the offline tuner plan:

  Stage 0  Fingerprint the platform.
  Stage 1  Enumerate the search space via ucc_info -A.
  Stage 2  Sweep algorithms + knobs for each cell (via sweep_cell).
  Stage 3  Coalesce decisions into TUNE ranges (done inside sweep_cell).
  Stage 4  Validate: run the generated config vs. UCC default on a few
           representative sizes; confirm it is faster (not just different).

Output files written to --output-dir:
  ucc_tuned.conf        UCC_CONFIG_FILE-compatible config (KEY=VALUE).
  ucc_tuned_env.sh      Shell script: source to export the same vars.
  fingerprint.json      Platform fingerprint used to tag this run.
  results.json          All SweepResult data (for later analysis).
  tuning_summary.txt    Human-readable per-cell log and skipped-items list.

Correctness note (from plan addendum):
  ucc_perftest does not validate output buffers — the -c flag selects the
  collective, it is NOT a correctness check.  Run MPI or gtest coverage
  separately before deploying generated configs.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ucc_tune_fingerprint import Fingerprint, collect as collect_fingerprint
from ucc_tune_runner import RunSpec, measure
from ucc_tune_space import (
    bytes_to_count,
    competition_env,
    msg_size_grid,
    parse_ucc_info_algs,
    run_ucc_info_algs,
    run_ucc_info_raw,
    tune_env_var,
)
from ucc_tune_sweep import (
    SweepResult,
    SweepSpec,
    TuneRange,
    _compute_team_bands,
    _fmt_bytes,
    _mem_type_for_tune,
    sweep_cell,
)

logger = logging.getLogger(__name__)

# Collectives to skip by default: *v variants and rooted collectives.
# Rationale: size-0 lookup collapse makes message-range tuning dubious for these.
_ASYMMETRIC_COLLS = frozenset({
    "allgatherv", "alltoallv", "reduce_scatterv",
    "gatherv", "scatterv",
    "reduce", "gather", "scatter", "bcast",
})


# ---------------------------------------------------------------------------
# Emission helpers
# ---------------------------------------------------------------------------

def _collect_tune_tokens(
    results: list,     # list[SweepResult]
) -> dict:            # tune_var → list[str token]
    """
    Aggregate all TuneRange tokens across cells, grouped by component TUNE var.
    Uses non-overlapping team-size bands when multiple team sizes are present.
    """
    tokens: dict[str, list[str]] = {}
    for result in results:
        if not result.tune_ranges:
            continue
        spec = result.spec
        tune_var = tune_env_var(spec.component)
        mt = _mem_type_for_tune(spec.mem_type)
        bands = _compute_team_bands(spec.all_team_sizes or [spec.team_size])
        team_low, team_high = bands.get(spec.team_size, (spec.team_size, None))
        for tr in result.tune_ranges:
            tok = tr.tune_token(spec.collective, mt, team_low, team_high)
            tokens.setdefault(tune_var, []).append(tok)
    return tokens


def _collect_knob_overrides(
    results: list,     # list[SweepResult]
) -> tuple[dict, list[str]]:
    """
    Collect companion knob env var overrides from all TuneRanges.

    When the same env var appears with different values across ranges (which
    can happen because knob env vars are global, not range-scoped), the value
    from the range covering the most bytes is kept and a warning is recorded.

    For multi-team-size runs: if a knob's value differs across team sizes,
    it is omitted entirely (since UINT_RANGED knobs cannot be team-size-scoped)
    and the omission is recorded in warnings.

    Returns (knob_env: dict[env_var → value], warnings: list[str]).
    """
    # Track: env_var → list of (span_bytes, value, team_size)
    seen: dict[str, list[tuple[int, str, int]]] = {}
    for result in results:
        ts = result.spec.team_size
        for tr in result.tune_ranges:
            span = (
                (tr.end_bytes - tr.start_bytes)
                if tr.end_bytes is not None
                else (1 << 62)
            )
            for env_var, val in tr.knob_overrides.items():
                seen.setdefault(env_var, []).append((span, val, ts))

    knob_env: dict[str, str] = {}
    warnings: list[str] = []
    for env_var, entries in seen.items():
        # Check if values differ across team sizes.
        ts_to_vals: dict[int, set[str]] = {}
        for _, v, ts in entries:
            ts_to_vals.setdefault(ts, set()).add(v)
        all_team_sizes = sorted(ts_to_vals.keys())

        if len(all_team_sizes) > 1:
            # Multi-team-size run — check divergence.
            per_ts_values = {}
            divergent = False
            for ts in all_team_sizes:
                vals = ts_to_vals[ts]
                if len(vals) != 1:
                    divergent = True
                    break
                per_ts_values[ts] = next(iter(vals))
            else:
                # Exactly one value per team size — check if they differ.
                unique_vals = set(per_ts_values.values())
                if len(unique_vals) > 1:
                    divergent = True

            if divergent:
                w = (
                    f"Knob {env_var} has divergent values across team sizes "
                    f"{all_team_sizes}: omitted. Per-team-size knob tuning "
                    "requires separate config files."
                )
                warnings.append(w)
                logger.warning(w)
                continue

        # Pick the value from the largest-span range.
        entries.sort(reverse=True)
        best_val = entries[0][1]
        knob_env[env_var] = best_val
        distinct = {v for _, v, _ in entries}
        if len(distinct) > 1:
            w = (
                f"Knob conflict for {env_var}: values {sorted(distinct)} across ranges. "
                f"Using {best_val!r} (largest span). "
                "Consider per-range tuning for this knob."
            )
            warnings.append(w)
            logger.warning(w)

    return knob_env, warnings


def emit_conf(
    output_dir: Path,
    results: list,   # list[SweepResult]
    fingerprint: Fingerprint,
) -> dict:           # output paths
    """
    Write ucc_tuned.conf, ucc_tuned_env.sh, fingerprint.json, results.json.
    Returns a dict of {name: Path} for each output file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    tune_tokens = _collect_tune_tokens(results)
    knob_env, knob_warnings = _collect_knob_overrides(results)

    conf_lines = _build_conf_lines(tune_tokens, knob_env, fingerprint)
    sh_lines = _build_sh_lines(tune_tokens, knob_env, fingerprint)

    conf_path = output_dir / "ucc_tuned.conf"
    sh_path   = output_dir / "ucc_tuned_env.sh"
    fp_path   = output_dir / "fingerprint.json"
    res_path  = output_dir / "results.json"

    conf_path.write_text("\n".join(conf_lines) + "\n")
    sh_path.write_text("\n".join(sh_lines) + "\n")
    fp_path.write_text(json.dumps(dataclasses.asdict(fingerprint), indent=2))
    res_path.write_text(json.dumps(_results_to_json(results), indent=2))

    if knob_warnings:
        (output_dir / "knob_conflicts.txt").write_text(
            "\n".join(knob_warnings) + "\n"
        )

    return {
        "conf": conf_path,
        "sh": sh_path,
        "fingerprint": fp_path,
        "results": res_path,
    }


def _build_conf_lines(
    tune_tokens: dict,
    knob_env: dict,
    fingerprint: Fingerprint,
) -> list:  # list[str]
    """Build lines for the UCC_CONFIG_FILE-compatible ucc_tuned.conf file."""
    lines = [
        "# UCC Tuning Configuration",
        f"# Generated : {fingerprint.timestamp}",
        f"# UCC       : {fingerprint.ucc_version}",
        f"# CPU       : {fingerprint.cpu_model}",
        f"# GPU       : {fingerprint.gpu_model}",
        f"# Driver    : {fingerprint.gpu_driver}",
        f"# Hash      : {fingerprint.hash}",
        "#",
        "# Set UCC_CONFIG_FILE=/path/to/ucc_tuned.conf to apply.",
        "# Correctness note: validate buffer correctness separately —",
        "#   ucc_perftest does not check output buffers.",
        "# Version note: long TUNE strings require UCC_INI_MAX_LINE >= 8192",
        "#   (UCC commit 281a0eb5 or newer); older UCC silently truncates.",
        "",
    ]
    for tune_var, tokens in sorted(tune_tokens.items()):
        lines.append(f"{tune_var}={'#'.join(tokens)}")
    if knob_env:
        lines.append("")
    for env_var, val in sorted(knob_env.items()):
        lines.append(f"{env_var}={val}")
    return lines


def _build_sh_lines(
    tune_tokens: dict,
    knob_env: dict,
    fingerprint: Fingerprint,
) -> list:  # list[str]
    """Build lines for the sourceable shell env script."""
    lines = [
        "#!/bin/sh",
        "# UCC Tuning Environment",
        f"# Generated : {fingerprint.timestamp}",
        f"# UCC       : {fingerprint.ucc_version}",
        f"# Hash      : {fingerprint.hash}",
        "#",
        "# Usage: source ucc_tuned_env.sh",
        "# Version note: long TUNE strings require UCC_INI_MAX_LINE >= 8192",
        "#   (UCC commit 281a0eb5 or newer); older UCC silently truncates.",
        "",
    ]
    for tune_var, tokens in sorted(tune_tokens.items()):
        lines.append(f"export {tune_var}='{('#').join(tokens)}'")
    for env_var, val in sorted(knob_env.items()):
        lines.append(f"export {env_var}='{val}'")
    return lines


def _results_to_json(results: list) -> list:
    """Serialize SweepResults to a JSON-safe list."""
    out = []
    for r in results:
        spec = r.spec
        bands = _compute_team_bands(spec.all_team_sizes or [spec.team_size])
        team_low, team_high = bands.get(spec.team_size, (spec.team_size, None))
        out.append({
            "component":  spec.component,
            "collective": spec.collective,
            "mem_type":   spec.mem_type,
            "team_size":  spec.team_size,
            "size_decisions": [
                {
                    "size_bytes":        d.size_bytes,
                    "should_override":   d.should_override,
                    "winner_name":       d.winner_name,
                    "winner_id":         d.winner_id,
                    "winner_median_us":  d.winner_median_us,
                    "default_median_us": d.default_median_us,
                    "margin":            d.margin,
                    "knob_overrides":    d.knob_overrides,
                }
                for d in r.size_decisions
            ],
            "tune_ranges": [
                {
                    "start_bytes":    tr.start_bytes,
                    "end_bytes":      tr.end_bytes,
                    "alg_name":       tr.alg_name,
                    "alg_id":         tr.alg_id,
                    "knob_overrides": tr.knob_overrides,
                    "tune_token": tr.tune_token(
                        spec.collective,
                        _mem_type_for_tune(spec.mem_type),
                        team_low, team_high,
                    ),
                }
                for tr in r.tune_ranges
            ],
            "warnings": r.warnings,
        })
    return out


# ---------------------------------------------------------------------------
# Stage 4 — validation
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class ValidationPoint:
    collective: str
    mem_type: str
    size_bytes: int
    tuned_median_us: float
    default_median_us: float
    speedup: float         # (default - tuned) / default
    passed: bool           # True if speedup > margin_threshold


def validate(
    results: list,          # list[SweepResult] — the sweep output
    tune_tokens: dict,      # tune_var → list[token] from _collect_tune_tokens()
    knob_env: dict,         # env_var → val from _collect_knob_overrides()
    margin_threshold: float = 0.05,
    n_reps: int = 5,
    n_iter: int = 200,
    n_warmup: int = 20,
) -> list:    # list[ValidationPoint]
    """
    Stage 4: run a few representative (collective, mem_type, size) points with
    and without the generated config.  Returns one ValidationPoint per point.

    The tuned env is: competition_env(component) + full TUNE string for that
    component + companion knob overrides.
    Correctness is NOT checked here — run MPI/gtest coverage separately.
    """
    # Build the full tuned env: all tune vars + knob vars.
    tuned_env_base: dict = {}
    for tune_var, tokens in tune_tokens.items():
        tuned_env_base[tune_var] = "#".join(tokens)
    tuned_env_base.update(knob_env)

    points: list[ValidationPoint] = []

    for result in results:
        if not result.tune_ranges:
            continue
        spec = result.spec

        # Pick representative sizes: midpoint of each range, capped at 3 per cell.
        rep_sizes = _representative_sizes(result.tune_ranges, spec.msg_sizes_bytes)[:3]

        for size in rep_sizes:
            count = bytes_to_count(size, spec.datatype)
            comp_env = competition_env(spec.component)

            tuned_env = {**comp_env, **tuned_env_base}
            default_env = dict(comp_env)

            rs_tuned = RunSpec(
                collective=spec.collective,
                mem_type=spec.mem_type,
                count=count,
                datatype=spec.datatype,
                reduction_op=spec.reduction_op,
                n_reps=n_reps,
                n_iter=n_iter,
                n_warmup=n_warmup,
                persistent=spec.persistent,
                extra_env=tuned_env,
                mpi_launcher=list(spec.mpi_launcher),
                perftest_path=spec.perftest_path,
                timeout_s=spec.timeout_s,
            )
            rs_default = dataclasses.replace(rs_tuned, extra_env=default_env)

            try:
                r_tuned = measure(rs_tuned)
                r_default = measure(rs_default)
            except RuntimeError as exc:
                logger.warning(
                    "Validation failed at %s/%s size=%s: %s",
                    spec.component, spec.collective, _fmt_bytes(size), exc,
                )
                continue

            speedup = (r_default.median_us - r_tuned.median_us) / r_default.median_us
            passed = speedup > margin_threshold
            vp = ValidationPoint(
                collective=spec.collective,
                mem_type=spec.mem_type,
                size_bytes=size,
                tuned_median_us=r_tuned.median_us,
                default_median_us=r_default.median_us,
                speedup=speedup,
                passed=passed,
            )
            points.append(vp)

            status = "PASS" if passed else "FAIL"
            logger.info(
                "Validation [%s] %s/%s size=%s: tuned=%.2f us  default=%.2f us  speedup=%.1f%%",
                status, spec.collective, spec.mem_type, _fmt_bytes(size),
                r_tuned.median_us, r_default.median_us, speedup * 100,
            )

    return points


def _representative_sizes(
    tune_ranges: list,      # list[TuneRange]
    all_sizes: list,        # list[int] from spec.msg_sizes_bytes
) -> list:                  # list[int]
    """Pick one measured size from the middle of each TuneRange."""
    sizes = []
    for tr in tune_ranges:
        candidates = [
            s for s in all_sizes
            if s >= tr.start_bytes and (tr.end_bytes is None or s < tr.end_bytes)
        ]
        if candidates:
            sizes.append(candidates[len(candidates) // 2])
    return sizes


# ---------------------------------------------------------------------------
# Tuning summary log
# ---------------------------------------------------------------------------

def write_summary(
    output_dir: Path,
    results: list,
    validation_points: list,
    fingerprint: Fingerprint,
    skipped: list,          # list[str] describing skipped cells
) -> Path:
    """Write a human-readable tuning_summary.txt."""
    lines = [
        "=" * 72,
        "UCC Offline Tuner — Tuning Summary",
        "=" * 72,
        "",
        fingerprint.summary(),
        "",
        "-" * 72,
        "Tuning results",
        "-" * 72,
    ]

    for result in results:
        spec = result.spec
        label = (f"{spec.component}/{spec.collective} "
                  f"mem={spec.mem_type} team_size={spec.team_size}")
        lines.append(f"\n{label}")
        if not result.tune_ranges:
            lines.append("  (no overrides needed — UCC default is within margin)")
            continue
        bands = _compute_team_bands(spec.all_team_sizes or [spec.team_size])
        team_low, team_high = bands.get(spec.team_size, (spec.team_size, None))
        for tr in result.tune_ranges:
            mt = _mem_type_for_tune(spec.mem_type)
            tok = tr.tune_token(spec.collective, mt, team_low, team_high)
            lines.append(f"  {tok}")
            if tr.knob_overrides:
                for k, v in sorted(tr.knob_overrides.items()):
                    lines.append(f"    {k}={v}")
        overridden = sum(1 for d in result.size_decisions if d.should_override)
        total = len(result.size_decisions)
        lines.append(f"  ({overridden}/{total} size points overridden)")
        if result.warnings:
            for w in result.warnings:
                lines.append(f"  WARNING: {w}")

    if skipped:
        lines += ["", "-" * 72, "Skipped cells (no algorithms or all sizes failed)", "-" * 72]
        for s in skipped:
            lines.append(f"  {s}")

    if validation_points:
        lines += ["", "-" * 72, "Stage 4 validation", "-" * 72]
        pass_count = sum(1 for v in validation_points if v.passed)
        lines.append(f"  {pass_count}/{len(validation_points)} points passed")
        for vp in validation_points:
            status = "PASS" if vp.passed else "FAIL"
            lines.append(
                f"  [{status}] {vp.collective}/{vp.mem_type} "
                f"size={_fmt_bytes(vp.size_bytes)}: "
                f"speedup={vp.speedup*100:.1f}% "
                f"(tuned={vp.tuned_median_us:.1f}us "
                f"default={vp.default_median_us:.1f}us)"
            )
        lines.append("")
        lines.append(
            "NOTE: Correctness was NOT validated here. Run MPI/gtest "
            "coverage separately before deploying this config."
        )

    summary_path = output_dir / "tuning_summary.txt"
    summary_path.write_text("\n".join(lines) + "\n")
    return summary_path


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------

def run_tuning(
    component_collective_pairs: list,   # [(component, collective), ...]
    mem_types: list,                    # ["host", "cuda", ...]
    team_sizes: list,                   # [8, 64, ...]
    msg_sizes_bytes: list,
    skip_asymmetric: bool = True,
    alg_map: Optional[dict] = None,
    datatype: str = "float32",
    reduction_op: str = "sum",
    n_reps: int = 7,
    n_iter: int = 1000,
    n_warmup: int = 100,
    persistent: bool = True,
    margin_threshold: float = 0.05,
    mpi_launcher: Optional[list] = None,
    perftest_path: str = "ucc_perftest",
    ucc_info_path: str = "ucc_info",
    timeout_s: int = 120,
    extra_ucc_info_env: Optional[dict] = None,
) -> tuple[list, list]:   # (results, skipped_messages)
    """
    Stage 1–3: enumerate algorithms, sweep all cells, return SweepResults.

    Returns (results: list[SweepResult], skipped: list[str]).
    Skipped cells are logged so the output doesn't overclaim coverage.
    """
    if mpi_launcher is None:
        mpi_launcher = ["mpirun", "-np", "1"]

    logger.info("Stage 1: enumerating algorithms via ucc_info -A")
    raw_output = ""
    if alg_map is None:
        raw_output = run_ucc_info_raw(ucc_info_path, extra_ucc_info_env or {})
        alg_map = parse_ucc_info_algs(raw_output)
    if not alg_map:
        head = " ".join(raw_output.split("\n")[:5]) if raw_output else "(no output)"
        raise RuntimeError(
            f"ucc_info ({ucc_info_path}) returned empty algorithm map. "
            f"Output preview: {head}"
        )

    results: list[SweepResult] = []
    skipped: list[str] = []

    total = len(component_collective_pairs) * len(mem_types) * len(team_sizes)
    done = 0

    for comp, coll in component_collective_pairs:
        if skip_asymmetric and coll in _ASYMMETRIC_COLLS:
            msg = (f"{comp}/{coll}: skipped asymmetric collective "
                   "(use --force-asymmetric to include)")
            logger.warning(msg)
            skipped.append(msg)
            done += len(mem_types) * len(team_sizes)
            continue

        alg_list = alg_map.get(comp, {}).get(coll, [])
        if not alg_list:
            msg = (f"{comp}/{coll}: no algorithms found in ucc_info -A "
                   f"(component not built or collective not supported)")
            logger.warning(msg)
            skipped.append(msg)
            done += len(mem_types) * len(team_sizes)
            continue

        for mem_type in mem_types:
            for team_size in team_sizes:
                done += 1
                logger.info(
                    "Stage 2/3 [%d/%d]: %s/%s mem=%s team_size=%d",
                    done, total, comp, coll, mem_type, team_size,
                )
                spec = SweepSpec(
                    component=comp,
                    collective=coll,
                    mem_type=mem_type,
                    team_size=team_size,
                    msg_sizes_bytes=list(msg_sizes_bytes),
                    all_team_sizes=team_sizes,
                    alg_list=list(alg_list),
                    datatype=datatype,
                    reduction_op=reduction_op,
                    n_reps=n_reps,
                    n_iter=n_iter,
                    n_warmup=n_warmup,
                    persistent=persistent,
                    margin_threshold=margin_threshold,
                    mpi_launcher=list(mpi_launcher),
                    perftest_path=perftest_path,
                    timeout_s=timeout_s,
                )
                result = sweep_cell(spec)
                results.append(result)

                if not result.tune_ranges:
                    msg = (f"{comp}/{coll} mem={mem_type} team_size={team_size}: "
                           "UCC default is within margin for all sizes — no override emitted")
                    skipped.append(msg)

    return results, skipped


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="UCC offline algorithm tuner.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--collective", default="allreduce",
        help="Comma-separated collective names to tune.",
    )
    p.add_argument(
        "--component", default="",
        help=(
            "Comma-separated components to sweep (e.g. tl/ucp,tl/cuda). "
            "Default: all components that have the requested collective in ucc_info -A."
        ),
    )
    p.add_argument(
        "--mem-type", default="host",
        help="Comma-separated memory types (host, cuda, cuda-mng).",
    )
    p.add_argument(
        "--team-sizes", default="8",
        help="Comma-separated team sizes to tune for.",
    )
    p.add_argument("--min-bytes", type=int, default=8)
    p.add_argument("--max-bytes", type=int, default=1 << 20,
                   help="Max message size in bytes (default 1 MiB).")
    p.add_argument("--factor", type=int, default=2,
                   help="Grid multiplication factor (2 or 4).")
    p.add_argument("--datatype", default="float32")
    p.add_argument("--op", default="sum", dest="reduction_op")
    p.add_argument("--n-reps", type=int, default=7,
                   help="Independent perftest repetitions per measurement.")
    p.add_argument("--n-iter", type=int, default=1000,
                   help="Perftest -n iterations per rep.")
    p.add_argument("--n-warmup", type=int, default=100,
                   help="Perftest -w warmup iterations per rep.")
    p.add_argument("--no-persistent", action="store_true",
                   help="Disable persistent mode (includes init/finalize overhead).")
    p.add_argument("--margin", type=float, default=0.05,
                   help="Override UCC default only when speedup > this fraction.")
    p.add_argument("--launcher", default="mpirun -np 1",
                   help="MPI launcher prefix.")
    p.add_argument("--perftest", default="ucc_perftest")
    p.add_argument("--ucc-info", default="ucc_info")
    p.add_argument("--ucx-info", default="ucx_info")
    p.add_argument("--output-dir", default="./ucc_tuning_output",
                   help="Directory to write output files.")
    p.add_argument("--no-validate", action="store_true",
                    help="Skip Stage 4 validation runs.")
    p.add_argument("--force-asymmetric", action="store_true",
                   help=("Include asymmetric/*v/rooted collectives in the sweep "
                         "(default: skip them as tuning is dubious)."))
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main(argv=None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    output_dir = Path(args.output_dir)
    mpi_launcher = args.launcher.split()

    # Stage 0: fingerprint
    logger.info("Stage 0: collecting platform fingerprint")
    fingerprint = collect_fingerprint(args.ucc_info, args.ucx_info)
    logger.info("Fingerprint:\n%s", fingerprint.summary())

    # Build search space from CLI args
    collectives = [c.strip() for c in args.collective.split(",")]
    mem_types   = [m.strip() for m in args.mem_type.split(",")]
    team_sizes  = [int(t.strip()) for t in args.team_sizes.split(",")]
    sizes       = msg_size_grid(args.min_bytes, args.max_bytes, args.factor)

    # Resolve component/collective pairs
    alg_map = run_ucc_info_algs(args.ucc_info)
    if args.component:
        components = [c.strip() for c in args.component.split(",")]
        pairs = [(comp, coll) for comp in components for coll in collectives]
    else:
        # Auto-discover: use any component that has the requested collective.
        pairs = [
            (comp, coll)
            for coll in collectives
            for comp in alg_map
            if coll in alg_map.get(comp, {})
        ]
        if not pairs:
            logger.error(
                "No components found with collectives %s. "
                "Run ucc_info -A to see what is available.",
                collectives,
            )
            return 1

    logger.info(
        "Tuning %d (component, collective) × %d mem_types × %d team_sizes "
        "= %d cells, %d sizes each",
        len(pairs), len(mem_types), len(team_sizes),
        len(pairs) * len(mem_types) * len(team_sizes),
        len(sizes),
    )

    # Stages 1–3: sweep (alg_map passed to avoid duplicate ucc_info call)
    results, skipped = run_tuning(
        component_collective_pairs=pairs,
        mem_types=mem_types,
        team_sizes=team_sizes,
        msg_sizes_bytes=sizes,
        skip_asymmetric=not args.force_asymmetric,
        alg_map=alg_map,
        datatype=args.datatype,
        reduction_op=args.reduction_op,
        n_reps=args.n_reps,
        n_iter=args.n_iter,
        n_warmup=args.n_warmup,
        persistent=not args.no_persistent,
        margin_threshold=args.margin,
        mpi_launcher=mpi_launcher,
        perftest_path=args.perftest,
        ucc_info_path=args.ucc_info,
        timeout_s=300,
    )

    # Stage 4: validate
    validation_points: list = []
    if not args.no_validate:
        logger.info("Stage 4: validating generated config")
        tune_tokens = _collect_tune_tokens(results)
        knob_env, _ = _collect_knob_overrides(results)
        validation_points = validate(
            results,
            tune_tokens,
            knob_env,
            margin_threshold=args.margin,
            n_reps=min(args.n_reps, 5),
            n_iter=args.n_iter // 5,
            n_warmup=args.n_warmup // 5,
        )
        fail_count = sum(1 for v in validation_points if not v.passed)
        if fail_count:
            logger.warning(
                "%d/%d validation points failed — review tuning_summary.txt",
                fail_count, len(validation_points),
            )

    # Emit output files
    logger.info("Emitting output files to %s", output_dir)
    paths = emit_conf(output_dir, results, fingerprint)
    summary_path = write_summary(
        output_dir, results, validation_points, fingerprint, skipped
    )
    paths["summary"] = summary_path

    logger.info("Done.")
    logger.info("  Config  : %s", paths["conf"])
    logger.info("  Shell   : %s", paths["sh"])
    logger.info("  Summary : %s", paths["summary"])
    logger.info("")
    logger.info("To apply:  source %s", paths["sh"])
    logger.info("       or: export UCC_CONFIG_FILE=%s", paths["conf"])
    logger.info("")
    logger.info(
        "IMPORTANT: Run MPI/gtest correctness checks separately before "
        "deploying — ucc_perftest does not validate output buffers."
    )

    fail_validation = any(not v.passed for v in validation_points)
    return 1 if fail_validation else 0


if __name__ == "__main__":
    sys.exit(main())
