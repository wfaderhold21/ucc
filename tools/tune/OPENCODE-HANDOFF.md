# Implementation handoff → opencode (qwen3.6-27b)

**Repo:** `ucc-tuning` · **Branch:** `topic/autotune` · **Dir:** `tools/tune/`
**Source of decisions:** `DECISIONS.md` (§4 resolution) · **Date:** 2026-07-27

All changes below are **local, no-cluster** Python edits inside `tools/tune/`.
Do **not** touch any UCC C source or any tracked file outside `tools/tune/`.
After each task, run `python3 -m pytest -q` (baseline: **157 passed**) and keep it
green. Work the tasks in the numbered order — task 1 changes the token grammar
that tasks 2 depends on.

Ground rules:
- Match the existing code style (dataclasses, type hints in comments, `logger`).
- Every behavior change gets a unit test in the matching `test_*.py`.
- No new external dependencies.

---

## Task 1 — Non-overlapping team-size ranges  (§4.1 #1)

**Problem.** `TuneRange.tune_token()` hard-codes `:[{team_size}-inf]`
(`ucc_tune_sweep.py:134-145`). With `--team-sizes 8,64,512` this emits three
overlapping `[8-inf]`/`[64-inf]`/`[512-inf]` qualifiers for the same
collective+msg-range+mem-type, and the resolution order is undefined.

**Fix.** Make each measurement's token cover only the half-open team-size band
`[this_size .. next_size-1]`, with the largest size going to `inf`:
- `8,64,512` → `[8-63]`, `[64-511]`, `[512-inf]`.
- A single team size → `[8-inf]` (unchanged behavior).

**Where.**
- `tune_token()` currently receives `team_size: int`. Change the signature to
  accept the **band** instead — pass `(team_low, team_high)` where
  `team_high is None` means `inf`. Emit `[{low}-{high}]` (or `[{low}-inf]`).
- The caller must compute the band from the sorted, de-duplicated
  `--team-sizes` list. Callers of `tune_token()` are:
  - `ucc_offline_tune.py:79` (`_collect_tune_tokens`)
  - `ucc_offline_tune.py:250` (results.json emitter)
  - `ucc_offline_tune.py:418` (per-cell logging)
  - `ucc_tune_sweep.py` internal example strings (docstrings only — update text).
- Cleanest approach: compute a `team_size → (low, high)` band map **once** from
  the full `team_sizes` list in `run_tuning()` (`ucc_offline_tune.py:463-510`)
  and thread it to the emitters, rather than re-deriving it in three places.
  A `SweepResult` already knows its `spec.team_size`; a small helper
  `team_band(team_size, all_team_sizes) -> (low, high|None)` keeps it local.

**Edge cases to test (`test_ucc_tune_sweep.py` + `test_ucc_offline_tune.py`):**
- Single size → `[8-inf]`.
- Three sizes → exact non-overlapping bands, largest is `-inf`.
- Unsorted / duplicate input (`--team-sizes 512,8,8,64`) → sorted, deduped,
  correct bands.
- Two adjacent sizes `8,9` → `[8-8]`, `[9-inf]` (no gap, no overlap).

---

## Task 2 — Knob scoping for multi-team-size runs  (§4.1 #3)

**Context (already confirmed — do not re-investigate).** `UINT_RANGED`
(`src/utils/ucc_parser.h:303`) ranges over **msg-size + mem-type only, not
team-size**, so knob env vars genuinely cannot be team-size-scoped in one config
file. Today `_collect_knob_overrides()` (`ucc_offline_tune.py:84-125`) already
does "largest span wins" **and warns**, but it silently blends knob values
measured at *different team sizes* into one global env var — e.g. a radix chosen
at `team_size=8` is applied at `team_size=512`.

**Fix (guard, MVP scope).**
- When the run has **more than one team size** AND a knob's value differs across
  team sizes, **omit that knob** from `ucc_tuned.conf` / `ucc_tuned_env.sh`
  rather than emitting a single global value that is wrong for all but one band.
  Record the omission in `knob_conflicts.txt` (already written at
  `ucc_offline_tune.py:155-158`) with an explicit note that per-team-size knob
  tuning requires separate config files.
- When the run has a **single team size**, keep current behavior (emit the knob).
- Knobs whose value is identical across all team sizes are safe → still emit.

**Where.** Extend `_collect_knob_overrides()` to track `team_size` alongside
`(span, value)` so it can detect cross-team-size divergence specifically (today
it only tracks `(span, value)`). Its return already flows to the emitter; add a
third grouping key. Keep the existing intra-cell "largest span wins" for the
single-team-size case.

**Tests (`test_ucc_offline_tune.py`):**
- Single team size, conflicting spans → knob still emitted (largest-span value).
- Two team sizes, same knob value → emitted.
- Two team sizes, divergent knob values → **omitted**, and a
  `knob_conflicts.txt` line explains why.

**Explicitly out of scope (deferred, do NOT build):** per-team-size output
directories. Just guard + document.

---

## Task 3 — Skip asymmetric / `*v` / rooted collectives by default  (§4.1 #5)

**Problem.** For `allgatherv` / `alltoallv` / rooted `reduce`, the size-0 lookup
collapse means message-range tuning may be meaningless, but the sweep will still
emit dubious TUNE tokens for them.

**Fix.**
- Define a module-level constant of collectives to skip by default, e.g.
  `_ASYMMETRIC_COLLS = {"allgatherv", "alltoallv", "reduce_scatterv",
  "gatherv", "scatterv", "reduce", "gather", "scatter", "bcast"}` — **scope this
  carefully:** the concern is (a) the `*v` variants and (b) rooted collectives.
  Confirm the exact set against `ucc_pt_op_map` in
  `tools/perf/ucc_pt_config.cc` before finalizing; when unsure, skip only the
  clearly-affected ones (`*v` + rooted `reduce`/`gather`/`scatter`/`bcast`).
- In the pair-building step (`ucc_offline_tune.py:499-507` loop and the
  auto-discovery at `:627-645`), drop these collectives from the sweep with a
  `logger.warning` + a `skipped` entry (mirrors the existing empty-map skip
  path), so coverage is not overclaimed.
- Add an opt-in flag `--force-asymmetric` (argparse) that re-includes them for
  users who know what they want.

**Tests (`test_ucc_offline_tune.py`):**
- Default run with `alltoallv` in the pair list → skipped, warning recorded.
- `--force-asymmetric` → included.
- Symmetric collectives (`allreduce`, `alltoall`) → always included.

---

## Task 4 — Decision-free §3.3 follow-ups (bundle)

These need no design decision; they were already blessed in `NEXT-STEPS.md` §3.3.

**4a. Fail loudly on an empty algorithm map.** Today a total enumeration
failure degrades into per-cell "no algorithms found" warnings
(`ucc_offline_tune.py:501-507`) that read like a legitimate build config. Add a
pre-check right after `alg_map = run_ucc_info_algs(...)`
(`ucc_offline_tune.py:491`): if `alg_map` is empty (no components at all), raise
a hard error that names the `ucc_info` path and echoes the first ~5 lines of its
output. (Per-cell skips stay warnings — this is only the total-failure case.)

**4b. Deduplicate the `ucc_info -A` invocation.** `main()` calls
`run_ucc_info_algs()` (`:627`) for auto-discovery and then `run_tuning()` calls
it again (`:491`) — two subprocess launches per run under the MPI launcher.
Compute the map once in `main()` and thread it into `run_tuning()` via the
existing `extra_ucc_info_env`-adjacent path: add an optional
`alg_map: Optional[dict] = None` param to `run_tuning()`; if provided, skip the
internal `run_ucc_info_algs()` call. Update `TestRunTuning` in
`test_ucc_offline_tune.py` (the doc flagged this touches its signature).

**4c. Stamp a minimum-UCC-version note into the generated config.** In
`_build_conf_lines()` / `_build_sh_lines()` (`ucc_offline_tune.py:169-215`), add
a comment line noting that long TUNE strings require `UCC_INI_MAX_LINE` ≥ 8192
(UCC commit `281a0eb5` or newer); older UCC silently truncates. Header comment
only — no behavior change.

**Tests:** empty-map → raises with path in message; `run_tuning(alg_map=...)`
makes zero `ucc_info` subprocess calls (assert via the existing subprocess mock);
generated `.conf` contains the version-note comment.

---

## Not in this handoff

- **§4.1 #2** (msg-range boundary inclusive/exclusive) — deferred to a live
  cluster run; no code change.
- **§4.1 #6** (mid-sweep checkpoint/resume) — deferred.
- **§4.1 #4 / #8** — resolved/dropped in `DECISIONS.md` (#4 doc-corrected in
  `NEXT-STEPS.md` already).
- **§4.1 #7** (versioning) — a git/commit decision, not code: commit
  `tools/tune/` to `topic/autotune`, no `configure.ac` wiring until §3.2 passes.
- **§3.2 cluster verification** — that's **task 19** (remote), not opencode.

## Done criteria

- `python3 -m pytest -q` green, test count strictly above 157.
- A stubbed end-to-end run with `--team-sizes 8,64,512 --collective
  allreduce,alltoallv --mem-type host,cuda-mng` produces: non-overlapping
  team-size bands, `alltoallv` skipped (absent `--force-asymmetric`), a
  `knob_conflicts.txt` for any cross-team-size knob divergence, and a
  version-note comment in `ucc_tuned.conf`.
