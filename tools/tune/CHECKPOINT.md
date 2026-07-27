# UCC Offline Tuner — Implementation Checkpoint

**Branch:** `topic/autotune`
**Date:** 2026-06-24
**Status:** Offline tuner (Part 1) complete, tested. Online tuner (Part 2) not started.

---

## What exists

Six Python files, ~4 000 lines, 154 unit tests (all passing, no binary required):

```
tools/tune/
  ucc_tune_runner.py          # wraps ucc_perftest — the measurement oracle
  ucc_tune_space.py           # search space: alg enumeration, knob table, grid
  ucc_tune_sweep.py           # coordinate-descent sweep and range coalescing
  ucc_tune_fingerprint.py     # platform fingerprint (versions, GPU, CPU, hash)
  ucc_offline_tune.py         # top-level driver, emission, validation, CLI
  test_ucc_tune_runner.py     # 26 tests
  test_ucc_tune_space.py      # 45 tests
  test_ucc_tune_sweep.py      # 40 tests
  test_ucc_offline_tune.py    # 43 tests
```

No UCC C code was changed. All output feeds the existing UCC runtime via TUNE strings.

---

## What each module does

### `ucc_tune_runner.py` — measurement oracle

Wraps `ucc_perftest` with corrections from the plan addendum:

- **Single-size invocations** (`-b COUNT -e COUNT`): the exponential generator
  has a hard-coded ×2 factor that `-f` cannot override, so ranged sweeps
  are constructed externally by the caller.
- **Repeat-N statistics**: `measure(RunSpec)` runs perftest `n_reps` times
  independently, collects `avg_us` from each, applies **Tukey fence** outlier
  rejection (IQR-based), and returns median + IQR + CV.
- **Variance warning**: CV > `cv_warn_threshold` (default 10%) flags a noisy
  environment; the sweep layer records this in `SweepResult.warnings`.
- **Persistent mode** (`-p`) is the default to exclude per-iteration
  `ucc_collective_init/finalize` and barrier overhead from the measurement.
- `extra_env` carries all TUNE strings and CL/TL competition-control vars;
  the runner is policy-neutral.

Key types: `RunSpec`, `SingleRunSample`, `RunResult`.

### `ucc_tune_space.py` — search space definition

- **`parse_ucc_info_algs(output)`** / **`run_ucc_info_algs(path)`**: parses
  `ucc_info -A` output into `ComponentAlgs` (dict keyed by `"tl/ucp"`,
  `"cl/hier"`, etc., then by collective name). No hard-coded algorithm tables.
- **`_KNOBS` table**: maps `(component, collective, alg_name)` to a list of
  `Knob` objects. Covers all secondary knobs from `tl_ucp.c` and `tl_cuda.c`:
  radix (knomial, SRA, SAG, SRG), SRA/SRG pipeline params (keyed format
  `thresh=…:fragsize=…:nfrags=…:pdepth=…`), sliding window buffer size,
  batched/pairwise `NUM_POSTS`, CUDA ring `MAX_RINGS`/`NUM_CHUNKS`, NVLS
  `SM_COUNT`/`THREADS`.
- **`tune_env_var(component)`**: `"tl/ucp"` → `"UCC_TL_UCP_TUNE"`.
- **`competition_env(component)`**: returns `{"UCC_TLS": name, "UCC_CLS": "basic"}`
  for TL components (isolates the target; forcing TUNE alone is insufficient if
  CUDA/NCCL/HIER still score higher).
- **`msg_size_grid(min, max, factor)`**: geometric grid in bytes.
- **`bytes_to_count(bytes, dtype)`**: converts byte size to element count for
  perftest `-b`/`-e` flags.

### `ucc_tune_sweep.py` — coordinate-descent sweep

Implements Stages 2–3 of the plan for one `(component, collective, mem_type,
team_size)` cell. Entry point: `sweep_cell(SweepSpec) → SweepResult`.

**Two-pass algorithm:**

Pass 1 — algorithm sweep per size:
- For each size in the grid, force each algorithm in turn via
  `competition_env + UCC_TL_*_TUNE=…:0-inf:…:inf:@alg_name`.
- Also measure UCC's own default (competition_env only, no TUNE override).
- Winner = `argmin(median_us)`.
- `should_override = (default_us − winner_us) / default_us > margin_threshold`.
  Sizes within margin produce no TUNE token (UCC default is kept).

Preliminary coalesce — group adjacent same-algorithm override decisions:
- A no-override size between two same-alg override sizes **breaks the merge**
  (we assert nothing about that gap; it stays at UCC's default).
- Range boundaries: `start=0` if first measured size, else the first measured
  size in the group; `end=` first measured size of the next group, or `None`
  (→ `"inf"`).

Pass 2 — knob sweep per coalesced range:
- For each `TuneRange`, look up `knobs_for(component, collective, alg_name)`.
- Coordinate descent: sweep knob 1, lock best value; sweep knob 2 with knob 1
  fixed; and so on.
- Representative size = first measured size in the range.
- Best knob values propagate to all `SizeDecision`s in that range.

Final coalesce — same algorithm as preliminary, now with knob overrides as
part of the merge key (different knobs → separate ranges).

Key types: `SweepSpec`, `SizeDecision`, `TuneRange`, `SweepResult`.

`TuneRange.tune_token(collective, mem_type_tune, team_size)` emits
e.g. `"allreduce:0-128k:host:[8-inf]:inf:@sra_knomial"`.

**Important implementation note on mem_type strings:**
UCC TUNE grammar uses `"cuda-managed"` (from `ucc_mc_base.c`), not
`"cuda_managed"`. Perftest CLI uses `"cuda-mng"`. The mapping lives in
`_PERFTEST_TO_TUNE_MEM` in `ucc_tune_sweep.py`.

### `ucc_tune_fingerprint.py` — platform identity

Collects: UCC version (`ucc_info -v`), UCX version (`ucx_info -v`), CPU model
(`/proc/cpuinfo` or `sysctl`), GPU model/driver/CUDA (`nvidia-smi`). Never
raises — missing tools produce `"unknown"` or `"none"`.

SHA-256 hash over the stable fields (version strings + hw model) for output
file naming and future DB lookup. The hash does **not** include hostname or
timestamp.

### `ucc_offline_tune.py` — top-level driver

**`run_tuning(...)`** — Stages 0–3:
1. Fingerprint the platform.
2. Run `ucc_info -A` to enumerate available algorithms.
3. For each `(component, collective, mem_type, team_size)` cell: call
   `sweep_cell()` and collect results.
4. Log skipped cells (component not built, collective not supported, or all
   sizes within margin).

**`emit_conf(output_dir, results, fingerprint)`** — emission:
- `ucc_tuned.conf`: `UCC_CONFIG_FILE`-compatible `KEY=VALUE` file.
  Keys use full env var names (`UCC_TL_UCP_TUNE=…`, `UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX=4`).
- `ucc_tuned_env.sh`: sourceable shell script exporting the same vars.
- `fingerprint.json`: platform fingerprint for DB/launcher use.
- `results.json`: all `SweepResult` data (size decisions, ranges, warnings).
- `knob_conflicts.txt` (if any): warns when the same knob env var has different
  optimal values across ranges (since knob vars are global, not range-scoped).
  Conflict resolution: the value from the largest-span range wins.

**`validate(...)`** — Stage 4:
- Runs perftest at ≤3 representative sizes per cell with and without the
  generated config.
- Reports `PASS` when speedup > `margin_threshold`, `FAIL` otherwise.
- Returns exit code 1 if any validation point fails.
- **Does not validate buffer correctness.** `ucc_perftest -c` selects the
  collective; it does not check output buffers. Run MPI/gtest coverage
  separately before deploying generated configs.

**`write_summary(...)`** — `tuning_summary.txt`:
- Per-cell TUNE tokens emitted, count of size points overridden vs. total.
- Skipped cells with reason.
- Stage 4 validation table.
- Correctness disclaimer.

---

## How to run it

```bash
cd tools/tune

# Minimal: allreduce on host with 8-rank MPI
python3 ucc_offline_tune.py \
  --collective allreduce \
  --mem-type host \
  --team-sizes 8 \
  --launcher "mpirun -np 8" \
  --perftest /path/to/ucc_perftest \
  --ucc-info /path/to/ucc_info \
  --output-dir ./out/

# Apply the result
source ./out/ucc_tuned_env.sh
# or
export UCC_CONFIG_FILE=./out/ucc_tuned.conf

# Multi-collective, CUDA, multiple team sizes
python3 ucc_offline_tune.py \
  --collective allreduce,bcast,reduce_scatter \
  --component tl/ucp,tl/cuda \
  --mem-type host,cuda \
  --team-sizes 8,64,512 \
  --min-bytes 8 \
  --max-bytes 1073741824 \
  --factor 2 \
  --n-reps 7 \
  --n-iter 1000 \
  --launcher "srun -n 512 --gpus-per-task=1" \
  --output-dir ./out_h100_512gpu/

# Skip validation (faster, useful during development)
python3 ucc_offline_tune.py ... --no-validate
```

---

## Design decisions and rationale

**No new UCC runtime machinery.** The output is a plain `KEY=VALUE` config file
consumed by the existing score map at team-create time. Zero UCC C changes.

**Single-size perftest calls.** The exponential generator's hard-coded ×2 factor
means we cannot use perftest's built-in sweep for a ×4 grid. Each size point is
a separate process invocation. This is slower but correct.

**Repeat-N + Tukey fences.** Collective timing is noisy (NUMA effects, network
jitter, OS interference). One perftest run's `avg_us` is the mean of `n_iter`
iterations — still a single sample from the environment's noise distribution.
Seven independent runs with outlier rejection gives a robust median.

**Margin threshold.** We only override UCC's default when the winner is
demonstrably faster (>5% by default). This prevents config churn from
measurement noise and keeps generated configs minimal and trustworthy.

**Two-pass sweep.** Algorithm sweep uses default knobs for all candidates;
knob sweep runs only for the winning algorithm, once per coalesced range.
A full per-size-per-alg-per-knob grid would be combinatorially impractical.

**Competition control via `UCC_TLS`/`UCC_CLS`.** Forcing `UCC_TL_UCP_TUNE`
alone does not guarantee TL/UCP wins if CUDA/NCCL/HIER components score
higher. `competition_env()` restricts to the target component for unambiguous
isolated measurement.

**No-override gaps.** When a size point falls within the margin threshold (UCC
default is good enough), it produces no TUNE token. A no-override gap between
two override sizes with the same algorithm is kept as two separate ranges
rather than merged — the gap means we are not claiming coverage there.

**Knob conflict resolution.** Knob env vars (`UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX`)
are global, not range-scoped. When different ranges have different optimal
radix values, we pick the value from the largest range (most bytes covered)
and record the conflict. This is a known MVP limitation; the `UINT_RANGED`
config type may support range-based values, but the string format is not
documented and was not used here.

**External launcher over `ucc.conf` sections.** Native `ucc.conf` section
matching is CPU-vendor and team-size aware but does not match on GPU model,
driver version, NVLink topology, or UCC/UCX/NCCL versions. The generated
`fingerprint.json` is the hook for a future launcher that selects the right
`UCC_CONFIG_FILE` based on the live allocation's fingerprint. For now, users
select the right output directory manually.

---

## Known limitations and open items from the plan

**From the "Suggested MVP constraint" and addenda:**

1. **Correctness validation is out-of-band.** `ucc_perftest -c` selects
   the collective by name; it does not validate buffers. Stage 4 only checks
   that the tuned config is faster, not correct. Separate MPI/gtest runs are
   required before deploying a generated config.

2. **`*v`/rooted/asymmetric collectives.** Invalid or asymmetric message-size
   cases collapse to size `0` for `ucc_coll_score_map_lookup()`. Message-range
   tuning may not behave uniformly across `allgatherv`, `scatterv`, rooted
   `reduce`, or asymmetric-memory cases. The current sweep treats them
   identically to symmetric collectives.

3. **Datatype and reduction op not in score map.** The TUNE string does not
   encode datatype or reduction op. A tuned radix for `float32/sum` may not
   be optimal for `bfloat16/avg`. The `results.json` records what was used,
   but the config does not distinguish.

4. **No multi-root support.** Rooted collectives (`bcast`, `reduce`,
   `scatter`, `gather`) are measured with `root=0`. Performance may differ
   at other roots, especially for ring-based algorithms.

5. **Persistent/triggered requests not tested.** The TUNE string applies at
   team-create time. Persistent and triggered collective paths cache schedules.
   Whether the tuned algorithm selection propagates correctly to those paths
   was not verified.

6. **Knob range syntax (`UINT_RANGED`) not used.** For knobs like
   `UCC_TL_UCP_ALLREDUCE_KN_RADIX` that are declared `UINT_RANGED`, the UCC
   config type may accept a range-based string to specify different values
   per message size. Using this would eliminate the knob conflict problem but
   the exact format was not found in the source and is not used here.

7. **No resume/checkpoint for interrupted sweeps.** A large sweep (many cells,
   many sizes, many reps) can run for hours. If interrupted, it restarts from
   scratch. Intermediate `results.json` are not written until the entire sweep
   finishes.

8. **`ucc.conf` section matching not extended.** GPU model, NIC, driver, and
   version fields are not in the native section matcher. The generated config
   must be selected externally (via `UCC_CONFIG_FILE` set by a launcher
   script or the user). A full fingerprint-matching DB launcher is a future
   item.

---

## What is next

### Near term — before first real cluster run

- [ ] **End-to-end test on a real cluster** — run `ucc_offline_tune.py` for
  allreduce on one collective × one mem_type × one team_size. Verify the
  generated `ucc_tuned.conf` loads cleanly with UCC and the conf-file parser
  accepts the key names produced (`UCC_TL_UCP_TUNE`, etc.).
- [ ] **Validate the TUNE string grammar** — check that `0-inf`, `cuda-managed`
  (not `cuda_managed`), and `[8-inf]` team_size syntax are all accepted by
  the live `ucc_coll_score` parser.
- [ ] **Add mid-sweep JSON checkpointing** — write partial results after each
  cell so a long sweep can be resumed.
- [ ] **`ucc_info -A` format verification** — confirm that the regex-based
  parser handles the exact output of the installed `ucc_info` binary on
  target clusters (spacing, indentation can vary with build).

### Medium term

- [ ] **Fingerprint-matching launcher script** — shell or Python script that
  reads `fingerprint.json` files from a DB directory and sets
  `UCC_CONFIG_FILE` to the best match for the running allocation.
- [ ] **Correctness validation harness** — wrapper that runs MPI allreduce
  with the tuned config and compares output buffers, or delegates to existing
  UCC gtests run with the env vars from `ucc_tuned_env.sh`.
- [ ] **Regression baseline mode** — re-run the tuner after a UCC version bump
  and diff `results.json` to detect performance regressions.
- [ ] **`*v` collective handling** — special-case allgatherv / alltoallv /
  reduce_scatterv to account for the size=0 lookup collapse.
- [ ] **Resume from partial results.json** — skip already-measured cells.

### Long term — Part 2 (online tuner)

Part 2 requires UCC C code changes and is a separate project per the plan's
sequencing recommendation. Do not start until the offline tuner is producing
real configs on at least one target cluster.

Key engineering challenges (per the plan addendum):
- Score maps are built once at `ucc_team_build_score_map()`. There is no
  public runtime hook to patch or rebuild a team's score map mid-run.
- Persistent and triggered collective requests cache schedules. A score-map
  change after request creation may not affect those requests.
- **Rank divergence** is the hard correctness constraint: every rank must
  select the same algorithm or the collective deadlocks/corrupts. Online
  exploration must use team-wide aggregated measurements and a deterministic
  decision rule.

---

## File inventory

| File | Purpose | Lines |
|---|---|---|
| `ucc_tune_runner.py` | perftest wrapper, statistics | 443 |
| `ucc_tune_space.py` | algorithm enumeration, knob table, grid helpers | 560 |
| `ucc_tune_sweep.py` | coordinate-descent sweep, coalescing | 503 |
| `ucc_tune_fingerprint.py` | platform fingerprint | 197 |
| `ucc_offline_tune.py` | driver, emission, validation, CLI | 722 |
| `test_ucc_tune_runner.py` | 26 tests | 219 |
| `test_ucc_tune_space.py` | 45 tests | 277 |
| `test_ucc_tune_sweep.py` | 40 tests | 489 |
| `test_ucc_offline_tune.py` | 43 tests | 553 |
| **Total** | | **3 963** |
