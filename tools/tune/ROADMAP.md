# UCC Offline Tuner — Roadmap

**Branch:** `topic/autotune` · **Date:** 2026-09-09 · **Status:** active sequencing decision

**Supersedes** the ordered next-steps in `NEXT-STEPS.md` §3 and the "What is next"
section of `CHECKPOINT.md`. `DECISIONS.md` (§4 verdicts) and
`NO_REGRESSION_DESIGN.md` (emission semantics) remain valid as reference; this
document changes only *what gets built next and in what order*.

---

## 1. Why the reframe

The np=8 Gaia run emitted 6 algorithm ranges: 4 beneficial, 1 wrong
(`allreduce:4k-16k:@knomial`, 12.6% slower at 16,380 B), 1 indistinguishable.

`NO_REGRESSION_DESIGN.md` §1 traces the escape path, and the root cause is
mundane: `_group_to_range()` used the next anchor as an **exclusive** Python
endpoint and emitted that number into UCC's **inclusive** message-range grammar,
so a "no win at 16K" anchor became a claim about 16K. `_representative_sizes()`
carried the same off-by-one, so validation only ever sampled the winning end.

What was built in response was paired confirmation, simultaneous CIs, a
cell-wide Holm family, order-effect permutation tests, three-arm knob
attribution, and proof budgets. The design doc states plainly that these "are
not claimed as causes of the 4 KiB anchor" — they address a hypothetical failure
(noise-driven false wins) the data never demonstrated. That machinery then
proved too expensive and became opt-in (`--proof-mode`), returning the default
path to approximately the rule that produced the bad range.

The load-bearing fix — inclusive range semantics — is unconditional and small.

Three structural concerns follow from that:

1. **Sweep cost was never measured.** Every size point is a separate MPI job
   launch (forced by the perftest ×2 generator workaround). The number that
   decides whether offline tuning is viable at all appears in none of the four
   design documents.
2. **Rigor and usefulness are in tension, and rigor won.** Team bands are
   singletons (`[8-8]` asserts nothing at 9 ranks); knobs are omitted entirely
   in multi-team-size runs because they cannot be scoped; ranges cover only
   confirmed points. Each rule is individually correct; together they push the
   emitted config toward the empty set. How many of the original 6 ranges
   survive the new rules is unknown.
3. **No loop has ever closed.** Zero accepted configs (the correctness gate has
   no producer — `main()` always passes `correctness=None`, so every run ends at
   `ucc_offline_tune.py:1275` "PROVISIONAL ONLY"). Zero measured
   application-level speedup. The premise — that UCC's defaults leave real
   performance on the table at reachable scales — rests on one 6-point
   observation.

## 2. The decision

**Both deliverables, sequenced.** Ship a *discovery tool* first and use it to
prove UCC's defaults are actually leaving performance on the table. Harden into
a *deployable autotuner* only if the findings justify it.

Rationale: a discovery finding is reviewed by a human before it changes
anything, so a wrong range costs nothing and most of the statistical gating is
unnecessary. A deployed config is trusted unreviewed, which is what makes the
full no-regression design load-bearing — but that only pays for itself once the
wins are known to exist and to be cluster-specific.

## 3. Cost model (now known)

Read off `_sweep_cell_screening()` (`ucc_tune_sweep.py:349`):

```
invocations = cells x sizes x (n_algs + 1) x n_reps
```

Screening does **no** knob work at all — `--no-knobs` is a proof-mode-only flag.

Worked example, `tl/ucp` allreduce, default domain:

| Factor | Value | Source |
|---|---|---|
| sizes | 18 | 8 B -> 1 MB, factor 2 |
| algorithms | 5 | `src/components/tl/ucp/allreduce/allreduce.h:12-16` |
| arms | 6 | 5 algorithms + default |
| `n_reps` | 7 | current default |
| **launches/cell** | **756** | |

At np=8 that is roughly 20-40 min per cell. At np=512 under `srun` it is a
couple of hours per cell, and a 24-cell breadth run (4 collectives x 2 mem types
x 3 team sizes) is days.

`n_reps` is the largest lever and the one discovery cares least about:
7 -> 3 cuts cost 2.3x, and precision is not needed when a human triages every row.

---

## 4. Phase A — discovery

### A1. Algorithm readback  *(local, do first)*

**Problem.** `RunResult` (`ucc_tune_runner.py:175`) is pure timing. The tuner
never records which algorithm UCC's default path actually selected;
`policy_selected` (`ucc_offline_tune.py:407`) is the tuner's own prediction
derived from its emitted ranges, not a readback from UCC.

This is fatal for a discovery tool. "sra_knomial is 12% faster than default at
32 KiB" is not upstreamable. "Default selected knomial there; sra_knomial is 12%
faster" is.

**Fix.** Parse the selected algorithm from UCC info-level logs on every arm and
record it on the result.

**Three payoffs from one mechanism:**
- findings become upstreamable (you know what default chose);
- forced TUNE tokens are proven to select what they claim;
- subsumes the `NO_REGRESSION_DESIGN.md` §6 criterion-5 round-trip check;
- catches the default arm silently falling through to a different TL entirely.

### A2. Cost model + `--dry-run`  *(local)*

Print planned invocation count and estimated wall-clock before anything runs.
The formula is §3; it needs a measured per-launch overhead. Purpose is to size
the allocation ask instead of guessing at it.

### A3. `findings.md` / `findings.json`  *(local)*

Ranked lead list: cell, message range, default algorithm, winning algorithm,
speedup, n, CV — sorted by opportunity. Largely a new view over data
`results.json` already carries, once A1 fills in the default's algorithm.

Labeled explicitly as leads for human triage, **not** a deployable config. This
is the phase A product.

### A4. Breadth-first cluster run

Refresh `smoke_hpcac.sh` first — it is dated 7-28, predates the screening/proof
rescope, runs everything `--no-validate`, and never exercises the paired
validator or a replay of the known-bad anchor.

Then run wide and shallow: many collectives, memory types, and team sizes at
`--n-reps 3`. Breadth is what discovery needs; depth belongs to phase B on the
shortlist.

### A5. Triage

Review the findings by hand and look for structure. "sra_knomial beats default
for allreduce 4K-64K at np>=16" is a UCC patch, not a config file entry.

---

## 5. Gate into phase B

- **Findings are structural** (same pattern across machines) -> upstream better
  defaults into UCC's C source. Phase B may never be needed.
- **Findings are cluster-specific** -> phase B is justified, and it can now be
  designed against real cost and coverage numbers.
- **Findings are marginal** (wins inside noise) -> that is the result. It
  arrives before more machinery is built on an unverified premise.

## 6. Frozen until that gate

Keep, but stop extending:

- `--proof-mode` and all its machinery (implemented, tested, off by default);
- the correctness-gate producer (`--correctness-*` intake);
- the fingerprint-matching launcher;
- `configure.ac` / `tools/Makefile.am` wiring;
- the merge to `master` (per `DECISIONS.md` §7).

## 7. Unconditional — do not revert

- **Inclusive range semantics** (`NO_REGRESSION_DESIGN.md` §2). This was the
  actual root cause of the only demonstrated bad range.
- The `ucc_info -A` lowercase fix and `cuda_managed` spelling
  (`NEXT-STEPS.md` §2.1, §2.2).

---

## Current state

- 8 commits on `topic/autotune`, working tree clean.
- 190 unit tests + 3 subtests passing, no binary required.
- **Phase A1–A3 implemented (local, 2026-09-09):**
  - A1 algorithm readback: `parse_score_map()` in `ucc_tune_runner.py` parses
    the team-create score-map log; `RunResult`/`SingleRunSample`/`SizeDecision`
    carry `selected_component`/`selected_alg`; `--readback-level
    {off,info,debug}` (default `info` → component only, `debug` → algorithm via
    the init-function symbol).  Forced-arm readback mismatches emit a warning.
  - A2 cost model: `compute_cost_model()` + `--dry-run` + `--launch-overhead`
    print `cells × sizes × (n_algs + 1) × n_reps` and estimated wall-clock.
  - A3 findings: `write_findings()` emits `findings.json` + `findings.md` —
    ranked leads (cell, message range, default algorithm, winning algorithm,
    speedup, n, CV), labeled for human triage, not deployment.
- `smoke_hpcac.sh` refreshed: `--dry-run`, `--readback-level debug`, a
  proof-mode run that exercises the paired validator, and a replay of the
  known-bad `allreduce:4k-16k` anchor.
- **Still never run on hardware since the 2026-09-08 screening/proof rescope.**
  A4 (breadth-first cluster run) and A5 (triage) remain, gated on cluster
  access.
