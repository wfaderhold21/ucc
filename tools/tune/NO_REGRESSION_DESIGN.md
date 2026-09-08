# UCC offline tuner: no-regression emission design

Status: implementation-ready design; no tuner changes are included here.

> **2026-09-08:** this machinery is now **opt-in** behind `--proof-mode`. The
> default sweep is fast screening-only; the paired confirmation, boundary
> refinement, Holm family, and three-arm knob attribution in this document run
> only under `--proof-mode`.

## 1. Decision and evidence basis

The current tuner remains useful for discovery, but its output must be treated
as a set of candidates until an independent confirmation and final-config gate
pass.  This follows directly from the measured np=8 Gaia result: **4/6 emitted
algorithm ranges were beneficial (66.7% hit rate), one was wrong, and one was
indistinguishable from UCC default**.  The deployment-blocking miss was
`allreduce:4k-16k:@knomial`: 4 KiB was a real win, while 16,380 B was 12.6%
slower (95% CI +4.4% to +20.7%).

Static analysis identified the complete escape path:

1. `sweep_cell()` found a win at 4 KiB and no win at the next sparse anchor,
   16 KiB.
2. `_group_to_range()` used the next anchor as an exclusive Python endpoint,
   but `TuneRange.tune_token()` emitted the number into UCC's inclusive
   message-range grammar.
3. `_representative_sizes()` also treated the endpoint as exclusive, so
   `validate()` sampled only the winning end.
4. `_sweep_knobs_at_size()` selected radix at one point, and
   `_collect_knob_overrides()` made it global.  The failing A/B arm therefore
   changed both algorithm and radix, preventing attribution.

The changes below map to those demonstrated failures.  Paired confirmation and
incomplete-evidence rules additionally cover identified risks in the current
unpaired median/5% rule; they are not claimed as causes of the 4 KiB anchor.
The scope of every decision is the exact component, collective, memory type,
team size, datatype, reduction operation, and measured message domain in its
`CellKey`.  There is no extrapolation to np=32, `cl/hier`, device memory, other
datatypes, or sizes above the requested maximum.

## 2. Conservative inclusive range semantics

`TuneRange.start_bytes` and `TuneRange.end_bytes` become explicitly inclusive.
`end_bytes=None` is forbidden unless evidence includes the configured maximum
domain and the user explicitly requests an open tail; routine mode never emits
an `inf` message or team tail from a finite last anchor.  Team bands follow the
same rule: an observation at team size 8 describes `[8-8]`, not `[8-inf]`.
Unmeasured team sizes remain default unless separately confirmed.

Each measured byte has one of these states:

- `WIN(candidate)`: independent paired confirmation proves the candidate is at
  least the configured material margin faster than default.
- `DEFAULT`: the candidate loses, ties, is statistically indistinguishable,
  has incomplete evidence, or belongs to an unsupported regime.
- `REGRESSION`: paired evidence supports a slowdown.  This is also `DEFAULT`
  for emission and is retained as a stronger diagnostic state.

Only contiguous evidence for the same `WIN(candidate)` policy can form a
range.  A finite range begins at its first confirmed winning byte and ends at
its last confirmed winning byte.  A `DEFAULT`/`REGRESSION` anchor is never an
endpoint of the preceding winning range.  In particular, the old fixture
`4096=WIN(knomial), 16384=DEFAULT` can produce at most `[4096,last_confirmed_win]`;
it can never produce `[4096,16384]`.

Sparse space between unlike anchors is a default gap until boundary refinement
confirms points in it.  Sparse space between like winning anchors may be
covered only after the final-config gate tests the interval and the gap between
confirmed probes is no larger than the configured resolution.  The report
must still say that the guarantee is sampled at that resolution; it must not
claim every byte was measured.

Ranges are serialized without rounding an endpoint upward.  `_fmt_bytes()` may
use `k/M/G` only for exact multiples, as it does today.  All membership tests
use `start <= size <= end`.  `just_outside` means `start-1` and `end+1`, after
datatype/count alignment and domain clipping; alignment that maps two probes
to the same actual byte is deduplicated and recorded.

## 3. Boundary policy

The default policy is **bounded fixed-resolution bisection of bracketed
transitions**, with omission when the bracket remains unresolved.  It is not an
adaptive stop-on-a-good-result policy:

1. Start with adjacent coarse anchors whose states or candidate policies
   differ.
2. Probe the aligned integer midpoint with the same paired confirmation rule.
3. Continue on every newly exposed unlike-policy bracket until its width is at
   most `boundary_resolution_bytes`, or until
   `max_boundary_probes_per_transition` is exhausted.
4. End a winning range at the greatest confirmed winning byte on its side of
   the bracket.  Leave the remainder through the first default byte uncovered.
5. If probes show more than one transition, split and refine each transition
   within the same cap.  If the cap cannot resolve all transitions, omit the
   affected interval/range rather than assuming monotonicity.

Routine defaults are 1 KiB resolution, at most four new probes per coarse
transition, and at most 40 paired-confirmation points for the entire run.
Exhausting either budget changes unprocessed or unresolved points to
`DEFAULT`; it never relaxes a test.  Proof mode uses 256 B resolution and up to
12 probes.  A user may configure stricter resolution, but not a policy that
assigns a losing anchor to the previous range.

Alternatives were rejected as defaults:

- Emitting only singleton winning anchors has the lowest regression risk and
  cost, but is too narrow to preserve established broad allgather and
  reduce-scatter wins.  It remains the fallback when a transition cannot be
  resolved.
- Outcome-adaptive refinement can spend fewer probes, but optional stopping
  complicates inference and creates variable, potentially unbounded cost.
- Fixed bisection costs `ceil(log2(gap/resolution))` probes per simple
  transition, has a hard cap, and never assigns the unresolved part to the
  winner.  It is therefore the chosen balance.

## 4. Paired confirmation rule

### 4.1 Collection

The existing algorithm sweep is screening only.  It may nominate the
lowest-median forced algorithm, but screening samples cannot confirm that
nominee.  Confirmation collects fresh default/candidate pairs close in time.
Pair order uses a recorded seed and balanced randomized `AB`/`BA` blocks; the
two orders differ in count by at most one.  `A` is untuned UCC default and `B`
is the candidate.  No Tukey deletion is applied to paired evidence.  A failed
arm invalidates its pair, remains in the artifact, and is not imputed.

For complete pair `i`, store `d_i = log(B_i/A_i)`.  Negative values are wins.
Use an exact paired randomization (sign-flip) interval for the mean log ratio
and transform it back to a geometric-mean ratio.  The implementation must
enumerate all signs for small `n` and use a recorded-seed Monte Carlo
permutation only above the exact limit.  Simultaneous one-sided 95% intervals
use Holm correction across all confirmation decisions in one cell.  The
artifact records raw arms, order, seed, adjusted alpha, interval, ratio, and
decision so replay is deterministic.

The family is frozen per `(component, collective, memory type, team size,
datatype, operation)` cell before any emitted decision is mutated. It contains
every collected algorithm anchor, every collected boundary-refinement point,
and, for every collected knob candidate, all three attribution hypotheses:
`A0/D`, `A1/A0`, and `A1/D`. A knob is emitted only when its point's algorithm
hypothesis and all three candidate-specific hypotheses survive the single
cell-wide Holm pass, and exactly one candidate survives. Stable hypothesis IDs
break equal-p-value ties, so dictionary order cannot change thresholds. Every
collected member consumes one proof-budget point and each knob member's raw
p-value, adjusted threshold, decision, and reason are serialized. Incomplete,
budget-exhausted, noisy, order-confounded, or conflicting evidence omits the
knob.

The minimum is 10 complete pairs.  Collection may make 14 attempts to obtain
them.  If either arm's CV exceeds 10%, or a predeclared robust spread threshold
for the paired log ratios is exceeded, collect one second batch and analyze all
20 complete pairs without examining an interim effect verdict.  Twenty
complete pairs and 28 attempts are hard maxima.  Persistent high variance is
`DEFAULT`.

The candidate is `WIN` only when the simultaneous upper confidence bound for
`B/A` is below `1 - min_speedup` (routine default 0.95).  A lower bound above
1.0 is `REGRESSION`.  Everything else, including a numerical tie, a CI crossing
either threshold, or a nominal median win, is `DEFAULT`.  Thus the old
unpaired 5% median comparison is never sufficient for emission.

Ordering is checked by reporting AB and BA strata and an order-by-treatment
permutation test.  Both strata must have point estimates on the winning side,
and a significant interaction or a missing order stratum yields `DEFAULT`.
This prevents the fixed A-then-B drift seen in job 61761 from becoming proof.

### 4.2 Failure and correctness rules

The fallback is always no override:

- failed or missing default; fewer than 10 complete pairs; excessive failures;
- failed nominee, any partial advertised-algorithm sweep, or high variance
  after the bounded second batch;
- any candidate timeout, crash, parse failure, wrong byte count, non-finite
  timing, or correctness failure;
- a tie, indistinguishable result, supported regression, order effect, or
  exhausted proof budget;
- an unrequested/unmeasured component, memory type, team size, datatype,
  operation, or message domain.

Partial algorithm failure makes the anchor incomplete even if another
algorithm looks fast; it can still be reported as a lead but cannot be emitted.
Correctness is monotonic in strictness: a config is eligible only if the tuned
mode introduces zero failures relative to the same default-mode test set.  A
default correctness failure makes the regime inconclusive rather than allowing
the tuned arm to pass.

## 5. Algorithm and knob attribution

For every secondary knob, collect three arms with the same paired machinery:

- `D`: UCC default algorithm and default knobs;
- `A0`: forced candidate algorithm with its default/`auto` knob;
- `A1`: forced candidate algorithm with the candidate knob value.

First compare `A0/D`; the algorithm may be emitted alone only if it satisfies
the normal 5% superiority rule.  Then compare `A1/A0`; a knob is retained only
if it independently satisfies that rule.  Finally compare the joint `A1/D`
arm and require the normal rule again.  If knob confirmation fails but `A0/D`
passes, emit the algorithm without the knob.  If `A0/D` fails, a favorable
interaction in `A1/D` is recorded for research but is not emitted in routine
mode because attribution is unresolved.

`Knob` gains scope metadata derived from the UCC config type.  A
`UINT_RANGED` knob is emitted using UCC's message/memory grammar,
`[start-end:mem:value,...]default`, with exactly the same inclusive message
range as its algorithm evidence.  It must not be collapsed by “largest span
wins.”  Because this grammar cannot scope team size, a knob is emitted only in
a single-team artifact or when its value and ranges are identical across every
team represented by that artifact.  Otherwise create per-team config artifacts
or omit the knob.  A scalar knob that differs across message ranges, memory
types, or teams is likewise omitted or separated; it never leaks across cells.

## 6. Final emitted-config gate

Build a provisional config, then compare that exact config—not reconstructed
algorithm-only settings—with untuned UCC default using fresh paired samples.
For each inclusive range, `_validation_probe_sizes()` returns, after alignment
and deduplication:

- inclusive start and end;
- one byte/count just inside each end when the range permits;
- one byte/count just outside each end within the requested domain;
- midpoint plus 25% and 75% interior points for ranges wider than twice the
  configured resolution;
- every original or refined anchor in the range.

The outside probes verify that default behavior resumes; they compare the full
provisional config to default and must be indistinguishable because no override
should match there.  Runtime token round-trip tests additionally assert that
the selected algorithm is absent outside the inclusive range.

At an inside point, a lower simultaneous CI bound for tuned/default above 1.0
is an independently supported slowdown and rejects the range immediately.
Routine emission is stricter: the upper bound must be at or below 1.0.  A CI
that crosses 1.0 is within noise but unresolved, so the range is trimmed to
confirmed neighboring probes or removed.  This design does not use the
acceptance allowance to emit an indistinguishable override; default is safer.
After trimming, regenerate the config and rerun all affected boundary probes.
At most two trim/regenerate rounds are allowed; otherwise omit the range.

A config passes only when:

1. every inside probe is statistically no worse by the rule above;
2. every outside probe confirms no policy leakage;
3. no tested range point has a supported regression;
4. default and tuned correctness gates complete, and tuned has no new failure;
5. every emitted token and ranged knob round-trips through UCC parsing/lookup;
6. the report states the exact `CellKey`, requested domain, resolution, and all
   unmeasured regimes left on default.

## 7. Data model and code changes

Add these immutable records in `ucc_tune_sweep.py` (or a new
`ucc_tune_stats.py` imported by it):

- `CellKey(component, collective, mem_type, team_size, datatype, op)`;
- `ArmSample(pair_id, order, arm, latency_us, ok, failure)`;
- `PairedEvidence(samples, complete_pairs, ratio, ci_low, ci_high,
  adjusted_alpha, order_p, decision, reason)`;
- `PointDecision(size_bytes, policy, candidate, algorithm_evidence,
  knob_evidence, correctness, source)` where policy is `WIN`, `DEFAULT`, or
  `REGRESSION`;
- `InclusiveTuneRange(start_bytes, end_bytes, cell_key, candidate,
  evidence_points, resolution_bytes, knob_ranges)`;
- `ProofBudget(max_points, max_pairs, used_points, used_pairs)`.

Exact implementation targets:

- `ucc_tune_runner.py`: add `measure_paired()` and retain all raw ordered arm
  samples; keep `measure()` only for discovery.
- new `ucc_tune_stats.py`: `paired_log_ratio_ci()`, Holm adjustment,
  `classify_evidence()`, and deterministic replay serialization.
- `ucc_tune_sweep.py`: make `sweep_cell()` screen then confirm; replace
  `SizeDecision`, `_group_to_range()`, and `coalesce_ranges()` with inclusive
  point/range construction; add `refine_boundaries()`; replace
  `_sweep_knobs_at_size()` with three-arm `confirm_knob()`.
- `ucc_tune_space.py`: add knob config-type and scope metadata.
- `ucc_offline_tune.py`: replace `_collect_knob_overrides()` with ranged,
  cell-safe emission; replace `_representative_sizes()` with
  `_validation_probe_sizes()`; make `validate()` paired, simultaneous, and able
  to reject/trim/regenerate; serialize evidence and proof budget in
  `_results_to_json()` and the human summary.
- UCC-side parser/lookup is not changed.  A small test helper should exercise
  the existing inclusive lookup semantics rather than duplicating them in
  Python alone.

CLI additions are `--min-speedup` (0.05), `--min-pairs` (10), `--max-pairs`
(20), `--boundary-resolution-bytes` (1024 routine),
`--max-boundary-probes` (4 routine), `--max-confirmation-points` (40 routine),
and `--proof-mode` (256 B/12 probes with an explicitly larger point budget).
No flag may select the legacy unsafe emission rule.

## 8. Tests and bounded proof plan

### Deterministic local fixtures

Extend `test_ucc_tune_runner.py`, `test_ucc_tune_sweep.py`,
`test_ucc_offline_tune.py`, and `test_ucc_tune_space.py`; add
`test_ucc_tune_stats.py`.  Required fixtures cover:

- exact paired CI/Holm decisions for a 5% win, tie, supported regression,
  high variance, AB/BA drift, missing default, failed candidate, nine complete
  pairs, partial algorithm failure, and correctness failure;
- `4096=WIN(knomial), 16384=DEFAULT` never includes 16384 or an unresolved gap;
- inclusive lookups at 4096, 4100, 16380, 16384, and 16388;
- bisection caps, multiple observed transitions, alignment, singleton fallback,
  no finite-domain `inf` tail, and unmeasured team sizes staying default;
- start/end, just-inside/outside, quartile, midpoint, and anchor validation;
- three-arm attribution, distinct ranged radix values, no cross-range/team/memory
  leakage, and scalar-knob omission;
- range rejection/trim/regeneration and zero emitted overrides for identical
  alltoall arms.

### Retained-sample replay

Import the raw 61761 and 61763 samples without editing them.  Replay must:

- label 61761's fixed A-then-B result order-confounded and ineligible;
- reproduce the 61763 allreduce 4 KiB win and 16,380 B regression, causing the
  old `4k-16k` fixture to fail the new gate;
- retain the supported allgather and reduce-scatter classifications;
- produce zero false-positive overrides at all six pure-alltoall controls.

The seven-pair 61763 data are valid regression fixtures but do not satisfy the
new ten-pair emission minimum.  Replay therefore proves classification and
pre-fix failure; it cannot by itself authorize a new config.

### Minimal Gaia A/B

Only after separate authorization, run ten balanced pairs for the exact
provisional config and default.  The bounded set contains the allreduce 4 KiB
win, the refined upper bracket including 16,380/16,384 and one outside point;
allgather transition triplets around 64 KiB, 256 KiB, and 1 MiB;
reduce-scatter start/interior/end; and the six alltoall anchors as negative
controls.  Run the same scoped default/tuned correctness tests used by job
61763.  Acceptance requires zero new correctness failures, zero supported
slowdowns, and zero alltoall false positives.  Supported allgather and
reduce-scatter ranges remain; any point that cannot meet ten pairs or the
simultaneous gate falls back to default.

## 9. Cost and unresolved evidence

Every confirmed point costs 20 perftest arm invocations at the ten-pair
minimum, versus five independent invocations per arm in the run-of-record
sweep.  A simple boundary with the routine four-probe cap costs at most 40
pairs, or 80 arm invocations, at the ten-pair setting.  The routine global cap
of 40 confirmation points is 400 pairs/800 arm invocations beyond discovery at
the minimum.  If every point consumes its high-variance second batch, the hard
maximum is 800 pairs/1,600 arm invocations.  For the run-of-record-sized sweep
this is expected to make wall-clock measurement roughly two to three times
discovery alone, depending on algorithm count and transition count.  Proof
mode may be roughly three to four times discovery, but remains bounded by its
explicit point, probe, pair, and trim-round caps.  Parallel cells may reduce
wall time but do not change the statistical or launch budget.

The exact allreduce crossover, algorithm/radix attribution, np=32, `cl/hier`,
device memory, messages above 1 MiB, and the job 61711 hang remain unresolved.
They do not block this design: each is represented as unsupported/default.
Evidence needed to expand coverage is a ten-pair balanced A/B plus correctness
for that exact `CellKey` and message domain; attribution additionally requires
the three arms in section 5.  No new cluster job or tuner implementation is
authorized by this document.
