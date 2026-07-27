# UCC Offline Tuner — Resume Plan

**Branch:** `topic/autotune` · **UCC HEAD:** `281a0eb5` ("UTIL: fix ini max file line length (#1317)")
**Written:** 2026-07-25 · **Supersedes the "What is next" section of `CHECKPOINT.md`**

This document was produced by re-reading `CHECKPOINT.md` (dated 2026-06-24) and
cross-checking every claim in it against the actual tree. It records where the
project really stands, what drifted, what was fixed, and the ordered next steps.

`tools/tune/` is **untracked**. Nothing here has been committed; whether this
directory gets versioned at all is still an open decision (see §5).

---

## 1. Current state — one paragraph

The offline tuner (Part 1) is **code-complete and unit-tested, but has never been
run against a real UCC installation**, and the cross-check found **two defects
that would each have caused a first cluster run to fail** — one of them silently.
Both were narrow, evidence-backed, and mechanical, so both are now fixed with
regression tests. As of this document the tuner passes **157 unit tests** and
completes a full end-to-end driver run against stubbed `ucc_info`/`ucc_perftest`
binaries, emitting a syntactically valid `ucc_tuned.conf`. Everything else in
`CHECKPOINT.md` that could be checked statically — the knob table, the perftest
CLI contract, the config-file format, the fingerprint parsing — **verified
accurate**. The remaining work is genuinely blocked on cluster access, plus a
short list of design decisions the tuner cannot make for itself.

**Confidence in the checkpoint's self-assessment:** the *inventory* was
trustworthy (line counts and test counts matched to the digit). The *external
interface claims* were where it drifted — precisely the claims a unit test suite
cannot check, because the suite's fixtures were written from the same
assumptions as the code.

---

## 2. Checkpoint-vs-reality drift

### 2.1 Blocker, now fixed — `ucc_info -A` collective names are capitalised

`CHECKPOINT.md` listed "`ucc_info -A` format verification" as an open item, but
framed the risk as *"spacing, indentation can vary with build"*. The real defect
is neither: it is **capitalisation**, and it is deterministic, not build-dependent.

`tools/info/ucc_info.c:62` prints the collective line with
`ucc_coll_type_str()`, whose table (`src/utils/ucc_log.h:26`) returns
**capitalised** names — `Allreduce`, `Bcast`, `Alltoallv`, `Reduce_scatter`.
`parse_ucc_info_algs()` stored the captured name verbatim, while every consumer
(`main()` auto-discovery and `run_tuning()`'s
`alg_map.get(comp, {}).get(coll, [])`) looks it up with the **lowercase**
perftest/TUNE spelling.

Consequence, reproduced locally against a stub `ucc_info` emitting the exact real
format: with `--component` supplied, **every cell is skipped** with the
misleading message *"no algorithms found in ucc_info -A (component not built or
collective not supported)"*; without `--component`, the run aborts with *"No
components found with collectives ['allreduce']"*. Either way a first cluster run
produces an empty config and a plausible-sounding wrong explanation.

The 154 unit tests did not catch this because
`test_ucc_tune_space.py::_SAMPLE_OUTPUT` was hand-written with lowercase
collective names — the fixture encoded the same wrong assumption as the code.

**Fixed:** `parse_ucc_info_algs()` now `.lower()`s the collective key. Verified
safe: every one of the 16 names in `ucc_coll_type_str()` lowercases *exactly*
onto its `ucc_pt_op_map` spelling in `tools/perf/ucc_pt_config.cc`
(`Reduce_scatter` → `reduce_scatter`, etc.). `_SAMPLE_OUTPUT` was rewritten to
reproduce the real `ucc_info` layout byte-for-byte, and two regression tests were
added.

### 2.2 Blocker, now fixed — the `cuda-managed` note was backwards

`CHECKPOINT.md` carried this as an **"Important implementation note"**:

> UCC TUNE grammar uses `"cuda-managed"` (from `ucc_mc_base.c`), not
> `"cuda_managed"`.

This is exactly inverted, and the code implemented the inverted version.

The TUNE/score string is parsed by `ucc_coll_score_parse_str()`
(`src/coll_score/ucc_coll_score.c:643`) → `ucc_str_to_mtype_map()`
(`src/utils/ucc_string.c:191`) → `ucc_mem_type_from_str()`
(`src/utils/ucc_coll_utils.c:52`). That function's `STR_TYPE_CHECK` macro
(`ucc_coll_utils.c:12`) `strcasecmp()`s the token against the **stringified enum
suffix**, so the accepted spellings are `cuda_managed` (any case) and
`CudaManaged` — **underscore**. The hyphenated `"cuda-managed"` at
`src/components/mc/base/ucc_mc_base.c:23` is the *display* name returned by
`ucc_mem_type_str()` for log messages only.

A token the loop cannot match is not skipped — it returns
`UCC_ERR_INVALID_PARAM` and logs *"failed to parse token '…' in '…'"*, rejecting
the **entire** `UCC_TL_*_TUNE` value. So every `--mem-type cuda-mng` config the
tuner generated would have been thrown away wholesale at team-create time.

Independently corroborated in-tree:
- `test/gtest/utils/test_parser.cc:56` — UCC's own score-string test uses
  `0-4k:cuda_managed:6`.
- `src/components/tl/nccl/allgatherv/allgatherv.h:20` — UCC's own built-in tune
  string uses `allgatherv:cuda_managed:0-inf:@0`.

**Fixed:** `_PERFTEST_TO_TUNE_MEM["cuda-mng"]` is now `"cuda_managed"`, with the
provenance written into the source comment; the two tests that asserted the
hyphenated form were corrected and a negative assertion added.

### 2.3 The "plan" and "plan addendum" the checkpoint cites do not exist

`CHECKPOINT.md` justifies most of its design decisions by reference to "the
plan", "the plan addendum", "the plan's sequencing recommendation", and a
"Suggested MVP constraint". **No such document exists** anywhere in
`ucc-tuning/`, or elsewhere under `~/overnight/` (searched). It appears to have
been a conversational artifact that was never written to disk.

This is not a code problem, but it does mean several stated constraints are
currently **unverifiable and unattributed** — notably the Part 1 / Part 2
sequencing rule and the MVP scope boundary. Reconstructing or re-deciding them is
a design task (§4.1).

### 2.4 Minor drift

| Claim in `CHECKPOINT.md` | Reality |
|---|---|
| "Six Python files" | **Five** modules (+ four test files = nine `.py` files). Miscount only; the file *inventory table* is correct. |
| Line counts, per-file test counts, total 3 963 lines / 154 tests | **Exact.** Every number matched before the fixes above. |
| "`-f` cannot override the ×2 factor" | **Confirmed** — `tools/perf/ucc_pt_benchmark.cc:24,54` hard-code `2` into `ucc_pt_generator_exponential`; `cfg.mult_factor` (set by `-f` at `ucc_pt_config.cc:410`) is never passed. Single-size invocation is the correct workaround. This is arguably an upstream UCC bug worth a separate report. |

### 2.5 Claims that were checked and are accurate

Recording these so the next session does not re-audit them:

- **All 21 knob env vars in `_KNOBS` exist**, verified one by one against
  `src/components/tl/ucp/tl_ucp.c` and `src/components/tl/cuda/tl_cuda.c`.
- **Perftest CLI contract is right.** `-c -b -e -m -d -n -w -o -p` all exist
  (`ucc_pt_config.cc:113`); `_VALID_MEM_TYPES` matches `ucc_pt_memtype_map`
  exactly (including `cuda-mng`); `_VALID_DATATYPES` matches
  `ucc_pt_datatype_map`; the `_DATA_LINE_RE` row format matches
  `ucc_pt_benchmark::print_time` including the `N/A` case for `has_range()==0`.
- **Component keys are lowercase.** `ucc_info` prints `tl`/`cl` literally and
  `iface->super.name`, which `UCC_BASE_IFACE_DECLARE`
  (`src/components/base/ucc_base_iface.h:247`) stringifies from the lowercase
  macro arg. `"tl/ucp"` is correct as written.
- **`ucc_info -v` parsing is right.** `build_info.c:13` prints
  `# UCC version=%s revision %s`; the `UCC version=(\S+)` regex matches.
- **Config-file format is right.** `ucc_parse_file_config()` requires full
  `UCC_`-prefixed keys (`ucc_parser.c:378`) — which the emitter produces. The
  bundled inih accepts `#` as a *start-of-line* comment prefix but **not** as an
  inline one (`src/utils/ini.h:81,91`), so the `#`-joined TUNE token list is safe
  in a value. Env vars take precedence over the file (`ucc_parser.c:372`).
- **`[8-inf]` team-size syntax is valid** (`str_to_tsizes`,
  `ucc_coll_score.c:560`), and `inf` is accepted as the upper bound.
- **Token order within a TUNE string does not matter** — `ucc_coll_score_parse_str`
  field-matches each `:`-separated token against every field type in turn.
- **Line-length headroom exists.** `UCC_INI_MAX_LINE` is 8192 at this HEAD
  (raised from 500 by `281a0eb5` itself). Long TUNE strings are fine here but
  would silently truncate on any UCC older than that commit — worth a note in the
  generated config.

---

## 3. Ordered next steps

### 3.1 Now unblocked / mechanical (done — see §5)

- [x] Fix the `ucc_info -A` capitalisation blocker + regression tests.
- [x] Fix `cuda-managed` → `cuda_managed` + regression tests.
- [x] Add `tools/tune/.gitignore` so the stale `__pycache__/` and
      `.pytest_cache/` in this directory cannot be swept into a future commit.

### 3.2 First cluster run — the real gate (blocked, needs cluster)

Do these in order; each is cheap and each de-risks the next.

1. **Smoke the enumerator alone.** On the target cluster run
   `python3 ucc_tune_space.py` (it has a `_cli_main`) against the installed
   `ucc_info` and confirm the parsed component/collective map is non-empty and
   lowercase. This is now the single highest-value five-minute check: it is
   exactly what §2.1 broke, and a stub can only prove so much.
2. **Smoke the runner alone.** `python3 ucc_tune_runner.py` for one size, one
   collective, and confirm a timing row parses. Confirms `-p` persistent mode and
   the launcher prefix.
3. **Hand-verify one TUNE string end to end.** Export a single-token
   `UCC_TL_UCP_TUNE` by hand with `UCC_LOG_LEVEL=info` and confirm no
   *"failed to parse token"* appears and the intended algorithm is selected.
   Do this for **`host` and `cuda_managed` separately** — §2.2 means the managed
   path has never been exercised.
4. **Minimal full run:** one collective × one mem_type × one team_size, small
   grid, `--no-validate`. Then load the emitted `ucc_tuned.conf` via
   `UCC_CONFIG_FILE` and confirm clean parse.
5. **Then** widen to the multi-collective / multi-team-size invocation.

### 3.3 Follow-ups that need no design decision (safe to implement any time)

- **Fail loudly on an empty algorithm map.** Right now a total enumeration
  failure degrades into per-cell "skipped" warnings that read like a legitimate
  build configuration. It should be a hard error naming the `ucc_info` path and
  echoing the first lines of its output. (This is what made §2.1 silent.)
- **Deduplicate the `ucc_info -A` invocation.** `main()` calls
  `run_ucc_info_algs()` and then `run_tuning()` calls it again — two subprocess
  launches per run, under the MPI launcher. Threading the map through is a small
  signature change; deferred here only because it touches `run_tuning`'s public
  signature and `TestRunTuning`.
- **Stamp a minimum-UCC-version note into the generated config** referencing the
  8192-byte line limit (§2.5).

### 3.4 Medium term (unchanged from `CHECKPOINT.md`, still valid)

- Fingerprint-matching launcher script.
- Correctness-validation harness (MPI/gtest run under `ucc_tuned_env.sh`).
- Regression-baseline mode (diff `results.json` across UCC bumps).

---

## 4. Blocked / needs a decision — explicitly NOT done

### 4.1 Design decisions

1. **Team-size range semantics.** `TuneRange.tune_token()` hard-codes
   `[{team_size}-inf]`: a measurement taken at 8 ranks is asserted for *every*
   team of 8 or more. With `--team-sizes 8,64,512` this emits three overlapping
   `[8-inf]`, `[64-inf]`, `[512-inf]` qualifiers for the same range and the
   resolution order is not reasoned about anywhere. Options: bracket each
   measurement (`[8-63]`, `[64-511]`, `[512-inf]`), emit exact sizes (`[8]`), or
   keep open-ended and document last-wins. **This is a correctness-of-intent
   question, not a bug — it needs a call.**
2. **Msg-range boundary convention.** Emitted ranges abut on a shared endpoint
   (`0-32` and `32-inf` both include 32). UCC's own test fixture
   (`test_parser.cc:56`) uses the same `0-4K` / `4k-inf` idiom, so this is
   probably the house style — but the inclusive/exclusive semantics of
   `coll_score_add_range` were not traced, and it should be confirmed on a live
   run before anyone trusts a boundary size.
3. **Knob conflict resolution.** "Largest span wins" is implemented as described,
   but knob env vars are global across *cells* too — a radix chosen for
   `team_size=8` silently applies at `team_size=512`. Either scope the output per
   cell (one config directory per team size) or investigate `UINT_RANGED`.
   Requires a call on the output format.
4. **`UINT_RANGED` knob syntax.** *Resolved (2026-07-27, see `DECISIONS.md`):*
   the syntax **is** documented in-source at `src/utils/ucc_parser.h:303` —
   `[<munit>-<munit>:[mtype]:value,...]default_value`. It ranges over
   **msg-size + mem-type only, not team-size**, so it does **not** eliminate #3.
   No further investigation needed.
5. **`*v` / rooted / asymmetric collectives.** Unchanged from `CHECKPOINT.md`
   item 2 — the size-0 lookup collapse means message-range tuning may be
   meaningless for `allgatherv` / `alltoallv` / rooted `reduce`. Needs a policy:
   special-case, or refuse to tune them.
6. **Mid-sweep checkpointing / resume.** Needs an on-disk schema and a
   cell-identity/staleness rule (does a resumed sweep trust results from a
   different fingerprint?). Not mechanical.
7. **Whether `tools/tune/` is versioned at all**, and if so whether it is wired
   into `configure.ac` / a `tools/Makefile.am` for installation. It is currently
   in neither. **User's call.**
8. **Reconstruct or discard "the plan"** (§2.3) — in particular the Part 1 /
   Part 2 sequencing constraint that the checkpoint treats as binding.

### 4.2 Hard blockers

- **No cluster access in this session** (local, no-cluster task by instruction).
  Everything in §3.2 is gated on that.
- **No local UCC build possible** — macOS host, no UCX. `ucc_info` /
  `ucc_perftest` cannot be built or run here, so all verification above is
  source-reading plus stubbed end-to-end runs.
- **Part 2 (online tuner) remains correctly out of scope.** Its stated blocker —
  rank divergence, no runtime score-map hook — is unchanged and still requires
  the offline tuner to be producing real configs first.

---

## 5. What was actually changed in this session

All changes are in the untracked `tools/tune/`. **Nothing was committed.**

| File | Change |
|---|---|
| `ucc_tune_space.py` | `parse_ucc_info_algs()` lowercases the collective key; provenance comment on `_COLL_RE`. |
| `test_ucc_tune_space.py` | `_SAMPLE_OUTPUT` rewritten to the real capitalised `ucc_info -A` layout; +2 regression tests. |
| `ucc_tune_sweep.py` | `_PERFTEST_TO_TUNE_MEM["cuda-mng"]` → `"cuda_managed"`; provenance comment citing the parser chain. |
| `test_ucc_tune_sweep.py` | Two assertions corrected to the underscore form; +1 negative test. |
| `.gitignore` | New — ignores `__pycache__/`, `.pytest_cache/`, and output dirs. |
| `CHECKPOINT.md` | Updated to reflect the above. |
| `NEXT-STEPS.md` | This file. |

**Verification:**

```
$ python3 -m pytest -q          # tools/tune, Python 3.14.4 / pytest 9.0.3
157 passed in 0.16s             # was 154 passed before the fixes
```

Plus a full stubbed end-to-end driver run (`--collective allreduce
--mem-type host,cuda-mng --team-sizes 8 --no-validate`) against fake
`ucc_info`/`ucc_perftest` scripts reproducing the real output formats. Before the
fixes it aborted with *"No components found with collectives ['allreduce']"*;
after, it completes and emits:

```
UCC_TL_UCP_TUNE=allreduce:0-32:host:[8-inf]:inf:@knomial#allreduce:32-inf:host:[8-inf]:inf:@sra_knomial#allreduce:0-32:cuda_managed:[8-inf]:inf:@knomial#allreduce:32-inf:cuda_managed:[8-inf]:inf:@sra_knomial
```

**Deliberately not touched:** no UCC C source, no tracked file anywhere in the
repo (`git status` still reports exactly `?? tools/tune/`).
