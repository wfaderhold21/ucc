# UCC Offline Tuner — §4 Decision Record

**Branch:** `topic/autotune` · **Date:** 2026-07-27 · **Author:** design pass (Opus)
**Resolves:** `NEXT-STEPS.md` §4 ("Blocked / needs a decision") — 8 items in §4.1
plus disposition of the §4.2 hard blockers.

Each §4.1 item gets a one-line verdict — **do / defer / drop** — with rationale.
"do" items that are code changes are handed to implementation (opencode). Two
items were **answered by reading the source during this pass** and are recorded
as resolved.

---

## §4.1 Design decisions

| # | Item | Verdict | Decision |
|---|------|---------|----------|
| 1 | Team-size range semantics (`[ts-inf]` overlap) | **DO** | Emit **non-overlapping bracketed ranges** derived from sorted `--team-sizes` (`[8-63]`, `[64-511]`, `[512-inf]`), largest bound `inf`. |
| 2 | Msg-range boundary convention (shared endpoint) | **DEFER** | Keep the abutting `0-32`/`32-inf` idiom (matches UCC `test_parser.cc:56`); add a live-run check to confirm which side wins before trusting a boundary size. No code change now. |
| 3 | Knob conflict across cells (global env knobs) | **DO (guard)** | Knobs cannot be team-size-scoped in one file (see #4). Gate knob emission to **single-team-size runs**; in multi-size runs, **warn + omit** conflicting knobs. Per-cell config dirs deferred. |
| 4 | `UINT_RANGED` knob syntax | **DROP (resolved)** | **Answered:** syntax is documented at `src/utils/ucc_parser.h:303` — `[<munit>-<munit>:[mtype]:value,...]default_value`. It ranges over **msg-size + mem-type only, not team-size**, so it does **not** eliminate #3. No further investigation needed. |
| 5 | `*v` / rooted / asymmetric collectives | **DO (guard) / DEFER (policy)** | Size-0 lookup collapse makes msg-range tuning likely meaningless for `allgatherv`/`alltoallv`/rooted `reduce`. Default to **skip + warn**, with an opt-in flag to force. Full special-case policy deferred. |
| 6 | Mid-sweep checkpointing / resume | **DEFER** | Not needed for MVP or the first cluster runs; needs an on-disk schema + fingerprint-staleness rule. Revisit once §3.2 shows real sweep durations. |
| 7 | Is `tools/tune/` versioned / wired into build? | **DECISION** | **Commit to `topic/autotune` now; do NOT wire into `configure.ac`/`Makefile.am` and do NOT merge to `master` until §3.2 first-cluster-run passes.** (See "Versioning question" below.) |
| 8 | Reconstruct or discard "the plan" (§2.3) | **DROP** | The plan does not exist on disk. This doc + `NEXT-STEPS.md` §4 are the authoritative decision record going forward. The only binding constraint worth keeping — Part 1 before Part 2 — is independently sound and already recorded in §4.2. |

## §4.2 Hard blockers — disposition

- **No cluster access / no local UCC build** — cannot be cleared by this
  design task. All of §3.2 is gated on real hardware; route it to the remote
  verification pass (**task 19**, C1 on-cluster smoke sequence). No decision to
  make — it is a scheduling dependency, not an open question.
- **Part 2 (online tuner) out of scope** — confirmed. Stays out until the
  offline tuner emits real, cluster-validated configs.

---

## Versioning question (task-5 step 3) — resolved

**Decision: commit `tools/tune/` to the `topic/autotune` branch now; keep it out
of the build system and out of `master` until the first real cluster run
(§3.2) passes.**

Rationale:
- It is **substantial, real work** — ~3,963 lines, 157 passing unit tests, a
  code-complete offline tuner. Leaving it untracked (`?? tools/tune/`) risks
  losing it and blocks review/collaboration. Version control is warranted.
- But it has **never run against real UCC**, and this very branch's static
  review already found two would-be-fatal defects (§2.1, §2.2). Merging to
  `master` or wiring it into `configure.ac`/`tools/Makefile.am` now would bless
  unvalidated behavior and ship an installable tool that has never executed.
- The clean middle path: **track it on the topic branch** (preserves the work,
  enables review), **gate the `master` merge and build-system wiring on §3.2**
  (the enumerator/runner/end-to-end smoke on real hardware). The existing
  `.gitignore` already keeps `__pycache__/`/output dirs out of the commit.

Merge-to-master gate: §3.2 steps 1–5 green on a real cluster (task 19) **and**
items 1, 3, 5 below implemented.

---

## Sequenced "do" list (handed to opencode)

Implementation items, in dependency order. All are local, no-cluster edits.

1. **Item 1 — non-overlapping team-size ranges.** In `ucc_tune_sweep.py`,
   `TuneRange.tune_token()` (`:134`, hard-codes `:[{team_size}-inf]`): compute
   per-measurement brackets from the sorted `--team-sizes` list so each token
   covers only `[this_size .. next_size-1]`, largest → `inf`. Highest value —
   unblocks trustworthy multi-team-size configs. Update `test_ucc_tune_sweep.py`.
2. **Item 3 (guard) — knob scoping.** Emit knobs only for single-team-size
   runs; in multi-size runs, warn and omit knobs that differ across cells
   (TUNE strings are unaffected — they carry the bracketed team-size from #1).
3. **Item 5 (guard) — skip asymmetric collectives.** Default-skip
   `allgatherv`/`alltoallv`/rooted `reduce` with a warning; add `--force-vcoll`
   (or equiv) opt-in.
4. **§3.3 decision-free follow-ups** (already unblocked, bundle here): fail
   loudly on an empty algorithm map; dedupe the double `ucc_info -A` launch;
   stamp the min-UCC-version (8192-byte line) note into the generated config.

**Defer:** items 2 (verify on cluster), 6.
**Drop:** item 4 (answered — UINT_RANGED is documented, doesn't help #3),
item 8 (the plan does not exist).
**Decision-only, no code:** item 7 (versioning, above).

Cluster-gated verification (§3.2) is **task 19**, not part of this design pass.
