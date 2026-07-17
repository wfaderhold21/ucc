# TL/UCP Allreduce Performance — 512kB to 4MB

Optimization plans for `allreduce` in the 512kB–4MB message-size range in
`src/components/tl/ucp`.

## Shared background (applies to all plans)

- Selection: `allreduce/allreduce.h:24` routes `4k-inf` to `sra_knomial` (`@1`),
  so 512kB–4MB is always SRA (Rabenseifner: knomial reduce-scatter -> knomial
  allgather).
- SRA is wired as a **pipelined schedule** in `allreduce/allreduce_sra_knomial.c`.
- The pipeline plumbing (frag fan-out, `max_frag_count`, per-frag scratch
  sizing) is **already fully implemented and used by the CUDA in-place path**.
  The frag callbacks are memtype-agnostic
  (`allreduce_sra_knomial.c:57` `frag_setup`, `:86` `frag_init`). The host gap is
  a **heuristic choice, not a correctness limit**.

## Plans

| # | Plan | Priority | File |
|---|------|----------|------|
| 1 | Enable host-side SRA pipelining | do first | [plan-1-host-sra-pipelining.md](plan-1-host-sra-pipelining.md) |
| 2 | Radix + fragment/algorithm sweep (tune defaults) | high | [plan-2-radix-frag-sweep.md](plan-2-radix-frag-sweep.md) |
| 3 | Fine-grained reduce overlap inside knomial reduce-scatter | conditional | [plan-3-fine-grained-reduce-overlap.md](plan-3-fine-grained-reduce-overlap.md) |
| 4 | Trim redundant memory passes (in-place / proxy / ring) | after 1/2 | [plan-4-trim-memory-passes.md](plan-4-trim-memory-passes.md) |
| 5 | Ring as a selectable default in a sub-window | after 2 | [plan-5-ring-selection-window.md](plan-5-ring-selection-window.md) |

## Suggested order

**#1 -> #2** (which finalizes #1's constants and evaluates #5) -> then #3/#4
only if profiling still shows headroom.

## Status (2026-07-17)

- **#1 — DONE & shipped** (`93a9f4ff`): host-side SRA pipelining enabled.
- **#2 — DONE & shipped** (`33f4cbc9`): radix = non-factor (keep 4), frag = 512K,
  size-only adaptive `pdepth = (total ≥ 2MB) ? 4 : 2`. Crossover reconfirmed over
  64KB–64MB (job 10175). See plan-2.
- **#3 — DEPRIORITIZED** (`d5904f78`): profiling shows reduce is only 6.6% of
  cycles in the pipelined path (already hidden). See plan-3.
- **#4 — ANALYZED, no hot-path target:** both written targets are off the ALLREDUCE
  critical path (ring not selected in-window; proxy memcpy is REDUCE_SCATTER-only),
  and the ALLREDUCE reduce-scatter already writes its final segment straight to
  `dst` — no redundant full-buffer pass to trim. Consistent with #3 (network-bound).
- **#5 — CLOSED (data):** ring never wins any (size, rank) cell → no selection-string
  change. Residual scale headroom is PPN-driven, not a ring/selection matter. See
  plan-5.
