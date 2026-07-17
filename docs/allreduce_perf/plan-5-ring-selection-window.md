# Plan #5 — Ring as a selectable default in a sub-window

## Intent

Ring RS+AG moves contiguous `M/n` chunks — friendly to large transfers and often
beats knomial radix-4 at 1–4MB on flat fabrics at low-to-moderate rank counts
(knomial's fewer-rounds advantage stops mattering once bandwidth-bound). It's
implemented (`allreduce/allreduce_ring.c`, alg `@4`) but not in the default
string.

## Scope

- **In:** `UCC_TL_UCP_ALLREDUCE_DEFAULT_ALG_SELECT_STR` at
  `allreduce/allreduce.h:24`, driven by #2's data.
- **Out:** ring kernel changes (except whatever #4 does).

## Change shape

Only after #2 shows a clean win window, e.g.:

```c
"allreduce:0-4k:@0#allreduce:4k-512k:@1#allreduce:512k-2m:@4#allreduce:2m-inf:@1"
```

## Risk

`count % tsize != 0` -> ring returns `UCC_ERR_NOT_SUPPORTED`
(`allreduce/allreduce_ring.c:108`); the selection layer must fall back to SRA.
Verify the fallback actually triggers rather than erroring the collective. High
rank counts favor SRA (ring latency `2(n-1)`), so keep any ring window bounded in
both size and scale.

## Results & decision — NO ring window (data-closed)

Two independent sweeps (Plan #2 job 10088 at 256KB–4MB, and job 10175 widened to
**64KB–64MB** × ranks {8,32,64,128,256}) both show **`ring` is never the outright
best in any (size, rank) cell** on the thor fabric. Data:
`results/plan5-ring-crossover/summary.{csv,txt}`.

- Ring trails the shipped pipelined SRA path everywhere; it gets closest at
  32r/32MB (~−10%) but never crosses into a winning window.
- Ring *does* overtake `mono` (the non-pipelined fallback) at high-rank + large
  sizes (e.g. 128r/16MB: ring 1.87 vs mono 1.02 GB/s) — because mono collapses at
  scale, not because ring is fast. Irrelevant to selection since we ship the
  pipelined path, never mono.

→ **No change to `ALLREDUCE_DEFAULT_ALG_SELECT_STR` (`allreduce.h:24`).** SRA stays
the sole default across 4k–inf. Plan #5 is **closed** (no selectable ring
sub-window is justified on this fabric).

### Future lever (if the ≥128-rank / ≥4MB corner ever matters)

The one place the shipped size-only pdepth rule leaves ~6–12% on the table
(≥128 ranks, ≥4MB — see plan-2 job 10175) is driven by **PPN / NIC contention**,
not total rank count or algorithm choice. The correct fix would be a **PPN-aware**
pipeline-depth heuristic (cap depth when procs-per-node is high), which needs node
topology at `get_pipeline_params` time plus a PPN-isolated sweep (fix rank count,
vary PPN) to find the real threshold. Not a ring/selection change.
