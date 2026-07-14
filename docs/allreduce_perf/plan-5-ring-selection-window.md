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
