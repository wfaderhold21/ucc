# Plan #2 — Radix + fragment/algorithm sweep (tune the defaults)

## Intent

Turn the hard-coded constants from #1 and the fixed `REDUCE_SCATTER_KN_RADIX=4`
into measured optima, and decide whether `ring` should own a sub-window of the
selection string. Bandwidth-bound sizes are exactly where radix (rounds vs.
segment contiguity) and frag size actually move numbers, and the optimum shifts
with rank count.

## Scope

- **In:** measurement harness + config-default changes only (`tl_ucp.c` config
  table + the Plan-#1 constants + possibly
  `UCC_TL_UCP_ALLREDUCE_DEFAULT_ALG_SELECT_STR`). No kernel changes.
- **Out:** no new algorithm code.

## Anchors

- SRA radix auto-pick: `ucc_tl_ucp_get_knomial_radix()` `tl_ucp_coll.h:256`,
  called at `allreduce/allreduce_sra_knomial.c:115`.
- RS radix default `"4"`: `tl_ucp.c:166-169` (`REDUCE_SCATTER_KN_RADIX`).
- Pipeline constants: from Plan #1 (`allreduce_sra_knomial.c` host branch).
- Selection string to potentially edit: `allreduce/allreduce.h:24`.

## Method

Sweep the cross-product
`{radix in 2,4,8} × {frag_size in 128K,256K,512K} × {pdepth 2,3} × {alg: sra, ring}`
over 256KB–4MB × {2,4,8,16 ranks} via env vars
(`UCC_TL_UCP_REDUCE_SCATTER_KN_RADIX`, `UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE`,
`UCC_TL_UCP_ALLREDUCE_ALG=ring`). Fold winners back into the config defaults and,
if `ring` wins a contiguous window, add a segment to the selection string (e.g.
`allreduce:512k-2m:@4`). Runnable as a Workflow if we want the grid fanned out in
parallel.

## Risk

`ring` requires `count % tsize == 0` (`allreduce/allreduce_ring.c:108`) — only
select it where guaranteed, keep SRA as fallback.
