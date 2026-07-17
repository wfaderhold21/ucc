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

## Results & decisions (thor, HPC-X 2.25, host float32/sum)

Harness: `contrib/slurm_allreduce_sra_radix_frag_sweep.sh` (radix/frag/pdepth/alg
grid + PPN-sweep mode). Metric = ucc_perftest bus bandwidth (GB/s, higher better).

- **Full grid (job 10088):** 18 SRA (radix{2,4,8}×frag{128K,256K,512K}×pdepth{2,3})
  + mono + ring, ranks {2,4,8,16}, 256KB–4MB.
  - **radix — NON-FACTOR.** r2/r4/r8 within ~1% everywhere → keep default `4`,
    no `tl_ucp.c` change.
  - **frag_size=512KB** best at ≥1MB (matches Plan #1) → keep.
  - **ring loses everywhere** (30–45% slower), no contiguous win → no selection
    string change; keep SRA as the sole default path.
  - **mono beaten** at ≥512KB → re-confirms Plan #1.
- **Hi-rank confirmation (job 10090):** frag{256K,512K} × pdepth{2,3,4},
  8 nodes × PPN{1,2,4,8,16,32} → ranks {8,16,32,64,128,256}, 256KB–8MB.
  - **pdepth=4 is the true optimum at ≥2MB (≥4 frags of 512K) for 8–64 ranks:
    +6–23%** over depth 2. **Depth 3 is never uniquely best** — the earlier
    "pdepth=3 @ ≥4MB" read was an artifact of not testing depth 4.
  - At **≥128 ranks** (high PPN → NIC contention) the deeper pipeline regresses
    ~5–12% past 4MB; there depth 2 wins.

### Decision (shipped)

Size-only adaptive depth in `allreduce_sra_knomial.c` host branch:
`pp->pdepth = (total >= 4 * frag_size) ? 4 : 2` (i.e. depth 4 once ≥4 frags /
≥2MB, else 2). Chosen over a size+rank rule for simplicity, accepting the
≥128-rank / ≥4MB regression. radix, frag_size, threshold and the selection
string are left unchanged (all data-backed no-ops).
