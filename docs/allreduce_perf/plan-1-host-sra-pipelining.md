# Plan #1 — Enable host-side SRA pipelining (do first)

## Intent

Overlap the reduce-scatter and allgather phases across fragments on the host
path so that fragment *i*'s allgather (pure network) runs concurrently with
fragment *i+1*'s reduce-scatter (network + **CPU reduction**). Today the host
path runs one monolithic RS to completion, then one monolithic AG — the NIC is
idle during every reduce step and the CPU is idle during the entire AG.
Fragmenting hides the reduction cost and fills the phase-boundary bubble. This
is precisely the 512kB–4MB regime (large enough to amortize per-frag latency,
reduction cost is non-trivial).

## Scope

- **In:** change only the `auto` host branch of the SRA pipeline-param heuristic
  so it produces `n_frags>1` / `pdepth>=2` for host buffers above a threshold.
- **Out:** no change to frag_init/frag_setup, scratch sizing, the RS/AG kernels,
  or the selection string. No change to CUDA behavior. Env override
  `ALLREDUCE_SRA_KN_PIPELINE` continues to take precedence (already handled at
  `allreduce_sra_knomial.c:170-173`).

## The one change

`ucc_tl_ucp_allreduce_sra_knomial_get_pipeline_params()` —
`allreduce/allreduce_sra_knomial.c:163-193`. Current host branch (the `else`)
disables pipelining:

```c
} else {
    pp->threshold = SIZE_MAX;   // msgsize is never > threshold -> n_frags=1
    pp->n_frags   = 0;
    pp->frag_size = 0;
    pp->pdepth    = 1;
    pp->order     = UCC_PIPELINE_PARALLEL;
}
```

Replace the `else` body with real host defaults, mirroring the reduce-SRG
structure (`reduce/reduce_srg_knomial.c:289-295` is the analogous branch — good
precedent to cite in the commit):

```c
} else {
    /* Host path: pipeline RS->AG across fragments so the allgather of
       one fragment overlaps the reduce-scatter (incl. CPU reduction) of
       the next. Values are conservative defaults; override via
       UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE. */
    pp->threshold = 262144;     /* start pipelining above 256KB */
    pp->frag_size = 262144;     /* ~256KB fragments */
    pp->n_frags   = 2;          /* floor: at least 2 frags once over threshold */
    pp->pdepth    = 2;          /* 2 fragments in flight */
    pp->order     = UCC_PIPELINE_PARALLEL;
}
```

Feeds directly into the existing, unchanged flow: `ucc_pipeline_nfrags_pdepth()`
(`src/schedule/ucc_schedule_pipelined.h:56`) — with these values 512KB->2 frags,
1MB->4, 4MB->16 (each ~256KB); `pdepth` caps in-flight frags at 2 ->
`allreduce_sra_knomial.c:221-232` builds the pipelined schedule.

## Why this is safe

- Correctness is already proven by the CUDA in-place path, which runs the
  identical fragmented schedule. Per-frag scratch is sized from `max_frag_count`
  in `reduce_scatter/reduce_scatter_knomial.c:435` `compute_scratch_size` (reads
  `coll_args->max_frag_count`), so smaller frags -> smaller scratch, no overflow.
- `pdepth=2` means at most 2 concurrent RS executors -> bounded extra
  scratch/executor pressure, not N×.

## Risks / watch-items

- **Single-NIC ceiling:** pipelining won't raise raw network BW; the win is
  hiding reduction compute + removing the RS<->AG bubble. Expect a solid but
  bounded gain, larger as reduction cost grows (wide dtypes, `OP_AVG`).
- **Too many tiny frags at 4MB** (16×256KB) could add latency overhead at high
  rank counts — that's exactly what Plan #2's sweep tunes
  (`frag_size`/`threshold`/`pdepth`). Ship #1 with these defaults, let #2
  finalize them.
- **Non-power-of-radix "extra" ranks** send a full-buffer per frag; more frags
  -> more such sends. Measure at a non-power-of-2 team size too.

## Validation

- Build + gtest via the ryzen/HPC-X path.
- `ucc_perftest` allreduce sweep 256KB–4MB, host, at 2/4/8/16 ranks. A/B by
  toggling `UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE=auto` (new default) vs. an
  explicit disabling value to reproduce the old monolithic behavior.
- Confirm no regression below the 256KB threshold (should be byte-identical
  path).
