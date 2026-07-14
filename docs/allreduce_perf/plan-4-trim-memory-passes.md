# Plan #4 — Trim redundant memory passes (in-place / proxy / ring)

## Intent

At 512kB–4MB you're partly memory-BW bound; each avoided full-buffer pass is
measurable. Two concrete targets.

## Targets

1. **Ring out-of-place upfront copy** — `allreduce/allreduce_ring.c:55-64` does a
   full `src->dst` `ucc_mc_memcpy` before the pipeline (one extra pass over the
   whole buffer). Fold the copy into the first reduce-scatter step (copy only
   each segment as it's first touched) instead of a monolithic pre-pass. Scope:
   `allreduce_ring.c` + possibly `reduce_scatter/reduce_scatter_ring.c` first-step
   handling.
2. **Knomial proxy staging** — `reduce_scatter/reduce_scatter_knomial.c:367`
   proxy `ucc_mc_memcpy(wb.dst_data, wb.reduce_loop, ...)` and the input-sized
   scratch from `compute_scratch_size` (`:435-477`, e.g. `:459`
   `ucc_max(max_recv_size, data_size)` for in-place). Audit whether the final
   segment can be reduced directly into `dst` to drop the copy.

## Scope

- **In:** buffer-selection / final-write paths above.
- **Out:** knomial pattern math.

## Risk

Buffer aliasing correctness — these paths are shared across
allreduce/reduce/reduce_scatter (`GET_COUNT`/`get_rs_work_buf` switch on
`coll_type`). Any change needs the full gtest matrix (in-place + out-of-place +
proxy ranks + reduce/reduce_scatter, not just allreduce). Second-order gain; do
after #1/#2.
