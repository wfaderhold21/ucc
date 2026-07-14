# Plan #3 — Fine-grained reduce overlap inside knomial reduce-scatter

## Intent

Within one knomial step, the reduction can't start until the **last** peer's
segment arrives, then a single strided reduce runs over all `step_radix-1`
inputs. Reduce-as-you-receive would overlap reduction of early-arriving segments
with still-outstanding recvs. Complements #1 (which overlaps *across* frags; this
overlaps *within* a step) and helps most at higher radix.

## Scope

- **In:** the loop-phase send/recv/reduce sequence in
  `reduce_scatter/reduce_scatter_knomial.c:294-352`.
- **Out:** buffer layout / `get_rs_work_buf` (`:184`) untouched if possible;
  pattern math untouched.

## Anchors + current shape

`reduce_scatter_knomial.c:300-318` posts all `radix-1` recvs, then `:320`
`UCC_KN_PHASE_LOOP` waits for **all** via `ucc_tl_ucp_test`, then `:337`
`ucc_dt_reduce_strided(..., step_radix - 1, ...)` reduces the whole batch at
once:

```c
for (loop_step = radix - 1; loop_step > 0; loop_step--) {   // :300 post all recvs
    ...
    ucc_tl_ucp_recv_nb(wb.dst_loop, local_seg_count * dt_size, ...);
    wb.dst_loop = PTR_OFFSET(wb.dst_loop, local_seg_count * dt_size);
}
UCC_KN_PHASE_LOOP:
if (UCC_INPROGRESS == ucc_tl_ucp_test(task)) { ... }        // :321 wait for ALL
...
ucc_dt_reduce_strided(local_data, wb.dst_loop, wb.reduce_loop,
                      step_radix - 1, ...);                 // :337 one big reduce
```

## Approach

Drive reductions off individual recv completions (per-segment reduce as each
`recv` retires) instead of one barrier + one strided reduce. Requires threading a
per-segment completion count through the saved state
(`task->reduce_scatter_kn.phase` + a new counter) and issuing incremental
`ucc_dt_reduce` calls. This is the **most invasive** plan — the phase state
machine and executor task tracking (`etask`) need care.

## Risk / note

Higher complexity, and it competes with #1 for the same idle CPU cycles —
measure #1 alone first; only pursue #3 if profiling still shows reduction on the
critical path. Likely lower priority than #1/#2.
