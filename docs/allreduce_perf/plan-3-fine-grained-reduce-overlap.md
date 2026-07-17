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

## Profiling verdict (2026-07-17, thor 8-node IB, host f32/sum, 2-4MB) — DEPRIORITIZE

`perf record` (self cycles, rank 0) via
`contrib/slurm_profile_reduce_scatter.sh`. Self-% of the two contended leaves:

| config (np8, 2-4MB)                       | ucc_ec_cpu_reduce | net progress (uct+ucp) |
|-------------------------------------------|-------------------|------------------------|
| knomial RS standalone (radix4, no pipe)   | **26.7%**         | ~25.7%                 |
| ring RS standalone (no pipe)              | 43.5%             | ~24%                   |
| **allreduce SRA, pipelined (shipped)** np8| **6.6%**          | ~27%                   |
| allreduce SRA, pipelined np2              | 7.6%              | ~16.7%                 |

Findings:
1. Reduction is heavy in the *raw* knomial RS step (~27%, on par with network) —
   so the targeted phase does have reduce on its critical path un-pipelined.
2. **The shipped fragment pipeline (#1/#2) already hides most of it:** exposed
   reduce drops ~27% -> ~6.6% in the real allreduce; network dominates 4:1.
3. `ucc_cpu_executor_task_post` runs `ucc_ec_cpu_reduce` **synchronously inline
   on the single progress thread** (`ec_cpu.c:116`) — reduce is irreducible ALU
   work serialized with network progress. Plan #3 can only *reorder* it into
   spin-wait, and #1's cross-fragment pipelining already fills that spin-wait.
   #1 and #3 contend for the identical single-thread idle cycles.
4. Plan #3 is a no-op at radix 2 (np2: 1 recv/step, nothing to pre-reduce);
   needs radix >= 3 to apply at all.

**Decision: deprioritize #3.** In the 512KB-4MB target, reduction is not a
meaningfully *exposed* cost after #1/#2 (~7%, already overlapped). If reduce ever
must leave the critical path, the higher-leverage move is offloading it to a
helper/executor thread (true overlap), not same-thread per-segment reordering.
