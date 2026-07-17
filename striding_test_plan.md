# Alltoall Peer-Ordering ("Striding") Isolation Test Plan

> Baseline: **master** branch. On master the onesided alltoall has **no** peer
> reordering — `ucc_tl_ucp_alltoall_onesided_{get,put}_progress` walk peers purely
> sequentially:
>
> ```c
> ucc_rank_t peer = (grank + *posted + 1) % gsize;
> for (; *posted < gsize; peer = (peer + 1) % gsize) { ucc_tl_ucp_get_nb(... peer ...) }
> ```
>
> There is no local/remote classification, no stride, no interleave, no pacing
> beyond a single fixed `tokens` window. The "striding" scheme is a **prototype to
> add and measure**, not an existing knob.

## 1. Hypothesis

Reordering each rank's peer schedule so that **intra-node ops and inter-node ops
are issued interleaved** raises effective alltoall bandwidth versus master's
`peer = (grank + 1 + iter) % gsize`, where every rank marches through peers in
lockstep.

**Mechanism to prove or refute:** the two op classes are serviced by *orthogonal*
hardware lanes — an intra-node peer rides a UCX shared-memory transport
(`cma`/`xpmem`/`posix`, i.e. CPU + memory-controller bandwidth), an inter-node
peer rides RC/DC over the IB HCA (NIC + PCIe + DMA). Master's synchronized order
fill-drains each lane in alternation: while a node is doing its run of intra-node
ops the HCA is idle; while it is inter-node-bound the shm lane is idle.
Interleaving keeps *both* lanes busy in the windows where one would otherwise be
stranded, shrinking wall-clock time.

**Key subtlety vs. master:** master never issues an explicit `memcpy` for local
peers — it hands *every* peer to `ucc_tl_ucp_{get,put}_nb` and UCX picks the
transport. So "shmem op" = op whose peer is co-located and lands on a shm
transport; "RMA op" = op whose peer is remote and lands on IB. The experiment
controls the *order* those ops are posted, not which transport UCX chooses.

**Secondary hypothesis (layout sensitivity):** an ordering keyed on *global*
`rank % 2` desynchronizes under block placement but degenerates to a no-op under
`--map-by node` (all node-local ranks share parity). An ordering keyed on
node-local structure (via the `UCC_SBGP_NODE` sbgp) is layout-independent.

## 2. What is being isolated

Master couples nothing — it's a clean sequential baseline. The prototype adds, in
increasing sophistication:
1. **Stride permutation** — visit peers in a coprime-stride order to desync ranks.
2. **Local/remote classification** — split peers via the `UCC_SBGP_NODE` sbgp.
3. **Interleaving** — alternate remote/local peers in the issue order.
4. **Remote-list rotation** — offset each co-located rank's remote start so they
   target different remote nodes at the same step (incast spreading).

The test must **add each independently** so a bandwidth gain can be attributed to
a specific mechanism rather than the bundle. Message size, buffers, transport, and
the `tokens` window are held fixed.

## 3. Ordering policies under test

| ID | Policy | Purpose |
|----|--------|---------|
| `SEQ`        | Master baseline: `peer = (grank + 1 + iter) % gsize` | Control / synchronized |
| `STRIDE`     | Coprime-stride permutation only, no interleave | Isolate mechanism #1 |
| `ILV`        | Local/remote interleave (sbgp split), no stride, no rotate | Isolate #3 |
| `ILV+STRIDE` | Interleave + coprime stride, no rotate | #1 + #3 |
| `ILV+ROT`    | Interleave + remote-list rotation, no stride | #3 + #4 |
| `FULL`       | Stride + interleave + rotate | Upper bound |
| `RANK2`      | Naive global `rank % 2` phase split (shmem-first / RMA-first) | Show layout fragility |

## 4. Independent variables

- **Mapping:** `block` (contiguous, node 0 = ranks 0–31) and `--map-by node`
  (round-robin, node 0 = ranks 0,4,8,…). The two layouts of interest.
- **Scale:** 4 nodes × 32 PPN = 128 ranks (primary). Add 2×32 and 8×32 to see how
  the effect scales with the remote:local ratio.
- **Message size:** per-peer size sweep — 1 KB, 8 KB, 64 KB, 256 KB, 1 MB, 4 MB.
  Theory predicts the gain widens as both lanes approach saturation.

## 5. Metrics

Primary:
- **Effective alltoall bandwidth** (bytes moved / wall-clock), median of N iters
  after warmup, plus p10/p90.

Mechanism-confirming (this is what actually tests the *why*):
- **HCA tx/rx bytes over time** — sample
  `/sys/class/infiniband/*/ports/*/counters/port_{xmit,rcv}_data` (or
  `perfquery`) at a fixed interval. Expect: `SEQ` shows a sawtooth (HCA idle
  during intra-node-heavy windows); interleaved policies show a flatter, higher
  plateau.
- **Memory bandwidth over time** — `pcm-memory`, `likwid-perfctr` DRAM/MBOX
  counters, or `perf stat` uncore IMC events. Expect `SEQ` DRAM BW to sawtooth in
  **anti-phase** with HCA BW; interleaved policies overlap them.
- **Congestion** — IB PFC/pause and out-of-buffer counters. If interleaving also
  reduces incast, expect fewer pauses vs `SEQ`.

Deriving the occupancy "table" empirically: bucket issued ops by (timestep, class)
per node and plot intra-node-count vs inter-node-count per step. This reproduces
the analytical tables from real runs and is the direct visual proof of joint
lane utilization.

## 6. Test harness (in order of fidelity)

### Level 0 — Analytical simulator (fast, no cluster)
Standalone script (Python/C): given `(nnodes, ppn, mapping, policy)`, emit the
per-timestep `(intra_count, inter_count)` per node — i.e. generate the occupancy
tables we reasoned about. Confirms the counting and predicts which policies
flatten the load before spending cluster time. **Deliverable:** occupancy CSV +
heatmap per policy/mapping.

### Level 1 — Standalone UCP microbenchmark (isolates the effect)
Minimal MPI + UCX/UCP program (NOT full UCC) that mirrors master's mechanism:
- Register src/dst buffers, exchange rkeys with all peers.
- For each peer in a **pluggable ordering policy**, issue `ucp_get_nb`/`ucp_put_nb`
  — **for every peer, exactly as master does** (let UCX pick shm vs IB transport;
  do *not* hand-code a memcpy for local peers — that would diverge from what UCC
  actually executes).
- Detect co-location via an MPI split on hostname (used only to *classify* peers
  for the interleave policy and for occupancy bucketing, not to change the call).
- Fixed inflight window (mirror master's `tokens`); time the full sweep; report BW.
- Ordering policy selected by env var so all policies run from one binary.

This isolates *ordering* from the rest of UCC while keeping master's
"everything goes through UCP" behavior intact.

### Level 2 — In-situ UCC prototype (highest fidelity)
Prototype the reordering inside master's onesided alltoall. Because master
computes `peer` inline in the progress loop, this requires:
- Building a `peer_order[]` (and, for interleave, an `is_remote[]`) array at
  `_init` time using `ucc_topo_get_sbgp(tl_team->topo, UCC_SBGP_NODE)`.
- Replacing the inline `peer = (peer + 1) % gsize` in both `get_progress` and
  `put_progress` with `peer = peer_order[*posted % gsize]`.
- Gating the order-build on an env/config
  `UCC_TL_UCP_ALLTOALL_ONESIDED_ORDER=seq|stride|ilv|full` so all policies run
  from one build.

Run the real alltoall via the UCC perftest / gtest harness. Confirms the
microbenchmark result survives inside the real stack. Keep behind a hidden config.

## 7. Experiment matrix

```
policies × mappings × sizes × scales, each: warmup 20, measured 100 iters
  primary sweep: {SEQ, STRIDE, ILV, ILV+STRIDE, FULL} × {block, map-by-node}
                 × {8K, 64K, 256K, 1M, 4M} × {4x32}
  layout probe:  {RANK2, FULL} × {block, map-by-node} × {256K} × {4x32}
  scale probe:   {SEQ, FULL} × {block} × {256K} × {2x32, 4x32, 8x32}
```

## 8. Success / refutation criteria

- **Confirms hypothesis** if interleaved policies (`ILV`, `FULL`) beat `SEQ` on
  bandwidth **and** counter traces show HCA and DRAM BW overlapping (flatter,
  higher plateaus) instead of anti-phase sawtooth.
- **Confirms layout sensitivity** if `RANK2` matches `FULL` under `block` but
  collapses toward `SEQ` under `--map-by node`, while `FULL` holds under both.
- **Refutes / rethink** if `ILV` beats `SEQ` but counters show *no* change in
  temporal overlap — the gain is then coming from incast/PFC reduction or
  transport-queue effects, and mechanism #4 (rotation) should be isolated next.
- **Null result** if `STRIDE` alone (no interleave) captures most of the gain —
  meaning cross-rank desync, not intra-node lane overlap, is the driver.

## 9. Confounders to control

- **`tokens` window:** master derives one `tokens` value from a perf probe to
  `rank+1` and holds it fixed. Use the *same* `tokens` for every policy or the
  ordering comparison is contaminated. (Master has no fractional pacing / stagger
  to worry about — those are topic-branch-only.)
- **UCX transport selection:** pin `UCX_TLS` / rc/dc/shm choices consistently
  across policies so a policy change can't accidentally shift which transport a
  peer uses. Log `UCX_*` env for every run.
- **Warmup / rkey exchange / first-touch page faults:** exclude from timing.
- **NUMA:** pin buffers and ranks; a NUMA-crossing shm op changes intra-node cost
  independent of ordering.
- **Clock:** high-res timer over a barrier-bounded region; report the max across
  ranks (slowest finisher = collective time).

## 10. Phases / deliverables

1. **Level 0 simulator** → occupancy tables + heatmaps for both mappings. (cheap,
   do first; validates the model)
2. **Level 1 microbenchmark** → bandwidth-vs-size curves per policy/mapping.
3. **Counter instrumentation** → HCA/DRAM BW time series proving (or not) the
   overlap mechanism.
4. **Level 2 in-situ prototype** → confirm in the real UCC alltoall on top of
   master.
5. **Write-up** → which mechanism (stride / classify / interleave / rotate)
   accounts for the gain, and the layout-sensitivity result. This becomes the
   justification for the topic-branch reordering.
