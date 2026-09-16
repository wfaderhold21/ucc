# Checkpoint — UCC live link-quality monitor (long-haul collectives, step 1)

Handoff state as of 2026-09-15 (session 2). Read this, then pick up at "Open questions / next".

## Repo state

- **Branch:** `topic/shrink_v2_quality` (off `topic/shrink_v2`)
- **Repo:** `/Users/faderholdt/curr-work/ucc-shrink`
- **Working tree:** uncommitted changes (see "Changes this session"). Not committed.
- **Cluster build:** green. Full clean build + install succeeds on `hpcac-internal` (thor) via Slurm:

```
sbatch slurm-build-quality.sh        # full clean build + install
```

Build + runtime artifacts on the cluster:
- Install: `/global/home/users/faderholdt/ucc-staging/ucc-shrink/install`
- build log: `/global/home/users/faderholdt/ucc-staging/quality-build-<job>.out`
- Run scripts (in repo): `slurm-build-quality.sh`, `slurm-run-quality-mpi.sh`

## What the build needed (real blockers fixed this session)

1. **Core `-Wshadow` build break (pre-existing).** `src/core/ucc_progress_queue_mt.c`:
   `ucc_pq_locked_mt_drain` declared `*tmp` shadowed by the `ucc_list_extract_head`
   macro's internal `tmp`; every gcc-11+ build fails under UCC's `-Wall -Werror
   -Wshadow`. Fixed by renaming the outer variable to `tmp_task`. (Present on the
   parent branch too — a latent bug, not introduced here.)
2. **`ucs/time/time.h` not installed in HPC-X UCX.** This UCX ships only
   `ucs/time/time_def.h`, no `ucs_get_time()`. Replaced with a monotonic-ns helper
   using `clock_gettime(CLOCK_MONOTONIC)` (`ucc_tl_ucp_quality_now()` in
   `tl_ucp_quality.c`). Chosen over `ucc_get_time()` because that is
   `gettimeofday` (wall clock, NTP-skewable) — unsuitable for RTT.
3. **Service-worker eps never connected.** `ctx->service_worker.eps[rank]` was
   left all-NULL (service eps only connect for service/FT abort teams), so the
   probe could never send. Added lazy `ucc_tl_ucp_quality_connect_peer()` that
   connects each peer's service-worker endpoint from the context address exchange
   (`ucc_get_team_ep_addr` + `TL_UCP_EP_ADDR_WORKER_SERVICE`), called on first
   probe.
4. **Echo send missing datatype.** `req_recv_cb` set
   `UCP_OP_ATTR_FIELD_DATATYPE` but never `req_param.datatype`, so the echoed
   timestamp payload was not the 8-byte value.
5. **Echo sent from a stack local.** `req_recv_cb` echoed via a stack-local
   `uint64_t echo_ts`; `ucp_tag_send_nbx` is async and can reference the buffer
   after the callback returns → the echoed payload was garbage (observed ~0, then
   stack garbage). Fixed with a persistent `ctx->quality.echo_payload` field.

Also added (for observability / step-5 seed, NOT wired into the shrink decision):
- periodic `ucc_tl_ucp_quality_classify` pass in `ucc_tl_ucp_quality_progress`
  (refreshes live `state`), with a `tl_debug` on state transitions.
- `UCC_TL_UCP_QUALITY_BW_CHECK` config (default `n`) gating the throughput-vs-
  static-BW degradation check (see finding 2 below).

## Verification status (this session)

**Built:** yes, clean, confirmed on cluster (job 12497, BUILD_OK).

**Probe mechanics on real IB (2 × thor, mlx5_0:1, rc): VERIFIED**
- Probe request send → peer request-recv → echo send → sender echo-recv, all
  complete (1000s of round trips observed).
- Tag masking / sender extraction correct in both directions:
  `sender_tag=0x1ffe800000000000` (rank 0) vs `0x1ffe800000010000` (rank 1).
- No tag collision: the probe coexisted with the full barrier/allreduce
  correctness suite; all collectives completed correctly with probes active.
- Config knobs engage with the right prefix:
  `PERFTEST_UCC_*` for `ucc_perftest`, `UCC_*` for `ucc_test_mpi`.

To observe the probe you need the service worker to actually progress; the
`ucc_test_mpi` correctness suite provides the idle gaps that let it run. Command:
```
UCC_TL_UCP_SERVICE_WORKER=y UCC_TL_UCP_SERVICE_THROTTLING_THRESH=1 \
UCC_TL_UCP_QUALITY_MONITOR=y UCC_TL_UCP_QUALITY_PROBE_INTERVAL=10000 \
UCC_LOG_LEVEL=trace mpirun -np 2 ucc_test_mpi
```
(`slurm-run-quality-mpi.sh` runs exactly this on 2 nodes.)

## Two findings that block the rest of the plan

### Finding 1 — RTT is uncalibrated: the service worker is idle-gated progress
The probe runs over the *service worker*, but UCX/UCC only progresses the service
worker when the **main progress queue is empty** (`ucc_context_progress` gates
registered progress fns behind `ucc_progress_queue_is_empty` + a throttle). During
active collectives the service worker is not progressed; probes only fire in idle
gaps. Consequence: measured RTT is dominated by the *service-worker polling
latency*, not the wire. Observed healthy-link RTT: **26 µs … 1.2 s** (noise floor is
millisecond-to-second, versus ~1–5 µs real IB latency). The RTT-vs-static-latency
ratio check therefore *always* flags DEGRADED on an idle or small-message healthy
link.

### Finding 2 — throughput-vs-static-BW check false-positives
Static BW baseline is measured at 1 MiB; a latency-bound / small-message workload
(8-byte allreduce here) inherently sits far below 50% of it → always DEGRADED.
Freshness gating (added) doesn't fix the mismatch. Gated behind
`QUALITY_BW_CHECK=n` (default off) so it doesn't poison the default classifier.

### Net result
The RTT probe machinery is verified correct, but as currently measured the RTT
signal cannot support "predictive, false-positive-free" degradation detection: the
classifier DEGRADES every healthy peer because the RTT and BW signals are both
uncalibrated against the actual data path.

## Open questions / next (design decision required)

The degradation test ("netem: no false-positive shrink + HEALTHY→DEGRADED before
keepalive timeout") is **blocked** until the measurement is placed on a
continuously-progressed path. Candidate directions (tradeoffs for you to weigh):

A. **Probe over the main worker** (progressed continuously during collectives).
   RTT ≈ real wire latency; probe traffic shares the data path (reserved tags
   avoid collisions) — arguably measures the real data path, but mixes probe and
   collective traffic.
B. **Progress the service worker on a fixed timer** (dedicated progress thread /
   more aggressive hook), keeping probe traffic off the collective workers but
   needing a new progress mechanism in the TL (and careful shutdown).
C. Keep the service worker but lower/remove the idle-gating + throttle — minimal
   change, but fighting core `ucc_context_progress` behavior.

Remaining steps after the RTT fix: run the netem/congestion test (Finding-driven),
wire the classifier into `ucc_tl_ucp_mark_peer_failed`/abort (rebalance DEGRADED
vs shrink DEAD), and expose the state as a context/team attribute or debug counter
for tests (step already seeded by the periodic classify + transition log).

## Broader context (unchanged from session 1)

- Goal: long-haul (WAN/geo-distributed) collectives; fail-stop shrink is
  commoditized (NCCL 2.27, R²CCL/PReCCL/ReCoVer, ULFM); the open niche is
  **false-positive-free, predictive failure detection** — degraded/transient links,
  not dead ranks.
- This branch is the measurement substrate; it does NOT yet change the shrink
  decision path.
- DOCA/DPA offload (doca_pcc etc.) does not expose RTT/loss to the host; UCX
  `ucp_ep_evaluate_perf` and port-speed estimation are static/quantized → the
  estimator must be software-side (this monitor).
