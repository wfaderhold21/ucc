# Threaded / Offloaded Reduction in UCC — Implementation Plan

Branch: `topic/thread-allreduce`
Goal: offload reduction work off GPU SMs onto host CPU threads and, ultimately,
the BlueField DPU, by adding threads to the executor (`ec`) component.

Scope is four milestones, in dependency order:

- **D** — Vectorize + parallelize the CPU reduce kernel (foundation).
- **A** — Threaded CPU executor (worker pool + async task semantics).
- **B** — CUDA host-offload routing (SM → CPU threads).
- **C** — BlueField/DPU reduction offload (SM → DPU ARM cores).

Each milestone defines success criteria, the benchmark(s) used to measure them,
the feature set to implement, an evaluation pass against the criteria, and a
commit directive that MUST be satisfied before the next milestone starts.

---

## Current state (grounding)

- Executor contract: `src/components/ec/base/ucc_ec_base.h`
  (`ucc_ee_executor_ops_t`: `init/start/stop/finalize/task_post/task_test/task_finalize`;
  task types `REDUCE`, `REDUCE_STRIDED`, `REDUCE_MULTI_DST`, `COPY`, `COPY_MULTI`).
- CPU executor is synchronous: `src/components/ec/cpu/ec_cpu.c`
  (`ucc_cpu_executor_task_post` runs `ucc_ec_cpu_reduce` inline; `task_test` returns
  `task->status` immediately). No threads, no queue.
- CPU reduce kernel is scalar, single-threaded: `src/components/ec/cpu/ec_cpu_reduce.c`
  (`DO_DT_REDUCE_*` macros over `utils/ucc_math_op.h` `DO_OP_*`).
- CUDA executor runs reduction on SMs: `src/components/ec/cuda/ec_cuda_executor_persistent.c`,
  `ec_cuda_executor_interruptible.c`, kernels in `src/components/ec/cuda/kernel/`.
- ROCm already routes small reduces/copies to a nested CPU executor:
  `src/components/ec/rocm/ec_rocm_executor_interruptible.c` (`ec_rocm_use_host_ops()`,
  `REDUCE_HOST_LIMIT`/`COPY_HOST_LIMIT`). This is the template for Milestone B.
- Executor selection is memory-type driven: `src/core/ucc_coll.c:297-305`
  (CUDA→`UCC_EE_CUDA_STREAM`, ROCm→`UCC_EE_ROCM_STREAM`, HOST→`UCC_EE_CPU_THREAD`).
- Reduction dispatch: TLs call `ucc_dt_reduce()`/`ucc_dt_reduce_strided()`
  (`src/utils/ucc_dt_reduce.h`) → `ucc_ee_executor_task_post()`.
- No CPU thread pool exists in `src/` today. Prior prototype lives on
  `origin/topic/threaded_reduce` (pthread pool + chunked reduce; has known gaps:
  spin-wait, malloc-per-task, condvar-per-task, chunk srcs not offset, alpha mishandled).

## Benchmark harnesses (used across milestones)

- Executor microbenchmarks (measure the reduce op directly, no network):
  - `tools/perf/ucc_perftest` op types `reducedt` → `ucc_pt_op_reduce`
    (`tools/perf/ucc_pt_op_reduce.cc`, `UCC_EE_EXECUTOR_TASK_REDUCE`) and
    `reducedt_strided` → `ucc_pt_op_reduce_strided`
    (`tools/perf/ucc_pt_op_reduce_strided.cc`, `UCC_EE_EXECUTOR_TASK_REDUCE_STRIDED`).
  - `memcpy` → `ucc_pt_op_memcpy` (copy-path staging cost, relevant to B/C).
- End-to-end collectives:
  - `tools/perf/ucc_perftest` `reduce` (`ucc_pt_coll_reduce`) and
    `allreduce` (`ucc_pt_coll_allreduce`).
- Correctness unit tests:
  - `test/gtest/core/test_mc_reduce.cc` (reduce correctness across dt×op).
  - `test/gtest/core/test_ec_cuda.cc` (executor semantics).
  - `test/gtest/utils/test_lock_free_queue.cc` (queue used in Milestone A).
- SM-utilization evidence:
  - `nsys profile` / `ncu` on the `allreduce`/`reduce` perftest to quantify SM
    time spent in `executor_kernel`/reduce kernels (Milestones B, C).

---

## Prerequisite — Baseline capture (do once, before Milestone D)

On a clean `topic/thread-allreduce` tree (no feature work), record:

1. `ucc_perftest` op `reducedt` and `reducedt_strided` throughput (GB/s) for
   `-d float32,float64,int32` × `-o sum,prod,min,max` × `-m cpu`, over counts
   `1K..64M`, `-n 2` and `-n 8` source buffers.
2. `ucc_perftest` `reduce` and `allreduce` latency/bandwidth for `-m cuda` and
   `-m cuda_managed` across the same size sweep.
3. `ncu`/`nsys` SM-utilization and kernel-time breakdown of the CUDA reduce
   path (which kernels, how much SM time).
4. `test/gtest/core/test_mc_reduce` and `test_ec_cuda` pass state.

Commit this as `BASELINE.md` (numbers + exact command lines + machine spec)
plus any helper scripts under `tools/perf/`. This is the reference every
"match or beat" criterion below is judged against.

```
git add tools/perf BASELINE.md && \
git commit -m "perf: capture threaded-reduce baseline"
```

---

## Milestone D — Vectorize + parallelize the CPU reduce kernel

### Goal
Make `ucc_ec_cpu_reduce` fast enough that offloading reduction to CPU is
competitive, without touching the executor contract. Pure kernel work.

### Success criteria (measurable)
- **D1 correctness**: for every predefined `dt` × supported `op` in
  `ucc_ec_cpu_reduce`, results are bitwise-identical to the scalar reference
  for integer/uint types, and within 1 ULP (or documented float tolerance)
  for `float32/float64/bfloat16` — verified by an extended `test_mc_reduce`.
- **D2 throughput**: op `reducedt` / `reducedt_strided` CPU throughput at
  count ≥ 1M elements is ≥ 2× baseline for `sum`/`prod`/`min`/`max` on
  `float32/float64/int32/int64`.
- **D3 no-regression**: at count ≤ 1024 elements, throughput is within 5% of
  baseline (SIMD/chunking must not add small-message overhead).
- **D4 scaling**: with `N` threads, large-count throughput scales ≥ 0.6·N up
  to the socket core count (Amdahl-aware, not linear).

### Features
1. Add SIMD kernels under `src/utils/arch/x86_64/` (AVX2; AVX-512 behind a
   runtime/compile check) and `src/utils/arch/aarch64/` (NEON, for BlueField in
   Milestone C) for: `int8/16/32/64`, `uint8/16/32/64`, `float32`, `float64`.
   Cover `sum`, `prod`, `min`, `max`, `b*` bitwise/logical ops.
2. Refactor `ec_cpu_reduce.c`: dispatch `ucc_ec_cpu_reduce` on
   `(dt, op, count)` — scalar path for small messages, SIMD path otherwise.
   Keep the existing `DO_DT_REDUCE_*` macros as the scalar reference.
3. Add `ucc_ec_cpu_reduce_chunk(srcs, dst, op, dt, count, n_srcs, flags,
   start_idx, end_idx)` (correcting the reference impl: offset every `srcs[i]`
   by `start_idx * dt_size`, and thread `alpha` through correctly).
4. Add `ucc_ec_cpu_reduce_threaded()` that splits `count` across `num_threads`
   chunks, each chunk running the SIMD path. `bfloat16`/`fp16`/complex stay on
   the scalar path until a later sub-step (documented limitation).
5. Guard with `--enable-ec-threaded-reduce` (`config/m4/configure.m4`) so the
   build stays unchanged when disabled.

### Benchmarks
- `test/gtest/core/test_mc_reduce.cc`: extend to assert D1 across the full
  dt×op matrix, including odd counts (non-multiple-of-vector-width) and
  `n_srcs` 1..9.
- `ucc_perftest` op `reducedt` / `reducedt_strided`, `-m cpu`, size sweep
  `1K..64M`, `-n 2` and `-n 8`; compare D2/D3/D4 against BASELINE.

### Evaluation
Run the matrix; record GB/s and correctness. **Pass** = D1 holds exactly,
D2 ≥ 2× at every large size, D3 within 5% at small sizes, D4 ≥ 0.6·N. If a
criterion is missed, fix (not weaken) the criterion and re-run. Only proceed
after all four pass.

### Commit directive
```
git add src/components/ec/cpu src/utils/arch config/m4 test/gtest/core && \
git commit -m "EC/CPU: vectorize and parallelize reduce kernel"
```
Update `BASELINE.md` is not required here (baseline is pre-D); record new
numbers in the commit message body or a `NOTES.md` delta.

---

## Milestone A — Threaded CPU executor (worker pool)

### Goal
Give the CPU executor real asynchronous `task_post`/`task_test` semantics backed
by a worker pool, so a posted reduce runs off the caller's progress thread.

### Success criteria (measurable)
- **A1 correctness**: async semantics are race-free. `task_post` returns
  `UCC_INPROGRESS`; `task_test` returns `UCC_INPROGRESS` until the worker
  finishes, then `UCC_OK`; `task_finalize` returns the task to the pool. Verified
  by a new gtest (`test_ec_cpu_threaded.cc`) plus a stress run (many tasks,
  many threads) under ThreadSanitizer.
- **A2 no-regression**: with `USE_THREADED_REDUCE=0` (or `EXEC_NUM_WORKERS=1`
  synchronous fallback), op `reducedt` throughput is within 5% of baseline
  (the synchronous path must be preserved).
- **A3 overlap**: with `EXEC_NUM_WORKERS>1`, sustained post→complete task
  throughput exceeds the single-thread synchronous rate by ≥ 1.5× (proves the
  caller is not blocked on the reduce).
- **A4 latency**: median task latency (post→`task_test==UCC_OK`) at 1M-element
  `float32` sum is within 2× of the synchronous reduce time (no pathological
  queueing).

### Features
1. Implement `src/components/ec/cpu/ec_cpu_thread_pool.c/h` replacing the
   reference impl's spin-wait/malloc-per-task design:
   - Task queue: lock-free (`utils/ucc_lock_free_queue.h`), MPSC (producers =
     caller threads, consumer = pool). No spin-wait; use a futex/condvar to park
     idle workers.
   - Task objects: allocate `ucc_ec_cpu_task_t` from the existing
     `executor_tasks` mpool (`ec_cpu.c`), not malloc-per-post.
   - Completion: per-task atomic status + a single shared condvar (or futex),
     not a condvar-per-task.
2. `task_post` (threaded mode): copy `task_args` into the pooled task, enqueue,
   set status `UCC_INPROGRESS`, return. `task_test`: read atomic status.
   `task_finalize`: return task to mpool.
3. Worker loop dispatches `REDUCE`/`REDUCE_STRIDED` to
   `ucc_ec_cpu_reduce_threaded` (Milestone D) and `COPY` to `memcpy`;
   `REDUCE_MULTI_DST`/`COPY_MULTI` stay unsupported (documented).
4. Pool lifecycle in `ucc_ec_cpu_init`/`ucc_ec_cpu_finalize`, gated by
   `USE_THREADED_REDUCE`. Config: `EXEC_NUM_WORKERS`, `EXEC_MAX_TASKS`,
   `REDUCE_CHUNK_SIZE`, plus pinning knobs (`PIN_THREADS`, NUMA/socket) reusing
   the topology helpers from `origin/topic/threaded_reduce`, cleaned up.
5. Respect `ucc_thread_mode_t`: in `UCC_THREAD_SINGLE`/`FUNNELED`, workers are
   still safe because the pool is only ever driven by the single progress thread;
   document this. (No new user-visible threading guarantees are introduced.)

### Benchmarks
- New `test/gtest/core/test_ec_cpu_threaded.cc`: A1 semantics + stress.
- `ucc_perftest` op `reducedt`/`reducedt_strided`, `-m cpu`:
  - A2: `USE_THREADED_REDUCE=0` vs baseline.
  - A3: `EXEC_NUM_WORKERS=1,2,4,8` sustained throughput.
  - A4: latency distribution (add a `-l`/latency mode or use the existing
    per-test timing; if absent, add a small latency probe to the gtest).

### Evaluation
Run A1 under TSan (must be clean), then A2–A4 vs baseline. **Pass** = all four
criteria. A3/A4 are the real signals the pool is viable; if A4 misses, profile
queueing (contention, cache-line sharing on the status word) and fix.

### Commit directive
```
git add src/components/ec/cpu test/gtest/core config/m4 && \
git commit -m "EC/CPU: threaded executor with worker pool"
```

---

## Milestone B — CUDA host-offload routing (SM → CPU threads)

### Goal
Route `REDUCE`/`REDUCE_STRIDED` on the CUDA executor to the threaded CPU
executor (Milestone A) when a policy says host is cheaper, freeing SMs.

### Success criteria (measurable)
- **B1 correctness**: for offload-eligible cases, the host-reduced result is
  bitwise-identical (int/uint) / within fp tolerance (float types) to the
  GPU-reduced result, across the dt×op matrix, verified by an extended
  `test_ec_cuda.cc` comparing both paths.
- **B2 SM minimization**: `nsys`/`ncu` shows the CUDA reduce-kernel SM time for
  the offloaded portion drops to ~0 (the reduce no longer runs on the GPU).
- **B3 end-to-end**: `ucc_perftest` `reduce`/`allreduce`, `-m cuda` and
  `-m cuda_managed`, at offload-eligible sizes, stays within 10% of baseline
  latency/bandwidth (offload must not be slower than GPU for the sizes it picks).
- **B4 policy**: the offload decision (`ec_cuda_use_host_ops`) selects host for
  the sizes where B3 holds and GPU otherwise; instrumented via a counter/log.

### Features
1. Add `cpu_executor` to `ucc_ec_cuda_t` and its lifecycle (init/start/stop/
   finalize) mirroring `ucc_ec_rocm_t.cpu_executor` in
   `src/components/ec/rocm/ec_rocm_executor.c`. The CPU executor is the
   Milestone-A threaded instance.
2. Implement `ec_cuda_use_host_ops(task_args)` in
   `ec_cuda_executor_interruptible.c` (and the persistent path): route
   `REDUCE`/`REDUCE_STRIDED` to host when `USE_HOST_REDUCE=1` and
   `total_reduce_len <= REDUCE_HOST_LIMIT` and dt is host-supported
   (reuse ROCm's `ucc_ec_rocm_host_dt_supported` logic, factored into a shared
   helper).
3. Data residency: for `cuda_managed`/zero-copy buffers, post straight to the
   CPU executor. For device-resident buffers, stage D2H → host reduce → H2D
   using the executor `COPY` task and a staging-buffer pool in
   `ec_cuda_resources.c`.
4. Stream ordering: fence the staged path with CUDA events
   (`ucc_ec_ops_t.create_event/event_post/event_test`) so the host reduce is
   ordered after the D2H copy and before the H2D copy on the CUDA stream.
5. Config in `ec_cuda.c`: `USE_HOST_REDUCE`, `REDUCE_HOST_LIMIT`
   (`UCC_CONFIG_TYPE_MEMUNITS`), staging pool size.

### Benchmarks
- Extended `test/gtest/core/test_ec_cuda.cc`: B1 (host vs GPU parity).
- `ucc_perftest` `reduce`/`allreduce`, `-m cuda` and `-m cuda_managed`, size
  sweep; B3 vs baseline, B4 via log/counter.
- `nsys`/`ncu` profile of the offload-eligible run: B2.

### Evaluation
**Pass** = B1 exact, B2 SM time ≈ 0 for the reduce portion, B3 within 10% at
every eligible size, B4 selecting host only where B3 holds. If B3 misses for
device-resident buffers (PCIe round-trip dominates), narrow the policy to
managed/zero-copy buffers and re-evaluate — this is an acceptable, explicitly
documented scope for the first cut.

### Commit directive
```
git add src/components/ec/cuda src/components/ec/rocm test/gtest/core && \
git commit -m "EC/CUDA: host-offload reduce via threaded CPU executor"
```

---

## Milestone C — BlueField/DPU reduction offload (SM → DPU ARM cores)

### Goal
Execute reduction on DPU ARM cores (near the NIC), freeing both SMs and host
CPU, reusing the Milestone-D kernel and Milestone-A pool on the DPU.

### Success criteria (measurable)
- **C1 correctness**: DPU-reduced result matches the host/GPU reference across
  the dt×op matrix (bitwise for int/uint, fp tolerance otherwise).
- **C2 SM+host minimization**: `nsys` shows ~0 reduce SM time, and host CPU
  time in `ucc_ec_cpu_reduce` for the offloaded portion drops to ~0 (work moved
  to DPU).
- **C3 end-to-end**: `ucc_perftest` `reduce`/`allreduce` at DPU-eligible sizes
  is within 15% of baseline, and DPU-side reduce throughput (measured on the
  DPU via the Milestone-A pool) is ≥ 0.6× host-socket throughput per core.
- **C4 integration**: offload is opt-in (`USE_DOCA_REDUCE=1`) and falls back
  cleanly to Milestone B host-offload when DOCA is unavailable.

### Features
1. Add an `ec/doca` executor backend (or extend `cl/doca_urom`) that posts
   reduce tasks to the DPU over the DOCA command channel. Reuse the existing
   `contrib/doca_urom_ucc_plugin/dpu/worker_ucc.c` command/queue infrastructure;
   the DPU worker executes `ucc_ec_cpu_reduce_threaded` (Milestone D) on its
   ARM cores via the Milestone-A pool.
2. Wire `ucc_ee_type_t` (`src/ucc/api/ucc.h`) and the memory-type→executor map
   (`src/core/ucc_coll.c`) so DOCA-capable GPU buffers can select the DPU
   executor; keep `UCC_EE_LAST`/array sizing consistent.
3. Data movement: stage D2H/H2D (or DOCA memcpy) with CUDA-event fencing
   (reuse Milestone B staging/fencing).
4. Config: `USE_DOCA_REDUCE`, `DOCA_REDUCE_LIMIT`, DPU worker count/affinity
   (note `origin/topic/doca_urom_threads_fix` — do not re-enable broken core
   pinning; use the Milestone-A pinning helpers instead).
5. Fallback: on DOCA init failure or unsupported dt/op, route to the
   Milestone-B host path.

### Benchmarks
- DPU-side: run the Milestone-A pool microbenchmark on the DPU (build UCC for
  aarch64; op `reducedt` `-m cpu`) for C3 throughput.
- Host-side: `ucc_perftest` `reduce`/`allreduce` with `USE_DOCA_REDUCE=1` vs
  baseline (C3), `nsys` (C2), correctness parity vs host reference (C1).

### Evaluation
**Pass** = C1 exact, C2 ≈ 0 SM + ≈ 0 host reduce CPU, C3 within 15% end-to-end
and ≥ 0.6×/core DPU throughput, C4 clean fallback. If C3 misses due to DOCA
command-channel latency, document the crossover size and restrict the policy to
sizes above it (same discipline as B3).

### Commit directive
```
git add src/components/ec/doca src/components/cl/doca_urom src/core src/ucc/api contrib && \
git commit -m "EC/DOCA: BlueField reduction offload"
```

---

## Cross-milestone invariants

- Every milestone keeps the build green with `--enable-ec-threaded-reduce`
  **disabled** (no behavior change unless opted in).
- Correctness is always judged against the pre-D scalar reference; no milestone
  may silently change numeric results.
- "Match or beat" means: meet the stated threshold, or narrow scope with an
  explicit, committed rationale — never weaken a criterion to pass.
- Commit at every milestone boundary (directive above) before starting the next.
