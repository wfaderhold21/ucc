# Plan: One-sided (RMA) algorithms for TL/UCP

**Audience:** the implementing model (qwen3.8-27b). This document is prescriptive.
Follow the phases in order. Do not skip Phase 0. Every phase ends with a build +
test gate that must pass before the next phase starts.

**Goal:** add efficient one-sided (put/get/atomic) algorithms for the collective
types in `src/components/tl/ucp` that currently have none.

**Current state (verified by inspection):**

| Coll type        | one-sided alg today | file |
|------------------|---------------------|------|
| alltoall         | YES (`onesided`, put+get+auto, token-paced) | `alltoall/alltoall_onesided.c` |
| alltoallv        | YES (`onesided`, put + atomic signal)       | `alltoallv/alltoallv_onesided.c` |
| allreduce        | PARTIAL (`sliding_window`, put-based, DPU-oriented) | `allreduce/allreduce_sliding_window.c` |
| allgather        | **no** | |
| allgatherv       | **no** | |
| bcast            | **no** | |
| barrier          | **no** | |
| scatter          | **no** | |
| scatterv         | **no** | |
| gather           | **no** | |
| gatherv          | **no** | |
| reduce           | **no** | |
| reduce_scatter   | **no** | |
| reduce_scatterv  | **no** | |
| fanin / fanout   | **no** | |

---

## 0. Required reading before writing any code

Read these files completely, in this order. They define every pattern you must copy.

1. `src/components/tl/ucp/tl_ucp_sendrecv.h` — lines 377–740: `ucc_tl_ucp_get_memh`,
   `ucc_tl_ucp_check_memh`, `ucc_tl_ucp_resolve_p2p_by_va`, `ucc_tl_ucp_flush`,
   `ucc_tl_ucp_ep_flush`, `ucc_tl_ucp_put_nb`, `ucc_tl_ucp_get_nb`,
   `ucc_tl_ucp_atomic_inc`, `UCPCHECK_GOTO`.
2. `src/components/tl/ucp/tl_ucp_coll.h` — lines 202–228:
   `UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE`, `UCC_TL_UCP_TASK_ONESIDED_SYNC_COMPLETE`,
   `ucc_tl_ucp_test_onesided`.
3. `src/components/tl/ucp/alltoallv/alltoallv_onesided.c` — the *simple* template
   (start posts all RMA, progress polls, single sync counter).
4. `src/components/tl/ucp/alltoall/alltoall_onesided.c` — the *advanced* template
   (schedule + trailing barrier, put/get variants, token-based pacing derived from
   `ucp_ep_evaluate_perf`, resumable progress function).
5. `src/components/tl/ucp/tl_ucp_task.h` — the `ucc_tl_ucp_task_t` union
   (`onesided` counters at line ~50, per-algorithm state structs, `flush_posted`).
6. `src/components/tl/ucp/tl_ucp_coll.c` — `ucc_tl_ucp_alg_id_to_init()` dispatch.
7. `src/components/tl/ucp/alltoall/alltoall.c` — the `ucc_base_coll_alg_info_t`
   algorithm table + default-score-string function.
8. `src/components/tl/ucp/bcast/bcast_knomial.c` and
   `src/components/tl/ucp/coll_patterns/recursive_knomial.h` — the k-nomial pattern
   helpers you will reuse for tree-shaped one-sided algorithms.

---

## 1. The one-sided contract in this codebase (invariants — memorize these)

These are non-negotiable properties of TL/UCP RMA. Violating any of them produces
silent data corruption, not a compile error.

**I1 — Symmetric offsets.** `ucc_tl_ucp_resolve_p2p_by_va()` translates a *local*
virtual address into a remote one by `rva = remote_base + (local_va - local_base)`
(segment mode, `tl_ucp_sendrecv.h:520`) or, in global-memh mode,
`rva = dst_memh[peer]->address + (va - dst_memh[me]->address)`
(`ucc_tl_ucp_check_memh`, `tl_ucp_sendrecv.h:410`). **Consequence:** every
participating buffer must sit at the same offset within each rank's registered
segment / mem handle. All new algorithms inherit this restriction. Document it in
each new file's header comment. Do not attempt to relax it.

**I2 — Two addressing modes.** A one-sided op works if *either*
(a) the buffer lies inside a context segment registered via `ucc_context_params.mem_params`
(the SHMEM-style symmetric heap; `ctx->remote_info`, `team->va_base[]`), *or*
(b) the caller passed global memory handles (`args.dst_memh.global_memh`, an array
indexed by team rank) together with `UCC_COLL_ARGS_FLAG_DST_MEMH_GLOBAL`.
Mode (b) is required whenever the target buffer is a plain user buffer.
Follow `ucc_tl_ucp_alltoall_onesided_init()` (`alltoall_onesided.c:205–245`) for the
exact validation sequence.

**I3 — Argument gating.** A one-sided init **must** return `UCC_ERR_NOT_SUPPORTED`
(never an assert, never a silent wrong answer) when its preconditions are unmet:
missing `UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS`, missing global work buffer, missing
or non-global memh, in-place, user-defined datatype. The core falls back to another
TL/algorithm on `UCC_ERR_NOT_SUPPORTED` — see `ucc_coll_init()`,
`src/coll_score/ucc_coll_score_map.c:136–147`. This is the mechanism that makes
one-sided algorithms safe to register.

**I4 — Ordering: data before flag.** UCX does **not** guarantee that a `ucp_put_nbx`
is visible to the target before a later `ucp_atomic_op_nbx` on the same endpoint
completes. You must `ucc_tl_ucp_ep_flush(peer, ...)` (or a completed put callback)
between the data put and the signal. `alltoallv_onesided.c:52–58` puts then
immediately signals **without** a flush — do **not** copy that. Phase 0 provides
`ucc_tl_ucp_put_signal()` which does put → ep_flush → atomic add, and every new
algorithm uses it.

**I5 — Completion accounting.** `put_posted/put_completed`, `get_posted/get_completed`
and `flush_posted/flush_completed` are bumped by the helpers and their callbacks.
Local completion of a *put* means "source buffer reusable", **not** "data landed".
Remote visibility requires a flush. A *get* completing means the data is in the local
destination. `UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE()` covers all three counter pairs.

**I6 — `ucc_tl_ucp_task_reset()` zeroes the one-sided counters only by union
aliasing** (`tl_ucp_task.h:40–56`: `tagged.{send,recv}_{posted,completed}` occupy the
same four words as `onesided.{put,get}_{posted,completed}`). This works today. In
Phase 0 make it explicit so future field additions cannot break it silently.

**I7 — Sync-counter reuse is racy today.** `alltoallv_onesided_progress()` writes
`pSync[0] = 0` after the collective (`alltoallv_onesided.c:75`). A fast peer that has
already entered the *next* collective can increment the counter before that reset,
and the signal is lost. **Never reset a signal counter.** Phase 0 replaces this with
monotonically increasing expected values kept per team.

**I8 — Local copies use MC/EE, not RMA to self.** For rooted collectives the root's
own contribution is a local copy: use `ucc_mc_memcpy()` / the executor path, as in
the two-sided algorithms. Do not put to self.

---

## 2. Validation first (do this before Phase 0 code)

Per project rule: tests and validation are designed before implementation.

### 2.1 What already exists

- **gtest:** `test/gtest/coll/test_alltoall.cc` — `UCC_TEST_P(test_alltoall_0, single_onesided)`
  (line 213), with `data_init` taking a team whose `procs[i].p->onesided_buf[0..2]`
  are three registered segments (buf 0 = src, 1 = dst, 2 = work buffer), and
  `data_fini_onesided()` (line 135). Segment setup:
  `test/gtest/common/test_ucc.cc:440–450`; team created with
  `UCC_TEAM_FLAG_COLL_WORK_BUFFER` (line 201).
- **MPI test:** `test/mpi/test_alltoall.cc` (`is_onesided = (params.buffers != nullptr)`,
  line 20; sets `UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS`, line 50), driven by
  `test/mpi/test_mpi.cc` (`onesided_buffers[3]`, line 75; `onesided_teams`).
- **perftest:** `tools/perf/ucc_pt_coll_alltoall.cc:45–130` shows both the local-memh
  and global-memh (export + bcast + import) setups; `ucc_pt_comm.cc:141–190` allocates
  and registers the one-sided buffer.

### 2.2 What to add, per new algorithm (write the test first)

For each collective X you implement, in this order:

1. **gtest correctness case** in `test/gtest/coll/test_X.cc`: copy the
   `single_onesided` case from `test_alltoall.cc`, plus a `multiple_onesided` variant
   that runs the collective **twice back-to-back on the same team without an
   intervening barrier** — this is the case that catches the I7 counter-reuse bug.
   Cover team sizes {1, 2, 3, 4, 8, 15, 16} (non-power-of-2 sizes are mandatory:
   tree/dissemination algorithms break there first).
2. **Negative cases:** no `MEM_MAPPED_BUFFERS` flag → expect the collective to still
   complete (fallback path), and `init` to return `UCC_ERR_NOT_SUPPORTED` when called
   directly. In-place → `UCC_ERR_NOT_SUPPORTED`.
3. **MPI test** wiring in `test/mpi/test_X.cc` mirroring `test_alltoall.cc`'s
   `is_onesided` branch, including the `persistent` path (line 91).
4. **perftest** wiring in `tools/perf/ucc_pt_coll_X.cc` mirroring
   `ucc_pt_coll_alltoall.cc`, so the algorithm can be benchmarked against the
   two-sided baseline.

### 2.3 Gates (run for every phase)

- Build: `/build-hpcac-internal` (or `/build-ryzen-ucc-nocuda`).
- `/valgrind-ryzen-ucc` (ASan+LSan gtest run) — one-sided code registers memory and
  unpacks rkeys; leaks here are easy and invisible otherwise.
- Selection smoke test:
  `UCC_TL_UCP_TUNE=X:@onesided:inf ucc_perftest -c X -b <sz> -e <sz> -m ...`
  must actually pick the new algorithm (check with `UCC_LOG_LEVEL=debug`).
- Performance gate: new algorithm must beat the best two-sided algorithm for the
  message-size range it claims, at ≥2 team sizes, or it does not get a default score
  entry.

---

## 3. Phase 0 — shared one-sided infrastructure (blocking prerequisite)

Create `src/components/tl/ucp/tl_ucp_onesided.h` and `tl_ucp_onesided.c`.
Add both to `src/components/tl/ucp/Makefile.am` in the top-level `sources` list
(same place `tl_ucp_sendrecv.h` is listed).

### 3.1 Sync-buffer layout and slot allocation

Today: `ONESIDED_SYNC_SIZE 1`, `ONESIDED_REDUCE_SIZE 4` (`tl_ucp.h:37–38`), reported
to the user by `ucc_tl_ucp_context.c:1023–1026`. One counter is not enough for
dissemination barriers (needs `ceil(log2(N))` counters) or multi-phase algorithms.

Do this:

```c
/* tl_ucp.h */
#define UCC_TL_UCP_ONESIDED_N_SLOTS  32   /* >= ceil(log2(max supported ranks)) */
#define ONESIDED_SYNC_SIZE           UCC_TL_UCP_ONESIDED_N_SLOTS
#define ONESIDED_REDUCE_SIZE         4
```

and in `ucc_tl_ucp_context.c` report the size **in bytes**, with a comment stating the
unit (today the unit is undefined; tests over-allocate, so this is safe to fix):

```c
attr->attr.global_work_buffer_size =
    (ONESIDED_SYNC_SIZE + ONESIDED_REDUCE_SIZE) * sizeof(long);
```

Slot addressing helper (offset within the symmetric work buffer is identical on every
rank, so a local slot pointer is a valid RMA target under I1):

```c
static inline long *ucc_tl_ucp_onesided_slot(ucc_tl_ucp_task_t *task, int slot)
{
    return &((long *)TASK_ARGS(task).global_work_buffer)[slot];
}
```

**Monotonic expectations (fixes I7).** Add to `ucc_tl_ucp_team_t` (`tl_ucp.h`):

```c
long onesided_slot_base[UCC_TL_UCP_ONESIDED_N_SLOTS]; /* zero-initialized in team init */
```

A task that expects `k` signals on slot `s` computes
`expected = team->onesided_slot_base[s] + k` at start, waits for `*slot >= expected`,
and on completion sets `team->onesided_slot_base[s] = expected`. Counters are never
written back to zero. Store `expected` in the task's per-algorithm state struct.

### 3.2 Primitives

```c
/* atomic add of `value` into the peer's copy of the counter that lives at the same
 * symmetric offset as `local_slot`. Generalization of ucc_tl_ucp_atomic_inc(). */
ucc_status_t ucc_tl_ucp_atomic_add(long *local_slot, long value, ucc_rank_t peer,
                                   ucc_mem_map_mem_h *memh,
                                   ucc_tl_ucp_team_t *team,
                                   ucc_tl_ucp_task_t *task);

/* THE ordering-safe publish: put -> ep_flush -> atomic_add(1) on peer's `slot`.
 * Use this everywhere a peer must observe data. Never open-code put+signal. */
ucc_status_t ucc_tl_ucp_put_signal(void *src, void *dst, size_t len,
                                   ucc_memory_type_t mtype, ucc_rank_t peer,
                                   long *slot, ucc_mem_map_mem_h src_memh,
                                   ucc_mem_map_mem_h *dst_memh,
                                   ucc_tl_ucp_team_t *team,
                                   ucc_tl_ucp_task_t *task);

/* bounded-poll test: local RMA complete AND *slot >= expected */
static inline ucc_status_t
ucc_tl_ucp_test_onesided_slot(ucc_tl_ucp_task_t *task, long *slot, long expected);
```

Implement `ucc_tl_ucp_atomic_add` by generalizing `ucc_tl_ucp_atomic_inc`
(`tl_ucp_sendrecv.h:695–735`): same body, `value` instead of the hardcoded `one`, plus
the `task` argument so the request is accounted rather than freed inline. Keep
`ucc_tl_ucp_atomic_inc` as a thin wrapper so `alltoallv_onesided.c` keeps compiling.

`ucc_tl_ucp_test_onesided_slot` is `ucc_tl_ucp_test_onesided`
(`tl_ucp_coll.h:210–227`) with `UCC_TL_UCP_TASK_ONESIDED_SYNC_COMPLETE` replaced by
`(*slot >= expected)` (`>=`, not `==`, so that a peer racing ahead into the next
collective cannot deadlock this one).

### 3.3 Shared argument validation

Replace the copy-pasted validation blocks in `alltoall_onesided.c:205–245` and
`alltoallv_onesided.c:88–108` with one function:

```c
typedef enum {
    UCC_TL_UCP_ONESIDED_REQ_GWB        = UCC_BIT(0), /* needs global work buffer   */
    UCC_TL_UCP_ONESIDED_REQ_SRC_GLOBAL = UCC_BIT(1), /* needs global src memh      */
    UCC_TL_UCP_ONESIDED_REQ_DST_GLOBAL = UCC_BIT(2), /* needs global dst memh      */
} ucc_tl_ucp_onesided_req_t;

/* Returns UCC_ERR_NOT_SUPPORTED (with a tl_debug, not tl_error, for the
 * fallback-expected cases) when preconditions are unmet. Also normalizes
 * absent memh fields to NULL, exactly as the two existing inits do. */
ucc_status_t ucc_tl_ucp_onesided_check_args(ucc_base_coll_args_t *coll_args,
                                            ucc_tl_ucp_team_t *team,
                                            uint64_t reqs);
```

Note the log-level change: an unmet precondition is a *normal* fallback, so use
`tl_debug`, not `tl_error`. The existing code logs errors on this path and is noisy.

### 3.4 Pacing / flow-control helper

`alltoall_onesided.c` computes a token budget from `ucp_ep_evaluate_perf` and a
`percent_bw` config knob (lines 265–290), plus a shared
`alltoall_onesided_handle_completion()` window check (lines 17–36). Extract, verbatim
in behavior, into:

```c
typedef struct ucc_tl_ucp_onesided_window {
    uint32_t tokens;   /* max outstanding RMA ops */
    int64_t  npolls;   /* progress polls before yielding back to the queue */
} ucc_tl_ucp_onesided_window_t;

ucc_status_t ucc_tl_ucp_onesided_window_init(ucc_tl_ucp_task_t *task,
                                             size_t msg_size,
                                             ucc_rank_t concurrency,
                                             ucc_tl_ucp_onesided_window_t *win);

/* returns 1 = keep posting, 0 = yield (task stays UCC_INPROGRESS) */
static inline int
ucc_tl_ucp_onesided_window_check(ucc_tl_ucp_task_t *task, uint32_t *posted,
                                 uint32_t *completed,
                                 const ucc_tl_ucp_onesided_window_t *win);
```

`concurrency` is the number of processes per node sharing the NIC (from
`ucc_topo_get_sbgp(team->topo, UCC_SBGP_NODE)->group_size`, as alltoall does).
Every linear (O(N)-message) algorithm below **must** use this window. An unthrottled
N-way put storm is the single most common way one-sided collectives lose to two-sided
ones at scale.

Add a config knob per new linear algorithm family mirroring
`UCC_TL_UCP_ALLTOALL_ONESIDED_PERCENT_BW` (`tl_ucp.c:105–113`), or a single shared
`UCC_TL_UCP_ONESIDED_PERCENT_BW` used as the default. Prefer the shared knob.

### 3.5 Resumable progress

Linear one-sided algorithms must not post all N ops in `start()` if N is large;
they post inside `progress()` and resume from `posted` (this is exactly what
`ucc_tl_ucp_alltoall_onesided_put_progress()` does — peer is recomputed as
`(grank + posted + 1) % gsize`). Copy that structure. Multi-phase (tree,
recursive-doubling) algorithms keep an explicit `phase` field in their task state and
use the `SAVE_STATE`/re-enter idiom from the two-sided k-nomial algorithms.

### 3.6 Phase 0 exit criteria

- `alltoall_onesided.c` and `alltoallv_onesided.c` refactored onto the new helpers,
  with `alltoallv` now flush-ordered (I4) and using monotonic slots (I7).
- Existing `single_onesided` gtests still pass; new back-to-back
  `multiple_onesided` alltoallv test passes (it would have failed before).
- No behavior change in alltoall performance (measure before/after).

---

## 4. Phase 1 — counter-signal collectives (simplest; establishes the template)

All of these move data in a single phase and synchronize with one counter slot.
Implement in this order; each is a small variation of the previous.

### 4.1 `fanin` / `fanout` — one-sided (warm-up, no data movement)

- `fanout_onesided`: root does `ucc_tl_ucp_atomic_add(slot, 1, peer)` to every peer;
  peers wait `*slot >= expected`. Root completes immediately after posting.
- `fanin_onesided`: every non-root atomic-adds to the root's slot; root waits for
  `expected = base + (size - 1)`.
- New files `fanin/fanin_onesided.c`, `fanout/fanout_onesided.c`; add
  `UCC_TL_UCP_FANIN_ALG_ONESIDED` / `..._FANOUT_ALG_ONESIDED` enums.
- Note: fanin/fanout currently have no alg dispatch entry in
  `ucc_tl_ucp_alg_id_to_init()`. Add the `case UCC_COLL_TYPE_FANIN/FANOUT:` blocks.

### 4.2 `barrier_onesided` — dissemination with atomic counters

Highest leverage item in Phase 1: it removes tag matching from the sync path and lets
`alltoall_onesided` drop its trailing two-sided barrier task.

```
rounds = ceil(log2(size))
for r in 0 .. rounds-1:
    peer = (rank + 2^r) % size
    atomic_add(slot[r], 1, peer)          /* no data => no flush needed */
    wait until *slot[r] >= base[r] + 1
    base[r] += 1                          /* commit at task completion */
```

- Requires `UCC_TL_UCP_ONESIDED_N_SLOTS >= ceil(log2(size))`; if the team is larger
  than `2^UCC_TL_UCP_ONESIDED_N_SLOTS`, return `UCC_ERR_NOT_SUPPORTED`.
- Progress is resumable: keep `round` in `task->barrier_onesided.round`.
- Correct for non-power-of-2 sizes as written; add gtest sizes 3, 5, 7, 15.
- Once green, add a `barrier` variant selectable inside `alltoall_onesided_init()`
  instead of `ucc_tl_ucp_coll_init(&barrier_coll_args, ...)` (`alltoall_onesided.c:250`),
  gated by the same mem-mapped preconditions.

### 4.3 `scatter_onesided` — root-driven put

```
if rank == root:
    for peer in (root+1 .. root+size-1) mod size:
        put_signal(src + peer*blk, dst, blk, peer, slot)      /* dst is symmetric */
    local copy src + root*blk -> dst                          /* I8 */
else:
    wait *slot >= base + 1
```

- `blk = args.src.info.count / size * dt_size` at the root; non-roots use
  `args.dst.info.count * dt_size`. Validate they agree via the standard checks.
- Uses the pacing window (root posts N-1 puts).
- Requires global dst memh (`REQ_DST_GLOBAL`) or symmetric-segment dst.

### 4.4 `gather_onesided` — leaf-driven put

```
if rank != root:
    put_signal(src, dst + rank*blk, blk, root, slot)   /* dst VA is the LOCAL dst VA,
                                                          translated by I1 */
else:
    local copy src -> dst + root*blk
    wait *slot >= base + (size - 1)
```

- Careful: non-root ranks must compute the target address as *their own* `dst` VA plus
  `rank*blk`. Under I1 that resolves to the root's dst buffer at the same offset. This
  only works when every rank's `dst` buffer is registered at the same symmetric offset
  — for gather, non-roots normally pass `dst = NULL`. **Therefore:** in this algorithm
  require that all ranks pass a registered `dst` buffer of full size (state this in the
  init's precondition check and return `UCC_ERR_NOT_SUPPORTED` otherwise), *or* use the
  global-memh mode where `dst_memh[root]->address` is directly available. Prefer the
  global-memh mode: it is the mode `ucc_pt` and the MPI tests already exercise.
- Provide the root-driven `get` variant too (`gather_onesided_get`): root issues
  `size-1` gets from peers' `src` after a fanin. Better when the root's NIC is the
  bottleneck for injection rather than for bandwidth; it also does not need the
  all-ranks-registered-dst restriction.

### 4.5 Phase 1 exit criteria

Correctness gtests green for sizes {1,2,3,4,8,15,16}, ASan clean, and
`barrier_onesided` measurably faster than `barrier_knomial` at ≥ 8 ranks.

---

## 5. Phase 2 — vector and tree collectives

### 5.1 `allgather_onesided` (two variants + auto) — highest value item

Mirror `alltoall_onesided`'s structure exactly, including the put/get/auto config
enum (`ucc_tl_ucp_alltoall_onesided_alg_type`, `tl_ucp.h:46–51`) and the
`CONGESTION_THRESHOLD`-style ppn heuristic (`alltoall_onesided.c:14`).

- **put variant:** each rank puts its own block into every peer's `dst` at offset
  `grank*blk`, then signals. Wait for `size-1` signals. N-1 messages of `blk` bytes.
- **get variant:** entry sync (one-sided barrier from 4.2 — the source data must be
  readable), then each rank gets peer `p`'s block from `p`'s `src`/`dst` into
  `dst + p*blk`. No signals needed on the data path: a completed get means the data
  arrived. Wins under NIC congestion at high ppn, same as for alltoall.
- **auto:** `get` when `ppn >= CONGESTION_THRESHOLD`, else `put`.
- Both variants use the Phase 0 pacing window with `concurrency = ppn`.
- Local block: `ucc_mc_memcpy` from `src` to `dst + grank*blk` (skip when in-place).
- In-place *is* supportable here (source is `dst + grank*blk`); implement it, since
  in-place allgather is the common MPI usage.

**Optional follow-up (only after the linear variants are green):** a
recursive-doubling put+signal variant, `allgather_onesided_rd`: `log2(N)` steps,
step `s` exchanges `2^s * blk` bytes with `rank ^ 2^s`, signalling on
`slot[s]`. Total bytes moved equals the linear variant but message count drops from
`N-1` to `log2(N)`. Non-power-of-2 handling is the hard part — restrict to power-of-2
team sizes and return `UCC_ERR_NOT_SUPPORTED` otherwise rather than writing the
general form.

### 5.2 `allgatherv_onesided` — get-based

In allgatherv **all** ranks hold the full counts/displacements arrays, so a get-based
formulation needs no metadata exchange:

```
entry sync (one-sided barrier)
for p in (rank+1 .. rank+size-1) mod size:
    get(dst + displs[p]*rdt, peer_src_of_p, counts[p]*rdt, p)
local copy src -> dst + displs[rank]*rdt
```

Use `ucc_coll_args_get_count()` / `ucc_coll_args_get_displacement()` (see
`alltoallv_onesided.c:40–50`) so both 32- and 64-bit count modes work.
Also provide the put variant for symmetry (put own block into each peer at
`displs[rank]`, then signal).

### 5.3 `scatterv_onesided` / `gatherv_onesided`

- `scatterv_onesided`: root-driven put; root has counts/displs. The **destination**
  offset within a peer's dst buffer is the peer's local base (offset 0), so this is a
  direct extension of 4.3.
- `gatherv_onesided`: **must be root-driven `get`** — only the root knows the
  destination displacements, and leaves cannot compute where to put. Sequence:
  fanin (leaves signal "my src is ready"), then root issues `size-1` gets into
  `dst + displs[p]`. This is why gatherv gets the get treatment while scatterv gets put.

### 5.4 `bcast_onesided`

Two algorithms:

- **`bcast_onesided_linear` (get-based):** root signals all peers (or a one-sided
  fanout), each non-root gets the whole buffer from the root. Root-bandwidth bound —
  only register a default score for small teams.
- **`bcast_onesided_knomial` (put + signal):** the one to optimize.
  Reuse `ucc_knomial_pattern_init/next` from
  `coll_patterns/recursive_knomial.h` exactly as `bcast/bcast_knomial.c` does, but
  replace each send with `ucc_tl_ucp_put_signal(..., slot[level])` and each recv with
  a wait on `slot[level]`. Latency `log_k(N)`, no root bottleneck.
  Keep `phase` and the `ucc_knomial_pattern_t` in a
  `task->bcast_onesided` state struct; the progress function must be resumable
  (a node cannot post to its children until its parent's signal has arrived).
  Radix from `UCC_TL_UCP_BCAST_KN_RADIX` via `ucc_tl_ucp_get_radix_from_range()`.

---

## 6. Phase 3 — reduction collectives (requires new scratch infrastructure)

Reductions need a **remotely writable scratch buffer** to land peer contributions
before combining them locally. The existing `ONESIDED_REDUCE_SIZE` area is 4 longs —
useless for this. Do not attempt Phase 3 before 6.1 is done and reviewed.

### 6.1 Internal registered scratch segment (infra)

Today `ctx->remote_info[]` segments come only from `ucc_context_params.mem_params`
(`ucc_tl_ucp_context.c:547–640`), and the per-peer remote base addresses are exchanged
through the packed EP address (`ucc_tl_ucp_ctx_remote_pack_data`, line ~642). So bases
need not match across ranks — only *offsets within a segment* must (I1), which an
internally allocated, uniformly sized scratch segment satisfies by construction.

Work items:
1. New config knob `UCC_TL_UCP_ONESIDED_SCRATCH_SIZE` (default e.g. 8 MiB, 0 = disabled).
2. At context init, when the knob is non-zero, `ucc_malloc` + `ucp_mem_map` +
   `ucp_rkey_pack` one extra segment and append it to `ctx->remote_info`, bumping
   `ctx->n_rinfo_segs`. It must be the **last** segment so user segment indices are
   unchanged. Assert `n_rinfo_segs <= MAX_NR_SEGMENTS` (32, `tl_ucp.h:36`).
3. Expose `ucc_tl_ucp_onesided_scratch(ctx, size, &ptr)` — a bump/slot allocator over
   that segment, returning the **same offset on every rank** (allocate by a
   deterministic per-team, per-collective slot index, not by a local free-list, or I1
   breaks).
4. If the knob is 0 or the request exceeds the segment, reduction inits return
   `UCC_ERR_NOT_SUPPORTED`.
5. Because `resolve_p2p_by_va()` linear-scans all segments per RMA op
   (`tl_ucp_sendrecv.h:483–495`), adding a segment slightly raises per-op cost for
   everyone. Fold in the fix noted in `ucc-opt.md` §3 (cache the last-hit segment index
   on the task, check it first) as part of this phase.

### 6.2 `reduce_scatter_onesided` — direct, one phase

```
scratch = N blocks of blk bytes (symmetric offset)
for p != rank:
    put_signal(src + p*blk, scratch + rank*blk, blk, peer=p, slot)
wait *slot >= base + (size-1)
local reduce: dst = src[rank*blk] (+) scratch[0..N-1 except rank]
```

Traffic per rank `(N-1)*blk ≈ message_size` — bandwidth-optimal, single phase,
latency `O(1)`. Scratch `N*blk` — cap it: when `N*blk` exceeds the scratch segment,
return `UCC_ERR_NOT_SUPPORTED` and let the ring/knomial algorithms handle it.
Use the executor (`ucc_ee_executor_task_post` with `UCC_EE_EXECUTOR_TASK_REDUCE_MULTI`)
for the local combine, and the **non-blocking** `EXEC_TASK_TEST` + `SAVE_STATE` idiom,
not `EXEC_TASK_WAIT` (see `ucc-opt.md` §2 — `EXEC_TASK_WAIT` blocks the progress queue).

### 6.3 `reduce_scatterv_onesided`

Same as 6.2 with per-block counts/displacements from the args vectors. Peers can
compute each other's block offsets because reduce_scatterv counts are known to all
ranks.

### 6.4 `reduce_onesided` — k-nomial tree, put + signal

Children put their (partially reduced) data into the parent's scratch slot and signal;
the parent waits for its `k-1` children per level, reduces, and moves up. Reuse the
k-nomial pattern from `reduce/reduce_knomial.c`; replace send/recv with
`put_signal`/slot wait. Scratch per node: `(radix-1) * count * dt_size`.

### 6.5 `allreduce_onesided`

Two variants, complementary to the existing `sliding_window`:

- **`allreduce_onesided_rd` (recursive doubling)** for small/medium messages:
  `log2(N)` steps; step `s`: put the local accumulator into `rank ^ 2^s`'s scratch,
  signal `slot[s]`, wait, reduce into the accumulator. Restrict to power-of-2 sizes
  (return `UCC_ERR_NOT_SUPPORTED` otherwise) unless the two-sided
  `allreduce_sra_knomial` remainder handling is replicated faithfully.
- **`allreduce_onesided_rs_ag`** for large messages: `reduce_scatter_onesided` (6.2)
  followed by `allgather_onesided` (5.1), built as a `ucc_schedule_t` with two tasks
  and `ucc_task_subscribe_dep(..., UCC_EVENT_COMPLETED)` — exactly the schedule
  pattern in `alltoall_onesided_init()` (lines 246–300). This is the bandwidth-optimal
  one (`2*(N-1)/N * size` bytes) and should become the default one-sided allreduce
  above the sliding-window crossover.

---

## 7. Per-algorithm registration checklist (repeat verbatim for every algorithm)

For collective `X` and algorithm `onesided`:

1. **`X/X_onesided.c`** — new file. Copyright header
   `Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.`,
   then `#include "config.h"`, `"tl_ucp.h"`, `"X.h"`, `"core/ucc_progress_queue.h"`,
   `"utils/ucc_math.h"`, `"tl_ucp_sendrecv.h"`, `"tl_ucp_onesided.h"`.
   Define `ucc_tl_ucp_X_onesided_{init,start,progress,finalize}`.
2. **`X/X.h`** — add `UCC_TL_UCP_X_ALG_ONESIDED` to the enum **before** `_ALG_LAST`
   (never renumber existing entries), and declare the init prototype.
3. **`X/X.c`** — add the `ucc_base_coll_alg_info_t` table entry
   (`.id`, `.name = "onesided"`, `.desc = "..."`) at the new enum index.
   The table is sized `[UCC_TL_UCP_X_ALG_LAST + 1]`; the `_ALG_LAST` sentinel entry
   must stay last.
4. **`tl_ucp_coll.c`** — add `case UCC_TL_UCP_X_ALG_ONESIDED: *init = ...; break;`
   in `ucc_tl_ucp_alg_id_to_init()`. For fanin/fanout/gather/scatter, the
   `case UCC_COLL_TYPE_*` block itself may not exist yet — add it.
5. **`Makefile.am`** — add `X/X_onesided.c` to the `X = ...` variable.
6. **`tl_ucp_task.h`** — add the algorithm's state struct to the big union
   (`{ int phase; long expected; ucc_knomial_pattern_t p; ... } X_onesided;`).
   Union members are free; do not add fields outside the union.
7. **Do not change the default score string** for `X`. One-sided requires
   mem-mapped buffers, so it must stay opt-in via
   `UCC_TL_UCP_TUNE="X:@onesided:inf"` until the Phase-N performance gate is met.
   When it is met, extend `X_score_str_get()` / `UCC_TL_UCP_X_DEFAULT_ALG_SELECT_STR`
   and say so in the commit message.
8. **Tests** — §2.2, all four items.
9. **Docs** — add the algorithm to the table in `docs/` if one lists TL/UCP algorithms
   (`grep -rn "bruck" docs/` to check), and to the commit message.

### Skeleton to copy

```c
ucc_status_t ucc_tl_ucp_X_onesided_init(ucc_base_coll_args_t *coll_args,
                                        ucc_base_team_t      *team,
                                        ucc_coll_task_t     **task_h)
{
    ucc_tl_ucp_team_t *tl_team = ucc_derived_of(team, ucc_tl_ucp_team_t);
    ucc_tl_ucp_task_t *task;
    ucc_status_t       status;

    status = ucc_tl_ucp_onesided_check_args(coll_args, tl_team,
                                            UCC_TL_UCP_ONESIDED_REQ_GWB |
                                            UCC_TL_UCP_ONESIDED_REQ_DST_GLOBAL);
    if (UCC_OK != status) {
        return status;                    /* I3: NOT_SUPPORTED => core falls back */
    }
    task = ucc_tl_ucp_init_task(coll_args, team);
    if (ucc_unlikely(!task)) {
        return UCC_ERR_NO_MEMORY;
    }
    /* per-algorithm state init here (window, expected, phase) */
    task->super.post     = ucc_tl_ucp_X_onesided_start;
    task->super.progress = ucc_tl_ucp_X_onesided_progress;
    *task_h              = &task->super;
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_X_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task = ucc_derived_of(ctask, ucc_tl_ucp_task_t);

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    /* compute task->X_onesided.expected from team->onesided_slot_base[slot] here,
     * NOT in init() -- init may run long before post() for persistent collectives */
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(TASK_TEAM(task))->pq,
                                      &task->super);
}
```

---

## 8. Known traps (each of these has bitten the existing code)

1. **Persistent collectives.** `test/mpi/test_alltoall.cc:91` exercises the persistent
   path: `init()` runs once, `post()` many times. All sequence-dependent state
   (expected counter values, `posted`/`completed`, `phase`) must be (re)computed in
   `start()`, never in `init()`.
2. **`args.global_work_buffer` is per-collective-args, not per-team.** Two concurrent
   one-sided collectives on the same team sharing one work buffer will collide on slot
   0. Use distinct slots per collective family, and document the assignment in
   `tl_ucp_onesided.h` as a table (`slot 0: alltoallv, slot 1: scatter/gather,
   slots 2..2+log2(N): barrier/tree levels`, …).
3. **Non-power-of-2 team sizes** break naive dissemination/recursive-doubling. Either
   handle the remainder explicitly or refuse with `UCC_ERR_NOT_SUPPORTED`. Never
   produce wrong results.
4. **`size == 1`.** Every algorithm must short-circuit to a local copy and
   `UCC_OK`, without touching the work buffer.
5. **rkey leaks.** `ucc_tl_ucp_check_memh()` unpacks rkeys lazily into
   `dst_tl_data->rkey` and never re-checks for concurrent unpack. Do not add new
   unpack sites; always go through `resolve_p2p_by_va`.
6. **Do not busy-wait in `progress()`.** Bounded polls (`task->n_polls` /
   `win.npolls`), then return with `UCC_INPROGRESS` so the progress queue can advance
   other tasks. `EXEC_TASK_WAIT` violates this — use `EXEC_TASK_TEST` + `SAVE_STATE`.
7. **Memory type.** Pass `args.{src,dst}.info.mem_type` into the RMA helpers
   (`req_param.memory_type`); a wrong mem_type on a CUDA buffer silently degrades to a
   host-staged path or faults.
8. **`ucc_tl_ucp_ep_flush` accounting.** `flush_posted`/`flush_completed` participate
   in `UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE`. If you post a flush you must let the
   task reach completion through the same predicate; do not mix in raw
   `ucc_tl_ucp_flush()` (worker-wide, unaccounted) on a task path.

---

## 9. Suggested delivery order (one PR per line)

| # | PR | Depends on |
|---|----|-----------|
| 1 | Phase 0 infra + refactor of the two existing one-sided algs + `multiple_onesided` tests | — |
| 2 | `barrier_onesided` (+ use it in `alltoall_onesided`'s schedule) | 1 |
| 3 | `fanin`/`fanout` one-sided | 1 |
| 4 | `scatter_onesided`, `gather_onesided` (put + get) | 1 |
| 5 | `allgather_onesided` (put/get/auto) — the flagship | 1 |
| 6 | `allgatherv_onesided`, `scatterv_onesided`, `gatherv_onesided` | 5 |
| 7 | `bcast_onesided` (linear get + k-nomial put) | 2 |
| 8 | Scratch-segment infra + `resolve_p2p_by_va` segment cache | 1 |
| 9 | `reduce_scatter_onesided`, `reduce_scatterv_onesided` | 8 |
| 10 | `reduce_onesided` | 8 |
| 11 | `allreduce_onesided_rd` + `allreduce_onesided_rs_ag` | 5, 9 |

Each PR: builds clean, ASan clean, gtest + MPI test green at sizes {1,2,3,4,8,15,16},
and carries a `ucc_perftest` before/after table in its description.
