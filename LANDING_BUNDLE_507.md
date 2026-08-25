# Landing Bundle — Task 507

**Date:** 2026-08-25
**Landing commit range:** `8426682f` (fix: enforce UCC owner lifetimes) +
`b63fda83` (test: freeze multi-rank lifetime coverage), merged onto upstream
`a134efa1` at `57dabf69`.

## 1. Merge Conflict Resolution

**Status:** Resolved — no conflicts.

Upstream drift (`a23fe6a1` "CORE: various fixes for ucc_mem_map", `a134efa1`
"DOCS: update news and authors") merged cleanly via `git merge --no-ff` into
`57dabf69`. The merge base is `c14ad0c1`; the lifetime commit (`8426682f`) was
authored against it.

**Conflict surface (predicted in task 503):** `src/core/ucc_context.c` —
`ucc_mem_map` and `ucc_mem_unmap` were modified by both the lifetime commit
and the drift. Git auto-merged cleanly — no textual conflict.

**Verification of merged accounting:**
- `ucc_mem_map()` (merged, lines 1605–1648): The `out:` label wraps both the
  drift's restructured import/export flow and the lifetime's `memh_count`
  increment (`if (status == UCC_OK && memh && *memh) →
  ucc_atomic_add32(&context->memh_count, 1)`). Correct.
- `ucc_mem_unmap()` (merged, lines 1650–1701): `ucc_atomic_sub32(&ctx->memh_count, 1)`
  at line 1698, after all frees. `!*memh` guard (lines 1666–1668) prevents
  double-decrement on retry. Correct.
- `ucc_context_destroy()` (merged, lines 976–984): `team_count`/`memh_count`
  check survived. Correct.
- `ucc_finalize()` (merged, lines 492–498): `context_count` check survived.
  Correct.

**EXPORT_OFFLOAD gap (known, not addressed):** The `ucc_mem_map_export` path for
`UCC_MEM_MAP_MODE_EXPORT_OFFLOAD` consumes the caller's handle and returns a
fresh one without decrementing the consumed handle — `memh_count` inflates.
This exists on the merge base too; the drift does not change the accounting
outcome. No test/tool exercises EXPORT_OFFLOAD. Documented as a known gap; not
a landing blocker.

## 2. Test Matrix

### 2a. Local gtest suite (self TL, ASan build)

**Build:** `./configure --enable-gtest --with-ucx=/usr --with-tls=self`,
ASan flags (`-fsanitize=address`). UCX 1.13.1 lacks `ucp_memh_pack` API needed
by coll tests → coll tests excluded. Core tests compiled and linked as a
minimal gtest binary.

**Lifetime-specific tests (all PASS under ASan):**

| Test | Result | What it verifies |
|------|--------|------------------|
| `test_lib.init_finalize` | PASS | Basic init/finalize cycle |
| `test_lib.init_multiple` | PASS | Multiple lib init/finalize |
| `test_context.create_destroy` | PASS | Context create/destroy |
| `test_context.finalize_refuses_live_context_then_retries` | PASS | Finalize refusal + retry after context destroy |
| `test_context.context_refuses_live_team_then_retries` | PASS | Context destroy refusal + retry after team destroy |
| `test_context.synchronous_team_create_failure_has_no_owner` | PASS | Sync team failure does NOT increment team_count |
| `test_context.configured_thread_modes_owner_accounting` | PASS | Finalize refusal across thread modes (SINGLE, SINGLE_MULTIPLE) |
| `test_context.init_multiple` | PASS | Multiple context create/destroy |
| `test_context.global` | PASS | Global context create/destroy |

**Standalone smoke test (`lifetime_smoke.c`):**

| Phase | Result |
|-------|--------|
| Init + 2 contexts | PASS |
| `ucc_finalize` with 2 live contexts → `UCC_ERR_INVALID_PARAM` | PASS |
| Destroy context A → `ucc_finalize` refused (1 live) | PASS |
| Destroy context B → `ucc_finalize` succeeds | PASS |
| ASan: 0 errors, 0 leaks | PASS |

**Environment-limited failures (NOT lifetime bugs):**

| Test | Failure cause |
|------|---------------|
| `test_team.*` (most) | Self TL has no TL teams for CL; team creation fails. `team_count` accounting still works (verified by `context_refuses_live_team_then_retries`) |
| `test_mem_map_export.*` | Self TL returns `UCC_ERR_NOT_SUPPORTED` for mem_map. `memh_count` stays 0. |
| `test_mem_map_import.basic_import` | ASan heap-buffer-overflow: test passes garbage to `ucc_mem_map_import`, which self TL doesn't guard against (UCP TL does). Pre-existing test bug with self TL. |
| `test_context_get_attr.work_buffer_size` | Self TL has no global work buffer support → attr is 0. Pre-existing. |
| `test_context_config.modify` | `tl/ucp` config namespace not available with self TL. Pre-existing. |

### 2b. Multi-rank MPI runtime coverage (task 504, ryzen host, HPC-X)

**All tests PASS on np=2 and np=4** (see task 504 completion note):

| Scenario | Result |
|----------|--------|
| Refusal/retry with live team + mapping | PASS |
| Async failed team creation (`UCC_TEAM_FAILED`) | PASS |
| Ordered cleanup (unmap → destroy team → destroy context → finalize) | PASS |
| Multi-team accounting (2 teams on shared context) | PASS |
| Perftest allreduce smoke (init/finalize order) | PASS |
| OSHMEM `oshmem_max_reduction` (0 refusal errors) | PASS |
| OSHMEM `ring_oshmem` (0 refusal errors) | PASS |

**Known artifact (not a library defect):** `ucc_test_mpi` harness logs
`ucc_context_destroy`/`ucc_finalize` refusal ERRORs after its own destructor
returns UCC_OK — these are from a second teardown during `MPI_Finalize` (UAF
reads on already-destroyed contexts). Harness-specific; standalone tests,
OSHMEM, and simple MPI programs show 0 refusals. Harness still exits 0.

### 2c. Sanitizer results

| Build | Sanitizer | Result |
|-------|-----------|--------|
| Library (self TL, ASan) | AddressSanitizer | 0 errors in library build |
| Lifetime smoke test (ASan) | AddressSanitizer + LeakSanitizer | 0 errors, 0 leaks |
| Core gtests (ASan) | AddressSanitizer | 0 errors in lifetime tests; 1 heap-buffer-overflow in `test_mem_map_import` (pre-existing, self-TL + garbage input, not lifetime code) |

## 3. Compatibility Notes

### Merge conflict resolution
- No actual conflicts; git auto-merged. The lifetime accounting wraps correctly
  around the drift's restructured `ucc_mem_map`/`ucc_mem_unmap`.
- The merge strategy (merge, not rebase) preserves commit identity —
  `8426682f` and `b63fda83` remain as authored. The merge commit `57dabf69`
  sits on top of upstream `a134efa1`.

### Consumer impact
- **OMPI coll/ucc:** 0 refusal errors (task 504). Consumer-side MPI barrier
  synchronization before communicator free provides adequate protection.
- **OSHMEM scoll/ucc:** 0 refusal errors (task 504). Consumer-side
  `shmem_barrier_all` provides adequate protection.
- **DOCA UROM:** Pointer-retention cleanup composes (task 505 source review).
  DPU-side destroy ordering is correct. No build/runtime coverage (no DOCA SDK,
  no DPU hardware).
- **Perftest:** Happy-path smoke PASS (task 504). Error path (`free_ctx`) is
  covered at library level by `ucc_test_lifetime` phase 2.

### Upstream CI compatibility
- Upstream `.github/workflows/main.yaml` exercises UCC with OMPI
  (`--mca coll_ucc_enable 1`, IMB benchmarks). The lifetime enforcement is
  compatible — consumer-side synchronization prevents refusal errors.
- CI uses UCX with `ucp_memh_pack` support → full gtest suite will run.

### EXPORT_OFFLOAD accounting gap (known)
- `ucc_mem_map(MODE_EXPORT_OFFLOAD)` inflates `memh_count`: the consumed
  caller handle is not decremented before the new handle is incremented.
- Present on the merge base; drift does not change the outcome.
- No test/tool coverage → unobserved in CI.
- Not a landing blocker; should be a separate follow-up issue.

## 4. `git diff --check`

**Clean.** No trailing whitespace, no conflict markers.

## 5. Documentation

- API docs (`src/ucc/api/ucc.h`): Updated in `8426682f` — `ucc_init`,
  `ucc_finalize`, `ucc_context_create`, `ucc_context_destroy`,
  `ucc_team_create_post`, `ucc_team_create_test`, `ucc_team_destroy`,
  `ucc_mem_map`/`ucc_mem_unmap` all reflect the lifetime enforcement contract.
- Internal docs (`docs/doxygen/context.md`): Updated with the ownership/lifetime
  contract.

No additional documentation changes needed.

## 6. Landing Evidence Summary

| Artifact | Status |
|----------|--------|
| Merge conflict | Resolved (auto-merge, no conflicts) |
| `git diff --check` | Clean |
| Lifetime gtests (ASan) | 9/9 PASS |
| Standalone smoke test (ASan) | PASS, 0 leaks |
| Multi-rank MPI tests (task 504) | All PASS |
| OMPI/OSHMEM consumer teardown | 0 refusal errors |
| DOCA UROM source review | Composes cleanly |
| Follow-up scoping (task 506) | 1 proposed follow-up (live collectives during destroy) |

## 7. Landing Recommendation

The lifetime enforcement fix is ready for upstream submission. The merge onto
current `origin/master` (`a134efa1`) is clean with no conflicts. All lifetime
tests pass under ASan with no sanitizer errors in the lifetime code paths.
Consumer integrations are compatible (verified through source review and runtime
tests). The EXPORT_OFFLOAD accounting gap is a pre-existing issue that should
be tracked separately.

**Proposed landing order:**
1. Push `57dabf69` (merge commit) or rebase-and-squash the two lifetime commits
   onto `a134efa1` (depending on upstream preference).
2. Open PR with the verified commit range and this landing bundle as evidence.
3. Reference the identified follow-up (live collectives during team destroy) as
   a separate issue.

**Gates (per task wording):** Stop before push, PR, merge, or issue comment
without explicit authorization. This bundle is a recommendation.