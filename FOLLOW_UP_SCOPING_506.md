# Follow-up Scoping — Task 506

**Date:** 2026-08-25
**Commit:** `8426682f` (fix: enforce UCC owner lifetimes)
**Issue #1174 scope:** Owner-lifetime enforcement only (context→team, lib→context).
This task audits the four intentionally excluded areas and decides whether each
needs a separate follow-up issue. Issue #1174 scope is NOT expanded.

## 1. Team-after-destroy use

**Risk:** Using a team handle after `ucc_team_destroy` has been called on it.

**Current state:**
- API doc (ucc.h line 1599): "It is invalid to post a collective operation after
  the ucc_team_destroy operation."
- `ucc_team_destroy_single` (ucc_team.c lines 539–610) frees the team struct at
  line 608 (`ucc_free(team)`). No tombstone, no poison, no post-destroy guard.
- Subsequent use of the freed handle is a use-after-free — undefined behavior.

**Existing protection:** API contract only. No internal enforcement. Consumers
(OMPI, OSHMEM) conventionally never use a communicator/team after free because
their own API contracts forbid it (MPI: `MPI_Comm_free` invalidates the
communicator; OSHMEM: `shmem_team_destroy` invalidates the team).

**What a fix would require:** Either (a) keep the team struct alive as a
tombstone after destroy to detect post-destroy use, or (b) add a reference
count/proxy scheme. Both are API/ABI changes: the team handle would no longer
be a direct pointer whose lifetime matches create→destroy. Significant design
discussion needed.

**Decision: Defer.** The API contract is clear; consumers already obey it.
Fixing this requires broader API design work beyond the scope of a single
issue. If a real-world consumer bug is found, escalate to a design proposal.

---

## 2. Live execution engines during team destroy

**Risk:** Calling `ucc_team_destroy` while collectives are in flight on that
team. The destroy path frees CL teams, service team, and the team struct —
invalidating the handles that in-flight collectives depend on.

**Current state:**
- `ucc_team_destroy` (ucc_team.c lines 612–627) checks only that the team is
  in `UCC_TEAM_ACTIVE` or `UCC_TEAM_FAILED` state (not still being created).
  It does NOT check for outstanding collectives.
- `ucc_team_destroy_single` finalizes the internal service request (`team->sreq`)
  but has no visibility into user-posted collectives.
- The team struct has no field tracking outstanding-collective count.

**Existing protection:** Consumer-side synchronization. OMPI uses
`MPI_Barrier`/`MPI_Win_fence` before communicator free. OSHMEM uses
`shmem_barrier_all` before team teardown. Task 504 verified 0 refusal errors
for both consumers.

**What a fix would require:** Internal-only change — no API modification.
- Add an atomic `outstanding_colls` counter to `ucc_team_t`.
- Increment on `ucc_collective_post`, decrement on `ucc_collective_finalize`.
- In `ucc_team_destroy`: if `outstanding_colls > 0`, return
  `UCC_ERR_INVALID_PARAM` without destroying.
- Scope: bounded to `src/core/ucc_team.c`, `src/core/ucc_team.h`, and the
  collective post/finalize paths.

**Decision: Needs follow-up issue.** This is a real, fixable risk. The fix is
internal-only, scoped, and does not change the API contract. Proposed issue:

> **Issue: Refuse `ucc_team_destroy` while collectives are outstanding**
> - Add `outstanding_colls` atomic to `ucc_team_t`.
> - Account in `ucc_collective_post` (+1) and `ucc_collective_finalize` (-1).
> - `ucc_team_destroy` returns `UCC_ERR_INVALID_PARAM` when count > 0.
> - Excludes triggered collectives (event-driven, different lifecycle).
> - No API change; internal enforcement only.

---

## 3. Outstanding collective/request lifetime

**Risk:** A collective request outliving its team, or a collective that is
never finalized, leaking resources.

**Current state:**
- API doc (ucc.h line 1969): "On error, request handle becomes invalid, user is
  responsible to call ucc_collective_finalize to free allocated resources."
- `ucc_team_destroy_single` finalizes only the internal service request. User
  collective requests are not tracked or cleaned up.
- If a team is destroyed with outstanding collectives, the underlying CL/TL
  resources those collectives reference are freed → corruption/leak.

**Relationship to item 2:** This is the same root cause as "live execution
engines during destroy." If we add outstanding-collective tracking (item 2),
the team destroy refusal covers this case: you cannot destroy a team with
outstanding collectives, so collectives cannot outlive their team.

The separate question of "what happens if a user never calls
`ucc_collective_finalize` on a completed collective?" is a resource-leak
scenario that is already documented as the user's responsibility. Adding
internal tracking for this would require a garbage-collection or
reference-counting scheme across all collective paths — disproportionate
overhead.

**Decision: Covered by item 2.** The team-destroy-with-live-collectives refusal
closes the practical gap. Standalone "never finalized" leak is a user bug per
API contract; internal enforcement is disproportionate.

---

## 4. Arbitrary concurrent use/destruction

**Risk:** Multiple threads calling `ucc_team_destroy` simultaneously, or one
thread posting collectives while another destroys the team.

**Current state:**
- `ucc_team_destroy` reads `team->state` (line 619) and calls
  `ucc_team_destroy_single` without any lock.
- Two concurrent `ucc_team_destroy` calls → double-free.
- `ucc_collective_post` + concurrent `ucc_team_destroy` → UAF on CL/TL team
  handles.
- The API supports `UCC_THREAD_MULTIPLE` mode for collective progress, but team
  lifecycle operations have no internal synchronization.

**Existing protection:** API contract puts the burden on the user. The
documented contract (ucc.h lines 1599–1603) states it is invalid to overlap
team creation/destruction. MPI consumers naturally serialize communicator
creation/destruction through their own API contracts; OSHMEM similarly.

**What a fix would require:** Adding locks to team lifecycle operations
(`ucc_team_create_post`, `ucc_team_create_test`, `ucc_team_destroy`). This
would add overhead to collective paths if the same lock is used for
synchronization. Alternatively, a lightweight "destroying" flag with atomic
CAS. Either approach requires careful design to avoid deadlocks and performance
regression.

**Decision: Defer.** Full thread-safety for team lifecycle is a significant
design effort with potential hot-path impact. The API contract already states
the user's responsibility. Existing consumers (OMPI, OSHMEM) naturally
serialize these operations. If a UCC_THREAD_MULTIPLE consumer with concurrent
lifecycle operations emerges, this should be addressed as a design proposal,
not a scoped bug fix.

---

## Summary

| Excluded area | Decision | Rationale |
|---------------|----------|-----------|
| Team-after-destroy use | **Defer** | Requires API/ABI design change; consumer-side protection adequate |
| Live execution engines during destroy | **Needs follow-up** | Internal-only fix; real risk; scoped work |
| Outstanding collective lifetime | **Covered by item 2** | Same root cause; team-destroy refusal closes gap |
| Concurrent use/destruction | **Defer** | Significant locking design; API contract adequate for current consumers |

**Proposed follow-up issue (one):**

> **Issue title:** Refuse ucc_team_destroy while outstanding collectives exist
>
> **Scope (must not expand):**
> - Add `outstanding_colls` atomic counter to `ucc_team_t`.
> - Increment in `ucc_collective_post` paths, decrement in
>   `ucc_collective_finalize` paths.
> - `ucc_team_destroy` returns `UCC_ERR_INVALID_PARAM` when count > 0.
> - Exclude triggered (event-driven) collectives from accounting.
> - No API change.
>
> **Explicitly NOT in scope:**
> - Post-destroy use detection.
> - Thread-safe concurrent lifecycle operations.
> - Automatic collective cleanup on team destroy.
> - Issue #1174 scope expansion.

**Issue #1174 scope:** NOT expanded. The landed commit (`8426682f`) remains
scoped to owner-lifetime enforcement (context→team, lib→context). This document
identifies one follow-up (live collectives during destroy) that is adjacent but
separate.