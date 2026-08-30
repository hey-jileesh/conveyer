# ADR: rebuild vs live fold — exclusion owner (OQ-7)

| | |
|---|---|
| **Status** | **Accepted** (2026-08-30) — Option D adopted; validation 1's engine probe **gates hard**: F-7 may not lean on RB-1/RB-2's mechanical claims until the kernel probe passes on the pinned stack |
| **Date** | 2026-08-30 |
| **Reviewed** | 2026-08-30 — adopted with four refinements: the quiesce probe's blind window named (marker presence begins at commit; land→apply is invisible — the runbook must not overclaim); the tie-idempotency straddle variant pinned as its own harness case; rebuild's unannounced state change registered as a 006 D-3 perception question for F-7; validation 1 hardened to an adoption gate with the fallback's livelock re-pricing noted |
| **Decision owners** | The `conveyer-hpp.13` session (007.1 F-7 owns the conflict rule); 008 owns the efficiency runbook + retry alarm |
| **Informed by** | 007 D-5 ("atomically swapped" — mechanism unspecified) + D-3 (fold order-insensitivity, tier-2 full-rebuild-only demotion); 004 D-12 (concurrent siblings are deliberate); 004.1 I-11 (serializable MERGE, loser → `ValidationException` → retry → converges), I-19 (`before_id` capture), I-20 (single-writer is **per batch**; escape hatches governed, not ignored); 006 D-3 (perception contract); ADR-OQ5 (marker table: in-flight batches probe-able); ADR-OQ6 (marker spec) |

---

## Context

OQ-7 as posed: rebuild-vs-live-fold mutual exclusion has no owner — single-flight serializes *per batch* (execution name = `batch_id`), and 004 D-12 makes concurrent different-batch runs deliberate, so nothing anywhere serializes a full rebuild of a state table against the live folds MERGEing into it. The question offered two homes: a 007.1 run-mode precondition (the I-20 runbook idiom) or a named register row deferred to 008/ops.

First, establish that the hazard is real and directional. Rebuild (007 D-5) recomputes state from all facts and "atomically swaps." Suppose rebuild pins its fact read, and a live batch B then commits facts and MERGEs into state before the swap lands. If the swap simply wins, state becomes `fold(facts@pin)` — **B's contribution is erased**. This is not the disposable-state inconvenience D-5 prices: `batch-completed` was emitted for B, consumers gated on it, and under LWW nothing revisits B's domains until some *future* delivery happens to touch them — state is wrong, silently, indefinitely, for exactly the domains no one re-asserts. A lost update on the perception surface, in the forbidden direction. The reverse direction — a fold MERGE straddling the swap — and the rebuild losing its race are both loud-retry shapes. So OQ-7 **is** a failure-direction question, and the analysis of the last four ADRs applies: the mechanism must make the silent direction impossible and leave only loud ones.

Second, note what changed since OQ-7 was posed: the question predates the marker table. "Is any batch in flight for this feed?" is now a **probe** (marker presence without a commit-completion row — ADR-OQ5), not a runbook guess.

## Decision drivers

1. **The forbidden direction**: rebuild silently erasing a completed batch's state contribution. Loud, wasted work (rebuild retries, fold retries) is the permitted direction.
2. **Construction over discipline** (the arc's standing instrument): a runbook precondition or an ops lock is the shape DC-1/AE-7 demoted everywhere else.
3. **Locks have liveness problems**: a rebuild-in-progress flag is a mutable pointer someone must clear after a crashed rebuild — the classic distributed-lock trap (lease/TTL machinery to un-wedge a lane).
4. **The storage layer already has the right primitive**: the state table's root pointer *is* the epochal model's tiny mutable reference, and Iceberg commits against it are CAS-shaped — conditional, conflict-detected under serializable isolation (I-11 already leans on exactly this for sibling MERGEs).
5. **D-3's tier-2 posture**: demoted custom folds run *full-rebuild-only* — for those pipelines rebuild is not rare ops, it is the steady-state fold path, so the mechanism must not assume rebuilds are exceptional.

## Options

### Option 0 — No owner (the status quo the question names)

The silent lost-update stands as an unnamed hazard. Not lawful to leave: unlike OQ-6's "slow, not wrong," this failure is wrong state on the perception surface.

### Option A — 007.1 run-mode precondition (the I-20 runbook idiom)

"Verify no in-flight batches before starting a rebuild" — now at least probe-backed via the marker table. But it is the discipline shape in exact form: correctness resting on an operator remembering a check, decaying silently, with the D-3 tier-2 case (rebuild as the *routine* fold path) making the check a per-run ritual rather than a rare-ops step. I-20 itself only accepts runbook governance for *residuals it cannot close constructively*; here a constructive closure exists (Option D), so the precondition can't claim that defense.

### Option B — Defer to 008/ops with a named register row

Ownership assigned, mechanism unspecified — in practice an operational lock (pause the EventBridge rule / SFN during rebuild). Complects rebuild with orchestration state; the failure mode is the forgotten re-enable (a silently stopped lane), and the lock itself is discipline one level up. Also wrong owner: the hazard lives in the swap's commit semantics, which F-7 is drafting anyway — routing the correctness rule away from the doc that owns the mechanism splits what/who.

### Option C — A rebuild-in-progress flag the runner checks

An explicit lock object. Disqualified on driver 3: a flag that must be cleared is a mutable pointer with a liveness problem — a crashed rebuild wedges every fold until a human (or lease machinery nobody wants to build) intervenes. Update-in-place coordination for a system whose entire design refuses update-in-place.

### Option D (emergent) — No exclusion at all: both writers commit conditionally, conflicts are loud, quiesce demotes to efficiency

Dissolve the lock question the way OQ-5 dissolved the grain question. Rebuild and live folds are **two writers racing on one CAS-shaped reference**, and the storage layer already adjudicates that race — if F-7 specifies the swap correctly:

- **The swap is conditional**: rebuild captures the state table's current snapshot when it pins its fact read (the I-19 `before_id` idiom); the swap commits only if state is *still at that snapshot*. A live fold landing mid-rebuild moves the snapshot → the swap **fails loudly** → rebuild re-pins (now including the new facts) and retries. Rebuild never wins by overwriting.
- **The straddling fold is already handled**: a MERGE whose base pre-dates the swap conflicts under serializable isolation → `ValidationException` → `TransientError` → SFN retry → the rerun folds against rebuilt state and converges (I-11's existing path, unchanged — if the rebuild's pin included this batch's facts the re-MERGE no-ops on ordering ties; if not, it applies them; correct either way).
- **Quiesce becomes efficiency, not correctness**: on a busy feed a rebuild can lose repeatedly (livelock). The answer is operational — run it in a quiet window, checked by the marker-table in-flight probe — and 008 owns that runbook plus a retry alarm. The precondition Option A wanted as *the* enforcement survives as exactly what DC-1 made of AE-7's: defense-in-depth over a constructively closed invariant.

The ownership question then answers itself, because there is no exclusion to own: **F-7 owns the conflict rule** (it is a property of the swap it is already drafting); **008 owns the efficiency runbook and the alarm**; nobody owns a lock.

One load-bearing subtlety, promoted to an invariant below: all of this holds only if the swap is a commit **in the state table's own snapshot lineage**. A rebuild that writes a fresh table and swaps *names* in the catalog breaks conflict detection in both directions — the straddling MERGE lands happily on the orphaned old table, and the swap detects nothing — silently reconstructing the lost update inside the "safe" design.

## Comparison

| Criterion | 0 — no owner | A — runbook precondition | B — 008 ops lock | C — flag | D — conditional commits |
|---|---|---|---|---|---|
| Silent lost update (forbidden) | possible | possible when discipline lapses | possible on forgotten re-enable | closed while flag honored | **impossible by construction** |
| Failure modes remaining | silent | silent | silently stopped lane | wedged lane (crashed rebuild) | **loud retries only** |
| Liveness | — | — | forgotten re-enable | flag-clear problem | livelock → visible, ops-priced |
| D-3 tier-2 (rebuild routine) | — | per-run ritual | per-run orchestration surgery | flag churn | same mechanism, no ceremony |
| Owner coherence | none | 007.1 owns a ritual | mechanism/owner split | new machinery, new owner | **rule lives with the swap (F-7); ops owns efficiency** |
| New machinery | none | none | orchestration toggles | lock + lease | **none — I-11 + I-19 idioms reused** |

## Proposed outcome

**Option D.** In Y-statement form: *in the context of* serializing full rebuild against live folds on one state table, *facing* a silent lost-update hazard and an either/or question (runbook vs ops register) that offers only discipline in two costumes, *we propose* no mutual exclusion at all — the swap commits conditionally on the state snapshot captured at fact-pin, straddling MERGEs keep I-11's serializable conflict-retry path, and both writers' losses are loud retries — with the quiesce check demoted to an 008 efficiency runbook backed by the marker-table in-flight probe, *and against* the run-mode precondition (discipline enforcing what construction can) *and against* an ops lock or flag (forgotten re-enable, flag-clear liveness — coordination by mutable place), *to achieve* an invariant held by the storage layer's own CAS with zero new machinery and an ownership answer that dissolves (F-7 owns the conflict rule as part of the swap; 008 owns efficiency), *accepting* rebuild livelock on busy feeds as a visible, ops-priced residual and one hard constraint on F-7's freedom: the swap must live in the table's own snapshot lineage.

## Invariants for the F-7 ruling (RB-1…RB-3)

- **RB-1 — The swap is lineage-preserving.** Rebuild commits into the state table's own snapshot lineage (Iceberg-native replace/overwrite on the same table identity) — **never** a write-new-table-and-rename-in-catalog swap. Lineage is what makes the swap and concurrent MERGEs mutually conflict-detectable; a catalog rename silently disables both directions of protection and reconstructs the lost update inside the "safe" design. (It would also break as-of perception's time travel across the rebuild boundary — a second, independent reason.)
- **RB-2 — The swap is conditional and never wins by force.** Rebuild captures the state snapshot at fact-pin (the I-19 `before_id` idiom) and its swap validates state is still there; on conflict it fails loudly, re-pins, and retries. A rebuild retry metric rides the ledger like any attempt truth. No `--force` variant exists: a rebuild that must win against live folds is a quiesce problem (RB-3), not a flag.
- **RB-3 — Quiesce is efficiency, never correctness.** The "no in-flight batches" check (marker-table probe: presence without commit-completion) belongs to 008's rebuild runbook to bound retries on busy feeds — defense-in-depth over a constructively closed invariant, the DC-1/AE-7 disposition. Correctness never reads the probe; livelock surfaces as the RB-2 retry metric and its alarm, not as wrong state.

## Consequences

**Positive.** The last of the five open questions closes with zero new machinery — two existing idioms (I-11's serializable conflict path, I-19's `before_id` capture) compose into the invariant; the D-3 tier-2 pipelines get routine rebuilds with no per-run ceremony; the marker table gains its probe consumer on the ops side without entering the correctness path; OQ-7's either/or dissolves rather than being picked.

**Negative.** Rebuild on a busy feed can livelock until a quiet window — visible and alarmed, but real; F-7's swap-mechanism freedom is constrained by RB-1 (a name-swap rebuild implementation, arguably simpler to write, is forbidden); the conditional-swap validation and retry loop is rebuild-path code that must be tested against a concurrent-fold harness case.

**Neutral.** I-20's per-batch single-writer invariant is untouched — this ADR governs the cross-batch/table-grain race it explicitly never covered. The `nvh.40` attribution ruling is untouched (rebuild needs no attribution). Whether rebuild start/complete accretes marker rows (a `rebuild` stage kind under L-2's stage-scoped naming) is left to F-7 as optional observability — additive if wanted, never load-bearing.

**Registered question (adjacent, not ruled here).** A successful swap is a state change no `batch-completed` announced: rebuild typically runs *because* state diverged (a bug fix, a tier-2 demotion), so event-gated consumers see a step change outside the only novelty signal they gate on. Whether rebuild-complete emits an event — or the optional `rebuild` marker rows carry the announcement — is a **006 D-3 perception-contract question, registered to F-7's checklist** so it is answered by design rather than discovered by a confused consumer.

## Validation before adoption

1. **Engine check on the conditional swap — an adoption gate, not a to-do**: verify on the pinned Iceberg/PySpark stack that a same-lineage replace commit (a) can be made conditional on the captured base snapshot, and (b) mutually conflicts with a straddling MERGE under serializable isolation — the two mechanical claims RB-1/RB-2 stand on. Both are *engine behaviors, not design properties*, and Iceberg's overwrite-validation APIs are exactly where "works as documented" and "works on the pinned Spark stack" diverge — F-7 may not build on these claims before the probe passes (the same grade of falsification the EM review pass applied to P-2's claims). If either fails as stated, the fallback is the swap-as-MERGE shape (full-table MERGE with delete-of-absent), which keeps lineage trivially and pays write amplification — a mechanics substitution inside Option D, not a return to locks. **The fallback re-prices RB-3**: write amplification per losing retry changes the livelock economics, so the alarm threshold set under the replace shape must be re-derived if the fallback fires.
2. **Harness cases — three, not two**: the D-3/D-5 proof harness (fold(all) ≡ incremental) gains: (a) fold lands mid-rebuild → swap fails → re-pin → converge; (b) the straddle — MERGE conflicts post-swap → retry → converge; and (c) **the tie-idempotency variant that carries the post-swap retry's safety**: batch B's facts committed *before* the rebuild's pin, B's fold lands *after* the swap → the re-MERGE must no-op on ordering ties (D-2/D-4's tie discipline is what makes this retry safe, so it is asserted specifically, not inherited silently). All three join the build epic's register beside the F-5 and marker goldens.
3. **Runbook + alarm — with the probe's blind window named**: 008's register row gains the quiesce guidance (marker probe), the retry metric, and its persistent-livelock alarm threshold — one row, efficiency-classed, citing RB-3. The guidance must state the probe's scope honestly: **marker presence begins at commit, so a batch in land through apply is invisible to the in-flight probe** — a clean probe is not a guarantee of quiet, only of no batch at-or-past commit. RB-3 already makes this harmless (correctness never reads the probe; the cost is an extra swap retry when an early-stage batch reaches fold mid-rebuild), but a runbook that omits the caveat silently overclaims — the exact shape this arc exists to hunt.

## References

- 007 `design/007_record.md` D-5 (rebuild run mode, "atomically swapped"), D-3 (order-insensitivity, tier-2 full-rebuild-only)
- 004 `design/004_runner_spine.md` D-12 (deliberate sibling concurrency); 004.1 I-11 (serializable MERGE conflict path), I-19 (`before_id`), I-20 (per-batch single-writer + governed escape hatches)
- 006 `design/006_pure_core.md` D-3 (perception contract)
- `design/adr-oq5-batch-progress-grain.md` (marker table, in-flight probe, L-2 stage-scoped naming), `design/adr-oq6-fact-partition-spec.md` (marker spec)
- Beads: `conveyer-hpp.13` (F-7 owner), 008's rebuild-runbook register row
