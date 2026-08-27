# ADR: delta_filter predecessor-resolution source (OQ-1)

| | |
|---|---|
| **Status** | Proposed — input to `conveyer-hpp.13`; to be ruled **jointly with OQ-5** (run-ledger grain), per the DC-1 register row in 006.1 §16.2 |
| **Date** | 2026-08-26 |
| **Reviewed** | 2026-08-26 — argument form confirmed (a disqualification from failure-mode shape, not a robustness preference); hardened with invariants **M-1…M-3** and the Option-0 reframe below |
| **Decision owners** | The 007.1-completion session (`conveyer-hpp.13`); 008 co-owns the consuming mechanics |
| **Informed by** | 007 D-2 (v1.0.1), 004 §14.7 / 004.1 I-2·I-5·[S-12], 006.1 §16.2 [DC-1], 007.1 v0.1-seams+e4 §4 (OQ-5 stub), the AE-3/AE-7 adversarial findings |

---

## Context

`delta_filter` decides, per `record_key`, whether a candidate row is **novel** (becomes a fact) or an **unchanged re-assertion** (dropped, no fact). 007 D-2 settled the comparison baseline: *the feed's latest **completed** batch* — latest, because comparing against any older batch drops the A→B→A revert and leaves state wrong; completed, because dropping is only justified when the matching content is durably in the record.

D-2 also settled the failure policy: **fail open** — "no predecessor ⇒ everything is novel." A redundant fact is harmless (identical content occupies an identical fold position); a wrong drop is information lost forever.

What D-2 left open — this ADR's question — is the **resolution source**: from where does the runner learn *which* batch is the feed's latest completed one? No legal source currently exists:

- The **run ledger** knows about completions, but it is *deliberately lossy* (best-effort per-transition appends that may fail without failing the run), prunable by an ingestion-owned role, and 004 §14.7 is settled law: **no control decision reads the run ledger**.
- Ingestion's **delivery ledger** knows, but is IAM-unreachable from the batch lane by design (004.1 I-21).
- The **fact tables** cannot answer alone: a completed batch whose candidates all dropped commits **zero facts** (D-2's zero-fact corollary) and is invisible there.

## Decision drivers

1. **The fail-open law** (D-2, load-bearing): delta detection degrading must cost duplicate facts, **never wrong state**. A resolution mechanism whose failure mode is a *silent* stale predecessor produces wrong drops — the forbidden direction.
2. **The seam law** (004 §14.7): the ops/observability channel stays un-complected from control. 004's own erosion list names this exact move — *"A control decision based on the ledger complects control with a channel that is allowed to be lossy"* (item 7, line 353) — and states the pattern Option B extends: *"D-4's guards consult the data tables themselves, which is exactly why the ledger is allowed to be lossy"* (line 337). Reversing this is not erratum-grade; it reopens settled law.
3. **The DC-1 obligation** (006.1 §16.2): the OQ-5 ledger-grain ruling *must* weigh making the per-batch committed-table set durable and probe-able, so bind can refuse `fact-type-removed-in-flight` constructively instead of by runbook discipline.
4. **Economics**: snapshot-style feeds re-assert their full contents every delivery; without delta detection, fact tables grow by feed size per batch.

## Options

### Option 0 — Status quo: no resolution; everything is always novel

Run the D-2 fallback permanently. Never drop anything.

*Correctness is trivially perfect* — this is the floor both other options degrade to. The cost is maximal and permanent: fact-table growth of full feed size per delivery and a fold that re-chews value-identical assertions forever. This is exactly the economics D-2 exists to avoid — but it is a **legitimate Phase-1 deferral** if measured carrier volumes turn out small (see *Validation*).

### Option A — Read the run ledger, with a conservative fallback on ambiguity

Carve an exception into 004 §14.7 for this one read; on any *visible* doubt, fall back to everything-novel.

The disqualifying property is the **shape of its failure**. The ledger's loss mode is a *missing* completion row: resolution then finds the previous batch as "latest completed" **with no visible sign of doubt**. The conservative fallback cannot fire on what it cannot see — and a silently stale predecessor is precisely the A→B→A wrong drop. To become sound, A must cross-check the fact tables for batches newer than its resolution, which requires knowing the committed-table set — which *is* Option B's artifact, reached through a channel the architecture walled off. Two further costs: it reverses settled law rather than accreting, and it pressures the run ledger to harden into a control store — the "simple service growing duties sideways" complection 004 §14.7 exists to prevent.

### Option B — A durable completed-batch marker in the record

A small spine-owned control table — approximately `(feed_id, batch_id, fact_table, committed_at)` rows written **decide-then-do beside each guarded append** (the guard idiom, at the per-`(stage, table)` grain errata row 19 established), plus a completion row written as commit's last act — including for zero-fact batches, which the marker makes durably visible for the first time.

The marker table is itself a **fact table, not a pointer**: its rows are facts ("table T for batch B committed at t"), "latest completed" is a *derived read* over them, and it sits under the same append-only IAM regime as the fact tables (invariant M-1 below — an UPDATE-capable marker table is a mutable pointer, and reopens the silent-staleness hole through the back door).

Its failure mode points the permitted direction: a kill inside commit leaves *facts present, completion absent* — a state the marker table makes **directly probe-able**. Resolution sees the ambiguity, the fallback actually fires, and fail-open absorbs the residue as duplicate facts, never wrong state. The same artifact is the batch-completion fact the architecture already treats as first-class novelty (`batch-completed` on EventBridge is *move*, the run ledger is a lossy ops record — nothing today *remembers* the reified process; this table is the missing "remember") — and it is simultaneously the committed-table set DC-1 requires and the completed-batch surface 008's registered readers need.

## Comparison

| Criterion | 0 — always novel | A — run ledger | B — durable marker |
|---|---|---|---|
| Failure mode of a bad resolution | none exists | **silent stale predecessor → wrong drop** | visible ambiguity → fallback → extra facts |
| Direction under the fail-open law | permitted | **forbidden** | permitted |
| 004 §14.7 seam | untouched | **reversed** | untouched (additive) |
| Zero-fact completed batch | moot | visible *when the row survived* | durably visible — only option that shows it |
| Hardened-to-soundness endpoint | — | reconstructs B via a worse seam | already sound |
| DC-1 / OQ-5 / 008 synergy | none | none | one artifact serves all three |
| Concurrent-sibling residual (D-2) | vacuously closed | widened (adds a silent race) | narrowed (deterministic instance becomes visible) |
| Cost | zero build; maximal storage/fold | cheap-looking + hidden hardening arms | new table class (schema, bootstrap, IAM, maintenance) + commit-stage write + **couples the ruling to OQ-5** |

## Proposed outcome

**Option B**, ruled jointly with OQ-5 exactly as DC-1 requires.

In Y-statement form: *In the context of* resolving the feed's latest completed batch for `delta_filter`, *facing* the absence of any legal control source and a fail-open law that forbids silent wrong drops, *we propose* a durable completed-batch marker table written by commit at per-`(batch, fact_table)` grain plus a completion row, *and against* reading the deliberately lossy run ledger (its loss mode is invisible to the conservative fallback, and every hardening it needs reconstructs the marker through a forbidden seam) *and against* permanent always-novel (fact-table economics), *to achieve* a resolution source whose only failure direction is duplicate facts, one artifact that also discharges DC-1's bind-refusal input and 008's registered-reader need, *accepting* a new table class with its bootstrap/IAM/maintenance surface, one more write inside commit, and that OQ-1 can no longer be ruled independently of OQ-5 — **with invariants M-1…M-3 binding the schema ruling**.

The core argument is not general robustness — it is that the two mechanisms **fail in different directions, and only one direction is permitted here**. That is a disqualification of A, not a preference for B.

## Invariants for the marker schema ruling (M-1…M-3)

These are part of the proposal, not implementation advice: each closes a path by which Option B would quietly reconstruct the failure mode it exists to exclude. Per this ADR's own standard, they belong in the OQ-5 joint ruling as invariants, not left to discipline.

- **M-1 — Facts, not a pointer.** Marker rows are append-only facts under the same IAM regime as the fact tables; "latest completed" is a derived read, never a stored/updated value. If a marker row can be UPDATEd, the table has become a mutable pointer — value re-complected with time — and the silent-staleness hole returns through the back door.
- **M-2 — Ordering is data-borne, never wall-clock.** "Latest" is ordered by something in the data — ingestion's delivery sequence carried onto the batch — with `committed_at` as provenance only. Ordering two near-simultaneous completions of one feed by timestamp is timestamp-as-truth: it re-imports the concurrent-sibling residual this option claims to narrow. This ordering choice is part of the OQ-5 joint ruling.
- **M-3 — Retention is load-bearing; writes are guard-idempotent.** Pruning may remove rows *strictly older than* each feed's latest completed row, never the frontier — a pruned frontier reconstructs Option A's invisible loss mode inside Option B. And marker writes carry the same idempotency-key discipline as the guarded appends they sit beside (the guard idiom supplies it; the ruling must state it rather than assume it), so a rerun of a killed commit is a no-op. *(Strengthened by ADR-OQ2 N-4: while the Track E coherence probe consumes this table's `delivery_key`/`delivery_content_hash` columns, the frontier rule is not enough — the table is never pruned in Phase 1, and if retention is ever introduced, a probe lookback exceeding the horizon reads as unknown ⇒ refuse to drop.)*

## Consequences

**Positive.** Wrong-drop risk removed by construction; the last discipline-enforced invariant of the 006.1 run (fact-type removal in flight, AE-7/DC-1) becomes constructively refusable at bind; 008 inherits its completed-batch surface instead of inventing one; the D-2 concurrent-sibling residual narrows to its named, fail-open-absorbed core.

**Negative.** One more table class to bootstrap, grant (own-prefix, S-15 pattern), and maintain; commit's mechanics grow a marker write; the `hpp.13` session's first ruling gets bigger because OQ-1 and OQ-5 must now be settled together.

**Neutral.** The run ledger remains exactly what 004 declared it: an ops channel nobody's correctness depends on. Track E's second read (OQ-2, the superseded batch id) is unaffected — it still needs its 002.1 ruling regardless of this choice.

## Validation before adoption

Two checks belong at the top of the `hpp.13` session:

1. **The Option-0 escape hatch — correctly sized**: DC-1 wants the committed-table set for bind-refusal *regardless* of delta economics, so if OQ-5 rules the marker table in for DC-1 anyway, Option 0 saves only the resolution read — delta_filter gets its source for free. The carrier-x/carrier-y volume measurement therefore decides *when* delta detection turns on, not *whether* the table exists. (This strengthens the joint-ruling argument: the table's justification does not rest on delta economics alone.)
2. **The joint ruling**: settle the marker schema *as* the OQ-5 grain decision (per-`(stage, table)` rows vs per-table maps), with the DC-1 note on `conveyer-hpp.13` as a required input — the marker table is only "one artifact for three obligations" if it is designed once.

## References

- 007 `design/007_record.md` D-2 (v1.0.1) — baseline, fail-open law, zero-fact corollary, sibling residual, forward tripwire
- 004 `design/004_runner_spine.md` §14.7 · 004.1 I-2 / I-5 / [S-12] — run-ledger character and the control-read prohibition
- 006.1 `design/006.1_pure_core_lld.md` §16.2 [DC-1] · §11 [AE-7] — the required OQ-5 input and the enumeration-drift wedge
- 007.1 `design/007.1_record_lld.md` §4 (OQ-5 stub, v0.1-seams+e4)
- Beads: `conveyer-hpp.13` (decision owner), `conveyer-3w8` (errata), 008 registered-reader overlap
