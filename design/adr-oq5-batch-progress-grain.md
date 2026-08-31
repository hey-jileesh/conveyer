# ADR: batch-progress grain under per-type commit/fold (OQ-5)

| | |
|---|---|
| **Status** | **Accepted** (2026-08-29) — Option D adopted; this adoption **constitutes the joint ruling with ADR-OQ1**, whose status flips to Accepted with it, per the DC-1 register row |
| **Date** | 2026-08-27 |
| **Reviewed** | 2026-08-29 — adopted with four hardenings: the **write-order invariant** (no fact append may precede its marker row — the one point where soundness rested on an assumed rerun rather than construction; now in L-2, strengthening ADR-OQ1 M-3), intra-batch attribute-incoherence ⇒ refuse (L-3 + validation golden), completion rows named **commit-completion** so fold-row accretion never ambiguates the word, and the `batch_id` tiebreak documented as deterministic-arbitrary |
| **Decision owners** | The `conveyer-hpp.13` session (007.1 §4 / candidate F-n); 004.1 receives an additive erratum for the run-ledger columns |
| **Informed by** | 004.1 §6.5 (run-ledger schema, the map idiom, [E-9]), I-5, I-17, I-19 (attempt-truth vs batch-truth); 006.1 §11 [AE-7] + §16.2 [DC-1]; 007 D-5 + the `conveyer-nvh.40` unstamped-MERGE ruling; errata row 19 (per-(stage, table) guard grain); ADR-OQ1 (M-1…M-3), ADR-OQ2 (N-1…N-4, Accepted) |

---

## Context

OQ-5 was posed as: *"run-ledger grain under per-type commit/fold — per-(stage, table) rows (004.1-erratum-class schema change) vs per-table maps on one row?"* The pressure is real: 004.1 §6.5's ledger writes one row per stage transition per attempt with **singular** `snapshot_id`, `facts_appended`, `rows_merged` columns — and under 006 D-4 / errata row 19, commit is N guarded appends and fold is N MERGEs. A multi-type batch cannot be recorded truthfully in that schema.

But the question has been reshaped twice since it was posed, and the reshaping is the answer's shape:

- **[DC-1]** (critique gate) made a *durable, probe-able per-batch committed-table set* a required input to this decision — so bind can refuse `fact-type-removed-in-flight` constructively (006.1 §11 [AE-7]: the one wedge-class invariant currently held by runbook discipline).
- **ADR-OQ1** (Proposed) introduced the marker table — record-side, append-only, written by commit under the guard idiom — to be ruled *jointly* with OQ-5.
- **ADR-OQ2** (Accepted) hung the Track E coherence probe off that table (N-1…N-4: `delivery_key`/`delivery_content_hash` columns, marker *presence* including partial commits, never pruned).

## The diagnosis: OQ-5 complects two truths

004.1 I-19 already made the load-bearing distinction: *"attempt-truth and batch-truth are now two separately-sourced values instead of one complected field."* OQ-5 as posed asks one grain question about an artifact that is being asked to carry **both**:

- **Batch-truth** — *what committed durably, for which tables* — is what delta resolution, the DC-1 bind refusal, the Track E probe, and 008's registered readers need. It is control-adjacent, must be durable, and must make partial states visible.
- **Attempt-truth** — *what this attempt did, when, with what outcome* — is the run ledger's one job: deliberately lossy, ops-only, never read by control (004 §14.7, settled; re-affirmed by ADR-OQ1).

Braided into one artifact, the grain question is hard — every answer is wrong for one of the two consumers. **Separated, both answers are nearly forced.** That separation is not new design: it is I-19's seam plus ADR-OQ1's artifact, taken to their conclusion.

## Decision drivers

1. **Truthful recording**: an N-table batch's commit must be representable — the singular schema is falsified, not merely inelegant.
2. **[DC-1] discharge**: the committed-table set must be durable and probe-able by bind.
3. **M-1…M-3 / N-1…N-4** bind anything added to the marker table: append-only facts, data-borne ordering, guard-idempotent writes, genesis-complete retention, partial-commit visibility.
4. **The settled law**: control never reads the run ledger; run-ledger evolution and named queries (I-17, §11.5) should move additively.
5. **D-5's as-of mapping** rides "the ledger's batch→snapshot mapping" — lossy by [E-9] and 30-day snapshot-summary decay — and the `conveyer-nvh.40` ruling (attribution is the ledger's job; correctness never reads it) must not be reopened.

## Options

### Option 0 — Keep the singular schema

Not lawful: a multi-type batch's commit row cannot state what happened. Doing nothing here is not conservative; it is a false record.

### Option A — Widen the run ledger to per-(stage, table) rows and let it carry the committed-table set

Solves truthful recording, but asks the ops channel to serve the control consumers — the shape ADR-OQ1 already disqualified: control reading a deliberately lossy, other-role-prunable channel, reversing settled law. It also costs the largest migration (row-grain change to a live, indefinitely-retained table plus every §11.5 named query) for an artifact whose *one* consumer contract — the attempt story — never needed the finer grain.

### Option B — Run ledger gains additive per-table maps; marker table stays minimal per ADR-OQ1's sketch

Lawful and additive: the ledger keeps its row grain and gains `facts_appended_by_table` / `snapshot_ids_by_table` / `rows_merged_by_table` beside its existing map idiom; DC-1 is discharged by the marker. But it leaves the marker's own grain unsettled (the actual joint-ruling obligation), leaves the Track E probe's partial-visibility need (N-4) unstated at schema level, and leaves D-5's as-of mapping permanently on the lossy channel with [E-9]'s ambiguity accepted forever.

### Option C — Per-batch map rows in the marker table

One marker row per batch, with per-table maps accreting as appends land. Disqualified by **M-1's own argument re-applied at schema grain**: a map that accretes is a row that UPDATEs (a mutable pointer, value re-complected with time), and a map written once at completion is a silent partial window — the exact invisible-loss shape both prior ADRs exist to exclude. There is no third way to maintain a map row.

### Option D (emergent) — Two artifacts, two truths, each at its forced grain

**Batch-truth: the marker table at the guard grain.** One append-only row per `(batch_id, stage, table_name)` — the grain errata row 19 already established for the guards this table twins — carrying the commit's `snapshot_id` (I-19's own-commit resolution value, already in hand at write time), plus one **commit-completion row** per batch (stage-scoped by name — see L-2). A `stage` column from day one (`commit` now) reserves fold rows as a later *additive* accretion — giving D-5's as-of mapping a durable home when 008 wants it, without reopening `conveyer-nvh.40` (attribution stays never-read-by-correctness; only its channel gains a durable option). Delivery attributes (`delivery_key`, `delivery_content_hash`, `received_at`) ride **every** row, because the N-4 probe must key-match *partial* batches and a partial batch has no completion row to join — denormalization is the named price of partial visibility. Ordering for "latest completed" is `received_at` (data-borne, ingestion-asserted, the same value D-4's fold ties already trust) with `batch_id` as tiebreak; `committed_at` is provenance only (M-2).

> **Annotation [DC2-1]** (2026-08-30, bead `conveyer-hpp.13.22` — erratum note per house convention; the original text above, and the Proposed outcome's "`snapshot_id` carried", stand unaltered): the "already in hand at write time" phrasing predates the adoption review and is **superseded by the adopted L-2 marker-first hardening** — written marker-first, a guard-twin row's append snapshot does not yet exist at write time. Field-level resolution is 007.1 §6.3: `snapshot_id` is **NULL on every Phase-1 row**; 008's reserved fold-row/as-of accretion is the future bearer of the value.

**Attempt-truth: the run ledger, additively evolved.** Row grain unchanged — one row per stage transition per attempt. Three additive map columns beside the schema's existing map idiom (`co_effect_snapshot_ids`, `merge_summary`): `facts_appended_by_table`, `snapshot_ids_by_table`, `rows_merged_by_table`. The singular columns become totals (documented), so §11.5 named queries keep working. No row-grain migration, no control duty, no reversal.

**The write-order invariant** (the "same window" objection answered *by construction, not by rerun*): the marker row is written **before, or atomically with, its fact append — never after**. The order decides the probe's soundness. Marker-after-append would leave a killed attempt's facts with *no marker presence at all* — not the partial state N-4 covers but a batch the OQ-2 probe cannot see, permitting a drop against invisible facts (forbidden direction, silent) until a rerun that an abandoned batch never gets. Marker-first inverts the kill state into **marker-without-facts**: the probe and the DC-1 bind refusal over-refuse (permitted direction), the OQ-1 resolution read is untouched (it keys on completion rows), and the rerun discipline — D-4's guards consulting the fact tables, the guard-idempotent marker write (M-3) completing any missing twin — becomes a *completeness* nicety rather than a soundness dependency. This is the three ADRs' asymmetry argument applied one level down, to a single attempt's write sequence. The run ledger's [E-9] stays exactly as accepted — attempt-truth is allowed to be lossy; batch-truth now is not.

**DC-1 discharge, and the choice inside it**: bind refuses `fact-type-removed-in-flight` by probing the marker for in-flight batches' table sets — the **fail-fast closure**, consistent with P-4's bind-refusal idiom. The AE-7 row's alternative (door enumeration unioning spec types with observed tables) is *not* adopted: it would accommodate at runtime a state the law calls a breaking change, softening "removal ⇒ new pipeline" into "removal ⇒ tolerated drift." The runbook precondition demotes to defense-in-depth, as [DC-1] required.

## Comparison

| Criterion | 0 — singular | A — ledger per-(stage,table) + control duty | B — ledger maps + minimal marker | C — marker map rows | D — two truths, forced grains |
|---|---|---|---|---|---|
| Truthful N-table recording | **falsified** | yes | yes | yes (late or mutable) | yes |
| [DC-1] committed-table set | no | via forbidden channel | yes (marker) | silent partial window | **yes, probe-able incl. partial** |
| M-1 / failure shape | — | — | unstated at schema level | **update-in-place or silent window** | append-only; partial visible |
| N-4 probe over partial batches | — | — | needs the columns D pins | no | **pinned (attrs on every row)** |
| I-19 truth seam | braided | **braided harder** | separated, half-specified | separated, wrong grain | **separated, stated as invariant** |
| D-5 as-of channel | lossy forever | lossy forever | lossy forever | — | durable home reserved (additive, later) |
| [E-9] batch-truth ambiguity | stands | stands | narrowed | reintroduced | **self-healed by rerun discipline** |
| Blast radius | zero (and wrong) | row-grain migration + queries | additive | — | additive everywhere (ledger cols; marker is new) |

## Proposed outcome

**Option D.** In Y-statement form: *in the context of* recording batch progress under per-type commit/fold, *facing* one grain question braided across two truths with incompatible needs, *we propose* separating them along I-19's seam — batch-truth as the marker table at the per-`(batch, stage, table)` guard grain plus commit-completion rows (append-only, **marker-first write order**, delivery attributes denormalized, `snapshot_id` carried, `received_at` ordering, fold rows reserved as additive accretion), attempt-truth as the run ledger with its row grain intact and three additive per-table map columns — *and against* widening the ledger into a control channel (reverses settled law for the largest migration) *and against* map rows in the marker (M-1's disqualification at schema grain), *to achieve* truthful N-table recording, the [DC-1] fail-fast bind refusal, N-4's partial visibility, a durable future home for as-of, and additive-only evolution on every touched surface, *accepting* denormalized delivery attributes on every marker row, totals-semantics documentation on the ledger's singular columns, and that fold-row accretion (and with it durable as-of) is deliberately deferred to 008's registered question.

## Invariants for the ruling (L-1…L-4)

- **L-1 — One seam, stated structurally.** Batch-truth lives only in the marker table (record-side; control may probe it); attempt-truth lives only in the run ledger (lossy; control never reads it). Any value a control decision needs that exists only in the run ledger is a design defect, not a read to authorize.
- **L-2 — Batch-truth grain is the guard grain; write order is marker-first.** One append-only row per `(batch_id, stage, table_name)` plus one **commit-completion** row per batch; no maps, no aggregate rows (a maintained map is an UPDATE or a silent window — both disqualified). **No fact append may precede its marker row** — marker-first (or atomic) is what makes a killed attempt's state marker-without-facts (over-refusal, permitted) instead of facts-without-marker (invisible to the probe, forbidden); soundness must not depend on a rerun occurring. The `stage` column exists from day one; fold rows are an additive accretion, never a migration — and completion is **stage-scoped by name** (commit-completion now; a fold-completion row, if 008 ever wants one, is a new row kind, never a re-reading of the old word).
- **L-3 — Delivery attributes ride every marker row, and disagreement is ambiguity.** The N-4 probe must key-match partial batches, and a partial batch has no completion row to join. The denormalization is the priced cost of partial visibility; dropping it to "normalize" rebuilds the silent window. Its own failure mode is priced too: if the attributes *disagree within one batch* (a write bug), the probe treats the batch as incoherent ⇒ **refuse to drop** — never pick a row to believe.
- **L-4 — Run-ledger evolution is additive-only.** Row grain unchanged; per-table story in map columns beside the existing map idiom; singular columns documented as totals. A row-grain migration of an indefinitely-retained ops table, breaking §11.5's named queries, buys nothing any consumer contract needs.

## Consequences

**Positive.** Every marker consumer named across the three ADRs — delta resolution, DC-1 bind refusal, Track E probe, 008's registered readers — reads one artifact at one grain; the AE-7 wedge closes fail-fast at bind; the ledger's [E-9] ambiguity stops mattering for batch-truth (rerun heals the twin); as-of has a durable home reserved without reopening `conveyer-nvh.40`; 004.1's erratum is additive.

**Negative.** The marker table is now a real schema (nine-ish columns, two row kinds) rather than ADR-OQ1's minimal sketch — bootstrap, IAM (S-15 generation, DS-2 table-class stamping), and the never-pruned posture all land with it; delivery attributes are stored N+1 times per batch; the ledger's singular columns carry totals semantics that must be documented or misread.

**Neutral.** The run ledger's character (lossy, ops, indefinitely retained, [S-12] not-compliance-grade) is unchanged. Fold-row accretion and the as-of migration are deferred, reserved, and owned by 008's registered question. The door-enumeration-union alternative is rejected, not deferred.

## Validation before adoption

1. **Ordering key check (M-2)**: confirm `received_at` is per-feed monotonic enough under ingestion's registration semantics (002.1) to order "latest completed", with `batch_id` tiebreak — the same trust D-4's fold ties already place in it, but stated for this read. The tiebreak is **deterministic-arbitrary**: `batch_id` is UUIDv5 (I-22's pattern) and carries no temporal order — equal-`received_at` siblings get a stable but meaningless winner, and the ruling documents this so nobody later reads order into ids that don't carry it.
2. **Named-query sweep**: the 004.1 additive erratum lists every §11.5 query touched by totals-semantics and map columns; none may break.
3. **Schema fixture**: the marker table's DDL and goldens join the build epic's register (beside F-5's probe goldens from ADR-OQ2), covering: the two row kinds, the partial-commit probe scenario, the **marker-without-facts kill state** (L-2's write order under a kill between marker write and append ⇒ over-refusal), and the **intra-batch attribute-disagreement** case (L-3 ⇒ refuse).
4. **Joint-ruling completion**: adopting this ADR flips ADR-OQ1 to Accepted; both documents' status rows update together, and `conveyer-hpp.13`'s F-n for §4 cites this ADR as settled input.

## References

- 004.1 `design/004.1_runner_spine_lld.md` §6.5 (run-ledger schema, map idiom, [E-9]), I-5, I-17, I-19 (attempt-truth vs batch-truth), §11.5 (named queries)
- 006.1 `design/006.1_pure_core_lld.md` §11 [AE-7] (the wedge + the two constructive closures), §16.2 [DC-1]
- 007 `design/007_record.md` D-5 (as-of), errata-notes §2 / `conveyer-nvh.40` (unstamped-MERGE ruling); 004.1 errata row 19 (guard grain)
- 007.1 `design/007.1_record_lld.md` §4 (OQ-5 stub + DC-1 note)
- `design/adr-oq1-predecessor-source.md` (M-1…M-3), `design/adr-oq2-superseded-batch-source.md` (N-1…N-4, Accepted)
- Beads: `conveyer-hpp.13` (decision owner), `conveyer-kof` (002.1 field), 008's registered as-of/fold-row question
