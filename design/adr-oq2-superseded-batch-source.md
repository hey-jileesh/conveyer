# ADR: Track E superseded-batch source for the second read (OQ-2)

| | |
|---|---|
| **Status** | **Accepted** (2026-08-27) — Option D adopted; the 002.1 additive ruling proceeds as `conveyer-kof`, F-5 mechanics land in `conveyer-hpp.13`; companion to `design/adr-oq1-predecessor-source.md` |
| **Date** | 2026-08-26 |
| **Reviewed** | 2026-08-27 — adopted, with four hardenings: probe scope widened to marker *presence* (partial commits included), the genesis-complete retention invariant (N-4, strengthening ADR-OQ1 M-3), the probe-only interim's bootstrap residual named instead of asserted away, and refusals-as-data made an F-5 obligation; `delivery_content_hash` naming |
| **Decision owners** | 002.1 owns the event-contract change (its producer, its adjudication); the `conveyer-hpp.13` session owns F-5's consuming mechanics and the probe |
| **Informed by** | 007 D-2 (two-read law) · D-4 (Track E accretion); 002.1 §7 (ledger schema, `supersedes`), §8.3 (delivery_key matching + the concurrent-correction race), §9.5 (reconciliation sweep), G-06; 004.1 I-8 (router allowlist), I-21 (ingestion ledger unreachable), I-22 (event trust boundary), §6.1 (seed model); ADR-OQ1 (marker table, invariants M-1…M-3) |

---

## Context

007 D-2 settled the two-read law for corrections: when a delivery supersedes an earlier one (Track E), a candidate row may be dropped **only if its `content_hash` is unchanged in *both* the feed's latest completed batch *and* the superseded batch** — the second read is *"in addition, never instead"*, added because a one-sided comparison silently drops a corrected re-assertion that would win its fold tie (the 2026-08-10 critique). D-2's wording assumed the source: *"the superseded batch the supersession ledger names (002.1)."*

That source is unreachable by design. Supersession is **adjudicated by ingestion** — at registration, a new delivery whose `delivery_key` matches a prior registered delivery with different `content_hash` gets `supersedes = <prior delivery_id>` on its ledger row, and the prior delivery accretes a `superseded` disposition row (002.1 §8.3) — but that ledger sits behind the I-21 wall: *"ingestion's ledger is unreachable even under a misconfigured spec."* The batch lane must learn the superseded predecessor some other way.

Verified grounding this ADR stands on:

- The **v1 `delivery-registered` event already carries** `delivery_id`, `batch_id`, `delivery_key`, and `content_hash` (fixtures `contracts/fixtures/events/delivery-registered/`), but **not** `supersedes`. Ingestion mints `batch_id` at registration, so the delivery→batch mapping problem is already solved on ingestion's side.
- The **router forwards a minimal allowlisted projection** of the event (004.1 I-8, [S-2]); the job seed is narrow-typed at the trust boundary (I-22). A new field must be ruled into the event contract, the router allowlist, and the seed model — three additive edits, two documents.
- 002.1 names a **concurrent-correction race** (§8.3): two corrections of the same `delivery_key` registering simultaneously may *both* compute `supersedes = None`. The ledger is repaired by the weekly reconciliation sweep (§9.5) — but an already-emitted event is never re-emitted, so an event-borne field inherits this race permanently for the affected batch. 002.1 explicitly leans on the parent lane's delta detection for the harmless direction of this race; D-2 carries the matching accepted residual ("state one correction behind until the next delivery re-asserts").

## Decision drivers

1. **The two-read law** (007 D-2, settled): an unknown supersession means one-sided drops — outside the argued-safe envelope, in the forbidden direction, silently. The mechanism must make "supersession unknown" either impossible or *visible*, so the lane can refuse to drop instead of dropping wrongly.
2. **One authored source of judgment**: supersession semantics (`delivery_key` matching, disposition precedence, manifest/completeness nuances) are ingestion's, in one place (§8.3). A second implementation of that judgment in the batch lane forks the model — two authorities that drift.
3. **The I-21 wall**: cross-lane ledger reads are excluded by settled IAM law, deliberately.
4. **Additive-only evolution**: the event contract, router allowlist, and seed model all evolve additively; nothing here may reverse settled law.
5. **The ADR-OQ1 marker table** exists (proposed) and can carry two more columns; its invariants M-1…M-3 apply to anything added to it.

## Options

### Option 0 — No source: the second read never runs

With no way to learn the superseded batch, "when Track E applies" never fires and delta detection is one-read always.

This is not a neutral status quo — it **silently violates settled law** whenever a correction arrives: exactly the drops D-2's second read exists to forbid, with no signal that it happened. The lawful degenerate alternative (suspend dropping entirely) collapses into ADR-OQ1's Option 0 economics. Either way, doing nothing is a decision with the forbidden failure shape.

### Option A — Additive `supersedes_batch_id` on the `delivery-registered` event

Ingestion, which already computes supersession at registration and already puts `batch_id` on the event, adds the superseded delivery's batch id as an optional field. It rides the existing path — event → router projection → seed → batch args — is pinned for the batch's lifetime like every other seed value, and needs no new channel. Blast radius: one 002.1 additive ruling (producer + fixtures + G-06), plus two additive 004.1 errata (router allowlist I-8, seed model §6.1 with I-22 narrow-typing).

The honest residual: the field is a **registration-time snapshot** of an adjudication the ledger may later repair. Under the §8.3 race the event says `None` while the truth is a supersession — and for that batch the lane one-reads, silently, in the forbidden direction. The weekly sweep repairs the ledger, never the event. A-alone therefore carries a silent hole that is *larger than* D-2's named residual requires.

### Option B — Batch lane reads ingestion's ledger

Disqualified on the same shape as ADR-OQ1's Option A: it reverses settled law (I-21's wall is deliberate, not incidental), complects the lanes' stores, and couples the lane's correctness to another team's internal schema. Not erratum-grade.

### Option C — Lane-side derivation from the marker table

The event already carries `delivery_key` and `content_hash`; if the ADR-OQ1 marker table records both per batch (as `delivery_key` and `delivery_content_hash` — the lane already has a per-record `content_hash`, and the collision would confuse someone at 2am), the lane could *derive* supersession itself: "prior batch, same `delivery_key`, different `delivery_content_hash`." No producer change, and it self-heals the §8.3 race (derivation at batch time sees what registration time could not).

Disqualified as the *adjudicator* on driver 2: it re-implements ingestion's judgment in a second place. §8.3's matching is not one line — dispositions, manifest fallbacks, completeness modes — and the two implementations drift the first time 002.1 evolves. This is the "two taxonomies" fork the architecture's routing rule exists to refuse. But the observation that the lane *can see key-matches in its own record* is exactly what Option D keeps.

### Option D (emergent) — A as the authored fact, plus a detection-only coherence probe

The event field (Option A) is the **only adjudicator**. The marker table gains two columns (`delivery_key`, `delivery_content_hash` — under M-1…M-3 and N-4), and resolution runs a **probe that detects incoherence but never adjudicates**:

- Event says `supersedes = None`, but **any prior batch with marker presence** — completed *or* partial (the guard-twin rows written beside each append make a kill-in-commit state visible for free) — shares this `delivery_key` with different `delivery_content_hash` → **visible ambiguity** → this batch drops nothing (everything novel).
- Event names a superseded batch whose marker row is `delivery_key`-incoherent with this delivery, **or which has no marker row at all** (absence means *unknown*, per N-3) → **visible ambiguity** → drop nothing. The no-marker-row case bites only pre-marker targets, so this refusal is self-extinguishing.
- Event and marker agree → run the two-read exactly as D-2 states.

The probe converts A's silent race into a visible refusal-to-drop — the same move ADR-OQ1 makes for the first read. It never nominates a superseded target (that would be Option C); on doubt it widens the keep-set, which is the permitted direction by construction.

**The interim composition is lawful, with one named residual.** Before 002.1's field exists, every batch is "supersession unknown" — and the probe alone yields lawful behavior *for record content that post-dates the marker columns*: no key-match against a genesis-complete marker record ⇒ Track E provably doesn't apply ⇒ one-read is lawful; key-match ⇒ refuse to drop. The "provably" is earned by N-4 (marker presence covers partial commits; the record is never pruned), except at **bootstrap**: a correction superseding a *pre-marker* delivery finds no key-match, and the interim floor one-reads — silently, in the forbidden direction. This residual is bounded (it exists only for corrections of pre-marker deliveries, and drops only begin once a marked predecessor exists), it is **named here beside D-2's accepted residual rather than asserted away**, and it is *closed by the 002.1 field* — whose second read targets the named batch's facts directly, needing no marker, with the no-marker-row refusal above as its corroboration floor. The field's latency is therefore not free: it is the width of this window. F-5 remains **unblocked** on the ruling; the ruling's promptness prices the residual.

## Comparison

| Criterion | 0 — no source | A — event field alone | B — read ingestion's ledger | C — lane-side derivation | D — A + detection probe |
|---|---|---|---|---|---|
| Failure mode under the §8.3 race | silent one-read drops, always | **silent one-read drops for the raced batch** | n/a (disqualified prior) | self-heals the race | **visible refusal-to-drop** |
| Direction under the fail-open law | forbidden | forbidden (narrow window) | — | permitted | permitted |
| Supersession authority | none | ingestion (one) | ingestion (one) | **forked — two implementations** | ingestion (one); probe detects, never adjudicates |
| Settled-law posture | violates D-2 silently | additive | **reverses I-21** | additive | additive |
| Blocked on 002.1 ruling? | — | yes | — | no | **no — probe is the lawful interim** |
| Blast radius | zero | 002.1 + router allowlist + seed model (all additive) | IAM + cross-lane coupling | marker columns only | A's radius + two marker columns |
| Forged/wrong field (I-22 surface) | — | a wrong target can *cause* drops the true two-read forbids | — | — | probe cross-checks the named target; incoherence ⇒ refuse to drop |

## Proposed outcome

**Option D.** In Y-statement form: *in the context of* sourcing the superseded batch for D-2's second read, *facing* an adjudication that lives behind the I-21 wall and a registration-time race that no event can outlive, *we propose* the additive `supersedes_batch_id` field on `delivery-registered` (002.1's ruling, with router-allowlist and seed-model errata, I-22 narrow-typed) as the **single authored source**, hardened by a **detection-only coherence probe** over the marker table's new `delivery_key`/`delivery_content_hash` columns, *and against* lane-side adjudication (forks the judgment) *and against* the bare event field (its race window fails silently in the forbidden direction), *to achieve* a second read whose every failure mode is a visible refusal-to-drop, a lawful interim that unblocks F-5 before 002.1 rules, and one authority for what "supersedes" means, *accepting* two more marker columns, the three-surface additive blast radius, and that a probe-refused batch commits redundant facts (the permitted direction, priced).

## Invariants for the ruling (N-1…N-4)

- **N-1 — One authority.** Supersession is adjudicated only by ingestion (§8.3). The lane's probe detects incoherence between the event and the lane's own record and responds by refusing to drop — it never nominates a superseded target. If the probe ever *selects* a batch for the second read, Option C has been rebuilt by stealth.
- **N-2 — The probe checks both directions.** `None`-with-key-match and named-target-key-incoherence both refuse to drop. This is what makes a wrong or forged field (I-22's surface) unable to force a drop: causing one now requires the lane's own append-only record to corroborate the lie.
- **N-3 — The field is a snapshot, and says so.** `supersedes_batch_id` is documented as registration-time adjudication; the ledger's later reconciliation does not reach it. Consumers (F-5, 012's queue views) must not treat its absence as proof of no supersession — absence means *unknown*, and the probe governs. (Same epistemics as D-4/D-5: absence is not an assertion.)
- **N-4 — Probe scope and retention make "no key-match" provable.** The probe scans **marker presence**, not completed batches only — a kill-in-commit state (facts present, completion absent) has committed facts a wrong drop could interact with, and the per-append guard-twin rows make it visible at zero cost. And the scan is only proof against a record **complete to feed genesis**: a correction can re-assert a `delivery_key` from arbitrarily far back, so the marker table is **never pruned** (Phase 1 posture — rows are tiny, mirroring the run ledger's "kept indefinitely"); if retention is ever introduced, a lookback that exceeds the horizon reads as *unknown* ⇒ refuse to drop. This **strengthens ADR-OQ1's M-3** (frontier-only pruning remains the absolute floor, but while the probe consumes these columns the rule is: don't prune at all, or refuse beyond the horizon). Silence on either half reconstructs the invisible-loss shape inside Option D.

**F-5 mechanics obligation — refusals are data.** Every probe refusal is emitted with its reason (`none-with-key-match`, `target-incoherent`, `target-unmarked`, `horizon-exceeded`) as a run-ledger row and an EMF counter (`DeltaProbeRefusals`, reason-dimensioned) — observability, not control; the run ledger's channel contract is untouched. A refusal is safe but *symptomatic* — of the §8.3 race, a marker-write bug, or a misbehaving field — and "never silent drops" has a dual: **never uninspectable keeps**. Without this, a persistent incoherence pays the everything-novel price forever and nobody knows why. The alarm-worthy signal is a *persistent* nonzero rate on one feed.

## Consequences

**Positive.** The two-read law becomes implementable with every failure mode in the permitted direction; F-5 is unblocked immediately on the probe floor; ingestion keeps sole ownership of supersession semantics; the §8.3 race's silent window closes; the marker table's case strengthens again (fourth consumer).

**Negative.** Three additive surfaces must move (002.1 event + fixtures, router allowlist, seed model); the marker schema ruling grows two columns and their M-1…M-3 obligations; probe-refused batches pay the everything-novel price; `delivery_key` enters the lane's record — its classification (partner-derived string) must ride the DS-4/§16.9 tagging obligation.

**Neutral.** D-2's accepted concurrent-sibling residual is unchanged in kind — narrowed in instance, as with ADR-OQ1. This ADR depends on the marker *table* existing, whoever motivates it (DC-1's bind-refusal input does so independently of delta economics) — it does not depend on ADR-OQ1's *ruling*. The bootstrap residual named in Option D joins D-2's accepted residuals for the interim window and expires with the 002.1 field.

## Validation before adoption

1. **Confirm ingestion can emit the batch id, not just the delivery id**: the ledger fold that computes `supersedes` (002.1 §8.6 WON/TAKEN_OVER plans) must resolve the superseded delivery's `batch_id` from its own rows — a 002.1-internal check that decides whether the field is `supersedes_batch_id` (preferred, directly consumable) or `supersedes_delivery_id` plus a lane-side mapping (worse: the mapping needs the marker table anyway).
2. **Fixture discipline**: the field lands additively in the v1 event fixtures (G-06's corrected-re-send golden gains it; a `None`-race fixture pins the probe's refusal path). The probe's own goldens belong beside F-5's in the build epic's register.
3. **Sequencing check at `hpp.13`**: design F-5 against the pinned seed interface with the wait named (the P-6 idiom) — probe floor now, precision when 002.1's ruling lands.

## References

- 007 `design/007_record.md` D-2 (two-read law, accepted residuals) · D-4 (Track E accretion, absence asserts nothing)
- 002.1 `design/002.1_data_ingestion_lld.md` §7 (ledger schema: `supersedes`, `delivery_key`), §8.3 (matching + the concurrent-correction race), §8.6 (WON/TAKEN_OVER supersession detection), §9.5 (reconciliation sweep), G-06
- 004.1 `design/004.1_runner_spine_lld.md` I-8 (router allowlist), I-21 (ledger unreachable), I-22 (event trust boundary), §6.1 (seed model)
- `design/adr-oq1-predecessor-source.md` (marker table + M-1…M-3)
- `contracts/fixtures/events/delivery-registered/` (v1 payload surface)
- Beads: `conveyer-hpp.13` (F-5 owner), the 002.1 additive-ruling bead created alongside this ADR
