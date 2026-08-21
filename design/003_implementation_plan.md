# Implementation Phase Plan
## Realizing the Batch Lane — Sequencing, Seams, and Design-Doc Map

**Status:** Draft v0.2 · **Parent:** *001 Batch Data Processing Architecture* · **Purpose:** decompose the remaining work into phases, each anchored to a seam contract and a design doc to be authored before implementation. This document is a *derived view* over 001 §10, 001 §13, and 002 §16 — disposable, rebuilt as phases complete. The module design docs are the facts; this is the fold.

> **v0.2:** the stage sequence is now detailed per stage-cluster (docs 004–008), following the per-stage treatment 002/002.1 gave ingestion, rather than one monolithic runner doc.

---

## 0. Where We Are

**Complete — 002 / 002.1 (ingestion, Stage 0):** delivery ledger, registration core, `s3-push` + `sftp-pull` drivers, completeness model, absence detection, two exemplar feeds (`carrier-x/commission-statements`, `carrier-y/renewal-statements`), contracts, Terraform, test suite.

**Deferred inside 002, carried forward here:** `api-pull` / `db-unload` drivers (stubs; Track D), Lake Formation integration (IAM-only per D-9; Track B), secret rotation (Track B).

**The open seam:** `delivery-registered` is emitted and nothing consumes it. Everything downstream of that event is unbuilt.

---

## 1. Slicing the Stage Sequence

Ingestion was an independently deployable module with an event seam. The stages are different in kind: **functions sharing one execution context** (the context-enriching flow, 001 §3.5). Their seams are *data contracts* — what each stage receives and what it adds. So the sequence is detailed as:

- **One spine doc (004)** defining the execution context and stage protocol — the contract every stage doc writes against. Authored first; nothing else can be precise without it.
- **Four stage-cluster docs (005–008)**, clustered by ownership and flow role, not one per stage — `land` and `pull` are thin, and doc boundaries should follow real seams:

| Doc | Cluster | Stages | Flow role | Owner |
|---|---|---|---|---|
| 005 | Admission | `land`, `pre_check` | remember (raw) + validate | framework |
| 006 | Pure core | `pull`, `apply`, `post_check` | co-effects + **transform** | framework loads; **pipeline computes** |
| 007 | Record | `commit`, `fold` | remember (facts, state) | framework (pipeline may supply `fold`) |
| 008 | Broadcast | `publish` | route / announce | framework |

Each takes the 002 pattern: architecture doc (`00N`) then LLD (`00N.1`) when implementation starts.

---

## 2. Phase Map

| Phase | Deliverable | Design docs | Depends on | What it proves |
|---|---|---|---|---|
| **1** | Stage sequence: spine + four clusters | **004–008** (+ `.1` LLDs) | 002 (done) | Identity pipeline runs end-to-end from a fixture delivery; reruns are no-ops; kill/restart converges |
| **2** | Pipeline package contract + first real pipeline (carrier-x reference feed) | **009** | Phase 1 | Thin-package model works on real data; template extracted from a working example (001 §10.1) |
| **3** | Second pipeline (carrier-y reference feed) + generation spec ("skill") | **010** | Phase 2 | Abstraction test: two pipelines, one contract, no special cases; agents can stamp out pipelines (001 §8) |
| **4** | Cross-materialization to `domainDB` | **011** | Phase 2 | Publish seam feeds operational serving (001 §13.3) |
| **5** | Quarantine remediation workflow | **012** | Phase 1 | "Errors as data" closes its loop: queue, ownership, SLA (001 §13.1) |
| **6** | Scale proof: SG backfill + Glue→EMR thresholds | **013** | Phase 3 | Cost/volume story; declared placement criteria (001 §13.4) |

Phases 1→2→3 sequential; 4 and 5 design-parallel after their dependency; 6 last — thresholds need real pipelines to measure.

---

## 3. Phase 1 — The Stage Sequence (docs 004–008)

**Authoring order:** 004 first, alone. Then 005–008 can be drafted in parallel against 004's context contract; expect 004 revisions from the friction (cheap now).

**Build order:** spine + admission first (005 gives the first observable artifact — raw rows + quarantine from a real registered delivery), then 006/007 together (facts are meaningless without commit), then 008.

**Phase definition of done:** fixture delivery in → facts, current state, `batch-completed` out via a synthetic identity pipeline; rerun of the same delivery yields zero new facts; a mid-sequence kill and restart converges to the same state; purity linter (no `boto3`/`spark.read` in pipeline code) in CI.

### 3.1 Doc 004 — Runner spine

The execution context and stage protocol. Decisions to settle:

1. **Execution-context shape** — the accumulating value each stage receives and extends (lineage, raw df, co-effects, candidate facts, validation results). What is added at each stage; nothing ever removed (001 §3.5).
2. **Stage protocol** — the signature of a stage; how framework stages and pipeline-pure stages plug into one sequence; where the pipeline package's functions are bound.
3. **Orchestration shape** — one Spark job driving all stages vs. Step Functions states per stage; where retry boundaries sit given Iceberg's per-table atomic commits.
4. **Partial-failure semantics** — restart-from-which-stage; what makes each stage idempotent so restart is always safe.
5. **Batch lifecycle** — `batch-started` timing; what a consumer may assume between the two events.
6. **Runner packaging** — relationship to ingestion's effects/records idiom (002.1 §7.7); one runner library or two.
7. **Test substrate** — local Spark vs. DuckDB for golden tests (001 §8); accepted divergence risk.
8. **Purity enforcement** — the AST/import linter, shared with ingestion's plugin linter or forked.

### 3.2 Doc 005 — Admission (`land`, `pre_check`)

1. Raw table schema and the registered-delivery → raw-rows mapping; how the *verbatim* claim survives decompression/encoding (`format_hints` interpreted here, at read time — originals untouched, 002 §5.3).
2. Raw-contract validation semantics: what a raw contract can express; row-level failure → quarantine, never whole-file failure (001 §2.2).
3. Quarantine table schema, reasons taxonomy, and grants — designed for Phase 5's workflow, ahead of it.
4. Multi-object deliveries: one delivery → one raw batch; ordering and attribution of rows to objects.
5. `pre_check` mechanics: framework-driven from the declared schema (no pipeline code), or pipeline-supplied rules — and the boundary with `post_check` (content vs. business rules).

### 3.3 Doc 006 — Pure core (`pull`, `apply`, `post_check`)

1. Co-effect declaration granularity — table-level, or table + columns/filter; and **snapshot semantics**: co-effects must read a consistent as-of state (batch-coherent perception applied to the pipeline's own reads).
2. `apply` signature and dataframe contract: `(raw_df, co_effects) → facts_df`; what shape "candidate facts" have before commit stamps lineage.
3. `post_check` expression — declarative rules vs. pure code; violations carry reasons to quarantine.
4. Business-rule sourcing — rules referenced from the Event Model (Track A), not invented in transforms; where the rule/policy layer sits so *why* stays out of *how*.
5. The authoring surface preview — what a pipeline author writes here becomes the package contract frozen in 009.

### 3.4 Doc 007 — Record (`commit`, `fold`)

1. **Fact canonicalization** — the normative content-hash algorithm for delta detection (the fact-side analog of 002 §6.5): exactly what is hashed, exclusions (batch_id, received-at), stability across Spark versions.
2. Dedup mechanics on `(batch_id, record_key, content_hash)`; `record_key` definition contract.
3. Delta detection vs. superseded deliveries (Track E): default = corrected batch's genuine changes become the newest facts; per-feed-class overrides.
4. **Fold contract** — default last-write-wins mechanics; deterministic tiebreakers (001 §3.7: sequence/event-time, source timestamp, record hash); the contract a custom `fold()` must satisfy; proof obligation that fold(all facts) ≡ incremental folds.
5. MERGE mechanics, full-rebuild path, and as-of reads via time travel — the rebuild story made operational, not aspirational.
6. Append-only grants on fact tables; commit-stage-only write path (002 §9's access rules extended downstream).

### 3.5 Doc 008 — Broadcast (`publish`)

1. `batch-completed` payload — counts (rows in, facts emitted, quarantined), schema governed with the event backbone.
2. Consumer gating contract — what "you may now read batch N" means precisely; how consumers subscribe.
3. Catalog stats, compaction, snapshot expiration — cadence vs. the replay window promised in 001 §3.3.
4. The trigger seam to cross-materialization (011) — publish announces; it does not materialize.

---

## 4. Phase 2 — Pipeline Package + First Pipeline (doc 009)

Freeze the package contract (001 §7: `pipeline.yaml`, schemas, `transforms.py`, golden tests) by building the exemplar pipeline against the real `carrier-x` feed. IaC: merging `pipeline.yaml` provisions job, Step Functions, EventBridge rule, grants.

**Done when:** SFTP fixture → landing → facts → current state → `batch-completed` in a deployed environment; golden tests green in CI without AWS; package tree matches 001 §7 with nothing extra.

**Decisions:** `pipeline.yaml` schema (FeedConfig analog); schema format/home for contracts — needs Track A's provisional answer; `record_key` + `fact_type` taxonomy for the carrier-x feed tied to the Event Model; golden-fixture format shared with ingestion's conventions.

---

## 5. Phase 3 — Second Pipeline + Generation Spec (doc 010)

Renewal-statements pipeline (`carrier-y`) built *by following a draft generation spec*, then the spec corrected from friction. Ships: spec, two exemplar pipelines, review checklist (judgment concentrates in contracts + mapping spec, 001 §8).

**Done when:** a developer or agent, given driver choice, contracts, and mapping spec, produces a mergeable package; pipeline #2 needed no runner changes (if it did, the 004–008 interfaces were wrong — fix now, cheaply).

**Decisions:** spec format/location; the agent loop against golden tests; one generation spec covering source plugins (002 §16.5) and pipelines, or two coordinated.

---

## 6. Phase 4 — Cross-Materialization to `domainDB` (doc 011)

The `batch-completed`-triggered upsert from designated current-state tables into MongoDB (001 §6). Mostly an inventory exercise.

**Decisions:** the inventory (001 §13.3); idempotent upsert keying (domain-id — defend it); staleness contract ("current as of batch N"); failures block or trail the batch.

**Seam to defend:** materialized documents are a projection, never a system of record — nobody writes to them directly.

---

## 7. Phase 5 — Quarantine Remediation (doc 012)

Review queue, per-feed ownership, SLA, re-drive path. Shared-governance candidate with the EDW↔EDM fallout gap (001 §9).

**Invariant:** remediation produces *new deliveries or new facts* (correcting entries), never edits to quarantine, raw, or fact rows. Fixed data re-enters through the front door — a corrected re-send (002 §8) or a compensating fact.

**Decisions:** queue substrate and console (likely shared with Track C); remediation-outcome dispositions (accrete, don't update — the ledger's rule); SLA alerting via existing expectation machinery or separate; ownership registry location.

---

## 8. Phase 6 — Scale Proof + Placement Thresholds (doc 013)

SG backfill as pipeline #3; measured Glue vs. EMR Serverless crossover; partition validation (`bucket(domain_id)` + time) at backfill volume.

**Decisions:** placement thresholds as declared criteria (volume, runtime, spot economics), not folklore; fold determinism under very large per-aggregate histories; whether backfill batches need distinct lifecycle treatment.

---

## 9. Cross-Cutting Tracks (no phase gate; design when first needed)

| Track | Item | Trigger |
|---|---|---|
| **A** | Cross-lane contract governance (001 §13.2) — one authored source for taxonomy and rules, machine forms derived | Provisional answer needed by doc 006 §4 and doc 009; full design with the Event Model doc's owners |
| **B** | Lake Formation integration (002.1 D-9) + secret rotation (002 §16.2) | When the lane joins the shared lakehouse catalog; rotation when ops requires |
| **C** | Ledger fold serving / ops console (002 §16.1) | With Phase 5's review queue — likely the same console |
| **D** | `api-pull` / `db-unload` drivers (002 §13.4) | First resident feed needing one; each is a driver, not a redesign |
| **E** | Supersession semantics downstream (002 §16.4) | Decided in doc 007 §3, per feed class |

---

## 10. Doc Numbering

Module/stage architecture docs take the next integer (004–013 as mapped above); LLDs suffix `.1`. This plan is renumbered content-free glue — supersede it freely as phases close.
