# Large-Volume Batch Data Processing
## Same Programming Model, Second Execution Lane — Architecture Description (Target)

**Status:** Draft v0.1 · **Layer:** Data Intelligence Layer · **Pane:** Integrated Business Process and Sales Transaction Data (batch / lakehouse side) · **Pattern:** Immutable-fact pipeline on Iceberg (event-sourcing programming model, batch runtime)

> **About this document.** The operational pane runs an event-driven architecture (Event Sourcing + CQRS — see *Operational Event-Driven Architecture, As-Is*) for real-time, low-latency workloads. This document describes a **second execution lane** for large-volume batch data, where latency of seconds to minutes is acceptable and cost-per-TB dominates. The batch lane **reuses the programming model** of the operational architecture — raw data in, pure enrichment and application, immutable recorded facts, derived and disposable current state — and instantiates it on a substrate priced for volume: **Iceberg on S3**, executed as Spark jobs. It does **not** replace the event-driven architecture. The two lanes share contracts and identity; they differ only in runtime and latency class.
>
> **Audience.** Written for two readers: a **developer or LLM agent** implementing a pipeline within the framework (Sections 5–8 are the contract), and a **platform/leadership** reader who needs the shape, the boundary with the event lane, and the operating rationale (Sections 1–4, 9–11).

---

## 1. Context — Two Lanes, One Discipline

The Data Intelligence Layer processes change through two lanes:

| Lane | Runtime | Latency class | Optimized for | Substrate |
|---|---|---|---|---|
| **Event lane** *(existing, as-is doc)* | Domain command processor (interceptor pipeline) | Milliseconds — interactive commands, event-driven workflow | Per-command validation, immediate ack, live snapshot freshness | MongoDB event store + change streams + `domainDB` |
| **Batch lane** *(this document)* | Framework-run Spark jobs (Glue / EMR Serverless) | Seconds to minutes — files, feeds, backfills, high-volume history | Cost per TB, throughput, replay at scale, analytical co-location | Iceberg on S3 under SageMaker Lakehouse governance |

**Routing rule per dataset:** does anything downstream need this change reflected in under a second? Event lane. Is seconds-to-minutes acceptable? Batch lane. Typical batch-lane residents: carrier statement files, membership history, AOR/client-summary feeds, SG/LG backfills, and any historical replay.

**What is shared, not forked:** the event/fact taxonomy and contracts (owned by the *Event Model* document), the **domain-id (aggregate-root id)** as the identity anchor, and business-rule definitions for any domain touched by both lanes. The lanes are two runtimes of one model — not two models.

**What is deliberately different:** the batch lane relaxes everything the event lane pays for latency — no hot log, no per-command commits, no streaming order guarantees. Ordering, idempotency, and rebuild are achieved structurally within jobs (Section 4).

---

## 2. Scope & Responsibilities

The batch lane is responsible for:

1. **Landing raw data verbatim** — files and feeds registered append-only into a raw zone, exactly as received, with batch and source lineage.
2. **Validating against contracts** — schema and quality checks with quarantine, never silent drops or whole-file failures.
3. **Producing immutable facts** — pure transforms that turn (raw × current state) into recorded facts, appended once, never updated.
4. **Maintaining derived current state** — fold/merge of facts into current-state tables; disposable and rebuildable by re-running the fold.
5. **Publishing batch lifecycle events** — `batch-started` / `batch-completed` onto the event backbone so workflows and materializations can react.
6. **Cross-materializing selected outputs** — pushing designated current-state datasets into `domainDB` for operational serving (the "document store materialized from lakehouse tables" of the target architecture).

**Out of scope:** interactive command processing (event lane), analytical mart/metric computation and BI serving (analytical spoke — though it consumes the fact layer directly), and source-system internals.

---

## 3. Architecture Characteristics

The batch lane inherits the defining characteristics of the operational architecture, restated for the batch runtime:

**3.1 Immutable facts are the system of record.** The fact tables are append-only Iceberg tables. Raw and fact tables carry **append-only Lake Formation grants** — immutability is enforced by IAM, not convention. History is complete and replayable.

**3.2 Write path and read path are separated.** Facts are produced by pipeline jobs; current state is a separate, materialized projection. The two are optimized independently (CQRS in batch form).

**3.3 Current state is ephemeral and rebuildable.** Current-state tables are derived by folding facts. They can be discarded and rebuilt by re-running the fold over the fact table — and Iceberg **time travel** yields "state as of" any table snapshot for audit and reproducibility.

**3.4 Pure functional core, effects only at the edge.** All domain logic — apply, business-rule validation, fold — is implemented as **pure functions over dataframes**. Reads (co-effects) and writes (effects) are performed exclusively by the framework runner. Pipeline code physically cannot perform I/O (enforced in CI — Section 8).

**3.5 Context-enriching flow — data is added, never stripped.** The runner accumulates lineage, co-effects, produced facts, and validation results across stages; downstream stages see everything upstream produced.

**3.6 Idempotency by construction.** Delta detection (content hash against current facts) plus dedup keys `(batch_id, record_key, content_hash)` make file reruns and restarts no-ops. Purity makes re-execution safe; delta detection makes it silent.

**3.7 Deterministic ordering without a stream.** Ordering is per aggregate, applied inside the fold: group by domain-id, order by sequence/event-time with deterministic tiebreakers (source timestamp, then record hash). No order-guarantee middleware is required.

**3.8 Batch-coherent boundaries.** Every fact carries its `batch_id`; `batch-completed` events gate downstream consumption so no consumer observes half a batch.

**3.9 Native analytical co-location.** Facts and current state land governed in the lakehouse. The analytical spoke consumes them directly under the same catalog and permission model — no sync hop for batch-lane data.

**3.10 Domain-scoped aggregates.** Facts carry the same domain-id (aggregate-root id) as the event lane, so batch-lane facts and event-lane events join on the same aggregate (e.g., a carrier-statement fact and a quote-saved event on one client lifecycle).

---

## 4. Architectural Pattern — Immutable-Fact Pipeline (Medallion with Discipline)

The lane is structurally a medallion architecture — **raw ≈ bronze, immutable facts ≈ silver, current state ≈ gold** — but imports the discipline of the operational architecture that typical medallion implementations lack:

- Raw and fact layers are **genuinely append-only** (IAM-enforced, not convention).
- Current state is **explicitly a throwaway projection**, rebuildable from facts.
- All transformation logic is **pure**, with co-effects and effects as declared inputs and outputs rather than buried side reads and writes.

This discipline is what makes the lane replay-safe, auditable, and testable — the same properties Section 3 of the as-is document claims for the event lane.

**What the batch lane relaxes relative to the event lane:** no fronting log and no commit-latency constraint (Iceberg micro-batch/file-cadence appends are its sweet spot); table-level Iceberg concurrency is sufficient because writers are jobs, not concurrent commands; ordering and idempotency are solved inside jobs (3.6–3.7) rather than by streaming infrastructure.

---

## 5. Stage Model — The Onion, Translated to Batch

The framework runner executes a **fixed stage sequence**; a pipeline package supplies only the pure stages. The sequence is the batch translation of the processor's interceptor pipeline:

| # | Stage | Owner | Purpose | Effectful? |
|---|---|---|---|---|
| 1 | **land** | framework | Register the raw file/feed into the raw Iceberg table **verbatim**; stamp `batch_id`, source URI, received-at. The raw record is the batch lane's "command," preserved exactly. | write (raw zone) |
| 2 | **pre_check** | framework, driven by pipeline schema | Validate raw rows against the raw contract; failures stream to the **quarantine table** with reasons; the job continues. | write (quarantine) |
| 3 | **pull** | framework, driven by declared co-effects | Load declared inputs (current-state tables, reference data) as dataframes. Pipelines cannot read anything undeclared. | reads (co-effects) |
| 4 | **apply** | **pipeline — pure** | `(raw_df, co_effects) → facts_df`. The one function carrying domain logic: enrich, join, derive, and shape raw rows into candidate facts. No I/O permitted. | **pure** |
| 5 | **post_check** | **pipeline — pure** | Business-rule validation over produced facts; violations to quarantine with reasons. | **pure** |
| 6 | **commit** | framework | **Delta-detect** against existing facts via content hash; append only genuine changes to the fact table; idempotent on `(batch_id, record_key, content_hash)`. | **write (facts)** |
| 7 | **fold** | pipeline — pure fold fn, or framework default (last-write-wins) | `MERGE INTO` current state: group by domain-id, apply in deterministic order (3.7). | write (current state) |
| 8 | **publish** | framework | Emit `batch-completed` with counts (rows in, facts emitted, quarantined); update catalog stats; trigger compaction if thresholds hit. | side effects |

**Why this shape matters.** Exactly as in the event lane: the expensive correctness work (apply, post_check, fold logic) is pure and deterministic — safe to re-run, trivially testable — while all writes are concentrated in framework-owned stages written once. The implementation surface for a new pipeline collapses to pure functions plus contracts (Section 7), which is what makes the lane safely implementable by developers *and* LLM agents (Section 8).

---

## 6. AWS Service Mapping

| Concern | AWS choice | Notes |
|---|---|---|
| Raw zone, fact tables, current state, quarantine | **Iceberg on S3**, cataloged in **SageMaker Lakehouse / Glue Catalog** | Raw + fact tables: append-only **Lake Formation** grants |
| Compute | **Glue jobs (Spark)** initially; **EMR Serverless** when scale/cost tuning warrants | Both run the same runner library — switching is a config change |
| Orchestration | **Step Functions**, triggered by **EventBridge** (S3 object landing or schedule) | Deliberately the same engine as event-lane workflows — one operational skill set. MWAA only if cross-pipeline DAG dependencies become genuinely complex |
| Contracts | Schemas in **Git** (versioned, PR-reviewed) + **Glue Schema Registry**; quality rules via Glue Data Quality / deequ | The *Event Model* document is the human-readable source; schemas are its machine form |
| Lineage & audit | `batch_id` + source-lineage columns stamped by the framework; catalog-level lineage | Every fact traces to the exact raw file and job run that produced it |
| Batch lifecycle events | `batch-started` / `batch-completed` on **EventBridge** | The seam to the event lane: workflows, materializations, and metrics jobs react to batch completion as they react to domain events |
| Cross-materialization to `domainDB` | Glue job upserting from current-state Iceberg into **MongoDB** | Only for datasets that must surface operationally; triggered by `batch-completed` |
| Iceberg maintenance | Framework-scheduled compaction + snapshot expiration | Invisible to pipeline authors |

**Fact table baseline schema:** `domain_id`, `aggregate_seq` / event-time, `fact_type`, `batch_id`, `record_key`, `content_hash`, source lineage, payload (typed struct) — partitioned by `bucket(domain_id)` plus a time dimension.

> **Erratum (2026-08-30 — 007.1 F-3, per `design/adr-oq6-fact-partition-spec.md`, Accepted):** the partition clause above is **superseded for fact tables**. Every correctness-path fact-table read (commit guards, fact-presence doors, `read_batch`, delta predecessor reads, rerun anti-joins) is `batch_id`-keyed, so fact tables — and 007.1's batch-marker table — partition **`identity(batch_id)`**: absent-batch probes become metadata-only misses, and the compaction-clustering discipline dissolves structurally (each partition receives exactly one append and is never written again). The `bucket(domain_id)` + time instinct is **re-aimed, not refuted**: it describes the domain-keyed *perception* profile, which lives on the state tables — where 007.1 F-3 weighs it on its own merits and rules unpartitioned-with-sort-order (its PS-3: facts and state never share a layout by analogy). Occasional analytical reads over facts ride Iceberg column stats, with additive partition-spec evolution reserved under 008's cardinality review.

---

## 7. Pipeline Package — The Unit of Implementation

A pipeline is a thin, declarative package; everything operational lives in the runner.

```
pipelines/<domain>/
  pipeline.yaml      # source pattern, co-effects, output tables, schedule, fold mode
  schemas/           # raw contract + fact contract (versioned, additive-only)
  transforms.py      # apply(), post_check(), optionally fold()
  tests/             # golden input/output fixtures
```

- **`pipeline.yaml`** declares: the source landing pattern (S3 prefix / feed), the co-effect tables the pipeline may read, the fact and current-state output tables, the schedule or landing trigger, and the fold mode (custom vs. framework default).
- **`schemas/`** hold the raw and fact contracts. Versioning is **additive-only**; a breaking change means a new table, because folds and downstream consumers depend on replayability.
- **`transforms.py`** contains only pure functions over dataframes. No `boto3`, no `spark.read`/`spark.write`, no network — enforced mechanically (Section 8).
- **`tests/`** hold golden fixtures: sample raw input + co-effect snapshots → expected facts and expected current state.

**Deployment is config, not code.** Merging a new `pipeline.yaml` to main triggers IaC (CDK/Terraform module) that provisions the Glue job, Step Functions definition, EventBridge rule, and Lake Formation grants from the template. Humans review the PR; no one hand-builds infrastructure.

---

## 8. Implementation by Developers and LLM Agents

The framework is designed so that the implementation surface — pure functions plus two schema files — is small enough to be generated and verified without touching AWS:

- **Contract-first generation.** The input to a developer or agent is: raw contract, fact contract, and a mapping/rules spec. The output is `transforms.py` + test fixtures. Nothing else.
- **Purity enforced mechanically.** CI runs an import/AST linter on `transforms.py` — any I/O, credential, or network usage fails the build. An agent physically cannot ship a side effect.
- **Golden tests as the acceptance gate.** Fixtures execute under local Spark (or DuckDB for speed) in CI. If `apply` + `post_check` + `fold` reproduce expected outputs, the pipeline is behaviorally verified before it ever sees a cluster. Agents iterate against this loop autonomously.
- **A generation spec ("skill") as the recipe.** One authored document — *"to add a pipeline: produce these four files, obeying these invariants"* — plus two exemplar pipelines in the repo. This is the onboarding path for junior developers and agents alike; well-specified sources onboard in days.
- **Review model.** Human review concentrates where judgment lives: the contracts and the mapping/rules spec in the PR. The framework guarantees everything else.

---

## 9. Cross-Cutting Concerns / Quality Attributes

| Concern | Position | Status |
|---|---|---|
| **Source of truth** | Append-only Iceberg fact tables; current state derived | Documented |
| **Consistency model** | Facts durable on commit; current state consistent at fold completion; downstream gated on `batch-completed` | Documented |
| **Auditability** | Full fact history + batch/source lineage per fact + Iceberg time travel | Documented |
| **Rebuild / recovery** | Re-run fold over fact table; "as-of" via time travel | Documented |
| **Ordering** | Per-aggregate, deterministic within fold (3.7) | Documented |
| **Idempotency** | Delta detection + dedup key; reruns are no-ops (3.6) | Documented |
| **Immutability enforcement** | Append-only Lake Formation grants on raw + fact tables | Documented |
| **Error handling** | Quarantine with reasons, never silent drops or whole-file failure; standing remediation workflow (Step Functions + review queue) | **[TO DETAIL]** remediation workflow design — solves at platform level the same gap as today's unhandled EDW↔EDM fallouts |
| **Schema / contracts** | Git + Schema Registry; additive-only evolution | Documented; **[TO DETAIL]** contract governance with Event Model doc |
| **Purity / testability** | Pure stages, CI-enforced; golden tests in CI | Documented |
| **Security & governance** | Lakehouse catalog + Lake Formation, uniform with analytical spoke | Context |
| **Cost** | S3/Parquet storage + on-demand (spot-priced) job compute; MongoDB footprint scoped to millisecond-class needs only | Documented |

---

## 10. Sequencing

1. **Runner + first real pipeline end-to-end** — the carrier-x or carrier-y reference feed (already batch, already needed). Extract the template from the working example, not from a vacuum.
2. **Second pipeline** (the other of the two) validates the template and the generation spec.
3. **SG backfill as the third pipeline** — proves the scale story and connects Small Group into the lifecycle dataset.

Framed within the AWS engagement, the runner and template are a **Workstream 3 (scaffolding & accelerators)** deliverable; individual pipelines are **Workstream 1 (functional data products)** deliverables; this document is input to the Phase 1 architecture and design track.

---

## 11. Related Documents

- **Operational Event-Driven Architecture (As-Is)** — the event lane; source of the programming model this lane reuses.
- **Event Model** *(separate document)* — shared taxonomy and contracts governing both lanes.
- **Salesverse Data Hub Brief** — target lakehouse architecture, workstreams, and governance capabilities this lane lands on.

---

## 12. Glossary

| Term | Meaning |
|---|---|
| **Batch lane** | This architecture: the immutable-fact pipeline on Iceberg for latency-tolerant, high-volume data |
| **Event lane** | The existing event-driven architecture (Event Sourcing + CQRS) for millisecond-class workloads |
| **Raw zone** | Append-only landing of source data verbatim, with batch/source lineage ("bronze") |
| **Fact table** | Append-only Iceberg table of immutable recorded facts — the lane's system of record ("silver") |
| **Current state** | Derived, disposable projection folded from facts ("gold") |
| **Fold** | Pure merge of facts into current state, grouped by domain-id, deterministically ordered |
| **Co-effects** | Declared inputs loaded by the framework for the pure stages (current state, reference data) |
| **Effects** | Writes performed exclusively by framework stages (commit, fold-merge, publish) |
| **Quarantine** | Table of rows failing pre_check/post_check, with reasons; subject to a remediation workflow |
| **Delta detection** | Content-hash comparison against existing facts so only genuine changes are appended |
| **`batch_id`** | Lineage identifier stamped on every fact; anchors idempotency, audit, and batch-coherent consumption |
| **domain-id (aggregate-root id)** | Shared identity anchor with the event lane; joins facts and events on one aggregate |
| **Runner / framework** | Platform-owned library executing the fixed stage sequence and all effects |
| **Pipeline package** | The thin, declarative unit a developer or LLM agent implements (yaml + schemas + pure transforms + golden tests) |
| **Time travel** | Iceberg's table-snapshot history, used for as-of rebuilds and audit (distinct from domain snapshot documents) |

---

## 13. Open Items

1. **Quarantine remediation workflow** — review queue design, ownership, SLA; shared governance candidate with the EDW↔EDM fallout gap.
2. **Contract governance across lanes** — how the Event Model document governs both event-lane contracts and batch-lane schemas; single authored source for business rules used in both runtimes.
3. **Cross-materialization inventory** — which current-state datasets materialize into `domainDB`, at what cadence, keyed how.
4. **Compute placement thresholds** — criteria for moving a pipeline from Glue to EMR Serverless.
5. **Generation spec ("skill") authoring** — the agent/developer-facing implementation recipe and its exemplar pipelines.

Send detail on any of these and I'll fold it into the matching section.
