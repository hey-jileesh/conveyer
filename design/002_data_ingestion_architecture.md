# Data Ingestion Module
## Acquisition & Registration — Architecture Description (Target)

**Status:** Draft v0.1 · **Layer:** Data Intelligence Layer · **Parent:** *Large-Volume Batch Data Processing* (batch lane) · **Position:** Stage 0 — everything between a source system and the `land` stage · **Pattern:** Driver/plugin acquisition with a reified delivery ledger

> **About this document.** The batch lane's architecture description defines the stage sequence beginning at `land` — as if files materialize in the raw zone by themselves. This document specifies the module upstream of that boundary: how bytes travel from source systems into the landing zone, how deliveries become identified, idempotent, auditable units of work, and how new integrations are added as **source plugins** without writing effectful code. It is one of a set of per-stage documents; each stage module is independently developed, tested, and integrated against the contracts defined at its seams.
>
> **Audience.** A **developer or LLM agent** adding a source integration (Sections 5–8 are the contract), and a **platform reader** who needs the shape, the boundary with the stage sequence, and the operating rationale (Sections 1–4, 9–12).

---

## 1. Context — Position in the Pipeline

```
source system ──▶ [ INGESTION MODULE ] ──▶ stage sequence (parent doc)
                  acquire    register        land → pre_check → pull → apply
                  (move)     (remember)      → post_check → commit → fold → publish
```

The ingestion module owns two of the four flow roles, and only two:

| Role | Sub-component | Job |
|---|---|---|
| **Move** | Acquisition (transport drivers + source plugins) | Copy bytes from a source to the feed's landing prefix, **verbatim** |
| **Remember** | Registration (delivery ledger) | Record that a delivery happened: identity, content hash, completeness, disposition, `batch_id` |

**The downstream boundary.** The module's output — its entire interface to the stage sequence — is three things: (1) immutable objects in the feed's landing prefix, (2) an appended row in the delivery ledger, and (3) a `delivery-registered` event carrying `{feed_id, batch_id, object_uris, content_hash}` that triggers the feed's Step Functions execution. The `land` stage consumes a *registered delivery*; it never discovers files by listing prefixes.

**The upstream boundary.** Source-system internals are out of scope. The module sees sources only through transport drivers (Section 5).

**What this module never does:** parse, transform, filter, decompress-and-discard, or interpret bytes in any way; write to raw, fact, current-state, or quarantine tables; mint facts. All interpretation belongs to the pure stages (`pre_check`, `apply`), where it is versioned, golden-tested, and replayable. If interpretation happened here, it would happen in the one place that is effectful, untested, and unreplayable — and the raw table's "verbatim" claim would be quietly false.

---

## 2. Scope & Responsibilities

The ingestion module is responsible for:

1. **Acquiring source data verbatim** — pull (SFTP, API, DB unload) or receive (partner push to S3), byte-for-byte, into the feed's landing prefix.
2. **Registering every delivery** — appending a delivery ledger row with lineage, content hash, and disposition; never a silent skip.
3. **Minting `batch_id` idempotently** — deterministic on content, so re-deliveries and retries collapse to no-ops.
4. **Asserting completeness** — a delivery enters the stage sequence only when its declared completeness condition (manifest, trailer, or declared timer) holds.
5. **Detecting absence** — emitting `delivery-overdue` when an expected delivery does not arrive; "the file didn't come" is information.
6. **Triggering the stage sequence** — emitting `delivery-registered` onto EventBridge for the feed's Step Functions execution.

**Out of scope:** the stage sequence itself (parent doc), schema/quality validation (`pre_check` — the module checks *completeness*, not *content*), quarantine remediation, and source-system internals.

---

## 3. Information Model

**The delivery is the fact.** A delivery is "source X delivered content H for feed Y at time T." It happened; it cannot be updated. A corrected re-send is a *new* delivery that supersedes the old one — a correcting entry, never an erasure.

**The feed is the identity.** A feed (e.g., `carrier-x/commission-statements`) is a putative entity associated with a succession of deliveries over time. The feed's "current watermark" is a *derived* value — a fold over its deliveries — not a stored mutable cursor.

**The content hash is the value's true name.** Object paths and filenames are routing conventions; identity of *content* is the hash. The ledger maps meaningless names (URIs) to meaning (feed, batch, disposition).

**Questions the module must answer**, all as queries over the ledger: What arrived, when, from where? Is this delivery complete? Have we seen this content before? Which `batch_id` did this delivery become, and which raw rows trace back to it? What was expected but has not arrived? Why was a given file skipped (answer: `disposition = duplicate`)?

---

## 4. Architecture Characteristics

**4.1 The landing zone is append-only, by construction.** Objects are write-once: bucket versioning on, overwrite denied by bucket policy, received-at–prefixed paths so name collisions cannot occur. The landing zone extends the batch lane's IAM-enforced immutability one hop upstream. It is never a working area; nothing is ever "fixed in place."

**4.2 Process is reified as the delivery ledger.** Deliveries exist as *data* — an append-only table — not merely as S3 events and Step Functions executions (which are effects). The ledger drives idempotency, audit, duplicate explanation, and absence detection.

**4.3 Idempotency by construction, end to end.** `batch_id` is minted deterministically from `(feed_id, content_hash)` (or manifest id). Acquisition retries, duplicate S3 events, and partner re-sends of identical content all resolve to the same `batch_id` and dedup to ledger no-ops. Combined with the parent doc's commit-stage delta detection, the idempotency chain runs unbroken from source to fact table.

**4.4 Completeness is an assertion, not an inference.** What constitutes a complete delivery is declared per feed and asserted by the source wherever possible (manifest-last, trailer counts). Timers are a last resort and must be *declared* — a reviewed, named trade-off, never a constant buried in an orchestration definition.

**4.5 Plugins are data plus pure functions; the framework owns every effect.** New integrations are declarative source plugins configuring a small set of platform-owned transport drivers. Extension points accept **data, not callbacks** (Section 5).

**4.6 Isolation per plugin.** Each source plugin is provisioned an IAM role scoped to its own landing prefix, its own secret, and its own ledger partition. A plugin physically cannot write to another feed's landing path or read another feed's credentials. Blast radius of a bad plugin: one feed.

**4.7 Cursor state gets the epochal treatment.** Pull drivers derive "where did I leave off" by folding the ledger (max window successfully acquired). Where CAS semantics are genuinely needed, one conditional-write pointer per feed — mutable pointers countable on one hand, values immutable. There is no checkpoint file to edit during an incident; "re-pull last week" is an *argument* to `acquire`, not surgery on state.

**4.8 Absence is detected, not discovered downstream.** Each feed declares an expectation schedule; a scheduled comparison of ledger against expectations emits `delivery-overdue` events.

---

## 5. Component Model — Transport Drivers and Source Plugins

The plugin architecture splits the module into two artifacts of very different rarity, on either side of the *how* / *what* line:

### 5.1 Transport drivers — few, platform-owned, effectful, written once

A driver implements one acquisition mode. The initial set:

| Driver | Mode | Trigger shape |
|---|---|---|
| `s3-push` | Partner drops objects into the landing prefix directly | S3 object-created event |
| `sftp-pull` | Framework pulls from a remote SFTP endpoint | Schedule |
| `api-pull` | Framework extracts from an HTTP API, snapshots verbatim response bodies | Schedule |
| `db-unload` | Framework runs a source-side unload to files | Schedule |

Every driver implements the same interface — the module's entire abstraction:

```
acquire(source_config, window) → deliveries

  effects (the only two):
    - objects written to the plugin's landing prefix, verbatim
    - delivery rows appended to the ledger

  contract:
    - idempotent: re-running a window reproduces the same content
      hashes; the ledger dedups to no-ops
    - verbatim: no parsing, no filtering, no header repair; if bytes
      must be transcoded to be usable (e.g., EBCDIC), the original is
      kept alongside and lineage points at the original
```

One verb. No `transform`, no `route`, no `on_complete` callback. Writing a new driver is rare, real platform work, heavily reviewed — correct, because a driver holds credentials and performs effects. (`s3-push` is the degenerate driver: no acquisition code at all; the partner performs the move and the driver only registers.)

### 5.2 Source plugins — many, cheap, declarative

A new integration is a thin package. Most integrations are **configuration and contracts only — no code**:

```
sources/<source>/<feed>/
  source.yaml       # which driver, connection ref, trigger, completeness,
                    # expectation schedule, landing prefix (derived)
  contracts/        # raw contract for what this feed delivers (versioned,
                    # additive-only — shared governance with parent doc §7)
  tests/            # fixture deliveries → expected ledger rows and
                    # expected trigger events
```

**`source.yaml` specification:**

```yaml
feed_id: carrier-x/commission-statements
driver: sftp-pull                    # s3-push | sftp-pull | api-pull | db-unload
connection:
  secret_ref: arn:aws:secretsmanager:...   # reference, never the secret
  remote_path: /outbound/commissions/
trigger:
  schedule: cron(0 13 * * MON-FRI *)  # pull drivers; s3-push uses events
expectation:                          # absence detection (§4.8)
  expected: weekdays
  by: "06:00 America/New_York"
completeness:                         # exactly one of (§7):
  mode: manifest                      # manifest | trailer | timer
  manifest_pattern: "*.manifest.json"
  # mode: trailer
  #   count_field: TRAILER.record_count
  # mode: timer                       # last resort — declared, reviewed
  #   quiet_window_minutes: 10
  #   accepted_risk: "source cannot produce manifest; partial-batch
  #                   window documented and reviewed"
format_hints:                         # interpreted by the FRAMEWORK, not the plugin
  compression: gzip                   # original bytes always retained
pipeline: pipelines/commissions      # the stage-sequence package this feed triggers
```

When a feed genuinely needs logic — a completeness predicate no manifest can express, an API pagination quirk — it ships as a **pure function** in the package (data in, decision out), golden-tested, and subject to the same import/AST linter that keeps I/O out of `transforms.py`. The plugin surface is always declarations plus pure functions; effects never enter it.

### 5.3 The rule that keeps this from rotting

**Extension points accept data, not callbacks.** When someone asks for a hook, the answer is a new declarative field interpreted by the framework. "Skip the first two lines" is not a `preprocess` hook; it is `format_hints: {skip_leading_lines: 2}` handled by the framework's reader, once, tested, for everyone — and applied at *interpretation* time in the stage sequence, never to the stored bytes. The moment `source.yaml` grows a field whose value is code, the capability API has been built through the back door, and every seam in Section 10 reopens.

---

## 6. The Delivery Ledger

An append-only Iceberg table (`ingestion.delivery_ledger`), partitioned by `feed_id` and received-at date:

> **Why Iceberg and not DynamoDB.** The ledger is a fact table with an analytical workload: lateness checks against expectation schedules, duplicate-rate metrics, watermark folds, and lineage joins from fact tables back through `batch_id` to deliveries and driver runs. Those are scans, joins, and aggregations — plain queries in Iceberg under the same catalog and Lake Formation governance as the rest of the lane; in DynamoDB each would be a pre-designed GSI or an export-and-scan. Write volume is delivery cadence (per day, not per second), so DynamoDB's throughput is scale the ledger doesn't need — while its genuine advantage, conditional writes, is already assigned exactly one job in this design: the per-feed batch-id CAS pointer (Section 9). Immutable bulk in cheap, governed, queryable storage; the tiny mutable pointer set in the CAS store. Trade-off named: Iceberg's commit latency and small-file overhead would matter at streaming cadence; at delivery cadence they are negligible, and compaction is framework-owned.

| Column | Meaning |
|---|---|
| `feed_id` | The feed identity (`source/feed`) |
| `delivery_id` | UUID minted at registration — coordination-free value name |
| `object_uris` | Landing objects composing this delivery (array) |
| `content_hash` | Hash over delivery content — the true name of the value |
| `size_bytes` | Total size |
| `received_at` | When acquisition completed |
| `manifest_ref` | Manifest object, if completeness mode is manifest |
| `batch_id` | Minted deterministically on `(feed_id, content_hash)` |
| `disposition` | `registered` \| `duplicate` \| `superseded` \| `incomplete` \| `unreadable` |
| `supersedes` | `delivery_id` of the delivery this one corrects, if any |
| `driver`, `driver_run_id` | Which driver run acquired it — acquisition lineage |

**Dispositions accrete; they are never updated in place.** A delivery later superseded gets a *new* ledger row (or disposition fact) recording the supersession — the ledger is itself an event-sourced entity, folded when "current status of feed X" is needed. The module does not grow a mutable `status` column; the ingestion layer's own record-keeping obeys the same discipline as everything it feeds.

**Derived, disposable views over the ledger** (rebuildable folds, per the parent doc's current-state discipline): feed watermarks (Section 4.7), feed health/lateness dashboards, duplicate-rate metrics.

---

## 7. Completeness Model

Declared per feed in `source.yaml`, in strict priority order:

1. **Manifest-last.** Parts upload first; the manifest object uploads last and is the trigger. Completeness is a fact the source asserts. Preferred whenever the source can cooperate.
2. **Trailer / control totals.** The delivery carries its own count assertions; registration verifies object-level counts (row-level count validation remains `pre_check`'s job). Mismatch → `disposition: incomplete`, no trigger, alert to the feed owner.
3. **Declared timer.** Only when the source genuinely cannot cooperate: a quiet-window timer with the window and `accepted_risk` stated in `source.yaml`, reviewed in the PR. A timer complects *when* with *correctness* — the declaration exists so that this is a named, visible trade-off rather than a silent one.

An `incomplete` delivery never triggers the stage sequence, and never partially triggers it: batch-coherence (parent doc §3.8) begins here.

---

## 8. Batch Identity & Idempotency

**Minting rule:** `batch_id = f(feed_id, content_hash)` — or `f(feed_id, manifest_id)` for manifest feeds — computed at registration. Deterministic, so:

| Scenario | Outcome |
|---|---|
| Driver retry / duplicate S3 event | Same content hash → same `batch_id` → ledger dedups, `disposition: duplicate`, no trigger |
| Partner re-sends identical file | Same → no-op, and the ledger *explains* the skip |
| Partner re-sends corrected file (same name, new content) | New content hash → **new delivery, new `batch_id`**; prior delivery marked `superseded`; downstream commit-stage delta detection ensures only genuine changes become facts |
| Operator re-runs a window (`acquire(config, window)`) | Reproduces same hashes → no-ops throughout |

**Ledger vs. CAS pointer — two artifacts, two roles.** Registration involves both, and they are not interchangeable. The **ledger remembers**: an append-only fact table recording every delivery and its disposition, permanently and queryably — including *both* sides of a duplicate race (`registered` for the winner, `duplicate` for the loser). The **pointer coordinates**: when two concurrent registration runs compute the same `batch_id` (duplicate S3 events, an overlapping driver retry), a conditional write — "create this batch marker only if it doesn't exist" — decides atomically which run triggers the pipeline. The ledger cannot make that decision (appends never refuse, and Iceberg commits are seconds-slow — far too slow to arbitrate a race); the pointer cannot hold the record (it contains nothing but "batch X: taken," and its entries could be TTL-expired after batch completion with zero information loss). One is the logbook, the other is the turnstile: the *remember* and *coordinate* roles, kept separate.

The stage sequence inherits its `batch_id` from the delivery that triggered it. Every fact in the fact table therefore traces: fact → `batch_id` → delivery → objects → driver run → source. Lineage is a chain of ledger and framework stamps; no stage ever invents identity.

---

## 9. Orchestration & AWS Service Mapping

Trigger topology — push and pull sources are indistinguishable from registration onward:

```
s3-push:    partner PUT ──▶ S3 event ──▶ EventBridge ──▶ register ──▶ delivery-registered ──▶ Step Functions (stage sequence)
pull:       schedule ──▶ driver job (acquire) ──▶ objects + ledger ──▶ delivery-registered ──▶ Step Functions (stage sequence)
absence:    schedule ──▶ ledger-vs-expectation check ──▶ delivery-overdue ──▶ alerting / feed-owner queue
```

| Concern | AWS choice | Notes |
|---|---|---|
| Landing zone | S3, write-once (versioning + deny-overwrite policy), prefix per feed | `s3://landing/<source>/<feed>/received_at=<ts>/<original-name>` |
| Delivery ledger | Iceberg on S3, cataloged in Glue Catalog | Append-only Lake Formation grants, same as raw/fact tables |
| Drivers | Small Glue Python/Spark jobs (pull) or none (`s3-push`) | Same runner-library packaging as the stage-sequence framework |
| Batch-id CAS pointer (where needed) | DynamoDB conditional writes (or S3 conditional PUTs — `If-None-Match` marker objects) | First-writer-wins on duplicate registration needs an atomic conditional write; append-only Iceberg can't refuse a duplicate at write time. One item/marker per feed — the tiny mutable pointer set, not a database |
| Secrets | Secrets Manager, referenced by ARN in `source.yaml` | Plugins never contain credentials |
| Events | `delivery-registered`, `delivery-overdue` on EventBridge | Same backbone as `batch-started` / `batch-completed` |
| Orchestration | Step Functions per feed, provisioned from `source.yaml` | Same engine as the stage sequence — one operational skill set |
| Provisioning | Merging `source.yaml` triggers IaC (CDK/Terraform module) | Provisions schedule/event rule, driver job, scoped IAM role, ledger grants — deployment is config, not code (parent doc §7) |

**Access rules, enforced by construction:** nobody but the framework reads the landing zone (the published interfaces are fact and current-state tables — landing is private implementation); only registration writes the ledger; only the plugin's own role writes its landing prefix; fact tables accept writes only from the commit stage, and the runner refuses facts whose lineage lacks a registered `batch_id` — closing the "source is already clean, skip raw" bypass structurally.

---

## 10. What to Defend Against

The erosion vectors, each an easy choice that complects a seam. Defenses are structural — IAM, CI, bucket policy — never convention:

| # | Erosion | What it complects | Structural defense |
|---|---|---|---|
| 1 | Driver/plugin starts transforming ("just strips the BOM" — the invisible Unicode byte-order mark some tools prepend to text files) | *what* ⟷ *move*; logic runs once, effectful, unreplayable | No parse step in the driver template; AST/import linter on plugin code, same as `transforms.py`; ugliness handled in `pre_check`/`apply` via `format_hints` |
| 2 | Overwrites in the landing zone | value ⟷ time (PLOP one hop upstream); replays produce different facts than the original run | Versioning on, overwrite denied by policy, received-at prefixes; content hash as identity arbiter |
| 3 | "Clean" source writes straight to fact tables | source's *current* representation ⟷ your *permanent* record; no replay when interpretation improves | Commit-stage-only write grants on fact tables; runner rejects facts without registered-delivery lineage |
| 4 | Completeness by timer or folklore | *when* ⟷ *correctness*; batch-coherence becomes probabilistic | Completeness as declared assertion (manifest/trailer); timers allowed only with declared window + accepted risk |
| 5 | Ledger grows a mutable `status` column | the module's own record-keeping goes place-oriented | Dispositions accrete as new rows/facts; "current status" is a fold |
| 6 | Consumers query the landing zone directly | hidden consumer of an unpublished interface; source format change breaks unknown dashboards | Landing readable by framework only; published interfaces are fact + current-state tables |
| 7 | `source.yaml` grows a field whose value is code | capability API through the back door; every seam above reopens | Extension points accept data only; new needs become framework-interpreted declarative fields |

---

## 11. Independent Development, Testing & Integration

This module is developed and verified without the stage sequence, and integrated against contracts:

- **Unit surface.** Drivers test against local fixtures (an SFTP container, canned API responses); plugins' pure functions run under golden tests in CI, no AWS required.
- **The module's acceptance gate.** Fixture deliveries in → expected landing objects, ledger rows, and `delivery-registered` events out. Completeness and idempotency scenarios (retry, duplicate, corrected re-send, incomplete manifest) are standing fixtures every plugin inherits.
- **The integration contract** with the stage sequence is exactly the three outputs of Section 1 (objects, ledger row, event). The `land` stage is tested against synthetic registered deliveries; the ingestion module is tested against fixture sources. Neither needs the other running.
- **Contract-first generation.** The input to a developer or agent adding an integration: driver choice, connection details, completeness mode, raw contract. The output: `source.yaml` + contracts + fixtures. The generation spec ("skill") for this module is authored alongside the pipeline-package spec (parent doc §8), with two exemplar plugins in the repo.
- **The abstraction test.** Build **two** drivers before declaring the `acquire` interface done — `s3-push` and `sftp-pull` are the natural pair (one trigger-shaped, one schedule-shaped). If one contract covers both without special cases, the abstraction holds; driver-specific branches in the framework mean the interface is wrong, cheap to fix now.
- **Review model.** Human review concentrates where judgment lives: the raw contract, the completeness declaration, and any declared timer's accepted risk. The framework guarantees everything else.

---

## 12. Trade-offs, Named

- **Verbatim-forever storage costs.** Originals are kept; S3 lifecycle to Glacier is acceptable *provided* the replay window promised by the parent doc (§3.3) is honored.
- **The ledger is one more table** — but tiny, and it buys the entire operational surface (duplicates, lateness, audit, watermarks) as queries.
- **A narrow driver interface will refuse requests.** Someone will want source-side filtering "to save bandwidth"; the answer costs a conversation. Declarative surfaces grow vocabulary slower than hook APIs grow hooks — a feature, but it makes the platform team a deliberate, review-shaped bottleneck for genuinely new acquisition modes.
- **Per-plugin IAM roles are more IaC objects** — the price of blast-radius isolation, automated by the same template machinery the lane already committed to.
- **Manifest-last requires source cooperation** that will not always exist; the declared-timer fallback is an accepted, documented risk, not a silent one.
- **N small plugins instead of one universal ingester** — deliberate: small parameterizable movers are what a template plus an agent can stamp out; the universal ingester is the component that grows sideways into a monolith.

---

## 13. Sequencing

1. **Ledger + registration + `s3-push` driver** — the degenerate driver proves registration, minting, and triggering with no acquisition code.
2. **`sftp-pull` driver** — second implementation validates the `acquire` abstraction (Section 11) and the cursor-as-fold model. First real feed: commission or renewal statement files (aligned with parent doc §10 step 1).
3. **Absence detection + `delivery-overdue`** — closes the operational loop for the first two feeds.
4. **`api-pull` / `db-unload`** — added when a resident feed needs them; each is a driver, not a redesign.
5. **Generation spec + two exemplar plugins** — the onboarding path for developers and agents.

---

## 14. Related Documents

- **Large-Volume Batch Data Processing** (parent) — the stage sequence this module feeds; owner of raw/fact/current-state discipline.
- **Operational Event-Driven Architecture (As-Is)** — the event lane; source of the shared programming model.
- **Event Model** — shared taxonomy and contracts; raw contracts in `contracts/` are governed with it.
- **Per-stage module documents** *(planned, this series)* — one file per stage for independent development, test, and integration.

---

## 15. Glossary

| Term | Meaning |
|---|---|
| **Delivery** | The unit of ingestion: "source X delivered content H for feed Y at time T" — a fact, never updated, only superseded |
| **Feed** | The identity: a named source stream associated with a succession of deliveries over time |
| **Delivery ledger** | Append-only table reifying deliveries as data; drives idempotency, audit, and absence detection |
| **Transport driver** | Platform-owned, effectful implementation of one acquisition mode (`s3-push`, `sftp-pull`, …); implements `acquire` |
| **Source plugin** | Declarative integration package (`source.yaml` + contracts + fixtures + optional pure functions) configuring a driver |
| **Landing zone / prefix** | Write-once S3 area receiving verbatim source bytes; private to the framework |
| **Manifest** | Small metadata file the source uploads *last*, listing the delivery's files, counts, and checksums — its arrival is the source's own assertion that the delivery is complete (the "packing slip" of the delivery) |
| **Completeness mode** | Per-feed declaration of what constitutes a whole delivery: manifest, trailer, or declared timer |
| **Watermark** | A feed's acquisition progress — derived by folding the ledger, never a stored mutable cursor |
| **Disposition** | A delivery's accreted status: `registered`, `duplicate`, `superseded`, `incomplete`, `unreadable` |
| **`delivery-registered`** | Event triggering the stage sequence for a complete, deduplicated delivery |
| **`delivery-overdue`** | Event emitted when the expectation schedule finds a missing delivery |

---

## 16. Open Items

1. **Ledger fold cadence and serving** — where feed-health and watermark views materialize, and whether operations gets a console over them.
2. **Driver credential rotation** — Secrets Manager rotation hooks per driver type.
3. **Multi-object delivery hashing** — canonical content-hash definition over a set of objects (ordering, manifest inclusion).
4. **Supersession semantics downstream** — whether a superseded delivery's facts are ever compensated in the fact table, or (default) delta detection simply makes the corrected batch's facts the newest — decide and document per feed class.
5. **Generation spec authoring** — the agent/developer recipe for source plugins, coordinated with the pipeline-package spec (parent doc open item 5).
