# Data Ingestion Module — Low-Level Design (Phase 1) — **DOP Variant**

**Status:** v2.0-DOP — an exploratory full rewrite of *Data Ingestion LLD v1.1* under the four principles of data-oriented programming (Sharvit, [Principles of DOP](https://blog.klipse.tech/dop/2022/06/22/principles-of-dop.html)). **Does not supersede v1.1**; it is the same module re-specified with a different representation discipline, so the two can be compared decision-by-decision. · **Implements:** *Data Ingestion Module — Architecture Description* (Draft v0.1) · **Stack:** Python 3.12 + Terraform (unchanged) · **Scope:** identical to v1.1 §1.

> **How to use this document.** Read v1.1 first if you have not. Everything v1.1 specifies about *infrastructure, storage layout, algorithms, flows, IAM, Terraform, observability, and milestones* is carried over verbatim (§3 lists what is incorporated by reference). What this document rewrites is the **representation layer**: how data is shaped in code, where schemas live, and how validity is enforced. Where a section number is reused (§6, §7, §8, §12), the text here **replaces** the v1.1 section for this variant.

---

## 0. The four principles against v1.1 — what actually changes

Sharvit's DOP is four principles. v1.1 already satisfies two of them, so this rewrite is narrower than "adopt DOP" suggests:

| # | Principle | v1.1 status | v2.0-DOP change |
|---|---|---|---|
| P1 | **Separate code from data** | ✅ Satisfied by D-17: values + module-level functions; no service classes, no methods carrying logic | None. Carried over unchanged. |
| P2 | **Represent data with generic data structures** | ❌ Violated deliberately: every shape is a *named type* — frozen dataclasses (`RegistrationPlan`, `ClaimResult`, `StoreFx`…) and pydantic models (`FeedConfig`, `ManifestV1`…) | **The big rewrite.** All data is plain `dict` / `tuple` / scalar. No `class` statements exist anywhere in `ingestion/` — not even frozen dataclasses or Enums. §4. |
| P3 | **Data is immutable** | ✅ Satisfied twice over: frozen dataclasses in code; append-only ledger + immutable objects in storage | Structurally unchanged. But the *code-level* enforcement moves from the type system (`frozen=True`) to linter + convention, because plain dicts are mutable. §4.3 names this honestly. |
| P4 | **Separate data schema from data representation** | ❌ Inverted: schema is *derived from* representation (`make schemas` exports JSON Schema from pydantic models; pydantic complects the two) | **The second rewrite.** Hand-authored JSON Schema files in `contracts/` become the source of truth. Code validates *against* them at declared boundaries, selectively. §5. |

So: P1 and P3's substance survive intact; P2 and P4 drive every change below. The storage design is untouched — the Iceberg ledger, the CAS turnstile, canonical keys, content hashing, batch-id minting, the crash matrix, and all of Terraform are representation-independent and were already "data at rest as data."

---

## 1. Purpose, Scope, Non-Goals

Identical to v1.1 §1. Same Phase-1 slice: ledger, registration, `s3-push`, `sftp-pull`, absence detection, Terraform, two exemplar plugins. Same out-of-scope list.

---

## 2. Decision Record — delta

v1.1 decisions D-1 … D-16 stand unchanged. **D-17 is superseded** by D-18 … D-22:

| # | Decision | Choice | Rationale / trade-off |
|---|---|---|---|
| D-18 | Data representation (P2) | **Plain generic structures only**: `dict` with string keys for records, `tuple` for sequences of values, scalars, `None`. **Zero `class` statements in `ingestion/` and `sources/`** — no dataclasses, no pydantic models, no Enums, no NamedTuples. Closed vocabularies are string constants in `core/vocab.py` (e.g. `DISPOSITION_REGISTERED = "registered"`). | Buys: the in-memory shape **is** the wire shape — a ledger row, an event detail, a CAS item, and a golden fixture are the *same dict*, with zero conversion layers (v1.1 converts dataclass→dict→JSON at every boundary); any tool, test, or agent can fabricate data with a literal; plans serialize for free (§8). Costs: mypy strict on `core/` becomes near-worthless (`Mapping[str, Any]` everywhere) — dropped to lenient mode; IDE completion and rename-refactoring over field names are gone; a typo'd key is a runtime `KeyError`/`None`, not an import-time error. Mitigations: authored schemas at boundaries (D-19), key-constant module (D-21), golden coverage. This is the easy/simple trade run in the *opposite* direction from D-17 — named, not hidden. |
| D-19 | Schema authority (P4) | **Hand-authored JSON Schema (draft 2020-12) files in `contracts/` are the source of truth.** Runtime validation via the `jsonschema` library at declared boundaries only (§5.2 map). `make schemas` (generation) and its drift gate are **deleted**; a new gate `make contracts` lints the schema files themselves and runs example/counter-example fixtures against them. | Schema and representation evolve independently: a contract change is a PR to a `.json` file, reviewable by non-Python consumers, versioned separately from code releases. Validation is *selective* (P4's stated benefit): boundary data is validated, interior values are not re-validated on every hop — v1.1 pays pydantic construction cost on every model instantiation. Cost: JSON Schema's cross-field expressiveness is weaker than `@model_validator`; residual cross-checks become pure functions returning defect values (§5.4), and error messages need hand-curating (`core/validate.py` wraps `jsonschema` errors into the exact message strings v1.1's G-14 asserts). |
| D-20 | Immutability enforcement (P3, code level) | **Convention + linter, not types.** `core/` is forbidden (AST-enforced, §12.2) from mutating any dict or list: no subscript/attribute assignment except to fresh locals under construction, no calls to `update`/`append`/`pop`/`setdefault`/`clear`/`sort`/`extend`/`insert`/`remove`, no `del`. All collections crossing a function boundary are `tuple`, never `list`. Updates are expressed as new dicts: `{**row, "disposition": DISPOSITION_SUPERSEDED}`. **No persistent-collection library** (`pyrsistent`, `immutables` stay banned per D-17's stdlib-only rule, which survives). | Python has no cheap structural immutability; `deepfreeze`/`MappingProxyType` wrappers were rejected as ceremony that P2 code would have to unwrap constantly. The linter catches the mutation *forms*; aliasing bugs it cannot catch are bounded by `core/` being pure and small. Honest statement: v1.1's `frozen=True` was *stronger* here; this is the price of P2, paid consciously. |
| D-21 | Information paths | Field names are **string-key constants** in `core/keys.py` (`K_FEED_ID = "feed_id"`, …) — used by `core/` and tests; literal strings remain legal at boundaries where the dict mirrors an external contract. Generic access helpers in `core/data.py`: `get_in(m, path, default=None)`, `select_keys(m, keys)`, `assoc(m, **kv)` (returns a new dict). | One place to grep a field's spelling; `assoc`/`get_in` are the ~30-line generic toolkit that replaces per-type constructors. Rejected: a full lens/spec library — stdlib-only survives from D-17. |
| D-22 | Interior schemas are test-time only | Internal shapes (`registration-plan`, `claim-result`, `effects`) get schema files under `contracts/internal/` and are validated **only in tests and golden runs** (`validate_interior()` is a no-op when `CONVEYER_VALIDATE_INTERIOR` is unset). Boundary schemas (§5.2) always validate. | P4 applied literally: schema application is a choice per site, not a property of the data. Production hot path pays validation exactly where data crosses a trust boundary; the test suite pays it everywhere. |

---

## 3. Carried over verbatim from v1.1 (incorporated by reference)

Unchanged in every detail; re-read them there:

- **§3 System overview** — components, runtimes, flows, "what never happens here."
- **§5 Naming, identifiers, storage layout** — all physical names, canonical-key rule, `received_at` semantics.
- **§6.2 ledger physical schema** — the Iceberg column list, partition spec, population-by-disposition table, and append-only rules are unchanged (the Iceberg schema was *already* a schema separate from representation; this variant merely stops shadowing it with a pydantic twin).
- **§6.5 content-hash canonicalization, §6.6 batch-id minting** — normative algorithms unchanged.
- **§8.4 turnstile semantics** — DynamoDB key formats, claim/complete/sweep state machine, staleness threshold.
- **§8.5 state walk + crash matrix**, **§9 drivers/absence/maintenance algorithms**, **§10 Terraform (entire)**, **§11 observability**, **§13 milestones**, **§14 invariants checklist** (two enforcement cells updated in §12 here), **§15 exemplar plugins**, **§16 Phase 2 hooks**.

The repository layout (v1.1 §4) changes only in these entries:

```
ingestion/core/model.py      → DELETED (no models; shapes live in contracts/)
ingestion/core/vocab.py      → NEW  string constants for closed vocabularies (D-18)
ingestion/core/keys.py       → NEW  field-name constants (D-21)
ingestion/core/data.py       → NEW  generic access helpers: get_in / select_keys / assoc (D-21)
ingestion/core/validate.py   → NEW  loaded-schema registry + validate(name, value) → () | defects (§5.3)
contracts/                   → AUTHORED, not generated (D-19); adds ledger/, internal/, fixtures/
tests/golden/plans/*.json    → NEW  golden plans as literal JSON files (§8.3)
```

---

## 4. §7.0 replacement — Representation rules (normative)

The codebase contains exactly two kinds of things — **data** and **functions** — and the data is *generic* (P2):

1. **Data.** `dict` (string keys), `tuple`, `str`, `int`, `float`, `bool`, `None`, `datetime`/`date` (aware; the two permitted non-JSON scalars, serialized as ISO-8601 at boundaries by `core/data.py::jsonify/dejsonify`). Records are dicts; sequences are tuples; sets are `frozenset` where membership is the point (folds). Nothing else. No `class` statements anywhere in `ingestion/` or `sources/` (D-18).
2. **Self-description.** Every record that travels between modules carries no type tag *in code* — its shape is named by the schema that governs it (§5). Where a value is one of several variants (claim results, defects), the dict carries a `"kind"` key: data describing itself, replacing `isinstance`.
3. **Functions.** Module-level only, as in v1.1. Pure ones in `core/` (same purity linter), effectful ones in `effects/`.
4. **Effects are dicts of functions.** The v1.1 records-of-functions become nested plain dicts (§6 below): `fx["store"]["stream_sha256"](bucket, key)`. Same factories, same test substitution — a test double is a dict literal of local functions.
5. **Defects are values.** A defect is `{"kind": "defect", "reason": str}`. The pure core never raises (unchanged); `TransientError` remains the sole effect-side exception.
6. **Immutability by construction-then-hands-off** (D-20): build a dict in one place, never touch it again; "change" is `assoc`/`{**old, ...}`. Linter-enforced in `core/`; reviewed by convention in `effects/` and orchestration.
7. **Decide, then do** — unchanged (§8): pure planners return plans; the plan is now a dict, which is what makes §8.3's golden-plans-as-JSON possible.
8. **Stdlib only** — unchanged from D-17, plus one runtime dependency swap: `pydantic` **out**, `jsonschema` **in**.

### 4.1 The generic toolkit — `core/data.py` (complete surface)

```python
def get_in(m, path, default=None): ...        # path: tuple of keys/indices
def select_keys(m, keys): ...                 # new dict, subset of keys
def assoc(m, **kv): ...                       # {**m, **kv}
def assoc_in(m, path, v): ...                 # recursive, returns new nesting
def jsonify(v): ...                           # datetimes → ISO strings, tuples → lists (deep)
def dejsonify(v, datetime_keys): ...          # inverse at read boundaries; datetime_keys from schema x-datetime annotations
```

Six functions, ~60 lines, property-tested. This is the whole "framework."

### 4.2 Vocabularies — `core/vocab.py`

All closed sets from v1.1's `Literal`/Enum types, as constants plus a frozenset per set: `DISPOSITIONS`, `DRIVERS`, `COMPLETENESS_MODES`, `CLAIM_KINDS`, `VERDICTS`. Schemas repeat these as `"enum"` lists; `make contracts` cross-checks the two (a vocab/schema drift fails CI) so the constants cannot rot.

### 4.3 What got weaker, said plainly

`frozen=True` gave v1.1 *mechanical* immutability and constructor-checked field names on every interior value. This variant trades both for genericity. The compensations are real but different in kind: schemas at boundaries (always) and interiors (tests, D-22), key constants, the mutation linter, and ≥95 % branch coverage on `core/` (unchanged gate). A reader deciding between v1.1 and v2.0-DOP should weigh exactly this cell; nothing else in the module's guarantees moves.

---

## 5. §6 replacement — Contracts under Principle #4

### 5.1 Authored schema set — `contracts/` (source of truth, D-19)

```
contracts/
├── source/source-config.v1.json        FeedConfig shape (§5.4)
├── manifest/conveyer-manifest.v1.json  cooperating-source manifest (v1.1 §6.3 shape, verbatim)
├── events/delivery-registered.v1.json
├── events/delivery-overdue.v1.json
├── ledger/delivery-record.v1.json      NEW: the row contract, previously implicit in pydantic
│                                        + pyiceberg; mirrors v1.1 §6.2 column-for-column,
│                                        with the population-by-disposition table encoded as
│                                        if/then constraints per disposition
├── cas/claim-item.v1.json              NEW: the DynamoDB item shape (v1.1 §8.4 attribute list)
├── secret/sftp-secret.v1.json          v1.1 §6.7 shape
├── registry/feeds.v1.json              feeds.json envelope
├── internal/registration-plan.v1.json  test-time only (D-22)
├── internal/claim-result.v1.json       test-time only
└── fixtures/<schema>/{valid,invalid}/*.json   examples + counter-examples; make contracts
                                                runs every fixture against its schema
```

Conventions inside every schema: `"additionalProperties": false` everywhere v1.1 said `extra="forbid"` (all except the manifest, which keeps `extra="allow"` semantics by omitting the constraint on the envelope); `"x-datetime": true` annotation on ISO-timestamp string fields (drives `dejsonify`); `$id` = `conveyer:<path>`; versioning is additive-only, breaking ⇒ new file — unchanged policy.

### 5.2 Validation map — where schemas are applied (normative; the P4 "selective" choice)

| Boundary | Value | Schema | On failure |
|---|---|---|---|
| Registry load (registrar, drivers, absence) | each feed dict in `feeds.json` | `source-config.v1` + `feeds.v1` | `TransientError` (config is platform-owned; invalid config is an ops page, not a data defect) |
| Manifest read | parsed partner JSON | `conveyer-manifest.v1` | defect value → `unreadable` disposition (unchanged behavior) |
| Secret fetch | secret JSON | `sftp-secret.v1` | `TransientError` |
| Ledger append | every row in the plan | `delivery-record.v1` | raise — a schema-invalid row is a bug; better to DLQ than to append it (the ledger is forever) |
| Event emit | event detail dict | matching `events/*.v1` | raise — same reasoning |
| CAS read (takeover path) | claim item | `claim-item.v1` | `TransientError` (a corrupt item means the sweep retries later) |
| Interior values | plans, claim results, completeness results | `internal/*` | test/golden runs only (D-22) |

Everything not in this table is *not validated at runtime* — interior data flows on trust inside the pure core, which is exactly the cost profile P4 promises.

### 5.3 `core/validate.py`

```python
def load_contracts() -> dict            # schema-name → compiled validator (Draft 2020-12);
                                        # loaded once per container from the packaged contracts/
def validate(name, value) -> tuple      # () if valid, else tuple of defect dicts:
                                        # {"kind": "defect", "reason": <curated message>, "path": (...)}
def curated(errors) -> tuple            # maps jsonschema errors → the exact G-14 message strings
```

Pure (schemas are packaged data files read at import by the *effect side* and passed in — `core/` receives the compiled validators as values). `validate` never raises; callers in the table above decide raise-vs-defect.

### 5.4 Feed config — dict + cross-check functions

The `source.yaml` fields are exactly v1.1 §6.1's, now expressed in `source-config.v1.json`. JSON Schema carries: enums (`driver`, `completeness.mode`), patterns (`feed_id`, `by`, secret-ARN shape, `manifest_pattern` = `^\*[^*?\[]+$`), ranges (`quiet_window_minutes`), required-by-mode via `if/then` (sftp-pull ⇒ `connection`+`trigger`; s3-push ⇒ `partner_principal_arns`, no `trigger`; trailer ⇒ `trailer`; timer ⇒ `timer` + `accepted_risk` minLength 20).

Three rules exceed JSON Schema and live as pure functions in `core/feedcheck.py`, returning defect tuples with v1.1's exact error strings: timezone must resolve via `zoneinfo`; **s3-push + timer → "timer completeness is not supported for s3-push in Phase 1 (LLD D-10)"**; `api-pull`/`db-unload` → "driver not implemented in Phase 1". `make registry` = schema validation + `feedcheck` over every `sources/**/source.yaml`; Terraform's role is unchanged (D-12).

### 5.5 Ledger rows, events, claim items — one shape each, end to end

A ledger row is a dict conforming to `delivery-record.v1` from the moment a planner builds it to the moment `effects/ledger.py` hands it to pyarrow (`jsonify` at that seam only). An event is its own EventBridge `Detail`. A claim item is its own DynamoDB item (`jsonify` for the ISO fields; numbers stay numbers). **The v1.1 conversion layers (`dataclass → dict → JSON`, `ClaimItem` ↔ item marshalling, `model_dump()` at every emit) are deleted** — this is P2's concrete payoff in this codebase, and the claim-item takeover path (v1.1 §8.3 TAKEN_OVER) becomes literal: the plan is rebuilt from the *same dict* the dead run wrote.

---

## 6. §7.2–§7.7 replacement — signatures in generic terms

Logic, algorithms, and semantics are v1.1's, verbatim. Only shapes change:

| v1.1 construct | v2.0-DOP form |
|---|---|
| `RuntimeConfig` dataclass | `config: dict` from `from_env()`, validated once at startup against `internal/runtime-config.v1` (always, not test-only — env is a boundary) |
| `ObjectStat`, `RemoteFile`, `CompletenessResult`, `Defect` | dicts: `{"name", "bytes", "sha256"}`, `{"name", "bytes", "mtime"}`, `{"verdict", "reason", "asserted_record_count", "data_object_names": tuple}`, `{"kind": "defect", "reason"}` |
| `parse_manifest → ManifestV1 \| Defect` | `parse_manifest(raw: bytes, validator) → dict` — the manifest dict, or a defect dict (`"kind"` discriminates) |
| folds (`latest_dispositions`, `acquired_final`, …) | identical semantics; take `tuple[dict, ...]`, key access via `core/keys.py` constants |
| `Window`, `DeliveryOutcome` | dicts; `DeliveryOutcome` = `{"delivery_id", "batch_id", "disposition", "feed_id", "delivery_key"}` |
| Driver contract `AcquireFn` | `acquire(feed: dict, window: dict, fx: dict) → tuple[dict, ...]` — same one-function-per-driver rule, same permitted-effects rule, same abstraction test |
| `StoreFx`/`LedgerFx`/`CasFx`/`SftpFx`/`Effects` records | one nested dict built by `effects/build.py::build_effects(config)`: `{"store": {"list_prefix": fn, …}, "ledger": {"append": fn, "scan_feed": fn}, "cas": {…}, "emit": fn, "sftp_fx_for": fn, "now": fn, "new_delivery_id": fn, "config": config}` — same keys as v1.1's fields, same factory-per-module (`make_store_fx(client, config) → dict`), same no-mocking test rule (doubles are dict literals of local functions) |

Retry behavior, budgets, streaming, timeouts, and every number in v1.1 §7 stand.

---

## 7. §8 replacement — Registration: plans as literal data

The planner/interpreter split is unchanged. The plan becomes a dict — and because it is generic data (P2) with a schema (P4, `internal/registration-plan.v1`), it is **directly serializable**, which upgrades the golden suite (§8.3).

### 7.1 `RegistrationRequest` and the plan

```python
# built by drivers; interior value (schema: internal/, test-time)
req = {
  "feed": feed,                    # the registry dict, as loaded
  "delivery_id": ..., "delivery_key": ..., "received_at": dt,
  "driver": ..., "driver_run_id": ...,
  "completeness": completeness,    # §6 dict, verdict == "complete"
  "objects": (                     # staged objects, data AND manifest
    {"name", "role", "uri", "bytes", "sha256", "src_key"},  # src_key None for sftp
  ),
}

# returned by plan_registration / plan_nondelivery (pure, core/decisions.py)
plan = {
  "rows":   (row, ...),            # each conforms to ledger/delivery-record.v1
  "copies": ({"src_bucket","src_key","dst_bucket","dst_key"}, ...),
  "event":  detail_or_None,        # conforms to events/delivery-registered.v1
  "complete_claim": (feed_id, batch_id) or None,
  "outcome": outcome,              # §6 DeliveryOutcome dict
}
```

`plan_registration(claim, prior, req, recorded_at)` and `plan_nondelivery(...)` keep v1.1 §8.3's full decision table (WON / LOST_COMPLETED / LOST_IN_PROGRESS / TAKEN_OVER, supersession detection, append-on-change) — `claim` is now `{"kind": "WON" | ..., "item": claim_item_dict_or_None}`. The concurrent-correction race analysis and the reconciliation sweep are unchanged.

### 7.2 Interpreter

`execute(plan, fx)` is v1.1 §8.5's E1–E5 verbatim, reading the four plan keys. Ledger append and event emit validate against their boundary schemas (§5.2) before effecting — the only two lines added relative to v1.1.

### 7.3 Golden plans as JSON files (new capability, P2 payoff)

Each golden scenario G-01 … G-14 gains a committed expectation file `tests/golden/plans/G-xx.json`: the *exact* plan dict, `jsonify`-ed, with the seeded ids/clock the fixture injects. The assertion is `jsonify(plan) == json.load(...)` — a byte-diffable artifact reviewable in a PR without reading test code. v1.1 could not do this without writing the serialization layer this variant gets for free. (The second half of each golden test — run the interpreter, assert the world agrees — is unchanged.)

---

## 8. §12 replacement — Testing, CI gates, enforcement

Layers, local stack, coverage gates (core ≥ 95 % branch, overall ≥ 85 %), golden scenario table G-01…G-14, and make-target set are v1.1's, with these substitutions:

- `make schemas` (generate + drift) → **`make contracts`**: JSON-Schema-lint every file in `contracts/`, run all `fixtures/**/{valid,invalid}` cases, cross-check `vocab.py` enums against schema enums (§4.2). CI = `make lint contracts registry test`.
- `mypy` strict on `ingestion/core` → `mypy` lenient repo-wide (D-18 cost, recorded).
- G-14 asserts the **curated** messages from `core/validate.py::curated` + `feedcheck` — same exact strings as v1.1, so the acceptance bar does not soften.

**Purity linter v2** (`tools/purity_linter.py`) — purity rules (banned imports/calls, no `raise`/`try` in `core/`) unchanged; idiom rules replaced:

- **`class` is banned outright** in `ingestion/` and `sources/` — no exemptions (v1.1 exempted frozen dataclasses / pydantic / Enum).
- **Mutation ban in `core/`** (D-20): subscript/attribute assignment (except plain-name locals), `del`, augmented subscript assignment, and calls to `update/append/pop/popitem/setdefault/clear/sort/extend/insert/remove/add/discard` on any expression. Fixture-tested both ways.
- **Banned imports anywhere**: v1.1 list + `pydantic`, `dataclasses`, `attrs`, `enum`, `typing.NamedTuple`, `pyrsistent`, `immutables`.
- **List-across-boundary ban** in `core/`: `return` of a `list` display or comprehension is flagged (tuples only).

Invariants checklist (v1.1 §14): two cells update — *"`source.yaml` grows code"* is now enforced by `additionalProperties: false` in `source-config.v1.json` + `make registry`; *"mutable status column"* gains "every appended row is schema-validated against `delivery-record.v1` at the append boundary."

---

## 9. Honest assessment — what this variant buys and what it costs

**Buys** (each traceable to a principle):

1. **Zero representation conversions** (P2): one shape per fact from planner to Iceberg/DynamoDB/EventBridge; the marshalling code and its bug surface are deleted. The TAKEN_OVER resume path — the subtlest code in the module — now replays literally the dict the dead run persisted.
2. **Schemas as first-class, independently-versioned contracts** (P4): downstream consumers, partners, and the parent lane review `contracts/*.json` diffs without reading Python; contract releases decouple from code releases; the same files can validate the *other* side (a partner can test their manifest against `conveyer-manifest.v1.json` themselves).
3. **Selective validation cost** (P4): boundaries always, interiors only under test — v1.1 pays pydantic construction on every interior value, always.
4. **Fabrication and tooling** (P2): golden plans as diffable JSON; fixtures are literals; any agent or script can generate test data with no imports; `jq` works on a debug dump of any value in the system.
5. **Weaker coupling to Python** (P2+P4): a future Glue/Fargate promotion of a driver (D-2's named path), or a second-language consumer, inherits the contracts, not a pydantic dependency.

**Costs** (each named in a D-number):

1. **Static safety collapses inward** (D-18): mypy and the IDE stop understanding the data. Field-name errors surface at runtime or in tests, never at import. This is the dominant cost and it is paid on every future edit.
2. **Immutability demotes from mechanism to discipline** (D-20): the linter catches mutation forms, not aliasing; v1.1's `frozen=True` was categorically stronger.
3. **Validation ergonomics** (D-19): JSON Schema's `if/then` cross-field rules are harder to read than `@model_validator`, and error curation is hand-maintained code.
4. **Refactoring drag** (D-21): renaming a field is grep + schema edit + fixture sweep, with no tool support.

**Consultant's verdict.** Both documents describe the *same system* — same facts, same identities, same effects, same crash behavior — because v1.1 was already data-oriented where it counts: in what the system *remembers and exchanges*. The genuine fork is P2, and it is an easy-vs-easy question, not a simple-vs-complex one: v1.1's named types and this variant's generic maps are two *representations* of the same values, each easy for a different audience (typed-Python readers and IDEs vs. polyglot consumers, serializers, and agents). Sharvit's P4, however, improves v1.1 on the merits even if P2 is rejected: authoring `contracts/` by hand and validating pydantic models *against* them (instead of generating them) removes a real complection — schema-authority⟷implementation-language — at almost no cost. If you adopt one thing from this rewrite into the mainline, adopt D-19.

---

*End of v2.0-DOP variant. v1.1 remains the mainline build spec until a decision is recorded.*
