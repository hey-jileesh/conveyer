# ADR: fact-table partition spec (OQ-6)

| | |
|---|---|
| **Status** | **Accepted** (2026-08-30) — Option A adopted; scope: the fact tables **and the ADR-OQ5 marker table** (settled here, not left to analogy — see the outcome); the state-table half of F-3 is untouched |
| **Date** | 2026-08-29 |
| **Reviewed** | 2026-08-30 — adopted with four refinements: the dissolution claim strengthened (fact-table compaction itself becomes nearly moot, not just [T-9]); the marker table's spec settled in-scope under the same argument; validation 2 gains the Track E corrections-present dataset caveat; the forward-only reversibility priced in the accepting clause, with PS-1's no-rewrite stance stated as a choice, not a law reading |
| **Decision owners** | The `conveyer-hpp.13` session (007.1 F-3); 001 receives a recorded erratum note; 008's register updates ([T-9], cardinality review) |
| **Informed by** | 001 §6 (the sketch); 005.1 A-8 + §4.3 ([T-9] dissolution, [R2-3] scope limit), A-9 (fact-probe cost basis); 004.1 I-3, [T-9], I-4 (one-commit invariant); 007 D-2 (batch-clustered delta reads), D-5 (as-of split); errata row 19 (per-(stage, table) guards); ADR-OQ5 (marker table, guard-grain rows) |

---

## Context

OQ-6 as posed: 001 §6's fact-table baseline sketches `bucket(domain_id)` plus a time dimension, but every correctness-path read of a fact table is `batch_id`-keyed, and 005.1 A-8's precedent argues `identity(batch_id)`. May 007.1 depart from 001 §6 with a recorded erratum?

Two prior findings frame this. A-8 partitioned the **admission** tables `identity(batch_id)` and showed the payoff: every guard probe becomes a partition-level metadata miss, and the [T-9] obligation — *compaction must preserve `batch_id` clustering* — **dissolves structurally**, because rewrites cannot cross a partition boundary. But A-8 explicitly scoped itself to admission tables ([R2-3]); the fact tables' `batch_id` pruning today rests on I-3's write-order clustering + column stats, with [T-9] surviving as a discipline 008 must uphold forever. And A-9 noted that the doors' fact-table probes are "a second instance of an already-paid read" — paid *every attempt, every stage*, at whatever cost the layout gives them.

**A method note, honestly stated**: unlike OQ-1/2/5, this is not a failure-direction question. Every option below returns correct answers; a probe against a badly partitioned table is slow, not wrong. The analysis is therefore a complexity audit — what each layout complects, which obligations it dissolves versus which it leaves to discipline — with one familiar instrument still applying: [T-9] under the sketch is *economics enforced by discipline* (compaction must remember to preserve clustering, forever, silently degrading if it forgets), while `identity(batch_id)` is the same economics *by construction*.

## The diagnosis: 001's sketch aims perception layout at a process-hot table

Who actually reads fact tables, and how:

| Reader | Keyed by | Weight |
|---|---|---|
| Commit guards + fact-presence doors (006.1 §8.2, per errata row 19: per `(batch_id, fact_table)`) | `batch_id` | **every attempt, every stage** — the hottest read in the lane |
| `read_batch` / fold input (committed facts of this batch) | `batch_id` | every batch |
| delta_filter predecessor reads (D-2: "one batch-clustered read") | `batch_id` | every batch |
| Rerun `row_hash` anti-joins, I-24/F-8 backstop, I-19 read-backs | `batch_id` | every rerun |
| Rebuild (D-5: fold over *all* facts) | full scan | rare, layout-agnostic |
| As-of business time / audit / trend queries (D-5: "a query over facts") | `domain_id`, period | occasional, Athena-side |

Every read that carries the lane's per-batch cost is `batch_id`-keyed. The reads the 001 sketch optimizes — by-domain, by-time — are the *perception* profile, and the architecture already has a perception-optimized derivation of the facts: **the state tables**. The sketch complects the two table classes' read profiles: it lays out the process-side record for the analytical consumers, while the actual analytical surface (current state, and Athena over facts with column stats) goes unaided by it on the path that matters. The instinct in `bucket(domain_id)`+time is not wrong — it is aimed at the wrong table class, and F-3's state-table ruling is where it belongs on its merits (state's hot read *is* the domain-keyed MERGE).

## Decision drivers

1. **The hot path**: guard/door probes run every attempt; their cost basis should be structural, not stats-plus-discipline.
2. **Dissolve obligations over maintaining them**: [T-9] for fact tables is a forever-discipline on 008's compaction; A-8 showed it can be dissolved outright.
3. **The one-commit invariant** (I-4 / errata row 19): each `(batch, table)` append is one commit — the small-file bound *per partition* holds only if one append lands in one partition.
4. **Reversibility**: Iceberg partition-spec evolution is additive metadata — a future spec change re-lays only new data, no fact rewrite. The cheap-to-revisit option should win ties (the choose-reversible heuristic).
5. **Don't design for hypothetical queries**: the analytical need is unmeasured; carrier volumes and real query patterns arrive with 008's cardinality review, which A-8 already registered.

## Options

### Option 0 — Adopt 001 §6 as sketched: `bucket(domain_id)` + time dimension

The hottest reads become stats-aided scans: a guard probe for an *absent* batch — the common case on healthy runs — must consult file-level stats across every bucket instead of missing at the partition level. [T-9] survives as live discipline: any 008 compaction that fails to preserve `batch_id` clustering silently degrades every guard, door, and delta read from metadata-miss to scan, with no alarm — economics eroded by omission, the discipline-shaped hole this design culture keeps refusing. The analytical payoff accrues to queries that are occasional, Athena-side, and already served by column stats plus the state tables.

### Option A — `identity(batch_id)` per fact table: extend A-8's argument to where the same reads live

Every correctness-path read becomes a single-partition plan; absent-batch probes are metadata-only misses; [T-9] **dissolves structurally for fact tables** exactly as §4.3 dissolved it for admission tables — 008's register row converts from "preserve clustering forever" to "review partition cardinality with real data," the review A-8 already scheduled ([R2-3]'s asymmetry closes). One append = one partition = the bounded-file property I-4 already guarantees. Costs, named: partition count grows as batches × declared fact tables (thousands/year/table — same envelope A-8 accepted, same 008 review); by-domain and by-time analytical reads pay stats-based pruning within batch partitions rather than partition pruning — measured, and reversible by additive spec evolution if real query patterns demand more (driver 4).

### Option B — Composite: `identity(batch_id)` + `bucket(domain_id)`

Disqualified on driver 3: it multiplies partitions per append — one commit scatters into N buckets, so the small-file bound the one-commit invariant provides is broken *by the spec itself*, and 008 inherits a compaction burden the layout created. Both halves of the composite get worse together.

### Option C — Keep the sketch, uphold [T-9] by adding compaction tests

Honest about the discipline but keeps it: the obligation becomes tested discipline instead of dissolved. Strictly dominated by A wherever A is available — a test that a rewrite preserved clustering is a guardrail on a property `identity(batch_id)` makes unviolatable. Worth naming only because it is the default drift if OQ-6 goes unruled.

## Comparison

| Criterion | 0 — sketch as written | A — `identity(batch_id)` | B — composite | C — sketch + tested [T-9] |
|---|---|---|---|---|
| Guard/door probe (hottest read) | stats-aided scan | **metadata-only miss** | metadata miss | stats-aided scan |
| [T-9] for fact tables | live discipline, silent decay | **dissolved** | dissolved | tested discipline |
| One append → files | scattered by bucket | **one partition, I-4-bounded** | scattered × buckets | scattered by bucket |
| Delta/fold/rerun reads | clustered-if-maintained | single-partition | single-partition | clustered-and-tested |
| As-of business time / audit | partition-pruned | stats-pruned; spec evolution reserved | partition-pruned | partition-pruned |
| Partition cardinality | buckets × time (bounded) | batches × tables (linear; 008 reviews) | batches × buckets (worst) | buckets × time |
| Reversibility | evolution available | evolution available | — | evolution available |
| Consistency / governance | as sketched | 001 erratum + A-8 precedent | — | as sketched |

## Proposed outcome

**Option A**, with the departure from 001 §6 recorded as an erratum note — which the LLD has authority to do: §6 is a baseline sketch in the architecture doc, physical layout is the LLD's ground, and the recorded-erratum pattern is the standing convention. In Y-statement form: *in the context of* the fact tables' physical layout, *facing* a hot path that is entirely `batch_id`-keyed and an architecture sketch aimed at the analytical profile, *we propose* `identity(batch_id)` partitioning per declared fact table, no sort order (A-8's shape), with the 001 §6 departure recorded and the sketch's `bucket(domain_id)`+time instinct re-aimed at F-3's state-table ruling where that profile actually lives, *and against* the sketch as written (the hottest reads pay for queries the state tables serve, and [T-9] stays a silently-decaying discipline) *and against* the composite (breaks the one-append file bound by construction), *to achieve* metadata-only guard economics, the structural dissolution of [T-9] for fact tables, and a layout whose revisitation is additive metadata, *accepting* linear partition cardinality under 008's already-scheduled review and stats-grade (not partition-grade) pruning for occasional analytical reads until data argues otherwise — **a forward-only reversibility**: spec evolution re-lays new data only, so every batch written before an evolution keeps stats-grade pruning permanently under PS-1's no-rewrite stance, and the longer the decide-with-data wait, the more history stays coarse.

**The marker table is settled here too.** ADR-OQ5's L-2 fixed its grain but not its partition spec; leaving it to key "by analogy later" is exactly what PS-3 forbids. It adopts `identity(batch_id)` under this same argument — its probes are *absent-batch-heavy* (the OQ-1 resolution read, the DC-1 bind refusal, the OQ-2 probe all interrogate batches that may not be there), which is precisely the metadata-only-miss case — so the F-3 ruling ships all three specs as settled law: fact tables and marker table by this ADR, state tables by its own merits.

## Invariants for the F-3 ruling (PS-1…PS-3)

- **PS-1 — Layout serves the process side; perception rides stats and evolution.** The fact-table spec is chosen for the `batch_id`-keyed reads that run every attempt. Analytical pruning rides Iceberg column stats; if measured query patterns demand more, the answer is **additive partition-spec evolution owned by 008's review** — not a fact rewrite, and never a return of [T-9]-style clustering discipline. *Precision on the no-rewrite stance*: a content-preserving Iceberg rewrite violates no law — facts are values, and a rewrite preserving every row changes no fact. Forgoing rewrites is a **choice**, made on economics and erosion risk (rewrites are exactly where [T-9]-style discipline holes re-enter). Stating it as a choice cuts both ways: nobody later "corrects" PS-1 as a misreading of the immutability law, and nobody invokes that law to block a rewrite the data genuinely justifies — such a rewrite needs a decision record weighing the erosion risk, not an exemption from law.
- **PS-2 — `identity(batch_id)` is the leading transform, and append-locality is load-bearing.** Any future evolved spec keeps the guard probe a partition-level operation and must re-argue the small-file bound: a transform that scatters one `(batch, table)` append across partitions (bucketing, fine-grained time) re-creates Option B's disqualification for all data written after it and needs its own decision record, not a config change.
- **PS-3 — Facts and state do not share a layout by analogy.** The two table classes have opposite hot profiles (process-keyed vs domain-keyed). F-3's state-table ruling weighs `bucket(domain_id)` on the MERGE profile on its own merits; neither ruling is precedent for the other, and the 001 erratum says so — the sketch is re-aimed, not refuted.

## Consequences

**Positive.** The lane's hottest reads get the same structural economics A-8 gave admission — one argument, now uniform across every table the guards touch, the marker table included by this ruling. And the dissolution is stronger than [T-9] alone: under `identity(batch_id)`, **each partition receives exactly one append and is never written again**, so fact-table *compaction itself* becomes nearly moot — at most optional within-partition file merges that cannot cross a boundary by construction. 008's standing fact-table duty approaches zero: the register row shrinks from "preserve clustering forever" past "review cardinality" to "cardinality review plus optional merges." The [R2-3] asymmetry A-9 had to footnote disappears.

**Negative.** Partition cardinality grows linearly per fact table (008's review, now covering admission + fact + marker tables in one pass); as-of business-time and audit queries prune by stats, not partitions, until measurement says otherwise; 001 §6 carries an erratum note.

**Neutral.** The state-table half of F-3 is untouched — including whether `bucket(domain_id)` earns its place there. Rebuild is layout-agnostic. D-2's "bounded, batch-clustered read" language strengthens from clustering to partitioning with no textual change needed.

## Validation before adoption

1. **Cardinality math, one review**: batches/year × declared fact tables per pipeline under real carrier cadence, folded into the 008 review A-8 already registered (§15.2) — one review covering admission, fact, and marker tables, not three.
2. **Stats-pruning check — on corrections-present data**: run the known as-of business-time query shapes (D-5's "query over facts") against `identity(batch_id)` layout with realistic volumes. The dataset **must include Track E correction batches**: the implicit time-clustering argument (batch partitions accrete in time order, so event-time file stats give near-partition-grade time pruning for free) holds *except* for corrections, which carry old business time into new partitions — wide event-time ranges that time-ranged queries cannot skip via stats. A check on correction-free data passes on data cleaner than production. If pruning is inadequate, PS-1's evolution path is the answer and 008 owns the trigger — decided with data, not now.
3. **Erratum lands with F-3**: the 001 §6 note and the 008 register updates ([T-9] dissolution, merged cardinality review) ship in the same change as 007.1's F-3 ruling, citing this ADR.

## References

- 001 `design/001_batch_data_processing_architecture.md` §6 (the sketch, line 116)
- 005.1 `design/005.1_admission_lld.md` A-8 + §4.3 ([T-9] dissolution, cardinality counter-obligation, [R2-3]), A-9 (fact-probe cost basis)
- 004.1 `design/004.1_runner_spine_lld.md` I-3, I-4, [T-9]; errata row 19 (per-(stage, table) guard grain)
- 007 `design/007_record.md` D-2 (batch-clustered delta reads), D-5 (as-of split, rebuild)
- `design/adr-oq5-batch-progress-grain.md` (marker table at the guard grain)
- Beads: `conveyer-hpp.13` (F-3 owner), 008's registered cardinality/[T-9] rows
