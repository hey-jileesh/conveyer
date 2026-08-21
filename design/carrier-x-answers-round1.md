# Commission Statements — Answers, Round 1 (current-system reading)

**Companion to:** `carrier-x-question-pack.md` · **Feeds into:** `carrier-x-inventory.md`, bead `conveyer-frj` · **Gates:** 006 D-1, 007 D-3, 007 D-4

> **Read this first.** These answers describe **what the existing pipeline does today** — they were sourced from the current code and configuration, not from a business owner. That makes them excellent evidence of *mechanism* and weak evidence of *intent*. "The pipeline does not validate X" does not mean the business doesn't need X; it may mean nobody built it. Every gate disposition below is marked accordingly.
>
> **Two feeds, not one:** COINS (weekly) and ISG (monthly). Both carry a summary and a detail member.

---

## Q1 — What makes a statement wrong?

### Single line

| Probe | Answer |
|---|---|
| Negative commission? | Yes. Treated as true negative values that reduce totals. Most likely on adjustment/reversal-style lines, but the business circumstances are not documented. |
| `net = gross − fees` or other arithmetic? | No explicit rule. Hard checks today are required/equal-to field validation and record-count consistency — no financial balancing. |
| Fixed value sets? | Yes: commission category codes (10/20/30/40/50/60/70), product-type groups (`ADVCRE`, `MED`, `MEDHMO` → Medical; `DEN`, `DENPPO` → Dental; `VIS`, `VISSTD` → Vision), schedule values `CCMUFEE`/`MHAFEE` for Large Group ASO. **But these are mapping/defaulting rules, not validation rejects** — plus constants like `status=processed`, `data_quality_check=passed`. |
| Obvious on-sight errors? | No outlier checks implemented. A huge commission on a tiny premium flows through unless it violates a required-field or equality check. |

### Values matching a list

| Probe | Answer |
|---|---|
| What's list-driven? | Product codes, commission category codes, a few schedule/subtype codes, and a static company-code mapping in ISG utilities. **Agent IDs and policy numbers are not enforced against any master list** — parsed, derived, carried through. |
| Where does the list live, who owns it? | In code (hardcoded ISG mappings) or in metadata tables used by DQ rules where configured. **No ownership metadata anywhere.** |
| Freshness / time-versioned roster? | Not validated at load time. |
| Agent appointed last week? | **Unanswered — needs a business SME.** Noted: COINS is a weekly feed, ISG monthly. |

### Whole statement

| Probe | Answer |
|---|---|
| What must add up? | **No summary-vs-detail dollar reconciliation exists today.** Count checks that do exist are operational: parsed-vs-transformed row counts, and post-load Redshift-view vs Mongo-collection counts per run. Mismatch → parser exception, run fails, downstream blocked. |
| What happens on mismatch? | For the counts that are checked: exception, task/run marked failed, manual investigation/retry. For summary-vs-detail dollars: **no handling, because no check.** |

### The easy-to-forget ones

| Probe | Answer |
|---|---|
| Line-to-line rules? | None. No duplicate-policy detection within a file. |
| Date-dependent rules? | None. No appointment-window validation; dates are parsing/derivation and statement-period grouping only. |
| Prior-statement comparison? | None. Current behaviour is operational replacement for the same period bucket, not trend or anomaly checking. |

---

## Q2 — When a correction leaves a line out

> **Answer:** For correction processing there is no row-level update/merge logic. The flow is **replace-style**: clear the relevant data for that batch/period and reinsert from the corrected full file.

**Reading:** operationally this behaves as **(b) complete replacement** — an omitted line disappears from the resulting data — and it implies corrections arrive as **full files**, not partials. So the two sub-questions are consistent with each other, which is the good outcome.

**But the mechanism doesn't establish the meaning.** Replace-and-reinsert is the cheapest thing to implement whether or not the carrier intends omission as withdrawal. The business question — *does the carrier mean the line is withdrawn, or did they just not restate it?* — is still unanswered, and the answers differ in consequence: under (b) we must derive an explicit retraction; under (a) an omission should change nothing.

---

## Q3 — Full amount or the change?

| Probe | Answer |
|---|---|
| $15.00 or +$5.00? | The pipeline treats each loaded line as the amount on that line and reloads corrected batches by replacement; it computes no delta. **But the business may send $5.00 as an adjustment on the July statement.** |
| Negative lines? | Yes, allowed, flow through as arithmetic negatives. **No business semantics distinguish "reversal of what was reported" from "the commission is genuinely negative."** |
| Mixed behaviour? | **Yes.** Normal lines and explicit adjustment-style lines coexist. |
| How to tell them apart? | Classification fields — transaction subtype/category (COINS adjustment subtype; ISG adjustment / prior-balance / levy categories) — not the amount sign. |

**Reading:** this is the **mixed** case, explicitly. Corrections restate; prior-period fixes may also arrive as adjustment lines carrying a delta. The pipeline doesn't distinguish them today because it replaces wholesale — but the *data* does, via transaction subtype/category.

---

## Q4 — Which statement wins?

> **Answer:** No in-file version or sequence field decides "$10 vs $15." Statement period/date fields exist but are used to bucket and replace data for that period; whichever run is processed later for the same period effectively wins. "Newer" is driven by **processing order** — including filename timestamp and arrival handling — not by a business version field.

**Reading:** ordering is currently a property of *when we happened to run*, not of the data. Confirmation 2 is better news than it first looks, though: the statement period **does** live in columns (COINS `PYMNT_CYC_FRM_DTE` / `PYMNT_CYC_TO_DTE` → derived `batch_field`/`statement_date`; ISG `me_date`/`batch_field`), so period is real data. What's missing is a tiebreaker *between two statements about the same period*.

---

## Confirmations

| # | Answer |
|---|---|
| 1 | COINS summary = `COINS_Weekly_summary_YYYYMMDD.csv` / `COINS_Monthly_summary_*.csv`; COINS detail = `COINS_Weekly_detail_*.csv` / `COINS_Monthly_detail_*.csv`. ISG = `ISG_Monthly_Detail_*.txt` (`system_file_id=23`) and `ISG_Monthly_Summary_*.txt` (`system_file_id=22`). **COINS config marks summary as parent/required (`minimum_files=1`) and detail as dependent (`minimum_files=0`)** — i.e. detail is optional. "Summary always sent even with no activity" is not guaranteed in business logic. |
| 2 | Statement period lives in data columns, not only the filename (see Q4). Filename timestamp is used operationally for partitioning/ordering. |
| 3 | **Unanswered** — correction frequency. |
| 4 | Pipeline catches issues via sanity/DQ/count checks; on failure it logs events, marks task/run failed, and triggers SNS/email notifications. |

---

## What this moves, and what it doesn't

### 006 D-1 — declared-check falsifier: **no counterexample, but the inventory is empty**

Not one rule was found that the row / membership / batch_check grammar cannot express — because almost no business rules are enforced today. The falsifier is designed to be broken by a *real rule list*; a list of things the current system doesn't check can't break it and can't discharge it either. **The gate stays open.** Two follow-ups:

- The fixed value sets (category codes, product-type groups, schedule values) are **mapping and defaulting rules**, which in our model belong in the transform stage, not in checks. Worth separating explicitly when the real inventory is gathered — they'll otherwise be miscounted as validation rules.
- Agent IDs and policy numbers being unvalidated is a **finding to take to the business**, not a settled answer: "should an unknown agent ID be an error?" is exactly a membership check waiting to be authored.

### 006 D-5 — the detail/summary premise now looks shaky

Two contradictions surfaced:

1. D-5 was built on carrier feedback (2026-08-06) that **a missing member fails the whole batch**. But COINS config says summary is required (`minimum_files=1`) and **detail is optional (`minimum_files=0`)**. Either the config is wrong, or the premise is.
2. D-5's first real customer for `batch_check` was **detail/summary reconciliation** — and the answer to Q1 is that no such reconciliation is enforced today.

Neither kills the decision (a rule the business *wants* is still a rule), but both need resolving before v1.0. Reconfirm the member requirement, and ask directly: *should* the detail sum to the summary total, and what should happen when it doesn't?

### 007 D-4 — correction-omission: **leaning (b), not confirmed**

Replace-style + corrected full file behaves as complete replacement. If the business confirms intent, the **full-restatement feed class** activates — the framework derives explicit retraction facts for omitted keys and flips state to retracted rather than deleting. Note the contrast worth naming in review: the current clear-and-reinsert *erases* the prior assertion; under conveyer the correction accretes and state moves past it. Same visible outcome, recoverable history.

### 007 D-3 — restatement vs. increment: **mixed, confirmed**

This is the answer the decision record flagged as needing a domain-grain session, and it landed. Restatement is the norm; adjustment lines carrying deltas coexist and are identified by transaction subtype/category (not by sign). Two things follow:

- The **domain-grain session** is now due: are adjustment lines their own grain (each complete at line grain), or do they demand an integrating fold? The transaction subtype/category fields are the lever that may let us treat them as a separate, complete-at-line-grain domain — which would keep last-write-wins and avoid a custom fold entirely. Worth pursuing first.
- **Negative amounts have no agreed meaning.** "Reversal of what was reported" vs "genuinely negative commission" is undecided in the business, not just unimplemented. This must be answered before either path is designed.

### Q2 × Q3 — the hard pairing landed

Complete-replacement corrections *and* increment-shaped adjustment lines is the combination flagged in advance as the hardest case: reversal-under-supersession. **Treat it as one design input.** Sequence it as: (1) confirm Q2 intent with the business, (2) run the domain-grain session on adjustment lines, (3) only then decide whether a custom fold is needed.

### 007 D-3 ordering / [S-8] — filename mapping is load-bearing, partially

Period is in the data (good — no filename mapping needed for period). But **nothing in the data orders two statements about the same period**; today it's processing order. Options to put to the business: is there a field we've overlooked (statement generation timestamp, version, run id)? If genuinely not, arrival order becomes the declared ordering — a defensible choice that must be *declared and recorded*, not inherited by accident.

---

## Still open — the shortlist for the business conversation

1. The real rule list — what *should* be checked, not what is. (006 D-1's binding falsifier is still unmet.)
2. Should unknown agent IDs / policy numbers be errors? Where would the authoritative roster come from, and who owns it?
3. Should detail reconcile to summary in dollars? What happens when it doesn't?
4. Is detail genuinely optional (config says yes; earlier carrier feedback says no)?
5. Does an omitted line in a correction mean withdrawn, or merely not restated?
6. What does a negative line mean — reversal, or a genuinely negative commission?
7. Is there any field that orders two statements about the same period?
8. How often do corrections actually happen?
9. Can a statement reference an agent appointed after the period it covers?
