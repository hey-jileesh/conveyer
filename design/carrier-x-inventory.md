# Carrier-X Commission-Statements — Inventory Brief

**Tracking:** `conveyer-frj` · **Gates:** 006 v1.0 and 007 v1.0 (006 §6.4, 007 §5.7) · **Audience:** whoever holds the carrier-x relationship (business owner / EDI contact), plus the internal session over the feed spec and sample files.

Three questions, one conversation. Each closes a named deferral in the 006/007 decision records — either as predicted, or by reopening the owning decision before 009 freezes the authoring surface. Answers should be recorded back into this file and the bead; the "where answers land" table at the end maps each answer to its action.

> **Round 1 recorded (2026-08-20)** — see the dispositions section below. Sourced from the current system (`carrier-x-answers-round1.md` / `carrier-x-question-pack.md`): strong evidence of mechanism, weak evidence of intent. Q3 landed as *mixed, confirmed* (domain-grain session now due, `conveyer-p3g`), Q2 leans (b) unconfirmed, Q1's system inventory came back empty.
>
> **Gate re-scoped and discharged (2026-08-20)** — Q1's falsifier was discharged by adversarial construction (the scenario inventory below; recorded in 006 D-1); Q2/Q3 reclassified as **Phase 2 feed-onboarding gates** (007 §5.7) since neither can change a settled decision. **006 and 007 are v1.0.** Round 2 continues as the standing business confirmation — it feeds `checks.yaml` authoring and `conveyer-p3g`, and gates the commissions *build*, not the docs.

---

## What we already know — confirm, don't re-ask

- **Delivery mechanics:** SFTP pull from `/outbound/commissions/`, weekday deliveries expected by 06:00 ET, multi-part CSV (`COMM_*`), manifest-based completeness (per-file sha256 + record counts).
- **Members:** a delivery carries a **detail** member and a **summary** member; a missing member fails the whole batch (carrier feedback, 2026-08-06 — the source of 006 D-5).
- **Corrections:** a corrected delivery arrives as a re-sent manifest — same `manifest_id`, changed file hashes, later `created_at` (the supersession trigger, 002.1).
- **Filename carries a date** (`COMM_2026-07-24`): see supporting confirmation S-2 — this matters more than it looks.

---

## Q1 — The rule inventory (006 D-1's binding falsifier)

**Ask:** the complete list of validation and plausibility rules for a commission statement — what makes a line invalid, which cross-field identities must hold, which code sets or rosters lines reference (and where the authoritative list lives), and what must reconcile across the delivery.

**Why:** 006 D-1 commits `post_check` to three declared check kinds — **row** (an expression over one line's values), **membership** (a value must exist in a reference table), **batch_check** (an aggregate reconciled against the summary member) — with *zero* pipeline check code and no escape hatch. The prediction: every real rule maps. One counterexample reopens that rejection now, cheaply, instead of after 009 freezes the surface.

**Scaffold** — pre-seeded with the predicted rules; extend with every rule the spec/business session surfaces:

| # | Rule (plain language) | Example | Expected home | Status |
|---|---|---|---|---|
| 1 | Required fields present, typed, in range | commission amount is a decimal | admission contract (005) | predicted |
| 2 | Amount sign / bounds plausibility | commission not negative (unless reversal line — see Q3) | row | predicted |
| 3 | Cross-field arithmetic on one line | net = gross − fees | row | predicted |
| 4 | Line references a known agent / policy / product | agent id exists in the agent roster | membership (co-effect) | predicted |
| 5 | Detail reconciles to summary member | sum of detail commission = summary total | batch_check (D-5) | confirmed 2026-08-06 |
| 6 | Effective-dated reference validity | line valid only within the agent's appointment dates | membership + declared predicate (named growth candidate, 006 §5.1) | watch for |
| 7 | *…every further rule from the spec/session* | | | |

**Outcomes:** all rows map → D-1's escape-hatch rejection is final; 009 may freeze. Any rule that none of the three kinds (or the named conditioned-membership growth) can express → reopen 006 D-1 before v1.0.

---

## Q2 — Corrections that omit lines (007 D-4's trigger)

**Ask, verbatim:** *"When you re-send a corrected statement — same statement id, new version — and a line that appeared in the original does not appear in the correction: what does that mean? (a) Nothing — the correction restates what it restates, and an omitted line still stands as originally reported. (b) The correction is the complete replacement — anything absent from it no longer stands."* And the follow-up: *"Are corrections always complete re-statements of the full file set, or can they be partial (only the changed lines)?"*

**Why:** 007 D-4's default is **accretion — absence asserts nothing**: an omitted line produces no change, and retraction must be explicit (a reversal or zeroing line). If the carrier's corrections mean (b), that default silently mis-states their intent, and the **full-restatement feed class** (framework-derived retraction facts; state flips to retracted, never dropped) gains its first customer and gets designed.

**Outcomes:** (a) → deferral closes as predicted; default holds. (b) → the full-restatement override design activates for this feed class before the commissions pipeline builds. Partial corrections + (b) would be contradictory — if claimed, that inconsistency goes back to the carrier.

---

## Q3 — Restatement vs. increment (007 D-3's trigger)

**Ask, verbatim:** *"Does each line carry the full current value for its subject, or is it an adjustment to be added up? Concretely: commission for policy P in the June period was reported as 10.00; it is later determined to be 15.00. Does the next statement carry **15.00** (the corrected amount) or **+5.00** (an adjustment line)? Do negative/reversal lines occur, and does a reversal mean 'subtract this from what was reported' or 'the value is now negative'?"*

**Why:** 007 D-3's grain law requires each fact to be a **complete assertion of its domain's state** — that's what lets default last-write-wins fold it, with the proof obligations already met. Increment-shaped lines need an integrating fold, which is bind-refused in Phase 1: the custom-fold contract (including reversal-under-supersession — the part that cannot be designed by guessing) becomes a real design task, and **the Phase 2 exemplar pipeline cannot build on LWW** until it's done or the domain is re-grained.

**Outcomes:** restatement → deferral closes; LWW folds the feed. Increment → the custom-fold design task activates ahead of Phase 2. Mixed (restatements normally, adjustment lines for prior periods) → the domain-grain modeling session decides whether adjustment lines are their own domain grain (each complete at line grain) before any custom fold is contemplated.

**Interaction, named:** Q2(b) × Q3(increment) is the hardest combination — a complete-replacement correction over adjustment lines is exactly the reversal-under-supersession case 007 D-3 refuses to design speculatively. If both answers land that way, that pairing becomes the design input, not two separate ones.

---

## Supporting confirmations (feed 005 v1.x member grammar + the fact schema — not v1.0 gates)

| # | Confirm | Feeds |
|---|---|---|
| S-1 | Exact member set and filename patterns: which files are detail, which summary; is the summary always present, or absent in zero-activity periods? | 005 v1.x declared members (`required` flags — 006 §7 register) |
| S-2 | Where does the statement period live **in the data**? The filename carries a date (`COMM_2026-07-24`) — if the period rides *only* in the filename, the feed waits on the declared filename-mapping (006 D-4 / [S-8]) before it can build | fact schema; 005 v1.x/009 filename mapping |
| S-3 | What orders two assertions about the same policy/period across statements — a statement date column, a version field, line sequence? | 007 D-3's declared `ordering:` columns |
| S-4 | Summary member shape: exactly one total row? Several (per agent, per product)? | D-5's control-extraction LLD grain (006.1) |

---

## Round 1 — dispositions (2026-08-20, current-system reading)

New facts first: **two feeds, not one** — COINS (weekly) and ISG (monthly), both carrying summary + detail members (ISG ships `.txt`); COINS config marks **summary required, detail optional**; the statement period lives in **data columns** (COINS `PYMNT_CYC_FRM_DTE`/`PYMNT_CYC_TO_DTE`, ISG `me_date`); nothing in the data orders two statements about the same period — "newer" is processing order today.

| Gate | Disposition | What moved |
|---|---|---|
| Q1 · 006 D-1 | **open — falsifier unmet** | No counterexample, but the inventory is *empty rather than complete*: the current system enforces almost no business rules. The fixed value sets found (category codes, product groups, schedule values) are mapping/defaulting rules — `apply`'s ground, not checks; keep them out of the rule count. Agent/policy IDs unvalidated today = a membership check awaiting a business decision (round 2 #2). |
| Q2 · 007 D-4 | **open — leaning (b), unconfirmed** | Mechanism is replace-style: clear the period, reinsert the corrected full file — behaves as complete replacement and implies full-file corrections. But mechanism ≠ intent: replace-and-reinsert is the cheapest build under either meaning. If (b) confirms, the full-restatement feed class activates. |
| Q3 · 007 D-3 | **answered — mixed, confirmed** | Restatement is the norm; adjustment lines carrying deltas coexist, identified by transaction subtype/category — never by amount sign. The **domain-grain session is now due**: subtype/category is the lever that may make adjustment lines their own complete-at-line-grain domain, keeping default LWW and avoiding a custom fold. Prerequisite: negative-line semantics (round 2 #6) — undecided in the business, not merely unimplemented. |
| Q4 · ordering | **absorbed by design; question stands** | Period is real data → S-2 closes, no filename mapping needed for the period. No intra-period ordering field exists; 007 D-3's total order absorbs that honestly — declare the period columns as `ordering:`, and same-period ties fall to the stamped `received_at`: arrival order becomes *declared and recorded* instead of inherited by accident. Round 2 #7 still asks whether a real field exists. |
| S-1 · members | **answered — one premise to reconfirm** | Patterns recorded per feed. COINS "summary required / detail optional" matches 006's register design exactly: absent optional detail ⇒ zero rows ⇒ the batch_check reconciles 0 against the summary total, and the control source (summary) is the required member, so the coherence rule holds. Residue: the 2026-08-06 "a missing member fails the whole batch" feedback needs reconfirming — it likely meant the summary (round 2 #4) — and detail-to-summary dollar reconciliation is a *want*-question, since no such check exists today (round 2 #3). |
| S-4 · summary shape | **open** | Not covered by round 1. |

**The hard pairing landed:** Q2 leaning (b) × Q3 mixed is complete-replacement corrections over adjustment lines — reversal-under-supersession, flagged in advance. Treat as one design input, sequenced: confirm Q2 intent → domain-grain session → only then the custom-fold decision.

### Adversarial falsifier run (2026-08-20) — Q1 discharged by construction

The round-1 system inventory was empty (almost nothing enforced today), which can neither break nor discharge a falsifier. So the attack ran by construction: thirteen scenarios aimed at the grammar's weak classes — cross-line, cross-statement, date-conditioned, stateful — from commission-domain knowledge plus the round-1 data facts.

| # | Scenario | Verdict — where it lands |
|---|---|---|
| 1 | Negative amount on a non-adjustment line (sign × subtype coherence) | **row** |
| 2 | Cross-field arithmetic: net = gross − fees; commission ≈ premium × rate within tolerance | **row** (allowlist carries `abs`/`round` — its normal class) |
| 3 | Date sanity: period_from ≤ period_to; statement date ≥ period end | **row** |
| 4 | Category / product codes in fixed sets | **admission contract** (allowed-values), not post_check |
| 5 | Agent / policy exists in a roster or master | **membership** (roster ingested as a feed — D-2's known trade) |
| 6 | Agent appointed during the statement period; licensed in the policy's state | **conditioned membership** — named growth, 006 §5.1; additive |
| 7 | Detail dollars / counts reconcile to summary member | **batch_check** (D-5's first customer) |
| 8 | Per-agent subtotals reconcile (if S-4 says per-agent summary rows) | **grouped `batch_check`** — second named growth, 006 §5.1; additive, value-determined |
| 9 | This month's total is 3× last month's | **observability**, non-gating (004 §13.4) — an alarm, never admission |
| 10 | Duplicate policy line within one file | dissolves structurally — value-identical dupes collapse at 007 dedup; divergent dupes both commit and are the queryable 007 D-2(b) condition |
| 11 | Same period re-delivered without a correction marker | **registration / supersession** ground (002.1), not a check |
| 12 | Orphan adjustment: an adjustment line whose original was never reported | **`own_state` zone** — expressible as membership against the pipeline's own state; deliberately bind-refused (006 D-3) until serialize-honoring exists; feeds `conveyer-p3g` |
| 13 | YTD column consistency vs. prior statements | same `own_state` class; likely observability rather than a reject rule |

**Verdict pattern:** every scenario is expressible today, expressible via already-named additive growth (#6, #8), or lands in another designed home (#4, #9, #10, #11) — including two (#12, #13) landing in the deliberately locked `own_state` zone. **Zero scenarios need arbitrary check code.** Recorded as the falsifier discharge in 006 D-1; the business rule list is downgraded to standing confirmation — a future counterexample is an erratum adding a declared kind, additive by D-1's own argument.

### Round 2 — the business shortlist (standing confirmation; gates the build, not the docs)

What *should* be true, not what the code does. For the business owner of the carrier relationship:

1. The real rule list — what should be checked before a statement is trusted. (006 D-1's falsifier is still unmet.)
2. Should unknown agent IDs / policy numbers be errors? Where would the authoritative roster live, and who owns it?
3. Should detail reconcile to summary in dollars? What should happen when it doesn't?
4. Is detail genuinely optional? (Config says yes; the 2026-08-06 feedback said a missing member fails the batch.)
5. Does an omitted line in a correction mean *withdrawn*, or merely *not restated*?
6. What does a negative line mean — a reversal of what was reported, or a genuinely negative commission?
7. Is there any field that orders two statements about the same period?
8. How often do corrections actually happen?
9. Can a statement reference an agent appointed after the period it covers?

---

## Where answers land

| Answer | Owning decision | Action |
|---|---|---|
| Q1: all rules map | 006 D-1 | falsifier discharged; escape-hatch rejection final |
| Q1: counterexample | 006 D-1 | reopen D-1 in 006 before v1.0/009 freeze |
| Q2: (a) omission asserts nothing | 007 D-4 | deferral closed as predicted |
| Q2: (b) complete replacement | 007 D-4 | full-restatement feed class design activates |
| Q3: restatement | 007 D-3 | deferral closed; LWW folds the feed |
| Q3: increment / mixed | 007 D-3 | custom-fold design task (or re-grain) before Phase 2 |
| All three recorded | 006/007 | ~~both docs clear to v1.0; close `conveyer-frj`~~ **superseded 2026-08-20**: docs reached v1.0 via the adversarial discharge; `conveyer-frj` stays open as the standing business-confirmation watch + Phase 2 feed-onboarding gates (007 §5.7) |
