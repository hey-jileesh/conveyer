# Carrier-X Commission-Statements — Inventory Brief

**Tracking:** `conveyer-frj` · **Gates:** 006 v1.0 and 007 v1.0 (006 §6.4, 007 §5.7) · **Audience:** whoever holds the carrier-x relationship (business owner / EDI contact), plus the internal session over the feed spec and sample files.

Three questions, one conversation. Each closes a named deferral in the 006/007 decision records — either as predicted, or by reopening the owning decision before 009 freezes the authoring surface. Answers should be recorded back into this file and the bead; the "where answers land" table at the end maps each answer to its action.

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

## Where answers land

| Answer | Owning decision | Action |
|---|---|---|
| Q1: all rules map | 006 D-1 | falsifier discharged; escape-hatch rejection final |
| Q1: counterexample | 006 D-1 | reopen D-1 in 006 before v1.0/009 freeze |
| Q2: (a) omission asserts nothing | 007 D-4 | deferral closed as predicted |
| Q2: (b) complete replacement | 007 D-4 | full-restatement feed class design activates |
| Q3: restatement | 007 D-3 | deferral closed; LWW folds the feed |
| Q3: increment / mixed | 007 D-3 | custom-fold design task (or re-grain) before Phase 2 |
| All three recorded | 006/007 | both docs clear to v1.0; close `conveyer-frj` |
