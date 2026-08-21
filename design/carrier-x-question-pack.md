# Commission Statements — Questions for the Business

**Who this is for:** whoever knows how commission statements actually work — the business owner of the carrier relationship, or the developer who has been reading these files for years.

**Why we're asking.** We're calibrating the design of the framework that will process carrier files — not just commission statements, but every feed that follows. Commission statements are our reference case: real rules, real corrections, real awkward edge cases. Four questions decide how the framework handles checking, corrections, and conflicting versions across every feed we build on it. We'd rather answer them from how your statements actually behave than from assumptions, because these choices get locked in early and are expensive to unpick later.

**What we need from you:** how the statements behave in practice — including the weird cases. You don't need to know anything about how the system is built.

---

## Question 1 — What makes a statement wrong?

**The ask:** walk us through everything you check, or wish someone checked, before you trust a commission statement. What has burned you before?

Some prompts, to jog memory:

**Looking at a single line on its own**

- Can a commission amount be negative? Under what circumstances?
- Does net have to equal gross minus fees? Any other arithmetic that must hold?
- Are there fields with a fixed set of allowed values — transaction type, product category, status codes?
- What would be an obvious error on sight? (A $1M commission on a $500 premium?)

**Values that have to match a list we hold**

- Which values must exist in a list somewhere — agent IDs, policy numbers, product codes?
- Where does the authoritative version of that list live, and who owns it?
- How current does it need to be? Is yesterday's agent roster good enough?
- Can a legitimate statement reference an agent who was appointed last week?

**Things that only make sense across the whole statement**

- What has to add up? Does the detail total have to equal the summary total? Record counts?
- What do you do today when they don't match?

**And the ones that are easy to forget** — please think hard about these:

- Any rule that compares lines *to each other*? ("Flag if the same policy appears twice in one file.")
- Any rule that depends on *dates*? ("A line only counts if it falls inside the agent's appointment period.")
- Any rule that compares this statement to *previous* statements? ("Flag if this month's total is triple last month's.")

**What happens with your answers:** each rule becomes a written, reviewable rule in the system rather than something living in someone's head or a spreadsheet macro. The last group is the one most likely to change our design, which is why we're pushing on it.

---

## Question 2 — When a correction leaves a line out, what does that mean?

**A concrete case.** The original statement 1001 has three lines:

| Policy | Commission |
|---|---|
| P-100 | $10 |
| P-200 | $20 |
| P-300 | $30 |

A corrected statement 1001 arrives later with only two lines:

| Policy | Commission |
|---|---|
| P-100 | $10 |
| P-200 | $25 |

**What happened to P-300?**

- **(a)** Nothing. It still stands at $30. The correction only speaks about the lines it contains.
- **(b)** It's withdrawn. The correction is the complete replacement — anything not in it no longer stands.

**Then:**

- Are corrections always a full re-send of the whole statement, or can they be partial — only the changed lines?
- If the carrier needed to cancel a line outright, how would the file say so? A reversal line? A zero-amount line? Something else?

**Why it matters:** if we assume (a) and the truth is (b), withdrawn commissions keep being paid out on our side. If we assume (b) and the truth is (a), a partial correction silently wipes out most of a statement. There's no safe default — we need the real answer.

*(If the answer is "corrections are partial" **and** "anything absent is withdrawn," those two can't both be true — a partial correction would cancel everything it didn't mention. Worth resolving in the room.)*

---

## Question 3 — Does a line carry the full amount, or the change?

**A concrete case.** June commission for policy P-100 was reported as **$10.00**. It's later determined the correct figure is **$15.00**.

On the next statement, does the line for P-100 / June say:

- **$15.00** — the corrected full amount, replacing what came before, or
- **+$5.00** — an adjustment to be added to what was already reported?

**Then:**

- Do negative lines occur? If we see P-100 at **−$10.00**, does that mean "subtract $10 from what was reported" or "the commission for this is now negative ten dollars"?
- Is it ever both? Normal months carry full amounts, but prior-period fixes arrive as separate adjustment lines?
- If both happen — how do we tell an adjustment line from a normal one? A period field, a transaction code, something else?

**Why it matters:** these are two completely different ways of doing arithmetic. If every line is the full current amount, the newest line simply wins. If lines are adjustments, everything has to be added up in the right order, forever — and getting a re-sent correction right becomes genuinely hard. We can build either; we can't build both by accident.

---

## Question 4 — If two statements disagree, which one wins?

Two statements both say something about policy P-100 for June — one says $10, one says $15.

**What tells us which is newer?**

- A statement date or period **field inside the file**?
- A version number or sequence field?
- Or only the **file name and the time it arrived**?

**Why it matters:** if the answer is "only the file name," then the date buried in the filename (`COMM_2026-07-24`) is doing real work — it's business information sitting outside the data, and we have to handle it deliberately rather than treat it as a label. If there's a proper field in the file, this gets much simpler.

---

## Things we'd like confirmed while we're together

| # | Confirm |
|---|---|
| 1 | Which files are the detail and which are the summary. Is a summary always sent — even in a month with no activity? |
| 2 | Where does the statement period actually live? Only in the filename, or in a column too? |
| 3 | Roughly how often do corrections happen — every month, a few times a year, rarely? |
| 4 | When something looks wrong today, who notices, and what happens next? |

---

## Capture sheet

Fill in during the conversation; verbatim answers are more useful than summaries.

| # | Question | Answer |
|---|---|---|
| 1 | The rule list | |
| 2 | Omitted line: (a) still stands / (b) withdrawn | |
| 2b | Corrections full or partial | |
| 3 | $15.00 (full amount) or +$5.00 (adjustment) | |
| 3b | Meaning of a negative line | |
| 4 | What orders two conflicting statements | |

---

<details>
<summary><b>Internal only — how answers map to our design decisions</b></summary>

| Question | Gate | Outcomes |
|---|---|---|
| Q1 | 006 D-1 (declared-check falsifier) | Every rule maps to row / membership / batch_check → escape-hatch rejection final, 009 may freeze. Cross-line, cross-statement, or date-conditioned rule that none of the three kinds expresses → reopen D-1 before v1.0. Appointment-period is the already-named conditioned-membership growth candidate (not a counterexample); trend-vs-last-month is observability, not admission. |
| Q2 | 007 D-4 | (a) → accretion default holds, deferral closes. (b) → full-restatement feed class design activates before this pipeline builds. |
| Q3 | 007 D-3 | Full amount → LWW fold, deferral closes. Increment → custom-fold design task (incl. reversal-under-supersession) before Phase 2. Mixed → domain-grain session first. |
| Q2 × Q3 | 007 D-3/D-4 | (b) + increment is the hardest pairing — one design input, not two answers. Name it in the room. |
| Q4 | 007 D-3 ordering columns; S-2 / 006 D-4 [S-8] | Real column → declare as `ordering:`. Filename only → declared filename mapping becomes load-bearing; feed waits on it. |
| Confirmations 1–2 | 005 v1.x member grammar (`required` flags); fact schema | S-1 / S-2 from the inventory brief. |

Answers land in `carrier-x-inventory.md`; bead `conveyer-frj`.

</details>
