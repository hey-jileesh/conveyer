"""`delta_filter` — the per-type dedup + content-hash delta plan. LLD 007.1
§7.2 (F-5), 007 D-2 (the seam this section restates at F-4's per-table
grain). Register sources: ADR-OQ1/ADR-OQ2 (transcribed, never re-argued
here — `core/delta.py::resolve_predecessors` is where those adjudications
live); this module consumes their output, never re-derives it.

**The pure plan, per type (§7.2).** `delta_filter(candidates_t,
predecessor_facts_t) -> novel_t` is a pure plan builder (I-9's zone): the
framework owns every read (resolving `predecessor_batch_ids` via `core/
delta.py::resolve_predecessors`, then projecting the resolved batches' own
fact-table partitions to `(record_key, content_hash)`), this filter reads
nothing. `record_key` is the join identity (F-2, §5.2), `content_hash` the
compared value (F-1) — identity joins, value compares, never complected
(§5.1 fragment 1).

Semantics, per D-2, restated at this grain:

(a) **Within-batch collapse is unconditional** — `candidates_t` is first
    deduplicated on `(record_key, content_hash)` regardless of what
    `predecessor_facts_t` holds (runs even under a probe refusal, §7.2:
    "it runs even under a probe refusal ... N identical in-batch assertions
    are one fact, not N").
(b) **Divergent within-batch duplicates both commit** — two candidate rows
    sharing a `record_key` but carrying DIFFERENT `content_hash` values are
    not collapsed by (a) (their `(record_key, content_hash)` pairs differ),
    so both survive into `novel_t` (the observable-data condition, D-2(b)).
(c) **A candidate drops iff, per `record_key`, its `content_hash` is
    unchanged in *every* batch of the resolved predecessor set** — at most
    one predecessor batch normally, two when Track E applies (§7.2). Since
    `predecessor_facts_t` is already the UNION of the resolved batches'
    projected `(record_key, content_hash)` rows (batch identity is not a
    column this filter needs — the union already IS "every batch of the
    resolved set"), "unchanged in every batch" becomes: a `record_key`'s
    predecessor rows must all agree on ONE `content_hash` value (never more
    than one distinct value for that key across the whole union), and the
    candidate's own `content_hash` must equal that one value. A `record_key`
    whose predecessor rows carry more than one distinct `content_hash`
    (Track E's two-batch predecessor set disagreeing, or any predecessor
    inconsistency) can never satisfy "unchanged in every batch" for ANY
    candidate value — fail-open, the candidate keeps (§7.2's paths 5-8's own
    law, applied at this grain: never pick a row to believe, the L-3 idiom
    already governing `resolve_predecessors`).

**Zero predecessor rows, either from an empty resolved set (paths 1-3, 5-9's
probe refusals — `resolve_predecessors` already returns `predecessor_batch_
ids = ()` on every one of those) or from a resolved batch that itself
committed zero facts (path 4) is not special-cased here at all**: an empty
(or vacuous) `predecessor_facts_t` produces no `(record_key, content_hash)`
pairs at all, so no candidate can ever match one, and every candidate
survives (a) into `novel_t` — "everything novel", by construction, not by a
guard clause. **No enumerated §7.2 path (1-9) ever causes this filter to
drop a row it did not itself independently verify unchanged.**
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def delta_filter(candidates_t: DataFrame, predecessor_facts_t: DataFrame) -> DataFrame:
    """§7.2's per-type pure plan. `candidates_t` carries the fact type's own
    stamped candidate rows (post `frames/facts.py::stamp_fact_identity`, per
    §4.3's step order — `record_key`/`content_hash` already present);
    `predecessor_facts_t` carries only `record_key`/`content_hash` (the
    framework's own projection of the resolved predecessor batches' fact
    partitions, module docstring). Returns `novel_t`: `candidates_t`'s own
    schema, restricted to the surviving rows — every other column passes
    through unchanged (a join-based plan, no UDF, no action)."""
    within_batch = candidates_t.dropDuplicates(["record_key", "content_hash"])

    predecessor_pairs = predecessor_facts_t.select("record_key", "content_hash").distinct()

    # A record_key is "ambiguous" (never a safe drop target for ANY
    # candidate value, (c)'s fail-open law) when its own predecessor rows
    # disagree on content_hash -- found via a self-join on record_key with
    # a content_hash INEQUALITY, aliased on both sides so the equi/non-equi
    # join conditions never collide (`frames/business_checks.py::_join_
    # membership_marker`'s own aliasing idiom, applied here).
    left = predecessor_pairs.alias("_delta_pred_left")
    right = predecessor_pairs.alias("_delta_pred_right")
    ambiguous_keys = (
        left.join(
            right,
            (F.col("_delta_pred_left.record_key") == F.col("_delta_pred_right.record_key"))
            & (F.col("_delta_pred_left.content_hash") != F.col("_delta_pred_right.content_hash")),
            "inner",
        )
        .select(F.col("_delta_pred_left.record_key").alias("record_key"))
        .distinct()
    )

    # The predecessor pairs safe to compare candidates against: every
    # record_key's predecessor rows agree on exactly one content_hash.
    stable_pairs = predecessor_pairs.join(ambiguous_keys, on="record_key", how="left_anti")

    # A candidate drops iff its OWN (record_key, content_hash) pair exactly
    # matches a stable predecessor pair -- anything else (a genuine value
    # change, a never-before-seen record_key, or an ambiguous predecessor
    # key) fails the match and survives into novel_t.
    return within_batch.join(stable_pairs, on=["record_key", "content_hash"], how="left_anti")
