"""`resolve_predecessors` — the batch-grain predecessor resolution F-5's
`delta_filter` consumes. LLD 007.1 §7.2 (F-5); ADR-OQ1 (M-1…M-3), ADR-OQ2
(N-1…N-4), ADR-OQ5 L-3 (all transcribed, never re-argued here).

**One pure, total decision over already-read marker rows.** The three
marker reads (commit-completion rows, guard-twin/"presence" rows, the named
Track-E target's own rows) are the framework's (§7.2: "core/delta.py::
resolve_predecessors(...) — a pure, total decision over framework-read
plain values … `RunnerFx` accretes the marker read members additively");
this module never touches a catalog, a Spark session, or `RunnerFx` itself
-- callers hand it plain values already fetched, exactly `core/doors.py`'s
own "the planner takes booleans, not catalogs" posture applied to marker
rows instead of presence booleans.

**`MarkerRowWrite` (B9b, `stages/commit.py`'s own F-9 mechanics, §4.3) is
this module's write-side value counterpart** -- the decide-then-do marker
write (guard-twin and commit-completion rows alike) is orchestrated by the
STAGE (`table_has_batch`-style presence probe, then a conditional append,
both real `RunnerFx` effects, §4.3's normative order), never decided here;
this module only shapes the plain value the write effect consumes, the same
division of labour `MarkerRow` already holds for reads.

**Read 1 (ADR-OQ1) and read 2 (ADR-OQ2) run once per batch, independently,
then combine.** Read 1 resolves "the feed's latest completed batch" (self
-excluded, `max(received_at, batch_id)`, the coherence clause, and L-3's
read-1 extension [AE2-1] over the WINNER's own row set). Read 2 resolves
Track E's named superseded batch (or proves it absent) under a detection-
only coherence probe (N-1: never nominates, never adjudicates) yielding one
of ADR-OQ2's four reason codes, verbatim, on any refusal. **A read-2 refusal
forces the WHOLE resolution empty** (§7.2: "delta_probe_refusal set once
… non-null ⇒ every per-type filter short-circuits to keep-all" — read 1's
own, independently-legitimate finding is discarded too, matching "every
failure direction is keep" at BATCH grain, not just Track E's own
contribution): the two `PredecessorResolution` fields are therefore not
independent -- `probe_refusal is not None` if and only if
`predecessor_batch_ids == ()` for THAT reason (as opposed to genesis/
coherence-clause/L-3-disagreement, read 1's own three "empty, no refusal"
outcomes, §12's [AE2-10]: "Deliberately no probe metric for the read-1
coherence-clause keeps … they are not probe refusals").

**N-1, the standing audit (binding, ADR-OQ2).** "`resolve_predecessors`
takes the target *in* and never returns a batch id it was not given" -- by
construction here: read 1's contribution is always some `row.batch_id` out
of `completion_rows`; read 2's contribution is always exactly
`seed_attrs.supersedes_batch_id`, verbatim. No code path in this module
selects, infers, or nominates a batch id from data.

**Marker read errors are not this module's concern.** "A failed marker read
is a stage failure (retry), never a refusal — refusals are adjudication
outcomes, errors are errors; there is deliberately no fifth reason code for
infrastructure" (§7.2) -- a failed read never reaches this function at all;
its four callable inputs are the successfully-read result.
`horizon-exceeded` ships in `DELTA_PROBE_REFUSAL_REASONS` (§4.2 already
names it in the enum) but Phase 1 never models retention, so no code path
below produces it -- "constructed-but-unreachable" (this bead's own naming),
reserved so the rule is priced, never retrofitted (M-3's strengthened form).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

# ADR-OQ2's four probe-refusal reason codes, verbatim (007.1 §4.2/§7.2) --
# the one normative enumeration; `horizon-exceeded` is named but Phase 1
# never constructs it (see module docstring).
DELTA_PROBE_REFUSAL_REASONS: frozenset[str] = frozenset(
    {
        "none-with-key-match",
        "target-incoherent",
        "target-unmarked",
        "horizon-exceeded",
    }
)


@dataclass(frozen=True)
class MarkerRow:
    """One marker-table row (007.1 §6.3's DDL), narrowed to the columns
    `resolve_predecessors` reads -- `stage`/`table_name`/`snapshot_id`/
    `committed_at` never enter this decision (callers already sort rows into
    `completion_rows`/`presence_rows`/`target_rows` by row kind before
    calling; this module only ever groups by `batch_id`, never branches on
    which physical column discriminated the row kind)."""

    batch_id: str
    delivery_key: str
    delivery_content_hash: str
    received_at: datetime  # aware -- M-2's ordering key


@dataclass(frozen=True)
class MarkerRowWrite:
    """One marker-table row to WRITE (007.1 §6.3's DDL, B9b) -- the write-
    side counterpart to `MarkerRow` above (reads). `snapshot_id` is not a
    field here: §6.3's own resolution is that it is NULL on every Phase-1
    row by write-order necessity (L-2's marker-first ordering means a
    guard-twin row's own fact append has not committed yet, so no snapshot
    id exists to carry) -- the writer hardcodes the NULL, never threading a
    value that can never be anything else. `stage` is always `"commit"` in
    Phase 1 (§6.3: "fold rows reserved as additive accretion") but carried
    as a field rather than hardcoded here, matching `MarkerRow`'s own
    reader-side stance of not baking in a stage literal. `table_name` is
    either a real fact table (a guard-twin row) or the sentinel
    (`naming.COMMIT_COMPLETION_SENTINEL`, the one completion row per batch,
    §6.3 answer 1) -- this module does not import `naming` (a `core/`-to-
    `core/` dependency it has no other reason to take), so the caller
    (`stages/commit.py`) supplies whichever value applies."""

    batch_id: str
    feed_id: str
    stage: str
    table_name: str
    delivery_key: str
    delivery_content_hash: str
    received_at: datetime  # aware -- the seed's own value, §6.3's DDL
    committed_at: datetime  # aware -- the writing effect-point's own clock (fx.now()), provenance
    # only, never ordering (M-2) -- §6.3's own words


@dataclass(frozen=True)
class SeedAttrs:
    """The calling batch's own seed-derived attributes `resolve_predecessors`
    needs -- `batch_id` for self-exclusion everywhere (M-1: "a batch is
    never its own predecessor"), `delivery_key`/`delivery_content_hash` for
    read 2's field-absent key-match scan, and `supersedes_batch_id` for read
    2's Track-E adjudication (007 D-2/ADR-OQ2's field; `DeliveryRegisteredV1`
    does not carry it yet -- the `conveyer-kof` wait, 006.1 P-6's idiom.
    Every current caller passes `None`; this module's OWN behavior needs no
    change when the field lands (§7.2: "design against the pinned seed
    interface … no F-5 edit occurs")."""

    batch_id: str
    delivery_key: str
    delivery_content_hash: str
    supersedes_batch_id: str | None


@dataclass(frozen=True)
class PredecessorResolution:
    """§7.2's one resolution, one probe verdict. `predecessor_batch_ids`:
    empty = no predecessor ⇒ everything novel (read 1's genesis/coherence-
    clause/L-3-disagreement outcomes, OR a read-2 refusal forcing the whole
    set empty). `probe_refusal`: one of `DELTA_PROBE_REFUSAL_REASONS`, set
    only by read 2; `None` on every read-1-only outcome (§12 [AE2-10]) and
    on a successful (possibly vacuous) resolution."""

    predecessor_batch_ids: tuple[str, ...]
    probe_refusal: str | None


def _group_by_batch(rows: Sequence[MarkerRow]) -> dict[str, list[MarkerRow]]:
    groups: dict[str, list[MarkerRow]] = {}
    for row in rows:
        groups.setdefault(row.batch_id, []).append(row)
    return groups


def _attrs_agree(rows: Sequence[MarkerRow]) -> bool:
    """L-3: every row sharing one `batch_id` must agree on delivery
    attributes (`delivery_key`, `delivery_content_hash`, `received_at`) --
    disagreement (a write bug) makes the batch incoherent; `rows` is never
    empty at any call site below (each caller only invokes this over a
    batch it already knows has at least one row)."""
    distinct = {(row.delivery_key, row.delivery_content_hash, row.received_at) for row in rows}
    return len(distinct) <= 1


def _resolve_read1(
    self_batch_id: str,
    completion_rows: Sequence[MarkerRow],
    presence_rows: Sequence[MarkerRow],
) -> str | None:
    """ADR-OQ1 (M-1…M-3) + L-3's read-1 extension [AE2-1]. §7.2 paths 1-3
    and K-07's read-1 variant all resolve here to `None`; every other
    (self-excluded) outcome resolves to the winning batch id."""
    other_completions = [row for row in completion_rows if row.batch_id != self_batch_id]
    other_presence = [row for row in presence_rows if row.batch_id != self_batch_id]
    completed_ids = {row.batch_id for row in other_completions}

    # Coherence clause (paths 2/3): ANY same-feed batch (self excluded)
    # showing guard-twin presence with NO commit-completion row voids read 1
    # ENTIRELY -- an in-flight sibling might complete later with a later
    # (received_at, batch_id) than today's apparent winner, or a
    # kill-in-commit/marker-without-facts state already holds committed
    # facts a wrong drop could interact with. This is a blanket veto, not
    # scoped to a specific candidate -- "every batch whose committed facts a
    # wrong drop could interact with is visible to this clause" (§7.2).
    if any(row.batch_id not in completed_ids for row in other_presence):
        return None
    if not other_completions:
        return None  # path 1: genesis / fresh feed

    winner = max(other_completions, key=lambda row: (row.received_at, row.batch_id))
    winner_rows = [
        row for row in (*other_completions, *other_presence) if row.batch_id == winner.batch_id
    ]
    if not _attrs_agree(winner_rows):
        # K-07's read-1 variant [AE2-1]: the selected latest-completed
        # batch's OWN marker rows disagree internally -- never pick a row to
        # believe; names no predecessor (no re-attempt against a
        # second-best candidate -- the LLD states an unconditional "no
        # predecessor" outcome here, not a fallback search).
        return None
    return winner.batch_id


def _resolve_read2(
    seed_attrs: SeedAttrs,
    completion_rows: Sequence[MarkerRow],
    presence_rows: Sequence[MarkerRow],
    target_rows: Sequence[MarkerRow],
) -> tuple[str | None, str | None]:
    """ADR-OQ2 Option D (N-1…N-4). Returns `(target_batch_id, refusal)` --
    at most one of the two is non-`None` at once (a refusal never also
    names a target; a lawful field-absent no-hit names neither)."""
    if seed_attrs.supersedes_batch_id is None:
        # Field None/absent (§7.2): scan marker presence, ANY row kind,
        # self excluded, for the seed's OWN delivery_key.
        all_rows = [
            row for row in (*completion_rows, *presence_rows) if row.batch_id != seed_attrs.batch_id
        ]
        by_batch = _group_by_batch(all_rows)
        for rows in by_batch.values():
            if not any(row.delivery_key == seed_attrs.delivery_key for row in rows):
                continue  # not a candidate -- no key overlap on any of this batch's rows
            if not _attrs_agree(rows):
                return None, "none-with-key-match"
            if rows[0].delivery_content_hash != seed_attrs.delivery_content_hash:
                return None, "none-with-key-match"
        return None, None  # N-4: no hit against the genesis-complete record

    # Field names a target: read the target's own marker rows (both row
    # kinds -- `target_rows` is single-partition-scoped to this one
    # batch_id by the caller, §6.4's economics).
    target_id = seed_attrs.supersedes_batch_id
    own_target_rows = [row for row in target_rows if row.batch_id == target_id]
    if not own_target_rows:
        return None, "target-unmarked"  # N-3: absence is unknown, never "no supersession"
    if not _attrs_agree(own_target_rows):
        return None, "target-incoherent"  # L-3: internal disagreement
    if own_target_rows[0].delivery_key != seed_attrs.delivery_key:
        # Cross-check against THIS (superseding) delivery's own key --
        # ADR-OQ2: "Event names a superseded batch whose marker row is
        # delivery_key-incoherent with this delivery … visible ambiguity".
        return None, "target-incoherent"
    return target_id, None


def resolve_predecessors(
    seed_attrs: SeedAttrs,
    completion_rows: Sequence[MarkerRow],
    presence_rows: Sequence[MarkerRow],
    target_rows: Sequence[MarkerRow],
) -> PredecessorResolution:
    """007.1 §7.2's one batch-grain resolution, combining read 1 (ADR-OQ1)
    and read 2 (ADR-OQ2 Option D). Pure, total over its four plain-value
    arguments -- `completion_rows`/`presence_rows` are the feed's own commit
    -completion rows and guard-twin rows respectively (already scoped to the
    feed by the framework read); `target_rows` is the Track-E named target's
    own rows (empty when `seed_attrs.supersedes_batch_id is None`, or when
    the target genuinely carries no marker rows at all)."""
    target_id, refusal = _resolve_read2(seed_attrs, completion_rows, presence_rows, target_rows)
    if refusal is not None:
        # §7.2: "non-null ⇒ this batch dropped nothing (everything novel)"
        # -- a batch-grain short-circuit; read 1's own finding (if any) is
        # discarded too, never surfaced as a partial predecessor set.
        return PredecessorResolution(predecessor_batch_ids=(), probe_refusal=refusal)

    read1_id = _resolve_read1(seed_attrs.batch_id, completion_rows, presence_rows)
    predecessor_batch_ids = tuple(
        batch_id for batch_id in (read1_id, target_id) if batch_id is not None
    )
    return PredecessorResolution(predecessor_batch_ids=predecessor_batch_ids, probe_refusal=None)
