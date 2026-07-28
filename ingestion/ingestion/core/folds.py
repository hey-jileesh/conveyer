"""Ledger read model: latest-disposition, watermark, acquired-set folds — LLD §7.4.

The delivery ledger is event-sourced (append-only, §6.2): every function here
folds raw `DeliveryRecord` rows into a read model. Callers must always fold —
never read raw rows as "current state" (§6.2's append-only discipline).

All functions are pure: plain values in, plain values out, no I/O, no `now()`.
"""

from collections.abc import Sequence
from datetime import datetime

from ingestion.core.model import DeliveryRecord

# Tie-break order when two rows for the SAME delivery_id share the exact same
# `recorded_at` (e.g. a crash-recovery replay that re-appends a byte-identical
# row): unreadable > superseded > duplicate > incomplete > registered (LLD
# §7.4). Higher rank wins the max-by-(recorded_at, rank) comparison below.
_DISPOSITION_RANK: dict[str, int] = {
    "registered": 0,
    "incomplete": 1,
    "duplicate": 2,
    "superseded": 3,
    "unreadable": 4,
}

# Dispositions whose objects count as "we have these bytes" (LLD §7.4):
# `superseded` stays acquired — a correction arrives as new content (new
# bytes), it does not un-acquire the old ones.
_ACQUIRED_DISPOSITIONS = frozenset({"registered", "duplicate", "superseded"})

# Dispositions that mark a single-object (trailer/timer) delivery as
# defective at its exact observed bytes (LLD §7.4).
_DEFECTIVE_DISPOSITIONS = frozenset({"unreadable", "incomplete"})


def latest_dispositions(rows: Sequence[DeliveryRecord]) -> dict[str, DeliveryRecord]:
    """Group by `delivery_id`, keep the row with max `(recorded_at,
    disposition-rank)` — the rank only breaks EXACT `recorded_at` ties
    deterministically (LLD §7.4). Order-independent and idempotent under
    duplication: `latest_dispositions(rows + rows) == latest_dispositions(rows)`
    (§12.3 property).
    """
    latest: dict[str, DeliveryRecord] = {}
    for row in rows:
        key = (row.recorded_at, _DISPOSITION_RANK[row.disposition])
        current = latest.get(row.delivery_id)
        if current is None:
            latest[row.delivery_id] = row
            continue
        current_key = (current.recorded_at, _DISPOSITION_RANK[current.disposition])
        if key > current_key:
            latest[row.delivery_id] = row
    return latest


def registered_deliveries(rows: Sequence[DeliveryRecord]) -> list[DeliveryRecord]:
    """`latest_dispositions` filtered to `disposition == "registered"`,
    sorted `(received_at, delivery_id)` ascending for a deterministic,
    input-order-independent result (order is not specified by the LLD; this
    module picks one and holds it).
    """
    registered = [r for r in latest_dispositions(rows).values() if r.disposition == "registered"]
    registered.sort(key=lambda r: (r.received_at, r.delivery_id))
    return registered


def feed_watermark(rows: Sequence[DeliveryRecord]) -> datetime | None:
    """Max `received_at` over `registered_deliveries`; `None` if there are none."""
    registered = registered_deliveries(rows)
    if not registered:
        return None
    return max(r.received_at for r in registered)


def acquired_final(rows: Sequence[DeliveryRecord]) -> frozenset[tuple[str, int]]:
    """`(name, bytes)` for EVERY object (including `role == "manifest"`) of
    every delivery whose latest disposition is in
    `{registered, duplicate, superseded}` — `superseded` stays acquired: we
    HAVE those bytes, a correction arrives as new content (LLD §7.4).
    """
    acquired: set[tuple[str, int]] = set()
    for row in latest_dispositions(rows).values():
        if row.disposition in _ACQUIRED_DISPOSITIONS:
            acquired.update((o.name, o.bytes) for o in row.objects)
    return frozenset(acquired)


def observed_defective(rows: Sequence[DeliveryRecord]) -> frozenset[tuple[str, int]]:
    """`(name, bytes)` for SINGLE-OBJECT (trailer/timer) deliveries whose
    latest disposition is in `{unreadable, incomplete}` — defective at
    these exact bytes, so a fix (which necessarily changes the byte count)
    triggers re-selection while a never-fixed file is not re-streamed every
    run. Manifest-mode deliveries are deliberately EXCLUDED: they are
    re-examined manifest-first each run (§9.2), never via this set (LLD
    §7.4).
    """
    defective: set[tuple[str, int]] = set()
    for row in latest_dispositions(rows).values():
        if row.completeness_mode == "manifest":
            continue
        if row.disposition in _DEFECTIVE_DISPOSITIONS:
            defective.update((o.name, o.bytes) for o in row.objects)
    return frozenset(defective)
