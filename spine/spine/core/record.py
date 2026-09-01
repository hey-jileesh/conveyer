"""Framework fact-stamp + ordering-comparability constants — LLD 007.1 §5.1
fragment 4, §6.1, §8.1 (F-1/F-6's code home); consumed by 006.1 §4.1/§5.3.

**One normative home, imported everywhere else (both docs' "never a second
list" rule, made real in code).** `FACT_STAMP_COLUMNS`/`FACT_STAMP_TYPES`
(007.1 §5.1 fragment 4, §6.1) enumerate the seven framework-stamped columns
every fact table carries; `core/model.py`'s `FactSchemaModel` imports
`FACT_STAMP_COLUMNS` to check declared column names are disjoint from them
(006.1 §5's F3 check), and 007.1's `create_record_tables.py` (a later bead)
imports `FACT_STAMP_TYPES` to render the DDL's stamp-column block —
restating, never re-enumerating. `ORDERING_COMPARABLE_TYPES` (007.1 §8.1) is
the closed set of fact-column KINDS legal in a `FactSchemaModel.ordering:`
declaration; `core/model.py`'s F5 bind-adjacent validator imports it rather
than re-listing the set (006.1 §5.3's own text: "this doc never states a
second list").

This module has no dependency on `core/model.py` (or any other spine
module) — the correct direction is `model.py -> record.py`, mirroring
`core/contract.py`'s/`core/naming.py`'s existing precedent for shared
grammar constants that a pydantic-shaped module needs (see those modules'
docstrings): factoring the constant into the dependency-free, lower-level
module and importing it from the higher one, never the reverse.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

# 007.1 §5.1 fragment 4: "Every fact table's framework-stamped columns are
# exactly batch_id, delivery_id, feed_id, received_at, source_ts,
# content_hash, record_key" — D-1's hash-excluded stamps (batch_id,
# delivery_id, feed_id, received_at, source_ts) plus the two derived
# identities (content_hash, record_key). A stamp-set change is an edit to
# this constant + 007.1 §5.1 fragment 4/§6 -- never a second list.
FACT_STAMP_COLUMNS: frozenset[str] = frozenset(
    {
        "batch_id",
        "delivery_id",
        "feed_id",
        "received_at",
        "source_ts",
        "content_hash",
        "record_key",
    }
)


@dataclass(frozen=True)
class FactStampType:
    type: str  # Iceberg DDL type name, e.g. "string" / "timestamptz"
    nullable: bool


# 007.1 §6.1's DDL table: "batch_id/delivery_id/feed_id string non-null;
# received_at timestamptz non-null; source_ts timestamptz NULLABLE (null
# ranks lowest in the fold ordering, §8.1); content_hash/record_key string
# non-null (F-2: commit derives unconditionally)." Same change discipline as
# `FACT_STAMP_COLUMNS` -- a stamp-type change edits this constant + 007.1
# §6.1, never a second list.
FACT_STAMP_TYPES: Mapping[str, FactStampType] = {
    "batch_id": FactStampType(type="string", nullable=False),
    "delivery_id": FactStampType(type="string", nullable=False),
    "feed_id": FactStampType(type="string", nullable=False),
    "received_at": FactStampType(type="timestamptz", nullable=False),
    "source_ts": FactStampType(type="timestamptz", nullable=True),
    "content_hash": FactStampType(type="string", nullable=False),
    "record_key": FactStampType(type="string", nullable=False),
}

# 007.1 §8.1: the closed set of fact-column KINDS legal in an `ordering:`
# declaration -- bare kind names from 006.1 §4.1's `FACT_COLUMN_TYPE_RE`
# grammar (`decimal` covers every `decimal(p,s)` spelling; scale is not
# order-significant, F-6's own note). Excluded: `bool` (no customer, no
# recency meaning); `float`/`double` (structurally unrepresentable as fact
# columns at all, 007.1 §5.1 fragment 3); array/map/struct (not in the fact-
# column type grammar either).
ORDERING_COMPARABLE_TYPES: frozenset[str] = frozenset(
    {"string", "int", "long", "decimal", "date", "timestamp"}
)
