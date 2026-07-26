"""make_ledger_fx (pyiceberg, injectable catalog, D-7) -- LLD S7.7 / S6.2.

The Iceberg schema, partition spec, and Athena vacuum table properties are
defined here (not in `bootstrap/create_ledger.py`) because `append`'s pyarrow
conversion needs the EXACT same shape the bootstrap script creates the table
with -- keeping one authoritative definition avoids the two ever drifting.
`bootstrap/create_ledger.py` (which "owns the table", §6.2 -- the actual
`create_table_if_not_exists` call lives there) imports
`LEDGER_ICEBERG_SCHEMA` / `LEDGER_PARTITION_SPEC` / `LEDGER_TABLE_PROPERTIES`
/ `build_catalog` from here rather than redeclaring them (verified live:
pyiceberg's `create_table`/`create_table_if_not_exists` calls
`assign_fresh_schema_ids` internally and REASSIGNS every field id -- by
declaration order -- to whatever `create_table_if_not_exists` was called
with, matching the LLD's own "field ids implicit by declaration order" note.
The numbers below are therefore documentation, not load-bearing).

`append`'s pyarrow schema is derived from the LIVE table at
`make_ledger_fx`-build time (`schema_to_pyarrow(catalog.load_table(...).schema())`),
NOT re-derived from `LEDGER_ICEBERG_SCHEMA` independently -- verified live
that `Table.append` compares the provided PyArrow table against the
target's CURRENT schema by the PARQUET:field_id metadata pyiceberg embeds
on each pyarrow field, not by column name, so a pyarrow schema built from a
schema object with different (e.g. pre-reassignment) field ids is REJECTED
even when every column name matches.
"""

from __future__ import annotations

import functools
import random
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import pyarrow as pa  # type: ignore[import-untyped]
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, BooleanExpression, EqualTo, GreaterThanOrEqual, Reference
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import ListType, LongType, NestedField, StringType, StructType, TimestamptzType

from ingestion import observability
from ingestion.core.model import DeliveryRecord
from ingestion.effects.records import LedgerFx, TransientError

# --- LLD S6.2: pyiceberg schema (field ids implicit by declaration order) --

LEDGER_ICEBERG_SCHEMA = Schema(
    NestedField(1, "delivery_id", StringType(), required=True),
    NestedField(2, "feed_id", StringType(), required=True),
    NestedField(3, "delivery_key", StringType(), required=True),
    NestedField(4, "batch_id", StringType(), required=False),
    NestedField(5, "content_hash", StringType(), required=False),
    NestedField(6, "size_bytes", LongType(), required=False),
    NestedField(7, "object_uris", ListType(8, StringType(), element_required=True), required=True),
    NestedField(
        9,
        "objects",
        ListType(
            10,
            StructType(
                NestedField(11, "name", StringType(), required=True),
                NestedField(12, "role", StringType(), required=True),
                NestedField(13, "uri", StringType(), required=False),
                NestedField(14, "bytes", LongType(), required=True),
                NestedField(15, "sha256", StringType(), required=False),
            ),
            element_required=True,
        ),
        required=True,
    ),
    NestedField(16, "manifest_ref", StringType(), required=False),
    NestedField(17, "asserted_record_count", LongType(), required=False),
    NestedField(18, "completeness_mode", StringType(), required=True),
    NestedField(19, "received_at", TimestamptzType(), required=True),
    NestedField(20, "recorded_at", TimestamptzType(), required=True),
    NestedField(21, "disposition", StringType(), required=True),
    NestedField(22, "supersedes", StringType(), required=False),
    NestedField(23, "driver", StringType(), required=True),
    NestedField(24, "driver_run_id", StringType(), required=True),
    NestedField(25, "notes", StringType(), required=False),
)

# identity(feed_id) + day(received_at), per §6.2.
LEDGER_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="feed_id"),
    PartitionField(source_id=19, field_id=1001, transform=DayTransform(), name="received_at_day"),
)

# §9.4: Athena VACUUM ignores `history.expire.*`; its own 5 d default would
# silently shrink the audit window, so these are set explicitly at bootstrap.
LEDGER_TABLE_PROPERTIES = {
    "vacuum_max_snapshot_age_seconds": "2592000",  # 30 d
    "vacuum_min_snapshots_to_keep": "5",
}

_CATALOG_NAME = "conveyer"
_MAX_COMMIT_ATTEMPTS = 5
_BACKOFF_MIN_S = 0.5
_BACKOFF_MAX_S = 8.0


@dataclass(frozen=True)
class LedgerConfig:
    """Parameterizes which pyiceberg catalog `make_ledger_fx` builds (D-7).
    `sql_uri` is tests-only (SqlCatalog + SQLite + a local FS warehouse);
    production always uses `catalog_kind="glue"`.
    """

    catalog_kind: Literal["glue", "sql"]
    glue_database: str
    table_name: str
    warehouse_uri: str | None = None
    sql_uri: str | None = None  # sql: tests only


def build_catalog(config: LedgerConfig) -> Catalog:
    """Build the pyiceberg `Catalog` named by `config.catalog_kind`. Shared
    between `make_ledger_fx` (below) and `bootstrap/create_ledger.py`'s CLI
    entrypoint, so both build the identical catalog for a given config.

    The glue path is NOT exercised under moto (`moto[glue]` is not an
    installed test dependency, and pyiceberg's `GlueCatalog` performs real
    S3 writes for table metadata/data files that moto's S3 mock does not
    make trivial to wire end-to-end here) -- documented exclusion, same
    shape as `effects/sftp.py`'s (§12.5). `tests/conftest.py` exercises only
    `catalog_kind="sql"`.
    """
    if config.catalog_kind == "sql":
        if config.sql_uri is None or config.warehouse_uri is None:
            raise ValueError("catalog_kind='sql' requires both sql_uri and warehouse_uri")
        return SqlCatalog(_CATALOG_NAME, uri=config.sql_uri, warehouse=config.warehouse_uri)
    if config.warehouse_uri is None:
        raise ValueError("catalog_kind='glue' requires warehouse_uri")
    return GlueCatalog(_CATALOG_NAME, warehouse=config.warehouse_uri)


def _rows_to_arrow(rows: Sequence[DeliveryRecord], pa_schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist([r.model_dump(mode="python") for r in rows], schema=pa_schema)


def _append(
    catalog: Catalog, identifier: str, pa_schema: pa.Schema, rows: Sequence[DeliveryRecord]
) -> None:
    """`table.append()` with retry-on-conflict (LLD §7.7/§6.2): on
    `CommitFailedException` (an optimistic-concurrency conflict -- verified
    live that a stale `Table` handle raises exactly this on `.append()`),
    reload the table fresh from the catalog and retry, up to 5 attempts
    total, jittered 0.5-8 s backoff between attempts, `LedgerCommitRetries`
    emitted once per failed attempt (via `observability.emit_metric`,
    critique-gate F-3 -- previously a private hand-rolled EMF print
    duplicating `observability.py`'s own implementation). Exhausting all
    attempts -> `TransientError`.

    The metric's `feed_id` dimension (§11.2) is taken from `rows[0]` --
    every caller's `rows` share one feed_id in practice (`registrar.execute`
    passes a single delivery's registered + accretion rows;
    `maintenance/optimize.py`'s reconciliation batch, the one exception,
    already emits its own per-feed `SupersessionsReconciled` breakdown
    separately, §9.4) -- `rows` is non-empty here (`if not rows: return`
    above), so `rows[0]` always exists.
    """
    if not rows:
        return
    arrow_rows = _rows_to_arrow(rows, pa_schema)
    last_exc: CommitFailedException | None = None
    for attempt in range(1, _MAX_COMMIT_ATTEMPTS + 1):
        table = catalog.load_table(identifier)
        try:
            table.append(arrow_rows)
            return
        except CommitFailedException as exc:
            last_exc = exc
            observability.emit_metric("LedgerCommitRetries", 1, rows[0].feed_id)
            if attempt < _MAX_COMMIT_ATTEMPTS:
                time.sleep(random.uniform(_BACKOFF_MIN_S, _BACKOFF_MAX_S))
    raise TransientError(
        f"ledger append to {identifier} failed after {_MAX_COMMIT_ATTEMPTS} attempts: {last_exc}"
    ) from last_exc


def _scan_feed(
    catalog: Catalog, identifier: str, feed_id: str, since: datetime | None
) -> list[DeliveryRecord]:
    """RAW rows always -- folding is the caller's job (`core.folds`, §6.2)."""
    # pyiceberg's predicate classes are pydantic models whose mypy-visible
    # constructor is keyword-only (`term=`/`value=`) even though the
    # positional `(term, literal)` form also works at runtime -- use the
    # keyword form so both mypy and the runtime agree (verified live).
    # pyiceberg's predicate generics don't infer `value`'s literal type from
    # a plain positional-construction call; verified live these build and
    # evaluate correctly despite the stub's overly narrow `Literal[Any]`.
    expr: BooleanExpression = EqualTo(term=Reference("feed_id"), value=feed_id)  # type: ignore[arg-type]
    if since is not None:
        expr = And(
            expr,
            GreaterThanOrEqual(term=Reference("received_at"), value=since),  # type: ignore[arg-type]
        )
    table = catalog.load_table(identifier)
    raw_rows = table.scan(row_filter=expr).to_arrow().to_pylist()
    return [DeliveryRecord(**row) for row in raw_rows]


def make_ledger_fx(config: LedgerConfig) -> LedgerFx:
    catalog = build_catalog(config)
    identifier = f"{config.glue_database}.{config.table_name}"
    pa_schema = schema_to_pyarrow(catalog.load_table(identifier).schema())
    return LedgerFx(
        append=functools.partial(_append, catalog, identifier, pa_schema),
        scan_feed=functools.partial(_scan_feed, catalog, identifier),
    )
