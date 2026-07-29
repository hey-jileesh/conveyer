"""Shared scenario-suite helpers — table-DDL, spec/seed builders, batch ids.

Promoted out of `test_scenarios_core.py` into this dedicated, non-test
module (critique F7 hygiene, bead conveyer-nvh.43): `test_scenarios_kill.py`,
`test_entrypoint.py`, and `test_scenarios_fold.py` all used to import these
directly from `test_scenarios_core.py` (`tests/` has no `__init__.py`
anywhere in this repo, so same-directory modules import as bare top-level
names, per `[[spine-test-basename-collisions-and-scope-conflicts]]`) — a
real test FILE importing another test file's private helpers, rather than a
shared, non-test helper module (the same shape `killfx.py`/
`snapshot_asserts.py` already are). `test_scenarios_core.py` itself now
imports from here too, rather than keeping its own copies.

Every function below targets the `pipelines.identity`/
`pipelines.identity_violations` exemplar's fixed 9-column shape
(`domain_id, event_time, source_ts, content_hash, payload, batch_id,
delivery_id, feed_id, received_at`) — see `test_scenarios_core.py`'s own
module docstring for why that shape is shared byte-identically across
raw/candidate/admitted/fact/state.

Deliberately NOT promoted here: `test_scenarios_ledger.py`,
`test_stages_post_commit_fold_publish.py`, and
`test_stages_land_pre_pull_apply.py` each carry their OWN small, independent
copies of similarly-shaped helpers (different table shapes / different
pipelines in most cases) — a documented, deliberate choice recorded in
`test_scenarios_ledger.py`'s own module docstring, out of this bead's scope
to change.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from spine.binding import bind_transforms
from spine.config import RunConfig
from spine.context import BatchContext
from spine.core.model import PipelineSpecModel

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

CATALOG_PREFIX = "spine_cat."
EXEMPLAR_DIR = Path(__file__).resolve().parent.parent / "exemplar" / "identity"
FIXTURES_DIR = EXEMPLAR_DIR / "fixtures"

ROW_DDL = (
    "domain_id STRING, event_time STRING, source_ts STRING, content_hash STRING, "
    "payload STRING, batch_id STRING, delivery_id STRING, feed_id STRING, "
    "received_at TIMESTAMP"
)


def bare(qualified_table: str) -> str:
    assert qualified_table.startswith(CATALOG_PREFIX)
    return qualified_table.removeprefix(CATALOG_PREFIX)


def create_raw_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(f"CREATE TABLE {qualified_table} ({ROW_DDL}) USING iceberg")


def create_quarantine_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(
        f"CREATE TABLE {qualified_table} ({ROW_DDL}, reason STRING, check_stage STRING) "
        "USING iceberg"
    )


def create_fact_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(f"CREATE TABLE {qualified_table} ({ROW_DDL}) USING iceberg")


def create_state_table(spark: SparkSession, qualified_table: str) -> None:
    # write.merge.mode=merge-on-read: the fold no-op detection precondition
    # (effects/spark.py's own documented empirical finding) -- the default
    # LWW fold (identity declares no custom `fold`) emits full-shape facts
    # rows, so the state table must mirror the SAME 9-column shape.
    spark.sql(
        f"CREATE TABLE {qualified_table} ({ROW_DDL}) USING iceberg "
        "TBLPROPERTIES ('write.merge.mode'='merge-on-read')"
    )


def make_spec(
    *,
    transforms_module: str,
    raw_table: str,
    quarantine_table: str,
    fact_table: str,
    state_table: str,
) -> PipelineSpecModel:
    return PipelineSpecModel(
        pipeline="pipelines/identity",
        transforms_module=transforms_module,
        raw_table=raw_table,
        quarantine_table=quarantine_table,
        fact_table=fact_table,
        state_table=state_table,
        required_columns=["domain_id"],
    )


def make_seed(
    *, spec: PipelineSpecModel, batch_id: str, object_uris: tuple[str, ...]
) -> BatchContext:
    return BatchContext(
        pipeline="pipelines/identity",
        feed_id="feed/identity",
        delivery_id=str(uuid.UUID(int=1, version=4)),
        batch_id=batch_id,
        delivery_key="statement.csv",
        content_hash="sha256:" + "a" * 64,
        object_uris=object_uris,
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        spec=spec,
        run=RunConfig(),
        transforms=bind_transforms(spec),
        attempt_id="attempt-1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
    )


def batch_id(n: int) -> str:
    return str(uuid.UUID(int=n, version=5))


def quarantine_rows(
    spark: SparkSession, qualified_table: str, batch_id_value: str, check_stage: str
) -> list[dict[str, object]]:
    return [
        row.asDict()
        for row in spark.table(qualified_table)
        .where(f"batch_id = '{batch_id_value}' AND check_stage = '{check_stage}'")
        .collect()
    ]
