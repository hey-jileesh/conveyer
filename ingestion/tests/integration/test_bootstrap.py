"""Integration tests (LLD §12.1) for `bootstrap.create_ledger.bootstrap_ledger`
against a `SqlCatalog` (D-7 -- tests only; the real Glue path is untested
here, §12.5 documented exclusion, see `effects/ledger.py`/`bootstrap/create_ledger.py`
module docstrings).

Covers: the created table's column list, partition spec (`identity(feed_id)`
+ `day(received_at)`), and Athena vacuum table properties (§9.4 -- set
explicitly because Athena VACUUM ignores `history.expire.*` and its own 5 d
default would silently shrink the audit window); and idempotence -- running
bootstrap twice raises nothing and leaves the schema and any already-written
data unchanged.
"""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.bootstrap.create_ledger import bootstrap_ledger
from ingestion.core.model import DeliveryObject, DeliveryRecord
from ingestion.effects.ledger import (
    LEDGER_ICEBERG_SCHEMA,
    LedgerConfig,
    _rows_to_arrow,
    build_catalog,
    schema_to_pyarrow,
)

_GLUE_DATABASE = "conveyer_test_ingestion"
_TABLE_NAME = "delivery_ledger"


def _ledger_config(tmp_path) -> LedgerConfig:
    return LedgerConfig(
        catalog_kind="sql",
        glue_database=_GLUE_DATABASE,
        table_name=_TABLE_NAME,
        warehouse_uri=f"file://{tmp_path}/warehouse",
        sql_uri=f"sqlite:///{tmp_path}/catalog.db",
    )


def test_bootstrap_creates_table_with_expected_schema_partition_and_properties(tmp_path) -> None:
    config = _ledger_config(tmp_path)
    catalog = build_catalog(config)

    table = bootstrap_ledger(catalog, config.glue_database, config.table_name)

    assert [f.name for f in table.schema().fields] == [f.name for f in LEDGER_ICEBERG_SCHEMA.fields]
    partition_field_names = {f.name for f in table.spec().fields}
    assert partition_field_names == {"feed_id", "received_at_day"}
    assert table.properties["vacuum_max_snapshot_age_seconds"] == "2592000"
    assert table.properties["vacuum_min_snapshots_to_keep"] == "5"


def test_bootstrap_is_idempotent_and_preserves_data_across_reruns(tmp_path) -> None:
    config = _ledger_config(tmp_path)
    catalog = build_catalog(config)

    first = bootstrap_ledger(catalog, config.glue_database, config.table_name)

    pa_schema = schema_to_pyarrow(first.schema())
    row = DeliveryRecord(
        delivery_id="11111111-1111-4111-8111-111111111111",
        feed_id="carrier-x/a",
        delivery_key="manifest-1",
        batch_id="batch-1",
        content_hash="sha256:" + "a" * 64,
        size_bytes=1024,
        object_uris=["s3://lake/carrier-x/a/a.csv"],
        objects=[
            DeliveryObject(
                name="a.csv",
                role="data",
                uri="s3://lake/carrier-x/a/a.csv",
                bytes=1024,
                sha256="b" * 64,
            )
        ],
        manifest_ref=None,
        asserted_record_count=10,
        completeness_mode="manifest",
        received_at=datetime(2026, 7, 25, 9, tzinfo=UTC),
        recorded_at=datetime(2026, 7, 25, 9, tzinfo=UTC),
        disposition="registered",
        supersedes=None,
        driver="s3-push",
        driver_run_id="run-1",
        notes=None,
    )
    first.append(_rows_to_arrow([row], pa_schema))

    second = bootstrap_ledger(catalog, config.glue_database, config.table_name)  # rerun, no error

    assert second.schema() == first.schema()
    rows_after = catalog.load_table(f"{config.glue_database}.{config.table_name}").scan().to_arrow()
    assert rows_after.num_rows == 1
    assert rows_after.to_pylist()[0]["delivery_id"] == row.delivery_id


def test_bootstrap_namespace_creation_is_idempotent(tmp_path) -> None:
    config = _ledger_config(tmp_path)
    catalog = build_catalog(config)

    catalog.create_namespace_if_not_exists(config.glue_database)
    catalog.create_namespace_if_not_exists(config.glue_database)  # no error on rerun

    assert (config.glue_database,) in catalog.list_namespaces()
