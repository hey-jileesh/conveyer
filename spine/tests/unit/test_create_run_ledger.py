"""Unit tests for `spine.bootstrap.create_run_ledger` — LLD §6.5, I-17.

`create_run_ledger` (the idempotent `create_table_if_not_exists` call) is
tested directly against a `SqlCatalog` (SQLite + local FS warehouse, D-7 —
tests only), same shape as `ingestion/tests/integration/test_ledger_fx.py`'s
`bootstrap_ledger` coverage. `main()` — the real-AWS-Glue-Catalog entrypoint
— is NOT covered here (documented exclusion, matches ingestion's own
`bootstrap/create_ledger.py::main`, §12.5).
"""

from __future__ import annotations

from dataclasses import dataclass

from spine.bootstrap.create_run_ledger import create_run_ledger
from spine.effects import ledger


@dataclass(frozen=True)
class _FakeConfig:
    ledger_catalog_kind: str
    ledger_sql_uri: str | None
    warehouse_uri: str | None
    aws_region: str = "us-east-1"


def _sql_config(tmp_path) -> _FakeConfig:
    return _FakeConfig(
        ledger_catalog_kind="sql",
        ledger_sql_uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse_uri=f"file://{tmp_path}/warehouse",
    )


def _field_shapes(schema) -> dict[str, tuple[str, bool]]:
    """(type-as-string, required) per field name -- field IDs are reassigned
    by `create_table_if_not_exists` (declaration-order reassignment,
    documented in `effects/ledger.py`'s own module docstring), so comparing
    raw ids against the source `Schema` constant is not meaningful; names/
    types/nullability are what must match."""
    return {field.name: (str(field.field_type), field.required) for field in schema.fields}


def test_create_run_ledger_creates_table_with_exact_schema_and_properties(tmp_path) -> None:
    config = _sql_config(tmp_path)
    catalog = ledger.build_catalog(config)

    table = create_run_ledger(catalog, "spine_db", "run_ledger")

    assert _field_shapes(table.schema()) == _field_shapes(ledger.RUN_LEDGER_SCHEMA)
    assert table.properties == ledger.RUN_LEDGER_TABLE_PROPERTIES
    partition_fields = table.spec().fields
    assert len(partition_fields) == 1
    assert partition_fields[0].name == "started_at_day"


def test_create_run_ledger_is_idempotent_second_call_is_a_noop(tmp_path) -> None:
    config = _sql_config(tmp_path)
    catalog = ledger.build_catalog(config)

    first = create_run_ledger(catalog, "spine_db", "run_ledger")
    second = create_run_ledger(catalog, "spine_db", "run_ledger")

    assert first.location() == second.location()
    assert _field_shapes(second.schema()) == _field_shapes(ledger.RUN_LEDGER_SCHEMA)


def test_create_run_ledger_idempotently_creates_the_namespace(tmp_path) -> None:
    config = _sql_config(tmp_path)
    catalog = ledger.build_catalog(config)
    assert "spine_db" not in [ns[0] for ns in catalog.list_namespaces()]

    create_run_ledger(catalog, "spine_db", "run_ledger")
    create_run_ledger(catalog, "spine_db", "run_ledger")  # namespace already exists: no error

    assert "spine_db" in [ns[0] for ns in catalog.list_namespaces()]
