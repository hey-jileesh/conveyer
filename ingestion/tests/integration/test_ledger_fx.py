"""Integration tests (factory level, LLD §12.1) for `effects.ledger.make_ledger_fx`
against a `SqlCatalog` (SQLite + local FS warehouse, D-7 -- tests only).

Covers: append/scan round-trip (incl. exact `DeliveryRecord` equality
through the pyarrow conversion), `scan_feed`'s `feed_id` + `received_at >=
since` filtering, `append([])` as a no-op, and the `CommitFailedException`
retry path (§7.7) -- both "conflict then succeeds" and "exhausts all 5
attempts -> TransientError". The retry tests patch `ledger.time` (this
module's own `import time` binding, not the global `time` module) via
pytest's `monkeypatch` fixture to record/skip the real jittered sleep --
`monkeypatch` is a plain-attribute-swap fixture, not `unittest.mock`
(purity-linter-banned only for the latter, §12.2).
"""

from __future__ import annotations

import json
import types
from datetime import UTC, datetime

import pytest
from ingestion.bootstrap.create_ledger import bootstrap_ledger
from ingestion.core.model import DeliveryObject, DeliveryRecord
from ingestion.effects.ledger import LedgerConfig, _rows_to_arrow, build_catalog, make_ledger_fx
from ingestion.effects.records import TransientError

_GLUE_DATABASE = "conveyer_test_ingestion"
_TABLE_NAME = "delivery_ledger"


def _record(delivery_id: str, feed_id: str, received_at: datetime) -> DeliveryRecord:
    return DeliveryRecord(
        delivery_id=delivery_id,
        feed_id=feed_id,
        delivery_key="manifest-1",
        batch_id="batch-" + delivery_id,
        content_hash="sha256:" + "a" * 64,
        size_bytes=1024,
        object_uris=[f"s3://lake/{feed_id}/a.csv"],
        objects=[
            DeliveryObject(
                name="a.csv",
                role="data",
                uri=f"s3://lake/{feed_id}/a.csv",
                bytes=1024,
                sha256="b" * 64,
            )
        ],
        manifest_ref=None,
        asserted_record_count=10,
        completeness_mode="manifest",
        received_at=received_at,
        recorded_at=received_at,
        disposition="registered",
        supersedes=None,
        driver="s3-push",
        driver_run_id="run-1",
        notes=None,
    )


@pytest.fixture
def ledger_config(tmp_path) -> LedgerConfig:
    config = LedgerConfig(
        catalog_kind="sql",
        glue_database=_GLUE_DATABASE,
        table_name=_TABLE_NAME,
        warehouse_uri=f"file://{tmp_path}/warehouse",
        sql_uri=f"sqlite:///{tmp_path}/catalog.db",
    )
    bootstrap_ledger(build_catalog(config), config.glue_database, config.table_name)
    return config


def test_append_and_scan_feed_round_trips_exact_records(ledger_config: LedgerConfig) -> None:
    fx = make_ledger_fx(ledger_config)
    r1 = _record(
        "11111111-1111-4111-8111-111111111111", "carrier-x/a", datetime(2026, 7, 25, 9, tzinfo=UTC)
    )
    r2 = _record(
        "22222222-2222-4222-8222-222222222222", "carrier-x/a", datetime(2026, 7, 25, 10, tzinfo=UTC)
    )
    r3 = _record(
        "33333333-3333-4333-8333-333333333333", "carrier-y/b", datetime(2026, 7, 25, 9, tzinfo=UTC)
    )

    fx.append([r1, r2, r3])

    rows_a = fx.scan_feed("carrier-x/a", None)
    assert sorted(rows_a, key=lambda r: r.delivery_id) == sorted(
        [r1, r2], key=lambda r: r.delivery_id
    )
    rows_b = fx.scan_feed("carrier-y/b", None)
    assert rows_b == [r3]


def test_scan_feed_since_filters_on_received_at(ledger_config: LedgerConfig) -> None:
    fx = make_ledger_fx(ledger_config)
    early = _record(
        "11111111-1111-4111-8111-111111111111", "carrier-x/a", datetime(2026, 7, 25, 9, tzinfo=UTC)
    )
    late = _record(
        "22222222-2222-4222-8222-222222222222", "carrier-x/a", datetime(2026, 7, 25, 12, tzinfo=UTC)
    )
    fx.append([early, late])

    since = fx.scan_feed("carrier-x/a", datetime(2026, 7, 25, 10, tzinfo=UTC))

    assert since == [late]


def test_append_empty_rows_is_a_noop(ledger_config: LedgerConfig) -> None:
    fx = make_ledger_fx(ledger_config)

    fx.append([])

    assert fx.scan_feed("carrier-x/a", None) == []


def test_append_retries_on_commit_conflict_then_succeeds(
    ledger_config: LedgerConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ingestion.effects.ledger as ledger_mod

    catalog = build_catalog(ledger_config)
    identifier = f"{ledger_config.glue_database}.{ledger_config.table_name}"
    pa_schema = ledger_mod.schema_to_pyarrow(catalog.load_table(identifier).schema())

    # Capture a stale handle, then commit through a DIFFERENT handle so the
    # captured one is now behind the catalog's current snapshot pointer.
    stale = catalog.load_table(identifier)
    concurrent_writer = catalog.load_table(identifier)
    concurrent_row = _record(
        "44444444-4444-4444-8444-444444444444", "carrier-x/a", datetime(2026, 7, 25, 9, tzinfo=UTC)
    )
    concurrent_writer.append(_rows_to_arrow([concurrent_row], pa_schema))

    calls = {"n": 0}

    def load_table_returning_stale_once(ident: str):
        calls["n"] += 1
        return stale if calls["n"] == 1 else catalog.load_table(ident)

    fake_catalog = types.SimpleNamespace(load_table=load_table_returning_stale_once)
    sleeps: list[float] = []
    monkeypatch.setattr(ledger_mod, "time", types.SimpleNamespace(sleep=sleeps.append))

    retried_row = _record(
        "55555555-5555-4555-8555-555555555555", "carrier-x/a", datetime(2026, 7, 25, 9, tzinfo=UTC)
    )
    ledger_mod._append(fake_catalog, identifier, pa_schema, [retried_row])

    assert calls["n"] == 2  # one failed attempt, one successful reload+retry
    assert len(sleeps) == 1
    assert 0.5 <= sleeps[0] <= 8.0
    delivery_ids = {
        row["delivery_id"] for row in catalog.load_table(identifier).scan().to_arrow().to_pylist()
    }
    assert delivery_ids == {concurrent_row.delivery_id, retried_row.delivery_id}


def test_append_exhausts_retries_then_raises_transient_error(
    ledger_config: LedgerConfig, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import ingestion.effects.ledger as ledger_mod

    catalog = build_catalog(ledger_config)
    identifier = f"{ledger_config.glue_database}.{ledger_config.table_name}"
    pa_schema = ledger_mod.schema_to_pyarrow(catalog.load_table(identifier).schema())

    always_stale = catalog.load_table(identifier)
    concurrent_writer = catalog.load_table(identifier)
    concurrent_writer.append(
        _rows_to_arrow(
            [
                _record(
                    "66666666-6666-4666-8666-666666666666",
                    "carrier-x/a",
                    datetime(2026, 7, 25, 9, tzinfo=UTC),
                )
            ],
            pa_schema,
        )
    )

    calls = {"n": 0}

    def always_stale_load_table(ident: str):
        calls["n"] += 1
        return always_stale

    fake_catalog = types.SimpleNamespace(load_table=always_stale_load_table)
    sleeps: list[float] = []
    monkeypatch.setattr(ledger_mod, "time", types.SimpleNamespace(sleep=sleeps.append))

    row = _record(
        "77777777-7777-4777-8777-777777777777", "carrier-x/a", datetime(2026, 7, 25, 9, tzinfo=UTC)
    )
    with pytest.raises(TransientError, match="failed after 5 attempts"):
        ledger_mod._append(fake_catalog, identifier, pa_schema, [row])

    assert calls["n"] == 5
    assert len(sleeps) == 4  # backoff between attempts, none after the final failure
    metric_lines = [
        line for line in capsys.readouterr().out.splitlines() if "LedgerCommitRetries" in line
    ]
    assert len(metric_lines) == 5  # emitted once per failed attempt

    # F-3: the metric is emitted via `observability.emit_metric` (not a
    # private hand-rolled EMF print) -- assert the EXACT shape that
    # function produces, including the `_aws.Timestamp` field and the
    # `feed_id` dimension `effects/ledger.py::_append` derives from `rows`.
    for line in metric_lines:
        payload = json.loads(line)
        assert "Timestamp" in payload["_aws"]
        metrics_block = payload["_aws"]["CloudWatchMetrics"][0]
        assert metrics_block["Namespace"] == "Conveyer/Ingestion"
        assert metrics_block["Dimensions"] == [["feed_id"]]
        assert metrics_block["Metrics"] == [{"Name": "LedgerCommitRetries", "Unit": "Count"}]
        assert payload["feed_id"] == row.feed_id
        assert payload["LedgerCommitRetries"] == 1
