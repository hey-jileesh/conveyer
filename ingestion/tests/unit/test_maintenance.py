"""Unit tests for `ingestion.maintenance.optimize` -- LLD §9.4.

Athena has no moto coverage (§12.5 documented exclusion, see the module's
own docstring) so every test here drives `AthenaFx` through a plain local
fake (a record of functions, never a mock) rather than a real boto3 `athena`
client. Covers:

* `run_query`'s polling/sequencing: success after intermediate non-terminal
  states, `TransientError` on `FAILED`/`CANCELLED`, `TransientError` on
  timeout (never reaching a terminal state within `max_attempts`) -- the
  brief's three required scenarios.
* `optimize_sql`/`vacuum_sql`/`live_duplicates_sql`'s shape.
* `_row_to_delivery_record`'s parsing of an Athena result row (including the
  positional `objects`/`object_uris` JSON-cast convention and the
  fractional/whole-second timestamp formats) round-trips a `DeliveryRecord`
  built the same way `tests/unit/test_decisions.py` does, via
  `core.decisions._build_row`.
* `live_duplicates_from_rows`'s grouping: multi-registered same-key rows
  grouped, single-registered rows excluded, two DIFFERENT feeds sharing a
  `delivery_key` string kept separate (the cross-feed regression this
  module's grouping key was deliberately designed to prevent).
* `reconcile_supersessions`'s metric emission (EMF line via `capsys`, same
  pattern as `tests/integration/test_ledger_fx.py`) and idempotent append,
  against a real (lightweight, non-moto) `SqlCatalog` ledger -- same
  construction style as `test_ledger_fx.py`, no S3/DynamoDB needed here.
* `run_maintenance`'s full three-step orchestration end to end, Athena
  entirely faked (`get_results` echoes the ledger's own content, proving the
  parse round-trips through the real `plan_reconciliation` + `fx.ledger.append`
  path), including a second, idempotent no-op run.
* `build_athena_fx`'s construction-only smoke test (boto3 `client()` never
  makes a network call, so this is safe without moto or real AWS).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from ingestion import config as config_module
from ingestion.bootstrap.create_ledger import bootstrap_ledger
from ingestion.config import RuntimeConfig
from ingestion.core import decisions
from ingestion.core.model import DeliveryObject, DeliveryRecord
from ingestion.effects.ledger import LedgerConfig, build_catalog, make_ledger_fx
from ingestion.effects.records import Effects, TransientError
from ingestion.maintenance import optimize

_GLUE_DATABASE = "conveyer_test_ingestion"
_LEDGER_TABLE = "delivery_ledger"
NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _row(
    delivery_id: str,
    delivery_key: str,
    received_at: datetime,
    *,
    feed_id: str = "carrier-y/renewal-statements",
    disposition: str = "registered",
    batch_id: str = "b1",
) -> DeliveryRecord:
    return decisions._build_row(
        delivery_id=delivery_id,
        feed_id=feed_id,
        delivery_key=delivery_key,
        received_at=received_at,
        recorded_at=received_at,
        driver="s3-push",
        driver_run_id="run0",
        completeness_mode="manifest",
        asserted_record_count=None,
        disposition=disposition,  # type: ignore[arg-type]
        supersedes=None,
        content_hash="sha256:" + "1" * 64,
        batch_id=batch_id,
        size_bytes=1,
        objects=[],
        object_uris=[],
        manifest_ref=None,
    )


def _runtime_config(
    tmp_path: Path, *, maintenance_tables: tuple[str, ...] | None = None
) -> RuntimeConfig:
    return RuntimeConfig(
        env="test",
        aws_region="us-east-1",
        landing_bucket="landing",
        lake_bucket="lake",
        artifacts_bucket="artifacts",
        glue_database=_GLUE_DATABASE,
        ledger_table=_LEDGER_TABLE,
        cas_table="cas",
        event_bus="bus",
        registry_uri="s3://artifacts/registry/feeds.json",
        athena_workgroup="conveyer-test-workgroup",
        athena_output_uri="s3://artifacts/athena-output/",
        maintenance_tables=maintenance_tables or (f"{_GLUE_DATABASE}.{_LEDGER_TABLE}",),
        feed_id=None,
    )


def _bare_ledger_effects(
    tmp_path: Path, now: datetime, *, maintenance_tables: tuple[str, ...] | None = None
) -> Effects:
    """A real `SqlCatalog` ledger, no moto -- same lightweight construction
    style as `tests/integration/test_ledger_fx.py`. Every other `Effects`
    field is `None`/unused: `optimize.py`'s functions under test here only
    ever touch `fx.ledger`, `fx.now`, and `fx.config`.
    """
    config = _runtime_config(tmp_path, maintenance_tables=maintenance_tables)
    ledger_config = LedgerConfig(
        catalog_kind="sql",
        glue_database=_GLUE_DATABASE,
        table_name=_LEDGER_TABLE,
        warehouse_uri=f"file://{tmp_path}/warehouse",
        sql_uri=f"sqlite:///{tmp_path}/catalog.db",
    )
    bootstrap_ledger(build_catalog(ledger_config), _GLUE_DATABASE, _LEDGER_TABLE)
    return Effects(
        store=None,  # type: ignore[arg-type]
        cas=None,  # type: ignore[arg-type]
        emit=None,  # type: ignore[arg-type]
        sftp_fx_for=None,  # type: ignore[arg-type]
        invoke_async=None,  # type: ignore[arg-type]
        ledger=make_ledger_fx(ledger_config),
        now=lambda: now,
        new_delivery_id=lambda: "unused",
        config=config,
    )


# --- run_query: polling / sequencing (LLD §9.4 "each polled to completion") -


def _fake_athena_for_states(states: list[str]) -> optimize.AthenaFx:
    remaining = iter(states)
    return optimize.AthenaFx(
        start_query=lambda sql: "qid-1",
        poll=lambda qid: next(remaining),
        get_results=lambda qid: [],
    )


def test_run_query_returns_execution_id_once_succeeded() -> None:
    athena = _fake_athena_for_states(["QUEUED", "RUNNING", "SUCCEEDED"])
    sleeps: list[float] = []
    result = optimize.run_query(
        athena, "OPTIMIZE x", sleep_fn=sleeps.append, max_attempts=10, poll_interval_s=3.0
    )
    assert result == "qid-1"
    assert sleeps == [3.0, 3.0]  # one sleep between each non-terminal poll


def test_run_query_raises_transient_error_on_failed() -> None:
    athena = _fake_athena_for_states(["RUNNING", "FAILED"])
    with pytest.raises(TransientError, match="ended in FAILED"):
        optimize.run_query(athena, "VACUUM x", sleep_fn=lambda s: None, max_attempts=10)


def test_run_query_raises_transient_error_on_cancelled() -> None:
    athena = _fake_athena_for_states(["CANCELLED"])
    with pytest.raises(TransientError, match="ended in CANCELLED"):
        optimize.run_query(athena, "VACUUM x", sleep_fn=lambda s: None, max_attempts=10)


def test_run_query_raises_transient_error_on_timeout() -> None:
    athena = optimize.AthenaFx(
        start_query=lambda sql: "qid-timeout",
        poll=lambda qid: "RUNNING",  # never terminal
        get_results=lambda qid: [],
    )
    sleeps: list[float] = []
    with pytest.raises(TransientError, match="timed out after 4 polls"):
        optimize.run_query(athena, "VACUUM x", sleep_fn=sleeps.append, max_attempts=4)
    assert len(sleeps) == 3  # no sleep after the final (timed-out) attempt


# --- SQL builders ------------------------------------------------------------


def test_optimize_sql_targets_given_identifier() -> None:
    assert optimize.optimize_sql(f"{_GLUE_DATABASE}.{_LEDGER_TABLE}") == (
        f"OPTIMIZE {_GLUE_DATABASE}.{_LEDGER_TABLE} REWRITE DATA USING BIN_PACK"
    )


def test_optimize_sql_targets_a_second_non_ledger_identifier() -> None:
    assert optimize.optimize_sql("conveyer_dev_spine.run_ledger") == (
        "OPTIMIZE conveyer_dev_spine.run_ledger REWRITE DATA USING BIN_PACK"
    )


def test_vacuum_sql_targets_given_identifier() -> None:
    assert optimize.vacuum_sql(f"{_GLUE_DATABASE}.{_LEDGER_TABLE}") == (
        f"VACUUM {_GLUE_DATABASE}.{_LEDGER_TABLE}"
    )


def test_vacuum_sql_targets_a_second_non_ledger_identifier() -> None:
    assert optimize.vacuum_sql("conveyer_dev_spine.run_ledger") == (
        "VACUUM conveyer_dev_spine.run_ledger"
    )


def test_live_duplicates_sql_shape(tmp_path: Path) -> None:
    config = _runtime_config(tmp_path)
    sql = optimize.live_duplicates_sql(config)
    assert f"FROM {_GLUE_DATABASE}.{_LEDGER_TABLE}" in sql
    assert "ROW_NUMBER() OVER (PARTITION BY delivery_id ORDER BY recorded_at DESC)" in sql
    assert "disposition = 'registered'" in sql
    assert "HAVING COUNT(*) > 1" in sql
    assert "CAST(cr.objects AS JSON)" in sql
    assert "CAST(cr.object_uris AS JSON)" in sql


# --- Athena result-row -> DeliveryRecord parsing ----------------------------


def _to_athena_row(record: DeliveryRecord) -> dict[str, str | None]:
    """The inverse of `optimize._row_to_delivery_record` -- builds the
    Athena `GetQueryResults`-shaped row (every value a string, or `None` for
    NULL) a real query would have produced for `record`, per
    `live_duplicates_sql`'s documented `CAST(... AS JSON)` convention.
    """
    return {
        "delivery_id": record.delivery_id,
        "feed_id": record.feed_id,
        "delivery_key": record.delivery_key,
        "batch_id": record.batch_id,
        "content_hash": record.content_hash,
        "size_bytes": None if record.size_bytes is None else str(record.size_bytes),
        "object_uris_json": json.dumps(record.object_uris),
        "objects_json": json.dumps(
            [[o.name, o.role, o.uri, o.bytes, o.sha256] for o in record.objects]
        ),
        "manifest_ref": record.manifest_ref,
        "asserted_record_count": (
            None if record.asserted_record_count is None else str(record.asserted_record_count)
        ),
        "completeness_mode": record.completeness_mode,
        "received_at": record.received_at.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "recorded_at": record.recorded_at.strftime("%Y-%m-%d %H:%M:%S.%f"),
        "disposition": record.disposition,
        "supersedes": record.supersedes,
        "driver": record.driver,
        "driver_run_id": record.driver_run_id,
        "notes": record.notes,
    }


def test_row_to_delivery_record_round_trips_populated_row() -> None:
    record = decisions._build_row(
        delivery_id="d-1",
        feed_id="carrier-y/renewal-statements",
        delivery_key="k1",
        received_at=NOW,
        recorded_at=NOW,
        driver="s3-push",
        driver_run_id="run0",
        completeness_mode="manifest",
        asserted_record_count=3,
        disposition="registered",
        supersedes="d-0",
        content_hash="sha256:" + "1" * 64,
        batch_id="b1",
        size_bytes=100,
        objects=[
            DeliveryObject(
                name="part1.csv", role="data", uri="s3://lake/part1.csv", bytes=100, sha256="a" * 64
            )
        ],
        object_uris=["s3://lake/part1.csv"],
        manifest_ref="s3://lake/manifest.json",
    )
    row = _to_athena_row(record)
    assert optimize._row_to_delivery_record(row) == record


def test_row_to_delivery_record_handles_null_optional_fields_and_whole_second_timestamp() -> None:
    record = _row("d-1", "k1", NOW)
    row = _to_athena_row(record)
    row["received_at"] = "2026-01-01 12:00:00"  # no fractional part, unlike strftime's output
    row["recorded_at"] = "2026-01-01 12:00:00"
    parsed = optimize._row_to_delivery_record(row)
    assert parsed.batch_id == "b1"
    assert parsed.manifest_ref is None
    assert parsed.asserted_record_count is None
    assert parsed.supersedes is None
    assert parsed.notes is None
    assert parsed.received_at == NOW
    assert parsed.recorded_at == NOW


# --- live_duplicates_from_rows -----------------------------------------------


def test_live_duplicates_from_rows_groups_multi_registered_same_key() -> None:
    old = _row("d-old", "k1", NOW - timedelta(days=1))
    new = _row("d-new", "k1", NOW)
    grouped = optimize.live_duplicates_from_rows([old, new])
    assert len(grouped) == 1
    (records,) = grouped.values()
    assert {r.delivery_id for r in records} == {"d-old", "d-new"}


def test_live_duplicates_from_rows_excludes_single_registered_delivery() -> None:
    only = _row("d-1", "k1", NOW)
    assert optimize.live_duplicates_from_rows([only]) == {}


def test_live_duplicates_from_rows_keeps_different_feeds_separate_for_same_key() -> None:
    feed_a = _row("d-a", "shared-key", NOW - timedelta(days=1), feed_id="carrier-x/a")
    feed_b = _row("d-b", "shared-key", NOW, feed_id="carrier-y/b")
    grouped = optimize.live_duplicates_from_rows([feed_a, feed_b])
    # each feed has exactly ONE registered delivery under "shared-key" --
    # neither should be reported as a live duplicate of the other.
    assert grouped == {}


# --- reconcile_supersessions: real (non-moto) ledger, metric emission -------


def test_reconcile_supersessions_appends_accretion_row_and_emits_metric(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    fx = _bare_ledger_effects(tmp_path, NOW + timedelta(hours=1))
    old = _row("d-old", "k1", NOW - timedelta(days=1))
    new = _row("d-new", "k1", NOW)
    fx.ledger.append([old, new])

    live_duplicates = optimize.live_duplicates_from_rows(
        fx.ledger.scan_feed("carrier-y/renewal-statements", None)
    )
    result = optimize.reconcile_supersessions(fx, live_duplicates, fx.now())

    assert [r.delivery_id for r in result] == ["d-old"]
    assert result[0].disposition == "superseded"
    metric_lines = [
        line for line in capsys.readouterr().out.splitlines() if "SupersessionsReconciled" in line
    ]
    assert len(metric_lines) == 1
    payload = json.loads(metric_lines[0])
    assert payload["SupersessionsReconciled"] == 1
    assert payload["feed_id"] == "carrier-y/renewal-statements"

    all_rows = fx.ledger.scan_feed("carrier-y/renewal-statements", None)
    assert len(all_rows) == 3  # 2 seeded + 1 accretion row


def test_reconcile_supersessions_second_call_with_unchanged_input_appends_nothing(
    tmp_path: Path,
) -> None:
    fx = _bare_ledger_effects(tmp_path, NOW + timedelta(hours=1))
    old = _row("d-old", "k1", NOW - timedelta(days=1))
    new = _row("d-new", "k1", NOW)
    fx.ledger.append([old, new])

    live_duplicates = optimize.live_duplicates_from_rows(
        fx.ledger.scan_feed("carrier-y/renewal-statements", None)
    )
    optimize.reconcile_supersessions(fx, live_duplicates, fx.now())

    # re-fold from the ledger's now-current content -- the reconciled
    # delivery_key no longer has more than one live registered row.
    live_duplicates_2 = optimize.live_duplicates_from_rows(
        fx.ledger.scan_feed("carrier-y/renewal-statements", None)
    )
    result2 = optimize.reconcile_supersessions(fx, live_duplicates_2, fx.now())
    assert result2 == ()
    assert len(fx.ledger.scan_feed("carrier-y/renewal-statements", None)) == 3


# --- run_maintenance: full three-step orchestration, Athena faked ----------


def _fake_athena_echoing_ledger(
    scan_feed: Callable[[str, datetime | None], list[DeliveryRecord]],
    feed_id: str,
    *,
    recorded_sql: list[str] | None = None,
) -> optimize.AthenaFx:
    """Every step SUCCEEDS immediately; the reconciliation query's results
    are computed FROM the same local ledger `run_maintenance` will append
    to, round-tripped through the real Athena row shape -- proves
    `run_maintenance` end to end without ever needing a real Athena.
    `recorded_sql`, if given, accumulates every `start_query` SQL string in
    call order -- lets a test assert the OPTIMIZE/VACUUM loop hit every
    configured table identifier, not just the ledger.
    """

    def start_query(sql: str) -> str:
        if recorded_sql is not None:
            recorded_sql.append(sql)
        return "qid"

    def get_results(query_execution_id: str) -> list[dict[str, str | None]]:
        grouped = optimize.live_duplicates_from_rows(scan_feed(feed_id, None))
        flat = [record for records in grouped.values() for record in records]
        return [_to_athena_row(record) for record in flat]

    return optimize.AthenaFx(
        start_query=start_query,
        poll=lambda query_execution_id: "SUCCEEDED",
        get_results=get_results,
    )


def test_run_maintenance_full_orchestration_is_idempotent_on_second_run(tmp_path: Path) -> None:
    fx = _bare_ledger_effects(tmp_path, NOW + timedelta(hours=1))
    old = _row("d-old", "k1", NOW - timedelta(days=1))
    new = _row("d-new", "k1", NOW)
    fx.ledger.append([old, new])

    athena = _fake_athena_echoing_ledger(fx.ledger.scan_feed, "carrier-y/renewal-statements")
    sleeps: list[float] = []
    first = optimize.run_maintenance(fx, athena, sleep_fn=sleeps.append)
    assert [r.delivery_id for r in first] == ["d-old"]
    assert sleeps == []  # every poll SUCCEEDED on the first attempt

    second = optimize.run_maintenance(fx, athena, sleep_fn=sleeps.append)
    assert second == ()
    assert len(fx.ledger.scan_feed("carrier-y/renewal-statements", None)) == 3


def test_run_maintenance_loops_optimize_and_vacuum_over_every_configured_table(
    tmp_path: Path,
) -> None:
    """LLD 004.1 §12.6(3)/I-17: `CONVEYER_MAINTENANCE_TABLES`-driven
    multi-table loop. OPTIMIZE+VACUUM must run once per configured table
    identifier (here: the ingestion ledger PLUS a stand-in spine run-ledger
    identifier) -- but the supersession reconciliation query stays
    ledger-only, never looped.
    """
    spine_identifier = "conveyer_dev_spine.run_ledger"
    ledger_identifier = f"{_GLUE_DATABASE}.{_LEDGER_TABLE}"
    fx = _bare_ledger_effects(
        tmp_path,
        NOW + timedelta(hours=1),
        maintenance_tables=(ledger_identifier, spine_identifier),
    )
    old = _row("d-old", "k1", NOW - timedelta(days=1))
    new = _row("d-new", "k1", NOW)
    fx.ledger.append([old, new])

    recorded_sql: list[str] = []
    athena = _fake_athena_echoing_ledger(
        fx.ledger.scan_feed, "carrier-y/renewal-statements", recorded_sql=recorded_sql
    )
    result = optimize.run_maintenance(fx, athena)

    assert [r.delivery_id for r in result] == ["d-old"]
    assert recorded_sql == [
        optimize.optimize_sql(ledger_identifier),
        optimize.vacuum_sql(ledger_identifier),
        optimize.optimize_sql(spine_identifier),
        optimize.vacuum_sql(spine_identifier),
        optimize.live_duplicates_sql(fx.config),  # reconciliation: ledger-only, runs once
    ]


def test_run_maintenance_defaults_to_the_single_ledger_table_when_unconfigured(
    tmp_path: Path,
) -> None:
    """Confirms EXISTING BEHAVIOR IS UNCHANGED: a `RuntimeConfig` built the
    same way `from_env`'s default (`CONVEYER_MAINTENANCE_TABLES` unset)
    resolves -- `maintenance_tables` carrying exactly one entry -- makes
    `run_maintenance` OPTIMIZE+VACUUM the ledger once, same as before this
    change.
    """
    ledger_identifier = f"{_GLUE_DATABASE}.{_LEDGER_TABLE}"
    fx = _bare_ledger_effects(tmp_path, NOW + timedelta(hours=1))
    assert fx.config.maintenance_tables == (ledger_identifier,)

    recorded_sql: list[str] = []
    athena = _fake_athena_echoing_ledger(
        fx.ledger.scan_feed, "carrier-y/renewal-statements", recorded_sql=recorded_sql
    )
    optimize.run_maintenance(fx, athena)

    assert recorded_sql == [
        optimize.optimize_sql(ledger_identifier),
        optimize.vacuum_sql(ledger_identifier),
        optimize.live_duplicates_sql(fx.config),
    ]


# --- config: CONVEYER_MAINTENANCE_TABLES parsing (LLD 004.1 §12.6(3)) -------


def test_parse_maintenance_tables_defaults_to_single_ledger_identifier_when_unset() -> None:
    assert config_module._parse_maintenance_tables(
        {}, glue_database="db", ledger_table="ledger"
    ) == ("db.ledger",)


def test_parse_maintenance_tables_defaults_when_env_var_is_blank() -> None:
    assert config_module._parse_maintenance_tables(
        {"CONVEYER_MAINTENANCE_TABLES": ""}, glue_database="db", ledger_table="ledger"
    ) == ("db.ledger",)


def test_parse_maintenance_tables_splits_comma_list() -> None:
    assert config_module._parse_maintenance_tables(
        {"CONVEYER_MAINTENANCE_TABLES": "db.ledger,spine_db.run_ledger"},
        glue_database="db",
        ledger_table="ledger",
    ) == ("db.ledger", "spine_db.run_ledger")


def test_parse_maintenance_tables_trims_whitespace_and_drops_blank_entries() -> None:
    assert config_module._parse_maintenance_tables(
        {"CONVEYER_MAINTENANCE_TABLES": " db.ledger , spine_db.run_ledger ,, "},
        glue_database="db",
        ledger_table="ledger",
    ) == ("db.ledger", "spine_db.run_ledger")


def test_from_env_maintenance_tables_defaults_to_single_ledger_identifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CONVEYER_ENV", "test")
    monkeypatch.setenv("CONVEYER_AWS_REGION", "us-east-1")
    monkeypatch.setenv("CONVEYER_LANDING_BUCKET", "landing")
    monkeypatch.setenv("CONVEYER_LAKE_BUCKET", "lake")
    monkeypatch.setenv("CONVEYER_ARTIFACTS_BUCKET", "artifacts")
    monkeypatch.setenv("CONVEYER_GLUE_DATABASE", _GLUE_DATABASE)
    monkeypatch.setenv("CONVEYER_LEDGER_TABLE", _LEDGER_TABLE)
    monkeypatch.setenv("CONVEYER_CAS_TABLE", "cas")
    monkeypatch.setenv("CONVEYER_EVENT_BUS", "bus")
    monkeypatch.setenv("CONVEYER_REGISTRY_URI", "s3://artifacts/registry/feeds.json")
    monkeypatch.setenv("CONVEYER_ATHENA_WORKGROUP", "wg")
    monkeypatch.setenv("CONVEYER_ATHENA_OUTPUT_URI", "s3://artifacts/athena-output/")
    monkeypatch.delenv("CONVEYER_MAINTENANCE_TABLES", raising=False)
    monkeypatch.delenv("CONVEYER_FEED_ID", raising=False)

    resolved = config_module.from_env()
    assert resolved.maintenance_tables == (f"{_GLUE_DATABASE}.{_LEDGER_TABLE}",)


def test_from_env_maintenance_tables_reads_the_configured_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CONVEYER_ENV", "test")
    monkeypatch.setenv("CONVEYER_AWS_REGION", "us-east-1")
    monkeypatch.setenv("CONVEYER_LANDING_BUCKET", "landing")
    monkeypatch.setenv("CONVEYER_LAKE_BUCKET", "lake")
    monkeypatch.setenv("CONVEYER_ARTIFACTS_BUCKET", "artifacts")
    monkeypatch.setenv("CONVEYER_GLUE_DATABASE", _GLUE_DATABASE)
    monkeypatch.setenv("CONVEYER_LEDGER_TABLE", _LEDGER_TABLE)
    monkeypatch.setenv("CONVEYER_CAS_TABLE", "cas")
    monkeypatch.setenv("CONVEYER_EVENT_BUS", "bus")
    monkeypatch.setenv("CONVEYER_REGISTRY_URI", "s3://artifacts/registry/feeds.json")
    monkeypatch.setenv("CONVEYER_ATHENA_WORKGROUP", "wg")
    monkeypatch.setenv("CONVEYER_ATHENA_OUTPUT_URI", "s3://artifacts/athena-output/")
    monkeypatch.setenv(
        "CONVEYER_MAINTENANCE_TABLES",
        f"{_GLUE_DATABASE}.{_LEDGER_TABLE},conveyer_dev_spine.run_ledger",
    )
    monkeypatch.delenv("CONVEYER_FEED_ID", raising=False)

    resolved = config_module.from_env()
    assert resolved.maintenance_tables == (
        f"{_GLUE_DATABASE}.{_LEDGER_TABLE}",
        "conveyer_dev_spine.run_ledger",
    )


# --- build_athena_fx: construction-only smoke test --------------------------


def test_build_athena_fx_constructs_without_a_network_call(tmp_path: Path) -> None:
    # boto3 `client()` performs no network I/O at construction time, so this
    # is safe to run without moto or real AWS credentials (§12.5 exclusion
    # applies to actually CALLING the client, not building it).
    config = _runtime_config(tmp_path)
    athena = optimize.build_athena_fx(config)
    assert callable(athena.start_query)
    assert callable(athena.poll)
    assert callable(athena.get_results)
