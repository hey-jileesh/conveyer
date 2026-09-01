"""`glue_main.main(argv)` end-to-end — LLD §8.3, I-14, I-22, I-23, [H-5][T-16].

Drives the REAL entrypoint (unlike `test_scenarios_core.py`, which builds
`BatchContext` directly and bypasses the entrypoint entirely) through every
§8.3 step — argv parse, seed parse + `check_object_uris` (I-22), spec-URI
allowlist (I-23) + fetch + parse, binding-defect asserts ([H-5]), `bind_
transforms` (I-10), real local `SparkSession` build + Iceberg-extensions
assert ([T-16]), `RunnerFx` assembly, seed `BatchContext`, `spine.run.run` —
against the identity exemplar fixtures, over the shared session-scoped
`spark` fixture (real local Iceberg, `tests/conftest.py`, §12.1).

Two of `main()`'s own DI seams are exercised deliberately (see `glue_main`'s
module docstring for the full rationale):

* `fetch_spec` — `_make_fetch_spec`'s in-memory dispatcher, serving BOTH
  artifacts real `main()` fetches through this one seam (per-test `unique_
  table` names, no real S3 round-trip needed): the spec YAML itself, and
  (F-10) the sibling `table-classes.json` `naming.table_class_inventory_uri`
  derives from the same spec URI. The URI itself is still a genuine
  `s3://.../spine/specs/<slug>/` string and still passes `check_spec_uri_
  allowlist` for real.
* `fx_factory` — the REAL `make_runner_fx(spark, config)` production
  assembly, with ONLY `read_objects` wrapped to translate the (real,
  allowlisted) `s3://` object URIs to the real local exemplar fixture files
  by basename — the local substrate has no S3-compatible filesystem wired
  into Spark (no `hadoop-aws`), the same reason `test_scenarios_core.py`
  bypasses this entrypoint outright. Every other `RunnerFx` field (`append`,
  `merge`, `record_run` against the bootstrapped SQLite ledger, `emit`
  against the moto-backed bus, ...) is the untouched production closure.

Also covers (conveyer-nvh.47): `main()` installs `observability.
install_json_handler()` on the real root logger, idempotently across a
warm-process repeat invocation (`_clean_root_logger` mirrors ingestion's own
entrypoint tests' fixture of the same name, for the same
snapshot-and-restore reason).
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import yaml
from scenario_helpers import FIXTURES_DIR as _FIXTURES_DIR
from scenario_helpers import bare as _bare
from scenario_helpers import batch_id as _batch_id
from scenario_helpers import create_fact_table as _create_fact_table
from scenario_helpers import create_quarantine_table as _create_quarantine_table
from scenario_helpers import create_raw_table as _create_raw_table
from scenario_helpers import create_state_table as _create_state_table
from scenario_helpers import facts_appended_total as _facts_appended_total
from scenario_helpers import unique_pipeline as _unique_pipeline
from spine.bootstrap.create_record_tables import bootstrap_markers_table
from spine.core import naming
from spine.effects.build import make_runner_fx
from spine.entrypoints import glue_main

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyspark.sql import DataFrame, SparkSession
    from spine.effects.records import RunnerFx
    from tests.conftest import LedgerCatalogFixture, MotoEventsBus

_LANDING_BUCKET = "conveyer-test-landing"
_FEED_ID = "feed/identity"
_RECEIVED_AT = datetime(2026, 1, 1, tzinfo=UTC)


def _delivery_id(n: int) -> str:
    return str(uuid.UUID(int=n, version=4))


def _create_markers_table(spark: SparkSession, raw_table: str, pipeline: str) -> None:
    """B10 (bead conveyer-6pg.22): every scenario driving the real entrypoint
    all the way through `commit` needs a real markers table -- `glue_main.
    main`'s own bind-time `_committed_tables` read tolerates an absent table
    ([DC-1]'s own docstring), but `stages/commit.py`'s later marker WRITES do
    not. Mirrors `scenario_helpers.create_markers_table_for`, built from the
    raw YAML-dict `pipeline`/`raw_table` values this file authors directly
    (no `PipelineSpecModel` object exists yet at this point in these tests)."""
    bootstrap_markers_table(spark, naming.qualified(naming.markers_table(raw_table, pipeline)))


def _canonical_object_uri(delivery_id: str, name: str) -> str:
    # Mirrors `core/naming.py::_canonical_prefix`/`_format_received_at` for
    # `_RECEIVED_AT` (UTC, microsecond precision, dash-free ISO8601).
    return (
        f"s3://{_LANDING_BUCKET}/{_FEED_ID}/received_at=20260101T000000000000Z/"
        f"dl-{delivery_id}/{name}"
    )


def _argv(
    *,
    ledger_catalog: LedgerCatalogFixture,
    event_bus: str,
    delivery_json: str,
    pipeline_spec_uri: str,
) -> list[str]:
    return [
        "--conveyer-env", "test",
        "--conveyer-aws-region", "us-east-1",
        "--conveyer-catalog-kind", "hadoop",
        "--conveyer-warehouse-uri", ledger_catalog.config.warehouse_uri or "",
        "--conveyer-ledger-catalog-kind", "sql",
        "--conveyer-ledger-sql-uri", ledger_catalog.config.ledger_sql_uri or "",
        "--conveyer-spine-db", ledger_catalog.config.spine_db,
        "--conveyer-run-ledger-table", ledger_catalog.config.run_ledger_table,
        "--conveyer-event-bus", event_bus,
        "--conveyer-landing-bucket", _LANDING_BUCKET,
        "--conveyer-pipeline-spec-uri", pipeline_spec_uri,
        "--conveyer-delivery", delivery_json,
        "--conveyer-sfn-retry-count", "0",
        "--conveyer-sfn-redrive-count", "0",
        "--conveyer-run-config", "{}",
        "--conveyer-sla-minutes", "480",
        "--JOB_RUN_ID", "jr_entrypoint_test",
    ]  # fmt: skip


def _make_fx_factory(
    fixtures_subdir: str,
) -> Callable[[SparkSession, object], RunnerFx]:
    """Real `make_runner_fx`, `read_objects` wrapped to translate the (real,
    I-22-allowlisted) `s3://` object URIs to real local exemplar fixture
    files by basename -- see this module's own docstring."""
    fixtures_dir = _FIXTURES_DIR / fixtures_subdir

    def fx_factory(spark: SparkSession, config: object) -> RunnerFx:
        fx = make_runner_fx(spark, config)  # type: ignore[arg-type]
        real_read_objects = fx.read_objects

        def wrapped_read_objects(
            uris: tuple[str, ...], read: object, raw_contract: object
        ) -> DataFrame:
            real_paths = tuple(str(fixtures_dir / Path(u).name) for u in uris)
            return real_read_objects(real_paths, read, raw_contract)  # type: ignore[arg-type]

        return replace(fx, read_objects=wrapped_read_objects)

    return fx_factory


def _make_fetch_spec(
    *,
    spec_uri: str,
    spec_text: str,
    raw_table: str,
    quarantine_table: str,
    fact_table: str,
    state_table: str,
    markers_table: str,
) -> Callable[[str], str]:
    """Real `main()` fetches TWO artifacts through this one DI seam: the
    spec itself, and (F-10, `_load_table_class_inventory`) the sibling
    `table-classes.json` `naming.table_class_inventory_uri` derives from
    the SAME spec URI `check_spec_uri_allowlist` already validated -- both
    served from one in-memory lambda, dispatched by URI, mirroring what
    `bootstrap/create_record_tables.py::main` actually emits beside a real
    deployed spec (raw/quarantine/facts/state/marker, F-10's own class
    enumeration)."""
    inventory_uri = naming.table_class_inventory_uri(spec_uri)
    inventory = json.dumps(
        {
            raw_table: "raw",
            quarantine_table: "quarantine",
            fact_table: "facts",
            state_table: "state",
            markers_table: "marker",
        }
    )

    def fetch_spec(uri: str) -> str:
        if uri == inventory_uri:
            return inventory
        return spec_text

    return fetch_spec


def test_main_end_to_end_identity_clean_batch_matches_goldens_and_emits_both_events(
    spark: SparkSession,
    unique_table: Callable[[str], str],
    ledger_catalog: LedgerCatalogFixture,
    moto_events_bus: MotoEventsBus,
) -> None:
    raw_qt = unique_table("entrypoint_raw")
    qtn_qt = unique_table("entrypoint_qtn")
    fact_qt = unique_table("entrypoint_fact")
    state_qt = unique_table("entrypoint_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    pipeline = _unique_pipeline("ep1")
    _create_markers_table(spark, _bare(raw_qt), pipeline)

    spec_text = yaml.safe_dump(
        {
            "pipeline": pipeline,
            "transforms_module": "pipelines.identity.transforms",
            "raw_table": _bare(raw_qt),
            "quarantine_table": _bare(qtn_qt),
            "fact_types": {
                "identity": {
                    "fact_table": _bare(fact_qt),
                    "state_table": _bare(state_qt),
                    "schema": {
                        "columns": [
                            {"name": "domain_id", "type": "string"},
                            {"name": "event_time", "type": "string"},
                            {"name": "payload", "type": "string"},
                        ],
                        "domain_id_col": "domain_id",
                        "record_key": ["domain_id"],
                    },
                }
            },
            "read": {"dialect": {"format": "csv", "header": True}},
            "raw_contract": {
                "columns": [
                    {"name": "domain_id", "required": True, "nullable": False},
                    {"name": "event_time"},
                    {"name": "source_ts"},
                    {"name": "content_hash"},
                    {"name": "payload"},
                ]
            },
            "sla_minutes": 480,
        }
    )
    spec_uri = f"s3://some-artifacts-bucket/spine/specs/{naming.slug(pipeline)}/pipeline.yaml"

    batch_id = _batch_id(9001)
    delivery_id = _delivery_id(9001)
    delivery = {
        "schema_version": 1,
        "feed_id": _FEED_ID,
        "delivery_id": delivery_id,
        "batch_id": batch_id,
        "delivery_key": "statement.csv",
        "content_hash": "sha256:" + "a" * 64,
        "size_bytes": 10,
        "object_uris": [
            _canonical_object_uri(delivery_id, "object_1.csv"),
            _canonical_object_uri(delivery_id, "object_2.csv"),
        ],
        "received_at": _RECEIVED_AT.isoformat(),
        "pipeline": pipeline,
    }

    argv = _argv(
        ledger_catalog=ledger_catalog,
        event_bus=moto_events_bus.bus_name,
        delivery_json=json.dumps(delivery),
        pipeline_spec_uri=spec_uri,
    )
    fetch_spec = _make_fetch_spec(
        spec_uri=spec_uri,
        spec_text=spec_text,
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
        markers_table=naming.markers_table(_bare(raw_qt), pipeline),
    )

    result = glue_main.main(
        argv,
        fetch_spec=fetch_spec,
        fx_factory=_make_fx_factory("clean"),
    )

    assert result.raw_count == 3
    assert result.pre_quarantined_count == 0
    assert result.post_quarantined_count == 0
    assert _facts_appended_total(result) == 3
    assert result.guard_skips == ()
    assert result.published is True
    assert result.attempt_id == "jr_entrypoint_test"  # I-5, --JOB_RUN_ID fallback

    fact_golden = [("id-001", "alpha"), ("id-002", "bravo"), ("id-003", "charlie")]
    fact_rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(fact_qt).collect())
    assert fact_rows == fact_golden

    envelopes = [e for e in moto_events_bus.read_events() if e["detail"]["batch_id"] == batch_id]
    assert len(envelopes) == 2
    by_type = {e["detail-type"]: e["detail"] for e in envelopes}
    assert set(by_type) == {"batch-started", "batch-completed"}
    assert by_type["batch-completed"]["fact_count"] == 3


def test_main_raises_before_land_on_forged_object_uris_no_raw_rows_no_events(
    spark: SparkSession,
    unique_table: Callable[[str], str],
    ledger_catalog: LedgerCatalogFixture,
    moto_events_bus: MotoEventsBus,
) -> None:
    """R-12-style: a forged `object_uris` entry (wrong feed) is an I-22
    binding defect -- raises before `land`, so the (pre-created, empty) raw
    table gets no rows and the bus captures no events."""
    raw_qt = unique_table("entrypoint_forged_raw")
    qtn_qt = unique_table("entrypoint_forged_qtn")
    fact_qt = unique_table("entrypoint_forged_fact")
    state_qt = unique_table("entrypoint_forged_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)

    delivery_id = _delivery_id(9002)
    batch_id = _batch_id(9002)
    forged_uri = _canonical_object_uri(delivery_id, "object_1.csv").replace(
        _FEED_ID, "feed/some-other-feed"
    )
    # I-22 fails before spec fetch (`_never_fetch` asserts it's never
    # called), so `pipeline`/`spec_uri`'s exact content is never
    # dereferenced -- any grammar-legal pipeline string suffices.
    pipeline = _unique_pipeline("ep2")
    delivery = {
        "schema_version": 1,
        "feed_id": _FEED_ID,
        "delivery_id": delivery_id,
        "batch_id": batch_id,
        "delivery_key": "statement.csv",
        "content_hash": "sha256:" + "a" * 64,
        "size_bytes": 10,
        "object_uris": [forged_uri],
        "received_at": _RECEIVED_AT.isoformat(),
        "pipeline": pipeline,
    }
    spec_uri = f"s3://some-artifacts-bucket/spine/specs/{naming.slug(pipeline)}/pipeline.yaml"
    argv = _argv(
        ledger_catalog=ledger_catalog,
        event_bus=moto_events_bus.bus_name,
        delivery_json=json.dumps(delivery),
        pipeline_spec_uri=spec_uri,
    )

    def _never_fetch(uri: str) -> str:
        raise AssertionError("fetch_spec must not be called -- I-22 fails before spec fetch")

    try:
        glue_main.main(argv, fetch_spec=_never_fetch, fx_factory=_make_fx_factory("clean"))
        raise AssertionError("expected a ValueError (I-22 binding defect)")
    except ValueError as exc:
        assert "I-22" in str(exc)

    assert spark.table(raw_qt).limit(1).isEmpty()  # no land effect
    assert not [e for e in moto_events_bus.read_events() if e["detail"].get("batch_id") == batch_id]


# --- observability: JSON log handler install, idempotent across a warm-
# process repeat invocation (conveyer-nvh.47) --------------------------------


@pytest.fixture
def _clean_root_logger():
    """Snapshot + restore the REAL root logger around the test --
    `glue_main.main()` installs `observability.install_json_handler()` on
    `logging.getLogger()` itself (module-global, not a fixture-scoped
    logger) -- mirrors `ingestion/tests/integration/test_entrypoints.py`'s
    own `_clean_root_logger` fixture exactly, for the same reason: this must
    not leak a handler into the rest of the suite."""
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    root.handlers = []
    try:
        yield root
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)


def test_main_installs_json_log_handler_exactly_once_across_two_invocations(
    spark: SparkSession,
    unique_table: Callable[[str], str],
    ledger_catalog: LedgerCatalogFixture,
    moto_events_bus: MotoEventsBus,
    _clean_root_logger,
) -> None:
    from spine.observability import _JSON_HANDLER_NAME

    raw_qt = unique_table("entrypoint_loghandler_raw")
    qtn_qt = unique_table("entrypoint_loghandler_qtn")
    fact_qt = unique_table("entrypoint_loghandler_fact")
    state_qt = unique_table("entrypoint_loghandler_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    pipeline = _unique_pipeline("ep3")
    _create_markers_table(spark, _bare(raw_qt), pipeline)

    spec_text = yaml.safe_dump(
        {
            "pipeline": pipeline,
            "transforms_module": "pipelines.identity.transforms",
            "raw_table": _bare(raw_qt),
            "quarantine_table": _bare(qtn_qt),
            "fact_types": {
                "identity": {
                    "fact_table": _bare(fact_qt),
                    "state_table": _bare(state_qt),
                    "schema": {
                        "columns": [
                            {"name": "domain_id", "type": "string"},
                            {"name": "event_time", "type": "string"},
                            {"name": "payload", "type": "string"},
                        ],
                        "domain_id_col": "domain_id",
                        "record_key": ["domain_id"],
                    },
                }
            },
            "read": {"dialect": {"format": "csv", "header": True}},
            "raw_contract": {
                "columns": [
                    {"name": "domain_id", "required": True, "nullable": False},
                    {"name": "event_time"},
                    {"name": "source_ts"},
                    {"name": "content_hash"},
                    {"name": "payload"},
                ]
            },
            "sla_minutes": 480,
        }
    )
    spec_uri = f"s3://some-artifacts-bucket/spine/specs/{naming.slug(pipeline)}/pipeline.yaml"

    batch_id = _batch_id(9003)
    delivery_id = _delivery_id(9003)
    delivery = {
        "schema_version": 1,
        "feed_id": _FEED_ID,
        "delivery_id": delivery_id,
        "batch_id": batch_id,
        "delivery_key": "statement.csv",
        "content_hash": "sha256:" + "a" * 64,
        "size_bytes": 10,
        "object_uris": [_canonical_object_uri(delivery_id, "object_1.csv")],
        "received_at": _RECEIVED_AT.isoformat(),
        "pipeline": pipeline,
    }
    argv = _argv(
        ledger_catalog=ledger_catalog,
        event_bus=moto_events_bus.bus_name,
        delivery_json=json.dumps(delivery),
        pipeline_spec_uri=spec_uri,
    )

    fetch_spec = _make_fetch_spec(
        spec_uri=spec_uri,
        spec_text=spec_text,
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
        markers_table=naming.markers_table(_bare(raw_qt), pipeline),
    )

    glue_main.main(argv, fetch_spec=fetch_spec, fx_factory=_make_fx_factory("clean"))
    glue_main.main(  # warm-process repeat invocation (I-3 guard-skip, same batch_id)
        argv, fetch_spec=fetch_spec, fx_factory=_make_fx_factory("clean")
    )

    installed = [h for h in _clean_root_logger.handlers if h.name == _JSON_HANDLER_NAME]
    assert len(installed) == 1
