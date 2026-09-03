"""Unit tests for `spine.entrypoints.rebuild_main` — LLD 007.1 §9.4 (run-mode
trigger), A007-1 (bead conveyer-swb.13).

Covers the pure argv contract (`from_args`, its own `--conveyer-pipeline`
key, `_assert_pipeline_matches`) in isolation, the I-23 allowlist reuse (via
`glue_main.check_spec_uri_allowlist`, imported not duplicated), the
"no `--force` flag anywhere in this argv contract" claim mechanically, and
one genuine end-to-end round trip (`main()` -> a real Spark session + a real
SQLite pyiceberg run-ledger catalog) proving a rebuilt state table AND a
real `stage="rebuild"` ledger row both land, using the shared `spark`
fixture (`tests/conftest.py`) the same way `test_glue_main.py` does for its
own session-requiring tests.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import yaml
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from spine.bootstrap.create_record_tables import bootstrap_fact_table, bootstrap_state_table
from spine.bootstrap.create_run_ledger import create_run_ledger
from spine.core import naming
from spine.core.model import FactColumnSpec, FactSchemaModel, FactTypeModel, PipelineSpecModel
from spine.effects import ledger as ledger_effects
from spine.entrypoints import rebuild_main
from spine.entrypoints import session as session_mod
from spine.frames.checks import SESSION_PINS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_PIPELINE = "pipelines/rebuild-main-probe"

_SCHEMA = FactSchemaModel(
    columns=[
        FactColumnSpec(name="domain_id", type="string"),
        FactColumnSpec(name="event_time", type="timestamp"),
        FactColumnSpec(name="payload", type="string"),
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
    ordering=["event_time"],
)
_DF_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("delivery_id", StringType(), False),
        StructField("feed_id", StringType(), False),
        StructField("received_at", TimestampType(), False),
        StructField("event_time", TimestampType(), True),
        StructField("source_ts", TimestampType(), True),
        StructField("content_hash", StringType(), False),
        StructField("record_key", StringType(), False),
        StructField("domain_id", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)


def _spec_dict(*, pipeline: str, fact_bare: str, state_bare: str) -> dict:
    return {
        "pipeline": pipeline,
        "transforms_module": "pipelines.rebuild_main_probe",  # never imported by rebuild_main
        "raw_table": "lake.rmprobe__raw",
        "quarantine_table": "lake.rmprobe__quarantine",
        "fact_types": {
            "detail": {
                "fact_table": fact_bare,
                "state_table": state_bare,
                "schema": {
                    "columns": [
                        {"name": "domain_id", "type": "string"},
                        {"name": "event_time", "type": "timestamp"},
                        {"name": "payload", "type": "string"},
                    ],
                    "domain_id_col": "domain_id",
                    "record_key": ["domain_id"],
                    "ordering": ["event_time"],
                },
            }
        },
        "read": {"dialect": {"format": "csv"}},
        "raw_contract": {"columns": [{"name": "domain_id", "required": True, "nullable": False}]},
    }


def _argv(*, pipeline: str, pipeline_spec_uri: str, ledger_sql_uri: str) -> list[str]:
    return [
        "--conveyer-pipeline", pipeline,
        "--conveyer-pipeline-spec-uri", pipeline_spec_uri,
        # 6pg.35 item 4: required by `check_spec_uri_allowlist`'s bucket pin;
        # every `pipeline_spec_uri` this file uses is `file://`, which is
        # exempt from the bucket check itself (see glue_main.py's own
        # docstring), so this value is never asserted against -- only its
        # PRESENCE in argv (a required key regardless of scheme) matters.
        "--conveyer-artifacts-bucket", "unused-file-scheme-bucket",
        "--conveyer-env", "test",
        "--conveyer-aws-region", "us-east-1",
        "--conveyer-catalog-kind", "hadoop",
        "--conveyer-warehouse-uri", "/tmp/should-never-be-touched",
        "--conveyer-ledger-catalog-kind", "sql",
        "--conveyer-ledger-sql-uri", ledger_sql_uri,
        "--conveyer-spine-db", "spine_test_db",
        "--conveyer-run-ledger-table", "run_ledger",
    ]  # fmt: skip


def _file_fetch(uri: str) -> str:
    assert uri.startswith("file://")
    return Path(uri[len("file://") :]).read_text()


# --- pure: argv contract ------------------------------------------------------


def test_from_args_parses_minimal_rebuild_argv() -> None:
    argv = _argv(
        pipeline=_PIPELINE,
        pipeline_spec_uri="file:///x/spine/specs/p/y.yaml",
        ledger_sql_uri="sqlite:///:memory:",
    )
    config = rebuild_main.from_args(argv)
    assert config.pipeline == _PIPELINE
    assert config.catalog_kind == "hadoop"
    assert config.ledger_catalog_kind == "sql"
    assert config.spine_db == "spine_test_db"
    assert config.run_ledger_table == "run_ledger"
    assert config.warehouse_uri == "/tmp/should-never-be-touched"


def test_from_args_missing_required_key_raises_keyerror() -> None:
    argv = _argv(
        pipeline=_PIPELINE,
        pipeline_spec_uri="file:///x/spine/specs/p/y.yaml",
        ledger_sql_uri="sqlite:///:memory:",
    )
    argv = [a for a in argv if a not in ("--conveyer-spine-db", "spine_test_db")]
    with pytest.raises(KeyError):
        rebuild_main.from_args(argv)


def test_from_args_rejects_invalid_catalog_kind() -> None:
    argv = _argv(
        pipeline=_PIPELINE,
        pipeline_spec_uri="file:///x/spine/specs/p/y.yaml",
        ledger_sql_uri="sqlite:///:memory:",
    )
    argv[argv.index("hadoop")] = "bogus"
    with pytest.raises(ValueError, match="catalog_kind"):
        rebuild_main.from_args(argv)


def test_no_force_flag_anywhere_in_the_argv_contract() -> None:
    """RB-2, mechanically: this run mode's own argv key set never spells a
    `force`-shaped key -- there is no flag surface to weaken in the first
    place."""
    assert not any("force" in key.lower() for key in rebuild_main._ARGV_KEYS.values())


def test_main_passes_only_spec_and_record_run_into_rebuild_pipeline(
    monkeypatch: pytest.MonkeyPatch, spark: SparkSession, tmp_path: Path
) -> None:
    """M8 (bead conveyer-swb.25): sharpens the "no `--force` flag" claim
    beyond an argv-key-name substring check -- a name check alone would not
    catch an operator override reaching `effects.rebuild.rebuild_pipeline`
    through some OTHER (non-`force`-named) kwarg. Captures the ACTUAL call
    `rebuild_main.main` makes and asserts it carries exactly `spark`/`spec`/
    `record_run` -- no fourth argument, named or not, reaches the swap
    loop through this call site."""
    captured: dict[str, object] = {}

    def _fake_rebuild_pipeline(
        spark_arg: object, spec_arg: object, **kwargs: object
    ) -> dict[str, object]:
        captured["spark"] = spark_arg
        captured["spec"] = spec_arg
        captured["kwargs"] = kwargs
        return {}

    monkeypatch.setattr(rebuild_main, "rebuild_pipeline", _fake_rebuild_pipeline)

    spec_dict = _spec_dict(pipeline=_PIPELINE, fact_bare="lake.a", state_bare="lake.b")
    spec_dir = tmp_path / "spine" / "specs" / naming.slug(_PIPELINE)
    spec_dir.mkdir(parents=True)
    spec_path = spec_dir / "pipeline.yaml"
    spec_path.write_text(yaml.safe_dump(spec_dict))
    argv = _argv(
        pipeline=_PIPELINE,
        pipeline_spec_uri=f"file://{spec_path}",
        ledger_sql_uri=f"sqlite:///{tmp_path / 'ledger.db'}",
    )

    result = rebuild_main.main(argv, fetch_spec=_file_fetch)

    assert result == {}
    assert captured["spark"] is spark
    assert captured["spec"].pipeline == _PIPELINE
    assert set(captured["kwargs"]) == {"record_run"}
    assert callable(captured["kwargs"]["record_run"])


# --- pure: binding-defect asserts ---------------------------------------------


def test_assert_pipeline_matches_raises_on_mismatch() -> None:
    spec = PipelineSpecModel(
        **_spec_dict(pipeline=_PIPELINE, fact_bare="lake.a", state_bare="lake.b")
    )
    config = rebuild_main.from_args(
        _argv(
            pipeline="pipelines/some-other-pipeline",
            pipeline_spec_uri="file:///x/spine/specs/p/y.yaml",
            ledger_sql_uri="sqlite:///:memory:",
        )
    )
    with pytest.raises(ValueError, match="binding defect"):
        rebuild_main._assert_pipeline_matches(spec, config)


def test_assert_pipeline_matches_passes_when_equal() -> None:
    spec = PipelineSpecModel(
        **_spec_dict(pipeline=_PIPELINE, fact_bare="lake.a", state_bare="lake.b")
    )
    config = rebuild_main.from_args(
        _argv(
            pipeline=_PIPELINE,
            pipeline_spec_uri="file:///x/spine/specs/p/y.yaml",
            ledger_sql_uri="sqlite:///:memory:",
        )
    )
    rebuild_main._assert_pipeline_matches(spec, config)  # must not raise


# --- I-23 allowlist reuse (imported, not duplicated) --------------------------


def test_main_raises_before_fetch_on_spec_uri_allowlist_violation() -> None:
    """I-23: a `pipeline_spec_uri` whose path segment names a DIFFERENT
    pipeline than `--conveyer-pipeline` is rejected before any fetch --
    `glue_main.check_spec_uri_allowlist` is imported and reused, never
    re-derived (module docstring)."""

    def _never_call(uri: str) -> str:
        raise AssertionError(f"fetch_spec must not be called for this defect class: {uri!r}")

    argv = _argv(
        pipeline=_PIPELINE,
        pipeline_spec_uri="file:///x/spine/specs/pipelines--some-other-pipeline/y.yaml",
        ledger_sql_uri="sqlite:///:memory:",
    )
    with pytest.raises(ValueError, match="I-23"):
        rebuild_main.main(argv, fetch_spec=_never_call)


def test_main_raises_on_pipeline_mismatch_before_session_build() -> None:
    """The fetched spec's own declared `pipeline` disagreeing with
    `--conveyer-pipeline` is a binding defect raised BEFORE `session.
    build_session` -- proven via a poisoned `catalog_kind`/`warehouse_uri` combination that
    would raise a DIFFERENT, later error if the code incorrectly proceeded
    past this check (the same technique `test_glue_main.py`'s own
    `test_main_raises_on_pipeline_mismatch_before_session_build` uses)."""
    spec_uri = "file:///x/spine/specs/pipelines--rebuild-main-probe/y.yaml"

    def _fetch_wrong_pipeline(uri: str) -> str:
        return yaml.safe_dump(
            _spec_dict(pipeline="pipelines/someone-else", fact_bare="lake.a", state_bare="lake.b")
        )

    argv = _argv(
        pipeline=_PIPELINE, pipeline_spec_uri=spec_uri, ledger_sql_uri="sqlite:///:memory:"
    )
    argv[argv.index("hadoop")] = "hadoop"  # would raise ValueError("warehouse_uri...") if reached
    argv[argv.index("/tmp/should-never-be-touched")] = ""
    with pytest.raises(ValueError, match="binding defect"):
        rebuild_main.main(argv, fetch_spec=_fetch_wrong_pipeline)


# --- end-to-end: real Spark session + real SQLite ledger catalog -------------


def _bootstrap_tables(spark: SparkSession, prefix: str) -> tuple[str, str, FactTypeModel]:
    fact_qt = f"spine_cat.spine_test_tables.{prefix}fact_{uuid.uuid4().hex[:8]}"
    state_qt = f"spine_cat.spine_test_tables.{prefix}state_{uuid.uuid4().hex[:8]}"
    bootstrap_fact_table(spark, fact_qt, _SCHEMA)
    bootstrap_state_table(spark, state_qt, _SCHEMA)
    fact_bare = fact_qt.removeprefix("spine_cat.")
    state_bare = state_qt.removeprefix("spine_cat.")
    fact_type = FactTypeModel(fact_table=fact_bare, state_table=state_bare, schema=_SCHEMA)
    return fact_qt, state_qt, fact_type


def test_main_rebuilds_state_table_end_to_end(spark: SparkSession, tmp_path: Path) -> None:
    """The full `rebuild_main.main` composition, real Spark + real SQLite
    pyiceberg ledger: a fact table with one committed batch rebuilds its
    own state table via the production `effects.rebuild.rebuild_pipeline`
    path, AND a real `stage="rebuild"` ledger row lands (read back via
    pyiceberg, mirroring `conftest.py::LedgerCatalogFixture.rows`).

    M4 (bead conveyer-swb.25): `_bootstrap_tables` leaves the state table a
    genuinely NEVER-FOLDED, zero-snapshot table (DDL creation only, no
    manual genesis-seed step here anymore) -- this proves `rebuild_main.
    main` -> `effects.rebuild.rebuild_state_table`'s own genesis-seed fix
    converges a virgin state table by construction, at the real production
    entrypoint's grain, not just at `effects/rebuild.py`'s own unit grain
    (`test_k_suite_rebuild.py::test_rebuild_state_table_genesis_seeds_a_
    never_folded_state_table_and_converges`)."""
    fact_qt, state_qt, fact_type = _bootstrap_tables(spark, "rmend2end")

    row = Row(
        batch_id="b1",
        delivery_id="d",
        feed_id="f",
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        event_time=datetime(2026, 1, 1, tzinfo=UTC),
        source_ts=None,
        content_hash="0" * 64,
        record_key="d1",
        domain_id="d1",
        payload="p1",
    )
    spark.createDataFrame([row], schema=_DF_SCHEMA).writeTo(fact_qt).option(
        "check-nullability", "false"
    ).append()

    spec_dict = _spec_dict(
        pipeline=_PIPELINE, fact_bare=fact_type.fact_table, state_bare=fact_type.state_table
    )
    spec_dir = tmp_path / "spine" / "specs" / naming.slug(_PIPELINE)
    spec_dir.mkdir(parents=True)
    spec_path = spec_dir / "pipeline.yaml"
    spec_path.write_text(yaml.safe_dump(spec_dict))
    spec_uri = f"file://{spec_path}"

    ledger_sql_uri = f"sqlite:///{tmp_path / 'ledger.db'}"
    argv = _argv(pipeline=_PIPELINE, pipeline_spec_uri=spec_uri, ledger_sql_uri=ledger_sql_uri)

    # M3 (bead conveyer-swb.25): `RebuildConfig` (from `from_args`) flows
    # straight into `ledger.build_catalog` -- no `_as_runner_config`
    # adapter, deleted outright (`LedgerConfig`'s narrow Protocol).
    config = rebuild_main.from_args(argv)
    catalog = ledger_effects.build_catalog(config)
    create_run_ledger(catalog, config.spine_db, config.run_ledger_table)

    results = rebuild_main.main(argv, fetch_spec=_file_fetch)

    assert set(results) == {fact_type.state_table}
    assert results[fact_type.state_table].attempts == 1
    rows_final = sorted((r["domain_id"], r["payload"]) for r in spark.table(state_qt).collect())
    assert rows_final == [("d1", "p1")]

    ledger_rows = (
        catalog.load_table(f"{config.spine_db}.{config.run_ledger_table}")
        .scan()
        .to_arrow()
        .to_pylist()
    )
    assert len(ledger_rows) == 1
    assert ledger_rows[0]["stage"] == "rebuild"
    assert ledger_rows[0]["outcome"] == "ok"
    assert ledger_rows[0]["pipeline"] == _PIPELINE
    assert ledger_rows[0]["feed_id"] == fact_type.state_table
    assert ledger_rows[0]["state_read_snapshot_id"] is not None


# --- F2: one session contract, `entrypoints/session.py` -----------------------


def test_rebuild_main_session_conf_is_a_superset_of_session_pins() -> None:
    """F2 (bead conveyer-swb.25): the drift this bead closes -- before the
    fix, `rebuild_main`'s own `_catalog_conf` omitted `frames.checks.
    SESSION_PINS` entirely, while `glue_main`'s own copy carried it, the
    "two evaluators, same code, different engine semantics" class named in
    the critique. `rebuild_main.py` now builds its session conf via
    `entrypoints/session.py::catalog_conf` -- the SAME function `glue_main.
    py` calls -- so `SESSION_PINS` is present in EVERY `RebuildConfig`'s own
    conf by construction, proven directly here rather than only implicitly
    via the e2e test above."""
    config = rebuild_main.from_args(
        _argv(
            pipeline=_PIPELINE,
            pipeline_spec_uri="file:///x/spine/specs/p/y.yaml",
            ledger_sql_uri="sqlite:///:memory:",
        )
    )
    conf = session_mod.catalog_conf(config)
    assert SESSION_PINS.items() <= conf.items()
