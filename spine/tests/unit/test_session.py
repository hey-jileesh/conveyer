"""Unit tests for `spine.entrypoints.session` — F2 (critique gate
wf_78ea4599-a5b, bead conveyer-swb.25): `catalog_conf`/`build_session`/
`assert_iceberg_extensions_active` are the ONE authored source both
`entrypoints/glue_main.py` and `entrypoints/rebuild_main.py` import — this
file proves, directly against the shared module (not through either
entrypoint), that BOTH callers' own config shapes (`spine.config.
RunnerConfig` and `entrypoints/rebuild_main.py::RebuildConfig`) satisfy
`SessionConfig` structurally and that `catalog_conf` carries `frames.
checks.SESSION_PINS` for both — the exact drift this bead's fix closes
(`rebuild_main`'s pre-fix private `_catalog_conf` omitted `SESSION_PINS`
entirely, while `glue_main`'s own copy carried it).

`test_glue_main.py`/`test_rebuild_main.py` each additionally exercise this
module through their OWN entrypoint's composition (unchanged, still green)
— this file is the dedicated home for the shared module's own behavior and
the cross-caller superset proof neither entrypoint's own suite is the
natural place for.
"""

from __future__ import annotations

import pytest
from spine.config import RunnerConfig
from spine.entrypoints import rebuild_main, session
from spine.frames.checks import SESSION_PINS


def _runner_config(*, catalog_kind: str = "glue", warehouse_uri: str | None = None) -> RunnerConfig:
    return RunnerConfig(
        env="test",
        aws_region="us-east-1",
        catalog_kind=catalog_kind,  # type: ignore[arg-type]
        warehouse_uri=warehouse_uri,
        ledger_catalog_kind="sql",
        ledger_sql_uri="sqlite:///:memory:",
        spine_db="spine_test_db",
        run_ledger_table="run_ledger",
        event_bus="test-bus",
        landing_bucket="test-landing",
        artifacts_bucket="test-artifacts",
        pipeline_spec_uri="s3://test-artifacts/spine/specs/p/y.yaml",
        delivery_json="{}",
        attempt_id="a1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        run_config_json="{}",
        sla_minutes=480,
    )


def _rebuild_config(
    *, catalog_kind: str = "glue", warehouse_uri: str | None = None
) -> rebuild_main.RebuildConfig:
    return rebuild_main.RebuildConfig(
        pipeline="pipelines/session-probe",
        pipeline_spec_uri="file:///x/spine/specs/p/y.yaml",
        artifacts_bucket="unused-bucket",
        env="test",
        aws_region="us-east-1",
        catalog_kind=catalog_kind,  # type: ignore[arg-type]
        warehouse_uri=warehouse_uri,
        ledger_catalog_kind="sql",
        ledger_sql_uri="sqlite:///:memory:",
        spine_db="spine_test_db",
        run_ledger_table="run_ledger",
    )


# --- `catalog_conf` -----------------------------------------------------------


def test_catalog_conf_glue_kind_sets_glue_catalog_type() -> None:
    conf = session.catalog_conf(_runner_config(catalog_kind="glue"))
    assert conf["spark.sql.catalog.spine_cat.type"] == "glue"
    assert "spark.sql.catalog.spine_cat.warehouse" not in conf
    assert conf["spark.sql.extensions"] == session.ICEBERG_EXTENSIONS


def test_catalog_conf_hadoop_kind_sets_warehouse() -> None:
    conf = session.catalog_conf(_runner_config(catalog_kind="hadoop", warehouse_uri="/tmp/wh"))
    assert conf["spark.sql.catalog.spine_cat.type"] == "hadoop"
    assert conf["spark.sql.catalog.spine_cat.warehouse"] == "/tmp/wh"


def test_catalog_conf_hadoop_kind_requires_warehouse_uri() -> None:
    with pytest.raises(ValueError, match="warehouse_uri"):
        session.catalog_conf(_runner_config(catalog_kind="hadoop", warehouse_uri=None))


# --- F2's own fix: SESSION_PINS superset, for BOTH callers' own config -------


def test_catalog_conf_is_a_superset_of_session_pins_for_runner_config() -> None:
    """The batch entrypoint's own config shape."""
    conf = session.catalog_conf(_runner_config())
    assert SESSION_PINS.items() <= conf.items()


def test_catalog_conf_is_a_superset_of_session_pins_for_rebuild_config() -> None:
    """F2's own drift, closed: `RebuildConfig` (`entrypoints/rebuild_main.py`'s
    own minimal argv contract, a DIFFERENT concrete dataclass from
    `RunnerConfig`) satisfies `SessionConfig` structurally, with no adapter,
    and gets the IDENTICAL `SESSION_PINS`-carrying conf `RunnerConfig` does
    -- both entrypoints now build their session conf through this ONE
    function, so they cannot drift apart the way they did pre-fix."""
    conf = session.catalog_conf(_rebuild_config())
    assert SESSION_PINS.items() <= conf.items()


def test_catalog_conf_glue_and_rebuild_configs_agree_field_for_field() -> None:
    """Both concrete config shapes produce the EXACT same conf for the
    fields that matter (not just a superset check) -- a `glue`-kind config
    and a `hadoop`-kind config each agree between `RunnerConfig` and
    `RebuildConfig`, proving the shared function is genuinely shape-
    agnostic beyond the two `SessionConfig` fields it reads (N3 fix, bead
    conveyer-swb.28: `env` was dropped from the Protocol)."""
    for catalog_kind, warehouse_uri in (("glue", None), ("hadoop", "/tmp/wh")):
        runner_conf = session.catalog_conf(
            _runner_config(catalog_kind=catalog_kind, warehouse_uri=warehouse_uri)
        )
        rebuild_conf = session.catalog_conf(
            _rebuild_config(catalog_kind=catalog_kind, warehouse_uri=warehouse_uri)
        )
        assert runner_conf == rebuild_conf


# --- `assert_iceberg_extensions_active` ([T-16]) ------------------------------


class _FakeConf:
    def __init__(self, extensions: str) -> None:
        self._extensions = extensions

    def get(self, key: str, default: str = "") -> str:
        assert key == "spark.sql.extensions"
        return self._extensions or default


class _FakeSession:
    def __init__(self, extensions: str) -> None:
        self.conf = _FakeConf(extensions)


def test_assert_iceberg_extensions_active_passes_when_present() -> None:
    fake_session = _FakeSession(session.ICEBERG_EXTENSIONS)
    session.assert_iceberg_extensions_active(fake_session)  # type: ignore[arg-type]


def test_assert_iceberg_extensions_active_raises_when_absent() -> None:
    fake_session = _FakeSession("")
    with pytest.raises(AssertionError, match=r"\[T-16\]"):
        session.assert_iceberg_extensions_active(fake_session)  # type: ignore[arg-type]


# --- `build_session` -----------------------------------------------------------


def test_build_session_adopts_the_live_shared_session(spark) -> None:  # noqa: ANN001
    """`build_session` uses `getOrCreate()` -- inside an already-active JVM
    (the shared session-scoped `spark` fixture, `tests/conftest.py`), it
    ADOPTS that SAME live session rather than conflicting with it, matching
    `entrypoints/glue_main.py`/`entrypoints/rebuild_main.py`'s own test
    behavior (both already rely on exactly this adoption)."""
    adopted = session.build_session(
        _runner_config(catalog_kind="hadoop", warehouse_uri="/tmp/should-never-be-touched"),
        app_name="test-app",
    )
    assert adopted is spark
