"""Unit tests for `spine.effects.ledger` — LLD §6.5, §7.3, §7.6, §11.3,
[C-6], [S-7], [T-18].

Covers: `build_catalog`'s `sql` (validation + working `SqlCatalog`) and
`glue` (constructible with no network call, region-tagged) branches;
`_row_from_run_fact`'s field-set match against `RUN_LEDGER_SCHEMA`;
`_stage_metrics`/`_lifecycle_metrics`'s pure per-`RunFact` derivation
(conveyer-nvh.47 adds the latter: `JobAttempts`/`BatchesStarted`/
`BatchesCompleted`, plus a scrape-based test pinning that every
`Conveyer/Spine` metric named by a `monitoring.tf` alarm has an emitter
somewhere in this codebase); `record_run`'s channel ordering (INFO + EMF
before the ledger append is even attempted), its 2-attempt / <=2s-backoff
budget, that it NEVER raises regardless of what the catalog does, that a
lost append logs the row at WARNING with `error_message` omitted, and a
`RunFact` -> Arrow round trip covering both the all-`None`-nullable and
all-populated (incl. map fields) shapes.
"""

from __future__ import annotations

import logging
import re
import types
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from spine.bootstrap.create_run_ledger import create_run_ledger
from spine.core.run_facts import RunFact
from spine.effects import ledger

_NOW = datetime(2026, 7, 28, 10, 0, 0, tzinfo=UTC)
_SPINE_ROOT = Path(__file__).resolve().parents[2]  # .../conveyer/spine


@dataclass(frozen=True)
class _FakeConfig:
    ledger_catalog_kind: str
    ledger_sql_uri: str | None
    warehouse_uri: str | None
    aws_region: str = "us-east-1"
    spine_db: str = "spine_db"
    run_ledger_table: str = "run_ledger"


def _sql_config(tmp_path) -> _FakeConfig:
    return _FakeConfig(
        ledger_catalog_kind="sql",
        ledger_sql_uri=f"sqlite:///{tmp_path}/catalog.db",
        warehouse_uri=f"file://{tmp_path}/warehouse",
    )


def _bootstrap(config: _FakeConfig):
    catalog = ledger.build_catalog(config)
    create_run_ledger(catalog, config.spine_db, config.run_ledger_table)
    return catalog


def _rows(catalog, config: _FakeConfig) -> list[dict[str, object]]:
    identifier = f"{config.spine_db}.{config.run_ledger_table}"
    result: list[dict[str, object]] = catalog.load_table(identifier).scan().to_arrow().to_pylist()
    return result


def _run_fact(**overrides: object) -> RunFact:
    base: dict[str, object] = dict(
        batch_id="b1",
        pipeline="p1",
        feed_id="f1",
        attempt_id="a1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        stage="land",
        outcome="ok",
        started_at=_NOW,
        finished_at=_NOW,
    )
    base.update(overrides)
    return RunFact(**base)  # type: ignore[arg-type]


# --- build_catalog ------------------------------------------------------------


def test_build_catalog_sql_requires_ledger_sql_uri_and_warehouse_uri() -> None:
    with pytest.raises(ValueError, match="ledger_catalog_kind='sql'"):
        ledger.build_catalog(
            _FakeConfig(ledger_catalog_kind="sql", ledger_sql_uri=None, warehouse_uri=None)
        )


def test_build_catalog_sql_builds_a_working_catalog(tmp_path) -> None:
    config = _sql_config(tmp_path)
    catalog = ledger.build_catalog(config)
    catalog.create_namespace_if_not_exists(config.spine_db)
    table = catalog.create_table_if_not_exists(
        identifier=f"{config.spine_db}.{config.run_ledger_table}",
        schema=ledger.RUN_LEDGER_SCHEMA,
        partition_spec=ledger.RUN_LEDGER_PARTITION_SPEC,
        properties=ledger.RUN_LEDGER_TABLE_PROPERTIES,
    )
    assert table.properties == ledger.RUN_LEDGER_TABLE_PROPERTIES


def test_build_catalog_glue_constructs_without_network_call() -> None:
    from pyiceberg.catalog.glue import GlueCatalog

    config = _FakeConfig(
        ledger_catalog_kind="glue", ledger_sql_uri=None, warehouse_uri=None, aws_region="eu-west-1"
    )
    catalog = ledger.build_catalog(config)
    assert isinstance(catalog, GlueCatalog)
    assert catalog.glue.meta.region_name == "eu-west-1"


# --- _row_from_run_fact / _stage_metrics (pure) -------------------------------


def test_row_from_run_fact_field_set_matches_schema() -> None:
    row = ledger._row_from_run_fact(_run_fact(), datetime.now(UTC))
    schema_names = {field.name for field in ledger.RUN_LEDGER_SCHEMA.fields}
    assert set(row.keys()) == schema_names


def test_stage_metrics_always_includes_stage_seconds() -> None:
    rf = _run_fact(started_at=_NOW, finished_at=_NOW + timedelta(seconds=5))
    metrics = ledger._stage_metrics(rf)
    assert ("StageSeconds", 5.0, "land") in metrics


@pytest.mark.parametrize(
    ("field", "value", "expected_name", "expected_stage"),
    [
        ("raw_count", 100, "RawRows", None),
        ("pre_quarantined", 3, "QuarantinedRows", "land"),
        ("post_quarantined", 2, "QuarantinedRows", "land"),
        ("facts_appended", 7, "FactsAppended", None),
        ("rows_merged", 9, "RowsMerged", None),
    ],
)
def test_stage_metrics_conditional_fields(field, value, expected_name, expected_stage) -> None:
    rf = _run_fact(**{field: value})
    metrics = ledger._stage_metrics(rf)
    assert (expected_name, float(value), expected_stage) in metrics


def test_stage_metrics_guard_skips_on_skipped_guard_outcome() -> None:
    rf = _run_fact(stage="pre_check", outcome="skipped-guard")
    metrics = ledger._stage_metrics(rf)
    assert ("GuardSkips", 1.0, "pre_check") in metrics


def test_stage_metrics_no_extra_metrics_when_all_counts_none() -> None:
    rf = _run_fact()
    metrics = ledger._stage_metrics(rf)
    assert metrics == (("StageSeconds", 0.0, "land"),)


# --- _lifecycle_metrics (pure) — conveyer-nvh.47 -----------------------------


@pytest.mark.parametrize(
    ("stage", "outcome", "expected"),
    [
        ("land", "ok", (("JobAttempts", 1.0), ("BatchesStarted", 1.0))),
        ("land", "skipped-guard", (("JobAttempts", 1.0), ("BatchesStarted", 1.0))),
        ("land", "failed", (("JobAttempts", 1.0),)),  # an attempt happened even though it failed
        ("publish", "ok", (("BatchesCompleted", 1.0),)),
        ("publish", "failed", ()),  # unreachable via transition() in practice, still defensive
        ("commit", "ok", ()),  # no lifecycle metric on any other stage
    ],
)
def test_lifecycle_metrics(stage, outcome, expected) -> None:
    rf = _run_fact(stage=stage, outcome=outcome)
    assert ledger._lifecycle_metrics(rf) == expected


def test_emit_metrics_includes_lifecycle_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: calls.append(
            (name, value, stage)
        ),
    )
    ledger._emit_metrics(_run_fact(stage="land", outcome="ok"))
    assert ("JobAttempts", 1.0, None) in calls
    assert ("BatchesStarted", 1.0, None) in calls


# --- alarm/emitter coverage: every Conveyer/Spine metric an alarm names ------
# --- has an emitter somewhere (conveyer-nvh.47) ------------------------------

# Every metric name this module (or `entrypoints/router.py`, for
# `SingleFlightCollisions` -- a pre-Glue-job collision detected before any
# `RunFact` exists, out of this module's scope by construction) can ever
# emit under the `Conveyer/Spine` namespace -- the §11.1 list restated once
# here as the coverage oracle, so a future alarm added against an unemitted
# metric fails this test rather than silently re-creating the s11.4 gap.
_ALL_EMITTED_CONVEYER_SPINE_METRIC_NAMES = frozenset(
    {
        "StageSeconds",
        "RawRows",
        "QuarantinedRows",
        "FactsAppended",
        "RowsMerged",
        "GuardSkips",
        "JobAttempts",
        "BatchesStarted",
        "BatchesCompleted",
        "RunLedgerLoss",
        "PostCheckDrift",
        "PreCheckDrift",
        "SingleFlightCollisions",
    }
)


def _conveyer_spine_metric_names_referenced_by_alarms(monitoring_tf: Path) -> set[str]:
    """Metric names referenced via `SUM(<name>)` inside a `SCHEMA("Conveyer/
    Spine", ...)` Metrics Insights `metric_query` expression -- the only
    shape any `Conveyer/Spine` alarm in this codebase uses (the native
    `AWS/States`/`AWS/Lambda`/`AWS/SQS` alarms in these same files set a
    plain `metric_name`/`namespace` pair instead, deliberately excluded by
    this regex)."""
    text = monitoring_tf.read_text()
    pattern = re.compile(r'SUM\((\w+)\)\s+FROM\s+SCHEMA\(\\"Conveyer/Spine\\"')
    return set(pattern.findall(text))


def test_every_alarm_referenced_conveyer_spine_metric_has_an_emitter() -> None:
    """conveyer-nvh.47: the s11.4 `job_attempts` alarm named `JobAttempts`
    when nothing emitted it. Scrapes both `monitoring.tf` files' own
    `metric_query` expressions (rather than hardcoding the alarm list here)
    so this stays a live regression guard as alarms are added, not a
    point-in-time snapshot."""
    pipeline_tf = _SPINE_ROOT / "terraform" / "modules" / "spine-pipeline" / "monitoring.tf"
    platform_tf = _SPINE_ROOT / "terraform" / "modules" / "spine-platform" / "monitoring.tf"
    referenced = _conveyer_spine_metric_names_referenced_by_alarms(
        pipeline_tf
    ) | _conveyer_spine_metric_names_referenced_by_alarms(platform_tf)

    assert referenced, "sanity: the scrape itself must find at least one metric name"
    missing = referenced - _ALL_EMITTED_CONVEYER_SPINE_METRIC_NAMES
    assert not missing, f"alarm(s) reference metric(s) with no known emitter: {missing!r}"


# --- record_run: channel ordering, budget, never-raises, WARNING shape -------


def test_record_run_never_raises_when_catalog_factory_is_broken(tmp_path) -> None:
    def broken_catalog() -> None:
        raise RuntimeError("catalog boom")

    record_run = ledger.build_record_run(broken_catalog, _sql_config(tmp_path))
    record_run(_run_fact())  # must not raise


def test_record_run_never_raises_even_when_logging_itself_is_broken(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """The absolute backstop (§11.3): even a failure BEFORE the ledger
    append is attempted at all (e.g. a broken logger/metrics sink) must not
    escape `record_run`."""

    def broken_log_transition(_run_fact: RunFact) -> None:
        raise RuntimeError("logger boom")

    monkeypatch.setattr(ledger, "_log_transition", broken_log_transition)

    record_run = ledger.build_record_run(lambda: None, _sql_config(tmp_path))  # type: ignore[arg-type]
    record_run(_run_fact())  # must not raise


def test_record_run_channel_ordering_log_and_metrics_before_append_attempt(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    order: list[str] = []
    real_log = ledger._log_transition
    real_metrics = ledger._emit_metrics

    def recording_log(run_fact: RunFact) -> None:
        order.append("log")
        real_log(run_fact)

    def recording_metrics(run_fact: RunFact) -> None:
        order.append("metrics")
        real_metrics(run_fact)

    monkeypatch.setattr(ledger, "_log_transition", recording_log)
    monkeypatch.setattr(ledger, "_emit_metrics", recording_metrics)
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))

    class _AlwaysFailingCatalog:
        def load_table(self, _identifier: str):
            order.append("append_attempt")
            raise RuntimeError("boom")

    record_run = ledger.build_record_run(_AlwaysFailingCatalog(), _sql_config(tmp_path))
    record_run(_run_fact())

    assert order == ["log", "metrics", "append_attempt", "append_attempt"]


def test_record_run_budget_is_two_attempts_one_bounded_backoff(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=sleeps.append))
    calls = {"n": 0}

    class _AlwaysFailingCatalog:
        def load_table(self, _identifier: str):
            calls["n"] += 1
            raise RuntimeError("boom")

    record_run = ledger.build_record_run(_AlwaysFailingCatalog(), _sql_config(tmp_path))
    record_run(_run_fact())

    assert calls["n"] == 2  # [C-6]: exactly 2 attempts
    assert len(sleeps) == 1  # one gap between 2 attempts
    assert 0.0 <= sleeps[0] <= 2.0  # [C-6]: <= 2s total backoff


def test_record_run_success_appends_a_row_matching_schema(tmp_path) -> None:
    config = _sql_config(tmp_path)
    catalog = _bootstrap(config)
    record_run = ledger.build_record_run(catalog, config)

    record_run(_run_fact(raw_count=10, snapshot_id=42))

    rows = _rows(catalog, config)
    assert len(rows) == 1
    row = rows[0]
    assert row["batch_id"] == "b1"
    assert row["raw_count"] == 10
    assert row["snapshot_id"] == 42
    assert row["recorded_at"] is not None


def test_record_run_row_derivation_failure_hits_warning_and_loss_metric_not_silence(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    """conveyer-nvh.36 fix: a `RunFact` whose `co_effect_snapshot_ids` is a
    poisoned `Mapping` (raises on `dict(...)` conversion, standing in for
    `stages/pull.py`'s real `MappingProxyType` field before the fix) must
    still reach the WARNING + `RunLedgerLoss` path -- NOT be swallowed
    silently by `record_run`'s outer `except: pass`. Before this fix, the
    row-derivation exception (then `dataclasses.asdict`'s `copy.deepcopy`
    call) happened BEFORE `_try_append`/`_log_ledger_loss` ever ran, so
    neither fired; this test pins that it now does, for ANY row-derivation
    failure, not just the specific `MappingProxyType` shape."""
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))

    class _PoisonMapping(Mapping):
        def __iter__(self):
            raise RuntimeError("poison: cannot iterate")

        def __len__(self) -> int:
            return 1

        def __getitem__(self, key: object) -> object:
            raise RuntimeError("poison: cannot getitem")

    class _NeverCalledCatalog:
        def load_table(self, _identifier: str):
            raise AssertionError("must never be reached -- row derivation fails first")

    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )

    record_run = ledger.build_record_run(_NeverCalledCatalog(), _sql_config(tmp_path))
    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(
            _run_fact(stage="pull", co_effect_snapshot_ids=_PoisonMapping())
        )  # must not raise

    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_records) == 1
    assert warning_records[0].stage == "pull"
    run_ledger_loss_calls = [c for c in metric_calls if c[0] == "RunLedgerLoss"]
    assert len(run_ledger_loss_calls) == 1
    assert run_ledger_loss_calls[0] == ("RunLedgerLoss", 1.0, "pull")


def test_record_run_warning_log_omits_error_message_but_keeps_error_type(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))

    def broken_catalog() -> None:
        raise RuntimeError("boom")

    record_run = ledger.build_record_run(broken_catalog, _sql_config(tmp_path))
    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(
            _run_fact(
                stage="pull",
                outcome="failed",
                error_type="TransientError",
                error_message="row value 42 leaked here",
            )
        )

    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warning_records) == 1
    warning_record = warning_records[0]
    log_output = warning_record.getMessage()
    assert "row value 42 leaked here" not in log_output
    assert "TransientError" in log_output  # error_type name itself is fine, [S-7]
    assert "b1" in log_output  # batch_id, an id -- not a row value
    assert warning_record.batch_id == "b1"  # §11.2 identifier extras carried
    assert warning_record.stage == "pull"


# --- post_check drift WARNING + EMF (moved here from stages/post_check.py, ---
# --- critique F4, bead conveyer-nvh.43; gate corrected, critique F2, bead ---
# --- conveyer-azr.30, to fire on BOTH doors -- outcome="ok" (door 2, [DC-1] --
# --- fact-presence demotion) as well as outcome="skipped-guard" (door 4), ---
# --- an EXACT mirror of pre_check's own two cases below) --------------------


def test_record_run_post_check_drift_warns_and_emits_metric_on_ok_outcome(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    """The gap critique F2 fixes: door 2 (`stages/post_check.py`'s own
    [DC-1] fact-presence demotion) records drift on an `outcome="ok"`
    transition (the quarantine table's OWN guard was never present there,
    so `guard_skips` never accretes "post_check") -- the original
    (nvh.43) gate, `outcome == "skipped-guard"` only, silently swallowed
    this WARNING + EMF entirely (the drift still folded into the ledger
    row's `error_message`, but never alarmed)."""
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )

    def broken_catalog() -> None:
        raise RuntimeError("boom")

    record_run = ledger.build_record_run(broken_catalog, _sql_config(tmp_path))
    drift_text = "post-check drift: durable=0 recomputed=1 subset=False"
    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(_run_fact(stage="post_check", outcome="ok", error_message=drift_text))

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    drift_warnings = [r for r in warnings if "drift" in r.getMessage().lower()]
    assert len(drift_warnings) == 1
    assert drift_text in drift_warnings[0].getMessage()
    assert drift_warnings[0].stage == "post_check"
    assert drift_warnings[0].batch_id == "b1"
    drift_metrics = [c for c in metric_calls if c[0] == "PostCheckDrift"]
    assert drift_metrics == [("PostCheckDrift", 1, "post_check")]


def test_record_run_post_check_drift_warns_and_emits_metric_on_skipped_guard_outcome(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )

    def broken_catalog() -> None:
        raise RuntimeError("boom")

    record_run = ledger.build_record_run(broken_catalog, _sql_config(tmp_path))
    drift_text = "post-check drift: durable=1 recomputed=0 subset=False"
    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(_run_fact(stage="post_check", outcome="skipped-guard", error_message=drift_text))

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    drift_warnings = [r for r in warnings if "drift" in r.getMessage().lower()]
    assert len(drift_warnings) == 1
    assert drift_text in drift_warnings[0].getMessage()
    assert drift_warnings[0].stage == "post_check"
    assert drift_warnings[0].batch_id == "b1"
    drift_metrics = [c for c in metric_calls if c[0] == "PostCheckDrift"]
    assert drift_metrics == [("PostCheckDrift", 1, "post_check")]


@pytest.mark.parametrize("outcome", ["ok", "skipped-guard"])
def test_record_run_post_check_no_drift_does_not_warn_or_emit(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture, outcome: str
) -> None:
    """`error_message is None` (the fresh-compute path, OR either door's own
    no-drift case) -- no `PostCheckDrift` channel at all, regardless of
    outcome."""
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )
    record_run = ledger.build_record_run(lambda: None, _sql_config(tmp_path))  # type: ignore[arg-type]

    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(_run_fact(stage="post_check", outcome=outcome, error_message=None))

    assert not any("drift" in r.getMessage().lower() for r in caplog.records)
    assert not any(c[0] == "PostCheckDrift" for c in metric_calls)


def test_record_run_post_check_failed_outcome_does_not_emit_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    """A genuinely FAILED `post_check` transition (`outcome="failed"`,
    recorded via `run_facts.failed`, never `transition`) never reaches the
    drift channel, even though it too carries a non-`None` `error_message` --
    guarding on `outcome != "failed"` (not merely `error_message is not
    None`) keeps this precondition independent of `failed()`'s own unrelated
    `error_message` derivation, the same shape as pre_check's own gate
    below."""
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )
    record_run = ledger.build_record_run(lambda: None, _sql_config(tmp_path))  # type: ignore[arg-type]

    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(
            _run_fact(
                stage="post_check",
                outcome="failed",
                error_type="AssertionError",
                error_message="spine.stages.post_check:83",
            )
        )

    assert not any("post_check drift" in r.getMessage().lower() for r in caplog.records)
    assert not any(c[0] == "PostCheckDrift" for c in metric_calls)


# --- pre_check drift WARNING + EMF (005.1 A-9, bead conveyer-azr.18) ---------
#
# Unlike post_check, `pre_check`'s own [DC-1] door 2 (fact-presence
# demotion, §6.5) reaches this channel on `outcome="ok"` -- the quarantine
# table's OWN guard was never present there, so `guard_skips` never accretes
# "pre_check" -- while door 3 (guard-present subtraction) reaches it on
# `outcome="skipped-guard"`, same as post_check's one door. `record_run`
# gates on `stage == "pre_check" and error_message is not None` alone (no
# outcome restriction) -- both cases below must emit.


def test_record_run_pre_check_drift_warns_and_emits_metric_on_ok_outcome(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )

    def broken_catalog() -> None:
        raise RuntimeError("boom")

    record_run = ledger.build_record_run(broken_catalog, _sql_config(tmp_path))
    drift_text = (
        "pre_check drift: durable=0 recomputed=1 only_durable=0 only_recomputed=1 "
        "admitted_cast_failures=0 check_version=abc123"
    )
    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(_run_fact(stage="pre_check", outcome="ok", error_message=drift_text))

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    drift_warnings = [r for r in warnings if "drift" in r.getMessage().lower()]
    assert len(drift_warnings) == 1
    assert drift_text in drift_warnings[0].getMessage()
    assert drift_warnings[0].stage == "pre_check"
    assert drift_warnings[0].batch_id == "b1"
    drift_metrics = [c for c in metric_calls if c[0] == "PreCheckDrift"]
    assert drift_metrics == [("PreCheckDrift", 1, "pre_check")]


def test_record_run_pre_check_drift_warns_and_emits_metric_on_skipped_guard_outcome(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )
    record_run = ledger.build_record_run(lambda: None, _sql_config(tmp_path))  # type: ignore[arg-type]
    drift_text = "pre_check drift: durable=1 recomputed=2 only_durable=0 only_recomputed=1"
    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(_run_fact(stage="pre_check", outcome="skipped-guard", error_message=drift_text))

    drift_warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "drift" in r.getMessage().lower()
    ]
    assert len(drift_warnings) == 1
    assert drift_text in drift_warnings[0].getMessage()
    drift_metrics = [c for c in metric_calls if c[0] == "PreCheckDrift"]
    assert drift_metrics == [("PreCheckDrift", 1, "pre_check")]


def test_record_run_pre_check_no_drift_does_not_warn_or_emit(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )
    record_run = ledger.build_record_run(lambda: None, _sql_config(tmp_path))  # type: ignore[arg-type]

    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(_run_fact(stage="pre_check", outcome="ok", error_message=None))

    assert not any("drift" in r.getMessage().lower() for r in caplog.records)
    assert not any(c[0] == "PreCheckDrift" for c in metric_calls)


def test_record_run_pre_check_failed_outcome_does_not_emit_drift(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )
    record_run = ledger.build_record_run(lambda: None, _sql_config(tmp_path))  # type: ignore[arg-type]

    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(
            _run_fact(
                stage="pre_check",
                outcome="failed",
                error_type="AssertionError",
                error_message="spine.stages.pre_check:83",
            )
        )

    assert not any("pre_check drift" in r.getMessage().lower() for r in caplog.records)
    assert not any(c[0] == "PreCheckDrift" for c in metric_calls)


def test_record_run_pre_check_drift_does_not_leak_into_other_stage(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    """A non-`pre_check` stage carrying SOME `error_message` (e.g. post_check's
    own drift text) never trips the `PreCheckDrift` channel."""
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )
    record_run = ledger.build_record_run(lambda: None, _sql_config(tmp_path))  # type: ignore[arg-type]

    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(
            _run_fact(
                stage="post_check",
                outcome="skipped-guard",
                error_message="post-check drift: durable=1 recomputed=2 subset=False",
            )
        )

    assert not any(c[0] == "PreCheckDrift" for c in metric_calls)


# --- DivergentDuplicates WARNING + EMF (moved here from stages/commit.py, ---
# --- critique gate wf_24a3125f-ecc F1, bead conveyer-6pg.30 -- previously ---
# --- ZERO test coverage of any kind) -----------------------------------------


def test_record_run_divergent_duplicates_emits_metric_per_table_and_warns_only_if_positive(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    """§12 (D-2(b)): the EMF metric fires for EVERY table entry in the map,
    including a zero count (the pre-fix stage-side cadence, preserved) --
    the WARNING log fires ONLY for a table whose count is actually positive
    (`_emit_divergent_duplicates`'s own documented deviation from the
    drift-channel gate shape)."""
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None, dict[str, str] | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, extra_dims=None, **_kw: (
            metric_calls.append((name, value, stage, dict(extra_dims) if extra_dims else None))
        ),
    )

    def broken_catalog() -> None:
        raise RuntimeError("boom")

    record_run = ledger.build_record_run(broken_catalog, _sql_config(tmp_path))
    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(
            _run_fact(
                stage="commit",
                outcome="ok",
                divergent_duplicates_by_table={
                    "lake.orders__facts": 2,
                    "lake.shipments__facts": 0,
                },
            )
        )

    divergence_metrics = [c for c in metric_calls if c[0] == "DivergentDuplicates"]
    assert sorted(divergence_metrics) == [
        ("DivergentDuplicates", 0, "commit", {"table": "lake.shipments__facts"}),
        ("DivergentDuplicates", 2, "commit", {"table": "lake.orders__facts"}),
    ]

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    divergence_warnings = [r for r in warnings if "divergent duplicates" in r.getMessage().lower()]
    assert len(divergence_warnings) == 1  # only the positive-count table
    assert "table=lake.orders__facts" in divergence_warnings[0].getMessage()
    assert "count=2" in divergence_warnings[0].getMessage()
    assert divergence_warnings[0].stage == "commit"
    assert divergence_warnings[0].batch_id == "b1"


def test_record_run_divergent_duplicates_none_map_does_not_warn_or_emit(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    """A `commit` `RunFact` whose `divergent_duplicates_by_table` is `None`
    (a genuinely failed transition never reaches `_stage_fields` at all,
    §7.7 -- `failed()` never populates this field) never trips the channel."""
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )
    record_run = ledger.build_record_run(lambda: None, _sql_config(tmp_path))  # type: ignore[arg-type]

    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(
            _run_fact(
                stage="commit",
                outcome="failed",
                error_type="TransientError",
                error_message="spine.stages.commit:210",
            )
        )

    assert not any("divergent duplicates" in r.getMessage().lower() for r in caplog.records)
    assert not any(c[0] == "DivergentDuplicates" for c in metric_calls)


def test_record_run_divergent_duplicates_does_not_leak_into_other_stage(
    monkeypatch: pytest.MonkeyPatch, tmp_path, caplog: pytest.LogCaptureFixture
) -> None:
    """A non-`commit` stage never trips the `DivergentDuplicates` channel,
    even if it were (adversarially) carrying a populated map."""
    monkeypatch.setattr(ledger, "time", types.SimpleNamespace(sleep=lambda _s: None))
    metric_calls: list[tuple[str, float, str | None]] = []
    monkeypatch.setattr(
        ledger.observability,
        "emit_metric",
        lambda name, value, _pipeline, _feed_id, stage=None, **_kw: metric_calls.append(
            (name, value, stage)
        ),
    )
    record_run = ledger.build_record_run(lambda: None, _sql_config(tmp_path))  # type: ignore[arg-type]

    with caplog.at_level(logging.INFO, logger=ledger._LOGGER_NAME):
        record_run(
            _run_fact(
                stage="fold",
                outcome="ok",
                divergent_duplicates_by_table={"lake.orders__facts": 3},
            )
        )

    assert not any("divergent duplicates" in r.getMessage().lower() for r in caplog.records)
    assert not any(c[0] == "DivergentDuplicates" for c in metric_calls)


# --- RunFact -> Arrow round trip, every nullable field combination -----------


def test_run_fact_round_trip_all_nullable_fields_none(tmp_path) -> None:
    config = _sql_config(tmp_path)
    catalog = _bootstrap(config)
    record_run = ledger.build_record_run(catalog, config)

    record_run(_run_fact(stage="apply", outcome="ok"))  # every nullable field left None

    row = _rows(catalog, config)[0]
    for field in (
        "rows_in",
        "raw_count",
        "pre_quarantined",
        "post_quarantined",
        "facts_appended",
        "rows_merged",
        "snapshot_id",
        "state_read_snapshot_id",
        "co_effect_snapshot_ids",
        "merge_summary",
        "error_type",
        "error_message",
    ):
        assert row[field] is None, field


def test_run_fact_round_trip_all_nullable_fields_populated(tmp_path) -> None:
    config = _sql_config(tmp_path)
    catalog = _bootstrap(config)
    record_run = ledger.build_record_run(catalog, config)

    record_run(
        _run_fact(
            stage="fold",
            outcome="ok",
            rows_in=1,
            raw_count=2,
            pre_quarantined=3,
            post_quarantined=4,
            facts_appended=5,
            rows_merged=6,
            snapshot_id=7,
            state_read_snapshot_id=8,
            co_effect_snapshot_ids={"lookup": 9},
            merge_summary={"added-records": "6"},
            error_type=None,
            error_message=None,
        )
    )

    row = _rows(catalog, config)[0]
    assert row["rows_in"] == 1
    assert row["raw_count"] == 2
    assert row["pre_quarantined"] == 3
    assert row["post_quarantined"] == 4
    assert row["facts_appended"] == 5
    assert row["rows_merged"] == 6
    assert row["snapshot_id"] == 7
    assert row["state_read_snapshot_id"] == 8
    assert dict(row["co_effect_snapshot_ids"]) == {"lookup": 9}
    assert dict(row["merge_summary"]) == {"added-records": "6"}


def test_run_fact_round_trip_error_fields_populated_on_failed_outcome(tmp_path) -> None:
    config = _sql_config(tmp_path)
    catalog = _bootstrap(config)
    record_run = ledger.build_record_run(catalog, config)

    record_run(
        _run_fact(
            stage="commit", outcome="failed", error_type="TransientError", error_message="boom: x"
        )
    )

    row = _rows(catalog, config)[0]
    assert row["error_type"] == "TransientError"
    assert row["error_message"] == "boom: x"
