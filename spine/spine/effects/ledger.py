"""Run-ledger append (pyiceberg, injectable catalog) — best-effort, time-boxed. LLD §7.6.

`build_record_run(catalog_factory_or_catalog, config) -> record_run` builds
the `RunnerFx.record_run` closure. `catalog_factory_or_catalog` is either a
already-built pyiceberg `Catalog` (tests: build once, reuse) or a zero-arg
`Callable[[], Catalog]` (prod: `effects/build.py` can defer/rebuild catalog
construction per call) — distinguished by `callable(...)`, since a pyiceberg
`Catalog` instance is never itself callable.

The Iceberg schema / partition spec / table properties are defined here (not
in `bootstrap/create_run_ledger.py`) for the same reason ingestion's
`effects/ledger.py` keeps its own ledger's shape alongside `append`: the
pyarrow conversion on the hot append path must use the EXACT same shape the
bootstrap script created the table with, and one authoritative definition is
how that never drifts. `bootstrap/create_run_ledger.py` imports the three
constants plus `build_catalog` from here rather than redeclaring them.

Channel ordering [C-6, E-15] (§7.3, §7.6, §11.3): `record_run` logs the INFO
transition line and emits EMF metrics **first, unconditionally**; only then
does it attempt the pyiceberg append, under a **time-boxed budget: 2
attempts, <= 2 s total backoff** — with only 2 attempts there is exactly one
gap between them, so "<= 2 s total" is a single bounded sleep, not a per-
attempt allowance (unlike the 002.1 ledger's 5x/8s policy, which backs a
system of record; this channel deliberately is not one, §7.6). On exhaustion:
log the row at WARNING with `error_message` **omitted** ([S-7] — this table,
and this WARNING line, are both indefinitely retained and Athena/CloudWatch
-queried; exception text routinely embeds row values) plus EMF
`RunLedgerLoss`, then **return**. `record_run` catches everything end to end
(§11.3) — it is a fire-and-forget effect from `run.py` alone and must never
raise, regardless of what the catalog, the logger, or the metrics sink do.
**Row DERIVATION is inside the same guarded section as the append attempt**
(a fix, conveyer-nvh.36): a failure building the row itself (e.g. a
Mapping-typed field that raises on `dict(...)` conversion) is treated
exactly like an exhausted append budget — it still reaches the WARNING +
`RunLedgerLoss` path, never the silent, unaccounted loss a bare "derivation
raised, outer catch ate it" shape would produce. The one remaining bare
`except: pass` (around `_log_transition`/`_emit_metrics`/the post_check-drift
emission below, and around the loss-logging call itself) is the absolute
last resort: it exists only to protect against the logger/metrics sink or
the loss-accounting call itself being broken, not to hide a row-derivation
or append failure.

**Lifecycle honesty metrics (S11.1/S11.4, bead conveyer-nvh.47):**
`JobAttempts`/`BatchesStarted`/`BatchesCompleted` are the three §11.1-named
metrics no code emitted before this fix (the s11.4 `job_attempts` alarm,
`modules/spine-pipeline/monitoring.tf`, referenced `JobAttempts` against a
metric nothing ever wrote). Derived the same way `_stage_metrics` derives
everything else -- purely from one `RunFact`, no new state: `JobAttempts`
(dims `pipeline`/`feed_id` only, matching the alarm's own `SCHEMA(...)`
SELECT with no `stage` grouping) fires on **every `land` transition,
regardless of outcome** -- an attempt happened even if `land` itself failed,
and this is the storm-throttle signal [S-13], not a success counter.
`BatchesStarted` fires on `land` with outcome `ok` **or** `skipped-guard`
only (a batch that genuinely began, whether freshly landed or already
present on a rerun) -- never on a failed `land`. `BatchesCompleted` fires on
`publish` with outcome `ok` (the only outcome `publish` -- itself never
guard-skipped, I-7's unconditional emit -- can reach via `transition()`;
`failed()` never calls `_stage_fields()` so a genuinely failed attempt never
reaches this branch either). `SingleFlightCollisions` (the fourth §11.1
metric with no `_stage_metrics` home) is a `router.py`-only concern (a
collision is detected before a Glue job -- and therefore any `RunFact` --
ever exists) and is out of this module's scope entirely; `test_ledger.py`'s
own "every alarm-referenced metric has an emitter" test scopes its
assertion to the two files that between them cover every §11.1 metric
(this module + `router.py`), not to this module alone.

**Post-check drift WARNING + EMF (moved here from `stages/post_check.py`,
critique F4, bead conveyer-nvh.43):** I-12 [H-2]'s guard-skip read-back
subset mismatch is surfaced as data on the `RunFact` itself
(`stage="post_check"`, `outcome="skipped-guard"`, `error_message` carrying
the short, counts-only drift text, [S-7]) — `core/run_facts.py::
_stage_fields` is what threads `ctx.post_check_drift` into that field; this
module is where the actual WARNING log + EMF `PostCheckDrift` emission now
happens, derived purely from the `RunFact`, in the SAME unconditional,
before-the-append-attempt channel as `_log_transition`/`_emit_metrics`
(stages carry zero instrumentation, §7.3 — `post_check.py` itself only ever
sets the ctx field). Guarded on `outcome == "skipped-guard"`, not merely
`error_message is not None`: a genuinely FAILED `post_check` transition
(recorded via `failed()`, never `transition()`) never populates
`post_check_drift` in the first place, but gating on the outcome too keeps
this emission's precondition self-documenting and independent of that other
function's own behavior.

`recorded_at` (§6.5's "append time" column) is stamped here, with this
module's own wall clock (`datetime.now(UTC)`) — deliberately NOT `fx.now()`:
it is the ledger *write's* own timestamp (`core/run_facts.py`'s docstring),
a value the pure `transition`/`failed` functions cannot know.
"""

from __future__ import annotations

import dataclasses
import logging
import random
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io import AWS_REGION
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    IntegerType,
    LongType,
    MapType,
    NestedField,
    StringType,
    TimestamptzType,
)

from spine import observability
from spine.config import RunnerConfig
from spine.core.run_facts import RunFact

_LOGGER_NAME = "spine.ledger"

# --- §6.5: pyiceberg schema (field ids implicit by declaration order -- see
# ingestion's own equivalent module docstring: pyiceberg's
# `create_table_if_not_exists` reassigns every field id by declaration order
# at creation time, so the numbers below are documentation, not load-bearing).
RUN_LEDGER_SCHEMA = Schema(
    NestedField(1, "batch_id", StringType(), required=True),
    NestedField(2, "pipeline", StringType(), required=True),
    NestedField(3, "feed_id", StringType(), required=True),
    NestedField(4, "attempt_id", StringType(), required=True),
    NestedField(5, "sfn_retry_count", IntegerType(), required=True),
    NestedField(6, "sfn_redrive_count", IntegerType(), required=True),
    NestedField(7, "stage", StringType(), required=True),
    NestedField(8, "outcome", StringType(), required=True),
    NestedField(9, "started_at", TimestamptzType(), required=True),
    NestedField(10, "finished_at", TimestamptzType(), required=True),
    NestedField(11, "rows_in", LongType(), required=False),
    NestedField(12, "raw_count", LongType(), required=False),
    NestedField(13, "pre_quarantined", LongType(), required=False),
    NestedField(14, "post_quarantined", LongType(), required=False),
    NestedField(15, "facts_appended", LongType(), required=False),
    NestedField(16, "rows_merged", LongType(), required=False),
    NestedField(17, "snapshot_id", LongType(), required=False),
    NestedField(18, "state_read_snapshot_id", LongType(), required=False),
    NestedField(
        19,
        "co_effect_snapshot_ids",
        MapType(20, StringType(), 21, LongType(), value_required=True),
        required=False,
    ),
    NestedField(
        22,
        "merge_summary",
        MapType(23, StringType(), 24, StringType(), value_required=True),
        required=False,
    ),
    NestedField(25, "error_type", StringType(), required=False),
    NestedField(26, "error_message", StringType(), required=False),
    NestedField(27, "recorded_at", TimestamptzType(), required=True),
)

# day(started_at), §6.5.
RUN_LEDGER_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=9, field_id=1000, transform=DayTransform(), name="started_at_day"),
)

# I-17: 30 d vacuum window, keep >= 5 snapshots (the Athena-property trap
# 002.1 §9.4 documents), plus the two metadata-accretion properties per-
# transition pyiceberg commits need [T-18] (VACUUM alone does not remove
# metadata.json files).
RUN_LEDGER_TABLE_PROPERTIES = {
    "vacuum_max_snapshot_age_seconds": "2592000",
    "vacuum_min_snapshots_to_keep": "5",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
}

_CATALOG_NAME = "conveyer-spine-ledger"
_MAX_ATTEMPTS = 2  # [C-6]
_MAX_BACKOFF_S = 2.0  # [C-6]: TOTAL backoff budget across all gaps (one gap, here)


def build_catalog(config: RunnerConfig) -> Catalog:
    """Build the pyiceberg `Catalog` named by `config.ledger_catalog_kind`
    (I-2's driver-side pyiceberg substrate) -- shared between
    `build_record_run` and `bootstrap/create_run_ledger.py`'s CLI entrypoint
    so both build the identical catalog for a given config.

    `sql` (tests only): requires both `ledger_sql_uri` and `warehouse_uri`
    (SQLite + a local FS warehouse, D-7's pattern).

    `glue` (prod): constructed with an explicit region (`config.aws_region`)
    and deliberately **no** `warehouse` property -- recorded assumption:
    `RunnerConfig.warehouse_uri` is documented (`config.py`) as "hadoop only
    (tests)" for the *Spark* data-path catalog (I-2), so this module does not
    reuse it for the ledger's own Glue catalog; new tables under
    `config.spine_db` take their location from that Glue database's own
    `LocationUri` (Terraform-managed, `${p}-lake/spine/`), standard AWS Glue
    Catalog behavior. Unlike ingestion's `effects/ledger.py::build_catalog`,
    this path needs no untestable branch: `GlueCatalog.__init__` only builds
    a boto3 client (no network call), so it is fully exercised by a plain
    unit test (no `moto[glue]` needed) -- verified live.
    """
    if config.ledger_catalog_kind == "sql":
        if config.ledger_sql_uri is None or config.warehouse_uri is None:
            raise ValueError(
                "ledger_catalog_kind='sql' requires both ledger_sql_uri and warehouse_uri"
            )
        return SqlCatalog(_CATALOG_NAME, uri=config.ledger_sql_uri, warehouse=config.warehouse_uri)
    return GlueCatalog(_CATALOG_NAME, **{AWS_REGION: config.aws_region})


def _identifier(config: RunnerConfig) -> str:
    return f"{config.spine_db}.{config.run_ledger_table}"


def _row_from_run_fact(run_fact: RunFact, recorded_at: datetime) -> dict[str, Any]:
    """`RunFact` (a frozen dataclass, §6.5's columns minus `recorded_at`)
    plus the ledger's own append-time stamp -- the row this module writes,
    field-for-field identical to `RUN_LEDGER_SCHEMA`.

    Built via explicit field access (`dataclasses.fields` + `getattr`), NOT
    `dataclasses.asdict` -- `asdict`'s generic branch `copy.deepcopy`s any
    field that is not itself a dataclass/list/tuple/dict, and
    `types.MappingProxyType` (`stages/pull.py`'s own, deliberate,
    `context.py`-documented wrapping of `co_effect_snapshot_ids`)
    unconditionally fails `copy.deepcopy` -- verified even for an EMPTY
    mapping proxy, a CPython-level limitation, not a content-dependent bug.
    Each field is copied as-is except a bare shallow `dict(...)` for any
    `Mapping`-typed value (covers `MappingProxyType` and, defensively, any
    plain-`dict` field too) -- correct because every Mapping field here
    (`co_effect_snapshot_ids`, `merge_summary`) holds only flat str/int
    values, never a nested structure `asdict`'s recursive walk would have
    needed for. `BatchContext`/`stages/pull.py` are unchanged by this fix --
    §6.3's `MappingProxyType`-by-convention immutability stays exactly as
    documented; only the ledger's OWN row-derivation stops relying on
    `asdict`'s deepcopy semantics."""
    row: dict[str, Any] = {}
    for field in dataclasses.fields(run_fact):
        value = getattr(run_fact, field.name)
        row[field.name] = dict(value) if isinstance(value, Mapping) else value
    row["recorded_at"] = recorded_at
    return row


def _log_transition(run_fact: RunFact) -> None:
    """§7.3/§11.2: one INFO line per stage transition, emitted before the
    ledger append is even attempted."""
    logger = logging.getLogger(_LOGGER_NAME)
    logger.info(
        "stage transition: stage=%s outcome=%s",
        run_fact.stage,
        run_fact.outcome,
        extra={
            "batch_id": run_fact.batch_id,
            "pipeline": run_fact.pipeline,
            "feed_id": run_fact.feed_id,
            "attempt_id": run_fact.attempt_id,
            "stage": run_fact.stage,
        },
    )


def _stage_metrics(run_fact: RunFact) -> tuple[tuple[str, float, str | None], ...]:
    """§11.1's metric list, restricted to the ones a single stage transition
    can derive: always `StageSeconds`; the rest conditional on which count
    field THIS stage's `RunFact` populated (§6.5: "only the fields this
    stage produced"). Pure and independently testable from the I/O emitter
    below. Third element is the optional `stage` dimension."""
    duration = (run_fact.finished_at - run_fact.started_at).total_seconds()
    metrics: list[tuple[str, float, str | None]] = [("StageSeconds", duration, run_fact.stage)]
    if run_fact.raw_count is not None:
        metrics.append(("RawRows", float(run_fact.raw_count), None))
    if run_fact.pre_quarantined is not None:
        metrics.append(("QuarantinedRows", float(run_fact.pre_quarantined), run_fact.stage))
    if run_fact.post_quarantined is not None:
        metrics.append(("QuarantinedRows", float(run_fact.post_quarantined), run_fact.stage))
    if run_fact.facts_appended is not None:
        metrics.append(("FactsAppended", float(run_fact.facts_appended), None))
    if run_fact.rows_merged is not None:
        metrics.append(("RowsMerged", float(run_fact.rows_merged), None))
    if run_fact.outcome == "skipped-guard":
        metrics.append(("GuardSkips", 1.0, run_fact.stage))
    return tuple(metrics)


def _emit_metrics(run_fact: RunFact) -> None:
    for name, value, stage in _stage_metrics(run_fact):
        unit = "Seconds" if name == "StageSeconds" else "Count"
        observability.emit_metric(
            name, value, run_fact.pipeline, run_fact.feed_id, stage=stage, unit=unit
        )
    for name, value in _lifecycle_metrics(run_fact):
        observability.emit_metric(name, value, run_fact.pipeline, run_fact.feed_id, stage=None)


def _lifecycle_metrics(run_fact: RunFact) -> tuple[tuple[str, float], ...]:
    """§11.1's three batch/attempt-lifecycle metrics, dims `pipeline`/
    `feed_id` only (no `stage` -- see module docstring): `JobAttempts` on
    every `land` transition regardless of outcome (the s11.4 storm-throttle
    signal [S-13]); `BatchesStarted` on `land` with outcome `ok` or
    `skipped-guard` (never `failed`); `BatchesCompleted` on `publish` with
    outcome `ok` (the only outcome a non-failed `publish` transition can
    have). Pure and independently testable, same shape as `_stage_metrics`."""
    metrics: list[tuple[str, float]] = []
    if run_fact.stage == "land":
        metrics.append(("JobAttempts", 1.0))
        if run_fact.outcome in ("ok", "skipped-guard"):
            metrics.append(("BatchesStarted", 1.0))
    if run_fact.stage == "publish" and run_fact.outcome == "ok":
        metrics.append(("BatchesCompleted", 1.0))
    return tuple(metrics)


def _emit_post_check_drift(run_fact: RunFact) -> None:
    """I-12 [H-2] (moved here from `stages/post_check.py`, critique F4): a
    guard-skip rerun's read-back-vs-recomputed count mismatch, WARNING + EMF
    `PostCheckDrift`, derived purely from `run_fact.error_message` (already
    the short, counts-only drift text `post_check.py` set on `ctx.
    post_check_drift`, [S-7]) -- never invoked directly on a raw exception,
    so there is no row-value leakage risk to guard against here the way
    `_log_ledger_loss` does."""
    logger = logging.getLogger(_LOGGER_NAME)
    logger.warning(
        "post_check drift on guard-skip rerun (I-12 [H-2]): %s",
        run_fact.error_message,
        extra={
            "batch_id": run_fact.batch_id,
            "pipeline": run_fact.pipeline,
            "feed_id": run_fact.feed_id,
            "attempt_id": run_fact.attempt_id,
            "stage": run_fact.stage,
        },
    )
    observability.emit_metric(
        "PostCheckDrift", 1, run_fact.pipeline, run_fact.feed_id, stage=run_fact.stage
    )


def _log_ledger_loss(run_fact: RunFact, row: Mapping[str, Any]) -> None:
    """On budget exhaustion: WARNING with the row, `error_message` OMITTED
    [S-7] -- this channel's own WARNING line is exactly as indefinitely
    retained (CloudWatch) as the ledger table it failed to write to."""
    logger = logging.getLogger(_LOGGER_NAME)
    row_without_error_message = {k: v for k, v in row.items() if k != "error_message"}
    logger.warning(
        "run-ledger append lost after %d attempts: %s",
        _MAX_ATTEMPTS,
        row_without_error_message,
        extra={
            "batch_id": run_fact.batch_id,
            "pipeline": run_fact.pipeline,
            "feed_id": run_fact.feed_id,
            "attempt_id": run_fact.attempt_id,
            "stage": run_fact.stage,
        },
    )
    observability.emit_metric(
        "RunLedgerLoss", 1.0, run_fact.pipeline, run_fact.feed_id, stage=run_fact.stage
    )


def _try_append(
    get_catalog: Callable[[], Catalog], identifier: str, row: Mapping[str, Any]
) -> bool:
    """Exactly `_MAX_ATTEMPTS` tries, one bounded backoff sleep between them
    (`_MAX_BACKOFF_S` total, [C-6]) -- returns whether the append landed.
    Every step (catalog resolution, table load, schema derive, append) is
    inside the same `try`: any exception counts as this attempt's failure,
    since this channel must degrade to "try again" or "give up", never
    raise."""
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            catalog = get_catalog()
            table = catalog.load_table(identifier)
            pa_schema = schema_to_pyarrow(table.schema())
            arrow_row = pa.Table.from_pylist([dict(row)], schema=pa_schema)
            table.append(arrow_row)
            return True
        except Exception:  # noqa: BLE001 -- deliberately broad, see docstring
            if attempt < _MAX_ATTEMPTS:
                time.sleep(random.uniform(0.0, _MAX_BACKOFF_S))
    return False


def build_record_run(
    catalog_factory_or_catalog: Catalog | Callable[[], Catalog], config: RunnerConfig
) -> Callable[[RunFact], None]:
    identifier = _identifier(config)

    def get_catalog() -> Catalog:
        if callable(catalog_factory_or_catalog):
            return catalog_factory_or_catalog()
        return catalog_factory_or_catalog

    def record_run(run_fact: RunFact) -> None:
        try:
            _log_transition(run_fact)
            _emit_metrics(run_fact)
            if run_fact.stage == "post_check" and run_fact.outcome == "skipped-guard":
                if run_fact.error_message is not None:
                    _emit_post_check_drift(run_fact)
        except Exception:  # noqa: BLE001 -- §11.3: record_run NEVER raises
            pass

        # Failure topology: everything between "have a RunFact" and "append
        # succeeded" -- row DERIVATION included, not just the append itself
        # -- is one guarded section. A derivation failure (e.g. a poisoned
        # Mapping field) must land in the SAME WARNING + RunLedgerLoss path
        # as an exhausted append budget, never be swallowed silently before
        # it. `row` defaults to `{}` so a mid-derivation failure still logs
        # a (best-effort, possibly empty) loss row rather than needing one.
        row: Mapping[str, Any] = {}
        appended = False
        try:
            row = _row_from_run_fact(run_fact, datetime.now(UTC))
            appended = _try_append(get_catalog, identifier, row)
        except Exception:  # noqa: BLE001 -- row derivation OR append raised: a loss, not silence
            appended = False

        if not appended:
            try:
                _log_ledger_loss(run_fact, row)
            except Exception:  # noqa: BLE001 -- §11.3: absolute last resort (loss-logging itself)
                pass

    return record_run
