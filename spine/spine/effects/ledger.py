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
critique F4, bead conveyer-nvh.43; gate corrected, critique F2, bead
conveyer-azr.30):** I-12 [H-2]'s guard-skip read-back subset mismatch is
surfaced as data on the `RunFact` itself (`stage="post_check"`,
`error_message` carrying the short, counts-only drift text, [S-7]) —
`core/run_facts.py::_stage_fields` is what threads `ctx.post_check_drift`
into that field; this module is where the actual WARNING log + EMF
`PostCheckDrift` emission now happens, derived purely from the `RunFact`, in
the SAME unconditional, before-the-append-attempt channel as
`_log_transition`/`_emit_metrics` (stages carry zero instrumentation, §7.3 —
`post_check.py` itself only ever sets the ctx field). Gated on `outcome !=
"failed" and error_message is not None` — an EXACT mirror of the pre_check
gate below, NOT the single `outcome == "skipped-guard"` value the original
(nvh.43) gate used: 005.1 [R2-1] door 2 (`stages/post_check.py`'s own
[DC-1] fact-presence demotion) records drift on an `outcome="ok"`
transition too (the quarantine table's OWN guard was never present there,
so `guard_skips` never accretes `"post_check"`), while door 4 (the
guard-present hash-keyed subtraction, §8.2.4) reaches `outcome=
"skipped-guard"` — both are real drift events this stage may report, and
the original `outcome == "skipped-guard"`-only gate silently swallowed
door 2's WARNING + EMF (the drift still folded into the ledger row's
`error_message` via `core/run_facts.py`, but never alarmed — critique F2's
own finding). A genuinely FAILED `post_check` transition (`failed()`, never
`transition()`) never populates `post_check_drift` in the first place
(`failed()` never calls `_stage_fields`) — the explicit `outcome !=
"failed"` exclusion is defense in depth here, matching pre_check's own
identical reasoning below, not load-bearing on its own.

`recorded_at` (§6.5's "append time" column) is stamped here, with this
module's own wall clock (`datetime.now(UTC)`) — deliberately NOT `fx.now()`:
it is the ledger *write's* own timestamp (`core/run_facts.py`'s docstring),
a value the pure `transition`/`failed` functions cannot know.

**Pre-check drift WARNING + EMF (bead conveyer-azr.18, n3-context-wiring,
005.1 §3.5/A-9):** an EXACT mirror of `_emit_post_check_drift` above, for
`stage="pre_check"` — `core/run_facts.py::_stage_fields` is what threads
`ctx.pre_check_drift` into `RunFact.error_message` for that stage; this
module emits the WARNING log + EMF `PreCheckDrift` off the `RunFact` alone,
in the same unconditional, before-the-append-attempt channel, gated on
`stage == "pre_check" and outcome != "failed" and error_message is not
None` — NOT on a single outcome value the way post_check's `outcome ==
"skipped-guard"` gate is, because 005.1 §6.5's door 2 (the [DC-1]
fact-presence demotion) records drift on an `outcome="ok"` transition too
(a zero-violation rerun with facts already committed takes NO guard-skip
branch at all yet still recomputes the drift probe), while door 3 (the
guard-present subtraction path) reaches `outcome="skipped-guard"`, same as
post_check's one door. The explicit `outcome != "failed"` exclusion matters
because a genuinely FAILED pre_check transition (`run_facts.failed()`, never
`transition()`) ALSO carries a non-`None` `error_message` — the foreign
exception's own location string, never `ctx.pre_check_drift` (`failed()`
never calls `_stage_fields`) — so gating on `error_message is not None`
alone would have wrongly routed that text through this channel too.
`stages/pre_check.py` (bead conveyer-azr.19, n3-admission-cut) is what
actually sets `ctx.pre_check_drift` on its two rerun doors, per §6.5.

**Divergent-duplicates WARNING + EMF (moved here from `stages/commit.py`,
critique gate wf_24a3125f-ecc F1, bead conveyer-6pg.30):** §12's
`DivergentDuplicates` metric (D-2(b)'s observable-data condition, per fact
table) used to be emitted directly by `stages/commit.py` itself, the ONLY
`observability.*` call in any stage (004 §13.3 breach) and outside this
module's never-raise envelope. `stages/commit.py` now only computes the
pure per-table count into `ctx.divergent_duplicates_by_table`; this module
derives the WARNING + EMF purely from the `RunFact`, gated on `stage ==
"commit" and divergent_duplicates_by_table is not None`, the same shape as
`delta_probe_refusal`'s own gate. See `_emit_divergent_duplicates`'s own
docstring for why the EMF (per table, unconditional) and the WARNING log
(per table, count > 0 only) deliberately use different gates from each
other.
"""

from __future__ import annotations

import dataclasses
import logging
import random
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

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
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    TimestamptzType,
)

from spine import observability
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
    # M2 (bead conveyer-swb.25): one column, two meanings keyed by `stage` --
    # `stage="rebuild"` rows carry the pinned FACT-table snapshot id for that
    # attempt (`effects/rebuild.py::_rebuild_attempt_fact`); `stage="fold"`
    # rows carry permanently `None` (`core/run_facts.py::_stage_fields`'s own
    # `fold` branch). Interim shape, owed a purpose-built vocabulary row in
    # 004.1's own rebuild stage-vocabulary accretion (007.1 §16) -- see
    # `core/run_facts.py::RunFact.state_read_snapshot_id`'s own matching note.
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
    # 007.1 §4.2/§12 (errata item 20, bead conveyer-6pg.21, B9b): the per-
    # table maps, additive-only (L-4) -- new field ids only, 1-27 untouched.
    NestedField(
        28,
        "facts_appended_by_table",
        MapType(29, StringType(), 30, LongType(), value_required=True),
        required=False,
    ),
    NestedField(
        31,
        "snapshot_ids_by_table",
        MapType(32, StringType(), 33, LongType(), value_required=True),
        required=False,
    ),
    NestedField(
        34,
        "rows_merged_by_table",
        MapType(35, StringType(), 36, LongType(), value_required=True),
        required=False,
    ),
    NestedField(
        37,
        "delta_predecessor_batch_ids",
        ListType(38, StringType(), element_required=True),
        required=False,
    ),
    NestedField(
        39,
        "delta_read_snapshot_ids",
        MapType(40, StringType(), 41, LongType(), value_required=True),
        required=False,
    ),
    NestedField(42, "delta_probe_refusal", StringType(), required=False),
    # critique gate wf_24a3125f-ecc F1 (bead conveyer-6pg.30): additive-only
    # (L-4) -- new field ids only, 1-42 untouched. Moved commit's own
    # `DivergentDuplicates` metric onto this per-table map so `record_run`
    # (not `stages/commit.py`) derives the WARNING+EMF, mirroring
    # `delta_read_snapshot_ids`'s own path.
    NestedField(
        43,
        "divergent_duplicates_by_table",
        MapType(44, StringType(), 45, LongType(), value_required=True),
        required=False,
    ),
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


class LedgerConfig(Protocol):
    """The SIX fields `build_catalog`/`_identifier`/`build_record_run`
    genuinely read (M3, bead conveyer-swb.25) -- narrowed from `spine.
    config.RunnerConfig`'s full 18-field shape, which this module's own
    functions used to be typed against even though most of it (seed/SFN/
    SLA/event-bus/landing-bucket fields) is never touched here. `RunnerConfig`
    satisfies this Protocol structurally, unchanged -- every existing
    production/test call site keeps passing a `RunnerConfig` with zero
    changes. `entrypoints/rebuild_main.py::RebuildConfig` (its OWN minimal
    argv contract, with no seed/SFN/SLA fields to fabricate) ALSO satisfies
    it structurally, with no adapter -- this is what lets `rebuild_main.
    main` delete its own `_as_runner_config` fabrication entirely and pass
    its `RebuildConfig` straight through.

    Declared as read-only `@property` members (not plain annotations):
    both concrete configs are FROZEN dataclasses, whose fields mypy treats
    as read-only -- a plain-annotation `Protocol` member defaults to
    read-WRITE, which a frozen dataclass structurally fails (`expected
    settable variable, got read-only attribute`)."""

    @property
    def aws_region(self) -> str: ...
    @property
    def ledger_catalog_kind(self) -> Literal["glue", "sql"]: ...
    @property
    def ledger_sql_uri(self) -> str | None: ...  # SqlCatalog, tests only
    @property
    def warehouse_uri(self) -> str | None: ...  # SqlCatalog's own warehouse, tests only
    @property
    def spine_db(self) -> str: ...
    @property
    def run_ledger_table(self) -> str: ...


def build_catalog(config: LedgerConfig) -> Catalog:
    """Build the pyiceberg `Catalog` named by `config.ledger_catalog_kind`
    (I-2's driver-side pyiceberg substrate) -- shared between
    `build_record_run` and `bootstrap/create_run_ledger.py`'s CLI entrypoint
    so both build the identical catalog for a given config.

    `sql` (tests only): requires both `ledger_sql_uri` and `warehouse_uri`
    (SQLite + a local FS warehouse, D-7's pattern).

    `glue` (prod): constructed with an explicit region (`config.aws_region`)
    and deliberately **no** `warehouse` property -- recorded assumption:
    `RunnerConfig.warehouse_uri` (`LedgerConfig`'s own field, below) is
    documented (`config.py`) as "hadoop only (tests)" for the *Spark*
    data-path catalog (I-2), so this module does not reuse it for the
    ledger's own Glue catalog; new tables under
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


def _identifier(config: LedgerConfig) -> str:
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
    rerun's read-back-vs-recomputed count mismatch on either door 2
    (`outcome="ok"`) or door 4 (`outcome="skipped-guard"`, critique F2) --
    WARNING + EMF `PostCheckDrift`, derived purely from `run_fact.
    error_message` (already the short, counts-only drift text `post_check.py`
    set on `ctx.post_check_drift`, [S-7]) -- never invoked directly on a raw
    exception, so there is no row-value leakage risk to guard against here
    the way `_log_ledger_loss` does. Message text deliberately does not name
    a specific door (unlike the docstring above) -- door 2 is not a
    guard-skip rerun at all, so "guard-skip rerun" would misdescribe it."""
    logger = logging.getLogger(_LOGGER_NAME)
    logger.warning(
        "post_check drift (I-12 [H-2]): %s",
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


def _emit_pre_check_drift(run_fact: RunFact) -> None:
    """005.1 A-9/§3.5: an exact mirror of `_emit_post_check_drift` above, for
    `stage="pre_check"` — WARNING + EMF `PreCheckDrift`, derived purely from
    `run_fact.error_message` (already the short, counts-only drift text
    `stages/pre_check.py` sets on `ctx.pre_check_drift`, [S-7])."""
    logger = logging.getLogger(_LOGGER_NAME)
    logger.warning(
        "pre_check drift (005.1 A-9): %s",
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
        "PreCheckDrift", 1, run_fact.pipeline, run_fact.feed_id, stage=run_fact.stage
    )


def _emit_delta_probe_refusal(run_fact: RunFact) -> None:
    """007.1 §7.2/§12 (F-5, B9b): an exact mirror of `_emit_post_check_drift`
    / `_emit_pre_check_drift` above, for commit's own `delta_probe_refusal`
    -- WARNING + EMF `DeltaProbeRefusals`, reason-dimensioned (ADR-OQ2's four
    codes, verbatim). `run_fact.delta_probe_refusal` is already the value-
    free payload ([S-7]/[S-18]: the reason code IS the entire payload, per
    `stages/commit.py`'s own `resolve_predecessors` call -- no `delivery_
    key`/hash/batch content ever reaches this channel, by construction of
    what `ctx.delta_probe_refusal` is ever set to)."""
    logger = logging.getLogger(_LOGGER_NAME)
    logger.warning(
        "delta probe refusal (007.1 §7.2, F-5): %s",
        run_fact.delta_probe_refusal,
        extra={
            "batch_id": run_fact.batch_id,
            "pipeline": run_fact.pipeline,
            "feed_id": run_fact.feed_id,
            "attempt_id": run_fact.attempt_id,
            "stage": run_fact.stage,
        },
    )
    observability.emit_metric(
        "DeltaProbeRefusals",
        1,
        run_fact.pipeline,
        run_fact.feed_id,
        stage=run_fact.stage,
        extra_dims={"reason": run_fact.delta_probe_refusal or ""},
    )


def _emit_divergent_duplicates(run_fact: RunFact) -> None:
    """007.1 §12 (D-2(b)) -- WARNING + EMF `DivergentDuplicates`, table-
    dimensioned, mirroring `_emit_delta_probe_refusal` above. Moved here from
    `stages/commit.py`'s own naked `observability.emit_metric` call (critique
    gate wf_24a3125f-ecc F1, bead conveyer-6pg.30): that call was the ONLY
    `observability.*` reference in any `stages/*.py` module (004 §13.3 breach)
    and sat outside `record_run`'s never-raise envelope, so a broken metrics
    sink could fail `commit` itself. `run_fact.divergent_duplicates_by_table`
    is already the value-free payload ([S-7]/[S-18]: table names and counts
    only, never row content) -- `stages/commit.py` now only computes the pure
    per-table count and records it into the context.

    **EMF emitted for every table entry in the map, unconditionally
    (including a zero count)** -- an exact continuation of the pre-fix
    stage-side cadence (the metric was emitted for every non-guard-skipped,
    structurally-valid table reached, regardless of value), preserving
    CloudWatch's continuous per-batch datapoint for this "the metric is the
    page" symptomatic signal (§12's own words). **The WARNING log, by
    contrast, only fires for a table whose count is actually positive** -- a
    deliberate, documented deviation from the drift channels' "gate the
    whole emission" shape above: those channels' own `RunFact` field is only
    ever non-`None` when something anomalous happened, so gating on presence
    alone is correct there, but `divergent_duplicates_by_table` carries an
    entry for every reached table on every healthy commit -- warning on
    every one of them would make WARNING-level logs noise, not signal.
    """
    logger = logging.getLogger(_LOGGER_NAME)
    for table, count in (run_fact.divergent_duplicates_by_table or {}).items():
        if count > 0:
            logger.warning(
                "divergent duplicates (007.1 §12, D-2(b)): table=%s count=%d",
                table,
                count,
                extra={
                    "batch_id": run_fact.batch_id,
                    "pipeline": run_fact.pipeline,
                    "feed_id": run_fact.feed_id,
                    "attempt_id": run_fact.attempt_id,
                    "stage": run_fact.stage,
                },
            )
        observability.emit_metric(
            "DivergentDuplicates",
            count,
            run_fact.pipeline,
            run_fact.feed_id,
            stage=run_fact.stage,
            extra_dims={"table": table},
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
    catalog_factory_or_catalog: Catalog | Callable[[], Catalog], config: LedgerConfig
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
            # critique F2 (bead conveyer-azr.30): gated on outcome != "failed",
            # an EXACT mirror of pre_check's own gate below (was outcome ==
            # "skipped-guard" only, which silently dropped door 2's WARNING +
            # EMF -- see this module's own docstring for the full account).
            if (
                run_fact.stage == "post_check"
                and run_fact.outcome != "failed"
                and run_fact.error_message is not None
            ):
                _emit_post_check_drift(run_fact)
            # 005.1 A-9: gated on outcome != "failed", the SAME shape as
            # post_check's own gate above -- pre_check's own [DC-1] door 2
            # (fact-presence demotion, §6.5) recomputes the drift probe on
            # an outcome="ok" transition (the quarantine table's OWN guard
            # was never present, so `guard_skips` never accretes
            # "pre_check" there), while door 3 (guard-present subtraction)
            # reaches this branch with outcome="skipped-guard" -- both are
            # real drift events this stage may report. A genuinely FAILED
            # pre_check transition also carries a non-None `error_message`
            # (`_error_message(exc)`, via `failed()`, never `ctx.
            # pre_check_drift`) -- excluding "failed" explicitly keeps that
            # foreign text out of this channel.
            if (
                run_fact.stage == "pre_check"
                and run_fact.outcome != "failed"
                and run_fact.error_message is not None
            ):
                _emit_pre_check_drift(run_fact)
            # 007.1 §7.2/§12 (F-5, B9b): gated on `stage == "commit"` alone
            # (never `outcome != "failed"` -- unlike the two drift channels
            # above, `ctx.delta_probe_refusal` is set by `stages/commit.py`
            # itself on its OWN non-raising path; a genuinely FAILED commit
            # transition never reaches `_stage_fields`, so `run_fact.
            # delta_probe_refusal` is `None` there regardless -- the
            # `is not None` check alone already excludes it, matching
            # `outcome == "failed"`'s own effect without a redundant clause).
            if run_fact.stage == "commit" and run_fact.delta_probe_refusal is not None:
                _emit_delta_probe_refusal(run_fact)
            # critique gate wf_24a3125f-ecc F1 (bead conveyer-6pg.30): gated
            # on `stage == "commit"` and the map being present at all (a
            # genuinely FAILED commit transition never reaches
            # `_stage_fields`, so `divergent_duplicates_by_table` is `None`
            # there regardless -- the `is not None` check alone already
            # excludes it, same reasoning as `delta_probe_refusal`'s own
            # gate immediately above).
            if run_fact.stage == "commit" and run_fact.divergent_duplicates_by_table is not None:
                _emit_divergent_duplicates(run_fact)
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
