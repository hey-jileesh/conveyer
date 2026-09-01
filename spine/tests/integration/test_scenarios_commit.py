"""K-suite (record side) — `stages/commit.py`'s F-4/F-5/F-8/F-9 wiring. LLD
007.1 §13.1 K-04…K-13, §13.2 K-20/K-21. Bead `conveyer-6pg.21`, B9b.

Exercises the FRAMEWORK WIRING B9b lands: real `RunnerFx` marker read/write
effects (`effects/spark.py`), `core/delta.py::resolve_predecessors` fed by
those real reads, `frames/delta.py::delta_filter` fed by the framework's own
predecessor-partition projection, and `stages/commit.py`'s F-9 marker
mechanics + F-8 backstop — all via `local_runner_fx` (the real production
`RunnerFx` assembly) and real bootstrap DDL (`bootstrap.create_record_
tables`, via `scenario_helpers.create_fact_table`/`create_state_table`/
`create_markers_table`, B9b addendum 1's own DDL-parity swap). Most tests
call `stages.commit.run(ctx, fx)` DIRECTLY with a hand-built `ctx.admitted_
facts` — bypassing `land`/`pre_check`/`pull`/`apply`/`post_check` entirely,
the same "start mid-sequence" shape `test_stages_post_commit_fold_publish.
py`'s own (currently broken, out of scope) module docstring already used.

**`naming.markers_table` trap, found empirically writing this file.** It
derives `<db>.<slug>__markers` from ONLY the raw table's own DB component
plus the pipeline's `table_slug` (its trailing `/`-segment) — NOT from any
part of the raw table's own identifying name. Two tests sharing one literal
`pipeline` string would therefore derive and SHARE one marker table
(cross-test marker-row pollution); `_provision`/`_provision_2type` below
give every test its own unique `pipeline` slug and create the markers table
at its real derived name (`naming.markers_table`/`naming.qualified`) rather
than at an independently-`unique_table`'d one `stages/commit.py`'s own
lookup would never find.

**What this file does NOT re-derive (cited, not restated):**
- K-04 (the [DC-3] tying test at commit's own call site) is `tests/frames/
  test_facts.py`'s own obligation (B9a) — green already, unaffected by B9b.
- `resolve_predecessors`'s own pure decision matrix over all of §7.2's paths
  1-9, INCLUDING `target-incoherent`/`target-unmarked`/`horizon-exceeded`
  (Track E's own three probe-refusal reasons, unreachable through `stages/
  commit.py` in Phase 1 — no seed carries `supersedes_batch_id` yet, the
  `conveyer-kof` named wait) is `tests/unit/test_delta.py`'s own obligation
  (B6) — this file only exercises the ONE probe-refusal reason reachable
  through the real wiring today, `none-with-key-match` (K-06), plus the
  read-1 disagreement golden (K-07), both via REAL marker reads/writes.
- `delta_filter`'s own pure fail-open path matrix (paths 1-9 at the filter's
  OWN grain) is `tests/frames/test_frames_delta.py`'s own obligation (B9a)
  — this file's K-05 tests exercise the FULL wiring (a real two-batch
  commit sequence through real marker reads + the real predecessor-
  partition projection), not the filter's pure decision table again.

K-05's `None`-race fixture for §7.2 path 5 rides `conveyer-kof` (the named
wait, §16) — SKIPPED here with reason, per this bead's own DONE bar.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING

import killfx
import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from scenario_helpers import IDENTITY_FACT_SCHEMA as _IDENTITY_FACT_SCHEMA
from scenario_helpers import IDENTITY_RAW_CONTRACT as _IDENTITY_RAW_CONTRACT
from scenario_helpers import IDENTITY_READ as _IDENTITY_READ
from scenario_helpers import bare as _bare
from scenario_helpers import batch_id as _batch_id
from scenario_helpers import create_fact_table as _create_fact_table
from scenario_helpers import create_markers_table as _create_markers_table
from scenario_helpers import create_state_table as _create_state_table
from spine.binding import Transforms
from spine.config import RunConfig
from spine.context import BatchContext
from spine.core import naming
from spine.core.checks import checks_version
from spine.core.contract import check_version, read_spec_version
from spine.core.delta import MarkerRowWrite, SeedAttrs, resolve_predecessors
from spine.core.model import FactColumnSpec, FactSchemaModel, FactTypeModel, PipelineSpecModel
from spine.effects.records import RunnerFx
from spine.stages import commit as commit_stage

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyspark.sql import DataFrame

_FEED_ID = "feed/identity"
_T1 = datetime(2026, 1, 1, tzinfo=UTC)
_T2 = datetime(2026, 1, 2, tzinfo=UTC)
_T3 = datetime(2026, 1, 3, tzinfo=UTC)

_SHIPMENTS_SCHEMA = FactSchemaModel(
    columns=[
        FactColumnSpec(name="domain_id", type="string"),
        FactColumnSpec(name="shipped_at", type="string"),
        FactColumnSpec(name="payload", type="string"),
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
)


def _unique_pipeline(name: str) -> str:
    # No "-" separator (unlike a typical slug-uniquifier): `table_slug`'s
    # own output (this segment, trailing/-split) composes a RAW SQL
    # identifier component downstream (`naming.markers_table`), which
    # `_IDENTIFIER_RE` (`[A-Za-z_][A-Za-z0-9_]*`) forbids a dash in --
    # found empirically writing this file (`check_qualified_table` rejects
    # a dash-bearing markers-table name even though the SAME string is a
    # grammar-conforming pipeline segment on its own, `_PIPELINE_SEGMENT`
    # permitting single dashes). Alphanumeric-only keeps one literal string
    # legal under BOTH grammars.
    return f"pipelines/{name}{uuid.uuid4().hex[:8]}"


def _provision(
    spark: SparkSession, unique_table: Callable[[str], str], name: str
) -> tuple[PipelineSpecModel, str, str, str]:
    """One fact type (`identity`, `IDENTITY_FACT_SCHEMA`). Returns `(spec,
    pipeline, fact_qt, markers_qt)` with fact/state/markers all created --
    see module docstring for the `naming.markers_table` derivation trap
    this function exists to avoid."""
    pipeline = _unique_pipeline(name)
    raw_qt, qtn_qt = unique_table(f"{name}_raw"), unique_table(f"{name}_qtn")
    fact_qt, state_qt = unique_table(f"{name}_fact"), unique_table(f"{name}_state")
    markers_qt = naming.qualified(naming.markers_table(_bare(raw_qt), pipeline))
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    _create_markers_table(spark, markers_qt)
    spec = PipelineSpecModel(
        pipeline=pipeline,
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_types={
            "identity": FactTypeModel(
                fact_table=_bare(fact_qt), state_table=_bare(state_qt), schema=_IDENTITY_FACT_SCHEMA
            )
        },
        read=_IDENTITY_READ,
        raw_contract=_IDENTITY_RAW_CONTRACT,
    )
    return spec, pipeline, fact_qt, markers_qt


def _provision_2type(
    spark: SparkSession, unique_table: Callable[[str], str], name: str
) -> tuple[PipelineSpecModel, str, str, str, str]:
    """Two fact types (`orders`/`identity`-shaped, `shipments`/a distinct
    shape) sharing ONE markers table (§6.3: one per pipeline, not per
    type). Returns `(spec, pipeline, fact_qt_a, fact_qt_b, markers_qt)`."""
    pipeline = _unique_pipeline(name)
    raw_qt, qtn_qt = unique_table(f"{name}_raw"), unique_table(f"{name}_qtn")
    fact_a, state_a = unique_table(f"{name}_fact_a"), unique_table(f"{name}_state_a")
    fact_b, state_b = unique_table(f"{name}_fact_b"), unique_table(f"{name}_state_b")
    markers_qt = naming.qualified(naming.markers_table(_bare(raw_qt), pipeline))
    _create_fact_table(spark, fact_a)
    _create_state_table(spark, state_a)
    _create_fact_table(spark, fact_b, schema=_SHIPMENTS_SCHEMA)
    _create_state_table(spark, state_b, schema=_SHIPMENTS_SCHEMA)
    _create_markers_table(spark, markers_qt)
    spec = PipelineSpecModel(
        pipeline=pipeline,
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_types={
            "orders": FactTypeModel(
                fact_table=_bare(fact_a), state_table=_bare(state_a), schema=_IDENTITY_FACT_SCHEMA
            ),
            "shipments": FactTypeModel(
                fact_table=_bare(fact_b), state_table=_bare(state_b), schema=_SHIPMENTS_SCHEMA
            ),
        },
        read=_IDENTITY_READ,
        raw_contract=_IDENTITY_RAW_CONTRACT,
    )
    return spec, pipeline, fact_a, fact_b, markers_qt


def _identity_transforms() -> Transforms:
    # commit.run never calls ctx.transforms -- a placeholder satisfying the
    # BatchContext field's own required-ness (I-10), never invoked below.
    return Transforms(apply=lambda valid_df, co_effects: {"identity": valid_df})


def _make_ctx(
    *,
    spec: PipelineSpecModel,
    pipeline: str,
    batch_id: str,
    delivery_key: str = "statement.csv",
    content_hash: str = "sha256:" + "a" * 64,
    received_at: datetime = _T1,
    admitted_facts: Mapping[str, DataFrame],
) -> BatchContext:
    return BatchContext(
        pipeline=pipeline,
        feed_id=_FEED_ID,
        delivery_id=str(uuid.UUID(int=1, version=4)),
        batch_id=batch_id,
        delivery_key=delivery_key,
        content_hash=content_hash,
        object_uris=(),
        received_at=received_at,
        spec=spec,
        run=RunConfig(),
        transforms=_identity_transforms(),
        attempt_id="attempt-1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        read_spec_version=read_spec_version(spec.read),
        check_version=check_version(spec.raw_contract, spec.read),
        checks_version=checks_version(spec.checks),
        admitted_facts=MappingProxyType(dict(admitted_facts)),
    )


_IDENTITY_ROWS_SCHEMA = StructType(
    [
        StructField("domain_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)


def _identity_rows(spark: SparkSession, rows: tuple[tuple[str | None, str, str], ...]) -> DataFrame:
    # An EXPLICIT schema (never inferred): K-12's own NULL-domain_id vector
    # is a single row whose first column is `None` in every row, which
    # `createDataFrame`'s own type inference cannot resolve on its own
    # (`CANNOT_DETERMINE_TYPE`) -- found empirically writing this file.
    return spark.createDataFrame(
        [Row(domain_id=d, event_time=e, payload=p) for d, e, p in rows], _IDENTITY_ROWS_SCHEMA
    )


def _shipments_rows(spark: SparkSession, rows: tuple[tuple[str, str, str], ...]) -> DataFrame:
    return spark.createDataFrame(
        [Row(domain_id=d, shipped_at=e, payload=p) for d, e, p in rows],
        ["domain_id", "shipped_at", "payload"],
    )


def _fact_rows(spark: SparkSession, fact_qt: str, batch_id: str) -> list[tuple[str, str]]:
    return sorted(
        (r["domain_id"], r["payload"])
        for r in spark.table(fact_qt).where(f"batch_id = '{batch_id}'").collect()
    )


def _marker_rows_for(
    spark: SparkSession, markers_qt: str, batch_id: str, table_name: str
) -> list[dict[str, object]]:
    return [
        row.asDict()
        for row in spark.table(markers_qt)
        .where(f"batch_id = '{batch_id}' AND table_name = '{table_name}'")
        .collect()
    ]


# --- K-05: fail-open wiring -- a real two-batch commit sequence -------------


def test_k05_second_batch_drops_unchanged_keeps_changed_and_new(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    spec, pipeline, fact_qt, _markers_qt = _provision(spark, unique_table, "k05")

    b1, b2 = _batch_id(1), _batch_id(2)
    ctx1 = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b1,
        received_at=_T1,
        admitted_facts={
            "identity": _identity_rows(
                spark, (("d1", "2026-01-01", "p1"), ("d2", "2026-01-01", "p2"))
            )
        },
    )
    out1 = commit_stage.run(ctx1, local_runner_fx)
    assert dict(out1.facts_appended_by_table) == {_bare(fact_qt): 2}
    assert out1.delta_predecessor_batch_ids == ()  # genesis, path 1
    assert out1.delta_probe_refusal is None

    ctx2 = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b2,
        delivery_key="statement-2.csv",
        content_hash="sha256:" + "b" * 64,
        received_at=_T2,
        admitted_facts={
            "identity": _identity_rows(
                spark,
                (
                    ("d1", "2026-01-01", "p1"),
                    ("d2", "2026-01-01", "p2-changed"),
                    ("d3", "2026-01-01", "p3"),
                ),
            )
        },
    )
    out2 = commit_stage.run(ctx2, local_runner_fx)
    assert out2.delta_predecessor_batch_ids == (b1,)
    assert dict(out2.facts_appended_by_table) == {_bare(fact_qt): 2}  # d1 dropped, 2 survive
    assert _fact_rows(spark, fact_qt, b2) == sorted([("d2", "p2-changed"), ("d3", "p3")])


def test_k05_within_batch_divergent_duplicates_both_commit(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    """§7.2(b), at the full wiring grain: two candidate rows sharing a
    `domain_id` (hence `record_key`) but different `payload` (hence
    `content_hash`) both survive into the committed facts."""
    spec, pipeline, fact_qt, _markers_qt = _provision(spark, unique_table, "k05b")
    ctx = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=_batch_id(1),
        admitted_facts={
            "identity": _identity_rows(
                spark, (("d1", "2026-01-01", "pA"), ("d1", "2026-01-01", "pB"))
            )
        },
    )
    out = commit_stage.run(ctx, local_runner_fx)
    assert dict(out.facts_appended_by_table) == {_bare(fact_qt): 2}
    assert _fact_rows(spark, fact_qt, ctx.batch_id) == sorted([("d1", "pA"), ("d1", "pB")])


def test_k05_divergent_duplicate_key_never_dedupes_on_identical_resend(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    """007.1's ADR text (`design/007.1_record_lld.md` ~line 73, restated
    ~line 404) is a deliberately-accepted design consequence, not merely
    an observed one: a batch's within-batch divergent duplicates (§7.2(b))
    leave MORE THAN ONE `content_hash` committed for the same `record_key`
    -- so every future batch's predecessor projection for that key
    disagrees with itself, and `frames/delta.py::delta_filter`'s (c)
    fail-open law can never again find that key "unchanged in every batch"
    for ANY candidate value. The ADR names this precisely: "the duplicates
    themselves re-arm the next batch's filter" (line 73), "absorbed by
    fail-open" (line 404) -- so this golden reads as PINNED BY DESIGN, not
    discovered behavior.

    Batch 1 commits a divergent pair on `d1` (`pA`/`pB`, both survive per
    (b)) plus a clean control key `d2`. Batch 2 resends the IDENTICAL three
    rows on a new `batch_id`. `d1`'s rows survive AGAIN -- never deduped,
    4 rows total for `d1` across both batches -- while `d2` (never
    divergent, one stable predecessor value) dedupes to zero new rows in
    batch 2, per (c)'s ordinary unchanged-drop path."""
    spec, pipeline, fact_qt, _markers_qt = _provision(spark, unique_table, "k05c")

    b1, b2 = _batch_id(1), _batch_id(2)
    rows = (("d1", "2026-01-01", "pA"), ("d1", "2026-01-01", "pB"), ("d2", "2026-01-01", "clean"))

    ctx1 = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b1,
        received_at=_T1,
        admitted_facts={"identity": _identity_rows(spark, rows)},
    )
    out1 = commit_stage.run(ctx1, local_runner_fx)
    assert dict(out1.facts_appended_by_table) == {_bare(fact_qt): 3}

    ctx2 = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b2,
        delivery_key="statement-2.csv",
        content_hash="sha256:" + "b" * 64,
        received_at=_T2,
        admitted_facts={"identity": _identity_rows(spark, rows)},
    )
    out2 = commit_stage.run(ctx2, local_runner_fx)
    assert out2.delta_predecessor_batch_ids == (b1,)
    assert dict(out2.facts_appended_by_table) == {_bare(fact_qt): 2}  # d1 re-kept, d2 dropped
    assert _fact_rows(spark, fact_qt, b2) == sorted([("d1", "pA"), ("d1", "pB")])

    # The re-arm consequence, at the full-table grain: d1 (once divergent)
    # is never deduped -- both batches' rows persist, 4 total. d2 (never
    # divergent) dedupes normally -- only its one batch-1 row exists.
    d1_batches = sorted(
        r["batch_id"] for r in spark.table(fact_qt).where("domain_id = 'd1'").collect()
    )
    assert d1_batches == sorted([b1, b1, b2, b2])
    assert spark.table(fact_qt).where("domain_id = 'd2'").count() == 1


# --- K-06: the one Phase-1-reachable probe-refusal reason -------------------


def test_k06_none_with_key_match_refusal_recorded_and_emitted(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """§7.2 read 2's field-absent branch: a later delivery reusing the SAME
    `delivery_key` as an earlier one, but with a DIFFERENT `delivery_
    content_hash` (a corrected re-send under an unchanged key, `supersedes_
    batch_id` not yet landed -- `conveyer-kof`) -- refuses to drop anything;
    `ctx.delta_probe_refusal`/the ledger projection/EMF `DeltaProbeRefusals`
    (reason-dimensioned, [S-7]/[S-18] value-free) all assert it as data."""
    spec, pipeline, fact_qt, _markers_qt = _provision(spark, unique_table, "k06")

    b1, b2 = _batch_id(1), _batch_id(2)
    ctx1 = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b1,
        delivery_key="dk-shared",
        content_hash="sha256:" + "a" * 64,
        received_at=_T1,
        admitted_facts={"identity": _identity_rows(spark, (("d1", "2026-01-01", "p1"),))},
    )
    commit_stage.run(ctx1, local_runner_fx)

    # b2: SAME delivery_key, DIFFERENT content_hash -- key-match-with-
    # disagreement, §7.2's own refusal condition.
    ctx2 = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b2,
        delivery_key="dk-shared",
        content_hash="sha256:" + "b" * 64,
        received_at=_T2,
        admitted_facts={"identity": _identity_rows(spark, (("d1", "2026-01-01", "p1"),))},
    )
    capsys.readouterr()  # drain b1's own EMF lines before asserting b2's
    out2 = commit_stage.run(ctx2, local_runner_fx)

    assert out2.delta_probe_refusal == "none-with-key-match"
    assert out2.delta_predecessor_batch_ids == ()  # a refusal drops nothing -- everything novel
    assert dict(out2.facts_appended_by_table) == {_bare(fact_qt): 1}  # d1 kept, not dropped

    # The ledger projection (core/run_facts.py, L-4).
    from spine.core import run_facts

    t0 = local_runner_fx.now()
    run_fact = run_facts.transition("commit", ctx2, out2, t0, t0)
    assert run_fact.delta_probe_refusal == "none-with-key-match"

    # The reason-dimensioned EMF emission (effects/ledger.py -> observability.py),
    # value-free: the reason code is the entire payload, never delivery_key/hash.
    capsys.readouterr()
    local_runner_fx.record_run(run_fact)
    emitted = capsys.readouterr().out
    assert '"reason": "none-with-key-match"' in emitted
    assert "dk-shared" not in emitted  # [S-7]/[S-18]: delivery_key never leaks into the metric


# --- K-07: the L-3 disagreement golden --------------------------------------


def test_k07_read1_disagreeing_winner_names_no_predecessor(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    """ADR-OQ5 validation 3 / [AE2-1]: the SELECTED latest-completed batch's
    own marker rows disagree internally on delivery attributes (a write bug)
    -- resolution names NO predecessor rather than trust a corrupted winner.
    Constructed by writing the completion row TWICE, directly via `fx.
    append_marker_row` (bypassing `stages/commit.py`'s own guard, which
    would otherwise silently re-affirm and never construct a disagreement --
    exactly the "a write bug" scenario the LLD names, never reachable
    through commit's own normal decide-then-do path)."""
    markers_qt = naming.qualified(
        naming.markers_table(_bare(unique_table("k07_raw")), _unique_pipeline("k07"))
    )
    _create_markers_table(spark, markers_qt)
    markers_bare = _bare(markers_qt)

    b1 = _batch_id(1)
    write_a = MarkerRowWrite(
        batch_id=b1,
        feed_id=_FEED_ID,
        stage="commit",
        table_name=naming.COMMIT_COMPLETION_SENTINEL,
        delivery_key="dk-1",
        delivery_content_hash="dch-1",
        received_at=_T1,
        committed_at=_T1,
    )
    write_b = MarkerRowWrite(
        batch_id=b1,
        feed_id=_FEED_ID,
        stage="commit",
        table_name=naming.COMMIT_COMPLETION_SENTINEL,
        delivery_key="dk-1",
        delivery_content_hash="dch-DIFFERENT",
        received_at=_T1,
        committed_at=_T1,
    )
    local_runner_fx.append_marker_row(markers_bare, write_a)
    local_runner_fx.append_marker_row(markers_bare, write_b)  # the write bug: same key, disagreeing

    completion_rows = local_runner_fx.read_marker_completions(markers_bare, _FEED_ID)
    presence_rows = local_runner_fx.read_marker_presence(markers_bare, _FEED_ID)
    assert len(completion_rows) == 2  # both disagreeing rows really landed

    seed_attrs = SeedAttrs(
        batch_id=_batch_id(2),
        delivery_key="dk-2",
        delivery_content_hash="dch-2",
        supersedes_batch_id=None,
    )
    resolution = resolve_predecessors(seed_attrs, completion_rows, presence_rows, ())
    assert resolution.predecessor_batch_ids == ()
    assert resolution.probe_refusal is None  # a read-1 keep, never a probe refusal ([AE2-10])


# --- K-08: marker row-kind goldens ------------------------------------------


def test_k08_guard_twin_vs_completion_discriminated_by_sentinel_idempotent(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    markers_qt = naming.qualified(
        naming.markers_table(_bare(unique_table("k08_raw")), _unique_pipeline("k08"))
    )
    _create_markers_table(spark, markers_qt)
    markers_bare = _bare(markers_qt)
    b1 = _batch_id(1)
    twin = MarkerRowWrite(
        batch_id=b1,
        feed_id=_FEED_ID,
        stage="commit",
        table_name="some_db.some__facts",
        delivery_key="dk-1",
        delivery_content_hash="dch-1",
        received_at=_T1,
        committed_at=_T1,
    )
    completion = MarkerRowWrite(
        batch_id=b1,
        feed_id=_FEED_ID,
        stage="commit",
        table_name=naming.COMMIT_COMPLETION_SENTINEL,
        delivery_key="dk-1",
        delivery_content_hash="dch-1",
        received_at=_T1,
        committed_at=_T1,
    )
    for write in (twin, twin, completion, completion):  # M-3: a rerun re-affirms, never duplicates
        present = local_runner_fx.marker_row_present(
            markers_bare, write.batch_id, write.stage, write.table_name
        )
        if not present:
            local_runner_fx.append_marker_row(markers_bare, write)

    rows = [row.asDict() for row in spark.table(markers_qt).collect()]
    assert len(rows) == 2  # exactly one twin, one completion -- never duplicated
    by_table_name = {r["table_name"]: r for r in rows}
    assert set(by_table_name) == {"some_db.some__facts", naming.COMMIT_COMPLETION_SENTINEL}
    assert all(r["snapshot_id"] is None for r in rows)  # §6.3's write-order-necessity resolution


# --- K-09: partial-commit probe (guard-twin presence without completion) ---


def test_k09_guard_twin_without_completion_names_no_predecessor(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    markers_qt = naming.qualified(
        naming.markers_table(_bare(unique_table("k09_raw")), _unique_pipeline("k09"))
    )
    _create_markers_table(spark, markers_qt)
    markers_bare = _bare(markers_qt)
    b1 = _batch_id(1)
    twin = MarkerRowWrite(
        batch_id=b1,
        feed_id=_FEED_ID,
        stage="commit",
        table_name="some_db.some__facts",
        delivery_key="dk-1",
        delivery_content_hash="dch-1",
        received_at=_T1,
        committed_at=_T1,
    )
    local_runner_fx.append_marker_row(markers_bare, twin)  # twin only -- no completion row

    completion_rows = local_runner_fx.read_marker_completions(markers_bare, _FEED_ID)
    presence_rows = local_runner_fx.read_marker_presence(markers_bare, _FEED_ID)
    assert completion_rows == ()
    assert len(presence_rows) == 1

    seed_attrs = SeedAttrs(
        batch_id=_batch_id(2),
        delivery_key="dk-2",
        delivery_content_hash="dch-2",
        supersedes_batch_id=None,
    )
    resolution = resolve_predecessors(seed_attrs, completion_rows, presence_rows, ())
    assert resolution.predecessor_batch_ids == ()  # the coherence clause -- ADR-OQ2 hardening 1
    assert resolution.probe_refusal is None


# --- K-10: marker-without-facts kill, standing truth + rerun converges -----


def test_k10_kill_between_twin_and_append_then_rerun_converges(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    make_wrapped_fx: Callable[..., RunnerFx],
    unique_table: Callable[[str], str],
) -> None:
    spec, pipeline, fact_qt, markers_qt = _provision(spark, unique_table, "k10")
    b1 = _batch_id(1)
    ctx = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b1,
        admitted_facts={"identity": _identity_rows(spark, (("d1", "2026-01-01", "p1"),))},
    )

    # §4.3 step 4->5: kill AFTER the guard-twin's own append_marker_row call
    # (its 1st occurrence -- the one fact type's own twin), BEFORE the fact
    # append that would follow it.
    killed_fx = make_wrapped_fx(local_runner_fx, {"append_marker_row": killfx.kill_after(1)})
    with pytest.raises(killfx.SimulatedKill):
        commit_stage.run(ctx, killed_fx)

    # Standing truth (before ANY rerun): marker-without-facts, the designed
    # over-refusal state (L-2) -- the row standard, asserted first.
    assert spark.table(fact_qt).where(f"batch_id = '{b1}'").isEmpty()
    twin_rows = _marker_rows_for(spark, markers_qt, b1, _bare(fact_qt))
    assert len(twin_rows) == 1
    completion_rows = _marker_rows_for(spark, markers_qt, b1, naming.COMMIT_COMPLETION_SENTINEL)
    assert completion_rows == []  # no completion -- the batch is invisible as a predecessor

    # Rerun: fact guard absent -> delta re-runs, ensure-twin re-affirms
    # (M-3), append lands; converges.
    out = commit_stage.run(ctx, local_runner_fx)
    assert dict(out.facts_appended_by_table) == {_bare(fact_qt): 1}
    assert _fact_rows(spark, fact_qt, b1) == [("d1", "p1")]
    twin_rows_after = _marker_rows_for(spark, markers_qt, b1, _bare(fact_qt))
    assert len(twin_rows_after) == 1  # M-3: re-affirmed, never duplicated
    completion_rows_after = _marker_rows_for(
        spark, markers_qt, b1, naming.COMMIT_COMPLETION_SENTINEL
    )
    assert len(completion_rows_after) == 1


# --- K-11: zero-fact completion visibility ----------------------------------


def test_k11_zero_fact_batch_writes_completion_but_no_twin_or_facts(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
) -> None:
    spec, pipeline, fact_qt, markers_qt = _provision(spark, unique_table, "k11")
    b1, b2, b3 = _batch_id(1), _batch_id(2), _batch_id(3)
    ctx1 = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b1,
        received_at=_T1,
        admitted_facts={"identity": _identity_rows(spark, (("d1", "2026-01-01", "p1"),))},
    )
    commit_stage.run(ctx1, local_runner_fx)

    # b2 resubmits IDENTICAL content -- everything drops, zero-fact batch.
    ctx2 = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b2,
        delivery_key="dk-2",
        content_hash="sha256:" + "b" * 64,
        received_at=_T2,
        admitted_facts={"identity": _identity_rows(spark, (("d1", "2026-01-01", "p1"),))},
    )
    out2 = commit_stage.run(ctx2, local_runner_fx)
    assert dict(out2.facts_appended_by_table) == {_bare(fact_qt): 0}
    assert dict(out2.commit_snapshot_ids) == {}  # §4.2: absent key = zero-fact no-op
    assert spark.table(fact_qt).where(f"batch_id = '{b2}'").isEmpty()
    twin_b2 = _marker_rows_for(spark, markers_qt, b2, _bare(fact_qt))
    assert twin_b2 == []  # no twin -- F-4's zero-fact corollary
    completion_b2 = _marker_rows_for(spark, markers_qt, b2, naming.COMMIT_COMPLETION_SENTINEL)
    assert len(completion_b2) == 1  # the durable visibility, unconditional

    # b3: b2 resolves as the latest-completed predecessor despite carrying
    # no facts -- the comparison against it is vacuous, everything novel
    # (path 4's own priced keep).
    ctx3 = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b3,
        delivery_key="dk-3",
        content_hash="sha256:" + "c" * 64,
        received_at=_T3,
        admitted_facts={"identity": _identity_rows(spark, (("d1", "2026-01-01", "p1"),))},
    )
    out3 = commit_stage.run(ctx3, local_runner_fx)
    assert out3.delta_predecessor_batch_ids == (b2,)
    assert dict(out3.facts_appended_by_table) == {_bare(fact_qt): 1}  # vacuous compare -- kept


# --- K-12/K-13: the I-24 backstop at commit's own boundary ------------------
#
# The DOOR-side construction (a durable-authority door under drift/moved-pin
# admitting an unevaluated NULL-domain_id candidate set, 006.1's G-06 twin;
# the [AE-3] guard-present variant reaching commit through DURABLE_SUBTRACT)
# is 006.1/B3's own upstream territory (`post_check.py`'s doors) -- these two
# tests exercise commit's OWN half directly: however a NULL-domain_id or
# schema-drifted candidate set reaches commit, the I-24 backstop converts it
# into a named, deterministic defect, never a silent fold-breaking append.


def test_k12_null_domain_id_candidate_is_a_named_defect_never_silent(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    spec, pipeline, fact_qt, markers_qt = _provision(spark, unique_table, "k12")
    ctx = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=_batch_id(1),
        admitted_facts={"identity": _identity_rows(spark, ((None, "2026-01-01", "p1"),))},
    )
    with pytest.raises(ValueError, match="I-24 structural fact check failed"):
        commit_stage.run(ctx, local_runner_fx)
    assert (
        spark.table(fact_qt).where(f"batch_id = '{ctx.batch_id}'").isEmpty()
    )  # no append, no fold
    assert spark.table(markers_qt).isEmpty()  # no marker row either -- fails BEFORE any write


def test_k13_schema_drift_candidate_is_a_named_defect(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    """The [AE-3]-adjacent half of the SAME backstop: a candidate frame
    whose column set has drifted from the target fact table's (a distinct
    I-24 condition from the domain_id-null one, §7.3(b))."""
    spec, pipeline, _fact_qt, _markers_qt = _provision(spark, unique_table, "k13")
    # A candidate frame missing the declared `payload` column entirely --
    # commit's own structural check must catch this, never silently commit
    # a partial row.
    drifted = spark.createDataFrame(
        [Row(domain_id="d1", event_time="2026-01-01")], ["domain_id", "event_time"]
    )
    ctx = _make_ctx(
        spec=spec, pipeline=pipeline, batch_id=_batch_id(1), admitted_facts={"identity": drifted}
    )
    # frames/facts.py's own missing-declared-column guard fires first
    # (before commit's own §7.3 table-side check ever runs, F-1's own
    # documented ordering) -- still the SAME class of named defect, never a
    # silent append.
    with pytest.raises(ValueError, match="declared column"):
        commit_stage.run(ctx, local_runner_fx)


# --- K-20: between type appends (A complete, B untouched) ------------------


def test_k20_kill_between_type_appends_then_rerun_converges(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    make_wrapped_fx: Callable[..., RunnerFx],
    unique_table: Callable[[str], str],
) -> None:
    spec, pipeline, fact_a, fact_b, markers_qt = _provision_2type(spark, unique_table, "k20")
    b1 = _batch_id(1)
    ctx = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b1,
        admitted_facts={
            "orders": _identity_rows(spark, (("d1", "2026-01-01", "p1"),)),
            "shipments": _shipments_rows(spark, (("d1", "2026-01-01", "s1"),)),
        },
    )

    # `fx.append` is called once per NOVEL type, in declared order (orders,
    # then shipments) -- kill after the 1st call (orders' own append),
    # before the 2nd (shipments').
    killed_fx = make_wrapped_fx(local_runner_fx, {"append": killfx.kill_after(1)})
    with pytest.raises(killfx.SimulatedKill):
        commit_stage.run(ctx, killed_fx)

    # Standing truth: A's facts durable and sound; nothing for B; no
    # completion row -- the feed over-refuses for the WHOLE feed (shared
    # across types), never a partial-of-A perception.
    assert _fact_rows(spark, fact_a, b1) == [("d1", "p1")]
    assert spark.table(fact_b).where(f"batch_id = '{b1}'").isEmpty()
    completion_rows = _marker_rows_for(spark, markers_qt, b1, naming.COMMIT_COMPLETION_SENTINEL)
    assert completion_rows == []

    # Rerun: A's guard present -> ensure-twin + skip; B proceeds.
    out = commit_stage.run(ctx, local_runner_fx)
    assert dict(out.facts_appended_by_table) == {_bare(fact_a): 0, _bare(fact_b): 1}
    assert [r["domain_id"] for r in spark.table(fact_b).where(f"batch_id = '{b1}'").collect()] == [
        "d1"
    ]
    completion_rows_after = _marker_rows_for(
        spark, markers_qt, b1, naming.COMMIT_COMPLETION_SENTINEL
    )
    assert len(completion_rows_after) == 1


# --- K-21: after the last append, before the commit-completion row ---------


def test_k21_kill_after_last_twin_before_completion_then_rerun_opens_predecessor_window(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    make_wrapped_fx: Callable[..., RunnerFx],
    unique_table: Callable[[str], str],
) -> None:
    spec, pipeline, fact_qt, markers_qt = _provision(spark, unique_table, "k21")
    b1 = _batch_id(1)
    ctx1 = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b1,
        received_at=_T1,
        admitted_facts={"identity": _identity_rows(spark, (("d1", "2026-01-01", "p1"),))},
    )
    # For a single-type pipeline, `fx.append`'s 1st (and only) call IS the
    # last fact append -- the commit-completion marker write only happens
    # LATER, after the whole per-table loop returns (§4.3's own normative
    # order). Killing right after `append`'s own call-through (before
    # `_ensure_marker_row`'s subsequent completion write ever runs) is
    # exactly "after the last append, before the completion row" (§11's
    # row, single-type grain) -- NOT `append_marker_row`'s own occurrence 1,
    # which is the fact type's own GUARD-TWIN, landing BEFORE the fact
    # append within the same type's iteration (that shape is K-10's own
    # scenario instead, found empirically writing this file).
    killed_fx = make_wrapped_fx(local_runner_fx, {"append": killfx.kill_after(1)})
    with pytest.raises(killfx.SimulatedKill):
        commit_stage.run(ctx1, killed_fx)

    assert _fact_rows(spark, fact_qt, b1) == [("d1", "p1")]  # A's facts durable and sound
    completion_rows = _marker_rows_for(spark, markers_qt, b1, naming.COMMIT_COMPLETION_SENTINEL)
    assert completion_rows == []  # invisible as a predecessor -- no completion yet

    ctx1_rerun = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b1,
        received_at=_T1,
        admitted_facts={"identity": _identity_rows(spark, (("d1", "2026-01-01", "p1"),))},
    )
    out1_rerun = commit_stage.run(ctx1_rerun, local_runner_fx)
    assert dict(out1_rerun.facts_appended_by_table) == {_bare(fact_qt): 0}  # guard-present -> skip
    completion_rows_after = _marker_rows_for(
        spark, markers_qt, b1, naming.COMMIT_COMPLETION_SENTINEL
    )
    assert len(completion_rows_after) == 1  # decide-then-do (M-3): the completion lands now

    # [AE2-2]: the completion write opens the completed-but-unfolded window
    # -- b1 is now a LAWFUL predecessor for the feed's next batch, even
    # though its own fold (out of B9b's scope, B10's territory) never ran.
    b2 = _batch_id(2)
    ctx2 = _make_ctx(
        spec=spec,
        pipeline=pipeline,
        batch_id=b2,
        delivery_key="dk-2",
        content_hash="sha256:" + "b" * 64,
        received_at=_T2,
        admitted_facts={"identity": _identity_rows(spark, (("d1", "2026-01-01", "p1"),))},
    )
    out2 = commit_stage.run(ctx2, local_runner_fx)
    assert out2.delta_predecessor_batch_ids == (b1,)
    assert dict(out2.facts_appended_by_table) == {_bare(fact_qt): 0}  # d1 unchanged -> dropped
