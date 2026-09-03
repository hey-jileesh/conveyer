"""G-suite standing scenarios (006.1 §13.1) exercised at THIS bead's own grain
(bead conveyer-6pg.13, B3): `land` -> `pre_check` -> `pull` -> `apply` ->
`post_check`, over a genuinely MULTI-fact-type spec, stopping BEFORE `commit`/
`fold`/`publish` (007.1 B9b/B10's own territory -- `stages/commit.py`/
`stages/fold.py` still reference the deleted singular `spec.fact_table`/
`ctx.admitted_facts_df` fields as of this bead, `context.py`'s own docstring
names the gap). Direct stage calls (`land.run`/`pre_check.run`/`pull.run`/
`apply.run`/`post_check.run`), never `spine.run.run` -- the only way to reach
G-01..G-04/G-06's per-type mechanics without also depending on commit/fold.

Covers: **G-01** (declared row + membership checks e2e, multi-type, first-
failure `reason_code` + ordered `reason_detail`), **G-02** (fresh-path count
identity per type, one post append per batch), **G-03** (guard-present
rerun, typed hash-keyed subtraction -- cross-type value-identical rows are
NOT cross-subtracted, the `_conveyer_fact_type` tag's own golden), **G-04**
(a: DURABLE_AUTHORITY when ANY declared fact table already carries the
batch; b: zero-violation `--rN` under changed checks -- drift recorded,
never raised, no new rows; c: pre_check's own [DC-1] ANY-door), **G-06**
(NULL-`domain_id` end-to-end: the implicit check fires first, `record_key`
derives only when `domain_id_col` is outside the declared `record_key:`
columns), and **G-12** (the identity exemplar's own migration: `apply`
returns the one-entry mapping, the violations variant's rule lives in
`checks.yaml` -- pinned directly against the real `pipelines.identity`/
`pipelines.identity_violations` modules via `scenario_helpers.py`, this
bead's own migration target).

**Spec design, this file's own two-type exemplar (`orders`/`shipments`).**
Both types declare the identical `(domain_id: string, amount: decimal(10,2))`
candidate shape (G-03's own "cross-type value-identical row" precondition:
two types must be ABLE to produce byte-identical candidate rows for the tag
to have anything to discriminate). `orders` binds two checks -- a row check
(`positive-amount`, `amount > 0`) and a membership check (`known-domain`,
`domain_id` against a co-effect ref table) -- `shipments` binds none, so a
"clean" `shipments` row is admitted unconditionally (only the framework's
implicit `missing-domain-id` check ever fires on it). The raw contract
deliberately declares `domain_id` NULLABLE (`required: false, nullable:
true`) -- unlike the identity exemplar's own contract -- so a NULL candidate
`domain_id` can reach `apply`/`post_check` at all (G-06's own precondition;
pre_check's admission-level nullability is a DIFFERENT column class from the
fact schema's `domain_id_col`, 006 D-6, not entangled here on purpose).
`apply` is a plain, hand-written test double (not a `pipelines.*` module --
this file's own multi-type spec has no deployed pipeline counterpart): casts
`amount`, splits by the raw `kind` column into the two candidate frames.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import killfx
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from spine.binding import Transforms
from spine.bootstrap.create_admission_tables import (
    render_quarantine_create_table_sql,
    render_raw_create_table_sql,
)
from spine.bootstrap.create_record_tables import (
    bootstrap_fact_table,
    bootstrap_markers_table,
    bootstrap_state_table,
)
from spine.config import RunConfig
from spine.context import BatchContext
from spine.core import naming
from spine.core.checks import checks_version
from spine.core.contract import check_version, read_spec_version
from spine.core.model import (
    ChecksModel,
    CoEffectDecl,
    ColumnSpec,
    DialectModel,
    FactColumnSpec,
    FactSchemaModel,
    FactTypeModel,
    MembershipCheckModel,
    PipelineSpecModel,
    RawContractModel,
    ReadSpecModel,
    RowCheckModel,
)
from spine.effects.records import RunnerFx
from spine.stages import apply as apply_stage
from spine.stages import commit as commit_stage
from spine.stages import land, pre_check, pull
from spine.stages import post_check as post_check_stage

_CATALOG_PREFIX = "spine_cat."


def _bare(qualified_table: str) -> str:
    assert qualified_table.startswith(_CATALOG_PREFIX)
    return qualified_table.removeprefix(_CATALOG_PREFIX)


def _batch_id(n: int) -> str:
    return str(UUID(int=n, version=5))


# --- the two-type fact schema + spec (module docstring's own design) --------

_ORDERS = "orders"
_SHIPMENTS = "shipments"

_CANDIDATE_SCHEMA = FactSchemaModel(
    columns=[
        FactColumnSpec(name="domain_id", type="string"),
        FactColumnSpec(name="amount", type="decimal(10,2)"),
    ],
    domain_id_col="domain_id",
    record_key=["domain_id"],
)

_RAW_CONTRACT = RawContractModel(
    columns=[
        # deliberately nullable/not required -- see module docstring's G-06 note.
        ColumnSpec(name="domain_id"),
        ColumnSpec(name="amount"),
        ColumnSpec(name="kind"),
    ]
)
_READ = ReadSpecModel(dialect=DialectModel(format="csv", header=True))

_CHECKS = ChecksModel(
    checks=[
        RowCheckModel(
            kind="row",
            id="positive-amount",
            fact_type=_ORDERS,
            expr="amount > 0",
            reason="business/negative-amount",
        ),
        MembershipCheckModel(
            kind="membership",
            id="known-domain",
            fact_type=_ORDERS,
            columns=["domain_id"],
            co_effect="allow_ref",
            ref_columns=["id"],
            reason="business/unknown-domain",
        ),
    ]
)


def _apply(valid_df: DataFrame, co_effects: Mapping[str, DataFrame]) -> Mapping[str, DataFrame]:
    del co_effects
    casted = valid_df.withColumn("amount", F.col("amount").cast("decimal(10,2)"))
    orders = casted.filter(F.col("kind") == _ORDERS).select("domain_id", "amount")
    shipments = casted.filter(F.col("kind") == _SHIPMENTS).select("domain_id", "amount")
    return {_ORDERS: orders, _SHIPMENTS: shipments}


def _make_spec(
    *,
    raw_table: str,
    quarantine_table: str,
    orders_fact_table: str,
    orders_state_table: str,
    shipments_fact_table: str,
    shipments_state_table: str,
    co_effects: dict[str, CoEffectDecl] | None = None,
    checks: ChecksModel | None = None,
) -> PipelineSpecModel:
    return PipelineSpecModel(
        pipeline="pipelines/multi-type-g-suite",
        # never called -- this file's own `_apply` test double is used directly.
        transforms_module="pipelines.identity.transforms",
        raw_table=raw_table,
        quarantine_table=quarantine_table,
        fact_types={
            _ORDERS: FactTypeModel(
                fact_table=orders_fact_table,
                state_table=orders_state_table,
                schema=_CANDIDATE_SCHEMA,
            ),
            _SHIPMENTS: FactTypeModel(
                fact_table=shipments_fact_table,
                state_table=shipments_state_table,
                schema=_CANDIDATE_SCHEMA,
            ),
        },
        checks=checks if checks is not None else _CHECKS,
        co_effects=co_effects or {},
        read=_READ,
        raw_contract=_RAW_CONTRACT,
    )


def _make_seed(
    *, spec: PipelineSpecModel, batch_id: str, object_uris: tuple[str, ...]
) -> BatchContext:
    transforms = Transforms(apply=_apply)
    return BatchContext(
        pipeline="pipelines/multi-type-g-suite",
        feed_id="feed/multi-type-g-suite",
        delivery_id=str(UUID(int=1, version=4)),
        batch_id=batch_id,
        delivery_key="statement.csv",
        content_hash="sha256:" + "a" * 64,
        object_uris=object_uris,
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        spec=spec,
        run=RunConfig(),
        transforms=transforms,
        attempt_id="attempt-1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        read_spec_version=read_spec_version(spec.read),
        check_version=check_version(spec.raw_contract, spec.read),
        checks_version=checks_version(spec.checks),
    )


def _create_raw_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(render_raw_create_table_sql(qualified_table, _RAW_CONTRACT))


def _create_quarantine_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(render_quarantine_create_table_sql(qualified_table))


def _create_fact_table(spark: SparkSession, qualified_table: str) -> None:
    """[DC-1]'s presence probe needs a real table carrying `batch_id` --
    matches `test_stages_land_pre_pull_apply.py::_create_fact_table`'s own
    minimal shape (the physical candidate-column DDL is commit/fold's
    concern, out of this file's scope)."""
    spark.sql(f"CREATE TABLE {qualified_table} (domain_id STRING, batch_id STRING) USING iceberg")


def _create_ref_table(spark: SparkSession, qualified_table: str, ids: tuple[str, ...]) -> None:
    spark.sql(f"CREATE TABLE {qualified_table} (id STRING) USING iceberg")
    if ids:
        values = ", ".join(f"('{i}')" for i in ids)
        spark.sql(f"INSERT INTO {qualified_table} VALUES {values}")


def _write_csv(path: Path, text: str) -> tuple[str, ...]:
    path.write_text(text)
    return (str(path),)


def _run_through_post_check(seed: BatchContext, fx: RunnerFx) -> BatchContext:
    """`land -> pre_check -> pull -> apply -> post_check`, direct stage
    calls -- this file's own sequencing (module docstring)."""
    landed = land.run(seed, fx)
    checked = pre_check.run(landed, fx)
    pulled = pull.run(checked, fx)
    applied = apply_stage.run(pulled, fx)
    return post_check_stage.run(applied, fx)


class _Fixture:
    def __init__(
        self,
        spark: SparkSession,
        fx: RunnerFx,
        unique_table: Callable[[str], str],
        allow_ids: tuple[str, ...] = ("id-1", "id-2", "id-3", "id-4", "id-5"),
    ) -> None:
        raw_qt = unique_table("raw")
        qtn_qt = unique_table("qtn")
        orders_fact_qt = unique_table("orders_fact")
        orders_state_qt = unique_table("orders_state")
        shipments_fact_qt = unique_table("shipments_fact")
        shipments_state_qt = unique_table("shipments_state")
        ref_qt = unique_table("allow_ref")
        _create_raw_table(spark, raw_qt)
        _create_quarantine_table(spark, qtn_qt)
        _create_fact_table(spark, orders_fact_qt)
        _create_fact_table(spark, shipments_fact_qt)
        _create_ref_table(spark, ref_qt, allow_ids)
        self.spark = spark
        self.fx = fx
        self.qtn_qt = qtn_qt
        self.orders_fact_qt = orders_fact_qt
        self.shipments_fact_qt = shipments_fact_qt
        self.spec = _make_spec(
            raw_table=_bare(raw_qt),
            quarantine_table=_bare(qtn_qt),
            orders_fact_table=_bare(orders_fact_qt),
            orders_state_table=_bare(orders_state_qt),
            shipments_fact_table=_bare(shipments_fact_qt),
            shipments_state_table=_bare(shipments_state_qt),
            co_effects={"allow_ref": CoEffectDecl(table=_bare(ref_qt))},
        )

    def seed(
        self,
        batch_id: str,
        object_uris: tuple[str, ...],
        *,
        spec: PipelineSpecModel | None = None,
    ) -> BatchContext:
        return _make_seed(spec=spec or self.spec, batch_id=batch_id, object_uris=object_uris)

    def insert_fact_row(self, fact_table: str, batch_id: str, domain_id: str) -> None:
        self.spark.sql(f"INSERT INTO {fact_table} VALUES ('{domain_id}', '{batch_id}')")

    def quarantine_rows(self, batch_id: str) -> list[dict[str, object]]:
        return [
            row.asDict()
            for row in self.spark.table(self.qtn_qt)
            .where(f"batch_id = '{batch_id}' AND check_stage = 'post_check'")
            .collect()
        ]


# ============================================================================
# G-01: declared row + membership checks e2e (multi-type)
# ============================================================================


def test_g01_row_and_membership_checks_quarantine_with_ordered_reason_detail(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    fixture = _Fixture(spark, local_runner_fx, unique_table, allow_ids=("id-1",))
    batch_id = _batch_id(1)
    # id-1: clean (positive amount, known domain). id-2: negative amount AND
    # unknown domain -- BOTH checks fail, first-failure `positive-amount`
    # wins `reason_code` (authored order, P-5). id-3 (shipments): admitted
    # unconditionally -- shipments binds no checks at all.
    object_uris = _write_csv(
        tmp_path / "batch.csv",
        "domain_id,amount,kind\nid-1,10.00,orders\nid-2,-5.00,orders\nid-3,1.00,shipments\n",
    )
    seed = fixture.seed(batch_id, object_uris)

    after = _run_through_post_check(seed, local_runner_fx)

    assert after.admitted_facts is not None
    orders_admitted = sorted(r["domain_id"] for r in after.admitted_facts[_ORDERS].collect())
    assert orders_admitted == ["id-1"]
    shipments_admitted = sorted(r["domain_id"] for r in after.admitted_facts[_SHIPMENTS].collect())
    assert shipments_admitted == ["id-3"]

    rows = fixture.quarantine_rows(batch_id)
    assert len(rows) == 1
    row = rows[0]
    assert row["domain_id"] == "id-2"
    assert row["reason_code"] == "business/negative-amount"  # first-failure, authored order
    detail = json.loads(row["reason_detail"])
    assert [entry["id"] for entry in detail] == ["positive-amount", "known-domain"]
    assert all("version" in entry for entry in detail)
    assert row["record_key"] is not None  # domain_id non-null -> record_key derives


# ============================================================================
# G-02: fresh-path count identity per type; one post append per batch
# ============================================================================


def test_g02_fresh_path_count_identity_and_one_append_per_batch(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    fixture = _Fixture(spark, local_runner_fx, unique_table, allow_ids=("id-1", "id-2"))
    batch_id = _batch_id(2)
    object_uris = _write_csv(
        tmp_path / "batch.csv",
        "domain_id,amount,kind\n"
        "id-1,10.00,orders\n"
        "id-2,-1.00,orders\n"
        "id-3,1.00,shipments\n"
        "id-4,2.00,shipments\n",
    )
    seed = fixture.seed(batch_id, object_uris)
    qtn_snapshots = spark.read.format("iceberg").load(f"{fixture.qtn_qt}.snapshots")
    qtn_before = {r["snapshot_id"] for r in qtn_snapshots.collect()}

    after = _run_through_post_check(seed, local_runner_fx)

    # per-type count identity (candidate == admitted + violations)
    assert after.admitted_facts is not None
    assert after.admitted_facts[_ORDERS].count() == 1
    assert after.admitted_facts[_SHIPMENTS].count() == 2
    assert after.post_quarantined_count == 1

    qtn_after = spark.read.format("iceberg").load(f"{fixture.qtn_qt}.snapshots").collect()
    new_snapshots = {r["snapshot_id"] for r in qtn_after} - qtn_before
    assert len(new_snapshots) == 1  # ONE guarded append across both types (§8.1)


def test_g02_zero_violations_across_all_types_writes_nothing(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    fixture = _Fixture(spark, local_runner_fx, unique_table, allow_ids=("id-1",))
    batch_id = _batch_id(3)
    object_uris = _write_csv(
        tmp_path / "batch.csv", "domain_id,amount,kind\nid-1,10.00,orders\nid-2,1.00,shipments\n"
    )
    seed = fixture.seed(batch_id, object_uris)

    after = _run_through_post_check(seed, local_runner_fx)

    assert after.post_quarantined_count == 0
    assert after.post_quarantine_snapshot_id is None
    assert fixture.quarantine_rows(batch_id) == []


# ============================================================================
# G-03: guard-present rerun, typed hash-keyed subtraction (P-7's tag golden)
# ============================================================================


def test_g03_guard_present_rerun_cross_type_value_identical_rows_not_cross_subtracted(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    fixture = _Fixture(spark, local_runner_fx, unique_table, allow_ids=())
    batch_id = _batch_id(4)
    # id-9 is VALUE-IDENTICAL across both types after `apply`'s projection
    # (domain_id/amount) -- but only `orders` binds the membership check
    # that flags it (empty allow-list); `shipments` binds no checks at all,
    # so its value-identical id-9 row is genuinely clean.
    object_uris = _write_csv(
        tmp_path / "batch.csv", "domain_id,amount,kind\nid-9,5.00,orders\nid-9,5.00,shipments\n"
    )
    first_seed = fixture.seed(batch_id, object_uris)
    first = _run_through_post_check(first_seed, local_runner_fx)
    assert first.admitted_facts is not None
    assert first.admitted_facts[_ORDERS].count() == 0  # id-9 quarantined (unknown-domain)
    assert first.admitted_facts[_SHIPMENTS].count() == 1  # id-9 survives -- no checks bind it
    assert first.post_quarantined_count == 1

    # Rerun: the quarantine guard is now present -- DURABLE_SUBTRACT (§8.3).
    second_seed = fixture.seed(batch_id, object_uris)
    landed = land.run(second_seed, local_runner_fx)
    checked = pre_check.run(landed, local_runner_fx)
    pulled = pull.run(checked, local_runner_fx)
    applied = apply_stage.run(pulled, local_runner_fx)
    second = post_check_stage.run(applied, local_runner_fx)

    assert second.admitted_facts is not None
    # orders' id-9 hash-matches the durable quarantine row -- subtracted.
    assert second.admitted_facts[_ORDERS].count() == 0
    # shipments' VALUE-IDENTICAL id-9 candidate must NOT cross-subtract --
    # the tag (P-7(b)) is what keeps a durable orders hash inert against it.
    assert second.admitted_facts[_SHIPMENTS].count() == 1
    assert "post_check" in second.guard_skips
    assert second.post_quarantined_count == 1  # durable count, unchanged


# ============================================================================
# G-04: doors at per-type grain
# ============================================================================


def test_g04a_durable_authority_when_any_declared_fact_table_has_the_batch(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    fixture = _Fixture(spark, local_runner_fx, unique_table, allow_ids=())
    batch_id = _batch_id(5)
    # A row that WOULD be quarantined under a fresh evaluation (unknown
    # domain, empty allow-list) -- proves DURABLE_AUTHORITY admits it
    # unconditionally, no check evaluation gating admission (§8.2 [R2-1]).
    object_uris = _write_csv(tmp_path / "batch.csv", "domain_id,amount,kind\nid-7,5.00,orders\n")
    # Simulate "shipments already committed this batch" (commit's own
    # territory, out of this bead's scope -- a direct row insert stands in
    # for a real N-guarded-append commit, module docstring).
    fixture.insert_fact_row(fixture.shipments_fact_qt, batch_id, "id-nobody")
    seed = fixture.seed(batch_id, object_uris)

    after = _run_through_post_check(seed, local_runner_fx)

    assert after.admitted_facts is not None
    assert after.admitted_facts[_ORDERS].count() == 1  # admitted unconditionally
    assert [r["domain_id"] for r in after.admitted_facts[_ORDERS].collect()] == ["id-7"]
    assert after.post_quarantined_count == 0  # no append on this door
    assert "post_check" not in after.guard_skips  # the quarantine guard was never present
    assert fixture.quarantine_rows(batch_id) == []


def test_g04b_zero_violation_rerun_after_checks_tightened_drift_recorded_no_new_rows(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    fixture = _Fixture(spark, local_runner_fx, unique_table, allow_ids=("id-1",))
    batch_id = _batch_id(6)
    object_uris = _write_csv(tmp_path / "batch.csv", "domain_id,amount,kind\nid-1,10.00,orders\n")
    first = _run_through_post_check(fixture.seed(batch_id, object_uris), local_runner_fx)
    assert first.post_quarantined_count == 0
    assert first.admitted_facts is not None
    # simulate the deliberate `--rN`: this batch's facts are now durable.
    fixture.insert_fact_row(fixture.orders_fact_qt, batch_id, "id-1")

    # Tighten checks.yaml between attempts (a real rule change): id-1 would
    # now fail a NEW row check under fresh evaluation.
    tightened_checks = ChecksModel(
        checks=[
            *_CHECKS.checks,
            RowCheckModel(
                kind="row",
                id="amount-under-5",
                fact_type=_ORDERS,
                expr="amount < 5",
                reason="business/amount-too-high",
            ),
        ]
    )
    tightened_spec = fixture.spec.model_copy(update={"checks": tightened_checks})
    second_seed = fixture.seed(batch_id, object_uris, spec=tightened_spec)

    second = _run_through_post_check(second_seed, local_runner_fx)

    assert second.admitted_facts is not None
    assert second.admitted_facts[_ORDERS].count() == 1  # durable authoritative, unconditional
    assert second.post_quarantined_count == 0  # [R2-1]: no append
    assert fixture.quarantine_rows(batch_id) == []  # zero new rows
    assert second.post_check_drift is not None  # drift RECORDED
    assert second.post_check_drift.startswith("post_check drift: durable=0 recomputed=1")


def test_g04c_pre_check_any_door_fires_when_only_the_second_declared_type_has_facts(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    """[DC-1]'s own ANY-door, at pre_check's grain (§8.2's migration note):
    only `shipments` (the SECOND declared type) carries the batch -- proves
    the composition is a genuine `any(...)` over every declared type, not an
    accidental "first type only" check. A row that WOULD quarantine under a
    fresh evaluation (a tightened contract requiring `domain_id`, missing
    here) is used so door 2's own signature -- no append, but a REAL
    recompute-vs-durable mismatch demoted to the drift probe -- is
    observably distinct from door 3's (which would append)."""
    fixture = _Fixture(spark, local_runner_fx, unique_table, allow_ids=("id-1",))
    batch_id = _batch_id(7)
    fixture.insert_fact_row(fixture.shipments_fact_qt, batch_id, "id-1")
    tightened_contract = RawContractModel(
        columns=[
            ColumnSpec(name="domain_id", required=True, nullable=False),
            ColumnSpec(name="amount"),
            ColumnSpec(name="kind"),
        ]
    )
    spec = fixture.spec.model_copy(update={"raw_contract": tightened_contract})
    object_uris = _write_csv(tmp_path / "batch.csv", "domain_id,amount,kind\n,10.00,orders\n")
    seed = fixture.seed(batch_id, object_uris, spec=spec)

    landed = land.run(seed, local_runner_fx)
    after = pre_check.run(landed, local_runner_fx)

    assert after.pre_quarantined_count == 0  # door 2: no append, durable authoritative
    assert after.pre_check_drift is not None  # a REAL mismatch -- recorded, never raised
    assert after.pre_check_drift.startswith("pre_check drift: durable=0 recomputed=1")
    assert "pre_check" not in after.guard_skips  # the quarantine guard was never present
    pre_check_rows = (
        spark.table(fixture.qtn_qt)
        .where(f"batch_id = '{batch_id}' AND check_stage = 'pre_check'")
        .collect()
    )
    assert pre_check_rows == []  # zero new pre_check rows on this door


def test_g04a_kill_between_type_appends_rerun_authority_and_completion(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    make_wrapped_fx: Callable[..., RunnerFx],
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    """A006-11: G-04(a) composed as ONE real kill-between-type-appends ->
    rerun scenario (§13.1 G-04(a); §11's 'Kill between type appends
    (commit), rerun' row) -- not `test_g04a_durable_authority_when_any_
    declared_fact_table_has_the_batch`'s own direct-INSERT stand-in (module
    docstring's own "simulate ... commit's own territory, out of this
    bead's scope" carve-out, now closed at THIS grain).

    A real partial commit: attempt 1 admits BOTH types cleanly (zero
    violations -- the quarantine guard stays ABSENT), then `commit.run`
    itself is killed (`killfx.kill_after(1)` on `fx.append`, K-20's own
    mechanics) right after the FIRST declared type's (orders) own append --
    orders durable, shipments untouched, no completion row. The rerun
    re-enters through `land`(guard-skip)/`pre_check`/`pull`/`apply`/
    `post_check` under a TIGHTENED `ChecksModel` (test_g04b's own drift
    mechanism: a new row check orders' already-admitted candidate would now
    fail) -- post_check's own quarantine guard is still absent, but
    orders' own durable presence opens the ANY-table door ->
    DURABLE_AUTHORITY: both types admitted UNCONDITIONALLY (no check ever
    gates admission), drift recorded (never raised), zero new quarantine
    rows. `commit.run` on the rerun then completes shipments per-table
    (orders guard-skipped, untouched; shipments appended for the first
    time) -- the five points the bead names, all asserted below.

    Dedicated local setup (real bootstrap-DDL fact/state/markers tables),
    not the shared `_Fixture` (module docstring: its own minimal `(domain_
    id, batch_id)` stub predates commit/fold, too narrow for `commit.run`'s
    OWN structural check -- `test_g06_guard_present_drift_born_null_
    variant_ae3`'s own precedent, reused here without AE3's co-effect-drift
    machinery, which this scenario does not need (checks tighten between
    attempts; no domain_id resolution ever changes)."""
    raw_qt = unique_table("g04a_raw")
    qtn_qt = unique_table("g04a_qtn")
    orders_fact_qt = unique_table("g04a_orders_fact")
    orders_state_qt = unique_table("g04a_orders_state")
    shipments_fact_qt = unique_table("g04a_shipments_fact")
    shipments_state_qt = unique_table("g04a_shipments_state")
    ref_qt = unique_table("g04a_ref")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    bootstrap_fact_table(spark, orders_fact_qt, _CANDIDATE_SCHEMA)
    bootstrap_state_table(spark, orders_state_qt, _CANDIDATE_SCHEMA)
    bootstrap_fact_table(spark, shipments_fact_qt, _CANDIDATE_SCHEMA)
    bootstrap_state_table(spark, shipments_state_qt, _CANDIDATE_SCHEMA)
    _create_ref_table(spark, ref_qt, ("id-1",))

    pipeline = f"pipelines/g04a{uuid4().hex[:8]}"
    spec = _make_spec(
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        orders_fact_table=_bare(orders_fact_qt),
        orders_state_table=_bare(orders_state_qt),
        shipments_fact_table=_bare(shipments_fact_qt),
        shipments_state_table=_bare(shipments_state_qt),
        co_effects={"allow_ref": CoEffectDecl(table=_bare(ref_qt))},
    ).model_copy(update={"pipeline": pipeline})
    markers_qt = naming.qualified(naming.markers_table(spec.raw_table, spec.pipeline))
    bootstrap_markers_table(spark, markers_qt)

    object_uris = _write_csv(
        tmp_path / "batch.csv", "domain_id,amount,kind\nid-1,10.00,orders\nid-2,1.00,shipments\n"
    )
    batch_id = _batch_id(200)

    # Attempt 1: BOTH types resolve cleanly (id-1 known, positive; id-2 --
    # shipments binds no checks at all) -- zero violations, quarantine
    # guard ABSENT.
    after1 = _run_through_post_check(
        _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris), local_runner_fx
    )
    assert after1.post_quarantined_count == 0
    assert after1.admitted_facts[_ORDERS].count() == 1
    assert after1.admitted_facts[_SHIPMENTS].count() == 1

    # Commit, killed after the FIRST declared type's own append (orders) --
    # before shipments' own processing and before the unconditional
    # completion row (K-20's own mechanics, one call site over).
    killed_fx = make_wrapped_fx(local_runner_fx, {"append": killfx.kill_after(1)})
    with pytest.raises(killfx.SimulatedKill):
        commit_stage.run(after1, killed_fx)

    assert local_runner_fx.read_batch(_bare(orders_fact_qt), batch_id).count() == 1
    assert local_runner_fx.read_batch(_bare(shipments_fact_qt), batch_id).count() == 0
    assert (
        spark.table(markers_qt)
        .where(f"batch_id = '{batch_id}' AND table_name = '{naming.COMMIT_COMPLETION_SENTINEL}'")
        .isEmpty()
    )

    # Rerun: tighten checks.yaml between attempts (a real rule change) --
    # orders' already-admitted id-1 (amount=10) would now fail a NEW row
    # check under fresh evaluation.
    tightened_checks = ChecksModel(
        checks=[
            *_CHECKS.checks,
            RowCheckModel(
                kind="row",
                id="amount-under-5",
                fact_type=_ORDERS,
                expr="amount < 5",
                reason="business/amount-too-high",
            ),
        ]
    )
    tightened_spec = spec.model_copy(update={"checks": tightened_checks})
    after2 = _run_through_post_check(
        _make_seed(spec=tightened_spec, batch_id=batch_id, object_uris=object_uris),
        local_runner_fx,
    )

    # 1. DURABLE_AUTHORITY chosen -- unconditional admission despite the
    #    tightened check, the quarantine guard never accretes.
    assert "post_check" not in after2.guard_skips
    assert after2.admitted_facts[_ORDERS].count() == 1
    # 2. post_check_drift recorded (the tightened rule), never raised.
    assert after2.post_check_drift is not None
    assert after2.post_check_drift.startswith("post_check drift: durable=0 recomputed=1")
    # 3. no new quarantine rows.
    assert after2.post_quarantined_count == 0
    post_check_qtn_rows = spark.table(qtn_qt).where(
        f"batch_id = '{batch_id}' AND check_stage = 'post_check'"
    )
    assert post_check_qtn_rows.isEmpty()

    after_commit2 = commit_stage.run(after2, local_runner_fx)
    assert dict(after_commit2.facts_appended_by_table) == {
        _bare(orders_fact_qt): 0,
        _bare(shipments_fact_qt): 1,
    }
    assert "commit" in after_commit2.guard_skips

    orders_final = sorted(
        r["domain_id"]
        for r in local_runner_fx.read_batch(_bare(orders_fact_qt), batch_id).collect()
    )
    shipments_final = sorted(
        r["domain_id"]
        for r in local_runner_fx.read_batch(_bare(shipments_fact_qt), batch_id).collect()
    )
    # 4. B's facts appended.
    assert shipments_final == ["id-2"]
    # 5. A's untouched -- no duplicate append.
    assert orders_final == ["id-1"]


# ============================================================================
# G-06: NULL-domain_id end-to-end
# ============================================================================


def test_g06_null_domain_id_implicit_check_fires_first_no_record_key(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    fixture = _Fixture(spark, local_runner_fx, unique_table, allow_ids=("id-1",))
    batch_id = _batch_id(8)
    # A NULL domain_id row -- pre_check admits it (the raw contract declares
    # `domain_id` nullable, module docstring); it fails BOTH the implicit
    # check and (vacuously, per P-8's NULL-key rule) never reaches the
    # membership check's fail predicate at all (NULL key -> no failure from
    # that check specifically) -- the implicit check alone must fire, first.
    object_uris = _write_csv(tmp_path / "batch.csv", "domain_id,amount,kind\n,10.00,orders\n")
    seed = fixture.seed(batch_id, object_uris)

    _run_through_post_check(seed, local_runner_fx)

    rows = fixture.quarantine_rows(batch_id)
    assert len(rows) == 1
    row = rows[0]
    assert row["domain_id"] is None
    assert row["reason_code"] == "business/missing-domain-id"  # implicit check, entry 0
    detail = json.loads(row["reason_detail"])
    assert detail[0]["id"] == "missing-domain-id"
    assert row["record_key"] is None  # domain_id_col IS a record_key column -> NULL gate


def test_g06_null_domain_id_survives_when_domain_id_col_outside_record_key(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    """D-6's other half: a NULL `domain_id_col` value does NOT block
    `record_key` derivation when `domain_id_col` is NOT itself one of the
    declared `record_key:` columns -- a distinct schema (own `record_key`)
    proves the gate reads the DECLARATION, not "is this domain_id"."""
    schema = FactSchemaModel(
        columns=[
            FactColumnSpec(name="domain_id", type="string"),
            FactColumnSpec(name="amount", type="decimal(10,2)"),
        ],
        domain_id_col="domain_id",
        record_key=["amount"],  # deliberately NOT domain_id
    )
    fixture = _Fixture(spark, local_runner_fx, unique_table, allow_ids=("id-1",))
    spec = fixture.spec.model_copy(
        update={
            "fact_types": {
                _ORDERS: FactTypeModel(
                    fact_table=fixture.spec.fact_types[_ORDERS].fact_table,
                    state_table=fixture.spec.fact_types[_ORDERS].state_table,
                    schema=schema,
                ),
                _SHIPMENTS: fixture.spec.fact_types[_SHIPMENTS],
            }
        }
    )
    batch_id = _batch_id(9)
    object_uris = _write_csv(tmp_path / "batch.csv", "domain_id,amount,kind\n,10.00,orders\n")
    seed = fixture.seed(batch_id, object_uris, spec=spec)

    _run_through_post_check(seed, local_runner_fx)

    rows = fixture.quarantine_rows(batch_id)
    assert len(rows) == 1
    assert rows[0]["domain_id"] is None
    assert rows[0]["record_key"] is not None  # amount is non-null -> record_key derives


def test_g06_guard_present_drift_born_null_variant_ae3(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    """G-06's SECOND named variant, UN-SKIPPED for B10 (bead conveyer-6pg.22,
    addendum 2: 'commit landed (B9b); implement and un-skip it now') --
    007.1 §13.1's K-13, §7.3 path 2's mechanics: 'a drift-born NULL-domain_id
    candidate the original attempt never adjudicated survives the durable-
    hash anti-join and reaches the backstop through DURABLE_SUBTRACT --
    named defect, per-table wedge.'

    Real bootstrap-DDL fact/state/markers tables (`bootstrap_fact_table`/
    `bootstrap_state_table`/`bootstrap_markers_table`) for BOTH types --
    this file's own shared `_Fixture`/`_create_fact_table` is a minimal
    `(domain_id, batch_id)` stub (predates commit/fold, module docstring's
    "stopping BEFORE commit/fold"), too narrow for `commit.run`'s OWN
    structural check; a dedicated local spec is built here instead of
    widening the shared fixture other G-suite tests depend on.

    **The mechanism (scratch-validated, this bead):** a custom `apply`
    resolves `domain_id` via a LEFT JOIN against the `allow_ref` co-effect
    (NULL when unmatched) -- a realistic "resolve via lookup" transform
    shape, unlike this file's own shared `_apply` (which never reads co-
    effects at all and so cannot express this variant). Attempt 1: raw
    row "id-1" (present in `allow_ref`) resolves cleanly and is ADMITTED;
    a second row "id-bad" (negative amount) is a genuine `positive-amount`
    violation, durably quarantined -- the quarantine guard `(batch_id,
    "post_check")` is now PRESENT. Attempt 1 stops HERE, never calling
    `commit.run` -- direct stage calls have no `run.py` sequencing to
    interrupt, so simply not proceeding IS the "killed after post append,
    before commit" state (mirrors R-03's own "the pre-land kill point ...
    before its own fx.append ever executes" reasoning, one call site over).
    Between attempts, `allow_ref` is DRIFTED (`id-1` removed) -- attempt 2
    recomputes `apply` fresh (`pre_check`'s own zero-violation branch never
    guard-skips; `pull` always re-reads current co-effects) and "id-1" now
    resolves to a NULL `domain_id` -- a row post_check's DURABLE_SUBTRACT
    door (guard present) admits via hash anti-join against the durable
    violation set: the NULL-domain_id row's hash matches nothing durable
    (attempt 1 never produced it), so it SURVIVES into `admitted_facts
    ["orders"]`, unevaluated. `commit.run` on this admitted set hits I-24's
    per-table structural backstop -- the named defect, "orders" wedged,
    "shipments" (declared but empty, F-4's zero-fact corollary) untouched.
    """
    raw_qt = unique_table("ae3_raw")
    qtn_qt = unique_table("ae3_qtn")
    orders_fact_qt = unique_table("ae3_orders_fact")
    orders_state_qt = unique_table("ae3_orders_state")
    shipments_fact_qt = unique_table("ae3_shipments_fact")
    shipments_state_qt = unique_table("ae3_shipments_state")
    ref_qt = unique_table("ae3_ref")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    bootstrap_fact_table(spark, orders_fact_qt, _CANDIDATE_SCHEMA)
    bootstrap_state_table(spark, orders_state_qt, _CANDIDATE_SCHEMA)
    bootstrap_fact_table(spark, shipments_fact_qt, _CANDIDATE_SCHEMA)
    bootstrap_state_table(spark, shipments_state_qt, _CANDIDATE_SCHEMA)
    spark.sql(f"CREATE TABLE {ref_qt} (id STRING) USING iceberg")
    spark.sql(f"INSERT INTO {ref_qt} VALUES ('id-1')")

    pipeline = f"pipelines/ae3{uuid4().hex[:8]}"
    spec = _make_spec(
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        orders_fact_table=_bare(orders_fact_qt),
        orders_state_table=_bare(orders_state_qt),
        shipments_fact_table=_bare(shipments_fact_qt),
        shipments_state_table=_bare(shipments_state_qt),
        co_effects={"allow_ref": CoEffectDecl(table=_bare(ref_qt))},
    ).model_copy(update={"pipeline": pipeline})
    markers_qt = naming.qualified(naming.markers_table(spec.raw_table, spec.pipeline))
    bootstrap_markers_table(spark, markers_qt)

    def _apply_with_ref_resolution(
        valid_df: DataFrame, co_effects: Mapping[str, DataFrame]
    ) -> Mapping[str, DataFrame]:
        ref = co_effects["allow_ref"].select(F.col("id").alias("_ref_id"))
        resolved = valid_df.join(ref, valid_df["domain_id"] == ref["_ref_id"], "left")
        resolved = resolved.withColumn("domain_id", F.col("_ref_id")).drop("_ref_id")
        casted = resolved.withColumn("amount", F.col("amount").cast("decimal(10,2)"))
        orders = casted.filter(F.col("kind") == _ORDERS).select("domain_id", "amount")
        shipments = casted.filter(F.col("kind") == _SHIPMENTS).select("domain_id", "amount")
        return {_ORDERS: orders, _SHIPMENTS: shipments}

    def _seed(batch_id: str, object_uris: tuple[str, ...]) -> BatchContext:
        transforms = Transforms(apply=_apply_with_ref_resolution)
        return BatchContext(
            pipeline=pipeline,
            feed_id="feed/ae3",
            delivery_id=str(UUID(int=1, version=4)),
            batch_id=batch_id,
            delivery_key="statement.csv",
            content_hash="sha256:" + "a" * 64,
            object_uris=object_uris,
            received_at=datetime(2026, 1, 1, tzinfo=UTC),
            spec=spec,
            run=RunConfig(),
            transforms=transforms,
            attempt_id="attempt-1",
            sfn_retry_count=0,
            sfn_redrive_count=0,
            read_spec_version=read_spec_version(spec.read),
            check_version=check_version(spec.raw_contract, spec.read),
            checks_version=checks_version(spec.checks),
        )

    object_uris = _write_csv(
        tmp_path / "batch.csv", "domain_id,amount,kind\nid-1,10.00,orders\nid-bad,-5.00,orders\n"
    )
    batch_id = _batch_id(101)

    # Attempt 1: FRESH post_check -- "id-1" (in `allow_ref`) admits cleanly;
    # "id-bad" (negative amount) is a genuine violation, durably quarantined.
    after1 = _run_through_post_check(_seed(batch_id, object_uris), local_runner_fx)
    assert after1.post_quarantined_count == 1
    admitted1 = sorted(r["domain_id"] for r in after1.admitted_facts[_ORDERS].collect())
    assert admitted1 == ["id-1"]
    # "killed" here -- attempt 1 never calls commit.run (module docstring).

    # Drift the co-effect between attempts: "id-1" is no longer a known ref.
    spark.sql(f"DELETE FROM {ref_qt} WHERE id = 'id-1'")

    # Attempt 2: land guard-skips (raw already committed); pre_check/pull/
    # apply genuinely re-run (pre_check's own zero-violation branch never
    # guard-skips; pull always re-reads current co-effects) -- "id-1" now
    # resolves to a NULL domain_id. post_check's quarantine guard IS present
    # (attempt 1's "id-bad" row) -> DURABLE_SUBTRACT: the recomputed NULL-
    # domain_id candidate hashes to nothing durable and survives the anti-join.
    after2 = _run_through_post_check(_seed(batch_id, object_uris), local_runner_fx)
    assert "land" in after2.guard_skips
    assert "post_check" in after2.guard_skips  # quarantine guard present -> DURABLE_SUBTRACT
    admitted2 = after2.admitted_facts[_ORDERS].collect()
    assert len(admitted2) == 1
    assert admitted2[0]["domain_id"] is None  # the drift-born candidate, unevaluated

    # The backstop converts it into the named, per-table defect at commit --
    # never a silent fold break, never a quarantine row (§7.3's own words).
    with pytest.raises(ValueError, match="I-24"):
        commit_stage.run(after2, local_runner_fx)

    # per-table wedge: "orders" never appended -- verified via a durable
    # read-back (fx.read_batch), not a stale ctx field.
    orders_committed = local_runner_fx.read_batch(_bare(orders_fact_qt), batch_id)
    assert orders_committed.count() == 0


# ============================================================================
# K-12: the durable-authority door -> unevaluated set -> I-24 backstop
# (007.1 §7.3 path 3, §13.1 K-12 -- the 006.1 G-06 twin's FIRST variant)
# ============================================================================


def test_k12_durable_authority_door_admits_drift_born_null_backstop_names_defect(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    make_wrapped_fx: Callable[..., RunnerFx],
    unique_table: Callable[[str], str],
    tmp_path: Path,
) -> None:
    """A007-2: K-12 -- distinct from `test_g06_guard_present_drift_born_
    null_variant_ae3` (K-13's own DURABLE_SUBTRACT anchor, quarantine guard
    PRESENT). Here the quarantine guard is ABSENT and the ANY-table door
    (DURABLE_AUTHORITY) admits the whole candidate set UNEVALUATED, per
    §7.3 path 3: 'N-type batch killed between type appends (A has facts, B
    not), rerun under drift, ANY-table door admits an unevaluated set, B's
    append proceeds, a drift-born NULL-domain_id row reaches the backstop.'

    Clones `test_g06_guard_present_drift_born_null_variant_ae3`'s own
    fixture shape (real bootstrap-DDL tables, `allow_ref` co-effect,
    LEFT-JOIN `apply` resolving `domain_id` via lookup) -- `_Fixture`'s own
    minimal `(domain_id, batch_id)` stub is too narrow for `commit.run`'s
    OWN structural check (AE3's own reasoning, restated). Attempt 1 admits
    BOTH types cleanly (id-1/id-2 both resolve via `allow_ref`) -- zero
    violations, quarantine guard ABSENT (K-12's own precondition, the
    opposite of AE3's guard-PRESENT one). `commit.run` is killed
    (`killfx.kill_after(1)` on `fx.append`) right after orders' own append:
    orders durable, shipments untouched, no completion row. Between
    attempts, `allow_ref` drops "id-2" (shipments' own row) -- orders'
    "id-1" stays resolved (already durable, never re-evaluated on this
    door). Attempt 2 recomputes fresh (`pull` always re-reads co-effects):
    shipments' "id-2" now resolves to a NULL `domain_id`. post_check's own
    quarantine guard is STILL absent, but orders' durable presence opens
    the ANY-table door -> DURABLE_AUTHORITY: BOTH types admitted
    unconditionally, unevaluated -- shipments' drift-born NULL row survives
    with no check ever gating it. `commit.run` hits I-24's per-table
    backstop for shipments (facts-absent) while orders (facts-present) is
    guard-skipped -- the named defect, never a silent fold break, never a
    quarantine row."""
    raw_qt = unique_table("k12_raw")
    qtn_qt = unique_table("k12_qtn")
    orders_fact_qt = unique_table("k12_orders_fact")
    orders_state_qt = unique_table("k12_orders_state")
    shipments_fact_qt = unique_table("k12_shipments_fact")
    shipments_state_qt = unique_table("k12_shipments_state")
    ref_qt = unique_table("k12_ref")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    bootstrap_fact_table(spark, orders_fact_qt, _CANDIDATE_SCHEMA)
    bootstrap_state_table(spark, orders_state_qt, _CANDIDATE_SCHEMA)
    bootstrap_fact_table(spark, shipments_fact_qt, _CANDIDATE_SCHEMA)
    bootstrap_state_table(spark, shipments_state_qt, _CANDIDATE_SCHEMA)
    spark.sql(f"CREATE TABLE {ref_qt} (id STRING) USING iceberg")
    spark.sql(f"INSERT INTO {ref_qt} VALUES ('id-1'), ('id-2')")

    pipeline = f"pipelines/k12{uuid4().hex[:8]}"
    spec = _make_spec(
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        orders_fact_table=_bare(orders_fact_qt),
        orders_state_table=_bare(orders_state_qt),
        shipments_fact_table=_bare(shipments_fact_qt),
        shipments_state_table=_bare(shipments_state_qt),
        co_effects={"allow_ref": CoEffectDecl(table=_bare(ref_qt))},
    ).model_copy(update={"pipeline": pipeline})
    markers_qt = naming.qualified(naming.markers_table(spec.raw_table, spec.pipeline))
    bootstrap_markers_table(spark, markers_qt)

    def _apply_with_ref_resolution(
        valid_df: DataFrame, co_effects: Mapping[str, DataFrame]
    ) -> Mapping[str, DataFrame]:
        ref = co_effects["allow_ref"].select(F.col("id").alias("_ref_id"))
        resolved = valid_df.join(ref, valid_df["domain_id"] == ref["_ref_id"], "left")
        resolved = resolved.withColumn("domain_id", F.col("_ref_id")).drop("_ref_id")
        casted = resolved.withColumn("amount", F.col("amount").cast("decimal(10,2)"))
        orders = casted.filter(F.col("kind") == _ORDERS).select("domain_id", "amount")
        shipments = casted.filter(F.col("kind") == _SHIPMENTS).select("domain_id", "amount")
        return {_ORDERS: orders, _SHIPMENTS: shipments}

    def _seed(batch_id: str, object_uris: tuple[str, ...]) -> BatchContext:
        transforms = Transforms(apply=_apply_with_ref_resolution)
        return BatchContext(
            pipeline=pipeline,
            feed_id="feed/k12",
            delivery_id=str(UUID(int=1, version=4)),
            batch_id=batch_id,
            delivery_key="statement.csv",
            content_hash="sha256:" + "a" * 64,
            object_uris=object_uris,
            received_at=datetime(2026, 1, 1, tzinfo=UTC),
            spec=spec,
            run=RunConfig(),
            transforms=transforms,
            attempt_id="attempt-1",
            sfn_retry_count=0,
            sfn_redrive_count=0,
            read_spec_version=read_spec_version(spec.read),
            check_version=check_version(spec.raw_contract, spec.read),
            checks_version=checks_version(spec.checks),
        )

    object_uris = _write_csv(
        tmp_path / "batch.csv", "domain_id,amount,kind\nid-1,10.00,orders\nid-2,1.00,shipments\n"
    )
    batch_id = _batch_id(102)

    # Attempt 1: both types resolve cleanly via `allow_ref` -- zero
    # violations, quarantine guard ABSENT.
    after1 = _run_through_post_check(_seed(batch_id, object_uris), local_runner_fx)
    assert after1.post_quarantined_count == 0
    assert after1.admitted_facts[_ORDERS].count() == 1
    assert after1.admitted_facts[_SHIPMENTS].count() == 1

    # Commit, killed after the FIRST declared type's own append (orders) --
    # shipments never processed, no completion row (K-20's own mechanics).
    killed_fx = make_wrapped_fx(local_runner_fx, {"append": killfx.kill_after(1)})
    with pytest.raises(killfx.SimulatedKill):
        commit_stage.run(after1, killed_fx)

    assert local_runner_fx.read_batch(_bare(orders_fact_qt), batch_id).count() == 1
    assert local_runner_fx.read_batch(_bare(shipments_fact_qt), batch_id).count() == 0
    assert (
        spark.table(markers_qt)
        .where(f"batch_id = '{batch_id}' AND table_name = '{naming.COMMIT_COMPLETION_SENTINEL}'")
        .isEmpty()
    )

    # Drift the co-effect between attempts: "id-2" (shipments' own row) is
    # no longer a known ref -- orders' own "id-1" stays resolved.
    spark.sql(f"DELETE FROM {ref_qt} WHERE id = 'id-2'")

    # Attempt 2: post_check's own quarantine guard is STILL absent, but
    # orders' durable presence opens the ANY-table door -> DURABLE_AUTHORITY.
    after2 = _run_through_post_check(_seed(batch_id, object_uris), local_runner_fx)
    assert after2.post_quarantined_count == 0
    assert "post_check" not in after2.guard_skips  # the quarantine guard was never present
    admitted2_shipments = after2.admitted_facts[_SHIPMENTS].collect()
    assert len(admitted2_shipments) == 1
    assert admitted2_shipments[0]["domain_id"] is None  # the drift-born candidate, unevaluated

    # The backstop converts it into the named, per-table defect at commit --
    # orders (facts-present) is guard-skipped; shipments (facts-absent)
    # wedges. Never a silent fold break, never a quarantine row (§7.3).
    with pytest.raises(ValueError, match="I-24"):
        commit_stage.run(after2, local_runner_fx)

    shipments_committed = local_runner_fx.read_batch(_bare(shipments_fact_qt), batch_id)
    assert shipments_committed.count() == 0
    assert (
        spark.table(markers_qt)
        .where(f"batch_id = '{batch_id}' AND table_name = '{naming.COMMIT_COMPLETION_SENTINEL}'")
        .isEmpty()
    )


# ============================================================================
# G-12: exemplar migration goldens (real pipelines.identity/identity_violations)
# ============================================================================


def test_g12_identity_exemplar_apply_returns_one_entry_mapping(spark: SparkSession) -> None:
    import pipelines.identity.transforms as identity_transforms

    valid_df = spark.createDataFrame(
        [("id-1", "2026-01-01T00:00:00Z", "hello")], ["domain_id", "event_time", "payload"]
    )

    returned = identity_transforms.apply(valid_df, {})

    assert set(returned.keys()) == {"identity"}
    assert sorted(returned["identity"].columns) == sorted(["domain_id", "event_time", "payload"])
    assert not hasattr(identity_transforms, "post_check")


def test_g12_identity_violations_variant_has_no_post_check_re_exports_apply() -> None:
    import pipelines.identity.transforms as identity_transforms
    import pipelines.identity_violations.transforms as violations_transforms

    assert violations_transforms.apply is identity_transforms.apply
    assert not hasattr(violations_transforms, "post_check")
