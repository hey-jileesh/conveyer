"""Standing scenario suite (M4 gate) — R-05, R-12. LLD §12.4, §6.5, §9, §11.3,
I-10, I-22.

Own scenario-test file (bead conveyer-nvh.24); mirrors, rather than imports,
`test_scenarios_core.py`'s small `_bare`/`_create_*_table`/`_make_spec`/
`_make_seed`/`_batch_id` helpers — every integration suite in this package
carries its own copies of these (see `test_stages_post_commit_fold_publish.py`
and `test_stages_land_pre_pull_apply.py` for the identical, deliberate
pattern: `tests/` has no `__init__.py`, so a shared helper would have to live
in a dedicated non-test module like `snapshot_asserts.py`, and these helpers
are small enough that duplicating them per file costs less than a shared
import would).

**R-05** (`test_r05`): (a) an R-01-style fresh run over the identity exemplar
produces one run-ledger row per stage (all 8 of §7.3's `_SEQUENCE_ORDER`,
`pull` included), all `ok`, read back via `ledger_catalog` (pyiceberg, a
DIFFERENT catalog client from Spark's own `spine_cat`) — every §6.5 column
asserted per-stage, including `pull`'s own `co_effect_snapshot_ids` map
(incl. the I-6 zero-snapshot `-1` sentinel) and the fold's
`state_read_snapshot_id`/`merge_summary`. (b) a rerun over a FRESH
`BatchContext`, same `batch_id` (R-02's own precondition), produces one more
row per stage: `land`/`commit` (the only two guard-skip-capable stages for a
clean fixture, per R-02) come back `skipped-guard`, `commit`'s
`facts_appended = 0` signature explicit; `pre_check`/`pull`/`post_check`/
`fold` take their zero-violation / no-guard-skip-mechanism branches (§7.5) —
all four read back `ok`. (c) two one-shot failure injections (mirroring the
R-03 `KillFx` mechanism, simplified to "the very first effect call always
raises" rather than "the Nth commit raises then a rerun recovers" — this
bead only needs the resulting `failed` ledger row, not the recovery): a
`TransientError` wraps `read_objects` (land's first effect) — the resulting
row's `error_message` is `"TransientError: {first line}"` (§6.5 [S-7]); a
bare `ValueError` — a stand-in for "any other, non-spine-raised type" —
from the same injection point produces a row whose `error_message` is the
raising frame's `module:lineno`, no message text (proving the sensitive
planted string never reaches the row).

**Fixed bug, was open (bead conveyer-nvh.24 found it, conveyer-nvh.36 fixed
it)**: the `pull` stage's run-ledger row used to be silently lost on EVERY
attempt, for EVERY pipeline — `stages/pull.py` always wraps `BatchContext.
co_effect_snapshot_ids` in `types.MappingProxyType` (`context.py`'s own
documented construction-site contract, applied even to an EMPTY mapping);
`effects/ledger.py::_row_from_run_fact` used to derive the ledger row via
`dataclasses.asdict(run_fact)`, whose generic branch `copy.deepcopy`s any
field that is not itself a dataclass/list/tuple/dict — `MappingProxyType`
unconditionally fails `copy.deepcopy` (`TypeError: cannot pickle
'mappingproxy' object`, verified even for an empty mapping proxy — a
CPython-level limitation, not content-dependent). `record_run`'s own outer
`try/except Exception: pass` (the deliberate "never raises" contract,
§11.3) used to swallow this `TypeError` BEFORE `_try_append`/
`_log_ledger_loss` ever ran, so neither the WARNING log nor the
`RunLedgerLoss` metric fired either. **Fix** (`effects/ledger.py`):
`_row_from_run_fact` now builds the row via explicit `dataclasses.fields` +
`getattr` (never `asdict`'s deepcopy), converting any `Mapping`-typed value
to a plain `dict` at the boundary; `record_run`'s failure topology was also
hardened so a row-DERIVATION failure (not just an exhausted append budget)
lands in the same WARNING + `RunLedgerLoss` path (`tests/unit/test_ledger.
py::test_record_run_row_derivation_failure_hits_warning_and_loss_metric_
not_silence` pins this with a poisoned-`Mapping` `RunFact`). `test_r05`
below now asserts the full, NORMATIVE 8-rows-per-attempt shape;
`test_pull_ledger_row_co_effect_snapshot_ids_mappingproxy` (the former
`strict=True` xfail, un-xfailed by this fix) is the direct, minimal
reproduction, now passing.

**R-12** (`test_r12`): four binding/seed defects, each raising strictly
*before* `spine.run` is ever called — malformed `batch_id` (the seed model's
own UUIDv5 pattern, I-22); forged `object_uris` naming a foreign feed
(`core.naming.check_object_uris`, I-22); an out-of-namespace
`transforms_module` (rejected at `PipelineSpecModel` construction itself —
the pydantic `Field(pattern=...)` grammar, I-10 — never reaching
`bind_transforms`); and a `transforms_module` that IS in-namespace but
genuinely unimportable (passes the `PipelineSpecModel` grammar, fails inside
`bind_transforms`'s own `importlib.import_module`, I-10). `_entrypoint_order`
below is a tiny in-test helper reproducing LLD §8.3's fail-fast sequence
(parse seed → `check_object_uris` → parse `PipelineSpecModel` →
`bind_transforms` → seed `BatchContext` → `spine.run`) without importing
`spine.entrypoints.glue_main` — that module is still M5's one-line stub
(bead conveyer-nvh.27, not yet built) and, per its own docstring, is meant to
carry zero logic of its own anyway (all of it lives in the functions this
helper calls directly). Each case asserts the *complete* absence of a
data-path trace: no raw rows for the attempted `batch_id`, no bus events, no
ledger rows — the direct, mechanical consequence of the defect raising
before `BatchContext` (let alone `run()`) is ever constructed.

A third, standalone test (`test_record_run_call_sites_only_in_run_py`)
reifies §11.3's review rule as a textual scan: `fx.record_run(...)` — the
attribute-call shape, unambiguous against `effects/ledger.py`'s own `def
record_run(...)` closure definition and `effects/records.py`'s `record_run:
Callable[...]` field annotation — appears nowhere under `spine/spine/**`
except `run.py`.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

import pytest
from pydantic import ValidationError
from pyspark.sql import SparkSession
from spine.binding import bind_transforms
from spine.bootstrap.create_admission_tables import (
    render_quarantine_create_table_sql,
    render_raw_create_table_sql,
)
from spine.config import RunConfig, RunnerConfig
from spine.context import BatchContext
from spine.core import naming
from spine.core.contract import check_version, read_spec_version
from spine.core.model import (
    CoEffectDecl,
    ColumnSpec,
    DeliveryRegisteredV1,
    DialectModel,
    PipelineSpecModel,
    RawContractModel,
    ReadSpecModel,
)
from spine.core.run_facts import RunFact
from spine.effects import ledger
from spine.effects.records import RunnerFx, TransientError
from spine.run import run as run_sequence

if TYPE_CHECKING:
    from tests.conftest import LedgerCatalogFixture, MotoEventsBus

_CATALOG_PREFIX = "spine_cat."
_EXEMPLAR_DIR = Path(__file__).resolve().parent.parent / "exemplar" / "identity"
_FIXTURES_DIR = _EXEMPLAR_DIR / "fixtures"

_ROW_DDL = (
    "domain_id STRING, event_time STRING, source_ts STRING, content_hash STRING, "
    "payload STRING, batch_id STRING, delivery_id STRING, feed_id STRING, "
    "received_at TIMESTAMP"
)

# §7.3 STAGES order -- also `spine/stages/__init__.py::SEQUENCE`'s own order.
_SEQUENCE_ORDER = ("land", "pre_check", "pull", "apply", "post_check", "commit", "fold", "publish")

# §6.5's per-stage nullable count/snapshot columns -- used to assert "only
# the fields THIS stage produced" are non-null on a stage's own row.
_STAGE_ONLY_FIELDS = (
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
)


def _bare(qualified_table: str) -> str:
    assert qualified_table.startswith(_CATALOG_PREFIX)
    return qualified_table.removeprefix(_CATALOG_PREFIX)


def _create_raw_table(spark: SparkSession, qualified_table: str) -> None:
    # 005.1 §4.4 DDL parity swap (bead conveyer-azr.19, n3-admission-cut):
    # the SAME builder `bootstrap-admission` issues in production, not a
    # hand-rolled `_ROW_DDL` shape -- see `scenario_helpers.py`'s own
    # identical fix and this file's own docstring on why this module keeps
    # its own copy rather than importing that one.
    spark.sql(render_raw_create_table_sql(qualified_table, _IDENTITY_RAW_CONTRACT))


def _create_quarantine_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(render_quarantine_create_table_sql(qualified_table))


def _create_fact_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(f"CREATE TABLE {qualified_table} ({_ROW_DDL}) USING iceberg")


def _create_state_table(spark: SparkSession, qualified_table: str) -> None:
    # write.merge.mode=merge-on-read: the fold no-op detection precondition
    # (effects/spark.py's own documented empirical finding).
    spark.sql(
        f"CREATE TABLE {qualified_table} ({_ROW_DDL}) USING iceberg "
        "TBLPROPERTIES ('write.merge.mode'='merge-on-read')"
    )


def _create_coeff_table(spark: SparkSession, qualified_table: str) -> None:
    """Created, never inserted into -- zero snapshots (I-6's `-1` sentinel)."""
    spark.sql(f"CREATE TABLE {qualified_table} (k STRING) USING iceberg")


def _batch_id(n: int) -> str:
    return str(uuid.UUID(int=n, version=5))


# 005.1 §3.4/A-12: mirrors, rather than imports (this file's own docstring),
# `scenario_helpers.py`'s `IDENTITY_READ`/`IDENTITY_RAW_CONTRACT` -- same 5
# non-framework `_ROW_DDL` columns, `domain_id` `required: true, nullable:
# false` -- the one contract-declared constraint `stages/pre_check.py`'s
# real `compile_contract` (§6.1) compiles into its `not-nullable` check.
_IDENTITY_READ = ReadSpecModel(dialect=DialectModel(format="csv", header=True))
_IDENTITY_RAW_CONTRACT = RawContractModel(
    columns=[
        ColumnSpec(name="domain_id", required=True, nullable=False),
        ColumnSpec(name="event_time"),
        ColumnSpec(name="source_ts"),
        ColumnSpec(name="content_hash"),
        ColumnSpec(name="payload"),
    ]
)


def _make_spec(
    *,
    transforms_module: str,
    raw_table: str,
    quarantine_table: str,
    fact_table: str,
    state_table: str,
    co_effects: dict[str, CoEffectDecl] | None = None,
) -> PipelineSpecModel:
    return PipelineSpecModel(
        pipeline="pipelines/identity",
        transforms_module=transforms_module,
        raw_table=raw_table,
        quarantine_table=quarantine_table,
        fact_table=fact_table,
        state_table=state_table,
        read=_IDENTITY_READ,
        raw_contract=_IDENTITY_RAW_CONTRACT,
        co_effects=co_effects or {},
    )


def _make_seed(
    *, spec: PipelineSpecModel, batch_id: str, object_uris: tuple[str, ...]
) -> BatchContext:
    return BatchContext(
        pipeline="pipelines/identity",
        feed_id="feed/identity",
        delivery_id=str(uuid.UUID(int=1, version=4)),
        batch_id=batch_id,
        delivery_key="statement.csv",
        content_hash="sha256:" + "a" * 64,
        object_uris=object_uris,
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        spec=spec,
        run=RunConfig(),
        transforms=bind_transforms(spec),
        attempt_id="attempt-1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        read_spec_version=read_spec_version(spec.read),
        check_version=check_version(spec.raw_contract, spec.read),
    )


def _ledger_rows_for(ledger_catalog: LedgerCatalogFixture, batch_id: str) -> list[dict[str, Any]]:
    return [row for row in ledger_catalog.rows() if row["batch_id"] == batch_id]


def _raise_once(exc: BaseException) -> Callable[[Callable[..., object]], Callable[..., object]]:
    """`make_wrapped_fx` wrapper factory: the wrapped field becomes a
    callable that unconditionally raises `exc`, ignoring the original
    callable and every argument -- a one-shot failure injection (simpler
    than R-03's own `KillFx`: this bead only needs "the very first effect
    call fails", never a "raise once then recover" rerun)."""

    def _wrapper(_original: Callable[..., object]) -> Callable[..., object]:
        def _raiser(*args: object, **kwargs: object) -> object:
            raise exc

        return _raiser

    return _wrapper


def _canonical_uri(
    *, feed_id: str, delivery_id: str, received_at: datetime, landing_bucket: str, name: str
) -> str:
    """Builds a VALID I-22 canonical-shape URI the same way an honest
    producer would -- mirrors (does not import) `core/naming.py`'s own
    module-private `_format_received_at`/`_canonical_prefix` composition, so
    each R-12 sub-case below can isolate its OWN defect (a forged feed/
    delivery, a malformed batch_id, an out-of-namespace/broken module)
    without a coincidentally-also-forged URI muddying which check fired."""
    utc = received_at.astimezone(UTC)
    stamp = utc.strftime("%Y%m%dT%H%M%S") + f"{utc.microsecond:06d}Z"
    return f"s3://{landing_bucket}/{feed_id}/received_at={stamp}/dl-{delivery_id}/{name}"


def _valid_seed_json(
    *,
    batch_id: str,
    delivery_id: str,
    feed_id: str,
    received_at: datetime,
    object_uris: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "feed_id": feed_id,
        "delivery_id": delivery_id,
        "batch_id": batch_id,
        "delivery_key": "statement.csv",
        "content_hash": "sha256:" + "a" * 64,
        "size_bytes": 123,
        "object_uris": list(object_uris),
        "received_at": received_at.isoformat(),
        "pipeline": "pipelines/identity",
    }


def _valid_spec_data(
    *,
    transforms_module: str,
    raw_table: str,
    quarantine_table: str,
    fact_table: str,
    state_table: str,
) -> dict[str, Any]:
    return {
        "pipeline": "pipelines/identity",
        "transforms_module": transforms_module,
        "raw_table": raw_table,
        "quarantine_table": quarantine_table,
        "fact_table": fact_table,
        "state_table": state_table,
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
    }


def _entrypoint_order(
    *,
    seed_json: dict[str, Any],
    spec_data: dict[str, Any],
    config: RunnerConfig,
    fx: RunnerFx,
) -> BatchContext:
    """Mirrors LLD §8.3's fail-fast order WITHOUT importing `spine.
    entrypoints.glue_main` (M5, bead conveyer-nvh.27 -- still a one-line
    stub, and "any logic in the entrypoint is a review defect" per that
    section anyway, so every step below already lives in a function this
    helper merely calls in the documented order): parse seed
    (`DeliveryRegisteredV1`) -> `naming.check_object_uris` (I-22) -> parse
    `PipelineSpecModel` (I-10's namespace grammar) -> `bind_transforms`
    (I-10's importability/export/arity checks) -> seed `BatchContext` ->
    `spine.run`. Any raise below happens strictly before `run()` is ever
    reached -- R-12's "no raw rows, no events, no ledger rows" claim follows
    directly from this ordering, not from a separate guard this helper would
    otherwise have to fake."""
    seed_model = DeliveryRegisteredV1(**seed_json)
    naming.check_object_uris(
        seed_model.feed_id,
        seed_model.delivery_id,
        seed_model.received_at,
        seed_model.object_uris,
        config.landing_bucket,
    )
    spec = PipelineSpecModel(**spec_data)
    transforms = bind_transforms(spec)
    ctx = BatchContext(
        pipeline=seed_model.pipeline,
        feed_id=seed_model.feed_id,
        delivery_id=seed_model.delivery_id,
        batch_id=seed_model.batch_id,
        delivery_key=seed_model.delivery_key,
        content_hash=seed_model.content_hash,
        object_uris=tuple(seed_model.object_uris),
        received_at=seed_model.received_at,
        spec=spec,
        run=RunConfig(),
        transforms=transforms,
        attempt_id="attempt-1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        read_spec_version=read_spec_version(spec.read),
        check_version=check_version(spec.raw_contract, spec.read),
    )
    return run_sequence(ctx, fx)


# --- R-05: run ledger records every stage -----------------------------------


def test_r05(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    ledger_catalog: LedgerCatalogFixture,
    moto_events_bus: MotoEventsBus,
    make_wrapped_fx: Callable[..., RunnerFx],
) -> None:
    raw_qt = unique_table("r05_raw")
    qtn_qt = unique_table("r05_qtn")
    fact_qt = unique_table("r05_fact")
    state_qt = unique_table("r05_state")
    coeff_qt = unique_table("r05_coeff")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)
    _create_coeff_table(spark, coeff_qt)

    spec = _make_spec(
        transforms_module="pipelines.identity.transforms",
        raw_table=_bare(raw_qt),
        quarantine_table=_bare(qtn_qt),
        fact_table=_bare(fact_qt),
        state_table=_bare(state_qt),
        co_effects={"lookup": CoEffectDecl(table=_bare(coeff_qt))},
    )
    object_uris = (
        str(_FIXTURES_DIR / "clean" / "object_1.csv"),
        str(_FIXTURES_DIR / "clean" / "object_2.csv"),
    )

    # --- (a) fresh run: one ok row per stage (all 8, `pull` included), in
    # order, full §6.5 field check per stage -----------------------------
    batch_id = _batch_id(501)
    seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)

    result = run_sequence(seed, local_runner_fx)
    moto_events_bus.read_events()  # drain -- events are R-01's own concern

    assert result.facts_appended == 3
    assert result.guard_skips == ()
    # BatchContext-level proof the co-effect was genuinely pulled --
    # test_stages_land_pre_pull_apply.py already covers this at the
    # stage-unit level; re-asserted here since this run's own spec declares
    # one, and the ledger-row assertion below (`pull_row`) checks the SAME
    # value made it into the run-ledger, not just `BatchContext`.
    assert dict(result.co_effect_snapshot_ids) == {"lookup": -1}

    rows = _ledger_rows_for(ledger_catalog, batch_id)
    assert len(rows) == 8
    rows_sorted = sorted(rows, key=lambda row: row["recorded_at"])
    assert [row["stage"] for row in rows_sorted] == list(_SEQUENCE_ORDER)
    assert {row["outcome"] for row in rows} == {"ok"}  # fresh run: no guard-skip anywhere

    by_stage = {row["stage"]: row for row in rows}
    for row in rows:
        assert row["batch_id"] == batch_id
        assert row["pipeline"] == "pipelines/identity"
        assert row["feed_id"] == "feed/identity"
        assert row["attempt_id"] == "attempt-1"
        assert row["sfn_retry_count"] == 0
        assert row["sfn_redrive_count"] == 0
        assert row["started_at"] is not None
        assert row["finished_at"] is not None
        assert row["recorded_at"] is not None

    land_row = by_stage["land"]
    assert land_row["raw_count"] == 3
    assert land_row["snapshot_id"] is not None
    assert land_row["snapshot_id"] == result.land_snapshot_id
    for field in _STAGE_ONLY_FIELDS:
        if field in ("raw_count", "snapshot_id"):
            continue
        assert land_row[field] is None, field

    pre_row = by_stage["pre_check"]
    assert pre_row["pre_quarantined"] == 0  # zero-violation branch, clean fixture
    assert pre_row["snapshot_id"] is None  # no write -- zero violations, nothing to append

    pull_row = by_stage["pull"]
    assert dict(pull_row["co_effect_snapshot_ids"]) == {"lookup": -1}  # incl. I-6 -1 sentinel
    for field in _STAGE_ONLY_FIELDS:
        if field == "co_effect_snapshot_ids":
            continue
        assert pull_row[field] is None, field

    apply_row = by_stage["apply"]
    for field in _STAGE_ONLY_FIELDS:
        assert apply_row[field] is None, field

    post_row = by_stage["post_check"]
    assert post_row["post_quarantined"] == 0
    assert post_row["snapshot_id"] is None  # no write -- zero violations
    assert post_row["error_message"] is None  # no drift -- single, fresh attempt

    commit_row = by_stage["commit"]
    assert commit_row["facts_appended"] == 3
    assert commit_row["snapshot_id"] is not None
    assert commit_row["snapshot_id"] == result.fact_snapshot_id

    fold_row = by_stage["fold"]
    assert fold_row["state_read_snapshot_id"] == -1  # fresh state table, zero snapshots [E-14]
    assert fold_row["snapshot_id"] is not None
    assert fold_row["snapshot_id"] == result.state_snapshot_id
    assert dict(fold_row["merge_summary"])  # non-empty, verbatim Iceberg summary

    publish_row = by_stage["publish"]
    for field in _STAGE_ONLY_FIELDS:
        assert publish_row[field] is None, field

    # --- (b) rerun, FRESH BatchContext, same batch_id (R-02's own precondition)
    # -> one more row per stage (all 8): land/commit skipped-guard
    # (facts_appended=0 signature), pre_check/pull/post_check/fold read back
    # ok (§7.5: zero-violation and no-guard-skip-mechanism branches
    # respectively, never skipped-guard) ---------------------------------
    second_seed = _make_seed(spec=spec, batch_id=batch_id, object_uris=object_uris)
    second = run_sequence(second_seed, local_runner_fx)
    moto_events_bus.read_events()

    assert second.guard_skips == ("land", "commit")

    all_rows_sorted = sorted(
        _ledger_rows_for(ledger_catalog, batch_id), key=lambda row: row["recorded_at"]
    )
    assert len(all_rows_sorted) == 16  # 8 + 8
    second_attempt_rows = all_rows_sorted[8:]
    assert [row["stage"] for row in second_attempt_rows] == list(_SEQUENCE_ORDER)
    by_stage2 = {row["stage"]: row for row in second_attempt_rows}

    assert by_stage2["land"]["outcome"] == "skipped-guard"
    assert by_stage2["commit"]["outcome"] == "skipped-guard"
    assert by_stage2["commit"]["facts_appended"] == 0  # the ledger's healthy-rerun signature
    for stage in ("pre_check", "pull", "apply", "post_check", "fold", "publish"):
        assert by_stage2[stage]["outcome"] == "ok", stage
    assert dict(by_stage2["pull"]["co_effect_snapshot_ids"]) == {"lookup": -1}

    # --- (c) failed attempt #1: TransientError -> "{type}: {first line}" ------
    transient_batch_id = _batch_id(502)
    transient_seed = _make_seed(spec=spec, batch_id=transient_batch_id, object_uris=object_uris)
    transient_fx = make_wrapped_fx(
        local_runner_fx,
        {
            "read_objects": _raise_once(
                TransientError("commit failed: simulated infra hiccup\nsecond line")
            )
        },
    )
    with pytest.raises(TransientError):
        run_sequence(transient_seed, transient_fx)

    transient_rows = _ledger_rows_for(ledger_catalog, transient_batch_id)
    assert len(transient_rows) == 1  # land is stage 1 -- nothing else ever ran
    transient_row = transient_rows[0]
    assert transient_row["stage"] == "land"
    assert transient_row["outcome"] == "failed"
    assert transient_row["error_type"] == "TransientError"
    assert transient_row["error_message"] == "TransientError: commit failed: simulated infra hiccup"

    # --- (c) failed attempt #2: a foreign ValueError -> module:lineno, no ------
    # message text (proving the planted sensitive string never reaches the row)
    foreign_batch_id = _batch_id(503)
    foreign_seed = _make_seed(spec=spec, batch_id=foreign_batch_id, object_uris=object_uris)
    foreign_fx = make_wrapped_fx(
        local_runner_fx,
        {
            "read_objects": _raise_once(
                ValueError("row payload: super-secret-value-should-not-leak")
            )
        },
    )
    with pytest.raises(ValueError):
        run_sequence(foreign_seed, foreign_fx)

    foreign_rows = _ledger_rows_for(ledger_catalog, foreign_batch_id)
    assert len(foreign_rows) == 1
    foreign_row = foreign_rows[0]
    assert foreign_row["stage"] == "land"
    assert foreign_row["outcome"] == "failed"
    assert foreign_row["error_type"] == "ValueError"
    assert foreign_row["error_message"] is not None
    assert re.fullmatch(r"test_scenarios_ledger:\d+", foreign_row["error_message"])
    assert "super-secret-value-should-not-leak" not in foreign_row["error_message"]


# --- formerly a pinned, known production bug (bead conveyer-nvh.36 fixed
# it) -- see module docstring ------------------------------------------------


def test_pull_ledger_row_co_effect_snapshot_ids_mappingproxy(
    ledger_catalog: LedgerCatalogFixture,
) -> None:
    """NORMATIVE (§6.5/R-05): a `pull` stage's run-ledger row records its
    `co_effect_snapshot_ids` map (>=1 declared co-effect, incl. the I-6
    zero-snapshot `-1` sentinel). A minimal, direct reproduction (bypasses
    the full `run()` sequence -- `record_run` is the ONLY thing under test
    here): builds the exact `RunFact` shape `core/run_facts.py::_stage_
    fields`'s `pull` branch would produce (a `MappingProxyType`-wrapped
    map, matching `stages/pull.py`'s own construction) and records it via
    the REAL `effects.ledger.build_record_run` closure over `ledger_
    catalog`'s already-bootstrapped SQLite table. Formerly a `strict=True`
    xfail (this bead's own prior wave, conveyer-nvh.24) pinning the
    MappingProxyType/`dataclasses.asdict` bug described in the module
    docstring; un-xfailed by conveyer-nvh.36's `_row_from_run_fact` fix."""
    record_run = ledger.build_record_run(ledger_catalog.catalog, ledger_catalog.config)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    run_fact = RunFact(
        batch_id="pull-mappingproxy-repro",
        pipeline="pipelines/identity",
        feed_id="feed/identity",
        attempt_id="attempt-1",
        sfn_retry_count=0,
        sfn_redrive_count=0,
        stage="pull",
        outcome="ok",
        started_at=now,
        finished_at=now,
        co_effect_snapshot_ids=MappingProxyType({"lookup": -1}),
    )

    record_run(run_fact)

    rows = [row for row in ledger_catalog.rows() if row["batch_id"] == "pull-mappingproxy-repro"]
    assert len(rows) == 1
    assert dict(rows[0]["co_effect_snapshot_ids"]) == {"lookup": -1}


# --- R-12: binding/seed defects fail fast, strictly before run() -----------


def test_r12(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    ledger_catalog: LedgerCatalogFixture,
    moto_events_bus: MotoEventsBus,
) -> None:
    raw_qt = unique_table("r12_raw")
    qtn_qt = unique_table("r12_qtn")
    fact_qt = unique_table("r12_fact")
    state_qt = unique_table("r12_state")
    _create_raw_table(spark, raw_qt)
    _create_quarantine_table(spark, qtn_qt)
    _create_fact_table(spark, fact_qt)
    _create_state_table(spark, state_qt)

    config = local_runner_fx.config
    delivery_id = str(uuid.UUID(int=1, version=4))
    received_at = datetime(2026, 1, 1, tzinfo=UTC)
    good_feed_id = "feed/identity"
    good_uris = (
        _canonical_uri(
            feed_id=good_feed_id,
            delivery_id=delivery_id,
            received_at=received_at,
            landing_bucket=config.landing_bucket,
            name="object_1.csv",
        ),
    )

    def _run_case(*, batch_id: str, object_uris: tuple[str, ...], transforms_module: str) -> None:
        seed_json = _valid_seed_json(
            batch_id=batch_id,
            delivery_id=delivery_id,
            feed_id=good_feed_id,
            received_at=received_at,
            object_uris=object_uris,
        )
        spec_data = _valid_spec_data(
            transforms_module=transforms_module,
            raw_table=_bare(raw_qt),
            quarantine_table=_bare(qtn_qt),
            fact_table=_bare(fact_qt),
            state_table=_bare(state_qt),
        )
        _entrypoint_order(
            seed_json=seed_json, spec_data=spec_data, config=config, fx=local_runner_fx
        )

    def _assert_no_trace(batch_id: str) -> None:
        assert spark.table(raw_qt).where(f"batch_id = '{batch_id}'").count() == 0
        assert _ledger_rows_for(ledger_catalog, batch_id) == []
        leftover = [
            e for e in moto_events_bus.read_events() if e["detail"].get("batch_id") == batch_id
        ]
        assert leftover == []

    # 1. malformed batch_id -- DeliveryRegisteredV1's own UUIDv5 pattern rejects
    # it at seed parse, before check_object_uris is even called (I-22).
    bad_batch_id = "not-a-uuid"
    with pytest.raises(ValidationError):
        _run_case(
            batch_id=bad_batch_id,
            object_uris=good_uris,
            transforms_module="pipelines.identity.transforms",
        )
    _assert_no_trace(bad_batch_id)

    # 2. forged object_uris (wrong feed) -- naming.check_object_uris rejects
    # (I-22): the URI's own canonical prefix names a DIFFERENT feed than the
    # seed's own feed_id, so it fails self-consistency, not merely well-
    # formedness.
    forged_batch_id = _batch_id(602)
    forged_uris = (
        _canonical_uri(
            feed_id="feed/some-other-feed",
            delivery_id=delivery_id,
            received_at=received_at,
            landing_bucket=config.landing_bucket,
            name="object_1.csv",
        ),
    )
    with pytest.raises(ValueError, match="object_uris"):
        _run_case(
            batch_id=forged_batch_id,
            object_uris=forged_uris,
            transforms_module="pipelines.identity.transforms",
        )
    _assert_no_trace(forged_batch_id)

    # 3. out-of-namespace transforms_module -- rejected at PipelineSpecModel
    # CONSTRUCTION itself (the pydantic Field(pattern=...) grammar, I-10) --
    # never reaches bind_transforms.
    namespace_batch_id = _batch_id(603)
    with pytest.raises(ValidationError):
        _run_case(
            batch_id=namespace_batch_id, object_uris=good_uris, transforms_module="evil.module"
        )
    _assert_no_trace(namespace_batch_id)

    # 4. broken transforms_module -- IN-namespace (passes PipelineSpecModel's
    # grammar), but genuinely unimportable -- bind_transforms's own
    # importlib.import_module raises ImportError (I-10).
    broken_batch_id = _batch_id(604)
    with pytest.raises(ImportError):
        _run_case(
            batch_id=broken_batch_id,
            object_uris=good_uris,
            transforms_module="pipelines.this_module_does_not_exist_zzz",
        )
    _assert_no_trace(broken_batch_id)


# --- §11.3 review rule, reified: record_run is fire-and-forget from run.py -


def test_record_run_call_sites_only_in_run_py() -> None:
    """§11.3: "no call site of `record_run` sits inside the data path's
    error handling -- it is fire-and-forget from `run.py` only." A textual
    scan for the unambiguous attribute-call shape `.record_run(` across every
    `spine/spine/**/*.py` file: `effects/ledger.py`'s own `def record_run(
    ...)` closure definition has no leading dot, and `effects/records.py`'s
    `record_run: Callable[...]` field annotation has no open paren
    immediately after the name, so neither is a false positive here."""
    package_root = Path(__file__).resolve().parents[2] / "spine"
    run_py = package_root / "run.py"
    assert run_py.is_file()

    offenders = {
        path: count
        for path in sorted(package_root.rglob("*.py"))
        if (count := path.read_text().count(".record_run(")) > 0
    }

    assert set(offenders) == {run_py}
    assert offenders[run_py] >= 2  # at least the failed() + transition() call sites
