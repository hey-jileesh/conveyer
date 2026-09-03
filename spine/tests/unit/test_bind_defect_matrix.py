"""G-09 -- Bind-defect matrix (006.1 §13.1's own definition: "every §5 code
exercised"). Bead `conveyer-6pg.12`'s own DONE bar.

This file is deliberately a ONE-STOP acceptance surface, not a new
mechanism: every scenario below calls the SAME production entry points
already unit-tested elsewhere (`core/model.py`'s parse-phase validators —
`tests/unit/test_model.py`; `core/bind_checks.py::validate_bindings` —
`tests/unit/test_bind_checks.py`; `entrypoints/glue_main.py`'s K5 gate-2
functions — `tests/unit/test_glue_main.py`) so that "does every §5 row
fire" is answerable by reading ONE file, section-ordered exactly as §5.1
(S-grain) -> §5.2 (C-grain) -> §5.3 (F-grain) -> §5.4 (K-grain, incl. K5).

**Rows with NO distinct `bind-defect/<code>` string, by design ("(pydantic)"
in §5's own Phase/code columns) are still exercised here, documented as
such**: C2 (co-effect table identifier grammar), F1/F4 (fact-type/column
structural shape), K8 (`tolerance` decimal-literal grammar) all raise a
bare pydantic `ValidationError` with no custom message -- there is nothing
further to assert about the CODE for these, only that construction refuses.
The co-effect alias/column-name grammar (A006-8, §5.2 C1's "alias grammar"
half and §4.3's `CoEffectDecl.columns` per-name grammar) is the SAME
posture -- no distinct code, refusal only.

**A006-9 naming-drift fix, landed by this same bead**: C1's "duplicate
aliases" half now has its own distinct code, `duplicate-alias` --
`_find_duplicate_co_effect_aliases` (`core/model.py`) cheaply distinguishes
a duplicate key at exactly the `co_effects:` alias position from every
other duplicate-key position (which still discharges through S1's generic
`duplicate-key`, unchanged). K6's FIRST half ("check-reason-grammar") is
likewise no longer a bare pydantic `Field(pattern=...)` violation -- a
`@field_validator` now raises the named code ahead of K6's second half
(`check-reason-reserved`).

**F-10/[DC-1] (007.1's three proposed 006.1 erratum rows, landed in code by
this same bead) are NOT §5 table rows** -- 006.1's own doc text was never
edited to add them, so they are outside this matrix's "every §5 code"
scope by definition; `tests/unit/test_bind_checks.py` carries their own
dedicated coverage (`co-effect-class-not-in-inventory`,
`co-effect-table-class-drift`, `fact-type-removed-in-flight`).

**`marker-table-missing` (critique gate wf_78ea4599-a5b F3, bead
conveyer-swb.26) joins this same NOT-a-§5-row carve-out** -- exercised
directly below (`validate_bindings`'s own `markers_table` param) AND at
`entrypoints/glue_main.py::main`'s own grain (`tests/unit/test_glue_main.py::
test_main_raises_pre_land_when_marker_table_missing`), proving the
un-bootstrapped marker table now refuses PRE-LAND rather than only at
commit (`effects/spark.py::_require_marker_table`'s retry-class
`TransientError`).

**§4.4's two `transform-defect/*` runtime codes (A006-1)** are exercised in
their own section below, after §5.4 -- §5.5's own words ("the complete code
table is the union of §5.1-§5.4's columns plus §4.4's two `transform-
defect/*` runtime codes. Every code is exercised by G-09.") place them in
this matrix's scope even though they are not §5-numbered rows.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest
from pydantic import ValidationError
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType
from spine.binding import Transforms
from spine.config import RunConfig
from spine.context import BatchContext
from spine.core import bind_checks, model, record
from spine.core import naming as naming_module
from spine.core.checks import checks_version
from spine.core.contract import check_version, read_spec_version
from spine.entrypoints import glue_main
from spine.stages import apply as apply_stage

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from spine.effects.records import RunnerFx

_VALID_FACT_SCHEMA: dict[str, Any] = {
    "columns": [
        {"name": "domain_id", "type": "string"},
        {"name": "amount", "type": "decimal(10,2)"},
        {"name": "period", "type": "date"},
    ],
    "domain_id_col": "domain_id",
    "record_key": ["domain_id"],
}

_VALID_FACT_TYPE: dict[str, Any] = {
    "fact_table": "lake.g09__facts",
    "state_table": "lake.g09__state",
    "schema": _VALID_FACT_SCHEMA,
}

_VALID_SPEC: dict[str, Any] = {
    "pipeline": "pipelines/g09",
    "transforms_module": "pipelines.g09.transforms",
    "raw_table": "lake.g09__raw",
    "quarantine_table": "lake.g09__quarantine",
    "fact_types": {"detail": _VALID_FACT_TYPE},
    "read": {"dialect": {"format": "csv"}},
    "raw_contract": {"columns": [{"name": "id"}]},
}

_ROW_CHECK: dict[str, Any] = {
    "kind": "row",
    "id": "chk-amount-positive",
    "fact_type": "detail",
    "expr": "amount > 0",
    "reason": "business/negative-amount",
}

_MEMBERSHIP_CHECK: dict[str, Any] = {
    "kind": "membership",
    "id": "chk-unknown-code",
    "fact_type": "detail",
    "columns": ["domain_id"],
    "co_effect": "rate_cards",
    "ref_columns": ["code"],
    "reason": "business/unknown-code",
}


def _spec(**overrides: object) -> model.PipelineSpecModel:
    return model.PipelineSpecModel(**{**_VALID_SPEC, **overrides})


# --- §5.1 spec-grain (S1-S4) -------------------------------------------------


def test_s1_duplicate_key_top_level() -> None:
    text = "pipeline: pipelines/g09\npipeline: pipelines/other\n"
    with pytest.raises(ValueError, match="bind-defect/duplicate-key"):
        model.parse_pipeline_spec_yaml(text)


def test_s2_fact_table_collision() -> None:
    with pytest.raises(ValidationError, match="bind-defect/fact-table-collision"):
        model.PipelineSpecModel(
            **{
                **_VALID_SPEC,
                "fact_types": {
                    "a": _VALID_FACT_TYPE,
                    "b": {**_VALID_FACT_TYPE, "fact_table": "lake.other__facts"},
                },
            }
        )


def test_s3_custom_fold_refused() -> None:
    with pytest.raises(ValidationError, match="bind-defect/custom-fold-refused"):
        model.PipelineSpecModel(**{**_VALID_SPEC, "fold": "custom"})


def test_s4_stale_post_check_export() -> None:
    spec = _spec()
    defects = bind_checks.validate_bindings(
        spec, {}, bind_checks.TransformsMeta(has_post_check_export=True), {}, ()
    )
    assert [d.code for d in defects] == ["stale-post-check-export"]


def test_s4_stale_fold_export() -> None:
    # S4's own sibling tripwire (critique gate wf_24a3125f-ecc F2, bead
    # conveyer-6pg.31): 007.1 B10 dropped `Transforms.fold`/`bind_
    # transforms`'s fold-defaulting wiring outright (`stages/fold.py`'s
    # mechanical §8.2 reduce never called it) -- a module still exporting
    # `fold` is a stale export too, refused the same way `post_check` is.
    spec = _spec()
    defects = bind_checks.validate_bindings(
        spec,
        {},
        bind_checks.TransformsMeta(has_post_check_export=False, has_fold_export=True),
        {},
        (),
    )
    assert [d.code for d in defects] == ["stale-fold-export"]


# --- §5.2 co-effect checks (C1-C8) -------------------------------------------


def test_c1_duplicate_alias_raises_duplicate_alias() -> None:
    # A006-9(b): C1's own §5.2 label ("duplicate-alias") is now a distinct
    # code -- `_find_duplicate_co_effect_aliases` (`core/model.py`) cheaply
    # distinguishes a duplicate key at exactly the `co_effects:` alias
    # position from every OTHER duplicate-key position (still S1's generic
    # `duplicate-key`, `test_s1_duplicate_key_top_level` above).
    text = """
pipeline: pipelines/g09
transforms_module: pipelines.g09.transforms
raw_table: lake.g09__raw
quarantine_table: lake.g09__quarantine
co_effects:
  rate_cards: {table: lake.rate_cards}
  rate_cards: {table: lake.rate_cards_dup}
fact_types:
  detail:
    fact_table: lake.g09__facts
    state_table: lake.g09__state
    schema:
      columns: [{name: domain_id, type: string}]
      domain_id_col: domain_id
      record_key: [domain_id]
read:
  dialect: {format: csv}
raw_contract:
  columns: [{name: id}]
"""
    with pytest.raises(ValueError, match="bind-defect/duplicate-alias"):
        model.parse_pipeline_spec_yaml(text)


def test_c1b_co_effect_alias_grammar() -> None:
    # A006-8/§4.3: alias grammar, C1's OTHER half -- a single alias whose
    # own shape is illegal (never mind duplication) refuses at parse.
    with pytest.raises(ValidationError, match="bind-defect/co-effect-alias-grammar"):
        model.PipelineSpecModel(
            **{**_VALID_SPEC, "co_effects": {"Bad Alias!": {"table": "lake.rate_cards"}}}
        )


def test_c1c_co_effect_columns_grammar_is_bare_pydantic() -> None:
    # A006-8/§4.3: `CoEffectDecl.columns`'s own per-name identifier grammar
    # -- (pydantic) posture, same as C2 below: refusal only, no distinct code.
    with pytest.raises(ValidationError):
        model.PipelineSpecModel(
            **{
                **_VALID_SPEC,
                "co_effects": {"rc": {"table": "lake.rate_cards", "columns": ["bad col"]}},
            }
        )


def test_c2_co_effect_table_grammar_is_bare_pydantic() -> None:
    # (pydantic) in §5.2's own phase column -- no custom code, just a
    # rejected `<db>.<table>` identifier shape.
    with pytest.raises(ValidationError):
        model.PipelineSpecModel(
            **{**_VALID_SPEC, "co_effects": {"rc": {"table": "not-a-qualified-table"}}}
        )


def test_f6_co_effect_alias_too_long() -> None:
    # F-6 (security gate `wf_c9aadeb2-8eb`, LOW): alias grammar's own regex
    # is length-unbounded -- same `bind-defect/co-effect-alias-grammar` code
    # as the illegal-shape row above, now also refusing a >128-char alias.
    with pytest.raises(ValidationError, match="bind-defect/co-effect-alias-grammar"):
        model.PipelineSpecModel(
            **{**_VALID_SPEC, "co_effects": {"a" * 129: {"table": "lake.rate_cards"}}}
        )


def test_f6_co_effect_columns_too_long_is_bare_pydantic() -> None:
    # F-6's sibling cap on `CoEffectDecl.columns` -- same (pydantic) posture
    # as C1c/C2 above: refusal only, no distinct code.
    with pytest.raises(ValidationError):
        model.PipelineSpecModel(
            **{
                **_VALID_SPEC,
                "co_effects": {"rc": {"table": "lake.rate_cards", "columns": ["a" * 129]}},
            }
        )


def test_c3_co_effect_missing_table() -> None:
    spec = _spec(co_effects={"rc": {"table": "lake.rate_cards"}})
    defects = bind_checks.validate_bindings(
        spec, {"lake.rate_cards": None}, bind_checks.TransformsMeta(False), {}, ()
    )
    assert [d.code for d in defects] == ["co-effect-missing-table"]


def test_c4_co_effect_not_current_state() -> None:
    spec = _spec(co_effects={"rc": {"table": "lake.rate_cards"}})
    facts = {"lake.rate_cards": bind_checks.TableFacts(table_class="raw", columns={})}
    defects = bind_checks.validate_bindings(
        spec, facts, bind_checks.TransformsMeta(False), {"lake.rate_cards": "raw"}, ()
    )
    assert [d.code for d in defects] == ["co-effect-not-current-state"]


def test_c4_co_effect_quarantine_class_refused() -> None:
    # A006-12: C4's own §5.2 code exercised over EVERY non-state table class
    # named in the F-10 inventory (module docstring's "table_class_inventory
    # is the AUTHORITY") -- raw and unmarked were already covered
    # (`test_c4_co_effect_not_current_state`/`test_bind_checks.py`'s own
    # unmarked case); quarantine was not.
    spec = _spec(co_effects={"rc": {"table": "lake.rate_cards"}})
    facts = {"lake.rate_cards": bind_checks.TableFacts(table_class="quarantine", columns={})}
    defects = bind_checks.validate_bindings(
        spec, facts, bind_checks.TransformsMeta(False), {"lake.rate_cards": "quarantine"}, ()
    )
    assert [d.code for d in defects] == ["co-effect-not-current-state"]


def test_c4_co_effect_facts_class_refused() -> None:
    # A006-12's facts-class variant -- a fact table itself declared as a
    # co-effect (a fact table reading another pipeline's fact table, not its
    # current-state table) is refused the same way.
    spec = _spec(co_effects={"rc": {"table": "lake.rate_cards"}})
    facts = {"lake.rate_cards": bind_checks.TableFacts(table_class="facts", columns={})}
    defects = bind_checks.validate_bindings(
        spec, facts, bind_checks.TransformsMeta(False), {"lake.rate_cards": "facts"}, ()
    )
    assert [d.code for d in defects] == ["co-effect-not-current-state"]


def test_c5_co_effect_unknown_columns() -> None:
    spec = _spec(co_effects={"rc": {"table": "lake.rate_cards", "columns": ["code", "zzz"]}})
    facts = {"lake.rate_cards": bind_checks.TableFacts(table_class="state", columns={"code": "s"})}
    defects = bind_checks.validate_bindings(
        spec, facts, bind_checks.TransformsMeta(False), {"lake.rate_cards": "state"}, ()
    )
    assert [d.code for d in defects] == ["co-effect-unknown-columns"]


def test_c6_own_state_refused() -> None:
    spec = _spec(co_effects={"x": {"table": "lake.x", "own_state": True}})
    facts = {"lake.x": bind_checks.TableFacts(table_class="state", columns={})}
    defects = bind_checks.validate_bindings(
        spec, facts, bind_checks.TransformsMeta(False), {"lake.x": "state"}, ()
    )
    assert [d.code for d in defects] == ["own-state-refused"]


def test_c7_membership_unknown_co_effect() -> None:
    with pytest.raises(ValidationError, match="bind-defect/membership-unknown-co-effect"):
        model.PipelineSpecModel(
            **{**_VALID_SPEC, "co_effects": {}, "checks": {"checks": [_MEMBERSHIP_CHECK]}}
        )


def test_c8_membership_columns_outside_declaration_bind_half() -> None:
    spec = _spec(
        co_effects={"rate_cards": {"table": "lake.rate_cards"}},
        checks={"checks": [_MEMBERSHIP_CHECK]},
    )
    facts = {"lake.rate_cards": bind_checks.TableFacts(table_class="state", columns={"x": "s"})}
    defects = bind_checks.validate_bindings(
        spec, facts, bind_checks.TransformsMeta(False), {"lake.rate_cards": "state"}, ()
    )
    assert [d.code for d in defects] == ["membership-columns-outside-declaration"]


# --- F-10/[DC-1]-adjacent (not a §5 row, module docstring's own carve-out): -
# --- `marker-table-missing` (critique gate wf_78ea4599-a5b F3, bead --------
# --- conveyer-swb.26) -------------------------------------------------------


def test_marker_table_missing() -> None:
    spec = _spec()
    markers = naming_module.markers_table(spec.raw_table, spec.pipeline)
    defects = bind_checks.validate_bindings(
        spec, {}, bind_checks.TransformsMeta(False), {}, (), markers
    )
    assert [d.code for d in defects] == ["marker-table-missing"]
    assert markers in defects[0].detail


def test_marker_table_present_no_defect() -> None:
    spec = _spec()
    markers = naming_module.markers_table(spec.raw_table, spec.pipeline)
    facts = {markers: bind_checks.TableFacts(table_class="markers", columns={})}
    defects = bind_checks.validate_bindings(
        spec, facts, bind_checks.TransformsMeta(False), {}, (), markers
    )
    assert defects == ()


# --- §5.3 fact-schema checks (F1-F5) -----------------------------------------


def test_f1_fact_types_shape_is_bare_pydantic() -> None:
    with pytest.raises(ValidationError):
        model.PipelineSpecModel(**{**_VALID_SPEC, "fact_types": {}})


def test_f2_fact_schema_unknown_column_ref() -> None:
    with pytest.raises(ValidationError, match="bind-defect/fact-schema-unknown-column-ref"):
        model.FactSchemaModel(
            columns=[{"name": "a", "type": "string"}], domain_id_col="zzz", record_key=["a"]
        )


def test_f3_fact_column_reserved_name() -> None:
    # F3's own §5.3 row: disjoint from the framework fact-stamp set
    # (`record.py::FACT_STAMP_COLUMNS`) and the `_conveyer_` prefix --
    # exhaustive coverage of both lives in `tests/unit/test_model.py`
    # (`test_fact_schema_model_rejects_each_framework_stamp_column_name`/
    # `..._rejects_conveyer_prefixed_column_name`); one representative case
    # here for G-09.
    stamp_name = next(iter(record.FACT_STAMP_COLUMNS))
    with pytest.raises(ValidationError, match="bind-defect/fact-column-reserved-name"):
        model.FactSchemaModel(
            columns=[{"name": stamp_name, "type": "string"}],
            domain_id_col=stamp_name,
            record_key=[stamp_name],
        )


def test_f3b_fact_column_duplicate_name() -> None:
    # A006-9: duplicate declared column names are a DISTINCT uniqueness
    # rule, not F3's reserved-name check -- `core/model.py`'s own
    # `_check_columns` now raises a separate code for it.
    with pytest.raises(ValidationError, match="bind-defect/fact-column-duplicate-name"):
        model.FactSchemaModel(
            columns=[{"name": "a", "type": "string"}, {"name": "a", "type": "string"}],
            domain_id_col="a",
            record_key=["a"],
        )


def test_f4_float_double_structurally_unrepresentable() -> None:
    # (pydantic) -- FACT_COLUMN_TYPE_RE has no float/double alternative at all.
    with pytest.raises(ValidationError):
        model.FactColumnSpec(name="x", type="double")


def test_f5_ordering_type_not_comparable_bool_citing_imported_constant() -> None:
    # F5 explicitly named in this bead's DONE bar: `bool` is fact-column-
    # grammar-valid but excluded from `record.ORDERING_COMPARABLE_TYPES`
    # (the one imported constant `core/model.py`'s validator consumes,
    # never a second list).
    assert "bool" not in record.ORDERING_COMPARABLE_TYPES
    with pytest.raises(ValidationError, match="bind-defect/ordering-type-not-comparable"):
        model.FactSchemaModel(
            columns=[{"name": "a", "type": "string"}, {"name": "flag", "type": "bool"}],
            domain_id_col="a",
            record_key=["a"],
            ordering=["flag"],
        )


# --- §5.4 checks.yaml checks (K1-K9, incl. K5's engine-compile gate) --------


def test_k1a_check_duplicate_id() -> None:
    with pytest.raises(ValidationError, match="bind-defect/check-duplicate-id"):
        model.ChecksModel(checks=[_ROW_CHECK, {**_MEMBERSHIP_CHECK, "id": _ROW_CHECK["id"]}])


def test_k1b_check_id_reserved() -> None:
    # Explicitly named in this bead's DONE bar ("reserved check id" [AE-6]).
    with pytest.raises(ValidationError, match="bind-defect/check-id-reserved"):
        model.RowCheckModel(**{**_ROW_CHECK, "id": "missing-domain-id"})


def test_k2_check_unknown_fact_type() -> None:
    with pytest.raises(ValidationError, match="bind-defect/check-unknown-fact-type"):
        model.PipelineSpecModel(
            **{**_VALID_SPEC, "checks": {"checks": [{**_ROW_CHECK, "fact_type": "zzz"}]}}
        )


def test_k3_check_column_outside_type() -> None:
    with pytest.raises(ValidationError, match="bind-defect/check-column-outside-type"):
        model.PipelineSpecModel(
            **{**_VALID_SPEC, "checks": {"checks": [{**_ROW_CHECK, "expr": "unknown_col > 0"}]}}
        )


def test_k4_check_expression_rejected() -> None:
    with pytest.raises(ValidationError, match="bind-defect/check-expression-rejected"):
        model.PipelineSpecModel(
            **{**_VALID_SPEC, "checks": {"checks": [{**_ROW_CHECK, "expr": "rand() > 0"}]}}
        )


def test_k5a_check_expression_uncompilable(spark: SparkSession) -> None:
    # K3 (spec-parse) already refuses an unknown-column reference in any
    # REAL spec -- exercised directly at K5's own engine-compile grain
    # (`entrypoints/glue_main.py`, needs a live Spark session; this is bind's
    # THIRD, separate call, `core/bind_checks.py`'s own module docstring).
    fact_type = model.FactTypeModel(**_VALID_FACT_TYPE)
    probe_df = spark.createDataFrame([], schema=glue_main._fact_type_probe_schema(fact_type))
    with pytest.raises(ValueError, match="bind-defect/check-expression-uncompilable"):
        glue_main._compile_probe(probe_df, "totally_unknown_col > 0", "chk-1", "expr")


def test_k5b_check_expression_not_boolean(spark: SparkSession) -> None:
    fact_type = model.FactTypeModel(**_VALID_FACT_TYPE)
    probe_df = spark.createDataFrame([], schema=glue_main._fact_type_probe_schema(fact_type))
    with pytest.raises(ValueError, match="bind-defect/check-expression-not-boolean"):
        glue_main._assert_row_expr_boolean(probe_df, "chk-1", "amount + 1")


def test_k5c_check_expression_inexact_type(spark: SparkSession) -> None:
    # `batch_check` is unreachable through any real `PipelineSpecModel`
    # (K7 refuses it unconditionally at spec-parse, P-6's structural wait)
    # -- exercised directly against a hand-built `BatchCheckModel` per
    # `_assert_check_expressions_compile`'s own documented rationale.
    fact_schema = {
        **_VALID_FACT_SCHEMA,
        "columns": [*_VALID_FACT_SCHEMA["columns"], {"name": "qty", "type": "int"}],
    }
    fact_type = model.FactTypeModel(**{**_VALID_FACT_TYPE, "schema": fact_schema})
    probe_df = spark.createDataFrame([], schema=glue_main._fact_type_probe_schema(fact_type))
    with pytest.raises(ValueError, match="bind-defect/check-expression-inexact-type"):
        glue_main._assert_aggregate_dtype_exact(probe_df, "chk-1", "aggregate", "avg(qty)")


def test_k6a_check_reason_grammar() -> None:
    # A006-9: K6's FIRST half now has its own distinct code -- a
    # `@field_validator` raises `check-reason-grammar` ahead of K6's second
    # half (`check-reason-reserved`, below), rather than a bare pydantic
    # `Field(pattern=...)` violation.
    with pytest.raises(ValidationError, match="bind-defect/check-reason-grammar"):
        model.RowCheckModel(**{**_ROW_CHECK, "reason": "not-business-prefixed"})


def test_k6b_check_reason_reserved() -> None:
    with pytest.raises(ValidationError, match="bind-defect/check-reason-reserved"):
        model.RowCheckModel(**{**_ROW_CHECK, "reason": "business/missing-domain-id"})


def test_k6c_check_reason_too_long() -> None:
    # F-6 (security gate `wf_c9aadeb2-8eb`, LOW): K6's grammar half was
    # length-unbounded -- same 128-char cap `ColumnSpec.name`/check-id
    # already carry, now a THIRD named half ahead of the grammar/reserved
    # checks (checked first since `Field(max_length=...)` would otherwise
    # pre-empt this function's own named code, same reasoning as A006-9's
    # grammar half).
    with pytest.raises(ValidationError, match="bind-defect/check-reason-too-long"):
        model.RowCheckModel(**{**_ROW_CHECK, "reason": "business/" + "a" * 130})


def test_k7_batch_check_awaiting_member_grammar() -> None:
    batch_check = {
        "kind": "batch_check",
        "id": "chk-reconcile",
        "fact_type": "detail",
        "aggregate": "sum(amount)",
        "control": {"member": "summary", "expr": "total"},
    }
    with pytest.raises(ValidationError, match="bind-defect/batch-check-awaiting-member-grammar"):
        model.ChecksModel(checks=[batch_check])


def test_k8_tolerance_grammar_is_bare_pydantic() -> None:
    with pytest.raises(ValidationError):
        model.BatchCheckModel(
            kind="batch_check",
            id="chk-reconcile",
            fact_type="detail",
            aggregate="sum(amount)",
            control={"member": "summary", "expr": "total"},
            tolerance="-0.01",
        )


def test_k9_check_expression_mixed_types() -> None:
    # Explicitly named in this bead's DONE bar ("mixed-family expression" [AE-1]).
    with pytest.raises(ValidationError, match="bind-defect/check-expression-mixed-types"):
        model.PipelineSpecModel(
            **{**_VALID_SPEC, "checks": {"checks": [{**_ROW_CHECK, "expr": "amount = 'x'"}]}}
        )


# --- §4.4 runtime transform-defect codes (A006-1) ---------------------------
#
# `stages/apply.py`'s own runtime half of the return-shape law -- unlike
# every check above, these two codes are raised by `apply.run` itself
# (post-BIND, at STAGE time), never by a pydantic validator. Driven with a
# real `Transforms(apply=<double>)` seed, the pattern `tests/integration/
# test_stages_land_pre_pull_apply.py:455-478` already establishes for this
# exact stage.


def _g09_apply_ctx(spark: SparkSession, transforms: Transforms) -> BatchContext:
    spec = _spec()
    valid_df = spark.createDataFrame([], schema=StructType([StructField("id", StringType())]))
    return BatchContext(
        pipeline=spec.pipeline,
        feed_id="feed/g09",
        delivery_id=str(uuid.uuid4()),
        batch_id="batch-1",
        delivery_key="x.csv",
        content_hash="sha256:" + "a" * 64,
        object_uris=("s3://unused/x",),
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
        valid_df=valid_df,
        co_effects={},
    )


def test_g09_transform_defect_return_shape_missing_key(
    spark: SparkSession, local_runner_fx: RunnerFx
) -> None:
    ctx = _g09_apply_ctx(spark, Transforms(apply=lambda valid_df, co_effects: {}))
    with pytest.raises(
        ValueError, match=r"transform-defect/return-shape: missing=\['detail'\] extra=\[\]"
    ):
        apply_stage.run(ctx, local_runner_fx)


def test_g09_transform_defect_candidate_schema_missing_column(
    spark: SparkSession, local_runner_fx: RunnerFx
) -> None:
    missing_schema = StructType(
        [StructField("domain_id", StringType()), StructField("amount", DecimalType(10, 2))]
    )
    candidate: DataFrame = spark.createDataFrame([], schema=missing_schema)
    ctx = _g09_apply_ctx(
        spark, Transforms(apply=lambda valid_df, co_effects: {"detail": candidate})
    )
    with pytest.raises(
        ValueError,
        match=r"transform-defect/candidate-schema: fact_type='detail' diff=missing=\['period'\]",
    ):
        apply_stage.run(ctx, local_runner_fx)


def test_g09_transform_defect_candidate_schema_extra_column(
    spark: SparkSession, local_runner_fx: RunnerFx
) -> None:
    extra_schema = StructType(
        [
            StructField("domain_id", StringType()),
            StructField("amount", DecimalType(10, 2)),
            StructField("period", DateType()),
            StructField("bogus", StringType()),
        ]
    )
    candidate: DataFrame = spark.createDataFrame([], schema=extra_schema)
    ctx = _g09_apply_ctx(
        spark, Transforms(apply=lambda valid_df, co_effects: {"detail": candidate})
    )
    with pytest.raises(
        ValueError,
        match=r"transform-defect/candidate-schema: fact_type='detail' diff=extra=\['bogus'\]",
    ):
        apply_stage.run(ctx, local_runner_fx)


def test_g09_transform_defect_candidate_schema_type_mismatch(
    spark: SparkSession, local_runner_fx: RunnerFx
) -> None:
    mismatch_schema = StructType(
        [
            StructField("domain_id", StringType()),
            StructField("amount", StringType()),  # declared decimal(10,2)
            StructField("period", DateType()),
        ]
    )
    candidate: DataFrame = spark.createDataFrame([], schema=mismatch_schema)
    ctx = _g09_apply_ctx(
        spark, Transforms(apply=lambda valid_df, co_effects: {"detail": candidate})
    )
    with pytest.raises(
        ValueError,
        match=(
            r"transform-defect/candidate-schema: fact_type='detail' "
            r"diff=type_mismatch=\[\"amount: expected='decimal\(10,2\)' actual='string'\"\]"
        ),
    ):
        apply_stage.run(ctx, local_runner_fx)


def test_g09_transform_defect_nullable_only_diff_does_not_raise(
    spark: SparkSession, local_runner_fx: RunnerFx
) -> None:
    # The nullability-ignored rule: `FactColumnSpec` carries no nullability
    # of its own, so a candidate frame whose declared columns/types match
    # but whose Spark `nullable` flags differ must NOT raise.
    nullable_schema = StructType(
        [
            StructField("domain_id", StringType(), nullable=False),
            StructField("amount", DecimalType(10, 2), nullable=False),
            StructField("period", DateType(), nullable=False),
        ]
    )
    candidate: DataFrame = spark.createDataFrame([], schema=nullable_schema)
    ctx = _g09_apply_ctx(
        spark, Transforms(apply=lambda valid_df, co_effects: {"detail": candidate})
    )

    after = apply_stage.run(ctx, local_runner_fx)

    assert after.candidate_facts is not None
    assert after.candidate_facts["detail"].schema == nullable_schema
