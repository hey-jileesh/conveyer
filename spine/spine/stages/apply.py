"""`apply` stage — `ctx.transforms.apply(ctx.valid_df, ctx.co_effects)`. Pure. LLD 006.1 §4.4/§7.5.

Nothing else: no guard, no I/O, no logging. `fx` is accepted only to match
every other stage's `run(ctx, fx) -> BatchContext` shape (§7.3's uniform
`StageFn`) and is never called. The df handed to `apply` is `valid_df` --
001 §5's `raw_df` argument *post-admission* (pre_check's split), stated once
here per §7.5.

**006.1 §4.4 (bead conveyer-6pg.13, B3): the per-type return-shape law, its
runtime half.** `apply` now returns `Mapping[str, DataFrame]` — one
candidate frame per declared fact type (P-1) — rather than a single
`DataFrame`; the BIND half of the return-shape law (declared type set
non-empty/well-formed) is `core/model.py`'s F1 (§5.3); this stage owns the
RUNTIME half, asserted immediately after the call, before anything else
touches the returned mapping:

1. **Key-set assertion**: `set(returned.keys()) == set(spec.fact_types)` —
   any mismatch raises `transform-defect/return-shape: missing=[…]
   extra=[…]` (sorted, value-free — type NAMES, never cell values).
2. **Per-type, plan-level schema diff**: the returned frame's column
   name+type set against `spec.fact_types[t].schema_`'s declared columns —
   any mismatch raises `transform-defect/candidate-schema: fact_type=<t>
   diff=<value-free column/type diff>`. Both checks read only `DataFrame.
   schema` (a plan-level property, zero Spark execution — no `.collect()`/
   action, matching `frames/`'s own no-materialization discipline even
   though this file is `stages/`, not `frames/`) and `PipelineSpecModel`'s
   already-parsed declarations — never row values.

Both are D-4's "author bug — loud defect, never quarantine" made mechanical
(§4.4): a transform that returns the wrong type set, or a candidate frame
whose shape drifts from its own type's declared schema, is a pipeline-author
bug the framework catches BEFORE post_check ever compiles a check against
it — deterministic and value-free, matching A-10's message grammar.

`_fact_column_spark_type` mirrors `entrypoints/glue_main.py`'s own
module-private helper of the same shape (K5's probe-schema builder) —
deliberately duplicated, not cross-imported: both are the SAME one-line,
grammar-anchored mechanical mapping from 006.1 §4.1's `FACT_COLUMN_TYPE_RE`
kind strings to their Spark `DataType`, the same small-duplication
precedent `core/model.py::_fact_schema_family_map` and `frames/
business_checks.py::_schema_family_map` already established for this exact
class of mapping (cheap to keep in sync, and `entrypoints/` -> `stages/` is
the wrong import direction regardless).
"""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType
from typing import TYPE_CHECKING

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from pyspark.sql import DataFrame
    from pyspark.sql.types import DataType

    from spine.context import BatchContext
    from spine.core.model import FactSchemaModel
    from spine.effects.records import RunnerFx

# 006.1 §4.1's `FACT_COLUMN_TYPE_RE` bare-kind lookup -- `decimal` is parsed
# directly below (it carries precision/scale), the other six kinds are a
# straight constructor lookup.
_FACT_COLUMN_SPARK_TYPES: Mapping[str, Callable[[], DataType]] = {
    "string": StringType,
    "int": IntegerType,
    "long": LongType,
    "bool": BooleanType,
    "date": DateType,
    "timestamp": TimestampType,
}


def _fact_column_spark_type(type_str: str) -> DataType:
    """`FactColumnSpec.type` string (already `FACT_COLUMN_TYPE_RE`-valid by
    the time a `PipelineSpecModel` exists) -> its Spark `DataType`."""
    kind = type_str.split("(", 1)[0]
    if kind == "decimal":
        precision_str, scale_str = type_str[len("decimal(") : -1].split(",")
        return DecimalType(int(precision_str), int(scale_str))
    return _FACT_COLUMN_SPARK_TYPES[kind]()


def _assert_return_shape(
    returned: Mapping[str, DataFrame], declared_types: Mapping[str, object]
) -> None:
    """§4.4's key-set assertion: `returned`'s keys must equal exactly
    `spec.fact_types`'s declared type names -- no more, no fewer."""
    returned_keys = set(returned.keys())
    declared_keys = set(declared_types.keys())
    if returned_keys != declared_keys:
        missing = sorted(declared_keys - returned_keys)
        extra = sorted(returned_keys - declared_keys)
        raise ValueError(f"transform-defect/return-shape: missing={missing!r} extra={extra!r}")


def _schema_diff(frame: DataFrame, schema: FactSchemaModel) -> str | None:
    """A value-free column-name/type diff between `frame`'s ACTUAL schema
    and `schema`'s DECLARED columns -- `None` iff they match exactly (order-
    insensitive; a `FactColumnSpec` carries no nullability, so nullability
    is not compared). Plan-level only (`DataFrame.schema`), no action."""
    expected = {column.name: _fact_column_spark_type(column.type) for column in schema.columns}
    actual = {field.name: field.dataType for field in frame.schema.fields}
    if expected == actual:
        return None
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    type_mismatch = sorted(
        f"{name}: expected={expected[name].simpleString()!r} actual={actual[name].simpleString()!r}"
        for name in set(expected) & set(actual)
        if expected[name] != actual[name]
    )
    labeled = (("missing", missing), ("extra", extra), ("type_mismatch", type_mismatch))
    parts = [f"{label}={value!r}" for label, value in labeled if value]
    return " ".join(parts)


def _assert_candidate_schema(fact_type: str, frame: DataFrame, schema: FactSchemaModel) -> None:
    diff = _schema_diff(frame, schema)
    if diff is not None:
        raise ValueError(f"transform-defect/candidate-schema: fact_type={fact_type!r} diff={diff}")


def run(ctx: BatchContext, fx: RunnerFx) -> BatchContext:
    """§7.5 apply: pure transform, nothing else -- plus §4.4's runtime
    return-shape law, asserted immediately after the call."""
    del fx
    valid_df = ctx.valid_df
    co_effects = ctx.co_effects
    if valid_df is None or co_effects is None:  # sequencing: pre_check/pull run first
        raise ValueError(
            "apply: ctx.valid_df/ctx.co_effects must be populated -- "
            "pre_check and pull must run before apply"
        )
    returned = ctx.transforms.apply(valid_df, co_effects)
    _assert_return_shape(returned, ctx.spec.fact_types)
    for fact_type, frame in returned.items():
        _assert_candidate_schema(fact_type, frame, ctx.spec.fact_types[fact_type].schema_)
    return replace(ctx, candidate_facts=MappingProxyType(dict(returned)))
