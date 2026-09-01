"""`post_check`'s interpreter half — compile, evaluate, project. LLD 006.1
§7 (the post_check interpreter), §8.1 (the fresh-path split), P-5/P-7/P-8.

**Shape (framework code, `stages/post_check.py` composes this + `frames/
quarantine.py::shape_post_quarantine`; pipelines contribute zero check code
and zero attribution code — D-1/D-6, restated as law here).** `compile_
business_checks` turns a spec's `ChecksModel` + one fact type's `FactSchemaModel`
into a frozen `CompiledBusinessChecks` value (§7.1) — plain-value pure, zero
Spark execution, safe to build once per type per stage invocation. `evaluate`
adds the one internal `_conveyer_business_failures` column (§7.2); `admitted_
candidates`/`business_violations` are its two sanctioned exit projections —
the internal failures column, and every per-check membership marker column
this module adds along the way, are dropped before any frame leaves this
module (the `conveyer-azr` privacy rule already governing `frames/checks.py`'s
own `_conveyer_admission_failures`, restated here for the business-check
surface).

**§7.1 compilation, restated at this module's own grain.** Entry 0 of every
`CompiledBusinessChecks.entries` is the framework-reserved implicit check
(id `missing-domain-id`, reason `business/missing-domain-id`, version
`"fw-1"` — a governed constant bumped only if its own semantics ever change
— predicate `domain_id_col IS NULL`; D-6: evaluated first, before every
authored check). The ids/reasons are sourced from `core/model.py`'s
`RESERVED_CHECK_IDS`/`RESERVED_REASONS` (single-element, by construction —
[AE-6]'s own docstring calls each "the implicit check's ... id/reason") —
never re-authored as a second literal pair. The type's authored `row`/
`membership` checks follow in authored order (P-5: read per fact type,
`batch_check` entries are structurally excluded — P-6's dormancy, K7 already
refuses any `batch_check` at bind, so none can reach a validly-bound spec's
`ChecksModel` in practice; this module's `isinstance` filter is the type-safe
expression of that invariant, not a second enforcement of it).

**Row-check compilation, `_compiled_expr` (§7.1/§6.4's executed-text rule).**
A row check's `expr` text was already gate-1/gate-2/K9-validated at bind
(006.1 §5.4); this module re-derives a `ValidatedExpr` from the SAME pure
`check_grammar.validate_expression` gate 1 (never a second grammar) so that
the string `F.expr` receives is `ValidatedExpr.authored_text` byte-exact
[EM-6][AE-2] — never the raw `RowCheckModel.expr` field read directly. A
`GrammarDefect` result here is a framework defect (bind should have refused
the spec already), not authored data, so it raises rather than quarantining
— frames/ is not `core/**`, so `raise` is available (`ban_try_raise` is a
core-profile-only rule); this is the ONE call site where it fires.
`_compiled_expr` is the linter's named string-SQL sink exemption (`(spine/
frames/business_checks.py, _compiled_expr)`, 006.1 §13.4 item 1 — the
`_typed_expr` precedent, 005.1 §12.6 item 2).

**Membership evaluation, joins-first (§7.2, P-8).** `evaluate` performs one
LEFT JOIN per membership entry against a DISTINCT projection of the
declared `co_effect`'s `ref_columns` (never the raw co-effect frame —
duplicate ref rows must never fan the candidate set out; P-8's own words:
"the plan is a left join against the distinct projection"), producing one
`_conveyer_m_<check_id>` marker column per membership entry (the brief's own
naming). Multi-column membership is tuple equality realized as an AND of
pairwise equalities over POSITIONALLY-ALIASED ref columns (avoiding any
`columns`/`ref_columns` name-collision ambiguity in the join condition;
degenerates to genuine SQL tuple equality exactly when every candidate key
column is non-null, which is the only case the fail predicate below ever
consults). **Never fires on NULL key material (P-8, by explicit guard, not
by incidental join-null behavior):** the per-entry fail predicate is `(every
declared `columns` value is non-null) AND (no match found)` — a NULL key
column already fails to match in SQL's own null-unsafe `=`, but this module
does not rely on that alone; the explicit `isNotNull()` conjunction is the
actual, reviewable enforcement of P-8's law, robust to any future join-
condition change. Marker columns are join-plan detail, never part of the
exported "evaluated" value — `evaluate` drops every one it added before
returning, same discipline as the internal failures column.

**§7.3's reason shaping, this module's slice.** The struct riding inside
`_conveyer_business_failures` per entry is `struct<id, reason, version>` —
three fields, not `checks.py`'s pre_check `struct<code, column, expected>`
shape (a different governed vocabulary; business checks are id/reason/
version, 006.1 §4.2's own check-model fields). `business_violations`
projects `reason_code` = the first surviving entry's `reason` (evaluation-
order-first, D-6 verbatim: on a null-`domain_id` row the implicit check is
always first, so `reason_code` is deterministically `business/missing-
domain-id`) and `reason_detail` = `to_json` of the SAME array reduced to
`(id, version)` pairs only — `reason` is deliberately excluded from
`reason_detail` (§7.3: "Ids and versions only — never reasons' free text
beyond the code") — truncated at 32 entries with the terminal
`{"truncated": <n_total>}` element (005.1 §6.4's rule, reused verbatim,
including the null-field-omission `to_json` trick `checks.py::violations`
already documents).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spine.core import canonical
from spine.core.check_grammar import Family, GrammarDefect, family_of_kind, validate_expression
from spine.core.model import (
    RESERVED_CHECK_IDS,
    RESERVED_REASONS,
    ChecksModel,
    FactSchemaModel,
    MembershipCheckModel,
    RowCheckModel,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

# [AE-6]: `RESERVED_CHECK_IDS`/`RESERVED_REASONS` are single-element sets by
# construction (each docstring calls its member "the implicit check's own"
# id/reason, 006.1 §4.2) -- unwrapped once here rather than re-authoring a
# second literal pair, so a rename in `core/model.py` cannot silently drift
# from what this module materializes as entry 0.
_IMPLICIT_CHECK_ID: str = next(iter(RESERVED_CHECK_IDS))
_IMPLICIT_CHECK_REASON: str = next(iter(RESERVED_REASONS))
# §7.4: "the framework-reserved implicit check carries the literal version
# 'fw-1' (a governed constant, bumped only if its semantics ever change)" --
# no other normative home; authored here, at the one call site that mints it.
_IMPLICIT_CHECK_VERSION = "fw-1"

_FAILURES_COL = "_conveyer_business_failures"
_MARKER_PREFIX = "_conveyer_m_"
_TRUNCATE_AT = 32  # §7.3/005.1 §6.4: reason_detail truncates at 32 entries + one marker

# §7.3's per-check struct shape: `struct<id, reason, version>` -- distinct
# from `frames/checks.py`'s pre_check `struct<code, column, expected>` (a
# different governed vocabulary, 006.1 §4.2's own check-model fields).
_FAILURE_STRUCT_TYPE = StructType(
    [
        StructField("id", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("version", StringType(), True),
    ]
)


@dataclass(frozen=True, eq=False)
class CompiledRowCheck:
    """One compiled `row` check (or the entry-0 implicit check) -- `eq=False`
    for the same reason `frames/checks.py::CheckEntry` is (`predicate` is a
    `pyspark.sql.Column`; `Column.__eq__` builds a new `Column`, not a `bool`,
    and `Column` is unhashable). `predicate` is `True` on a row that FAILS
    this check (the `checks.py::CheckEntry` convention, reused)."""

    id: str
    reason: str
    version: str
    predicate: Column


@dataclass(frozen=True, eq=False)
class CompiledMembershipCheck:
    """One compiled `membership` check (P-8) -- carries the DECLARATION only;
    `evaluate` performs the actual join per invocation (a co-effect frame is
    a stage input, not compile-time state)."""

    id: str
    reason: str
    version: str
    co_effect: str
    columns: tuple[str, ...]
    ref_columns: tuple[str, ...]


CompiledCheckEntry = CompiledRowCheck | CompiledMembershipCheck


@dataclass(frozen=True, eq=False)
class CompiledBusinessChecks:
    """§7.1's frozen per-type value: `entries` in NORMATIVE evaluation order
    -- entry 0 the implicit check, then authored checks in authored order
    (P-5, read per `fact_type`). `eq=False`: entries may carry a `Column`
    (see `CompiledRowCheck`)."""

    fact_type: str
    entries: tuple[CompiledCheckEntry, ...]


def _schema_family_map(schema: FactSchemaModel) -> dict[str, Family | None]:
    """Reduces a bound fact type's declared columns down to the coarse
    four-family partition `check_grammar.validate_expression` consumes --
    the same mechanical kind-stripping `core/model.py::_fact_schema_family_
    map` performs for K3/K9 at bind, re-derived here (never imported: that
    helper is module-private, and the derivation is a one-line, grammar-
    anchored mechanical step, not business logic that could drift)."""
    return {column.name: family_of_kind(column.type.split("(", 1)[0]) for column in schema.columns}


def _compiled_expr(text: str, family_map: Mapping[str, Family | None]) -> Column:
    """§7.1/§6.4: re-derives a `ValidatedExpr` via gate 1 (never a second
    grammar) and returns `F.expr(validated.authored_text)` -- byte-exact,
    the one string this module ever executes [EM-6][AE-2]. A `GrammarDefect`
    here is a FRAMEWORK defect (bind already validated every authored check,
    006.1 §5.4 K4/K5/K9) -- raises, never quarantines; the linter's named
    string-SQL sink exemption for this function (006.1 §13.4 item 1)."""
    validated = validate_expression(text, "scalar", family_map)
    if isinstance(validated, GrammarDefect):
        raise ValueError(
            "business_checks: expression rejected at compile (framework defect -- "
            f"bind should have refused this spec): {validated.code}: {validated.detail}"
        )
    return F.expr(validated.authored_text)


def compile_business_checks(
    checks: ChecksModel, fact_type: str, schema: FactSchemaModel
) -> CompiledBusinessChecks:
    """§7.1: `checks` (the spec-wide `ChecksModel`) + one fact type's
    `FactSchemaModel` -> a frozen `CompiledBusinessChecks`. Plain-value pure,
    zero Spark execution beyond building lazy `Column` expression trees (no
    `.collect()`/action) -- safe to call once per type per stage invocation.
    `batch_check` entries are structurally excluded (P-6's dormancy; K7
    already refuses any `batch_check` at bind, so none can reach a validly-
    bound spec)."""
    family_map = _schema_family_map(schema)
    entries: list[CompiledCheckEntry] = [
        CompiledRowCheck(
            id=_IMPLICIT_CHECK_ID,
            reason=_IMPLICIT_CHECK_REASON,
            version=_IMPLICIT_CHECK_VERSION,
            predicate=F.col(schema.domain_id_col).isNull(),
        )
    ]
    for check in checks.checks:
        if not isinstance(check, (RowCheckModel, MembershipCheckModel)):
            continue  # BatchCheckModel: dormant (P-6), never reaches a bound spec (K7)
        if check.fact_type != fact_type:
            continue
        # §7.4: per-check content hash -- `canonical.row_hash` over the
        # parsed check's own `model_dump(mode="json")`, the SAME sha256(
        # canonical_json(...)) idiom `core/contract.py::check_version`/
        # `read_spec_version` already establish (A-11's class), reused
        # rather than a second hand-rolled hashlib call.
        version = canonical.row_hash(check.model_dump(mode="json"))
        if isinstance(check, RowCheckModel):
            holds = _compiled_expr(check.expr, family_map)
            # §7.2's three-valued law, realized for free by `F.when`'s own
            # NULL-predicate handling: `holds` is the authored condition
            # that must HOLD; `~holds` is TRUE (fails) iff `holds` is FALSE,
            # FALSE (passes) iff `holds` is TRUE, and NULL iff `holds` is
            # NULL -- `F.when(NULL, ...)` never matches, so a NULL-valued
            # `~holds` naturally falls to the "no failure" branch. No extra
            # guard needed; verified empirically (repl-driven-python).
            entries.append(
                CompiledRowCheck(
                    id=check.id, reason=check.reason, version=version, predicate=~holds
                )
            )
        else:
            entries.append(
                CompiledMembershipCheck(
                    id=check.id,
                    reason=check.reason,
                    version=version,
                    co_effect=check.co_effect,
                    columns=tuple(check.columns),
                    ref_columns=tuple(check.ref_columns),
                )
            )
    return CompiledBusinessChecks(fact_type=fact_type, entries=tuple(entries))


def _join_membership_marker(
    candidate_df: DataFrame, entry: CompiledMembershipCheck, ref_df: DataFrame
) -> tuple[DataFrame, str, Column]:
    """One membership check's join half (P-8): left-joins `candidate_df`
    against the DISTINCT projection of `ref_df`'s declared `ref_columns`
    (duplicate ref rows must never fan the candidate set out), producing a
    `_conveyer_m_<check_id>` marker column (TRUE on match, NULL otherwise).
    Ref columns are aliased to positional, check-id-namespaced names before
    the join (`_conveyer_ref_<id>_<i>`) so a `columns`/`ref_columns` name
    collision (e.g. both literally `domain_id`) can never produce a Spark
    join-column ambiguity; the aliases are dropped again immediately after.

    Returns `(joined_df, marker_column_name, fail_predicate)` -- the fail
    predicate is `(every declared columns value is non-null) AND (marker IS
    NULL)`: an EXPLICIT guard against NULL key material (P-8), not reliance
    on SQL's own null-unsafe `=` inside the join condition alone."""
    marker_col = f"{_MARKER_PREFIX}{entry.id}"
    alias_names = [f"_conveyer_ref_{entry.id}_{i}" for i in range(len(entry.ref_columns))]
    # `strict=True`: `columns`/`ref_columns` arity equality is bind-time-
    # guaranteed (`MembershipCheckModel._check_arity`, K's own model
    # validator) -- a mismatch here would be a framework defect, and
    # `zip(strict=True)` is the loud, immediate signal of exactly that.
    ref_aliases = zip(entry.ref_columns, alias_names, strict=True)
    distinct_ref = (
        ref_df.select(*[F.col(rc).alias(alias) for rc, alias in ref_aliases])
        .distinct()
        .withColumn(marker_col, F.lit(True))
    )
    join_cond: Column | None = None
    for cand_col, alias in zip(entry.columns, alias_names, strict=True):
        cond = F.col(cand_col) == F.col(alias)
        join_cond = cond if join_cond is None else (join_cond & cond)
    joined = candidate_df.join(distinct_ref, on=join_cond, how="left").drop(*alias_names)
    all_non_null: Column | None = None
    for cand_col in entry.columns:
        non_null = F.col(cand_col).isNotNull()
        all_non_null = non_null if all_non_null is None else (all_non_null & non_null)
    assert all_non_null is not None, "MembershipCheckModel.columns is min_length=1 (bind-time)"
    predicate = all_non_null & F.col(marker_col).isNull()
    return joined, marker_col, predicate


def evaluate(
    candidate_df: DataFrame, compiled: CompiledBusinessChecks, co_effects: Mapping[str, DataFrame]
) -> DataFrame:
    """§7.2: one evaluation, two outputs (`admitted_candidates`/`business_
    violations` below). Membership joins run FIRST (§7.2's own ordering:
    "membership joins first (P-8's markers)"), adding one marker column per
    membership entry; the ONE projection then builds `_conveyer_business_
    failures` = `filter(array(<struct(id, reason, version) per entry,
    evaluation order>), x -> x.failed)` (the `frames/checks.py`-established
    idiom, realized here via `F.when(pred, struct(...)).otherwise(NULL)` +
    `F.filter(..., isNotNull)`, order-preserving). Every membership marker
    column this call added is dropped before returning -- marker columns
    are join-plan detail, never part of the exported "evaluated" value
    (the same discipline `_conveyer_business_failures` itself is under,
    enforced by `admitted_candidates`/`business_violations`)."""
    working = candidate_df
    marker_predicates: dict[str, Column] = {}
    marker_cols: list[str] = []
    for entry in compiled.entries:
        if isinstance(entry, CompiledMembershipCheck):
            if entry.co_effect not in co_effects:
                raise ValueError(
                    f"business_checks.evaluate: membership check {entry.id!r} references "
                    f"undeclared co_effect {entry.co_effect!r} (bind should have refused this)"
                )
            working, marker_col, predicate = _join_membership_marker(
                working, entry, co_effects[entry.co_effect]
            )
            marker_predicates[entry.id] = predicate
            marker_cols.append(marker_col)

    per_check = []
    for entry in compiled.entries:
        predicate = (
            entry.predicate if isinstance(entry, CompiledRowCheck) else marker_predicates[entry.id]
        )
        per_check.append(
            F.when(
                predicate,
                F.struct(
                    F.lit(entry.id).alias("id"),
                    F.lit(entry.reason).alias("reason"),
                    F.lit(entry.version).alias("version"),
                ),
            ).otherwise(F.lit(None).cast(_FAILURE_STRUCT_TYPE))
        )
    failures = F.filter(F.array(*per_check), lambda x: x.isNotNull())
    evaluated = working.withColumn(_FAILURES_COL, failures)
    return evaluated.drop(*marker_cols) if marker_cols else evaluated


def admitted_candidates(evaluated_df: DataFrame) -> DataFrame:
    """§7.2/§8.1's fresh-path admit projection: `evaluated_df` (an
    `evaluate()` output) filtered to rows with an EMPTY `_conveyer_business_
    failures` array, with that internal column dropped. Every other column
    `evaluated_df` carries (the declared candidate columns, D-4) passes
    through unchanged -- the stage's own `apply` return-shape law (006.1
    §4.4) is what already guarantees `candidate_df` carried exactly the
    declared columns and nothing else; this function does not re-check it."""
    return evaluated_df.filter(F.size(F.col(_FAILURES_COL)) == 0).drop(_FAILURES_COL)


def business_violations(evaluated_df: DataFrame) -> DataFrame:
    """§7.2/§7.3: `evaluated_df` filtered to rows with at least one failure,
    with `reason_code`/`reason_detail` added and the internal `_conveyer_
    business_failures` column dropped. `reason_code` = the first surviving
    entry's `reason` (evaluation-order-first, D-6: a null-`domain_id` row's
    code is deterministically `business/missing-domain-id`, since the
    implicit check is always entry 0). `reason_detail` = deterministic
    `to_json` of the SAME array projected to `(id, version)` pairs ONLY --
    `reason` is excluded (§7.3: "Ids and versions only"); truncated at 32
    entries with the terminal `{"truncated": <n_total>}` element (005.1
    §6.4's rule; the null-field-omission `to_json` trick `frames/checks.py::
    violations` already documents, reused verbatim)."""
    failures = F.col(_FAILURES_COL)
    reshaped = F.transform(
        failures,
        lambda x: F.struct(
            x["id"].alias("id"),
            x["version"].alias("version"),
            F.lit(None).cast(IntegerType()).alias("truncated"),
        ),
    )
    total = F.size(failures)
    truncated_slice = F.slice(reshaped, 1, _TRUNCATE_AT)
    marker = F.array(
        F.struct(
            F.lit(None).cast(StringType()).alias("id"),
            F.lit(None).cast(StringType()).alias("version"),
            total.alias("truncated"),
        )
    )
    detail_array = F.when(total > _TRUNCATE_AT, F.concat(truncated_slice, marker)).otherwise(
        reshaped
    )
    shaped = (
        evaluated_df.filter(F.size(failures) > 0)
        .withColumn("reason_code", F.element_at(failures, 1).getField("reason"))
        .withColumn("reason_detail", F.to_json(detail_array))
    )
    return shaped.drop(_FAILURES_COL)
