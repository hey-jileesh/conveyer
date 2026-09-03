"""The expression subset allowlist — a versioned data module. LLD 006.1 §6
(P-2's two-gate design; this module is gate 1 plus P-9 rule 1); §5.4's K4/K9
bind checks consume `validate_expression` directly.

**The allowed-node set is defined against the pinned parser's actual parse
shapes [EM-2], never authored from surface syntax** — sqlglot (dialect
`spark`, exact-pinned `==` version, `spine/pyproject.toml`) normalizes
aggressively: `DATE '…'`/`date('…')`/`TIMESTAMP '…'`/`timestamp('…')` all
parse as `exp.Cast` — the SAME node kind the banned constructor casts
`int(x)`/`double(x)` use — while `CAST(x AS T)`/`x::t` parse as
`exp.TryCast` (a `Cast` SUBCLASS: `isinstance(node, exp.Cast)` would
therefore silently admit `TryCast` too — every dispatch in this module is by
**exact** `type(node)`, never `isinstance`, precisely to keep the two
apart); `nvl`/`ifnull` → `Coalesce`; `ltrim`/`rtrim` → `Trim`;
`year`/`month`/`day`/`datediff` inject `TsOrDsToDate` wrappers;
`date_add`/`date_sub` → `TsOrDsAdd` (`date_sub` additionally synthesizing
`Mul`+`Neg` around the offset — the arithmetic handlers already admit that
shape for free, no special case needed); `a NOT LIKE b`/`a IS NOT DISTINCT
FROM b` fold their negation into a `negate=True` FIELD on the plain
`Like`/`NullSafeEQ` node rather than a `Not(...)` wrapper — a genuinely
different-looking authored string can be structurally IDENTICAL to (or, for
`negate`, a same-node-different-field variant of) an admitted construct;
pure node-kind enumeration cannot both admit the typed literals and reject
casts, and cannot see the hidden extra-argument fields (`Trim.expression`,
`Length.binary`, `Round.truncate`, `Floor.decimals`/`.to`, `In.unnest`,
`DateDiff.unit`) sqlglot's grammar accepts for OTHER overloads of the same
function name. **The corpus (G-07), not this docstring, is the enforcement
artifact** [AE-8]: every accept/reject claim here is pinned by an executed
test row, discovered empirically in the kernel before being written into
this module (`repl-driven-python`'s own rule) — a future sqlglot upgrade
that shifts any of these shapes fails the corpus, not production.

**Gate 1 (node-kind + structural predicates, K4) and P-9 rule 1 (family
coherence, K9) run together, in one bottom-up recursive walk** —
`_walk`/`_HANDLERS_SCALAR`/`_HANDLERS_AGGREGATE_EXTRA` below. Each handler
both (a) checks the node's shape is exactly the admitted one (rejecting any
hidden extra field) and (b) computes the node's coarse `Family` from its
already-validated children, unifying `NULL`'s polymorphism in along the way
— `check-expression-rejected` for (a), `check-expression-mixed-types` for
(b). **No nesting of aggregates (§6.3)**: an aggregate handler always
re-enters its own argument via `_HANDLERS_SCALAR` alone, never the
aggregate-inclusive table it was itself dispatched from — an aggregate
inside an aggregate's argument is then simply an unrecognized node kind
under that stricter table, no separate nesting check needed.

**Totality — no `raise` outside `_parse_expression` (`core/**`'s
`ban_try_raise`, allowlisted here as the one `try`).** `sqlglot.parse_one`
raises `SqlglotError` (subclasses `ParseError`/`TokenError`) on malformed
text — including the perhaps-surprising case of a well-formed-looking but
syntactically incomplete fragment (`"a AND"`), which sqlglot's DEFAULT
error level raises immediately for (rather than returning a partial tree
with missing children) — `_parse_expression` is the one place this module
converts that raise into a returned `None`, per §6.4's totality contract;
every other function in this module returns a value, never raises.

**Two positions (§6.2/§6.3), one dispatch mechanism.** `scalar` covers row
`expr`, `control.expr`, and every function-argument position; `aggregate`
covers `batch_check.aggregate` only — its top-level table is
`_HANDLERS_SCALAR ∪ _HANDLERS_AGGREGATE_EXTRA` (so `sum(net) + sum(fees)`,
mixing an aggregate with §6.2 arithmetic, is authorable), while an
aggregate's own argument re-enters at `_HANDLERS_SCALAR` alone.

**Column families come from the caller, never re-derived here.**
`validate_expression`'s `schema` parameter is `Mapping[str, Family | None]`
— already-reduced FROM a bound fact type's declared column-kind grammar
(006.1 §4.1's `string|int|long|bool|decimal(p,s)|date|timestamp`) down to
this module's coarse four-family partition; an undeclared/unknown column
name maps to `None` (polymorphic, same treatment as a NULL literal) so a
K3 (column existence) violation never ALSO spuriously trips K9 here — K3 is
the caller's own, separate concern (this module has no notion of a bound
fact type's declared columns, only of families).
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, cast

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

# A version bump is a reviewed diff to this module + its A-15 (G-08) rows +
# a cross-lane portability note (§6.4) -- 006 §8's "every allowlist
# addition is a semantics decision," made mechanical. Excluded from
# `checks_version`'s hashed object (`core/checks.py::checks_version`) --
# grammar releases are additive and semantics-pinned by the executable
# table, the executing artifact is already content-pinned (I-23).
CHECK_GRAMMAR_VERSION = 1

Family = Literal["string", "numeric", "temporal", "boolean"]
Position = Literal["scalar", "aggregate"]

# §6.1: a numeric `Literal`'s `.this` is always the RAW TEXT (a `str`,
# regardless of int/decimal/float shape) -- sqlglot gives `1.245E2` and
# `1.245` the SAME node kind [EM-2], so float/double rejection (scientific
# notation included) is a literal-TEXT structural predicate, not a node-kind
# distinction. A plain decimal-point number with no exponent (`1.245`) is
# NOT scientific notation and stays admitted (the `decimal` literal member).
_SCIENTIFIC_NOTATION_RE = re.compile(r"[eE]")


@dataclass(frozen=True)
class ValidatedExpr:
    """§6.4's executed-text rule [EM-6][AE-2]: `authored_text` is the
    authored string BYTE-EXACT -- the one string gate 2 compiles and
    stage-time `F.expr` receives; this module is acceptance judge and
    reference/family extractor only, never a renderer into execution."""

    authored_text: str
    referenced_columns: frozenset[str]  # K3's input
    family: Family | None  # the whole expression's own top-level family


@dataclass(frozen=True)
class GrammarDefect:
    code: str  # "check-expression-rejected" (K4) | "check-expression-mixed-types" (K9)
    detail: str  # value-free machine detail (A-10's rule -- authored text/shape only)


@dataclass(frozen=True)
class _NodeInfo:
    family: Family | None
    referenced_columns: frozenset[str]


_WalkResult = _NodeInfo | GrammarDefect
_HandlerTable = Mapping[type, "_Handler"]
_Handler = Callable[[exp.Expression, Mapping[str, "Family | None"], _HandlerTable], _WalkResult]


def _defect(code: str, detail: str) -> GrammarDefect:
    return GrammarDefect(code=code, detail=detail)


def _is_scientific_notation(text: str) -> bool:
    return bool(_SCIENTIFIC_NOTATION_RE.search(text))


# [EM-1]: the ONE `Cast` shape admitted -- `this` a string `Literal` and
# `to` one of the temporal kinds. `TIMESTAMP '…'`/`CAST(x AS TIMESTAMP)`
# normalize to `to=TIMESTAMPTZ` while the function-call spelling
# `timestamp('…')` normalizes to `to=TIMESTAMP` (verified empirically,
# pinned sqlglot/spark dialect) -- both admitted, extending `date('…')`'s
# own "structurally identical spelling, same value" precedent symmetrically
# to the temporal function-call form.
_TYPED_LITERAL_CAST_KINDS = frozenset(
    {exp.DataType.Type.DATE, exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ}
)


def _is_typed_literal_cast(node: exp.Expression) -> bool:
    this = node.this
    to = node.args.get("to")
    return (
        isinstance(this, exp.Literal)
        and bool(this.args.get("is_string"))
        and to is not None
        and to.this in _TYPED_LITERAL_CAST_KINDS
    )


def _unify(a: Family | None, b: Family | None) -> tuple[Family | None, bool]:
    """`NULL` (and an undeclared column) is polymorphic -- a `None` family
    unifies with anything; two concrete families unify only if equal.
    Returns `(resolved_family, ok)`."""
    if a is None:
        return b, True
    if b is None:
        return a, True
    if a == b:
        return a, True
    return None, False


def _walk(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    handler = allowed.get(type(node))
    if handler is None:
        return _defect(
            "check-expression-rejected", f"node kind not in the allowed set: {type(node).__name__}"
        )
    return handler(node, schema, allowed)


def _walk_all(
    nodes: Sequence[exp.Expression], schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> list[_NodeInfo] | GrammarDefect:
    infos: list[_NodeInfo] = []
    for child in nodes:
        result = _walk(child, schema, allowed)
        if isinstance(result, GrammarDefect):
            return result
        infos.append(result)
    return infos


def _union_columns(infos: Sequence[_NodeInfo]) -> frozenset[str]:
    out: frozenset[str] = frozenset()
    for info in infos:
        out = out | info.referenced_columns
    return out


# --- leaf handlers -----------------------------------------------------------


def _h_column(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    # A qualified reference (`t.amount`) is structurally rejected -- bound
    # candidate columns are authored bare, one relation at a time (K3's own
    # ground); this is what keeps `_conveyer_fact_type`-style collisions and
    # cross-relation confusion unreachable at the grammar layer.
    if node.args.get("table") is not None:
        return _defect("check-expression-rejected", "qualified column references are not allowed")
    name = node.name
    return _NodeInfo(family=schema.get(name), referenced_columns=frozenset({name}))


def _h_literal(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    if node.args.get("is_string"):
        return _NodeInfo(family="string", referenced_columns=frozenset())
    if _is_scientific_notation(str(node.this)):
        return _defect(
            "check-expression-rejected",
            "float/double literals are not allowed (scientific notation)",
        )
    return _NodeInfo(family="numeric", referenced_columns=frozenset())


def _h_boolean(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    return _NodeInfo(family="boolean", referenced_columns=frozenset())


def _h_null(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    return _NodeInfo(family=None, referenced_columns=frozenset())


def _h_paren(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    return _walk(node.this, schema, allowed)


def _h_cast(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    if not _is_typed_literal_cast(node):
        return _defect(
            "check-expression-rejected",
            "CAST is only admitted in the typed-literal shape (DATE '...'/TIMESTAMP '...')",
        )
    return _NodeInfo(family="temporal", referenced_columns=frozenset())


# --- binary/unary family-rule handler factories ------------------------------


def _same_family_binary(result_family: Family) -> _Handler:
    """Both sides unify to ONE (possibly `None`/polymorphic) family; the
    RESULT family is fixed regardless of which family that was -- the
    comparison operators' shape (`=`, `!=`, `<`, `<=`, `>`, `>=`, `<=>`)."""

    def handler(
        node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
    ) -> _WalkResult:
        left = _walk(node.this, schema, allowed)
        if isinstance(left, GrammarDefect):
            return left
        right = _walk(node.expression, schema, allowed)
        if isinstance(right, GrammarDefect):
            return right
        _resolved, ok = _unify(left.family, right.family)
        if not ok:
            return _defect(
                "check-expression-mixed-types",
                f"comparison operands have incompatible families: "
                f"{left.family!r} vs {right.family!r}",
            )
        return _NodeInfo(
            family=result_family,
            referenced_columns=left.referenced_columns | right.referenced_columns,
        )

    return handler


def _fixed_family_binary(operand_family: Family, result_family: Family) -> _Handler:
    """Both sides must be a SPECIFIC family (not merely "same as each
    other") -- arithmetic (numeric) and AND/OR (boolean)."""

    def handler(
        node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
    ) -> _WalkResult:
        left = _walk(node.this, schema, allowed)
        if isinstance(left, GrammarDefect):
            return left
        right = _walk(node.expression, schema, allowed)
        if isinstance(right, GrammarDefect):
            return right
        for side in (left, right):
            if side.family is not None and side.family != operand_family:
                return _defect(
                    "check-expression-mixed-types",
                    f"operand is not {operand_family}: {side.family!r}",
                )
        return _NodeInfo(
            family=result_family,
            referenced_columns=left.referenced_columns | right.referenced_columns,
        )

    return handler


def _fixed_family_unary(operand_family: Family, result_family: Family) -> _Handler:
    def handler(
        node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
    ) -> _WalkResult:
        inner = _walk(node.this, schema, allowed)
        if isinstance(inner, GrammarDefect):
            return inner
        if inner.family is not None and inner.family != operand_family:
            return _defect(
                "check-expression-mixed-types", f"operand is not {operand_family}: {inner.family!r}"
            )
        return _NodeInfo(family=result_family, referenced_columns=inner.referenced_columns)

    return handler


# --- special-shape handlers (each closes a hidden-extra-field gap found in
# the kernel -- see this module's own docstring) -----------------------------


def _h_is(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    # `negate=True` is a same-node alternate spelling this grammar does not
    # admit -- an author reaching for "IS NOT X" writes `NOT (x IS NULL)`
    # via the already-admitted `Not` node, one spelling per semantic.
    if node.args.get("negate"):
        return _defect(
            "check-expression-rejected",
            "IS is only admitted as plain IS NULL -- author NOT (x IS NULL)",
        )
    if not isinstance(node.expression, exp.Null):
        return _defect(
            "check-expression-rejected",
            "IS is only admitted as IS NULL (IS TRUE/FALSE/DISTINCT FROM are not allowed)",
        )
    inner = _walk(node.this, schema, allowed)
    if isinstance(inner, GrammarDefect):
        return inner
    return _NodeInfo(family="boolean", referenced_columns=inner.referenced_columns)


def _h_in(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    if node.args.get("query") is not None:
        return _defect("check-expression-rejected", "IN with a subquery is not allowed")
    if node.args.get("unnest") is not None:
        return _defect("check-expression-rejected", "IN UNNEST(...) is not allowed")
    if node.args.get("field") is not None or node.args.get("is_global"):
        return _defect("check-expression-rejected", "IN is only admitted over a literal list")
    items = node.args.get("expressions") or []
    if not items:
        return _defect("check-expression-rejected", "IN requires a non-empty literal list")
    for item in items:
        if isinstance(item, exp.Null):
            return _defect("check-expression-rejected", "NULL is not allowed inside an IN list")
        # [D006-3]: IN is admitted over literal lists only (§6.2's "no
        # subquery node exists to accept" -- the doc's own wording extends
        # to "no column/expression member either"). A member's node kind is
        # exactly one of: `Literal` (numeric/string), `Boolean`, or `Cast`
        # (the [EM-1] typed-literal shape, itself re-validated below via the
        # normal walk -- this gate only excludes columns/expressions, not
        # non-typed-literal casts, which `_h_cast` still refuses on its own
        # terms). A bare column reference (`amount IN (qty, 5)`) is
        # otherwise structurally indistinguishable from `x = col OR ...` and
        # was accepted before this fix -- an authorable form outside §6.2's
        # own prose.
        #
        # [F1 critique fix]: sqlglot parses a negative numeric literal
        # (`-1`) as `exp.Neg(exp.Literal)`, not `exp.Literal` itself -- a
        # literal list member under §6.2 that this gate previously refused
        # with the misleading "column or expression" detail. Admit it, but
        # ONLY when `.this` is a non-string `Literal`: `Neg` wrapping a
        # `Column` (`-qty`) stays refused by the same test (its `.this` is a
        # `Column`, not a `Literal`), and `Neg` wrapping a string/boolean
        # literal (`-'x'`, `-TRUE`) is nonsensical SQL that sqlglot still
        # parses syntactically -- excluded explicitly rather than left to
        # the arithmetic-unary walk below (which would happily unify a
        # string's `None`-mapped family against numeric and accept it).
        is_negative_numeric_literal = (
            isinstance(item, exp.Neg)
            and isinstance(item.this, exp.Literal)
            and not item.this.args.get("is_string")
        )
        is_admitted_literal = isinstance(item, (exp.Literal, exp.Boolean, exp.Cast))
        if not is_admitted_literal and not is_negative_numeric_literal:
            return _defect(
                "check-expression-rejected",
                "IN list members must be literals -- a column or expression member is not allowed",
            )
    subject = _walk(node.this, schema, allowed)
    if isinstance(subject, GrammarDefect):
        return subject
    family = subject.family
    columns = subject.referenced_columns
    for item in items:
        item_info = _walk(item, schema, allowed)
        if isinstance(item_info, GrammarDefect):
            return item_info
        resolved, ok = _unify(family, item_info.family)
        if not ok:
            return _defect(
                "check-expression-mixed-types",
                f"IN list member has an incompatible family: {item_info.family!r} vs {family!r}",
            )
        family = resolved
        columns = columns | item_info.referenced_columns
    return _NodeInfo(family="boolean", referenced_columns=columns)


def _h_between(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    subject = _walk(node.this, schema, allowed)
    if isinstance(subject, GrammarDefect):
        return subject
    low = _walk(node.args["low"], schema, allowed)
    if isinstance(low, GrammarDefect):
        return low
    high = _walk(node.args["high"], schema, allowed)
    if isinstance(high, GrammarDefect):
        return high
    resolved, ok = _unify(subject.family, low.family)
    if not ok:
        return _defect("check-expression-mixed-types", "BETWEEN subject/low family mismatch")
    _resolved2, ok = _unify(resolved, high.family)
    if not ok:
        return _defect("check-expression-mixed-types", "BETWEEN subject/high family mismatch")
    return _NodeInfo(
        family="boolean",
        referenced_columns=subject.referenced_columns
        | low.referenced_columns
        | high.referenced_columns,
    )


def _h_like(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    # [AE-9]: the ESCAPE clause is rejected -- `Escape` is simply absent
    # from every handler table, so `a LIKE 'x' ESCAPE '\\'` (which wraps
    # the whole `Like` node in an `exp.Escape`) is rejected at the OUTER
    # dispatch before this handler ever runs; `negate=True` (the `NOT LIKE`
    # shorthand -- a same-node field, not a `Not(...)` wrapper) is rejected
    # here for the same one-spelling-per-semantic reason as `_h_is`.
    if node.args.get("negate"):
        return _defect(
            "check-expression-rejected",
            "NOT LIKE shorthand is not allowed -- author NOT (x LIKE y)",
        )
    subject = _walk(node.this, schema, allowed)
    if isinstance(subject, GrammarDefect):
        return subject
    pattern = _walk(node.expression, schema, allowed)
    if isinstance(pattern, GrammarDefect):
        return pattern
    for side in (subject, pattern):
        if side.family is not None and side.family != "string":
            return _defect(
                "check-expression-mixed-types", f"LIKE operand is not string: {side.family!r}"
            )
    return _NodeInfo(
        family="boolean", referenced_columns=subject.referenced_columns | pattern.referenced_columns
    )


def _h_case(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    columns: frozenset[str] = frozenset()
    branch_family: Family | None = None
    for if_node in node.args.get("ifs") or []:
        cond = _walk(if_node.this, schema, allowed)
        if isinstance(cond, GrammarDefect):
            return cond
        if cond.family is not None and cond.family != "boolean":
            return _defect(
                "check-expression-mixed-types",
                f"CASE WHEN condition is not boolean: {cond.family!r}",
            )
        columns = columns | cond.referenced_columns
        branch = _walk(if_node.args["true"], schema, allowed)
        if isinstance(branch, GrammarDefect):
            return branch
        resolved, ok = _unify(branch_family, branch.family)
        if not ok:
            return _defect(
                "check-expression-mixed-types", "CASE branches have incompatible families"
            )
        branch_family = resolved
        columns = columns | branch.referenced_columns
    default = node.args.get("default")
    if default is not None:
        default_info = _walk(default, schema, allowed)
        if isinstance(default_info, GrammarDefect):
            return default_info
        resolved, ok = _unify(branch_family, default_info.family)
        if not ok:
            return _defect("check-expression-mixed-types", "CASE ELSE has an incompatible family")
        branch_family = resolved
        columns = columns | default_info.referenced_columns
    return _NodeInfo(family=branch_family, referenced_columns=columns)


def _variadic_children(node: exp.Expression) -> list[exp.Expression]:
    """`this` + `expressions` -- the shared shape of Coalesce/Greatest/Least/Concat."""
    out = [node.this] if node.this is not None else []
    out.extend(node.args.get("expressions") or [])
    return out


def _h_homogeneous_variadic(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    """Coalesce (covers `nvl`/`ifnull`)/Greatest/Least: every argument
    unifies to ONE family (NULL polymorphic), that family is the result --
    [DC-3]'s "greatest/least argument homogeneity" divergence point."""
    children = _variadic_children(node)
    infos = _walk_all(children, schema, allowed)
    if isinstance(infos, GrammarDefect):
        return infos
    family: Family | None = None
    for info in infos:
        resolved, ok = _unify(family, info.family)
        if not ok:
            return _defect("check-expression-mixed-types", "arguments have incompatible families")
        family = resolved
    return _NodeInfo(family=family, referenced_columns=_union_columns(infos))


def _h_fixed_family_variadic(fixed_family: Family, result_family: Family) -> _Handler:
    """Concat: every argument must be a SPECIFIC family (not merely
    homogeneous with each other) -- [DC-3]'s "concat argument families"
    divergence point."""

    def handler(
        node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
    ) -> _WalkResult:
        children = _variadic_children(node)
        infos = _walk_all(children, schema, allowed)
        if isinstance(infos, GrammarDefect):
            return infos
        for info in infos:
            if info.family is not None and info.family != fixed_family:
                return _defect(
                    "check-expression-mixed-types",
                    f"argument is not {fixed_family}: {info.family!r}",
                )
        return _NodeInfo(family=result_family, referenced_columns=_union_columns(infos))

    return handler


def _h_nullif(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    """[DC-3]'s "nullif polymorphism" divergence point: both arguments must
    unify to ONE family (any family), which is the result."""
    left = _walk(node.this, schema, allowed)
    if isinstance(left, GrammarDefect):
        return left
    right = _walk(node.expression, schema, allowed)
    if isinstance(right, GrammarDefect):
        return right
    resolved, ok = _unify(left.family, right.family)
    if not ok:
        return _defect(
            "check-expression-mixed-types", "nullif arguments have incompatible families"
        )
    return _NodeInfo(
        family=resolved, referenced_columns=left.referenced_columns | right.referenced_columns
    )


def _h_trim(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    # `trim(chars FROM x)` (a genuinely different 2-arg overload, `this` =
    # the trim characters, `expression` = the target) is a hidden extra
    # field this grammar does not admit -- single-argument trim only.
    if node.args.get("expression") is not None:
        return _defect(
            "check-expression-rejected",
            "TRIM(chars FROM x) is not allowed -- single-argument trim only",
        )
    inner = _walk(node.this, schema, allowed)
    if isinstance(inner, GrammarDefect):
        return inner
    if inner.family is not None and inner.family != "string":
        return _defect(
            "check-expression-mixed-types", f"trim argument is not string: {inner.family!r}"
        )
    return _NodeInfo(family="string", referenced_columns=inner.referenced_columns)


def _h_length(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    if node.args.get("binary") is not None or node.args.get("encoding") is not None:
        return _defect("check-expression-rejected", "length() takes exactly one argument")
    inner = _walk(node.this, schema, allowed)
    if isinstance(inner, GrammarDefect):
        return inner
    if inner.family is not None and inner.family != "string":
        return _defect(
            "check-expression-mixed-types", f"length argument is not string: {inner.family!r}"
        )
    return _NodeInfo(family="numeric", referenced_columns=inner.referenced_columns)


def _h_round(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    if node.args.get("truncate") is not None:
        return _defect("check-expression-rejected", "round() takes at most two arguments")
    inner = _walk(node.this, schema, allowed)
    if isinstance(inner, GrammarDefect):
        return inner
    if inner.family is not None and inner.family != "numeric":
        return _defect(
            "check-expression-mixed-types", f"round argument is not numeric: {inner.family!r}"
        )
    columns = inner.referenced_columns
    decimals = node.args.get("decimals")
    if decimals is not None:
        decimals_info = _walk(decimals, schema, allowed)
        if isinstance(decimals_info, GrammarDefect):
            return decimals_info
        if decimals_info.family is not None and decimals_info.family != "numeric":
            return _defect("check-expression-mixed-types", "round decimals argument is not numeric")
        columns = columns | decimals_info.referenced_columns
    return _NodeInfo(family="numeric", referenced_columns=columns)


def _h_floor_ceil(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    # `floor(x, decimals)`/`floor(x TO unit)` are hidden extra-field
    # overloads this grammar does not admit -- single-argument only.
    if node.args.get("decimals") is not None or node.args.get("to") is not None:
        return _defect("check-expression-rejected", "floor()/ceil() take exactly one argument")
    inner = _walk(node.this, schema, allowed)
    if isinstance(inner, GrammarDefect):
        return inner
    if inner.family is not None and inner.family != "numeric":
        return _defect("check-expression-mixed-types", f"argument is not numeric: {inner.family!r}")
    return _NodeInfo(family="numeric", referenced_columns=inner.referenced_columns)


def _h_substring(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    """[DC-3]'s "substring result family feeding enclosing comparisons"
    divergence point -- the RESULT is string, checkable by an enclosing
    comparison against another string."""
    # [M5 critique fix]: `Substring.arg_types` carries a hidden 4th
    # `zero_start` field this handler never checked -- a previously
    # unnoticed hidden-extra-argument hole (this module's own recurring
    # hazard class) that silently accepted a 4-argument `substring(...)`
    # call, including an ENTIRELY UNVALIDATED 4th-position expression (a
    # bare column reference in that slot was never even walked). Arity is
    # (1, 3); reject the extra argument outright.
    if node.args.get("zero_start") is not None:
        return _defect("check-expression-rejected", "substring() takes at most three arguments")
    subject = _walk(node.this, schema, allowed)
    if isinstance(subject, GrammarDefect):
        return subject
    if subject.family is not None and subject.family != "string":
        return _defect(
            "check-expression-mixed-types", f"substring subject is not string: {subject.family!r}"
        )
    columns = subject.referenced_columns
    for key in ("start", "length"):
        value = node.args.get(key)
        if value is None:
            continue
        value_info = _walk(value, schema, allowed)
        if isinstance(value_info, GrammarDefect):
            return value_info
        if value_info.family is not None and value_info.family != "numeric":
            return _defect("check-expression-mixed-types", f"substring {key} is not numeric")
        columns = columns | value_info.referenced_columns
    return _NodeInfo(family="string", referenced_columns=columns)


def _h_replace(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    """[DC-3]'s "replace result family feeding enclosing comparisons"
    divergence point (result = string, symmetric to substring)."""
    this = _walk(node.this, schema, allowed)
    if isinstance(this, GrammarDefect):
        return this
    expression = _walk(node.expression, schema, allowed)
    if isinstance(expression, GrammarDefect):
        return expression
    columns = this.referenced_columns | expression.referenced_columns
    parts = [this, expression]
    replacement = node.args.get("replacement")
    if replacement is not None:
        replacement_info = _walk(replacement, schema, allowed)
        if isinstance(replacement_info, GrammarDefect):
            return replacement_info
        columns = columns | replacement_info.referenced_columns
        parts.append(replacement_info)
    for part in parts:
        if part.family is not None and part.family != "string":
            return _defect(
                "check-expression-mixed-types", f"replace argument is not string: {part.family!r}"
            )
    return _NodeInfo(family="string", referenced_columns=columns)


def _h_ts_or_ds_to_date(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    """The wrapper sqlglot injects around `year`/`month`/`day`/`datediff`'s
    arguments -- never authored directly, but the walk still recurses
    through it (it is a genuine tree node)."""
    inner = _walk(node.this, schema, allowed)
    if isinstance(inner, GrammarDefect):
        return inner
    if inner.family is not None and inner.family != "temporal":
        return _defect(
            "check-expression-mixed-types",
            f"temporal function argument is not temporal: {inner.family!r}",
        )
    return _NodeInfo(family="temporal", referenced_columns=inner.referenced_columns)


def _h_datediff(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    # The 3-arg ANSI form `datediff(unit, start, end)` sets `unit` (and
    # swaps `this`/`expression`'s roles relative to the 2-arg native Spark
    # form) -- a hidden extra-argument overload this grammar does not
    # admit; only the native 2-arg `datediff(end, start)` is authorable.
    if node.args.get("unit") is not None:
        return _defect("check-expression-rejected", "datediff() takes exactly two arguments")
    this = _walk(node.this, schema, allowed)
    if isinstance(this, GrammarDefect):
        return this
    expression = _walk(node.expression, schema, allowed)
    if isinstance(expression, GrammarDefect):
        return expression
    for side in (this, expression):
        if side.family is not None and side.family != "temporal":
            return _defect(
                "check-expression-mixed-types",
                f"datediff argument is not temporal: {side.family!r}",
            )
    return _NodeInfo(
        family="numeric", referenced_columns=this.referenced_columns | expression.referenced_columns
    )


def _h_ts_or_ds_add(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    """Covers both `date_add`/`date_sub` -- `date_sub` additionally
    synthesizes `Mul(offset, Neg(1))` around the numeric argument, a shape
    the already-admitted arithmetic handlers resolve to `numeric` for free,
    no special case needed here."""
    unit = node.args.get("unit")
    if not (isinstance(unit, exp.Var) and unit.this == "DAY"):
        return _defect("check-expression-rejected", "date_add/date_sub must be day-granularity")
    this = _walk(node.this, schema, allowed)
    if isinstance(this, GrammarDefect):
        return this
    if this.family is not None and this.family != "temporal":
        return _defect(
            "check-expression-mixed-types",
            f"date_add/date_sub subject is not temporal: {this.family!r}",
        )
    expression = _walk(node.expression, schema, allowed)
    if isinstance(expression, GrammarDefect):
        return expression
    if expression.family is not None and expression.family != "numeric":
        return _defect(
            "check-expression-mixed-types",
            f"date_add/date_sub offset is not numeric: {expression.family!r}",
        )
    return _NodeInfo(
        family="temporal",
        referenced_columns=this.referenced_columns | expression.referenced_columns,
    )


# --- aggregate handlers (aggregate position only; args re-enter SCALAR only) --


def _aggregate_unary(operand_family: Family | None, result_family: Family | None) -> _Handler:
    """`result_family=None` means the result is POLYMORPHIC -- passes the
    (already-validated) argument's own resolved family straight through,
    rather than a fixed family regardless of input (min/max's [M5 critique
    fix] shape: §7.5's reconciliation ground makes them family-unrestricted
    on the way IN, but the way OUT was wrongly hardcoded to `"numeric"`,
    silently accepting `min(seen_at) = 5`/`min(name) = 5` -- a temporal/
    string aggregate compared against a numeric literal)."""

    def handler(
        node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
    ) -> _WalkResult:
        # [M5 critique fix]: `Min`/`Max` (and, structurally, `Count`) carry a
        # hidden `expressions` field sqlglot's grammar accepts for a SECOND+
        # positional argument (`min(a, b)`) that `Sum`/`Avg` do not have at
        # all (their arity is enforced at the sqlglot parse layer instead) --
        # exactly this module's own "hidden extra-argument field" hazard,
        # previously unchecked here, silently admitting a second (even
        # cross-family) argument. FunctionEntry's own arity is (1, 1) for
        # every one of these four functions; reject the extra argument.
        if node.args.get("expressions"):
            return _defect(
                "check-expression-rejected",
                "aggregate function admits exactly one argument -- extra arguments are not allowed",
            )
        # §6.3: "no nesting of aggregates" -- the argument re-enters under
        # the SCALAR table regardless of the outer `allowed`, so a nested
        # aggregate is simply an unrecognized node kind there.
        inner = _walk(node.this, schema, _HANDLERS_SCALAR)
        if isinstance(inner, GrammarDefect):
            return inner
        if (
            operand_family is not None
            and inner.family is not None
            and inner.family != operand_family
        ):
            return _defect(
                "check-expression-mixed-types",
                f"aggregate argument is not {operand_family}: {inner.family!r}",
            )
        family = result_family if result_family is not None else inner.family
        return _NodeInfo(family=family, referenced_columns=inner.referenced_columns)

    return handler


def _h_count(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    # [EM-9]: `*` is not in the allowed set at all -- `count(*)`'s argument
    # is a bare `exp.Star`, unrecognized by `_HANDLERS_SCALAR`; the
    # authorable row-count idiom is `count(1)`. `count(DISTINCT ...)` is
    # rejected the same way `DISTINCT` is everywhere else (not a member).
    # [M5 critique fix]: `Count.arg_types` also carries the hidden
    # `expressions` field described on `_aggregate_unary` above -- reject a
    # second+ argument the same way.
    if node.args.get("expressions"):
        return _defect(
            "check-expression-rejected",
            "count admits exactly one argument -- extra arguments are not allowed",
        )
    this = node.this
    if isinstance(this, exp.Star):
        return _defect("check-expression-rejected", "count(*) is not allowed -- author count(1)")
    if isinstance(this, exp.Distinct):
        return _defect("check-expression-rejected", "count(DISTINCT ...) is not allowed")
    inner = _walk(this, schema, _HANDLERS_SCALAR)
    if isinstance(inner, GrammarDefect):
        return inner
    return _NodeInfo(family="numeric", referenced_columns=inner.referenced_columns)


# --- node-kind tables (§6.1/§6.2/§6.3) ---------------------------------------

_HANDLERS_SCALAR: dict[type, _Handler] = {
    exp.Column: _h_column,
    exp.Literal: _h_literal,
    exp.Boolean: _h_boolean,
    exp.Null: _h_null,
    exp.Paren: _h_paren,
    exp.Cast: _h_cast,
    # comparisons -- same-family both sides, boolean result
    exp.EQ: _same_family_binary("boolean"),
    exp.NEQ: _same_family_binary("boolean"),
    exp.LT: _same_family_binary("boolean"),
    exp.LTE: _same_family_binary("boolean"),
    exp.GT: _same_family_binary("boolean"),
    exp.GTE: _same_family_binary("boolean"),
    exp.NullSafeEQ: _same_family_binary(
        "boolean"
    ),  # `<=>`, also `IS NOT DISTINCT FROM`'s parse shape
    # boolean connectives
    exp.And: _fixed_family_binary("boolean", "boolean"),
    exp.Or: _fixed_family_binary("boolean", "boolean"),
    exp.Not: _fixed_family_unary("boolean", "boolean"),
    exp.Is: _h_is,
    exp.In: _h_in,
    exp.Between: _h_between,
    exp.Like: _h_like,
    # arithmetic -- numeric both sides, numeric result
    exp.Add: _fixed_family_binary("numeric", "numeric"),
    exp.Sub: _fixed_family_binary("numeric", "numeric"),
    exp.Mul: _fixed_family_binary("numeric", "numeric"),
    exp.Div: _fixed_family_binary("numeric", "numeric"),
    exp.Mod: _fixed_family_binary("numeric", "numeric"),
    exp.Neg: _fixed_family_unary("numeric", "numeric"),
    # conditional
    exp.Case: _h_case,
    exp.Coalesce: _h_homogeneous_variadic,  # covers nvl/ifnull -- same node kind
    exp.Nullif: _h_nullif,
    # numeric functions
    exp.Abs: _fixed_family_unary("numeric", "numeric"),
    exp.Round: _h_round,
    exp.Floor: _h_floor_ceil,
    exp.Ceil: _h_floor_ceil,
    exp.Greatest: _h_homogeneous_variadic,
    exp.Least: _h_homogeneous_variadic,
    # string functions
    exp.Length: _h_length,
    exp.Trim: _h_trim,  # covers ltrim/rtrim/trim -- same node kind, `position` field
    exp.Upper: _fixed_family_unary("string", "string"),
    exp.Lower: _fixed_family_unary("string", "string"),
    exp.Substring: _h_substring,
    exp.Concat: _h_fixed_family_variadic("string", "string"),
    exp.Replace: _h_replace,
    # temporal functions
    exp.TsOrDsToDate: _h_ts_or_ds_to_date,  # the year/month/day/datediff injected wrapper
    exp.Year: _fixed_family_unary("temporal", "numeric"),
    exp.Month: _fixed_family_unary("temporal", "numeric"),
    exp.Day: _fixed_family_unary("temporal", "numeric"),
    exp.DateDiff: _h_datediff,
    exp.TsOrDsAdd: _h_ts_or_ds_add,  # covers date_add/date_sub -- same node kind
}

_HANDLERS_AGGREGATE_EXTRA: dict[type, _Handler] = {
    exp.Sum: _aggregate_unary("numeric", "numeric"),
    exp.Count: _h_count,
    # any comparable family in; result family PASSES THROUGH the argument's
    # own family (§7.5's reconciliation ground) -- [M5 critique fix]: was
    # hardcoded to a fixed "numeric" result regardless of input, silently
    # accepting `min(seen_at) = 5`/`min(name) = 5`.
    exp.Min: _aggregate_unary(None, None),
    exp.Max: _aggregate_unary(None, None),
    exp.Avg: _aggregate_unary("numeric", "numeric"),
}

_HANDLERS_BY_POSITION: Mapping[Position, dict[type, _Handler]] = {
    "scalar": _HANDLERS_SCALAR,
    "aggregate": {**_HANDLERS_SCALAR, **_HANDLERS_AGGREGATE_EXTRA},
}

# --- Track A's data surface (P-2/§6.4; A006-2/A006-5): the AUTHORED-SPELLING
# function names this grammar admits, one entry per name (several spellings
# share one node kind -- `ltrim`/`rtrim`/`trim` all parse to `exp.Trim`,
# `nvl`/`ifnull`/`coalesce` all parse to `exp.Coalesce`, `date_add`/`date_sub`
# both parse to `exp.TsOrDsAdd` -- each spelling is its own name here since
# each is an independent authored surface with its own portability note).
# `test_check_grammar.py`'s [DC-3] per-argument-position signature corpus is
# tied to this exact set (a completeness assertion), so the corpus cannot
# silently drift from this data module -- add a function here FIRST, then
# its corpus rows, never the reverse. Comparison/boolean/arithmetic
# OPERATORS (`=`, `AND`, `+`, `CASE`, `IN`, ...) and the `TsOrDsToDate`
# wrapper sqlglot injects around `year`/`month`/`day`/`datediff` (never
# authored directly) are excluded -- this set is function-CALL syntax only.
SCALAR_FUNCTION_NAMES: frozenset[str] = frozenset(
    {
        "abs",
        "round",
        "floor",
        "ceil",
        "greatest",
        "least",
        "length",
        "trim",
        "ltrim",
        "rtrim",
        "upper",
        "lower",
        "substring",
        "concat",
        "replace",
        "coalesce",
        "nvl",
        "ifnull",
        "nullif",
        "year",
        "month",
        "day",
        "datediff",
        "date_add",
        "date_sub",
    }
)
AGGREGATE_FUNCTION_NAMES: frozenset[str] = frozenset({"sum", "count", "avg", "min", "max"})


@dataclass(frozen=True)
class FunctionEntry:
    """One §6.2/§6.3 function's data record (P-2's "scalar functions with
    arity + event-lane portability notes"; §6.4's "per-function event-lane
    portability notes ride the data module as fields"). `arity` is this
    GRAMMAR's own admitted (min, max) authored argument count -- not
    necessarily sqlglot's raw parse-time bound (`round(a, b, c)` parses at
    the sqlglot layer but is refused here by `_h_round`'s `truncate` check,
    so `round`'s own arity is `(1, 2)`); `999` is the "no upper bound"
    sentinel for a variadic function. `arg_families`/`result_family` use
    `None` for a homogeneous-but-polymorphic position (unifies with its
    siblings, any concrete family) -- `greatest`/`least`/`coalesce`/`nvl`/
    `ifnull`/`nullif`'s own divergence points, `count`/`min`/`max`'s
    argument (structurally family-unrestricted, §7.5's reconciliation
    ground for `min`/`max`)."""

    name: str
    node_kind: type
    arity: tuple[int, int]
    arg_families: tuple[Family | None, ...]
    result_family: Family | None
    portability_note: str


SCALAR_FUNCTIONS: tuple[FunctionEntry, ...] = (
    FunctionEntry(
        "abs", exp.Abs, (1, 1), ("numeric",), "numeric", "no cross-engine quirks; portable"
    ),
    FunctionEntry(
        "round",
        exp.Round,
        (1, 2),
        ("numeric", "numeric"),
        "numeric",
        "HALF_UP on Spark -- confirm the event lane's rounding mode matches before porting",
    ),
    FunctionEntry(
        "floor",
        exp.Floor,
        (1, 1),
        ("numeric",),
        "numeric",
        "single-argument only; the decimal-place/TO-unit overloads are refused",
    ),
    FunctionEntry(
        "ceil",
        exp.Ceil,
        (1, 1),
        ("numeric",),
        "numeric",
        "single-argument only; the decimal-place/TO-unit overloads are refused",
    ),
    FunctionEntry(
        "greatest",
        exp.Greatest,
        (1, 999),
        (None,),
        None,
        "NULL-skipping homogeneous variadic -- confirm the event lane also skips NULLs "
        "rather than propagating",
    ),
    FunctionEntry(
        "least",
        exp.Least,
        (1, 999),
        (None,),
        None,
        "NULL-skipping homogeneous variadic -- confirm the event lane also skips NULLs "
        "rather than propagating",
    ),
    FunctionEntry(
        "length",
        exp.Length,
        (1, 1),
        ("string",),
        "numeric",
        "counts characters, not bytes -- confirm parity for multi-byte UTF-8 payloads",
    ),
    FunctionEntry(
        "trim",
        exp.Trim,
        (1, 1),
        ("string",),
        "string",
        "single-argument only (no TRIM(chars FROM x) overload); whitespace only",
    ),
    FunctionEntry(
        "ltrim",
        exp.Trim,
        (1, 1),
        ("string",),
        "string",
        "single-argument only (no TRIM(chars FROM x) overload); whitespace only",
    ),
    FunctionEntry(
        "rtrim",
        exp.Trim,
        (1, 1),
        ("string",),
        "string",
        "single-argument only (no TRIM(chars FROM x) overload); whitespace only",
    ),
    FunctionEntry(
        "upper",
        exp.Upper,
        (1, 1),
        ("string",),
        "string",
        "locale-independent ASCII case folding under Spark -- confirm parity for non-ASCII scripts",
    ),
    FunctionEntry(
        "lower",
        exp.Lower,
        (1, 1),
        ("string",),
        "string",
        "locale-independent ASCII case folding under Spark -- confirm parity for non-ASCII scripts",
    ),
    FunctionEntry(
        "substring",
        exp.Substring,
        (1, 3),
        ("string", "numeric", "numeric"),
        "string",
        "1-indexed, matching SQL convention -- confirm the event lane's substring is not 0-indexed",
    ),
    FunctionEntry(
        "concat",
        exp.Concat,
        (1, 999),
        ("string",),
        "string",
        "a NULL argument yields NULL (Spark semantics, not empty-string concatenation) -- "
        "confirm parity",
    ),
    FunctionEntry(
        "replace",
        exp.Replace,
        (2, 3),
        ("string", "string", "string"),
        "string",
        "case-sensitive literal substring replacement, not regex",
    ),
    FunctionEntry(
        "coalesce",
        exp.Coalesce,
        (1, 999),
        (None,),
        None,
        "NULL-polymorphic homogeneous variadic; first non-NULL argument wins",
    ),
    FunctionEntry(
        "nvl",
        exp.Coalesce,
        (1, 999),
        (None,),
        None,
        "normalizes to the same node as coalesce()/ifnull() -- NULL-polymorphic, first "
        "non-NULL wins",
    ),
    FunctionEntry(
        "ifnull",
        exp.Coalesce,
        (1, 999),
        (None,),
        None,
        "normalizes to the same node as coalesce()/nvl() -- NULL-polymorphic, first non-NULL wins",
    ),
    FunctionEntry(
        "nullif",
        exp.Nullif,
        (2, 2),
        (None, None),
        None,
        "returns NULL on equality, the left value otherwise -- both arguments unify to one "
        "polymorphic family",
    ),
    FunctionEntry(
        "year",
        exp.Year,
        (1, 1),
        ("temporal",),
        "numeric",
        "extracted under the pinned session time zone (SESSION_PINS) -- confirm the event "
        "lane extracts under the same zone",
    ),
    FunctionEntry(
        "month",
        exp.Month,
        (1, 1),
        ("temporal",),
        "numeric",
        "extracted under the pinned session time zone (SESSION_PINS) -- confirm the event "
        "lane extracts under the same zone",
    ),
    FunctionEntry(
        "day",
        exp.Day,
        (1, 1),
        ("temporal",),
        "numeric",
        "extracted under the pinned session time zone (SESSION_PINS) -- confirm the event "
        "lane extracts under the same zone",
    ),
    FunctionEntry(
        "datediff",
        exp.DateDiff,
        (2, 2),
        ("temporal", "temporal"),
        "numeric",
        "day-granularity difference (end - start); only the native 2-arg Spark form is "
        "admitted, not the 3-arg ANSI unit form",
    ),
    FunctionEntry(
        "date_add",
        exp.TsOrDsAdd,
        (2, 2),
        ("temporal", "numeric"),
        "temporal",
        "day-granularity only; result is DATE-typed even over a TIMESTAMP subject",
    ),
    FunctionEntry(
        "date_sub",
        exp.TsOrDsAdd,
        (2, 2),
        ("temporal", "numeric"),
        "temporal",
        "day-granularity only; result is DATE-typed even over a TIMESTAMP subject",
    ),
)

AGGREGATE_FUNCTIONS: tuple[FunctionEntry, ...] = (
    FunctionEntry(
        "sum",
        exp.Sum,
        (1, 1),
        ("numeric",),
        "numeric",
        "returns NULL over an empty or all-NULL set, not zero",
    ),
    FunctionEntry(
        "count",
        exp.Count,
        (1, 1),
        (None,),
        "numeric",
        "count(1) counts rows; count(col) skips NULLs -- author with the intended spelling",
    ),
    FunctionEntry(
        "avg",
        exp.Avg,
        (1, 1),
        ("numeric",),
        "numeric",
        "widens int to double, keeps decimal as decimal -- confirm the event lane's avg "
        "widening matches",
    ),
    FunctionEntry(
        "min",
        exp.Min,
        (1, 1),
        (None,),
        None,
        "family-polymorphic in AND out; comparable across any single family, no cross-family "
        "reject applies -- the result carries whatever family the argument resolved to",
    ),
    FunctionEntry(
        "max",
        exp.Max,
        (1, 1),
        (None,),
        None,
        "family-polymorphic in AND out; comparable across any single family, no cross-family "
        "reject applies -- the result carries whatever family the argument resolved to",
    ),
)


def _parse_expression(text: str) -> exp.Expression | None:
    """The one `try` in this module (`core/**`'s `ban_try_raise` exemption
    mechanism, `tools/linter_configs/spine.py::_TRY_RAISE_ALLOWLIST`) --
    converts sqlglot's raised `SqlglotError` (parse or tokenize failure,
    including an incomplete-but-well-formed-looking fragment like `"a
    AND"`, which sqlglot's default error level raises for immediately) into
    a returned `None`, per this module's totality contract."""
    try:
        # sqlglot's own stub return type is `exp.Expr` -- a parallel type
        # in sqlglot's hierarchy, not `exp.Expression` itself (verified:
        # `exp.Expr is not exp.Expression`), even though every concrete
        # node class (`exp.Column`, `exp.Cast`, ...) genuinely IS an
        # `exp.Expression` at runtime -- a `cast` states that runtime truth
        # for mypy rather than threading the library's own alias through
        # this module's entire type surface.
        return cast(exp.Expression, sqlglot.parse_one(text, dialect="spark"))
    except SqlglotError:
        return None


def validate_expression(
    text: str, position: Position, schema: Mapping[str, Family | None]
) -> ValidatedExpr | GrammarDefect:
    """P-2 gate 1 (node-kind + structural predicates, K4) and P-9 rule 1
    (family coherence, K9) in one pass — §6.4/§6.5. `schema` maps a bound
    fact type's declared column names to their coarse family (`None` for an
    undeclared/unknown column — K3's existence check is the caller's own,
    separate concern; an undeclared column is polymorphic here so it never
    spuriously trips K9 on its own). Plain-value pure, total: never raises."""
    tree = _parse_expression(text)
    if tree is None:
        return _defect("check-expression-rejected", "text does not parse as a SQL expression")
    allowed = _HANDLERS_BY_POSITION[position]
    result = _walk(tree, schema, allowed)
    if isinstance(result, GrammarDefect):
        return result
    return ValidatedExpr(
        authored_text=text, referenced_columns=result.referenced_columns, family=result.family
    )


def family_of_kind(kind: str) -> Family | None:
    """Maps a 006.1 §4.1 fact-column KIND (`string|int|long|bool|decimal|
    date|timestamp` — the bare grammar prefix, precision/scale already
    stripped by the caller) down to this module's coarse four-family
    partition. `None` for an unrecognized kind (defensive; every real
    caller's kind already passed `FACT_COLUMN_TYPE_RE`)."""
    if kind == "string":
        return "string"
    if kind in ("int", "long", "decimal"):
        return "numeric"
    if kind in ("date", "timestamp"):
        return "temporal"
    if kind == "bool":
        return "boolean"
    return None


# =============================================================================
# §6.3/§7.5 [EM-4]: `compile_aggregate` — the aggregate position's ONE
# framework-composed translation, dormant behind P-6/K7 (conveyer-swb.15,
# D006-1's "build now, dormant" ruling).
#
# **Why a plain-value tree, not a `Column` (deviation from §6.4's own literal
# phrasing).** §6.4 describes the aggregate position as "compiled ... as a
# Column-expression tree" — but `core/**` bans importing `pyspark`
# (`tools/linter_configs/spine.py::_CORE_PROFILE`, unconditional, no
# per-file exemption mechanism for import-root bans), so this module can
# never hold a live `Column`. The split this module already draws for the
# SCALAR position — `validate_expression` (pure, this module) hands a
# byte-exact `authored_text` to `frames/business_checks.py::_compiled_expr`,
# which is the one place `F.expr` actually runs — extends here unchanged:
# `compile_aggregate` (this function) walks the ACCEPTED AST into a pure
# `AggNode` value tree (never a `Column`, never regenerated SQL text); the
# FRAMES layer (`frames/business_checks.py::aggregate_column`, which may
# import pyspark) turns that tree into the real Column-expression tree §6.4
# actually means — "no sqlglot-rendered text executes" holds exactly the
# same either way, since neither this module nor that one ever calls
# `sqlglot`'s own `.sql()` renderer to produce anything that executes.
#
# **Scope, pinned to §13.2's own DC-2 property definition.** §6.3 permits an
# aggregate's argument to be an arbitrary §6.2 scalar expression, but §13.2
# defines the DC-2 fidelity property's OWN generator as "compositions of the
# five §6.3 aggregates, §6.2 ARITHMETIC, and literals" — that is this
# function's exact, deliberate coverage: the five aggregate calls, `+ - * /
# %` and unary `-`, bare columns, and non-string literals. An aggregate
# argument nested inside a broader §6.2 construct (`CASE`, `coalesce`,
# `round`, a string function, ...) is grammar-ACCEPTED by gate 1 (§6.1/§6.2)
# but returns a `GrammarDefect` here — a named, deletable, additive gap
# (D006-1's own framing: "additive and reversible by deletion"), not a
# silent mistranslation; extending it is 005 v1.x's — or a later bead's —
# job, against this function's own kernel-validated corpus.
# =============================================================================

AggOp = Literal["+", "-", "*", "/", "%"]


@dataclass(frozen=True)
class AggColumnRef:
    """A bare column reference inside an aggregate-position expression."""

    name: str


@dataclass(frozen=True)
class AggLiteral:
    """A numeric literal, raw text (sqlglot's own `Literal.this` shape,
    §6.1's own docstring) — int-vs-decimal rendering is the frames layer's
    call (`"." in text`), never decided here."""

    text: str


@dataclass(frozen=True)
class AggNeg:
    operand: AggNode


@dataclass(frozen=True)
class AggBinOp:
    op: AggOp
    left: AggNode
    right: AggNode


@dataclass(frozen=True)
class AggCall:
    """One of the five §6.3 aggregate functions. `coalesce_zero` is [EM-4]'s
    own per-node law: `True` for `sum` only — `min`/`max`/`avg` "take no
    zero" (§7.5 verbatim) and stay NULL over an empty candidate set. `count`
    carries `coalesce_zero=False` here too: unlike `sum`, `count` is ALREADY
    zero-safe by SQL definition (count over zero rows, or over an all-NULL
    column, is 0, never NULL) — the frames layer's own `count` translation
    (`frames/business_checks.py::_aggregate_call_column`) reproduces that
    unconditionally, needing no candidate-set-empty gate at all, which is
    exactly why it is not a THIRD `coalesce_zero` variant here."""

    kind: Literal["sum", "count", "min", "max", "avg"]
    argument: AggNode
    coalesce_zero: bool


AggNode = AggColumnRef | AggLiteral | AggNeg | AggBinOp | AggCall


@dataclass(frozen=True)
class CompiledAggregate:
    """§7.5/[EM-4]'s pure translation result: `tree` is the framework-
    composed value tree (never a `Column`, never regenerated SQL — this
    module's own docstring); `referenced_columns` is K3's input, the same
    role `ValidatedExpr.referenced_columns` plays for the scalar position."""

    tree: AggNode
    referenced_columns: frozenset[str]


_AGG_OP_BY_NODE: Mapping[type, AggOp] = {
    exp.Add: "+",
    exp.Sub: "-",
    exp.Mul: "*",
    exp.Div: "/",
    exp.Mod: "%",
}
_AGG_CALL_BY_NODE: Mapping[type, Literal["sum", "count", "min", "max", "avg"]] = {
    exp.Sum: "sum",
    exp.Count: "count",
    exp.Min: "min",
    exp.Max: "max",
    exp.Avg: "avg",
}


def _compile_agg_node(node: exp.Expression) -> tuple[AggNode, frozenset[str]] | None:
    """Bottom-up, pure, total (never raises — returns `None` on anything
    outside this function's own scoped surface, per this section's own
    docstring). No family/K9 checks here: `compile_aggregate`'s caller
    obligation is a text ALREADY accepted by gate 1 at `"aggregate"`
    position (this function's own precondition, mirroring `frames/
    business_checks.py::_compiled_expr`'s identical "already validated at
    bind" contract for the scalar position) — re-deriving family coherence
    a second time here would be a second type calculus, exactly what
    §6.5 rule 2's "one-oracle rule" and this bead's own brief reject."""
    node_type = type(node)
    if node_type is exp.Paren:
        return _compile_agg_node(node.this)
    if node_type is exp.Column:
        return AggColumnRef(name=node.name), frozenset({node.name})
    if node_type is exp.Literal:
        if node.args.get("is_string"):
            return None  # out of scope: the aggregate surface is numeric-only per §6.3
        return AggLiteral(text=str(node.this)), frozenset()
    if node_type is exp.Neg:
        inner = _compile_agg_node(node.this)
        if inner is None:
            return None
        inner_node, inner_cols = inner
        return AggNeg(operand=inner_node), inner_cols
    op = _AGG_OP_BY_NODE.get(node_type)
    if op is not None:
        left = _compile_agg_node(node.this)
        right = _compile_agg_node(node.expression)
        if left is None or right is None:
            return None
        left_node, left_cols = left
        right_node, right_cols = right
        return AggBinOp(op=op, left=left_node, right=right_node), left_cols | right_cols
    kind = _AGG_CALL_BY_NODE.get(node_type)
    if kind is not None:
        # §6.3: "no nesting of aggregates" — already gate-1-refused at bind
        # (this function's own precondition), so `node.this` here is always
        # a §6.2 scalar-position expression, never a nested aggregate call.
        inner = _compile_agg_node(node.this)
        if inner is None:
            return None
        arg_node, cols = inner
        return AggCall(kind=kind, argument=arg_node, coalesce_zero=(kind == "sum")), cols
    return None  # out of this function's scoped surface (§13.2's own generator scope)


def compile_aggregate(
    validated: ValidatedExpr, schema: Mapping[str, Family | None]
) -> CompiledAggregate | GrammarDefect:
    """§7.5/[EM-4]: `validated` MUST already be an ACCEPTED `"aggregate"`-
    position `ValidatedExpr` (`validate_expression(text, "aggregate",
    schema)` — the caller's own obligation, this function's precondition,
    exactly as `frames/business_checks.py::_compiled_expr` requires for the
    scalar position). Re-parses `validated.authored_text` BYTE-EXACT (a
    deterministic re-parse — the SAME `_parse_expression` this module's own
    gate 1 uses, never a second grammar) and walks it into a pure `AggNode`
    tree (never a `Column` — this section's own docstring). `schema` is
    accepted for signature symmetry with `validate_expression` and as a
    reserved hook for 005 v1.x's member-grammar coherence check (§9's named
    wait) — this function does not itself consult it: family coherence was
    already asserted once, at gate 1, over the SAME text (§6.5 rule 1's
    one-pass law; re-deriving it here would be the second type calculus
    §6.5 rule 2 and this bead's own brief both reject). A construct outside
    this function's own scoped surface (this section's docstring) returns a
    `GrammarDefect` — a framework-coverage gap, not an authored-spec defect,
    but shaped identically so callers already handling `GrammarDefect`
    (e.g. `entrypoints/glue_main.py`'s K5) need no second result type."""
    tree = _parse_expression(validated.authored_text)
    if tree is None:
        return _defect(
            "check-expression-rejected",
            "compile_aggregate: authored_text does not re-parse (framework defect)",
        )
    result = _compile_agg_node(tree)
    if result is None:
        return _defect(
            "check-expression-rejected",
            "compile_aggregate: construct outside its scoped surface (the five §6.3 "
            "aggregates, §6.2 arithmetic, columns, and literals — §13.2's own DC-2 "
            "property scope)",
        )
    agg_node, referenced_columns = result
    return CompiledAggregate(tree=agg_node, referenced_columns=referenced_columns)
