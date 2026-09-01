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


def _aggregate_unary(operand_family: Family | None, result_family: Family) -> _Handler:
    def handler(
        node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
    ) -> _WalkResult:
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
        return _NodeInfo(family=result_family, referenced_columns=inner.referenced_columns)

    return handler


def _h_count(
    node: exp.Expression, schema: Mapping[str, Family | None], allowed: _HandlerTable
) -> _WalkResult:
    # [EM-9]: `*` is not in the allowed set at all -- `count(*)`'s argument
    # is a bare `exp.Star`, unrecognized by `_HANDLERS_SCALAR`; the
    # authorable row-count idiom is `count(1)`. `count(DISTINCT ...)` is
    # rejected the same way `DISTINCT` is everywhere else (not a member).
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
    exp.Min: _aggregate_unary(
        None, "numeric"
    ),  # any comparable family in; numeric out (§7.5's reconciliation ground)
    exp.Max: _aggregate_unary(None, "numeric"),
    exp.Avg: _aggregate_unary("numeric", "numeric"),
}

_HANDLERS_BY_POSITION: Mapping[Position, dict[type, _Handler]] = {
    "scalar": _HANDLERS_SCALAR,
    "aggregate": {**_HANDLERS_SCALAR, **_HANDLERS_AGGREGATE_EXTRA},
}


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
