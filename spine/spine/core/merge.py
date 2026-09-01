"""`merge_spec` — `MergeSpec` shaping + identifier validation. LLD 004.1
§6.7; v2 per-type derivation + the ordering-comparison renderer, LLD 007.1
§4.1/§8.2.

Split choice (recorded per the bead's ask): `fx.merge(spec, source_df)`
renders and executes exactly one `MERGE INTO` — §6.7 assigns that rendering
to the effect (`effects/spark.py`, bead `conveyer-nvh.18`; already anticipated
by `tools/linter_configs/spine.py`'s `_STRING_SQL_EXEMPTION`, which names
`("spine/effects/spark.py", "render_merge")`). This module owns only the
**values** the render step consumes (`MergeSpec`, `ordering_predicate`'s SQL
text) and the identifier validation/quoting that must happen *before* any
SQL text is assembled ([S-10]) — no `render_merge` lives here; that
pure-string-assembly function, if factored out at all, belongs beside its
one caller in `effects/spark.py`.

**v1 -> v2 (bead conveyer-6pg.17, B6).** 006.1 P-1's hard cut (bead
conveyer-6pg's B0) deleted `PipelineSpecModel.fact_table`/`.state_table`/
the single-type `domain_id_col`-as-source-of-truth in favor of the per-type
`fact_types: dict[str, FactTypeModel]` register — `merge_spec`'s v1
signature (`spec: PipelineSpecModel, source_field_names, ordering_cols`)
read `spec.state_table` directly and cannot be given a coherent meaning
under that shape (there is no longer exactly one state table per pipeline).
`MergeSpec`'s own dataclass shape is unchanged (007.1 §4.1: "v1 shape kept
… v2 derivation + semantics") — `merge_spec` is redefined to derive ALL
FOUR fields **purely from one `FactTypeModel`** (which nests its own
`FactSchemaModel`, 006.1 §4.3) rather than from a caller-supplied source
schema + ordering-cols pair: fold's per-table loop (F-4, 007.1 §7.1) calls
this once per declared fact type, in declared order, needing nothing beyond
the bound spec. `update_cols` therefore no longer tracks "whatever the
transform's output schema happened to contain" (v1's contract) — it is now
the state table's OWN declared column set (§6.2: "the winning fact carried
whole"), mechanical and bind-derived. The one caller with the old 3-arg
call shape (`spine/stages/fold.py`) is on this bead's documented baseline-
broken list (already fails one line earlier, at `ctx.spec.state_table`,
which B0 deleted) — its own rewiring to the per-type loop is B9/B10 ground,
untouched here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from spine.core import record
from spine.core.model import FactTypeModel, check_qualified_table

# §6.7: every identifier (table dot-components, key/ordering/update column
# names) validated against this grammar before any SQL is assembled.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_identifier(value: str, role: str) -> str:
    # `.fullmatch`, not `.match`: Python's `$` matches just before a trailing
    # "\n" even without MULTILINE, so `.match()` would wrongly accept e.g.
    # "amount\n" as a conforming identifier. `.fullmatch` closes that gap.
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"{role} is not a valid identifier (§6.7): {value!r}")
    return value


def quote_identifier(value: str) -> str:
    """Backtick-quote an already-validated identifier, doubling any embedded
    backtick as defense in depth (§6.7) -- `value` must already have passed
    `_check_identifier`, which forbids backticks outright; the doubling is
    redundant-by-construction belt-and-braces, not a primary defense."""
    return "`" + value.replace("`", "``") + "`"


# 007.1 §4.1: "`ordering_cols` always ends with the two framework elements"
# -- the framework-appended tail of every ordering struct (D-3's struct;
# F-6/§8.1 pins each element's own comparison semantics). One constant, so
# the v2 derivation and `MergeSpec.__post_init__`'s construction-time
# assertion cannot drift apart.
_ORDERING_SUFFIX: tuple[str, str] = ("source_ts", "content_hash")


@dataclass(frozen=True)
class MergeSpec:  # core/merge.py — names and predicates as data
    target_table: str  # "<db>.<table>"; EACH dot-component grammar-checked --
    # 007.1 §4.1: `FactTypeModel.state_table` for the type
    key_cols: tuple[str, ...]  # 007.1 §4.1: (declared domain_id_col,) --
    # Phase 1: exactly one
    ordering_cols: tuple[str, ...]  # 007.1 §4.1: declared `ordering:` cols in
    # declared order, always ending with `_ORDERING_SUFFIX`
    update_cols: tuple[str, ...]  # all non-key state columns -- 007.1 §4.1:
    # "every §6.2 state column except key_cols -- the winning fact carried
    # whole"

    def __post_init__(self) -> None:
        # 007.1 §4.1: "Construction asserts the suffix; a `MergeSpec` without
        # it is a framework bug, not a reachable spec state." -- fires for
        # EVERY `MergeSpec`, both this module's own v2 derivation below and
        # any hand-built spec a test (or a future caller) constructs
        # directly, so the invariant cannot be forgotten at a second call
        # site. A plain `assert` (not a `raise`, [DC-6]'s "core/** bans
        # try/raise" idiom does not reach `assert`, LLD 004.1 §7.0): this is
        # an internal-invariant check on an already-bind-derived value, not
        # validation of untrusted input -- the same posture `core/model.py`/
        # `core/naming.py`/`core/reading.py` already take for their own
        # "this cannot fail on any real caller" assertions.
        assert self.ordering_cols[-2:] == _ORDERING_SUFFIX, (
            f"MergeSpec.ordering_cols must end with {_ORDERING_SUFFIX!r} (007.1 §4.1): "
            f"{self.ordering_cols!r}"
        )


def merge_spec(fact_type: FactTypeModel) -> MergeSpec:
    """007.1 §4.1's v2 pure derivation: shape one `MergeSpec` **purely from
    a bound `FactTypeModel`** (which nests its own `FactSchemaModel`, 006.1
    §4.3) -- no field is authored directly, and no field comes from a
    caller-supplied source schema (v1's `source_field_names` contract is
    retired, see this module's docstring). Fold's per-table loop (F-4, LLD
    007.1 §7.1) calls this once per declared fact type, in declared order.

    - `target_table` = `fact_type.state_table` (already identifier-checked
      at `FactTypeModel` construction, §6.7 -- re-checked here anyway,
      defense in depth, the same posture v1 took for its own caller-
      supplied inputs).
    - `key_cols` = `(fact_type.schema_.domain_id_col,)` -- Phase 1: exactly
      one.
    - `ordering_cols` = the declared `ordering:` columns, in declared order,
      with `_ORDERING_SUFFIX` appended ([H-6] discharged mechanically from
      the declaration -- §4.1).
    - `update_cols` = every §6.2 state column except `key_cols` -- the state
      row's column set is `FACT_STAMP_COLUMNS` (framework stamps, in
      `record.FACT_STAMP_TYPES`'s constant order) followed by the declared
      columns (contract order, §6.1's "stamps, then declared") -- "the
      winning fact carried whole", mechanical, never re-authored.

    Every identifier this function selects (`target_table`'s dot-components,
    `key_cols`, `ordering_cols`, `update_cols`) is validated against §6.7's
    grammar before `MergeSpec` construction, matching v1's own defense-in-
    depth posture even though every name here already passed
    `FactSchemaModel`'s/`FactTypeModel`'s own pydantic-level grammar checks
    at spec parse -- a second, cheap check at the one place SQL identifiers
    are about to be assembled from, never trusted-because-checked-elsewhere
    ([S-10])."""
    check_qualified_table(fact_type.state_table)
    schema = fact_type.schema_
    domain_id_col = schema.domain_id_col
    key_cols = (domain_id_col,)
    _check_identifier(domain_id_col, "key_cols")

    ordering_cols = tuple(schema.ordering) + _ORDERING_SUFFIX
    for col in ordering_cols:
        _check_identifier(col, "ordering_cols")

    # §6.2: "Column set = §6.1's, verbatim … stamps, then declared" --
    # `record.FACT_STAMP_TYPES` is the one normative stamp enumeration+order
    # (007.1 §5.1 fragment 4/§6.1), never re-listed here.
    stamp_cols = tuple(record.FACT_STAMP_TYPES.keys())
    declared_cols = tuple(column.name for column in schema.columns)
    all_state_cols = stamp_cols + declared_cols
    key_set = set(key_cols)
    update_cols = tuple(col for col in all_state_cols if col not in key_set)
    for col in update_cols:
        _check_identifier(col, "update_cols")

    return MergeSpec(
        target_table=fact_type.state_table,
        key_cols=key_cols,
        ordering_cols=ordering_cols,
        update_cols=update_cols,
    )


def ordering_predicate(spec: MergeSpec) -> str:
    """007.1 §8.2's "rendering decision": §8.1's ordering-struct comparison,
    rendered as one **explicit field-wise boolean** SQL expression over
    `spec.ordering_cols` -- NEVER native `struct(...)`/tuple comparison
    (§8.2's own rejected rendering: its null ordering is engine
    implementation behavior, not documented contract). Pure SQL-text
    builder over already-§6.7-identifier-validated column names (`merge_spec`
    validates every element of `ordering_cols` before a `MergeSpec` can
    exist); consumed by `effects/spark.py::_build_merge` at the `MERGE INTO`
    site (B10 ground, not wired here) as the `WHEN MATCHED AND <this>` guard.

    Per element `e` (`t`/`s` are the MERGE statement's target/source
    aliases, matching this module's sibling `render_merge`'s existing
    convention):

        gt_e  ≡ (t.e IS NULL AND s.e IS NOT NULL)
                OR (s.e IS NOT NULL AND t.e IS NOT NULL AND s.e > t.e)
        tie_e ≡ s.e <=> t.e   -- SQL null-safe equality (NULL <=> NULL = TRUE)

    every term rendered **three-valued-total** (never evaluates to SQL
    `NULL`, so the chain's truth value never leans on `NULL`-propagation
    coincidences) -- chained lexicographically, right-to-left:
    `gt_1 OR (tie_1 AND (gt_2 OR (tie_2 AND … gt_k)))`. The final element is
    always `content_hash` (F-6/§8.1: 64-hex string, never null), so a real
    `MergeSpec`-shaped input makes the chain total and a full tie renders
    the whole expression `FALSE` -- the fold's no-op. This function itself
    stays total for any non-empty `ordering_cols` (asserted -- a `MergeSpec`
    can never carry an empty one, `__post_init__`'s own suffix assertion
    guarantees at least two elements)."""
    columns = spec.ordering_cols
    assert columns, (
        "MergeSpec.ordering_cols is never empty -- __post_init__'s own "
        "suffix assertion guarantees at least two elements"
    )

    def _gt(col: str) -> str:
        q = quote_identifier(col)
        return (
            f"((t.{q} IS NULL AND s.{q} IS NOT NULL) "
            f"OR (s.{q} IS NOT NULL AND t.{q} IS NOT NULL AND s.{q} > t.{q}))"
        )

    def _tie(col: str) -> str:
        q = quote_identifier(col)
        return f"s.{q} <=> t.{q}"

    expr = _gt(columns[-1])
    for col in reversed(columns[:-1]):
        expr = f"({_gt(col)} OR ({_tie(col)} AND {expr}))"
    return expr
