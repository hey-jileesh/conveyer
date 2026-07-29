"""`merge_spec` — `MergeSpec` shaping + identifier validation. LLD §6.7.

Split choice (recorded per the bead's ask): `fx.merge(spec, source_df)`
renders and executes exactly one `MERGE INTO` — §6.7 assigns that rendering
to the effect (`effects/spark.py`, bead `conveyer-nvh.18`; already anticipated
by `tools/linter_configs/spine.py`'s `_STRING_SQL_EXEMPTION`, which names
`("spine/effects/spark.py", "render_merge")`). This module owns only the
**value** the render step consumes (`MergeSpec`) and the identifier
validation/quoting that must happen *before* any SQL text is assembled
([S-10]) — no `render_merge` lives here; that pure-string-assembly function,
if factored out at all, belongs beside its one caller in `effects/spark.py`.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from spine.core.model import PipelineSpecModel, check_qualified_table

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


@dataclass(frozen=True)
class MergeSpec:  # core/merge.py — names and predicates as data
    target_table: str  # "<db>.<table>"; EACH dot-component grammar-checked
    key_cols: tuple[str, ...]
    ordering_cols: tuple[str, ...]
    update_cols: tuple[str, ...]  # all non-key source columns


def merge_spec(
    spec: PipelineSpecModel,
    source_field_names: Sequence[str],
    ordering_cols: Sequence[str],
) -> MergeSpec:
    """Shape a `MergeSpec` from the pipeline spec's `state_table`/
    `domain_id_col` (Phase 1: `key_cols = (domain_id_col,)`), the fold's
    source (i.e. `new_rows`) schema field names, and the ordering columns
    the caller supplies (`frames.default_lww_fold`'s hardcoded key today,
    007's negotiated one later — I-11; NOT derived here, since `core/` may
    not depend on `frames/`). `update_cols` is every source column that
    isn't a key column, taken **as the transform emitted them** — every
    identifier (target table's dot-components, `key_cols`, `ordering_cols`,
    AND `update_cols`) is validated against §6.7's grammar before this
    function returns; a transform emitting a non-conforming column name
    (spaces, dots, backticks, `;`) is a defect that fails the batch here,
    before any SQL is assembled ([S-10])."""
    check_qualified_table(spec.state_table)
    key_cols = (spec.domain_id_col,)
    for col in key_cols:
        _check_identifier(col, "key_cols")
    ordering = tuple(ordering_cols)
    for col in ordering:
        _check_identifier(col, "ordering_cols")
    key_set = set(key_cols)
    update_cols = tuple(col for col in source_field_names if col not in key_set)
    for col in update_cols:
        _check_identifier(col, "update_cols")
    return MergeSpec(
        target_table=spec.state_table,
        key_cols=key_cols,
        ordering_cols=ordering,
        update_cols=update_cols,
    )
