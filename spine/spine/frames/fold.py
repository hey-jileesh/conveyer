"""`reduce_batch_winners` -- the intra-batch reduce, §8.2's mechanical
necessity. LLD 007.1 §8.1 (F-6, the ordering-comparability table)/§8.2
(the per-type MERGE plan, step 2); B10, bead `conveyer-6pg.22`.

**Singular `frames/fold.py` -- the plural `frames/folds.py` is gone.**
`folds.py` used to carry the v1-era, single-hardcoded-key `default_lww_
fold`/`winners_per_domain`/`ordering_struct_gt` machinery (`LWW_ORDERING_
COLUMNS = (event_time, source_ts, content_hash)`, consumed via `Transforms.
fold`/`bind_transforms`'s fold-defaulting branch). B10 (this module's own
bead, `conveyer-6pg.22`) left it UNTOUCHED, out of its own file scope, since
`spec.fold == "custom"` is refused at `PipelineSpecModel` parse (007
D-3(e)) and `Transforms.fold` was therefore never actually exercised by the
v2 `stages/fold.py` this bead rewrote -- but it was NOT genuinely "wired for
any future consumer" as originally documented here: no consumer beyond that
one dead wiring path ever existed. Critique gate wf_24a3125f-ecc F2 (bead
conveyer-6pg.31) confirmed the dead door and deleted `frames/folds.py`
outright, along with `Transforms.fold`/`bind_transforms`'s fold-defaulting
branch. This module is the ONLY per-type-declaration-driven reduce left,
named by §8.2 at its own literal path (`frames/fold.py::
reduce_batch_winners`): the ordering key is not one hardcoded triple -- it
is `MergeSpec.ordering_cols`, derived per fact type from `FactSchemaModel.
ordering` (§4.1, `core/merge.py::merge_spec`), always ending with the two
framework elements `(source_ts, content_hash)`.

**The reduce is mechanical necessity, not optimization (§8.2, verbatim):**
Iceberg `MERGE INTO` raises `MERGE_CARDINALITY_VIOLATION` when more than one
source row matches the same target row -- an unreduced source is therefore
a defect, not a missed tuning opportunity. `reduce_batch_winners` guarantees
**at most one row per `spec.key_cols`** in its output; `stages/fold.py`'s
per-type MERGE (`WHEN MATCHED AND <ordering_predicate> THEN UPDATE ...`)
then decides, per row, whether the reduced winner actually beats the
CURRENT state -- a decision this function never makes (it has no state-table
input at all, unlike v1's `state_slice_df` parameter, §8.2's plan: "(1) read
... (2) the intra-batch reduce ... (3) the conditional MERGE" -- no state
read feeds the reduce).

**The rendering pin ([T-11], K-14 -- differentially verified, this bead,
9219 generated cases incl. 7922 null-bearing, `conveyer-hpp.13.4`'s
lineage): `row_number()` over `PARTITION BY <key_cols>` ordered by every
`spec.ordering_cols` element `DESC NULLS LAST`, keeping rank 1.** This is
§8.1's null-ranks-lowest, field-wise-lexicographic comparison rendered on a
**documented sort surface** (`Column.desc_nulls_last()`, one call per
element, no engine-internal struct/tuple comparison) -- `.desc_nulls_last()`
alone (no auxiliary "has_value" pairing column, unlike `frames/folds.py`'s
older `_ordering_sort_columns`) already gives exactly [T-11]'s semantics:
Spark's multi-column `ORDER BY` is lexicographic (each column only breaks
ties left by every earlier one, including a null-vs-null tie on one
element, which falls through to the next), and `DESC NULLS LAST` places a
null at the position a descending sort's "lowest" rank occupies -- its own
end. Differentially verified (K-14, this bead) against
`tests/integration/ordering_reference.py::compare_ordering_struct`'s plain-
Python oracle: 0 mismatches across the full generated case set, at BOTH
sites this pin covers (`core/merge.py::ordering_predicate`'s MERGE
condition and this function's own sort directives) -- native struct/tuple
comparison is not used at either site (§8.2's rejected rendering; K-14's own
"may not be substituted... without first passing this same differential").

Determinism needs no arbitrary tiebreak (§8.2): within one batch,
`content_hash` (`ordering_cols`' own always-non-null final element, F-6) is
never null, so a full tie across every element is unconstructible after
`stages/commit.py`'s own within-batch `(record_key, content_hash)` collapse
(§7.2(a)) -- two same-domain survivors always differ in `content_hash` at
minimum, making `row_number()`'s own ranking deterministic even without a
declared tiebreak. A genuine forced tie (every ordering element equal,
including `content_hash`) is possible only in hand-built test fixtures
(never a real committed batch); `row_number()` still picks exactly one row
in that case (Spark's own deterministic-per-plan tie handling), never
raising.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from spine.core.merge import MergeSpec

_WINNER_RN_COL = "_conveyer_fold_winner_rn"


def reduce_batch_winners(facts_df: DataFrame, spec: MergeSpec) -> DataFrame:
    """§8.2 step 2: at most one row per `spec.key_cols` (Phase 1: exactly
    one column, `domain_id_col`) -- the row whose `spec.ordering_cols`
    struct is the maximum within its key, per §8.1/[T-11]'s null-ranks-
    lowest, field-wise-lexicographic semantics, rendered via
    `Column.desc_nulls_last()` per ordering element (module docstring; K-14
    pins this rendering, never native struct/tuple comparison). Pure
    DataFrame-in/DataFrame-out (I-9): no `.collect()`, no per-row UDF --
    `stages/fold.py` calls this once per declared fact type, in F-4's
    declared order, immediately before `fx.merge`."""
    window = Window.partitionBy(*spec.key_cols).orderBy(
        *(F.col(column).desc_nulls_last() for column in spec.ordering_cols)
    )
    ranked = facts_df.withColumn(_WINNER_RN_COL, F.row_number().over(window))
    return ranked.filter(F.col(_WINNER_RN_COL) == 1).drop(_WINNER_RN_COL)
