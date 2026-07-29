"""`shape_quarantine` — quarantine rows `(batch_id, check_stage, reason)`. LLD §7.5, I-12."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from spine.core.model import LineageStamp

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def shape_quarantine(
    violations_df: DataFrame,
    stamp: LineageStamp,
    check_stage: str,
    reason: str | None = None,
) -> DataFrame:
    """Quarantine row shape (I-12): candidate columns + `reason: string` +
    the two guard keys `batch_id`, `check_stage` — nothing else.

    `reason` handles the two shapes violations arrive in:
    - `None` (default): `violations_df` already carries its OWN `reason`
      column — the I-12 contract a pipeline's `post_check` output satisfies
      (`Quarantined = tuple[Record, str]`, one reason per row). Used as-is.
    - a literal `str`: `violations_df` has no `reason` column of its own —
      the shape `frames.checks.pre_violations` (I-P2) produces — and every
      row is stamped with this one literal reason.

    Supplying a literal `reason` when `violations_df` already has one, or
    omitting it when the column is absent, is a caller defect: raised here
    (not silently guessed) since either would otherwise silently overwrite a
    real per-row reason or silently emit a NULL one.
    """
    if reason is None:
        if "reason" not in violations_df.columns:
            raise ValueError(
                "shape_quarantine: violations_df has no 'reason' column and no "
                "literal reason was supplied"
            )
        shaped = violations_df
    else:
        if "reason" in violations_df.columns:
            raise ValueError(
                "shape_quarantine: violations_df already carries a 'reason' column; "
                "do not also pass a literal reason"
            )
        shaped = violations_df.withColumn("reason", F.lit(reason))
    return shaped.withColumn("batch_id", F.lit(stamp.batch_id)).withColumn(
        "check_stage", F.lit(check_stage)
    )
