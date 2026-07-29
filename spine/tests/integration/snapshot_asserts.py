"""R-13 harness — the one-commit invariant (I-4), as a reusable assertion.

`snapshot_ids(spark, table)` captures a table's whole current snapshot-id set
(the harness's own "before" checkpoint, taken by the caller before an
effectful stage/fx call runs); `snapshot_delta(spark, table, before_ids)`
asserts **exactly one** new snapshot appeared since that checkpoint (I-4's
mechanically-checkable half: "a stage's write to a table is exactly one
`df.writeTo(t).append()` call or one `MERGE INTO` statement" ⇒ exactly one
new commit, never zero-but-should-have-been-one, never two-or-more) and
returns that snapshot's own `(snapshot_id, summary)` for a caller to further
assert the `conveyer.batch-id`/`conveyer.stage` stamp on
(`assert_stamped_batch`) — the same summary shape `append`/`merge`
(`spine/effects/spark.py`) themselves resolve and return.

This bead (`conveyer-nvh.18`) ships the harness plus its own self-test
(`test_spark_fx.py::test_snapshot_delta_harness_asserts_exactly_one_append`);
M4 (`conveyer-nvh.2x`+, R-01/R-03/R-08/R-13 scenario suite) is this module's
first real *user*, wrapping whole multi-stage runs rather than one `fx` call
at a time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def snapshot_ids(spark: SparkSession, table: str) -> frozenset[int]:
    """Every snapshot id currently in `table`'s snapshot log (a fully-
    qualified `spine_cat.<db>.<table>` identifier) — the harness's "before"
    checkpoint. Works on a zero-snapshot table (empty result -> empty set)."""
    rows = spark.read.format("iceberg").load(f"{table}.snapshots").select("snapshot_id").collect()
    return frozenset(int(row["snapshot_id"]) for row in rows)


def snapshot_delta(
    spark: SparkSession, table: str, before_ids: frozenset[int]
) -> tuple[int, dict[str, str]]:
    """Asserts exactly one new snapshot appeared on `table` since
    `before_ids` (I-4) and returns that snapshot's `(snapshot_id, summary)`.

    Raises `AssertionError` (not a silent falsy return) on zero new
    snapshots (the effectful call was supposed to commit but didn't) or on
    more than one (a one-commit-invariant violation — a banned per-partition
    write loop, a chunked append, or similar, per I-4's ban list).
    """
    after_ids = snapshot_ids(spark, table)
    new_ids = after_ids - before_ids
    if len(new_ids) != 1:
        raise AssertionError(
            f"one-commit invariant (I-4) violated on {table}: expected exactly one new "
            f"snapshot since {sorted(before_ids)!r}, got {sorted(new_ids)!r} "
            f"(current snapshots: {sorted(after_ids)!r})"
        )
    (new_id,) = new_ids
    rows = (
        spark.read.format("iceberg")
        .load(f"{table}.snapshots")
        .where(F.col("snapshot_id") == F.lit(new_id))
        .select("summary")
        .collect()
    )
    summary = dict(rows[0]["summary"] or {}) if rows else {}
    return new_id, summary


def assert_no_new_snapshot(spark: SparkSession, table: str, before_ids: frozenset[int]) -> None:
    """The fold no-op / guard-skip counterpart to `snapshot_delta`: asserts
    zero new snapshots appeared (a healthy rerun's guarded stages never
    write at all; §7.5's fold no-op is the one case where a real `merge`
    call may still leave a harmless empty commit behind at the *physical*
    Iceberg level -- see `effects/spark.py`'s module docstring -- so this
    assertion is for the stages that genuinely make no `fx` call at all on a
    guard-skip, not for fold's own MergeResult no-op)."""
    after_ids = snapshot_ids(spark, table)
    new_ids = after_ids - before_ids
    if new_ids:
        raise AssertionError(
            f"expected zero new snapshots on {table} since {sorted(before_ids)!r}, "
            f"got {sorted(new_ids)!r}"
        )


def assert_stamped_batch(
    summary: dict[str, str], batch_id: str, stage_key: str | None = None
) -> None:
    """Asserts a commit's own summary carries the I-3/I-19 lineage stamp:
    `conveyer.batch-id == batch_id`, and (when `stage_key` is given)
    `conveyer.stage == stage_key`."""
    assert summary.get("conveyer.batch-id") == batch_id, (
        f"expected conveyer.batch-id={batch_id!r} in summary, got {summary!r}"
    )
    if stage_key is not None:
        assert summary.get("conveyer.stage") == stage_key, (
            f"expected conveyer.stage={stage_key!r} in summary, got {summary!r}"
        )
