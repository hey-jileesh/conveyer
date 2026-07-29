"""Data-path fx: pinned reads, guards, one-commit append, rendered `MERGE INTO`. LLD §7.6.

`build_spark_fx(spark, config) -> SparkFx` assembles the seven Spark-side
`RunnerFx` callables (`effects/build.py::make_runner_fx` splices them into the
production `RunnerFx`; `tests/conftest.py`'s `local_runner_fx` gets them via
the identical call, since `make_runner_fx` IS the production assembly path --
no test-only fork).

Implementation notes, keyed to §7.6 / I-3 / I-4 / I-6 / I-11 / I-19 and this
bead's own empirical findings (`conveyer-nvh.18`, own-commit attribution
hardened by `conveyer-nvh.40` [F1]):

- **Catalog wiring**: every table identifier is qualified via
  `core.naming.qualified` (`spine_cat.<db>.<table>`); the Spark session
  itself (prod `type=glue`, tests `type=hadoop`) is built by the entrypoint /
  `tests/conftest.py`, never here.
- **Snapshot resolution never uses `spark.sql`/string SQL** [S-6]: "current
  snapshot" is read off Iceberg's `<table>.refs` metadata table (the `main`
  branch head) via `.load(...)` (not a sink the string-SQL rule scopes) and a
  column-object filter; the full snapshot log for stamped-summary walks comes
  off `<table>.snapshots`, same mechanism.
- **`append`'s signature is `(table, df, batch_id, stage_key)`, not the
  literal two-arg §7.6 shape** -- see `effects/records.py::RunnerFx.append`'s
  own docstring for the full rationale (this bead's deliberate, documented
  extension: the batch/stage snapshot-property stamp can't be reliably
  derived from `(table, df)` alone, especially for a legitimately empty
  `df`).
- **MERGE own-commit attribution is unique-child-of-`before_id`, never
  "current" -- `conveyer-nvh.40` [F1]**: the ORIGINAL implementation read
  `<table>.refs`'s `main` head straight AFTER the `MERGE INTO` statement
  returned and treated it as "our" commit. Under D-12 concurrent sibling
  folds into the same state table, a sibling committing between our own
  commit and that probe lands its snapshot id + summary on OUR batch --
  wrong lineage, wrong `rows_merged`, wrong no-op detection. Fixed (this
  bead) to match I-19's own stated design: `before_id` is captured before
  the statement, and after it succeeds, own-commit resolution walks
  `<table>.snapshots` for the (at most one, `_find_child_of`) snapshot whose
  `parent_id` equals `before_id` -- structurally invariant to how much LATER
  it is resolved (unlike "current"), since Iceberg's `main` history is a
  linear chain: whoever commits FIRST after `before_id` is that snapshot's
  unique child, no matter what commits after. **Empirically verified (this
  bead's own scratch probe, two writers against one local Hadoop-catalog
  table)**: a sibling commit landing AFTER our own `MERGE INTO` returns,
  before we resolve, is correctly excluded -- our own commit remains the
  unique child of `before_id` regardless of how many further commits pile on
  afterward. **The residual gap the critique itself names ("internal
  commit-rebase makes strict unique-child imperfect")**: if a sibling
  commits BEFORE our own `MERGE INTO` statement executes (using the SAME
  `before_id` as its own base, since Spark's MERGE reads *live* table state,
  never a value pinned at `before_id`-capture time), our own successful
  commit's parent becomes the SIBLING'S snapshot, not `before_id` -- chain
  topology alone cannot then distinguish "the unique child of `before_id`"
  (the sibling) from ours (verified empirically, same scratch probe,
  opposite commit ordering). This is NOT a logical no-op (real rows really
  did merge) and must not be reported as one: `merge` detects it cheaply,
  pre-emptively, by re-reading current immediately before issuing the
  statement and comparing against `before_id` (`base_shifted`) -- a shift
  means our own eventual commit cannot legitimately be `before_id`'s unique
  child, so the result is reported as **unattributable**
  (`MergeResult(None, None, attributable=False)`, a THIRD state distinct
  from `attributable=True`'s logical no-op -- see `effects/records.py::
  MergeResult`'s own docstring for why this needs a dedicated field rather
  than reusing the no-op shape) and a `logger.warning(...)` is emitted (the
  effects layer's own permitted I/O) so this is visible even though no
  ledger/`BatchContext` field yet threads the distinction further --
  recorded as this bead's own owed follow-up, not silently assumed solved.
  This narrows, but does not mathematically close, the same race one level
  down (the sub-statement gap between the pre-commit re-read and the
  `MERGE INTO` statement's own execution) -- named here rather than assumed
  away, mirroring I-20's own "naming the premise" style for its own
  narrower residual window.
- **MERGE no-op detection is summary-based, not snapshot-id-based** --
  empirically (this bead, local Spark 3.5.9 / Iceberg 1.6.1, both
  copy-on-write and merge-on-read `write.merge.mode`), a `MERGE INTO`
  statement with at least one MATCHED or NOT MATCHED row **always** commits a
  new child snapshot, even when every `WHEN MATCHED AND ...` condition
  evaluates false and nothing is inserted -- contradicting §7.6/I-19's literal
  words ("an unchanged snapshot id after MERGE is an explicit no-op result
  ... may commit nothing"). Under copy-on-write this phantom commit rewrites
  the touched files byte-for-byte identically (summary shows nonzero
  `added-records`/`deleted-records` even though no row's *values* changed);
  under `write.merge.mode = merge-on-read` the phantom commit's summary
  reliably shows `changed-partition-count = "0"` and no add/delete-record
  keys at all -- a genuine zero-effect signal COW can't provide. `merge`
  below therefore reports `MergeResult(None, None)` (preserving I-19's
  *contract* -- `state_snapshot_id = None`, `rows_merged = 0` downstream) by
  inspecting `changed-partition-count` on the RESOLVED own-commit candidate's
  summary (never "current"'s, per the attribution fix above), not by
  diffing snapshot ids. **Precondition this relies on**: the target
  (state) table must be created with `write.merge.mode = merge-on-read` --
  this bead's own tests set that property on every state table they create;
  real state-table DDL is 007/009 territory (mirroring how `core/merge.py`
  already documents "the state-table schema must carry the ordering
  columns" as an inherited obligation). A COW-mode state table would still
  behave *correctly* (values converge identically) but would never report a
  no-op `MergeResult` -- reported as a recorded deviation, not silently
  assumed away.
- **`render_merge`** renders exactly one `MERGE INTO` as a plain string (no
  Spark call inside it -- pure, unit-testable without a session); its one
  caller, `merge`, passes the resulting string to `spark.sql` as a plain
  variable (never an inline f-string), so the call site never matches the
  string-SQL checker's "stringy literal fed directly to a sink" pattern
  regardless of the exemption. `render_merge` is still the literal
  `(spine/effects/spark.py, render_merge)` entry `tools/linter_configs/
  spine.py`'s `_STRING_SQL_EXEMPTION` already names -- unchanged, no config
  edit needed.
- **CommitFailedException / ValidationException / CommitStateUnknownException
  mapping** [T-10]: these are Iceberg (JVM) exceptions PySpark does not
  translate to a Python-native type -- they surface as `py4j.protocol.
  Py4JJavaError`, inspected here via `java_exception.getClass().getName()`
  (never `str(exc)`, which would round-trip back into the JVM gateway for a
  full Java stack trace). A real concurrent-commit conflict is impractical
  to force from a single local JVM (`local[2]`, no second writer) -- this
  bead's tests validate the mapping function directly against a
  `Py4JJavaError` built with a duck-typed fake `java_exception` (real
  `Py4JJavaError.__init__` only touches `._target_id`; no live gateway
  needed), not by trying to provoke a genuine race.
- **`MERGE_CARDINALITY_VIOLATION` -> a named defect, NOT `TransientError`**
  [T-12] (bead conveyer-nvh.36): Spark's own MERGE row-cardinality validator
  (a target row matched by more than one source row for the same key --
  only fires on the MATCHED branch, `test_scenarios_fold.py`'s own
  empirical note) surfaces as `Py4JJavaError` wrapping a Java
  `org.apache.spark.SparkException`, NOT one of the three Iceberg FQCNs
  above -- `is_transient_iceberg_failure` correctly does not recognize it,
  so left unmapped it would previously propagate as a raw `Py4JJavaError`.
  I-11 [T-12] requires this to surface as a *named*, deterministic defect
  (retrying via SFN would never help a genuinely non-conforming custom
  fold): `is_merge_cardinality_violation` inspects the wrapped Java
  exception's own `getMessage()` (verified live, this bead: contains the
  literal condition marker `[MERGE_CARDINALITY_VIOLATION]`; a class-name
  check alone can't distinguish this from any other `SparkException`) --
  still never `str(exc)`, for the same round-trip reason as the Iceberg
  predicate above. `merge` checks this BEFORE `is_transient_iceberg_
  failure` and raises a plain `ValueError` (never the caller-visible JVM
  message text -- a fixed, fixed-shape string) so this can never be
  mistaken for a retryable `TransientError`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pyspark.sql import functions as F

from spine.config import RunConfig, RunnerConfig
from spine.core.merge import MergeSpec, quote_identifier
from spine.core.naming import qualified
from spine.effects.records import MergeResult, TransientError

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from pydantic import JsonValue
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

_MERGE_SRC_VIEW = "_conveyer_merge_src"

_BATCH_ID_COL = "batch_id"
_STAGE_KEY_COL = "check_stage"
_BATCH_ID_PROPERTY = "conveyer.batch-id"
_STAGE_PROPERTY = "conveyer.stage"
_ADDED_RECORDS_KEY = "added-records"
_CHANGED_PARTITION_COUNT_KEY = "changed-partition-count"

# nvh.40 [F1]: the two points `merge`'s own-commit-race test seam
# (`_merge_race_probe`) is called at -- see that function's docstring.
_MERGE_PRE_COMMIT = "pre-commit"
_MERGE_POST_COMMIT = "post-commit"

# I-11 [T-10]: Iceberg (JVM) exception FQCNs PySpark leaves untranslated --
# surfaced as `Py4JJavaError`, matched here on the wrapped Java exception's
# own class name (never on `str(exc)`, which round-trips the JVM gateway).
_TRANSIENT_JAVA_EXCEPTION_MARKERS: tuple[str, ...] = (
    "org.apache.iceberg.exceptions.CommitFailedException",
    "org.apache.iceberg.exceptions.CommitStateUnknownException",
    "org.apache.iceberg.exceptions.ValidationException",
)

# I-11 [T-12]: Spark's own MERGE row-cardinality validator's condition
# marker, embedded in the wrapped Java exception's own `getMessage()` (see
# `is_merge_cardinality_violation` / module docstring).
_MERGE_CARDINALITY_VIOLATION_MARKER = "MERGE_CARDINALITY_VIOLATION"


def _java_exception_class_name(exc: BaseException) -> str:
    java_exception = getattr(exc, "java_exception", None)
    if java_exception is None:
        return ""
    try:
        return str(java_exception.getClass().getName())
    except Exception:  # noqa: BLE001 -- defensive: a broken fake/gateway must not mask exc
        return ""


def _java_exception_message(exc: BaseException) -> str:
    java_exception = getattr(exc, "java_exception", None)
    if java_exception is None:
        return ""
    try:
        return str(java_exception.getMessage())
    except Exception:  # noqa: BLE001 -- defensive: a broken fake/gateway must not mask exc
        return ""


def is_transient_iceberg_failure(exc: BaseException) -> bool:
    """True iff `exc` wraps one of I-11/I-19's three surfaced Iceberg (JVM)
    commit exceptions [T-10] -- the predicate `append`/`merge` use to decide
    `TransientError` vs. re-raising the original defect untouched. Exported
    (not private) so it is directly unit-testable against a constructed
    `Py4JJavaError` without a live Spark session."""
    class_name = _java_exception_class_name(exc)
    return any(marker in class_name for marker in _TRANSIENT_JAVA_EXCEPTION_MARKERS)


def is_merge_cardinality_violation(exc: BaseException) -> bool:
    """True iff `exc` wraps I-11 [T-12]'s deterministic `MERGE_CARDINALITY_
    VIOLATION` -- Spark's own MERGE row validator, raised only when the
    MATCHED branch's target row is hit by more than one source row for the
    same key (`test_scenarios_fold.py`'s own empirical note: an
    INSERT-only duplicate-key MERGE against an absent target raises
    nothing). Surfaces as `Py4JJavaError` wrapping a Java `org.apache.
    spark.SparkException` -- a class-name check alone can't distinguish
    this from any other `SparkException`, so this predicate inspects the
    wrapped Java exception's own `getMessage()` for the literal condition
    marker instead (never `str(exc)`, for the identical round-trip reason
    `is_transient_iceberg_failure` avoids it). Exported (not private) for
    the same direct-unit-testability reason as that predicate."""
    return _MERGE_CARDINALITY_VIOLATION_MARKER in _java_exception_message(exc)


def _qualified_snapshots(spark: SparkSession, qt: str) -> DataFrame:
    return spark.read.format("iceberg").load(f"{qt}.snapshots")


def _qualified_refs(spark: SparkSession, qt: str) -> DataFrame:
    return spark.read.format("iceberg").load(f"{qt}.refs")


def _current_snapshot_id(spark: SparkSession, qt: str) -> int | None:
    """The `main` branch head via `<table>.refs` (I-6) -- `None` for a
    zero-snapshot table."""
    rows = (
        _qualified_refs(spark, qt)
        .where(F.col("name") == F.lit("main"))
        .select("snapshot_id")
        .collect()
    )
    return int(rows[0]["snapshot_id"]) if rows else None


def _find_stamped_snapshot(
    spark: SparkSession, qt: str, batch_id: str, stage_key: str | None
) -> tuple[int, dict[str, str]] | None:
    """Newest-first walk of `<table>.snapshots` for the commit stamped with
    `batch_id` (+ `stage_key`, when given) -- I-19's own-commit / lineage
    resolution mechanism, shared by `append`'s own-snapshot resolution and
    the public `resolve_batch_snapshot`. `None` when absent (never written,
    or the stamped snapshot has since expired -- both look identical here,
    matching I-19's documented degrade-to-`None`)."""
    ordered = (
        _qualified_snapshots(spark, qt)
        .orderBy(F.col("committed_at").desc())
        .select("snapshot_id", "summary")
        .collect()
    )
    for row in ordered:
        summary = dict(row["summary"] or {})
        if summary.get(_BATCH_ID_PROPERTY) != batch_id:
            continue
        if stage_key is not None and summary.get(_STAGE_PROPERTY) != stage_key:
            continue
        return int(row["snapshot_id"]), summary
    return None


def _find_child_of(
    spark: SparkSession, qt: str, parent_id: int | None
) -> list[tuple[int, dict[str, str]]]:
    """Every snapshot in `qt.snapshots` whose own `parent_id` equals
    `parent_id` (`None` -> `qt`'s root snapshot, `parent_id IS NULL`) --
    `merge`'s own-commit resolution mechanism (nvh.40 [F1], I-19). Iceberg's
    `main`-branch history is a linear chain, so once ANY commit has landed
    on top of `parent_id`, there is structurally at most one such row
    (whoever committed first) -- unlike reading "current", this is
    invariant to how much LATER it is resolved, only to WHO committed
    first. See module docstring for the empirically-verified race this
    fixes, and the one it narrows but cannot fully close."""
    predicate = (
        F.col("parent_id").isNull() if parent_id is None else F.col("parent_id") == F.lit(parent_id)
    )
    rows = (
        _qualified_snapshots(spark, qt).where(predicate).select("snapshot_id", "summary").collect()
    )
    return [(int(row["snapshot_id"]), dict(row["summary"] or {})) for row in rows]


def _merge_race_probe(spark: SparkSession, qt: str, point: str) -> None:
    """No-op in production -- the one seam `merge`'s own-commit-race tests
    monkeypatch (`spark_fx._merge_race_probe = ...`) to inject a sibling
    commit at a named point around the `MERGE INTO` statement:
    `_MERGE_PRE_COMMIT` (right before it executes) or `_MERGE_POST_COMMIT`
    (right after it succeeds, before own-commit resolution). Reproduces,
    deterministically and single-threaded, the two races this bead's own
    scratch probe found empirically against a real local Iceberg table: a
    sibling landing AFTER our own commit (survived by unique-child-of-
    `before_id` resolution) and one landing BEFORE our own `MERGE INTO`
    executes (NOT survivable by chain topology alone -- caught instead by
    the pre-commit `base_shifted` check, module docstring has the full
    account)."""
    del spark, qt, point


def render_merge(spec: MergeSpec) -> str:
    """Exactly one `MERGE INTO` statement (§6.7, I-11) as a plain string --
    pure, no Spark call, no I/O; `merge` (this module's one caller) executes
    the returned text via `spark.sql(sql_text)` with `sql_text` a plain
    variable, never an inline f-string, so the string-SQL checker's
    direct-stringy-argument pattern never matches this call site regardless
    of the `_STRING_SQL_EXEMPTION` entry naming this function.

    Every identifier is already validated (`core.merge.merge_spec`/
    `_check_identifier`, §6.7 [S-10]) before it ever reaches `spec` -- this
    function only quotes (`core.merge.quote_identifier`) and assembles.
    Ordering-struct comparison (`struct(s.col, ...) > struct(t.col, ...)`)
    reproduces I-11's null-ranks-lowest, field-wise, strict-`>` semantics
    natively in Spark SQL -- verified empirically (this bead) against
    `frames.folds.ordering_struct_gt`'s independent pure-Python reference
    over a battery of null/tie/lexicographic cases; native struct comparison
    requires both sides' structs to share field names, which `struct(s.col)`/
    `struct(t.col)` do (Spark derives the field name from the referenced
    column's own simple name, not its table-alias qualifier).
    """
    target = qualified(spec.target_table)
    key_predicate = " AND ".join(
        f"t.{quote_identifier(c)} = s.{quote_identifier(c)}" for c in spec.key_cols
    )
    src_struct = "struct(" + ", ".join(f"s.{quote_identifier(c)}" for c in spec.ordering_cols) + ")"
    tgt_struct = "struct(" + ", ".join(f"t.{quote_identifier(c)}" for c in spec.ordering_cols) + ")"
    update_set = ", ".join(
        f"t.{quote_identifier(c)} = s.{quote_identifier(c)}" for c in spec.update_cols
    )
    insert_cols = (*spec.key_cols, *spec.update_cols)
    insert_col_list = ", ".join(quote_identifier(c) for c in insert_cols)
    insert_val_list = ", ".join(f"s.{quote_identifier(c)}" for c in insert_cols)
    return (
        f"MERGE INTO {target} t "
        f"USING {_MERGE_SRC_VIEW} s "
        f"ON {key_predicate} "
        f"WHEN MATCHED AND {src_struct} > {tgt_struct} THEN UPDATE SET {update_set} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_col_list}) VALUES ({insert_val_list})"
    )


def _build_read_objects(
    spark: SparkSession,
) -> Callable[[tuple[str, ...], Mapping[str, Any]], DataFrame]:
    def read_objects(uris: tuple[str, ...], read_hints: Mapping[str, JsonValue]) -> DataFrame:
        # I-P1, provisional: CSV, UTF-8, header row, all columns string (the
        # default when `inferSchema` is left unset), mode=FAILFAST -- a
        # malformed row (wrong column count vs. the header) is a
        # deterministic job failure (verified empirically: Spark raises
        # `Py4JJavaError`/`MALFORMED_CSV_RECORD` under FAILFAST for a ragged
        # row). `read_hints` (`spec.read`) is accepted but not yet consumed --
        # 005 owns real reader-hint semantics (I-P1); Phase 1's reader is
        # fixed regardless of hint content.
        #
        # nvh.38: `escape='"'` restores RFC-4180 doubled-quote decoding.
        # Spark's CSV reader defaults `escape` to backslash, not the quote
        # character -- a well-formed doubled-quote value (what `csv.writer`
        # emits for a field containing a quote and/or comma, e.g.
        # `"has""embedded,quote"`) was misparsed as a ragged row and killed
        # the whole batch under FAILFAST. This is 005's provisional reader
        # accepting valid CSV, not new reader-hint semantics -- 005 still
        # owns real reader semantics (I-P1). `multiLine` is deliberately
        # left unset: not needed for doubled-quote decoding (verified), and
        # enabling it would trade away FAILFAST's ability to catch an
        # unclosed quote (a genuinely malformed row) for support of the
        # separate, unreported embedded-newline-in-a-quoted-field case.
        del read_hints
        return (
            spark.read.format("csv")
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("mode", "FAILFAST")
            .option("escape", '"')
            .load(list(uris))
        )

    return read_objects


def _build_read_table(spark: SparkSession) -> Callable[[str], tuple[DataFrame, int]]:
    def read_table(table: str) -> tuple[DataFrame, int]:
        qt = qualified(table)
        sid = _current_snapshot_id(spark, qt)
        if sid is None:  # I-6 [T-19]: zero-snapshot table, explicit branch
            return spark.table(qt), -1
        return spark.read.option("snapshot-id", sid).table(qt), sid

    return read_table


def _build_read_batch(spark: SparkSession) -> Callable[[str, str], DataFrame]:
    def read_batch(table: str, batch_id: str) -> DataFrame:
        # Current-snapshot, unpinned: column-object predicate, never string
        # SQL [S-6]; no pinning needed (D-10/I-20 single writer + append-only).
        return spark.table(qualified(table)).where(F.col(_BATCH_ID_COL) == F.lit(batch_id))

    return read_batch


def _build_table_has_batch(
    spark: SparkSession,
) -> Callable[[str, str, str | None], bool]:
    def table_has_batch(table: str, batch_id: str, stage_key: str | None) -> bool:
        # I-3: guards read DATA, never snapshot metadata; predicate from
        # column objects and literals only.
        predicate = F.col(_BATCH_ID_COL) == F.lit(batch_id)
        if stage_key is not None:
            predicate = predicate & (F.col(_STAGE_KEY_COL) == F.lit(stage_key))
        return not spark.table(qualified(table)).where(predicate).limit(1).isEmpty()

    return table_has_batch


def _build_resolve_batch_snapshot(
    spark: SparkSession,
) -> Callable[[str, str, str | None], int | None]:
    def resolve_batch_snapshot(table: str, batch_id: str, stage_key: str | None) -> int | None:
        # Lineage-only by contract (I-3): a guard calling this is a review
        # defect -- this function itself has no way to enforce that, it is
        # simply never wired into `table_has_batch`'s own implementation
        # above.
        found = _find_stamped_snapshot(spark, qualified(table), batch_id, stage_key)
        return found[0] if found is not None else None

    return resolve_batch_snapshot


def _build_append(
    spark: SparkSession, run_config: RunConfig
) -> Callable[[str, DataFrame, str, str | None], tuple[int, dict[str, str]]]:
    def append(
        table: str, df: DataFrame, batch_id: str, stage_key: str | None
    ) -> tuple[int, dict[str, str]]:
        qt = qualified(table)
        to_write = df
        # RunConfig: repartition is a knob, not automatic -- only fires when
        # BOTH the toggle is on AND a concrete partition count is configured
        # (`shuffle_partitions=None` means "AQE decides", per RunConfig's own
        # docstring; there is nothing sensible to repartition to otherwise).
        if run_config.repartition_before_write and run_config.shuffle_partitions is not None:
            to_write = to_write.repartition(run_config.shuffle_partitions)
        target_file_size_bytes = str(run_config.target_file_size_mb * 1024 * 1024)
        writer = (
            to_write.writeTo(qt)
            .tableProperty("write.target-file-size-bytes", target_file_size_bytes)
            .option(f"snapshot-property.{_BATCH_ID_PROPERTY}", batch_id)
        )
        if stage_key is not None:
            writer = writer.option(f"snapshot-property.{_STAGE_PROPERTY}", stage_key)
        try:
            writer.append()  # I-4: exactly one commit
        except Exception as exc:  # noqa: BLE001 -- mapped below or re-raised untouched
            if is_transient_iceberg_failure(exc):  # pragma: no cover -- see module docstring:
                # a genuine local commit conflict needs a second live writer,
                # impractical from one local[2] JVM; covered at the predicate
                # level (`test_is_transient_iceberg_failure_*`) instead.
                raise TransientError(f"append to {qt} failed: {exc}") from exc
            raise
        found = _find_stamped_snapshot(spark, qt, batch_id, stage_key)
        if found is None:  # pragma: no cover -- defensive: should be unreachable
            # right after our own successful commit; treated as an infra
            # hiccup (metadata visibility lag) rather than a silent lie.
            raise TransientError(
                f"append to {qt} committed but its own stamped snapshot "
                f"(batch_id={batch_id!r}, stage={stage_key!r}) could not be resolved"
            )
        _snapshot_id, summary = found
        return int(summary.get(_ADDED_RECORDS_KEY, "0")), summary

    return append


def _build_merge(spark: SparkSession) -> Callable[[MergeSpec, DataFrame], MergeResult]:
    def merge(spec: MergeSpec, source_df: DataFrame) -> MergeResult:
        qt = qualified(spec.target_table)
        before_id = _current_snapshot_id(spark, qt)
        source_df.createOrReplaceTempView(_MERGE_SRC_VIEW)
        sql_text = render_merge(spec)
        _merge_race_probe(spark, qt, _MERGE_PRE_COMMIT)
        # nvh.40 [F1]: cheap pre-emptive detection of the residual race the
        # module docstring names -- a sibling that has ALREADY committed by
        # now shifts our own eventual commit's true parent away from
        # `before_id` (Spark's MERGE reads live table state, never a value
        # pinned at `before_id`-capture time), so "the unique child of
        # `before_id`" found below would be the SIBLING's commit, not ours.
        base_shifted = _current_snapshot_id(spark, qt) != before_id
        try:
            spark.sql(sql_text)
        except Exception as exc:  # noqa: BLE001 -- mapped below or re-raised untouched
            if is_merge_cardinality_violation(exc):
                # I-11 [T-12]: a deterministic, non-conforming custom fold --
                # a NAMED defect, never TransientError (retrying via SFN
                # would not help; the same duplicate rows would violate the
                # cardinality precondition again).
                raise ValueError(
                    f"fold cardinality defect: fold must emit at most one row per "
                    f"domain_id (merge into {qt} raised Spark's own "
                    f"MERGE_CARDINALITY_VIOLATION, I-11 [T-12]) -- the custom fold "
                    f"bound to this pipeline emitted more than one row for the same "
                    f"key in a single batch"
                ) from exc
            if is_transient_iceberg_failure(exc):  # pragma: no cover -- see append's identical note
                raise TransientError(f"merge into {qt} failed: {exc}") from exc
            raise
        _merge_race_probe(spark, qt, _MERGE_POST_COMMIT)
        if base_shifted:
            # I-19/[F1]: real rows may well have merged, but we cannot
            # safely name the snapshot as ours -- reported distinct from a
            # logical no-op (`attributable=False`; see `MergeResult`'s own
            # docstring), plus a WARNING (the effects layer's own permitted
            # I/O, S-18: identifiers/counts only, never row values).
            logger.warning(
                "fx.merge: own commit unattributable (qt=%s, before_id=%s) -- a "
                "sibling commit landed before this MERGE INTO executed [F1]",
                qt,
                before_id,
            )
            return MergeResult(None, None, attributable=False)
        found = _find_child_of(spark, qt, before_id)
        if len(found) != 1:  # pragma: no cover -- defensive: see module docstring; with
            # `base_shifted` False, our own successful commit should be the
            # unique child of `before_id` -- kept as a guard against the
            # sub-statement race the docstring names as narrowed, not
            # mathematically closed, rather than assumed impossible.
            logger.warning(
                "fx.merge: own commit unattributable (qt=%s, before_id=%s, "
                "candidates=%d) -- could not uniquely resolve as the child of "
                "before_id [F1]",
                qt,
                before_id,
                len(found),
            )
            return MergeResult(None, None, attributable=False)
        snapshot_id, summary = found[0]
        if summary.get(_CHANGED_PARTITION_COUNT_KEY, "0") == "0":
            # Module docstring: a healthy-rerun MERGE still commits a
            # snapshot on this runtime, but (under `write.merge.mode =
            # merge-on-read`) its summary reliably shows zero changed
            # partitions -- I-19's no-op contract, reported at the
            # MergeResult level even though a harmless empty snapshot
            # exists physically in the table's history.
            return MergeResult(None, None)
        return MergeResult(snapshot_id, summary)

    return merge


@dataclass(frozen=True)
class SparkFx:
    """The seven Spark-side `RunnerFx` callables (§7.6), assembled together
    since they all close over the same `SparkSession` (+ `RunConfig` for
    `append`). `effects/build.py::make_runner_fx` splices these fields
    directly into the production `RunnerFx`."""

    read_objects: Callable[[tuple[str, ...], Mapping[str, JsonValue]], DataFrame]
    read_table: Callable[[str], tuple[DataFrame, int]]
    read_batch: Callable[[str, str], DataFrame]
    table_has_batch: Callable[[str, str, str | None], bool]
    append: Callable[[str, DataFrame, str, str | None], tuple[int, dict[str, str]]]
    merge: Callable[[MergeSpec, DataFrame], MergeResult]
    resolve_batch_snapshot: Callable[[str, str, str | None], int | None]


def build_spark_fx(spark: SparkSession, config: RunnerConfig) -> SparkFx:
    """Assembles all seven Spark-side closures over one `SparkSession` and
    the `RunConfig` parsed once from `config.run_config_json` (framework-
    owned tuning surface, §6.4) -- the identical call production (`effects/
    build.py::make_runner_fx`) and tests (`tests/conftest.py::local_runner_
    fx`, via that same production call) both make; no test-only fork."""
    run_config = RunConfig.model_validate_json(config.run_config_json)
    return SparkFx(
        read_objects=_build_read_objects(spark),
        read_table=_build_read_table(spark),
        read_batch=_build_read_batch(spark),
        table_has_batch=_build_table_has_batch(spark),
        append=_build_append(spark, run_config),
        merge=_build_merge(spark),
        resolve_batch_snapshot=_build_resolve_batch_snapshot(spark),
    )
