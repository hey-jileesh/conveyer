"""`RunnerFx` (record of functions) + `TransientError`, the one exception class. LLD §7.6.

`TransientError` is the package's ONLY exception class (§7.0 rule 4, restated
at §7.6): infra hiccups (Iceberg commit conflicts after retries, EventBridge
per-entry failures, S3/Glue-catalog 5xx) that should fail the job so SFN
retries it (D-1). Raised only by `effects/*.py` functions; `core/`/`frames/`
never raise it (defects are values there) -- `core/run_facts.py` already
depends on this class existing under exactly this name, name-checked rather
than imported (see that module's own docstring for why). Exempted from the
purity linter's class-shape rule by `tools/linter_configs/spine.py`'s
`class_shape_allowlist` (a plain `Exception` subclass is not a frozen
dataclass/`BaseModel`/`Enum`).

`RunnerFx` is 004 §6.2's effect record, hardened by §7.6 with `read_objects`
(land's delivery read) and summary-returning writes (I-19: every write-side
callable returns its OWN commit's snapshot id / summary, never a value the
caller would have to re-derive by reading "current" back). Every field is a
bare `Callable` (or plain data) -- no sub-records, unlike ingestion's
`Effects` (S7.7) -- because §7.6 pins one flat record shape. `now` and
`config` are the only non-`Callable`-shaped fields (`now` supplies its OWN
clock per §6.6 [H-1]; `config` rides along as plain data).

`make_runner_fx(spark, config)` (`effects/build.py`) assembles the production
closures; `tests/conftest.py` assembles the identical shape over local Spark
+ moto (no mocks, ever) -- both build values of exactly this dataclass.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from spine.config import RunnerConfig
    from spine.core.bind_checks import TableFacts
    from spine.core.delta import MarkerRow, MarkerRowWrite
    from spine.core.merge import MergeSpec
    from spine.core.model import RawContractModel, ReadSpecModel
    from spine.core.run_facts import RunFact


class TransientError(Exception):
    """Infra failure that should retry / alarm (§7.0, §7.6). Raised only by
    effect functions in `spine/effects/*.py` -- job failure -> SFN retry ->
    alarm on exhaustion (D-1). The one exception class in the whole package.
    """


@dataclass(frozen=True)
class MergeResult:  # §7.6
    snapshot_id: int | None  # None = no-op merge OR unattributable (see `attributable`), I-19
    summary: Mapping[str, str] | None  # own commit's Iceberg summary, verbatim; None otherwise
    # nvh.40 [F1]: a THIRD field, not a bigger `snapshot_id`/`summary` union -- `(None, None)`
    # is ambiguous between "logical no-op" (I-19: healthy rerun, nothing to report) and
    # "own commit happened but could not be safely identified" (a concurrent sibling shifted
    # `fx.merge`'s own-commit resolution off `before_id` before ITS commit ran -- chain
    # topology alone can't then tell the sibling's commit apart from ours, `effects/spark.py`'s
    # own module docstring has the full empirical account). Conflating the two in the ledger
    # would silently under-report `rows_merged`/`state_snapshot_id` for a batch that DID change
    # state -- a correctness defect, not a cosmetic one -- so this is a separate, explicit
    # tri-state rather than a doc-comment convention on the same two fields. Defaults `True` so
    # every OTHER caller/construction site (single-writer happy path, no-op, every existing
    # test) is unaffected. `effects/spark.py::build_merge` logs a WARNING on the `False` path
    # (the effects layer's own permitted I/O); no ledger/`BatchContext` field yet distinguishes
    # it from a no-op downstream -- recorded as this bead's own owed follow-up, not silently
    # assumed solved by adding this field alone.
    attributable: bool = True


@dataclass(frozen=True)
class RunnerFx:  # spine/effects/records.py — §7.6's full signature
    read_objects: Callable[[tuple[str, ...], ReadSpecModel, RawContractModel], DataFrame]
    # (object_uris, read, raw_contract) -> raw DataFrame; land's delivery read (005.1 §5.8,
    # hardened from 004.1's provisional `Mapping[str, JsonValue]` hints shape -- errata-notes
    # §1.1's class, discharged by bead conveyer-azr.19, n3-admission-cut)
    read_table: Callable[[str], tuple[DataFrame, int]]  # pinned read: (table) -> (df, snapshot_id)
    # I-6 -- current snapshot resolved first, then read AT that snapshot;
    # zero-snapshot table: sid = -1 sentinel
    read_batch: Callable[[str, str], DataFrame]  # (table, batch_id) -> df; current-snapshot,
    # column-object `batch_id` predicate (D-3 read-by-name)
    table_has_batch: Callable[[str, str, str | None], bool]  # (table, batch_id, stage_key) ->
    # present; I-3 guard -- reads data, never snapshot metadata
    describe_table: Callable[[str], TableFacts | None]  # (table) -> TableFacts | None; P-4's
    # additive bind-time effect (006.1 §16.4 item 3) -- `None` iff the table does not exist;
    # otherwise its `conveyer.table-class` property (provenance, [DS-2]) + column name->type map.
    # [DS-6]: no new IAM objects -- I-21's existing per-table `Get*` grants cover it.
    append: Callable[[str, DataFrame, str, str | None], tuple[int, Mapping[str, str]]]
    # (table, df, batch_id, stage_key) -> (rows_appended, summary); ONE commit (I-4), own
    # snapshot resolved via stamped summary (I-19). DEVIATION from §7.6's literal two-arg
    # `Callable[[str, DataFrame], ...]` shape, recorded here (bead conveyer-nvh.18): the
    # `conveyer.batch-id`/`conveyer.stage` snapshot-property stamp §7.6 requires on every
    # commit can't be derived purely from `(table, df)` in general -- raw/fact lineage
    # (`frames.stamp_raw_lineage`/`stamp_fact_lineage`) stamps a `batch_id` column, but
    # deriving it via a DataFrame action (`.distinct().collect()`) is fragile for a
    # legitimately empty `df` (zero rows -> nothing to derive from), and neither raw nor
    # fact lineage carries a `stage`/`check_stage` column at all -- only the quarantine
    # table's `check_stage` column disambiguates `pre_check` vs `post_check` sharing one
    # table. `table_has_batch`/`resolve_batch_snapshot` already solve this identical
    # problem by taking `stage_key` as an explicit trailing argument rather than inferring
    # it from data; `append` now matches that shape instead of reinventing a fragile
    # column-inference path. `batch_id`/`stage_key` are exactly the values the calling
    # stage already has in hand (`ctx.batch_id`, and the same `stage_key` it passes to the
    # paired `table_has_batch`/`resolve_batch_snapshot` call for that write) -- no new
    # state, no I/O; a one-line threading change at each M3 call site.
    merge: Callable[[MergeSpec, DataFrame], MergeResult]  # (spec, source_df) -> MergeResult;
    # own snapshot | no-op (I-19)
    resolve_batch_snapshot: Callable[[str, str, str | None], int | None]  # (table, batch_id,
    # stage_key) -> snapshot_id | None; stamped-summary lookup for guard-skip lineage (I-19) --
    # NEVER called by guards (I-3)
    # --- 007.1 §4.3/§6.3 (F-9, B9b): the marker table's own read/write effects ------------------
    marker_row_present: Callable[[str, str, str, str], bool]  # (markers_table, batch_id, stage,
    # table_name) -> present; I-3-style guard (reads DATA, never snapshot metadata) over the
    # marker table's own compound idempotency key (§6.3) -- `table_has_batch`'s existing shape
    # cannot be reused as-is: that function's hardcoded `check_stage` column name is the
    # quarantine table's own convention, and it has no `table_name` predicate at all (the marker
    # table's guard-twin/completion discrimination, §6.3 answer 1)
    append_marker_row: Callable[[str, MarkerRowWrite], None]  # (markers_table, write) -> None;
    # ONE row, `snapshot_id` hardcoded NULL (§6.3's own write-order-necessity resolution) --
    # decide-then-do orchestration (the presence probe above, then this) is the CALLING stage's
    # (§4.3's normative order), never this effect's own concern, matching `plan_append`'s existing
    # split of guard mechanics (effect) from the append decision (pure plan)
    read_marker_completions: Callable[[str, str], tuple[MarkerRow, ...]]  # (markers_table,
    # feed_id) -> every commit-completion row (table_name = the sentinel) for that feed, ANY
    # batch_id -- §7.2 read 1's own input; NOT batch-keyed (a feed-wide scan, §6.3's "one priced
    # read" economics extended to this read too, [AE2-9]: no shared snapshot pin needed)
    read_marker_presence: Callable[[str, str], tuple[MarkerRow, ...]]  # (markers_table, feed_id)
    # -> every guard-twin row (table_name != the sentinel) for that feed, ANY batch_id -- §7.2
    # read 1's coherence clause AND read 2's field-absent key-match scan both consume this
    read_marker_target: Callable[[str, str], tuple[MarkerRow, ...]]  # (markers_table,
    # target_batch_id) -> every row (any table_name) for ONE batch_id -- §7.2 read 2's named-
    # target read, single-partition under §6.4's identity(batch_id) (metadata-only-miss
    # economics); called only when a seed's `supersedes_batch_id` names a target (`conveyer-kof`'s
    # own field -- unreachable in Phase 1, the named wait, §16)
    record_run: Callable[[RunFact], None]  # best-effort, NEVER raises (§7.3, §11.3)
    emit: Callable[[str, BaseModel], None]  # (detail_type, model) -> None; PutEvents,
    # raises TransientError on ANY failed entry (I-7, [T-17])
    now: Callable[[], datetime]  # attempt's own clock (§6.6 [H-1])
    config: RunnerConfig
