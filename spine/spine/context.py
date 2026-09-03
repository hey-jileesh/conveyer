"""`BatchContext` — the accreting value the sequence driver folds over. LLD §6.3.

One frozen dataclass; every post-seed field defaults to `None` (or `()` /
`False`) and is set exactly once via `dataclasses.replace` -- **setting a
non-`None` field twice is a bug**, asserted in `run.py` (bead `conveyer-
nvh.19`) against `SET_ONCE_EXEMPT_FIELDS` below. `guard_skips` accretes
(§6.3, [E-13]) and is the one field exempt from that rule by name.

`DataFrame`-typed fields are lazy plans (004 D-2) -- this module never reads
or computes over them, only carries them, so it does not need a real pyspark
import to be importable (no `SparkSession` required just to build a seed
`BatchContext` in a unit test). `from __future__ import annotations` postpones
annotation evaluation to strings; `DataFrame` itself is only imported under
`TYPE_CHECKING`.

`Mapping`-typed fields (`co_effects`, `co_effect_snapshot_ids`,
and 007.1 §4.2's seven per-type deltas below: `facts_appended_by_table`,
`commit_snapshot_ids`, `rows_merged_by_table`, `fold_snapshot_ids`,
`delta_read_snapshot_ids`) are documented as `types.MappingProxyType`-wrapped
by convention: whichever stage constructs them (`pull`, `commit`, `fold`)
wraps the built dict once before `dataclasses.replace`, so callers never see
a mutable mapping on the context. This module does not enforce the wrapping
(a frozen dataclass field's value is not itself made read-only by the
dataclass) -- it is a construction-site contract, same as "set exactly once"
itself.

**007.1 §4.2 additions (bead conveyer-6pg.17, B6): the seven per-type
`BatchContext` deltas.** `facts_appended_by_table`/`commit_snapshot_ids`
(commit, per fact table) and `rows_merged_by_table`/`fold_snapshot_ids`
(fold, per state table) are F-4's per-table generalization made fields
(errata-notes item 20's field-level ground); `delta_predecessor_batch_ids`/
`delta_read_snapshot_ids`/`delta_probe_refusal` (commit) are F-5's resolved
predecessor set, its one pinned per-table read snapshot, and its probe
verdict (`core/delta.py::resolve_predecessors`'s `PredecessorResolution`,
projected set-once). All seven are the additive-field class §4.5 already
covers -- no exemption-list change, `run.py`'s reflective set-once assertion
(`SET_ONCE_EXEMPT_FIELDS` below) already treats them like any other
post-seed field. **Marker writes accrete no context field in Phase 1**
(007.1 §4.2's own note): batch-truth lives only in the marker table (F-9,
L-1's seam applied to the context itself) -- these seven fields carry
commit/fold's OWN attempt-scoped bookkeeping, never the marker table's
durable state.

**005.1 §3.5 additions (bead conveyer-azr.18, n3-context-wiring)**:
`read_spec_version`/`check_version` (A-11) are **seed-adjacent**, not
post-seed-optional -- they carry no default, same as the other seed fields
above, because they are pure functions of the parsed spec (`core.contract.
read_spec_version`/`check_version`), computed exactly once by the entrypoint
(`entrypoints/glue_main.py::_seed_batch_context`) right after spec parse and
carried unchanged for the rest of the run; the reflective set-once assertion
(`run.py::_assert_set_once`) already covers a no-default field as
"already-set from the very first stage onward", so no exemption-list change
is needed for either. `pre_check_drift` (A-9) is a genuine post-seed,
stage-set field -- an EXACT mirror of `post_check_drift` below, just for
`pre_check`'s own guard-skip rerun doors (§6.5): `None` until
`stages/pre_check.py` (bead conveyer-azr.19, n3-admission-cut) sets it on a
drift mismatch, folded into the ledger row's `error_message` by
`core/run_facts.py::_stage_fields` and surfaced as WARNING + EMF
`PreCheckDrift` by `effects/ledger.py::record_run`, exactly as
`post_check_drift` already is.

**006.1 §4.5 additions (bead conveyer-6pg.13, B3 -- the apply/post_check
per-type flip).** `checks_version` is **seed-adjacent**, the SAME class as
`read_spec_version`/`check_version` above -- a pure function of the parsed
spec (`core.checks.checks_version(spec.checks)`), computed exactly once by
the entrypoint right after spec parse and carried unchanged for the rest of
the run; P-3 (§7.4). `candidate_facts`/`admitted_facts` (`Mapping[str,
DataFrame]`) REPLACE the singular `candidate_facts_df`/`admitted_facts_df`
(006.1 P-1's per-type declaration surface, errata-notes item 19's
completion): `apply` now returns one candidate frame per declared fact type
(§4.4's runtime return-shape law, enforced in `stages/apply.py`), and
`post_check`'s interpreter now admits/quarantines per type (§7-§8).
**Erratum (critique gate wf_24a3125f-ecc ruling 1, bead conveyer-6pg.29,
F4):** this paragraph originally claimed `stages/commit.py`/`stages/fold.py`
"still read the now-deleted singular fields ... and stay broken" pending
007.1 B9b/B10 -- both landed (bead conveyer-6pg.21 and conveyer-6pg.22
respectively) before this bead, so the claim is stale. Both stages have long
since moved to the per-table maps above; the singular scalar fields those
two stages used to set (`facts_appended`, `fact_snapshot_id`,
`committed_facts_df`, `state_read_snapshot_id`, `state_snapshot_id`,
`merge_summary`) were themselves permanently vestigial (never read by any
consumer once the per-table maps landed) and have been deleted from
`BatchContext` by F4 -- see `core/run_facts.py`'s "totals / one-snapshot
symmetric rule" for how the ledger's own equivalently-named columns are
now derived purely from the per-table maps instead.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Final

from spine.binding import Transforms
from spine.config import RunConfig
from spine.core.model import BatchCompletedV1, PipelineSpecModel

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pyspark.sql import DataFrame

# The accreting-tuple exemption [E-13]: `guard_skips` is the one field a
# stage may set (accrete into) more than once. `run.py`'s set-once assertion
# (bead conveyer-nvh.19) checks every other field against this set.
SET_ONCE_EXEMPT_FIELDS: Final[frozenset[str]] = frozenset({"guard_skips"})


@dataclass(frozen=True)
class BatchContext:
    # --- seed (set once, at construction) -----------------------------------
    pipeline: str
    feed_id: str
    delivery_id: str
    batch_id: str
    delivery_key: str
    content_hash: str
    object_uris: tuple[str, ...]  # I-22-checked
    received_at: datetime  # aware
    spec: PipelineSpecModel
    run: RunConfig
    transforms: Transforms  # record of fns, I-10
    attempt_id: str  # I-5
    sfn_retry_count: int
    sfn_redrive_count: int
    read_spec_version: str  # 005.1 A-11/§3.5: computed once at seed from spec.read
    check_version: str  # 005.1 A-11/§3.5: computed once at seed from (raw_contract, read)
    checks_version: str  # 006.1 P-3/§4.5: computed once at seed from spec.checks

    # --- accreting exemption [E-13] ------------------------------------------
    guard_skips: tuple[str, ...] = ()  # tuples are immutable -- no default_factory needed
    # accretes stage names whose table write was guard-skipped

    # --- land ----------------------------------------------------------------
    raw_df: DataFrame | None = None
    raw_count: int | None = None
    land_snapshot_id: int | None = None  # from append summary, or stamped-summary
    # resolution on guard-skip; None after snapshot expiry (I-19)
    started_emitted: bool = False

    # --- pre_check -------------------------------------------------------------
    valid_df: DataFrame | None = None
    pre_quarantined_count: int | None = None
    pre_quarantine_snapshot_id: int | None = None
    pre_check_drift: str | None = None  # 005.1 A-9 [DC-1]: set on the guard-skip
    # read-back subset-mismatch/fact-presence-demotion doors only (WARNING+EMF
    # companion); counts only, no row values [S-7] -- folded into the ledger
    # row's `error_message` by `core/run_facts.py::_stage_fields`, an exact
    # mirror of `post_check_drift` below

    # --- pull ------------------------------------------------------------------
    co_effects: Mapping[str, DataFrame] | None = None  # pinned reads, I-6
    co_effect_snapshot_ids: Mapping[str, int] | None = None  # -1 = existed, no
    # snapshot -- I-6

    # --- apply -------------------------------------------------------------------
    candidate_facts: Mapping[str, DataFrame] | None = None  # 006.1 §4.5: replaces
    # candidate_facts_df -- one candidate frame per declared fact type (P-1)

    # --- post_check ----------------------------------------------------------------
    admitted_facts: Mapping[str, DataFrame] | None = None  # 006.1 §4.5: replaces
    # admitted_facts_df -- one admitted frame per declared fact type (P-1)
    post_quarantined_count: int | None = None
    post_quarantine_snapshot_id: int | None = None
    post_check_drift: str | None = None  # I-12 [H-2]: set on the guard-skip
    # read-back subset-mismatch path only (WARNING+EMF companion); counts
    # only, no row values [S-7] -- folded into the ledger row's
    # `error_message` by `core/run_facts.py::_stage_fields`

    # --- commit --------------------------------------------------------------------
    facts_appended_by_table: Mapping[str, int] | None = None  # 007.1 §4.2:
    # per FACT TABLE, rows appended by this attempt; a 0 entry for every
    # guard-skipped table (F-4's per-table decide-then-do, never silent)
    commit_snapshot_ids: Mapping[str, int] | None = None  # 007.1 §4.2: per
    # fact table, this attempt's own append snapshot (I-19); absent key =
    # skip / zero-fact no-op
    delta_predecessor_batch_ids: tuple[str, ...] = ()  # 007.1 §4.2/F-5: the
    # resolved predecessor set (the feed's latest completed batch, plus the
    # superseded batch when Track E applies); empty = no predecessor ⇒
    # everything novel -- `core.delta.resolve_predecessors`'
    # `PredecessorResolution.predecessor_batch_ids`
    delta_read_snapshot_ids: Mapping[str, int] | None = None  # 007.1 §4.2:
    # per fact table, the ONE pinned snapshot every predecessor-partition
    # read of that table used
    delta_probe_refusal: str | None = None  # 007.1 §4.2/F-5: one of ADR-OQ2's
    # four reason codes verbatim, or None; non-null ⇒ this batch dropped
    # nothing (everything novel) -- `core.delta.resolve_predecessors`'
    # `PredecessorResolution.probe_refusal`
    divergent_duplicates_by_table: Mapping[str, int] | None = None  # 007.1
    # §12 (D-2(b)): per fact table, the count of `record_key`s carrying more
    # than one distinct `content_hash` within this batch's own stamped
    # candidates for that table -- absent key = guard-skipped table (§4.2's
    # convention). Critique gate wf_24a3125f-ecc F1 (bead conveyer-6pg.30):
    # the pure count is computed in `stages/commit.py`, recorded here; the
    # WARNING+EMF `DivergentDuplicates` emission lives in `effects/
    # ledger.py::record_run`, derived purely from the ledger row, mirroring
    # `delta_probe_refusal`'s own channel -- `stages/commit.py` no longer
    # calls `observability.*` directly (004 §13.3: stages carry zero
    # instrumentation).

    # --- fold ------------------------------------------------------------------------
    rows_merged_by_table: Mapping[str, int] | None = None  # 007.1 §4.2: per
    # STATE TABLE, rows merged by this attempt
    fold_snapshot_ids: Mapping[str, int] | None = None  # 007.1 §4.2: per
    # state table, this attempt's own MERGE snapshot; absent key = no-op

    # --- publish -----------------------------------------------------------------------
    published: bool = False
    completed_event: BatchCompletedV1 | None = None
