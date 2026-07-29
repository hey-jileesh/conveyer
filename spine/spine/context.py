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

`Mapping`-typed fields (`co_effects`, `co_effect_snapshot_ids`, `merge_summary`)
are documented as `types.MappingProxyType`-wrapped by convention: whichever
stage constructs them (`pull`, `fold`) wraps the built dict once before
`dataclasses.replace`, so callers never see a mutable mapping on the context.
This module does not enforce the wrapping (a frozen dataclass field's value
is not itself made read-only by the dataclass) -- it is a construction-site
contract, same as "set exactly once" itself.
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

    # --- pull ------------------------------------------------------------------
    co_effects: Mapping[str, DataFrame] | None = None  # pinned reads, I-6
    co_effect_snapshot_ids: Mapping[str, int] | None = None  # -1 = existed, no
    # snapshot -- I-6

    # --- apply -------------------------------------------------------------------
    candidate_facts_df: DataFrame | None = None

    # --- post_check ----------------------------------------------------------------
    admitted_facts_df: DataFrame | None = None
    post_quarantined_count: int | None = None
    post_quarantine_snapshot_id: int | None = None
    post_check_drift: str | None = None  # I-12 [H-2]: set on the guard-skip
    # read-back subset-mismatch path only (WARNING+EMF companion); counts
    # only, no row values [S-7] -- folded into the ledger row's
    # `error_message` by `core/run_facts.py::_stage_fields`

    # --- commit --------------------------------------------------------------------
    facts_appended: int | None = None  # this attempt's appended count (0 on
    # guard-skip; the ledger signature)
    fact_snapshot_id: int | None = None  # own commit, or stamped-summary
    # resolution on guard-skip (I-19)
    committed_facts_df: DataFrame | None = None  # read back by name (004 D-3);
    # candidate_facts_df/admitted_facts_df are dead past here

    # --- fold ------------------------------------------------------------------------
    state_read_snapshot_id: int | None = None  # the pinned snapshot the fold's
    # state slice was read at [E-14]
    state_snapshot_id: int | None = None  # None when the merge was skipped
    # (empty facts) or a no-op (I-19) [C-7, T-8]
    merge_summary: Mapping[str, str] | None = None  # own commit's Iceberg
    # summary, verbatim; None on skip/no-op

    # --- publish -----------------------------------------------------------------------
    published: bool = False
    completed_event: BatchCompletedV1 | None = None
