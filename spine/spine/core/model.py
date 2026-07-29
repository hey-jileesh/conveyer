"""pydantic contracts: seed event, `PipelineSpec`, lifecycle events, `LineageStamp`. LLD §6.

All boundary contracts are pydantic v2 models (parse, don't validate — with
narrow types, §6 preamble): a field this design later trusts (a name, an id,
a URI) carries a pattern, a bound, or a shape check here, at parse time —
"it parsed as `str`" is not trust. Internal-only values that never cross a
serialization boundary are `@dataclass(frozen=True)` (§7.0 rule 1) —
`LineageStamp` (§7.5 [C-5]) is the one such value in this module.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator

from spine.core.naming import BATCH_ID_RE, _check_pipeline_slug_grammar
from spine.core.naming import check_qualified_table as check_qualified_table

# --- §6.1 shared patterns ----------------------------------------------------
#
# `BATCH_ID_RE`, the pipeline-slug grammar, and `check_qualified_table` are
# now single-sourced in `core/naming.py` (stdlib-pure, critique F5, bead
# conveyer-nvh.43) and imported here rather than re-derived: `core/naming.py`
# importing FROM this module would break `entrypoints/router.py`'s stdlib+
# boto3-only zip-purity constraint (§7.1, I-8 — this module is
# pydantic-shaped), but the reverse direction (this module importing the
# stdlib-pure `core/naming.py`) carries no such constraint, so it is the
# correct place to remove the duplication. `check_qualified_table` stays
# re-exported from here (unchanged name/behavior) so `CoEffectDecl.table`'s
# validator, `PipelineSpecModel`'s four table-field validators, and
# `core/merge.py`'s existing `from spine.core.model import ...
# check_qualified_table` import all keep working unmodified -- the
# `import check_qualified_table as check_qualified_table` redundant-alias
# form (not a plain import) is required by mypy's own `no_implicit_reexport`
# (the `spine.core.*` strict override, pyproject.toml): a name merely
# imported, not locally defined, is otherwise not part of this module's
# public interface, and `core/merge.py`'s import of it would fail
# `attr-defined`. `BATCH_ID_RE`/`_check_pipeline_slug_grammar` need no such
# alias -- nothing outside this module imports them FROM here.

# UUIDv4, [H-4]
_DELIVERY_ID_RE = r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
_FEED_ID_RE = r"^[a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9-]*$"


# --- §6.1 Seed event — spine-side `DeliveryRegisteredV1` --------------------


class DeliveryRegisteredV1(BaseModel):  # parse of SFN execution input
    model_config = ConfigDict(extra="allow")  # tolerant reader; unknown fields ignored
    schema_version: Literal[1]
    feed_id: str = Field(pattern=_FEED_ID_RE)
    delivery_id: str = Field(
        pattern=_DELIVERY_ID_RE
    )  # participates in the I-22 URI/name composition, so it is narrow-typed like batch_id [H-4]
    batch_id: str = Field(pattern=BATCH_ID_RE)
    delivery_key: str
    content_hash: str  # OPAQUE lineage here (004 D-13); never parsed
    size_bytes: int
    object_uris: list[str] = Field(min_length=1, max_length=256)  # each <=1024 chars, I-22
    received_at: AwareDatetime
    pipeline: str  # slug grammar re-checked at parse

    @field_validator("object_uris")
    @classmethod
    def _check_object_uri_lengths(cls, value: list[str]) -> list[str]:
        too_long = [uri for uri in value if len(uri) > 1024]
        if too_long:
            raise ValueError(
                f"object_uris entries must each be <= 1024 chars: {len(too_long)} over"
            )
        return value

    @field_validator("pipeline")
    @classmethod
    def _check_pipeline(cls, value: str) -> str:
        return _check_pipeline_slug_grammar(value)


# --- §6.2 `PipelineSpec` — what the runner consumes -------------------------


class CoEffectDecl(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table: str  # bare "<db>.<table>"; identifier-grammar checked
    own_state: bool = False  # self-reference flag — 004 §7.3 obligation to 006;
    # Phase 1: WARNING when true and serialize is false

    @field_validator("table")
    @classmethod
    def _check_table(cls, value: str) -> str:
        return check_qualified_table(value)


class PipelineSpecModel(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pipeline: str  # must equal seed.pipeline, else binding defect
    transforms_module: str = Field(pattern=r"^pipelines\.[a-z0-9_]+(\.[a-z0-9_]+)*$")  # I-10
    co_effects: dict[str, CoEffectDecl] = {}  # name -> declaration; the ONLY reads pull performs;
    # ALSO an IaC input -- grants generated from it (I-21)
    raw_table: str
    quarantine_table: str
    fact_table: str
    state_table: str
    fold: Literal["default-lww", "custom"] = "default-lww"
    serialize: bool = False  # declared, not honored in Phase 1 (004 §16.2)
    domain_id_col: str = "domain_id"
    required_columns: list[str] = []  # PROVISIONAL pre_check contract (I-P2); 005 replaces
    read: dict[str, JsonValue] = {}  # PROVISIONAL reader hints only (I-P1); 005 owns
    # per-ATTEMPT budget (I-18). Two-sources-of-truth guard [H-5]: the DEPLOYED
    # timeouts are Terraform-time values; until 009 derives both from one
    # authored source, the entrypoint asserts this field equals
    # RunnerConfig.sla_minutes (the TF-passed value) -- binding-defect class,
    # so a spec edit that silently changes nothing fails loudly instead.
    sla_minutes: int = 480

    @field_validator("pipeline")
    @classmethod
    def _check_pipeline(cls, value: str) -> str:
        return _check_pipeline_slug_grammar(value)

    @field_validator("raw_table", "quarantine_table", "fact_table", "state_table")
    @classmethod
    def _check_tables(cls, value: str) -> str:
        return check_qualified_table(value)


# --- §6.6 Lifecycle events (payloads = batch truth, I-19) -------------------


class BatchStartedV1(BaseModel):  # DetailType "batch-started"; source conveyer.spine
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    pipeline: str
    feed_id: str
    batch_id: str
    delivery_id: str
    raw_count: int  # from read-back (durable), not attempt scaffolding
    land_snapshot_id: int | None  # stamped-summary resolution; None after expiry
    started_at: AwareDatetime  # ATTEMPT-truth -- declared exception [H-1]: `fx.now()` has no
    # durable source, so a rerun's emission carries its own clock; consumers dedup on
    # batch_id, the timestamp is informational (§6.6).


class BatchCompletedV1(BaseModel):  # PROVISIONAL -- 008 owns and freezes the payload
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    pipeline: str
    feed_id: str
    batch_id: str
    delivery_id: str
    raw_count: int
    pre_quarantined: int
    post_quarantined: int  # read-back by (batch_id[, stage])
    fact_count: int  # count of committed_facts_df -- batch truth [E-1].
    # Named fact_count, NOT facts_appended: the ledger's facts_appended is an
    # ATTEMPT delta (0 on guard-skip); one identifier must not carry two
    # sourcings [H-1].
    fact_snapshot_id: int | None
    state_snapshot_id: int | None  # None: expiry / fold no-op
    completed_at: AwareDatetime  # ATTEMPT-truth -- see BatchStartedV1.started_at.


# --- §7.5 [C-5] `LineageStamp` — `frames/` receives lineage as a value ------


@dataclass(frozen=True)
class LineageStamp:
    batch_id: str
    delivery_id: str
    feed_id: str
    received_at: datetime
    source_uri: str | None = None
