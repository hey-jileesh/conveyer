"""Pattern accept/reject corpora — LLD §12.4, §6.1, §6.2.

Boundary contracts trust narrow types, not `str` (§6 preamble): this suite
pins the accept/reject corpus for every regex-checked field named there --
`batch_id` (UUIDv5 only), `delivery_id` (UUIDv4 only, [H-4]), `feed_id`,
`transforms_module` (namespace-constrained, I-10), the `pipeline` slug
grammar (§5), and `check_qualified_table`'s "<db>.<table>" identifier
grammar (§6.2, shared by `CoEffectDecl.table` and `PipelineSpecModel`'s four
table fields) -- via full model construction (`pydantic.ValidationError` is
the observable contract, not the private regex object).

Trailing-/embedded-newline regression (bead `conveyer-nvh.34`, M1.fix):
Python's `$` matches just before a trailing `"\n"` even without
`re.MULTILINE`, so `str.match()` against a `^...$`-anchored pattern
silently accepts e.g. `"my_table\n"`. `_check_pipeline_slug_grammar` and
`check_qualified_table` (`spine/core/model.py`) used `.match()` and were
vulnerable; both are now `.fullmatch()` (mirroring `core/naming.py`'s and
`core/merge.py`'s existing fix for the same bug class, bead `nvh.14`).
Every `Field(pattern=...)` pydantic field below (`batch_id`, `delivery_id`,
`feed_id`, `transforms_module`) was ALSO empirically probed for this same
bypass and found already immune: pydantic v2's compiled `Field(pattern=...)`
constraint runs on the Rust `regex` crate, whose `$`/`^` (absent an
explicit `(?m)` flag) anchor to the true start/end of the string --
unlike Python's `re` module, it has no trailing-newline special case, so
no additional hardening was needed there. The reject-corpus cases below
for those fields exist to pin that already-correct behavior against
regression (e.g. a future switch to `field_validator` + Python `re`).
"""

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError
from spine.core import model

_U5 = str(uuid.uuid5(uuid.NAMESPACE_DNS, "batch"))  # a valid UUIDv5
_U5_OTHER = str(uuid.uuid5(uuid.NAMESPACE_URL, "another"))  # a second, distinct UUIDv5
_U4 = str(uuid.uuid4())  # a valid UUIDv4

_BASE_SEED: dict[str, Any] = {
    "schema_version": 1,
    "feed_id": "carrier-x/commission-statements",
    "delivery_id": _U4,
    "batch_id": _U5,
    "delivery_key": "statement.csv",
    "content_hash": "sha256:" + "a" * 64,
    "size_bytes": 1,
    "object_uris": ["s3://bucket/statement.csv"],
    "received_at": datetime(2026, 7, 25, 9, 0, 0, tzinfo=UTC),
    "pipeline": "pipelines/commissions",
}


def _build_seed(**overrides: Any) -> model.DeliveryRegisteredV1:
    data = dict(_BASE_SEED)
    data.update(overrides)
    return model.DeliveryRegisteredV1(**data)


# --- batch_id: UUIDv5 accepted, UUIDv4 rejected -----------------------------


@pytest.mark.parametrize("batch_id", [_U5, _U5_OTHER])
def test_batch_id_accepts_uuidv5(batch_id: str) -> None:
    assert _build_seed(batch_id=batch_id).batch_id == batch_id


@pytest.mark.parametrize(
    "batch_id",
    [
        _U4,
        "not-a-uuid",
        "",
        _U5 + "\n",  # trailing newline (nvh.34 regression case)
        "\n" + _U5,  # leading newline
        _U5[:19] + "\n" + _U5[19:],  # embedded newline
    ],
)
def test_batch_id_rejects_non_uuidv5(batch_id: str) -> None:
    with pytest.raises(ValidationError):
        _build_seed(batch_id=batch_id)


# --- delivery_id: UUIDv4 accepted, UUIDv5 rejected [H-4] --------------------


def test_delivery_id_accepts_uuidv4() -> None:
    assert _build_seed(delivery_id=_U4).delivery_id == _U4


@pytest.mark.parametrize(
    "delivery_id",
    [
        _U5,
        "not-a-uuid",
        "",
        _U4 + "\n",  # trailing newline (nvh.34 regression case)
        "\n" + _U4,  # leading newline
    ],
)
def test_delivery_id_rejects_non_uuidv4(delivery_id: str) -> None:
    with pytest.raises(ValidationError):
        _build_seed(delivery_id=delivery_id)


# --- feed_id -----------------------------------------------------------------


@pytest.mark.parametrize(
    "feed_id",
    [
        "carrier-x/commission-statements",
        "a/b",
        "carrier--x/statements",  # feed_id grammar (unlike pipeline slug) allows "--"
    ],
)
def test_feed_id_accepts(feed_id: str) -> None:
    assert _build_seed(feed_id=feed_id).feed_id == feed_id


@pytest.mark.parametrize(
    "feed_id",
    [
        "carrierx",  # no slash
        "Carrier-X/commission-statements",  # uppercase
        "/commission-statements",  # empty source segment
        "carrier-x/",  # empty feed segment
        "",
        "carrier-x/commission-statements\n",  # trailing newline (nvh.34 regression case)
        "\ncarrier-x/commission-statements",  # leading newline
        "carrier-x\n/commission-statements",  # embedded newline
    ],
)
def test_feed_id_rejects(feed_id: str) -> None:
    with pytest.raises(ValidationError):
        _build_seed(feed_id=feed_id)


# --- pipeline: §5 slug grammar (no "--" inside a segment) -------------------


@pytest.mark.parametrize(
    "pipeline",
    ["pipelines/commissions", "pipelines/renewals", "a/b/c", "single-segment"],
)
def test_pipeline_accepts_slug_grammar(pipeline: str) -> None:
    assert _build_seed(pipeline=pipeline).pipeline == pipeline


@pytest.mark.parametrize(
    "pipeline",
    [
        "pipelines--evil",  # "--" inside a segment (would break slug injectivity, S-11)
        "Pipelines/commissions",  # uppercase
        "",  # empty
        "/commissions",  # empty leading segment
        "pipelines/",  # empty trailing segment
        "pipelines/commissions\n",  # trailing newline (nvh.34 regression case:
        # `_check_pipeline_slug_grammar` used `.match()`, which Python's `$`
        # would let a single trailing "\n" slip through)
        "\npipelines/commissions",  # leading newline
        "pipelines\n/commissions",  # embedded newline
    ],
)
def test_pipeline_rejects_non_slug_grammar(pipeline: str) -> None:
    with pytest.raises(ValidationError):
        _build_seed(pipeline=pipeline)


# --- transforms_module: namespace-constrained (I-10) ------------------------

_BASE_SPEC: dict[str, Any] = {
    "pipeline": "pipelines/commissions",
    "transforms_module": "pipelines.commissions.transforms",
    "raw_table": "lake.commissions__raw",
    "quarantine_table": "lake.commissions__quarantine",
    # 006.1 P-1: singular fact_table/state_table replaced by a per-type
    # `fact_types` mapping -- the four-table-fields corpus below now
    # exercises `_check_tables` via `raw_table` alone (still shared by
    # `quarantine_table`); `fact_table`/`state_table`'s own identifier
    # grammar is `FactTypeModel`'s own field_validator, unit-tested there.
    "fact_types": {
        "detail": {
            "fact_table": "lake.commissions__facts",
            "state_table": "lake.commissions__state",
            "schema": {
                "columns": [{"name": "domain_id", "type": "string"}],
                "domain_id_col": "domain_id",
                "record_key": ["domain_id"],
            },
        }
    },
    "read": {"dialect": {"format": "csv"}},
    "raw_contract": {"columns": [{"name": "id"}]},
}


def _build_spec(**overrides: Any) -> model.PipelineSpecModel:
    data = dict(_BASE_SPEC)
    data.update(overrides)
    return model.PipelineSpecModel(**data)


@pytest.mark.parametrize(
    "transforms_module",
    [
        "pipelines.commissions.transforms",
        "pipelines.commissions.sub.transforms",
        "pipelines.x",
    ],
)
def test_transforms_module_accepts_in_namespace(transforms_module: str) -> None:
    assert _build_spec(transforms_module=transforms_module).transforms_module == transforms_module


@pytest.mark.parametrize(
    "transforms_module",
    [
        "evil.module",  # out of the `pipelines.` namespace entirely
        "pipelines",  # namespace root alone, no submodule
        "pipelines.",  # trailing dot, empty final segment
        "pipelines.Commissions",  # uppercase segment
        "",
        "pipelines.commissions.transforms\n",  # trailing newline (nvh.34 regression case)
        "\npipelines.commissions.transforms",  # leading newline
        "pipelines.commissions\n.transforms",  # embedded newline
    ],
)
def test_transforms_module_rejects_out_of_namespace(transforms_module: str) -> None:
    with pytest.raises(ValidationError):
        _build_spec(transforms_module=transforms_module)


# --- check_qualified_table: "<db>.<table>" identifier grammar (§6.2, §6.7) --
# Shared by `CoEffectDecl.table`, `PipelineSpecModel`'s two remaining table
# fields (`raw_table`/`quarantine_table` -- `fact_table`/`state_table` moved
# to `FactTypeModel`'s own `_check_tables` field_validator, 006.1 P-1), and
# `FactTypeModel`'s two; exercised here through `CoEffectDecl` as the
# narrowest model that carries it.


@pytest.mark.parametrize(
    "table",
    ["lake.rate_cards", "lake.commissions__raw", "a.b.c"],
)
def test_qualified_table_accepts(table: str) -> None:
    assert model.CoEffectDecl(table=table).table == table


@pytest.mark.parametrize(
    "table",
    [
        "badtable",  # no dot at all
        "lake.bad-table",  # dash in the table component
        "lake.",  # empty table component
        ".table",  # empty db component
        "lake.bad table",  # space
        "",
        "lake.rate_cards\n",  # trailing newline (nvh.34 regression case:
        # `check_qualified_table` used `.match()` per dot-component, which
        # Python's `$` would let a single trailing "\n" slip through)
        "\nlake.rate_cards",  # leading newline
        "lake.rate\ncards",  # embedded newline within a component
        "lake\n.rate_cards",  # newline immediately before the dot
    ],
)
def test_qualified_table_rejects(table: str) -> None:
    with pytest.raises(ValidationError):
        model.CoEffectDecl(table=table)


@pytest.mark.parametrize(
    "table",
    ["lake.rate_cards\n", "lake.commissions\n__raw"],
)
def test_pipeline_spec_model_table_fields_reject_newline(table: str) -> None:
    """Same corpus, via `PipelineSpecModel`'s two remaining table fields
    (`raw_table`/`quarantine_table` share `_check_tables`'s single
    `field_validator` -- `raw_table` stands in for both)."""
    with pytest.raises(ValidationError):
        _build_spec(raw_table=table)


@pytest.mark.parametrize("table", ["lake.rate_cards\n", "lake.commissions\n__facts"])
def test_fact_type_model_table_fields_reject_newline(table: str) -> None:
    """006.1 P-1: `FactTypeModel.fact_table`/`.state_table` carry their own
    `_check_tables` field_validator (the same grammar, a separate model) --
    `fact_table` stands in for both."""
    with pytest.raises(ValidationError):
        model.FactTypeModel(**{**_BASE_SPEC["fact_types"]["detail"], "fact_table": table})
