"""Unit tests for `spine.core.bind_checks.validate_bindings` — LLD 006.1 §5
(P-4), §5.2 (C3-C6, C8's bind half), §5.1/§4.4 (S4); 007.1 F-10/[DC-1]
(the `table_class_inventory`/`committed_tables` params, this bead's own
B2<->B7 seam).

Every check this module owns needs both `spec.co_effects`/`spec.checks` (a
real `PipelineSpecModel`) and synthetic `CatalogFacts` -- no Spark, no
catalog, no moto needed; `bind_checks.py`'s own contract is plain-value
pure.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from spine.core import bind_checks
from spine.core.model import PipelineSpecModel

if TYPE_CHECKING:
    from spine.core.bind_checks import (
        BindDefect,
        CatalogFacts,
        CommittedTables,
        TableClassInventory,
    )

_FACT_TYPE: dict = {
    "fact_table": "lake.commissions__facts",
    "state_table": "lake.commissions__state",
    "schema": {
        "columns": [
            {"name": "domain_id", "type": "string"},
            {"name": "amount", "type": "decimal(10,2)"},
        ],
        "domain_id_col": "domain_id",
        "record_key": ["domain_id"],
    },
}


def _spec(**overrides: object) -> PipelineSpecModel:
    base: dict = dict(
        pipeline="pipelines/commissions",
        transforms_module="pipelines.commissions.transforms",
        raw_table="lake.commissions__raw",
        quarantine_table="lake.commissions__quarantine",
        fact_types={"detail": _FACT_TYPE},
        read={"dialect": {"format": "csv"}},
        raw_contract={"columns": [{"name": "id"}]},
    )
    base.update(overrides)
    return PipelineSpecModel(**base)


def _transforms_meta(
    has_post_check_export: bool = False, has_fold_export: bool = False
) -> bind_checks.TransformsMeta:
    return bind_checks.TransformsMeta(
        has_post_check_export=has_post_check_export, has_fold_export=has_fold_export
    )


def _validate(
    spec: PipelineSpecModel,
    facts: CatalogFacts,
    transforms_meta: bind_checks.TransformsMeta | None = None,
    table_class_inventory: TableClassInventory | None = None,
    committed_tables: CommittedTables = (),
) -> tuple[BindDefect, ...]:
    """Test-local convenience wrapper: fills the F-10/[DC-1] params with
    their INTERIM-stub-equivalent defaults (`table_class_inventory` derived
    from `facts` itself, `committed_tables` empty) so every EXISTING test
    below -- none of which is about F-10/[DC-1] -- keeps asserting exactly
    what it asserted before those two params existed."""
    if transforms_meta is None:
        transforms_meta = _transforms_meta()
    if table_class_inventory is None:
        # The SAME degenerate derivation `glue_main.py`'s interim B2<->B7
        # stub uses (module docstring: "inventory == catalog, always
        # agrees") -- a table with no catalog `table_class` gets NO
        # inventory entry either (never a `None`-valued entry), matching
        # the real stub's own `is not None` filter exactly.
        table_class_inventory = {
            table: tf.table_class
            for table, tf in facts.items()
            if tf is not None and tf.table_class is not None
        }
    return bind_checks.validate_bindings(
        spec, facts, transforms_meta, table_class_inventory, committed_tables
    )


def test_validate_bindings_clean_spec_no_defects() -> None:
    spec = _spec(co_effects={"rate_cards": {"table": "lake.rate_cards"}})
    facts = {
        "lake.rate_cards": bind_checks.TableFacts(table_class="state", columns={"code": "string"})
    }
    assert _validate(spec, facts) == ()


def test_validate_bindings_no_co_effects_no_defects() -> None:
    spec = _spec()
    assert _validate(spec, {}) == ()


# --- S4 ----------------------------------------------------------------------


def test_validate_bindings_flags_stale_post_check_export() -> None:
    spec = _spec()
    defects = _validate(spec, {}, transforms_meta=_transforms_meta(has_post_check_export=True))
    assert len(defects) == 1
    assert defects[0].code == "stale-post-check-export"


def test_validate_bindings_flags_stale_fold_export() -> None:
    """Critique gate wf_24a3125f-ecc F2 (bead conveyer-6pg.31): S4's own
    sibling tripwire -- 007.1 B10 dropped `Transforms.fold`/`bind_
    transforms`'s fold-defaulting wiring outright, so a module still
    exporting `fold` is a stale export too."""
    spec = _spec()
    defects = _validate(spec, {}, transforms_meta=_transforms_meta(has_fold_export=True))
    assert len(defects) == 1
    assert defects[0].code == "stale-fold-export"


def test_validate_bindings_flags_both_stale_exports_together() -> None:
    spec = _spec()
    defects = _validate(
        spec, {}, transforms_meta=_transforms_meta(has_post_check_export=True, has_fold_export=True)
    )
    assert {d.code for d in defects} == {"stale-post-check-export", "stale-fold-export"}


# --- C3/C4/C5/C6 ---------------------------------------------------------


def test_validate_bindings_flags_missing_co_effect_table() -> None:
    spec = _spec(co_effects={"rate_cards": {"table": "lake.rate_cards"}})
    defects = _validate(spec, {"lake.rate_cards": None})
    assert [d.code for d in defects] == ["co-effect-missing-table"]


def test_validate_bindings_flags_wrong_table_class() -> None:
    spec = _spec(co_effects={"rate_cards": {"table": "lake.rate_cards"}})
    facts = {"lake.rate_cards": bind_checks.TableFacts(table_class="raw", columns={})}
    defects = _validate(spec, facts)
    assert [d.code for d in defects] == ["co-effect-not-current-state"]


def test_validate_bindings_flags_class_not_in_inventory_when_catalog_property_unset() -> None:
    # F-10: the inventory is the AUTHORITY, not the live catalog property --
    # an unset `conveyer.table-class` property means the (interim-stub)
    # inventory has NO entry for this table either, which is now its own,
    # distinct, more actionable code (never silently folded into
    # "co-effect-not-current-state", which now means "the inventory itself
    # names a non-state class").
    spec = _spec(co_effects={"rate_cards": {"table": "lake.rate_cards"}})
    facts = {"lake.rate_cards": bind_checks.TableFacts(table_class=None, columns={})}
    defects = _validate(spec, facts)
    assert [d.code for d in defects] == ["co-effect-class-not-in-inventory"]


def test_validate_bindings_flags_unknown_declared_columns() -> None:
    spec = _spec(
        co_effects={"rate_cards": {"table": "lake.rate_cards", "columns": ["code", "zzz"]}}
    )
    facts = {
        "lake.rate_cards": bind_checks.TableFacts(table_class="state", columns={"code": "string"})
    }
    defects = _validate(spec, facts)
    assert [d.code for d in defects] == ["co-effect-unknown-columns"]
    assert "zzz" in defects[0].detail


def test_validate_bindings_flags_own_state() -> None:
    spec = _spec(co_effects={"x": {"table": "lake.x", "own_state": True}})
    facts = {"lake.x": bind_checks.TableFacts(table_class="state", columns={})}
    defects = _validate(spec, facts)
    assert [d.code for d in defects] == ["own-state-refused"]


def test_validate_bindings_own_state_reported_even_without_a_table() -> None:
    # own_state is a declaration defect (the catalog cannot cure it) --
    # checked independently of table existence; both fire together.
    spec = _spec(co_effects={"x": {"table": "lake.x", "own_state": True}})
    defects = _validate(spec, {"lake.x": None})
    codes = {d.code for d in defects}
    assert codes == {"own-state-refused", "co-effect-missing-table"}


def test_validate_bindings_reports_every_declared_co_effect_even_if_unreferenced() -> None:
    # D-2's list applies to the DECLARATION, not to usage by a check.
    spec = _spec(co_effects={"unused": {"table": "lake.unused"}})
    defects = _validate(spec, {"lake.unused": None})
    assert [d.code for d in defects] == ["co-effect-missing-table"]


# --- C4, F-10-anchored: the inventory is the authority ------------------


def test_validate_bindings_flags_table_class_drift_against_inventory() -> None:
    # The inventory says "state" (authoritative); the live catalog property
    # disagrees -- a distinct, security-relevant finding from either
    # "wrong class" or "not in inventory" (007.1 F-10, [DS-2]'s drift-audit
    # class, detected here too).
    spec = _spec(co_effects={"rate_cards": {"table": "lake.rate_cards"}})
    facts = {"lake.rate_cards": bind_checks.TableFacts(table_class="raw", columns={})}
    defects = _validate(spec, facts, table_class_inventory={"lake.rate_cards": "state"})
    assert [d.code for d in defects] == ["co-effect-table-class-drift"]
    assert "lake.rate_cards" in defects[0].detail


def test_validate_bindings_no_drift_when_catalog_and_inventory_agree() -> None:
    spec = _spec(co_effects={"rate_cards": {"table": "lake.rate_cards"}})
    facts = {"lake.rate_cards": bind_checks.TableFacts(table_class="state", columns={})}
    defects = _validate(spec, facts, table_class_inventory={"lake.rate_cards": "state"})
    assert defects == ()


# --- [DC-1]: `fact-type-removed-in-flight` -------------------------------


def test_validate_bindings_flags_fact_type_removed_in_flight() -> None:
    spec = _spec()  # declares only "lake.commissions__facts" (see _FACT_TYPE)
    defects = _validate(
        spec, {}, committed_tables=("lake.commissions__facts", "lake.removed__facts")
    )
    assert [d.code for d in defects] == ["fact-type-removed-in-flight"]
    assert "lake.removed__facts" in defects[0].detail
    assert "lake.commissions__facts" not in defects[0].detail


def test_validate_bindings_committed_subset_of_declared_no_defect() -> None:
    spec = _spec()
    defects = _validate(spec, {}, committed_tables=("lake.commissions__facts",))
    assert defects == ()


def test_validate_bindings_empty_committed_tables_never_fires() -> None:
    # The interim B2<->B7 stub's own safety property: an empty set is a
    # subset of every deployed fact-table set by construction.
    spec = _spec()
    defects = _validate(spec, {}, committed_tables=())
    assert defects == ()


# --- C8's bind half (membership ref_columns against the CATALOG schema,
# only when the co-effect declares no columns: of its own) -----------------


def test_validate_bindings_flags_membership_ref_columns_outside_catalog_schema() -> None:
    spec = _spec(
        co_effects={"rate_cards": {"table": "lake.rate_cards"}},
        checks={
            "checks": [
                {
                    "kind": "membership",
                    "id": "chk-1",
                    "fact_type": "detail",
                    "columns": ["domain_id"],
                    "co_effect": "rate_cards",
                    "ref_columns": ["zzz"],
                    "reason": "business/unknown-code",
                }
            ]
        },
    )
    facts = {
        "lake.rate_cards": bind_checks.TableFacts(table_class="state", columns={"code": "string"})
    }
    defects = _validate(spec, facts)
    assert [d.code for d in defects] == ["membership-columns-outside-declaration"]
    assert "chk-1" in defects[0].detail


def test_validate_bindings_skips_membership_check_when_co_effect_declares_columns() -> None:
    # The declared-columns half is already checked at PARSE time
    # (`MembershipCheckModel`'s own C8 half via `PipelineSpecModel`'s
    # cross-validator) -- this module's job is only the undeclared case.
    spec = _spec(
        co_effects={"rate_cards": {"table": "lake.rate_cards", "columns": ["code"]}},
        checks={
            "checks": [
                {
                    "kind": "membership",
                    "id": "chk-1",
                    "fact_type": "detail",
                    "columns": ["domain_id"],
                    "co_effect": "rate_cards",
                    "ref_columns": ["code"],
                    "reason": "business/unknown-code",
                }
            ]
        },
    )
    # The catalog DOES have "code" (matching the co-effect's own declared
    # columns, so C5 stays clean) but NOT "zzz" -- irrelevant here since
    # `ref_columns` only ever names "code", proving C8's bind half is the
    # thing that's skipped, not accidentally passing via a lenient catalog.
    facts = {
        "lake.rate_cards": bind_checks.TableFacts(table_class="state", columns={"code": "string"})
    }
    assert _validate(spec, facts) == ()
