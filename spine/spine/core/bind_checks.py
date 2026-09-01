"""`validate_bindings` — the bind-phase half of the bind-time validator
inventory. LLD 006.1 §5 (P-4's decision), §5.2 (C3-C6, C8's bind half),
§4.4/§5.1 (S4); 007.1 F-10/[DC-1] (§16's three proposed 006.1 erratum rows,
landed here -- see the "F-10 / [DC-1]" section below).

**Phase split (§5's own phase column), normative.** Every *parse*-phase
check in §5.1-§5.4 (S1-S3, C1/C2/C7, C8's declared-columns half, F1-F5,
K1-K4/K6-K9) is a pydantic model validator in `core/model.py` — a pure
function of the spec alone, no catalog needed. This module carries exactly
the *bind*-phase checks that need `CatalogFacts` or the bound-transforms
export summary: **S4** (stale `post_check` export) and its sibling
`stale-fold-export` check (critique gate wf_24a3125f-ecc F2, bead
conveyer-6pg.31 -- 007.1 B10 dropped `Transforms.fold`, the same hard-cut
shape S4 already gives `post_check`), **C3** (co-effect table
exists), **C4** (co-effect table-class), **C5** (co-effect declared columns
⊆ catalog schema), **C6** (`own_state` refusal), and **C8's bind half**
(membership `ref_columns` ⊆ the co-effect's CATALOG schema, only when the
co-effect declares no `columns:` of its own — the declared-columns half is
already checked at parse, in `MembershipCheckModel`). **K5 (the engine
compile gate) is deliberately absent here** — it needs a live Spark session
to compile against (`df.select(F.expr(text))`, P-2 gate 2), which this
function's `plain, total over plain values` contract excludes; K5 is the
entrypoint's own effectful step (P-4: "acquires `CatalogFacts`... then
calls pure `validate_bindings`... " — gate 2 is a THIRD, separate bind-time
call the entrypoint makes, not folded into this function), landing with the
bind-step entrypoint wiring bead.

**`TableFacts`/`CatalogFacts` — the plain-value shape `fx.describe_table`
(a later, additive `RunnerFx` field, 004.1-erratum class, §16.4) will
produce.** This module owns the VALUE type and the pure validators that
consume it now; the effect that PRODUCES real instances lands with the
entrypoint bead. `CatalogFacts` is keyed by qualified table identifier
(`<db>.<table>`, matching `CoEffectDecl.table`/`FactTypeModel.fact_table`/
`.state_table`) — `None` at a key means the table does not exist.

**Raises nothing (`core/**`'s `ban_try_raise`).** `validate_bindings`
RETURNS a (possibly empty) tuple of `BindDefect` values; turning a non-
empty tuple into the pinned `bind-defect/<code>: <detail>` plain `ValueError`
(P-4) is the entrypoint's job, not this function's — this module never
raises.

**F-10 / [DC-1] — the B2<->B7 seam (007.1 §16's three proposed 006.1
erratum rows; B2, `conveyer-6pg.12`, landed their PURE halves; B7,
`conveyer-6pg.18`, lands the effect-side acquisition — 007.1 §6.5/§4.3,
`bootstrap/create_record_tables.py` + `entrypoints/glue_main.py`).**
`validate_bindings` takes two more plain-value inputs, both content-pinned
or derived facts the entrypoint acquires BEFORE calling this function —
never a second catalog read inside it:

* **`table_class_inventory` (F-10, 007.1 §6.5's `table-classes.json`).**
  The content-pinned table→class inventory the deploy step emits beside the
  deployed spec (the I-23 idiom) is the bind-time AUTHORITY for every
  class-dependent check (C4 first): a co-effect table absent from the
  inventory, or whose live catalog `conveyer.table-class` property
  disagrees with the inventory's entry, refuses at bind — the catalog
  property itself demotes to provenance/ergonomics (F-10's own words),
  watched by the (out-of-scope-here) class-property drift audit. B7 lands
  the real load: `bootstrap/create_record_tables.py::main` emits `table-
  classes.json` beside the deployed spec (`core/naming.py::
  table_class_inventory_uri`'s own derivation); `entrypoints/glue_main.py::
  _load_table_class_inventory` fetches it via the SAME `fetch_spec` DI seam
  the spec fetch itself already uses and `json.loads`s it.
* **`committed_tables` (007.1 §4.3's [DC-1] discharge — the derived marker
  probe read, `committed_tables(batch_id)`).** The distinct fact-table
  names this batch has ALREADY durably committed facts for (guard-twin rows
  at `stage='commit'`, sentinel excluded) — sourced from the marker table
  B7 creates. **This is bind's ONE ruled data-read (007.1 §4.3, ADR-OQ5's
  fail-fast closure) — the single exception to §5.6's "bind never reads
  data" stance, named here explicitly rather than a silent erosion.**
  `validate_bindings` itself stays plain-value-pure (the read already
  happened, upstream, by the time this plain value arrives); refuses
  `bind-defect/fact-type-removed-in-flight` when `committed_tables` is not
  a subset of the deployed spec's fact-table set — a redeploy that drops a
  fact type while a batch is mid-flight for it. B7 lands the real probe:
  `entrypoints/glue_main.py::_committed_tables` reads the DISTINCT
  guard-twin `table_name` at `(batch_id, stage='commit')`, sentinel
  (`core/naming.py::COMMIT_COMPLETION_SENTINEL`) excluded, tolerating a
  not-yet-provisioned marker table by returning `()` (an empty set is a
  subset of every fact-table set by construction, so this check correctly
  stays silent in that case too)."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spine.core.model import PipelineSpecModel


@dataclass(frozen=True)
class TableFacts:
    """One `fx.describe_table` result for an existing table."""

    table_class: str | None  # the `conveyer.table-class` property; None if unset
    columns: Mapping[str, str]  # column name -> type, as reported by the catalog


# Qualified table identifier ("<db>.<table>") -> its facts, or `None` if the
# table does not exist. One entry per table the spec references: every
# declared co-effect's `.table`, plus (existence-only, per P-4) every
# declared type's `fact_table`/`state_table`.
CatalogFacts = Mapping[str, "TableFacts | None"]

# Qualified table identifier -> its `conveyer.table-class` (F-10's
# content-pinned `table-classes.json`, the bind-time AUTHORITY for
# class-dependent checks -- see the module docstring's "F-10 / [DC-1]"
# section). A table absent from this mapping is NOT the same as an absent
# table in `CatalogFacts` (C3's own, separate existence check) -- it means
# "the provisioning-layer inventory has no entry for this table", the S-15
# signal F-10 names.
TableClassInventory = Mapping[str, str]

# The distinct fact-table names this batch has already durably committed
# facts for (007.1 §4.3's `committed_tables(batch_id)`, [DC-1]'s discharge)
# -- a plain-value fact the entrypoint's marker-table probe hands in.
CommittedTables = Collection[str]


@dataclass(frozen=True)
class TransformsMeta:
    """The plain summary `binding.py`'s `bind_transforms` hands this
    module for S4 -- whether the module.py still exports a `post_check`
    (006.1 §4.4's hard-cut tripwire, A-12 idiom). Apply/arity enforcement
    stays `bind_transforms`'s own, direct concern.

    `has_fold_export` (critique gate wf_24a3125f-ecc F2, bead
    conveyer-6pg.31): the SAME class of tripwire, for `fold` -- 007.1 B10
    dropped `Transforms.fold`/`bind_transforms`'s fold-defaulting wiring
    outright (`stages/fold.py`'s own mechanical §8.2 reduce never called
    it), so a module still exporting `fold` is now a stale export too,
    exercised by the `stale-fold-export` check beside S4 below. Defaulted
    to `False` (unlike `has_post_check_export`, which predates this field
    and stays required) so every pre-existing direct `TransformsMeta(...)`
    call site across the repo -- most of them exercising unrelated C/K-grain
    checks -- keeps constructing without this field in mind."""

    has_post_check_export: bool
    has_fold_export: bool = False


@dataclass(frozen=True)
class BindDefect:
    code: str  # e.g. "co-effect-missing-table" -- the §5 table's own code column
    detail: (
        str  # value-free machine detail (A-10: aliases/ids/columns/counts, never cell values/URIs)
    )


def _defect(code: str, detail: str) -> BindDefect:
    return BindDefect(code=code, detail=detail)


def validate_bindings(
    spec: PipelineSpecModel,
    catalog_facts: CatalogFacts,
    transforms_meta: TransformsMeta,
    table_class_inventory: TableClassInventory,
    committed_tables: CommittedTables,
) -> tuple[BindDefect, ...]:
    """Pure, total over plain values. Every declared co-effect is checked
    regardless of whether any check references it (D-2's list applies to
    the declaration, not to usage); `own_state` is checked independently of
    table existence (a declaration defect the catalog cannot cure), while
    class/column checks are skipped for a co-effect whose table does not
    exist (nothing further to check without one -- C3 alone reports it).

    `table_class_inventory`/`committed_tables` are the F-10/[DC-1] plain-
    value inputs (module docstring's own section) -- both already-acquired
    facts, never a second catalog/marker read inside this function."""
    defects: list[BindDefect] = []

    if transforms_meta.has_post_check_export:
        defects.append(
            _defect(
                "stale-post-check-export", "transforms module still exports post_check (006.1 A-12)"
            )
        )

    # Critique gate wf_24a3125f-ecc F2 (bead conveyer-6pg.31): S4's own
    # sibling tripwire, identical shape -- `Transforms.fold`/`bind_
    # transforms`'s fold-defaulting wiring is gone (`stages/fold.py`'s own
    # mechanical §8.2 reduce never called it), so a module still exporting
    # `fold` is a stale export too, not merely an ignored one.
    if transforms_meta.has_fold_export:
        defects.append(
            _defect("stale-fold-export", "transforms module still exports fold (007.1 B10)")
        )

    # [DC-1] (007.1 §4.3's discharge, proposed 006.1 erratum row) -- bind's
    # ONE ruled data-read (a marker-table probe result, already resolved by
    # the caller into this plain `committed_tables` value): refuse when a
    # batch has already durably committed facts for a type the currently
    # deployed spec no longer declares (a fact type removed mid-flight).
    declared_fact_tables = {fact_type.fact_table for fact_type in spec.fact_types.values()}
    orphaned_commits = sorted(set(committed_tables) - declared_fact_tables)
    if orphaned_commits:
        defects.append(
            _defect(
                "fact-type-removed-in-flight",
                f"committed fact table(s) no longer declared in the deployed spec: "
                f"{orphaned_commits!r}",
            )
        )

    for alias, decl in spec.co_effects.items():
        if decl.own_state:
            defects.append(
                _defect("own-state-refused", f"co-effect {alias!r} declares own_state: true")
            )
        facts = catalog_facts.get(decl.table)
        if facts is None:
            defects.append(
                _defect(
                    "co-effect-missing-table",
                    f"co-effect {alias!r} table {decl.table!r} does not exist",
                )
            )
            continue
        # C4, F-10-anchored: the inventory is the AUTHORITY, not the live
        # catalog property (module docstring's "F-10 / [DC-1]" section) --
        # three distinct, actionable causes: absent from the inventory (the
        # S-15 provisioning-not-run signal), the inventory itself names a
        # non-state class, or the inventory and the live catalog property
        # disagree (drift -- the class-property drift audit's own alarm
        # class, [DS-2], detected here at bind too).
        inventory_class = table_class_inventory.get(decl.table)
        if inventory_class is None:
            defects.append(
                _defect(
                    "co-effect-class-not-in-inventory",
                    f"co-effect {alias!r} table {decl.table!r} is absent from the "
                    "table-class inventory (F-10)",
                )
            )
        elif inventory_class != "state":
            defects.append(
                _defect(
                    "co-effect-not-current-state",
                    f"co-effect {alias!r} table {decl.table!r} table-class="
                    f"{inventory_class!r} (inventory)",
                )
            )
        elif facts.table_class != inventory_class:
            defects.append(
                _defect(
                    "co-effect-table-class-drift",
                    f"co-effect {alias!r} table {decl.table!r} inventory table-class="
                    f"{inventory_class!r} but catalog property={facts.table_class!r}",
                )
            )
        if decl.columns is not None:
            missing = sorted(set(decl.columns) - set(facts.columns))
            if missing:
                defects.append(
                    _defect(
                        "co-effect-unknown-columns",
                        f"co-effect {alias!r} declares unknown columns: {missing!r}",
                    )
                )

    for check in spec.checks.checks:
        if check.kind != "membership":
            continue
        member_decl = spec.co_effects.get(check.co_effect)
        if member_decl is None or member_decl.columns is not None:
            # C7 (parse) already refuses an unknown alias; a co-effect that
            # DOES declare `columns:` was already checked against them at
            # parse (MembershipCheckModel's own C8 half).
            continue
        facts = catalog_facts.get(member_decl.table)
        if facts is None:
            continue  # C3 already reported the missing table
        missing = sorted(set(check.ref_columns) - set(facts.columns))
        if missing:
            defects.append(
                _defect(
                    "membership-columns-outside-declaration",
                    f"check {check.id!r} references unknown co-effect columns: {missing!r}",
                )
            )

    return tuple(defects)
