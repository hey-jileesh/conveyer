"""Unit tests for `spine.binding` — LLD §7.4.

`Transforms` (a frozen record of the pipeline's pure functions) is covered
first (M1 scope). `bind_transforms` (M3, bead conveyer-nvh.19) is covered by
a binding matrix below: missing export, non-callable export, bad arity,
out-of-namespace module (both a defensive-bypass-of-the-model-validator
case and a hostile `pipelines.__init__` probe that legitimately passes the
`pipelines.` grammar), and a broken import raising a clearly-wrapped
`ImportError`.

**Critique gate wf_24a3125f-ecc F2 (bead conveyer-6pg.31): `Transforms`
drops `fold`; `bind_transforms` no longer looks for a `fold` export at
all.** Every test below that used to construct a `Transforms(..., fold=...)`
or assert fold-defaulting/`spec.fold=="custom"`-WARNING behavior is either
dropped (the mechanism it covered no longer exists) or trimmed to drop the
now-nonexistent `fold=` kwarg -- `frames/folds.py` (the v1-era `default_lww_
fold` machinery this file used to import for the fold-defaulting assertion)
is deleted outright.

The binding matrix imports real throwaway modules under the REAL `pipelines`
package's namespace (`spine/pipelines/__init__.py`, ships in the wheel,
I-10/D-2) rather than faking a separate `pipelines` package -- `pipelines`
is a regular package with a normal (list) `__path__`, so a temp directory is
appended to that list for the duration of one test only (`temp_pipelines_module`
fixture below), letting `importlib.import_module("pipelines.<name>")` find a
throwaway module there exactly as it would find a real one shipped in the
wheel. `__path__` is restored (and any temp submodule entries evicted from
`sys.modules`) in every case, confining the manipulation strictly to each
test.
"""

from __future__ import annotations

import dataclasses
import logging
import sys
import textwrap
from collections.abc import Iterator
from pathlib import Path

import pipelines
import pytest
from spine.binding import Transforms, bind_transforms
from spine.core.model import (
    ChecksModel,
    CoEffectDecl,
    ColumnSpec,
    DialectModel,
    PipelineSpecModel,
    RawContractModel,
    ReadSpecModel,
)


def test_transforms_is_a_frozen_record_of_functions() -> None:
    # 006.1 §4.4 (bead conveyer-6pg.13, B3): `Transforms` drops `post_check`;
    # `apply` now returns a `Mapping[str, DataFrame]`. Critique gate
    # wf_24a3125f-ecc F2 (bead conveyer-6pg.31): `Transforms` drops `fold`
    # too -- `apply` is its ONLY field now.
    transforms = Transforms(apply=lambda valid_df, co_effects: {"t": valid_df})
    assert transforms.apply("valid", {}) == {"t": "valid"}
    assert not hasattr(transforms, "fold")
    with pytest.raises(dataclasses.FrozenInstanceError):
        transforms.apply = lambda a, b: a  # type: ignore[misc]


# --- `bind_transforms` matrix (M3, §7.4) -------------------------------------


def _make_spec(**overrides: object) -> PipelineSpecModel:
    base: dict[str, object] = dict(
        pipeline="pipelines/commissions",
        transforms_module="pipelines.commissions.transforms",
        raw_table="lake.commissions__raw",
        quarantine_table="lake.commissions__quarantine",
        # 006.1 P-1: singular fact_table/state_table replaced by a per-type
        # `fact_types` mapping -- this fixture just needs SOME valid spec,
        # not to exercise fact-type semantics (binding.py never reads
        # fact_types).
        fact_types={
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
        read={"dialect": {"format": "csv"}},
        raw_contract={"columns": [{"name": "id"}]},
    )
    base.update(overrides)
    return PipelineSpecModel(**base)  # type: ignore[arg-type]


@pytest.fixture
def temp_pipelines_module(tmp_path: Path) -> Iterator[Path]:
    """Appends `tmp_path` to the real `pipelines` package's `__path__` for
    the duration of one test, so `importlib.import_module("pipelines.foo")`
    can find a throwaway `tmp_path / "foo.py"` -- restores `__path__` and
    evicts any temp submodules from `sys.modules` afterward."""
    pipelines.__path__.append(str(tmp_path))
    try:
        yield tmp_path
    finally:
        pipelines.__path__.remove(str(tmp_path))
        for name in list(sys.modules):
            if name == "pipelines" or not name.startswith("pipelines."):
                continue
            mod_file = getattr(sys.modules[name], "__file__", "") or ""
            if mod_file.startswith(str(tmp_path)):
                del sys.modules[name]


def test_bind_transforms_missing_apply_export_raises_clearly(
    temp_pipelines_module: Path,
) -> None:
    # 006.1 §4.4 (bead conveyer-6pg.13, B3): `apply` is the ONLY required
    # export now -- a module exporting neither is a missing-`apply` defect,
    # not a missing-`post_check` one (that export is no longer looked for
    # at all here; a STALE `post_check` export is S4's concern instead,
    # `core/bind_checks.py`'s own module docstring).
    (temp_pipelines_module / "missing_export.py").write_text(
        textwrap.dedent(
            """
            def fold(state_slice, facts_df):
                return facts_df
            """
        )
    )
    spec = _make_spec(transforms_module="pipelines.missing_export")

    with pytest.raises(AttributeError, match="apply"):
        bind_transforms(spec)


def test_bind_transforms_non_callable_export_raises_clearly(
    temp_pipelines_module: Path,
) -> None:
    (temp_pipelines_module / "non_callable.py").write_text('apply = "not a function"\n')
    spec = _make_spec(transforms_module="pipelines.non_callable")

    with pytest.raises(TypeError, match="apply"):
        bind_transforms(spec)


def test_bind_transforms_bad_arity_raises_clearly(temp_pipelines_module: Path) -> None:
    (temp_pipelines_module / "bad_arity.py").write_text(
        textwrap.dedent(
            """
            def apply(valid_df):
                return {"t": valid_df}
            """
        )
    )
    spec = _make_spec(transforms_module="pipelines.bad_arity")

    with pytest.raises(TypeError, match="exactly 2 positional"):
        bind_transforms(spec)


def test_bind_transforms_rejects_out_of_namespace_module_defensively() -> None:
    """The `PipelineSpecModel` field pattern already forbids a non-`pipelines.`
    `transforms_module` -- `model_construct` bypasses that validator so this
    test exercises `bind_transforms`'s OWN defensive re-check directly."""
    spec = PipelineSpecModel.model_construct(
        pipeline="pipelines/commissions",
        transforms_module="evil.module",
        co_effects={},
        raw_table="lake.commissions__raw",
        quarantine_table="lake.commissions__quarantine",
        fact_types={},  # 006.1 P-1; `model_construct` bypasses validation, shape only
        checks=ChecksModel(),
        fold="default-lww",
        serialize=False,
        domain_id_col="domain_id",
        read=ReadSpecModel(dialect=DialectModel(format="csv")),
        raw_contract=RawContractModel(columns=[ColumnSpec(name="id")]),
        sla_minutes=480,
    )

    with pytest.raises(ValueError, match="pipelines"):
        bind_transforms(spec)


def test_bind_transforms_hostile_pipelines_init_probe_fails_closed() -> None:
    """`pipelines.__init__` legitimately matches the `^pipelines\\.` grammar
    (I-10) and IS importable (it resolves to the real `pipelines` package's
    own docstring-only init module) -- but that module exports no `apply`,
    so binding still fails closed, clearly."""
    spec = _make_spec(transforms_module="pipelines.__init__")

    with pytest.raises(AttributeError, match="apply"):
        bind_transforms(spec)


def test_bind_transforms_broken_import_raises_import_error_wrapped_clearly(
    temp_pipelines_module: Path,
) -> None:
    (temp_pipelines_module / "broken_import.py").write_text(
        "import this_module_does_not_exist_anywhere\n"
    )
    spec = _make_spec(transforms_module="pipelines.broken_import")

    with pytest.raises(ImportError, match="pipelines.broken_import"):
        bind_transforms(spec)


def test_bind_transforms_binds_the_modules_own_apply(
    temp_pipelines_module: Path,
) -> None:
    (temp_pipelines_module / "good_apply_only.py").write_text(
        textwrap.dedent(
            """
            def apply(valid_df, co_effects):
                return {"t": valid_df}
            """
        )
    )
    spec = _make_spec(transforms_module="pipelines.good_apply_only")

    transforms = bind_transforms(spec)

    assert transforms.apply("valid", {}) == {"t": "valid"}
    assert not hasattr(transforms, "fold")


def test_bind_transforms_ignores_a_module_that_also_exports_fold(
    temp_pipelines_module: Path,
) -> None:
    """Critique gate wf_24a3125f-ecc F2 (bead conveyer-6pg.31): `bind_
    transforms` no longer looks for a `fold` export at all -- binding
    succeeds identically whether or not the module happens to define one,
    the SAME asymmetric "not looked for, not refused here" contract S4
    already gives a stale `post_check` export (this module's own
    docstring). A module still exporting `fold` is a bind-defect (`stale-
    fold-export`), but that refusal is `core/bind_checks.py`'s concern,
    exercised in `tests/unit/test_bind_checks.py`/`test_bind_defect_
    matrix.py`, not this function's own."""
    (temp_pipelines_module / "good_with_stray_fold.py").write_text(
        textwrap.dedent(
            """
            def apply(valid_df, co_effects):
                return {"t": valid_df}

            def fold(state_slice, facts_df):
                return facts_df
            """
        )
    )
    spec = _make_spec(transforms_module="pipelines.good_with_stray_fold")

    transforms = bind_transforms(spec)

    assert transforms.apply("valid", {}) == {"t": "valid"}
    assert not hasattr(transforms, "fold")


# --- F4: one spec-shape WARNING, once per bind (bead conveyer-nvh.43) -------

_VALID_APPLY = textwrap.dedent(
    """
    def apply(valid_df, co_effects):
        return {"t": valid_df}
    """
)


def test_bind_transforms_own_state_without_serialize_warns_once(
    temp_pipelines_module: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (temp_pipelines_module / "own_state_no_serialize.py").write_text(_VALID_APPLY)
    spec = _make_spec(
        transforms_module="pipelines.own_state_no_serialize",
        co_effects={"self": CoEffectDecl(table="lake.commissions__state", own_state=True)},
        serialize=False,
    )

    with caplog.at_level(logging.WARNING, logger="spine.binding"):
        bind_transforms(spec)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("self" in r.getMessage() and "own_state" in r.getMessage() for r in warnings)


def test_bind_transforms_own_state_with_serialize_does_not_warn(
    temp_pipelines_module: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (temp_pipelines_module / "own_state_with_serialize.py").write_text(_VALID_APPLY)
    spec = _make_spec(
        transforms_module="pipelines.own_state_with_serialize",
        co_effects={"self": CoEffectDecl(table="lake.commissions__state", own_state=True)},
        serialize=True,
    )

    with caplog.at_level(logging.WARNING, logger="spine.binding"):
        bind_transforms(spec)

    assert caplog.records == []


def test_bind_transforms_valid_spec_does_not_warn(
    temp_pipelines_module: Path, caplog: pytest.LogCaptureFixture
) -> None:
    # Critique gate wf_24a3125f-ecc F2 (bead conveyer-6pg.31) deleted the
    # F12-guard `spec.fold == "custom"` WARNING this section used to also
    # cover (`test_bind_transforms_fold_custom_warns_once`/`..._default_lww_
    # fold_does_not_warn`, both removed) -- it warned about the dead
    # default-lww fold-defaulting wiring this bead also removed, and was
    # unreachable anyway (`spec.fold == "custom"` already raises at S3
    # parse). Only the F4 own_state/serialize WARNING survives; this test
    # is its own "clean spec, no warning" control case.
    (temp_pipelines_module / "clean_spec_flag.py").write_text(_VALID_APPLY)
    spec = _make_spec(transforms_module="pipelines.clean_spec_flag")

    with caplog.at_level(logging.WARNING, logger="spine.binding"):
        bind_transforms(spec)

    assert caplog.records == []
