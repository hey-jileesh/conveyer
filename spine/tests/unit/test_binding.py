"""Unit tests for `spine.binding` — LLD §7.4.

`Transforms` (a frozen record of the pipeline's pure functions) is covered
first (M1 scope). `bind_transforms` (M3, bead conveyer-nvh.19) is covered by
a binding matrix below: missing export, non-callable export, bad arity,
out-of-namespace module (both a defensive-bypass-of-the-model-validator
case and a hostile `pipelines.__init__` probe that legitimately passes the
`pipelines.` grammar), absent fold defaulting to `frames.default_lww_fold`,
and a broken import raising a clearly-wrapped `ImportError`.

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
from spine.core.model import CoEffectDecl, PipelineSpecModel


def test_transforms_is_a_frozen_record_of_functions() -> None:
    transforms = Transforms(
        apply=lambda valid_df, co_effects: valid_df,
        post_check=lambda candidate_df, co_effects: candidate_df,
        fold=lambda state_slice, facts_df: facts_df,
    )
    assert transforms.apply("valid", {}) == "valid"
    assert transforms.post_check("candidate", {}) == "candidate"
    assert transforms.fold("slice", "facts") == "facts"
    with pytest.raises(dataclasses.FrozenInstanceError):
        transforms.apply = lambda a, b: a  # type: ignore[misc]


# --- `bind_transforms` matrix (M3, §7.4) -------------------------------------


def _make_spec(**overrides: object) -> PipelineSpecModel:
    base: dict[str, object] = dict(
        pipeline="pipelines/commissions",
        transforms_module="pipelines.commissions.transforms",
        raw_table="lake.commissions__raw",
        quarantine_table="lake.commissions__quarantine",
        fact_table="lake.commissions__facts",
        state_table="lake.commissions__state",
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


def test_bind_transforms_missing_export_raises_clearly(
    temp_pipelines_module: Path,
) -> None:
    (temp_pipelines_module / "missing_export.py").write_text(
        textwrap.dedent(
            """
            def apply(valid_df, co_effects):
                return valid_df
            """
        )
    )
    spec = _make_spec(transforms_module="pipelines.missing_export")

    with pytest.raises(AttributeError, match="post_check"):
        bind_transforms(spec)


def test_bind_transforms_non_callable_export_raises_clearly(
    temp_pipelines_module: Path,
) -> None:
    (temp_pipelines_module / "non_callable.py").write_text(
        textwrap.dedent(
            """
            apply = "not a function"

            def post_check(candidate_df, co_effects):
                return candidate_df
            """
        )
    )
    spec = _make_spec(transforms_module="pipelines.non_callable")

    with pytest.raises(TypeError, match="apply"):
        bind_transforms(spec)


def test_bind_transforms_bad_arity_raises_clearly(temp_pipelines_module: Path) -> None:
    (temp_pipelines_module / "bad_arity.py").write_text(
        textwrap.dedent(
            """
            def apply(valid_df):
                return valid_df

            def post_check(candidate_df, co_effects):
                return candidate_df
            """
        )
    )
    spec = _make_spec(transforms_module="pipelines.bad_arity")

    with pytest.raises(TypeError, match="exactly 2 positional"):
        bind_transforms(spec)


def test_bind_transforms_custom_fold_bad_arity_raises_clearly(
    temp_pipelines_module: Path,
) -> None:
    (temp_pipelines_module / "bad_fold_arity.py").write_text(
        textwrap.dedent(
            """
            def apply(valid_df, co_effects):
                return valid_df

            def post_check(candidate_df, co_effects):
                return candidate_df

            def fold(state_slice, facts_df, extra):
                return facts_df
            """
        )
    )
    spec = _make_spec(transforms_module="pipelines.bad_fold_arity")

    with pytest.raises(TypeError, match=r"fold.*exactly 2 positional"):
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
        fact_table="lake.commissions__facts",
        state_table="lake.commissions__state",
        fold="default-lww",
        serialize=False,
        domain_id_col="domain_id",
        required_columns=[],
        read={},
        sla_minutes=480,
    )

    with pytest.raises(ValueError, match="pipelines"):
        bind_transforms(spec)


def test_bind_transforms_hostile_pipelines_init_probe_fails_closed() -> None:
    """`pipelines.__init__` legitimately matches the `^pipelines\\.` grammar
    (I-10) and IS importable (it resolves to the real `pipelines` package's
    own docstring-only init module) -- but that module exports neither
    `apply` nor `post_check`, so binding still fails closed, clearly."""
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


def test_bind_transforms_binds_the_modules_own_callables(
    temp_pipelines_module: Path,
) -> None:
    (temp_pipelines_module / "good_with_fold.py").write_text(
        textwrap.dedent(
            """
            def apply(valid_df, co_effects):
                return valid_df

            def post_check(candidate_df, co_effects):
                return candidate_df

            def fold(state_slice, facts_df):
                return facts_df
            """
        )
    )
    spec = _make_spec(transforms_module="pipelines.good_with_fold")

    transforms = bind_transforms(spec)

    assert transforms.apply("valid", {}) == "valid"
    assert transforms.post_check("candidate", {}) == "candidate"
    assert transforms.fold("slice", "facts") == "facts"


def test_bind_transforms_absent_fold_binds_default_lww_partial(
    temp_pipelines_module: Path,
) -> None:
    """No `fold` export -> `frames.default_lww_fold` partial-applied with
    `spec.domain_id_col` -- verified by shape (the bound function, its
    `functools.partial` target, and its remaining 2-positional-arg
    signature), not by invoking it against real data: `default_lww_fold` is
    a plan builder needing pyspark installed but no live `SparkSession` to
    import/partial-apply, and this test stays session-free."""
    import functools
    import inspect

    from spine.frames import folds

    (temp_pipelines_module / "no_fold_transforms.py").write_text(
        textwrap.dedent(
            """
            def apply(valid_df, co_effects):
                return valid_df

            def post_check(candidate_df, co_effects):
                return candidate_df
            """
        )
    )
    spec = _make_spec(transforms_module="pipelines.no_fold_transforms", domain_id_col="policy_id")

    transforms = bind_transforms(spec)

    assert isinstance(transforms.fold, functools.partial)
    assert transforms.fold.func is folds.default_lww_fold
    assert transforms.fold.keywords == {"domain_id_col": "policy_id"}
    remaining = [
        p
        for p in inspect.signature(transforms.fold).parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    assert [p.name for p in remaining] == ["state_slice_df", "facts_df"]


# --- F4/F12-guard: two spec-shape WARNINGs, once per bind (bead conveyer-nvh.43) --

_VALID_APPLY_POST_CHECK = textwrap.dedent(
    """
    def apply(valid_df, co_effects):
        return valid_df

    def post_check(candidate_df, co_effects):
        return candidate_df
    """
)


def test_bind_transforms_own_state_without_serialize_warns_once(
    temp_pipelines_module: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (temp_pipelines_module / "own_state_no_serialize.py").write_text(_VALID_APPLY_POST_CHECK)
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
    (temp_pipelines_module / "own_state_with_serialize.py").write_text(_VALID_APPLY_POST_CHECK)
    spec = _make_spec(
        transforms_module="pipelines.own_state_with_serialize",
        co_effects={"self": CoEffectDecl(table="lake.commissions__state", own_state=True)},
        serialize=True,
    )

    with caplog.at_level(logging.WARNING, logger="spine.binding"):
        bind_transforms(spec)

    assert caplog.records == []


def test_bind_transforms_fold_custom_warns_once(
    temp_pipelines_module: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (temp_pipelines_module / "custom_fold_spec_flag.py").write_text(_VALID_APPLY_POST_CHECK)
    spec = _make_spec(transforms_module="pipelines.custom_fold_spec_flag", fold="custom")

    with caplog.at_level(logging.WARNING, logger="spine.binding"):
        bind_transforms(spec)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("custom" in r.getMessage() and spec.pipeline in r.getMessage() for r in warnings)


def test_bind_transforms_default_lww_fold_does_not_warn(
    temp_pipelines_module: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (temp_pipelines_module / "default_fold_spec_flag.py").write_text(_VALID_APPLY_POST_CHECK)
    spec = _make_spec(transforms_module="pipelines.default_fold_spec_flag", fold="default-lww")

    with caplog.at_level(logging.WARNING, logger="spine.binding"):
        bind_transforms(spec)

    assert caplog.records == []
