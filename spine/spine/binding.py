"""`bind_transforms(spec) -> Transforms` — importlib binding, namespace-constrained. LLD §7.4.

`Transforms` is the frozen record of a pipeline's three pure functions
(`apply`, `post_check`, `fold`) that rides `BatchContext.transforms` (I-10).
`bind_transforms` is the ONE place `importlib` is called in the whole
package: `importlib.import_module(spec.transforms_module)`, required
exports present (`apply`, `post_check` -- `fold` optional), arity-checked
via `inspect.signature` (2 positional parameters each), `fold` absent ⇒
`frames.default_lww_fold` partial-applied with `spec.domain_id_col`. Any
failure raises **before** `run()` is called (I-10) -- these are plain
built-in exceptions (`AttributeError`/`TypeError`/`ValueError`/
`ImportError`), not `TransientError`: a binding defect is a permanent
config/code error, not an infra hiccup an SFN retry could resolve (§7.6's
`TransientError` is reserved for `effects/*.py`); this matches 002.1 §7.0's
carve-out that boundary parses in `config.py`/`binding.py`/entrypoints may
raise plain exceptions (7.0 delta note).

`spec.transforms_module` is pydantic-pattern-guarded to `^pipelines\\.
[a-z0-9_]+(\\.[a-z0-9_]+)*$` at the `PipelineSpecModel` boundary (I-10,
[S-3]) -- `_assert_in_namespace` below re-checks the same `pipelines.`
prefix defensively, in case a `PipelineSpecModel` ever reaches this function
without having gone through normal validation (e.g. `model_construct`).
This does not add meaningful new coverage against a *validated* spec (the
model already guarantees it) but keeps `bind_transforms` fail-safe on its
own, independent of the model's own validators ever being bypassed.

`frames.folds` (needed only for the fold-defaulting branch) is imported
LAZILY, inside `bind_transforms`, not at module top -- it pulls in pyspark
(`pyspark.sql.Window`/`functions`), and this module must stay importable
(and its own failure-path branches testable) with zero pyspark/SparkSession
requirement, matching `context.py`'s and this module's own prior
docstring's convention. `default_lww_fold` itself is a plan builder (a
`DataFrame -> DataFrame` function definition), so importing `frames.folds`
and partial-applying it needs pyspark installed but no live `SparkSession` --
only the "no fold export" success branch pays that import cost; every
failure-path branch below (missing export, non-callable, bad arity,
out-of-namespace, broken import) returns before ever touching `frames`.

`from __future__ import annotations` postpones evaluation of the
`Callable[[DataFrame, ...], DataFrame]` annotations to strings, so this
module does not require a real pyspark import (or a SparkSession) merely to
be imported -- `DataFrame` is only ever needed under `TYPE_CHECKING`,
matching `context.py`'s convention for the same reason.

**Two spec-shape WARNINGs fire here, once per `bind_transforms` call
(critique F4/F12-guard, bead conveyer-nvh.43)** — `bind_transforms` runs
exactly ONCE per run, pre-land, so it is the natural place for a diagnostic
that is pure spec inspection and would otherwise have to re-fire on every
attempt (or every stage) of the same run:

* **F4** (moved from `stages/pull.py`, which used to log this on every
  `pull` invocation, i.e. once per ATTEMPT rather than once per run):
  `decl.own_state and not spec.serialize` for any declared co-effect — Phase
  1 does not honor `serialize` (004 §16.2) — logs one WARNING naming the
  co-effect and its table.
* **F12-guard** (critique deviation-12): `spec.fold == "custom"` — `stages/
  fold.py`'s own documented gap is that it always falls back to the
  default-lww ordering key (`frames.folds.LWW_ORDERING_COLUMNS`) regardless
  of `spec.fold`, which is only correct by accident for a genuinely custom
  fold (there is no Phase 1 contract for a custom fold to declare its own
  ordering columns; 007 owns the real resolution) — logs one WARNING naming
  the pipeline.

Both checks are pure spec inspection, independent of whether
`transforms_module` even imports successfully, so they run FIRST, before
`_assert_in_namespace`/`importlib.import_module` — a spec whose binding
later fails still gets these two diagnostics logged (harmless: the run is
aborting either way, pre-land, so no effect has run yet regardless).
"""

from __future__ import annotations

import functools
import importlib
import inspect
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

_LOGGER_NAME = "spine.binding"

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from spine.core.model import PipelineSpecModel

# `inspect.Parameter` kinds counted toward the "N positional arguments"
# arity contract (§7.4): a bare `*args`/`**kwargs` catch-all or a keyword-
# only parameter does not count -- the contract is an EXACT positional
# count, not "at least N".
_POSITIONAL_KINDS = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)


@dataclass(frozen=True)
class Transforms:
    apply: Callable[[DataFrame, Mapping[str, DataFrame]], DataFrame]
    post_check: Callable[[DataFrame, Mapping[str, DataFrame]], DataFrame]
    fold: Callable[[DataFrame, DataFrame], DataFrame]


def _warn_own_state_without_serialize(spec: PipelineSpecModel) -> None:
    """F4 (moved from `stages/pull.py`, bead conveyer-nvh.43): a co-effect
    declared as the pipeline's own current-state table (`decl.own_state`)
    without opting into serialized execution (`spec.serialize`) is a named,
    deliberate Phase 1 honoring-gap signal (004 §16.2) -- logged once per
    `bind_transforms` call (i.e. once per run), not once per `pull`
    invocation as the prior stage-level placement did."""
    logger = logging.getLogger(_LOGGER_NAME)
    for name, decl in spec.co_effects.items():
        if decl.own_state and not spec.serialize:
            logger.warning(
                "co-effect %r (table=%r) is own_state but spec.serialize is False "
                "-- Phase 1 does not honor serialize (004 §16.2)",
                name,
                decl.table,
                extra={"pipeline": spec.pipeline, "stage": "bind_transforms"},
            )


def _warn_custom_fold_ordering_gap(spec: PipelineSpecModel) -> None:
    """F12-guard (critique deviation-12, bead conveyer-nvh.43): `stages/
    fold.py` always resolves the MERGE ordering columns to the default-lww
    key (`frames.folds.LWW_ORDERING_COLUMNS`) regardless of `spec.fold` --
    correct only by accident for a genuinely custom fold, since Phase 1 has
    no contract for a custom fold to declare its own ordering columns (007
    owns that resolution). One WARNING per bind, naming the pipeline."""
    if spec.fold == "custom":
        logger = logging.getLogger(_LOGGER_NAME)
        logger.warning(
            "spec.fold == 'custom' for pipeline %r -- stages/fold.py still resolves "
            "MERGE ordering columns to the default-lww key regardless (007 owns real "
            "custom-fold ordering-key resolution)",
            spec.pipeline,
            extra={"pipeline": spec.pipeline, "stage": "bind_transforms"},
        )


def _assert_in_namespace(transforms_module: str) -> None:
    """Defensive re-check of I-10's `pipelines.` namespace constraint --
    the `PipelineSpecModel` field pattern already enforces this; this is a
    belt-and-braces check so `bind_transforms` never calls `importlib` on a
    non-`pipelines.` dotted path even if a spec reached it unvalidated."""
    if not transforms_module.startswith("pipelines."):
        raise ValueError(
            f"bind_transforms: {transforms_module!r} is outside the 'pipelines.' namespace"
        )


def _require_callable(module: Any, name: str, module_name: str) -> Callable[..., Any]:
    if not hasattr(module, name):
        raise AttributeError(f"bind_transforms: {module_name!r} does not export {name!r}")
    fn = getattr(module, name)
    if not callable(fn):
        raise TypeError(
            f"bind_transforms: {module_name}.{name} is not callable (got {type(fn).__name__})"
        )
    return fn  # type: ignore[no-any-return]


def _check_arity(fn: Callable[..., Any], expected: int, name: str, module_name: str) -> None:
    positional = [
        p for p in inspect.signature(fn).parameters.values() if p.kind in _POSITIONAL_KINDS
    ]
    if len(positional) != expected:
        raise TypeError(
            f"bind_transforms: {module_name}.{name} must accept exactly {expected} "
            f"positional arguments, got {len(positional)}"
        )


def bind_transforms(spec: PipelineSpecModel) -> Transforms:
    """`importlib.import_module(spec.transforms_module)` (namespace-
    constrained, I-10); require callables `apply`, `post_check`; `fold`
    optional -- absent ⇒ `frames.default_lww_fold` partial-applied with
    `spec.domain_id_col`; arity-checked via `inspect.signature` (2, 2, 2
    positional). Any failure raises before `run()` is called (I-10)."""
    _warn_own_state_without_serialize(spec)
    _warn_custom_fold_ordering_gap(spec)
    _assert_in_namespace(spec.transforms_module)
    try:
        module = importlib.import_module(spec.transforms_module)
    except ImportError as exc:
        raise ImportError(
            f"bind_transforms: cannot import {spec.transforms_module!r}: {exc}"
        ) from exc

    apply_fn = _require_callable(module, "apply", spec.transforms_module)
    post_check_fn = _require_callable(module, "post_check", spec.transforms_module)
    _check_arity(apply_fn, 2, "apply", spec.transforms_module)
    _check_arity(post_check_fn, 2, "post_check", spec.transforms_module)

    fold_fn = getattr(module, "fold", None)
    if fold_fn is None:
        from spine.frames import folds  # lazy: pulls in pyspark, see module docstring

        fold_fn = functools.partial(folds.default_lww_fold, domain_id_col=spec.domain_id_col)
    else:
        if not callable(fold_fn):
            raise TypeError(
                f"bind_transforms: {spec.transforms_module}.fold is not callable "
                f"(got {type(fold_fn).__name__})"
            )
        _check_arity(fold_fn, 2, "fold", spec.transforms_module)

    return Transforms(apply=apply_fn, post_check=post_check_fn, fold=fold_fn)
