"""`bind_transforms(spec) -> Transforms` — importlib binding, namespace-constrained. LLD §7.4.

`Transforms` is the frozen record of a pipeline's ONE pure function
(`apply`) that rides `BatchContext.transforms` (I-10). **006.1 §4.4 (bead
conveyer-6pg.13, B3): `Transforms` DROPS `post_check`** (errata row 19's
completion) -- `apply` is now the interpreter's own boundary
(`(valid_df, co_effects) -> Mapping[str, DataFrame]`, one candidate frame
per declared fact type); business-rule evaluation moved entirely into the
framework's `post_check` STAGE (`frames/business_checks.py` +
`checks.yaml`'s declared checks, 006.1 §7) -- pipelines contribute zero
check code (D-1/D-6). A transforms module still exporting `post_check` is
therefore a STALE export, not a binding requirement any more: `bind_
transforms` itself no longer looks for it at all (a stale export is caught
elsewhere -- `core/bind_checks.py`'s S4 check, fed by `entrypoints/
glue_main.py::_acquire_transforms_meta`'s OWN independent `hasattr` read of
the raw imported module, deliberately kept separate from this function's
contract, per that module's own docstring) -- so this function's own
requirement is simply "no longer checks for it," never "refuses it."

**`Transforms` DROPS `fold` too (critique gate wf_24a3125f-ecc F2, bead
conveyer-6pg.31).** 007.1 B10 (bead conveyer-6pg.22) rewrote `stages/
fold.py` into a purely mechanical, per-declared-fact-type reduce
(`frames/fold.py::reduce_batch_winners`, driven by `MergeSpec.ordering_cols`
alone) that never calls `ctx.transforms.fold` at all -- so this module's OWN
fold-defaulting wiring (`fold` optional, absent ⇒ `frames.default_lww_fold`
partial-applied with `spec.domain_id_col`) was binding a member `stages/
fold.py` never read: a pipeline exporting `fold` bound cleanly and was
silently ignored, the fail-silent asymmetry against S4's own loud stale-
`post_check`-export refusal above. Fixed by dropping the member outright,
the same "hard cut, not a silent no-op" shape B3 already applied to
`post_check`: a transforms module still exporting `fold` is now a STALE
export too, refused at bind by `core/bind_checks.py`'s own new
`stale-fold-export` check (`TransformsMeta.has_fold_export`, `entrypoints/
glue_main.py::_acquire_transforms_meta`'s own `hasattr` read, mirroring S4
exactly). **The reserved custom-fold seam is NOT removed by this fix** --
`PipelineSpecModel.fold`'s `Literal["default-lww", "custom"]` field and its
`_check_fold_not_custom` bind-time refusal (007 D-3(e)) both stay untouched;
only the dead, silently-ignored WIRING that used to sit between a bound
`Transforms.fold` and a stage that never called it is gone. `frames/
folds.py` (the v1-era `default_lww_fold`/`winners_per_domain`/
`ordering_struct_gt` machinery this wiring was the sole production consumer
of) is deleted outright too -- see that module's own former docstring,
retained in `git log`, not here.

`bind_transforms` is the ONE place `importlib` is called in the whole
package: `importlib.import_module(spec.transforms_module)`, required
export present (`apply`, arity 2). Any failure raises **before** `run()` is
called (I-10) -- these are plain built-in exceptions (`AttributeError`/
`TypeError`/`ValueError`/`ImportError`), not `TransientError`: a binding
defect is a permanent config/code error, not an infra hiccup an SFN retry
could resolve (§7.6's `TransientError` is reserved for `effects/*.py`);
this matches 002.1 §7.0's carve-out that boundary parses in `config.py`/
`binding.py`/entrypoints may raise plain exceptions (7.0 delta note).

`spec.transforms_module` is pydantic-pattern-guarded to `^pipelines\\.
[a-z0-9_]+(\\.[a-z0-9_]+)*$` at the `PipelineSpecModel` boundary (I-10,
[S-3]) -- `_assert_in_namespace` below re-checks the same `pipelines.`
prefix defensively, in case a `PipelineSpecModel` ever reaches this function
without having gone through normal validation (e.g. `model_construct`).
This does not add meaningful new coverage against a *validated* spec (the
model already guarantees it) but keeps `bind_transforms` fail-safe on its
own, independent of the model's own validators ever being bypassed.

`from __future__ import annotations` postpones evaluation of the
`Callable[[DataFrame, ...], DataFrame]` annotations to strings, so this
module does not require a real pyspark import (or a SparkSession) merely to
be imported -- `DataFrame` is only ever needed under `TYPE_CHECKING`,
matching `context.py`'s convention for the same reason.

**One spec-shape WARNING fires here, once per `bind_transforms` call
(critique F4, bead conveyer-nvh.43)** — `bind_transforms` runs exactly ONCE
per run, pre-land, so it is the natural place for a diagnostic that is pure
spec inspection and would otherwise have to re-fire on every attempt (or
every stage) of the same run: `decl.own_state and not spec.serialize` for
any declared co-effect — Phase 1 does not honor `serialize` (004 §16.2) —
logs one WARNING naming the co-effect and its table. **The second, F12-guard
WARNING this section used to also document (`spec.fold == "custom"`) is
gone, along with the dead default-lww fold-defaulting wiring it was
warning ABOUT** (critique gate wf_24a3125f-ecc F2, bead conveyer-6pg.31,
this module's own docstring above has the account) -- it is UNREACHABLE
regardless: `spec.fold == "custom"` already raises at `PipelineSpecModel`
parse (S3, `core/model.py::_check_fold_not_custom`), so no spec reaching
this function could ever have carried it.

This check is pure spec inspection, independent of whether
`transforms_module` even imports successfully, so it runs FIRST, before
`_assert_in_namespace`/`importlib.import_module` — a spec whose binding
later fails still gets this diagnostic logged (harmless: the run is
aborting either way, pre-land, so no effect has run yet regardless).
"""

from __future__ import annotations

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
    # 006.1 §4.4: `apply` now returns a MAPPING -- one candidate frame per
    # declared fact type (P-1); `post_check` is gone (the framework's own
    # interpreter stage now owns business-rule evaluation, §7). Critique
    # gate wf_24a3125f-ecc F2 (bead conveyer-6pg.31): `fold` is gone too --
    # `stages/fold.py`'s own mechanical §8.2 reduce never called it (module
    # docstring above has the account).
    apply: Callable[[DataFrame, Mapping[str, DataFrame]], Mapping[str, DataFrame]]


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
    constrained, I-10); require callable `apply`, arity-checked via
    `inspect.signature` (2 positional). Any failure raises before `run()` is
    called (I-10). **006.1 §4.4: no longer looks for `post_check` at all**
    -- a stale `post_check` export is a different function's concern (S4,
    this module's own docstring). **Critique gate wf_24a3125f-ecc F2 (bead
    conveyer-6pg.31): no longer looks for `fold` either** -- a stale `fold`
    export is that same function's concern too now (S4's own sibling
    `stale-fold-export` check, this module's own docstring)."""
    _warn_own_state_without_serialize(spec)
    _assert_in_namespace(spec.transforms_module)
    try:
        module = importlib.import_module(spec.transforms_module)
    except ImportError as exc:
        raise ImportError(
            f"bind_transforms: cannot import {spec.transforms_module!r}: {exc}"
        ) from exc

    apply_fn = _require_callable(module, "apply", spec.transforms_module)
    _check_arity(apply_fn, 2, "apply", spec.transforms_module)

    return Transforms(apply=apply_fn)
