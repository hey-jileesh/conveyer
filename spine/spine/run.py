"""The sequence driver — `run(seed, fx)` folds over `STAGES`. LLD §7.3.

`run(seed, fx, stages=None)` folds `stages` (name, `StageFn`) pairs over the
context: each stage is called `stage(ctx, fx) -> BatchContext`; on success a
`RunFact` is recorded via `fx.record_run(run_facts.transition(...))` and the
loop advances to the returned context; on any `BaseException` a `"failed"`
`RunFact` is recorded (best-effort) and the exception is re-raised
unchanged, unwrapped -- the whole job fails so SFN retries it (D-1). Stages
carry zero instrumentation (004 §13.3); `core/run_facts.py` derives every
`RunFact` purely from `(stage, ctx_before, ctx_after, t0, t1)`.

`stages` defaults to `None` rather than `run.py` importing `spine.stages`'s
`SEQUENCE` at MODULE level, even though `spine/stages/__init__.py::SEQUENCE`
is a real, fully-defined tuple today (bead conveyer-nvh.22, M3 -- the eight
`(name, StageFn)` pairs in §7.3 order): the lazy, function-local import
inside `run()` itself, executed only the one time `run()` is actually
CALLED with no explicit `stages` argument, is still the right shape even
now that the historical reason for it (this module being written before
`spine.stages` existed, when a module-level import would have broken this
module's own importability) no longer applies -- it keeps `run.py`'s own
import time decoupled from `spine.stages`'s (and everything IT imports,
transitively every stage module), which stays valuable regardless of
whether the target package is a stub or complete. (The alternative most
likely to complicate this needlessly -- a module-level `try/except
ImportError` fallback -- would silently swallow a genuine `SEQUENCE`-
definition bug in `spine.stages`, so it was rejected then and stays
rejected now.) Most tests pass an explicit stub `stages` sequence and never
exercise this branch at all; the standing scenario suite
(`tests/integration/test_scenarios_core.py`) is this branch's own real
exercise, calling `spine.run.run(seed, fx)` with no explicit `stages`
argument.

**Set-once assertion [E-13] (§6.3):** after each stage succeeds, `run()`
asserts no already-set (non-default) field of `ctx_before` changed in
`ctx_after`, except the fields named in `context.SET_ONCE_EXEMPT_FIELDS`
(`guard_skips`, which accretes). Gated by `if __debug__:` -- the same
mechanism Python's own `assert` statement uses (stripped under `python -O`)
-- so the reflection walk over every `BatchContext` field costs nothing on
an interpreter run with optimizations enabled; this repo never runs Glue
jobs with `-O`, so in practice the assertion IS live end-to-end, giving the
"asserted in run.py under tests" contract real teeth in CI while still
honoring "zero per-field cost when the LLD implies it" for anyone who does
disable assertions. A dedicated `RunConfig`/`run()` boolean flag was
considered and rejected: it would be plumbing invented solely for this
check, and `__debug__` is the standard, already-idiomatic Python mechanism
for exactly this "test/dev-time assertion, free in an optimized run" shape.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from spine.core import run_facts

if TYPE_CHECKING:
    from collections.abc import Callable

    from spine.context import BatchContext
    from spine.effects.records import RunnerFx

    StageFn = Callable[[BatchContext, RunnerFx], BatchContext]
    Sequence = tuple[tuple[str, StageFn], ...]


def _assert_set_once(stage: str, before: BatchContext, after: BatchContext) -> None:
    """Every non-exempt field whose `before` value already differs from its
    dataclass default must be UNCHANGED in `after` (§6.3 [E-13]). A field
    with no default (`dataclasses.MISSING` -- the seed fields, always
    required) is treated as already-set from the very first stage onward,
    so any change to a seed field at any stage is a violation too."""
    from spine.context import SET_ONCE_EXEMPT_FIELDS

    for field in dataclasses.fields(before):
        if field.name in SET_ONCE_EXEMPT_FIELDS:
            continue
        default = field.default
        before_value = getattr(before, field.name)
        after_value = getattr(after, field.name)
        already_set = default is dataclasses.MISSING or before_value != default
        if already_set and before_value != after_value:
            raise AssertionError(
                f"stage {stage!r} overwrote already-set field {field.name!r} "
                "-- every post-seed field is set exactly once (§6.3 [E-13])"
            )


def run(
    seed: BatchContext,
    fx: RunnerFx,
    stages: Sequence | None = None,
) -> BatchContext:
    """Fold `stages` (default: `spine.stages.SEQUENCE`) over `seed`. §7.3."""
    if stages is None:
        # Lazy, function-local import (module docstring) -- SEQUENCE is a
        # real tuple (spine/stages/__init__.py, bead conveyer-nvh.22), not a
        # stub; this stays a local import for the decoupling reason the
        # module docstring gives, not because SEQUENCE is missing.
        from spine.stages import SEQUENCE as default_stages

        stages = default_stages

    ctx = seed
    for name, stage in stages:
        t0 = fx.now()
        try:
            nxt = stage(ctx, fx)
        except BaseException as exc:  # whole-job retry, D-1 (§7.3) -- re-raised below
            fx.record_run(run_facts.failed(name, ctx, t0, fx.now(), exc))
            raise
        if __debug__:
            _assert_set_once(name, ctx, nxt)
        fx.record_run(run_facts.transition(name, ctx, nxt, t0, fx.now()))
        ctx = nxt
    return ctx
