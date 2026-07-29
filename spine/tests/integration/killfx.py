"""Failure-injection wrappers for R-03/R-08 (LLD §12.4, I-4, I-19 [T-4]).

Both wrappers below have the `make_wrapped_fx`-shaped signature
(`Callable[[Callable], Callable]`, `tests/conftest.py`'s own mechanism, G-3):
`make_wrapped_fx(fx, {"append": kill_after(1)})` replaces one NAMED
`RunnerFx` field with `wrapper(original_callable)`, everything else passing
through unchanged.

**`kill_after`** — R-03's `KillFx`. `SimulatedKill` stands in for "the
process died right after committing effect number N": `BatchContext` is
memory-only and a restart re-enters `run()` from stage 1 (`land`), so
raising immediately after a wrapped call commits/returns is behaviorally
identical to a hard process kill at that instant — no signal handling or
`os._exit` needed, an ordinary exception unwinds `run.py`'s stage loop
exactly like an uncaught `KeyboardInterrupt`/`SIGKILL` would (the just-
completed commit is left intact and durable; everything after it never
ran). Mid-commit death (killed WHILE an `append`/`MERGE` is executing) is a
different failure mode this module does not simulate — Iceberg commits are
atomic (D-4's premise, no partial-commit state exists to simulate); R-13
covers that atomicity claim directly instead by asserting exactly one new
snapshot per effectful stage.

`kill_after`'s counter is **per wrapped field, not shared globally across
every `RunnerFx` field** — deliberate: `append`/`read_table`/`emit` are each
called more than once across one whole `SEQUENCE` run by DIFFERENT stages
(e.g. `append` is called by `land`, `pre_check` (only when there are
violations), `post_check` (ditto), and `commit`, in that order; `read_table`
is called by `pull`, then again later by `commit`'s own I-24 structural
check, then again by `fold`; `emit` is called by `land` — "batch-started" —
then again by `publish` — "batch-completed"). A single field's Nth
invocation, counted independently of every other field, is what lets one
wrapped field unambiguously identify ONE specific stage's own effect —
`test_scenarios_kill.py`'s own module docstring spells out, per kill point,
which `(field, occurrence)` pair that is.

`before=True` raises BEFORE calling the wrapped function through on the
target occurrence — used only for the [E-16] pre-land kill point: no effect
happens at all (the underlying commit never executes), matching a process
death before ANY effect has committed. `before=False` (default) calls
through first — the real effect genuinely commits/completes, exactly like
every other kill point — then raises, matching R-03's literal wording
("immediately AFTER the Nth effect commit").

**`flaky_once`** — R-08's merge-conflict injector. Raises `exc_factory()` on
the wrapped field's FIRST call only, with NO call-through at all (a real
Iceberg commit conflict — `CommitFailedException`/`ValidationException` et
al, `effects/spark.py`'s own `is_transient_iceberg_failure` — never
partially commits; D-4's atomicity premise applies here too), then passes
every subsequent call straight through, unwrapped, forever after — modeling
"the merge failed once, transiently, then a later attempt succeeds cleanly."
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

_F = TypeVar("_F", bound=Callable[..., Any])


class SimulatedKill(Exception):
    """Stands in for a process death at one wrapped fx call's boundary
    (R-03 [E-16]) — a plain `Exception` subclass (not `BaseException`):
    `run.py`'s own `except BaseException` ledger-recording clause catches
    and re-raises either shape identically, so an ordinary `Exception` is
    sufficient and keeps this test-only class out of `BaseException`
    territory (reserved for genuine process-level signals)."""


def kill_after(occurrence: int, *, before: bool = False) -> Callable[[_F], _F]:
    """`make_wrapped_fx`-shaped: raises `SimulatedKill` at this field's
    `occurrence`-th call (1-indexed), independent of any other wrapped
    field's own counter. `before=True` raises before calling through (no
    effect at all, the [E-16] pre-land point); otherwise calls through
    first, then raises. Every call before/after the target occurrence
    passes straight through untouched."""

    def wrapper(fn: _F) -> _F:
        count = 0

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            nonlocal count
            count += 1
            if count == occurrence and before:
                raise SimulatedKill(f"simulated kill BEFORE call #{count} to {fn!r}")
            result = fn(*args, **kwargs)
            if count == occurrence and not before:
                raise SimulatedKill(f"simulated kill AFTER call #{count} to {fn!r}")
            return result

        return wrapped  # type: ignore[return-value]

    return wrapper


def flaky_once(exc_factory: Callable[[], BaseException]) -> Callable[[_F], _F]:
    """`make_wrapped_fx`-shaped: raises `exc_factory()` on the wrapped
    field's FIRST call, with no call-through (R-08: a merge conflict commits
    nothing); every subsequent call passes straight through, unwrapped."""

    def wrapper(fn: _F) -> _F:
        count = 0

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            nonlocal count
            count += 1
            if count == 1:
                raise exc_factory()
            return fn(*args, **kwargs)

        return wrapped  # type: ignore[return-value]

    return wrapper
