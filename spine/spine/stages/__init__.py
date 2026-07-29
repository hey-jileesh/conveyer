"""SEQUENCE: the ordered tuple `run.py` folds over. LLD §7.3, §7.5.

`run.py`'s own module docstring lazily imports `SEQUENCE` from this package
(`from spine.stages import SEQUENCE`) the one time `run()` is called with no
explicit `stages` argument — this is that package's real definition, owned
by this bead (`conveyer-nvh.22`). Every stage module exposes a bare `run(ctx,
fx) -> BatchContext` function (§7.5's uniform `StageFn` shape); `SEQUENCE` is
simply those eight callables, `(name, callable)` pairs, in the fixed §7.3
order `land → pre_check → pull → apply → post_check → commit → fold →
publish`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from spine.stages import apply, commit, fold, land, post_check, pre_check, publish, pull

if TYPE_CHECKING:
    from collections.abc import Callable

    from spine.context import BatchContext
    from spine.effects.records import RunnerFx

    StageFn = Callable[[BatchContext, RunnerFx], BatchContext]

SEQUENCE: tuple[tuple[str, StageFn], ...] = (
    ("land", land.run),
    ("pre_check", pre_check.run),
    ("pull", pull.run),
    ("apply", apply.run),
    ("post_check", post_check.run),
    ("commit", commit.run),
    ("fold", fold.run),
    ("publish", publish.run),
)
