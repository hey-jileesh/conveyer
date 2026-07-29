"""`plan_append` — `AppendPlan` decisions (decide-then-do). LLD §7.5, §7.7, I-3.

The guard mechanics themselves (`table_has_batch` reading data, never
snapshot metadata — I-3) live in `effects/spark.py`; this module is the pure
decision half of decide-then-do: given the table, an optional quarantine
stage key, and whether the batch is already present (a plain `bool` the
effect computed), decide whether to append. No data, no I/O, no Spark.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AppendPlan:
    table: str
    stage_key: str | None
    do_append: bool


def plan_append(table: str, stage_key: str | None, present: bool) -> AppendPlan:
    """Decide whether to append: append iff the batch is NOT already present.
    `stage_key` distinguishes quarantine sub-streams within one table
    (`pre_check` / `post_check`, §7.5) and rides along unexamined -- this
    function's only decision variable is `present`."""
    return AppendPlan(table=table, stage_key=stage_key, do_append=not present)
