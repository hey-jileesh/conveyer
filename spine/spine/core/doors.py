"""The fact-presence door planner — LLD 006.1 §8.2 (P-7), §8.4.

**One pure planner, ANY-table composition.** `any_fact_present` is the
shared primitive every fact-presence door in this architecture composes
from — pre_check's [DC-1] door, post_check's row doors (`post_check_path`
below), and the `batch_check` demotion door (§8.4: "renders a verdict iff
no declared fact table has the batch — the ANY composition again"). Its
input, `fact_presence: Mapping[str, bool]`, is one `table_has_batch` probe
per declared fact table (P-1's `fact_types` enumeration) — this module
takes the already-probed booleans, never a catalog handle (§15's invariant:
"doors reading metadata... probes are `table_has_batch` only; the planner
takes booleans, not catalogs").

**Why ANY, not per-table or ALL (the [H-2] wedge argument, normative).** A
kill between type appends leaves facts of type A durable and type B absent.
The verdict that admitted A's facts was already rendered by the attempt
that committed them; re-adjudicating (re-rendering row verdicts, or
re-evaluating `batch_check`) against re-resolved co-effect snapshots while
A stands durable can quarantine candidates whose siblings are already
facts, or fail the batch outright — the exact wedge [H-2]/[R2-1] closed at
batch grain. An ALL-tables door would re-adjudicate in exactly this
partial-kill window; a per-table door would render FRESH verdicts for B
against drifted perception while A's stand — two attempts' judgment inside
one batch. The sound composition is batch-grain authority (ANY durable fact
⇒ the batch's judgment is settled) over per-table completion (each table's
own append/skip decision, 007.1 F-4's separate ground) — durable state is
authoritative the moment ANY of it exists, because the verdict that
admitted it was already rendered once.

Plain-value pure — no I/O, no catalog, no Spark."""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum


def any_fact_present(fact_presence: Mapping[str, bool]) -> bool:
    """P-7(a)/§8.2: presence in ANY declared fact table."""
    return any(fact_presence.values())


class PostCheckPath(Enum):
    """§8.2's decision table."""

    FRESH = "fresh"
    DURABLE_SUBTRACT = "durable_subtract"
    DURABLE_AUTHORITY = "durable_authority"


def post_check_path(*, q_guard_present: bool, fact_presence: Mapping[str, bool]) -> PostCheckPath:
    """§8.2:

    | q_guard | any-table facts | Path |
    |---|---|---|
    | absent | absent | FRESH |
    | present | -- | DURABLE_SUBTRACT |
    | absent | present | DURABLE_AUTHORITY |

    The guard's presence decides everything when it is present -- fact
    presence is consulted only in the guard-absent branch."""
    if q_guard_present:
        return PostCheckPath.DURABLE_SUBTRACT
    if any_fact_present(fact_presence):
        return PostCheckPath.DURABLE_AUTHORITY
    return PostCheckPath.FRESH
