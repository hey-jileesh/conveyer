"""Unit tests for `spine.core.guards.plan_append` — LLD §7.5, §7.7, I-3.

`plan_append`'s only decision variable is `present`: append iff NOT already
present. Exhaustive example-based truth table plus a property test over
arbitrary `table`/`stage_key`/`present` combinations (§12.4).
"""

import dataclasses

import pytest
from hypothesis import given
from hypothesis import strategies as st
from spine.core import guards


def test_append_when_absent() -> None:
    plan = guards.plan_append("lake.x__raw", None, present=False)
    assert plan == guards.AppendPlan(table="lake.x__raw", stage_key=None, do_append=True)


def test_no_append_when_present() -> None:
    plan = guards.plan_append("lake.x__raw", None, present=True)
    assert plan == guards.AppendPlan(table="lake.x__raw", stage_key=None, do_append=False)


def test_stage_key_rides_along_unexamined() -> None:
    plan = guards.plan_append("lake.x__quarantine", "pre_check", present=False)
    assert plan.stage_key == "pre_check"
    assert plan.do_append is True


def test_append_plan_is_frozen() -> None:
    plan = guards.plan_append("t", None, present=False)
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.do_append = False  # type: ignore[misc]


@given(
    table=st.text(min_size=1, max_size=30),
    stage_key=st.one_of(st.none(), st.text(min_size=1, max_size=20)),
    present=st.booleans(),
)
def test_plan_append_truth_table(table: str, stage_key: str | None, present: bool) -> None:
    plan = guards.plan_append(table, stage_key, present)
    assert plan.table == table
    assert plan.stage_key == stage_key
    assert plan.do_append == (not present)
