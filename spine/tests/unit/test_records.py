"""Unit tests for `spine.effects.records` — LLD §7.6.

Covers: `TransientError` is a plain, raisable/catchable `Exception`
subclass and the ONLY exception type this module defines; `MergeResult` is a
frozen dataclass with the `(snapshot_id, summary, attributable)` shape (the
third field, nvh.40 [F1], defaults `True` -- distinguishing an unattributable
own-commit resolution from a logical no-op, both `(None, None)` otherwise);
`RunnerFx` is a frozen dataclass carrying exactly §7.6's full field list and
is constructible from plain callables (record-of-functions, no framework).
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime

import pytest
from spine.effects.records import MergeResult, RunnerFx, TransientError


def test_transient_error_is_a_plain_exception_subclass() -> None:
    assert issubclass(TransientError, Exception)
    with pytest.raises(TransientError, match="boom"):
        raise TransientError("boom")


def test_merge_result_is_frozen_dataclass_with_expected_fields() -> None:
    assert dataclasses.is_dataclass(MergeResult)
    result = MergeResult(snapshot_id=42, summary={"added-records": "3"})
    assert result.snapshot_id == 42
    assert result.summary == {"added-records": "3"}
    with pytest.raises(dataclasses.FrozenInstanceError):
        result.snapshot_id = 1  # type: ignore[misc]


def test_merge_result_no_op_shape() -> None:
    result = MergeResult(snapshot_id=None, summary=None)
    assert result.snapshot_id is None
    assert result.summary is None
    assert result.attributable is True


def test_merge_result_unattributable_is_distinct_from_no_op() -> None:
    """nvh.40 [F1]: `(None, None)` alone is ambiguous between "healthy
    rerun, nothing to report" (I-19 no-op, `attributable=True` by default)
    and "a real commit happened but couldn't be safely identified as ours"
    -- `attributable=False` is a distinct, explicitly-constructed state,
    never the default."""
    no_op = MergeResult(snapshot_id=None, summary=None)
    unattributable = MergeResult(snapshot_id=None, summary=None, attributable=False)
    assert unattributable.attributable is False
    assert unattributable != no_op


_EXPECTED_FIELDS = {
    "read_objects",
    "read_table",
    "read_batch",
    "table_has_batch",
    "append",
    "merge",
    "resolve_batch_snapshot",
    "record_run",
    "emit",
    "now",
    "config",
}


def test_runner_fx_has_exactly_the_section_7_6_fields() -> None:
    field_names = {f.name for f in dataclasses.fields(RunnerFx)}
    assert field_names == _EXPECTED_FIELDS


def test_runner_fx_is_frozen_and_constructible_from_plain_callables() -> None:
    fx = RunnerFx(
        read_objects=lambda uris, hints: "df",
        read_table=lambda t: ("df", 1),
        read_batch=lambda t, b: "df",
        table_has_batch=lambda t, b, k: False,
        append=lambda t, df, batch_id, stage_key: (0, {}),
        merge=lambda spec, df: MergeResult(None, None),
        resolve_batch_snapshot=lambda t, b, k: None,
        record_run=lambda rf: None,
        emit=lambda dt, model: None,
        now=lambda: datetime.now(UTC),
        config="dummy-config",
    )
    assert fx.append("t", "df", "b1", None) == (0, {})
    assert fx.table_has_batch("t", "b", None) is False
    assert isinstance(fx.now(), datetime)
    with pytest.raises(dataclasses.FrozenInstanceError):
        fx.config = "other"  # type: ignore[misc]
