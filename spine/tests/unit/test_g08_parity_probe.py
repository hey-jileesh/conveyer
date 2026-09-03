"""Local rehearsal for `spine.probes.g08_parity` — the standalone Glue-parity
probe for G-08's discriminator rows (LLD 006.1 §13.1/§14 B5, bead
`conveyer-6pg.15`/`.16`). Requests the shared `spark` fixture
(`tests/conftest.py`) exactly like `tests/unit/test_glue_main.py`'s own
JVM-touching cases (same directory: both are entrypoint-adjacent modules
tested under `tests/unit`, not `tests/frames`).

This suite is the local half's own proof that the probe and its vector
table are self-consistent under one engine; the real cross-engine Glue
claim is B5-gate's job (`conveyer-6pg.16`, LEAVE OPEN, human-supervised).
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from spine.probes import g08_parity


def test_run_probe_all_vectors_pass_under_local_spark(spark: SparkSession) -> None:
    results = g08_parity.run_probe(spark)
    failed = [r for r in results if not r.passed]
    assert failed == [], (
        f"{len(failed)}/{len(results)} G-08 discriminator rows failed under local Spark: {failed}"
    )
    assert len(results) == len(g08_parity.G08_VECTORS)


def test_build_session_adopts_the_shared_session(spark: SparkSession) -> None:
    # No `.master(...)` set (mirrors `entrypoints/session.py::build_session`'s
    # documented idiom): inside the shared test JVM, `getOrCreate()` adopts the
    # already-live `spark` fixture rather than building a second session.
    assert g08_parity._build_session() is spark


def test_evaluate_reports_a_value_mismatch(spark: SparkSession) -> None:
    df = g08_parity._build_probe_df(spark)
    bad = g08_parity.ParityVector("deliberate-mismatch", "i + 1", 999)
    result = g08_parity._evaluate(df, bad)
    assert result.passed is False
    assert result.actual == 6
    assert result.error is None


def test_evaluate_reports_a_grammar_rejection(spark: SparkSession) -> None:
    df = g08_parity._build_probe_df(spark)
    # `bround` is deliberately NOT grammar-admitted (G-08's own negative
    # control, [EM-8]) -- a non-`raw` vector naming it must surface as a
    # probe failure, never a silent skip.
    rejected = g08_parity.ParityVector("bround-without-raw-flag", "bround(2.5)", Decimal("2"))
    result = g08_parity._evaluate(df, rejected)
    assert result.passed is False
    assert result.error == "grammar rejected"


def test_print_report_returns_false_on_any_failure(capsys: pytest.CaptureFixture[str]) -> None:
    passing = g08_parity.ParityResult("ok", "1", 1, 1, True, None)
    failing = g08_parity.ParityResult("bad", "1", 1, 2, False, None)
    assert g08_parity._print_report([passing]) is True
    assert g08_parity._print_report([passing, failing]) is False
    out = capsys.readouterr().out
    assert "[PASS] ok" in out
    assert "[FAIL] bad" in out


def test_probe_df_for_rows_override_and_empty_frame(spark: SparkSession) -> None:
    # A006-4: `rows=None` (the default) is the shared single-row frame every
    # scalar-position vector uses; `rows=()` builds a genuinely EMPTY frame
    # (the empty-set aggregate discriminators); a non-empty override supplies
    # exactly those rows.
    default_vector = g08_parity.ParityVector("default", "i", 5)
    assert g08_parity._probe_df_for(spark, default_vector).count() == 1

    empty_vector = g08_parity.ParityVector("empty", "sum(i)", None, group=True, rows=())
    assert g08_parity._probe_df_for(spark, empty_vector).count() == 0

    two_row = (g08_parity._row(), g08_parity._row(i=7))
    override_vector = g08_parity.ParityVector("override", "count(1)", 2, group=True, rows=two_row)
    assert g08_parity._probe_df_for(spark, override_vector).count() == 2


def test_evaluate_group_true_reduces_via_agg(spark: SparkSession) -> None:
    # A `group=True` vector must reduce via `df.agg(...)`, not `df.select
    # (...)` -- over a 2-row frame, `count(1)` correctly reduces to a SINGLE
    # row with value 2 only under `.agg(...)`.
    two_row = (g08_parity._row(), g08_parity._row(i=7))
    df = g08_parity._build_probe_df(spark, two_row)
    vector = g08_parity.ParityVector("count-two-rows", "count(1)", 2, group=True)
    result = g08_parity._evaluate(df, vector)
    assert result.passed is True
    assert result.actual == 2


def test_evaluate_group_true_validates_at_aggregate_position(spark: SparkSession) -> None:
    # `sum(...)` is an aggregate-position-only member (§6.3) -- a `group=True`
    # vector must validate at "aggregate" position, not "scalar" (which
    # would reject it as "aggregate outside aggregate position").
    df = g08_parity._build_probe_df(spark)
    vector = g08_parity.ParityVector("sum-i", "sum(i)", 5, group=True)
    result = g08_parity._evaluate(df, vector)
    assert result.passed is True
    assert result.error is None


def test_main_raises_systemexit_1_on_any_failure(
    spark: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        g08_parity,
        "run_probe",
        lambda _spark: [g08_parity.ParityResult("bad", "1", 1, 2, False, None)],
    )
    with pytest.raises(SystemExit) as exc_info:
        g08_parity.main()
    assert exc_info.value.code == 1
