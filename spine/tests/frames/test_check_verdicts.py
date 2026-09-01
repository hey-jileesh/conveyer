"""Cross-lane check-verdict vectors — LLD 006.1 §13.3 (**G-10**): "the spine
evaluates every committed vector."

**Vector location and shape** (`contracts/fixtures/check-verdicts/*.json`,
§13.3, normative here only for location/shape — governance, authoring
format, and the event-lane harness are 009/Track A's, §16.1): one JSON
array per file of `{"check": ..., "input": ..., "verdict": ...}` triples.
`check` is a parsed `RowCheckModel`/`MembershipCheckModel`
(`model_dump(mode="json")` — `kind`/`id`/`fact_type`/`expr`-or-`columns`/
`reason`, §4.2's own shape). `input` is `{"row": {<col>: <value>}}` for a
row check, or `{"row": {...}, "ref_rows": [[<ref-col-values, positional>],
...]}` for a membership check (the candidate row plus the co-effect
reference rows the check's `columns`/`ref_columns` name). `verdict` is
`"pass"` or `"fail"`.

**This family's own untagging parser** (004 D-13: shared vectors, never
shared code — every consumer, including this one, writes its own; see
`test_canonical.py::_parse_fixture_value`/`test_fact_hash.py`'s own copies
for the sibling precedent). Row values follow 005.1 §15.2's tagged-JSON
convention (`$decimal`/`$date`/`$timestamp` single-key wrapper objects for
the three types plain JSON has no native representation for; every other
JSON type — string, int, bool, null — rides bare). **This family's own
documented extension of that convention**: a typed-NULL column is authored
as `{"$decimal": null}` / `{"$date": null}` / `{"$timestamp": null}` — tag
present, inner value `null` — rather than a bare JSON `null`. This is
needed (and safe to add locally, per A-6's "never shared code" license)
because row-check grammar validation (K9) requires a real column FAMILY
for every column the check's `expr` references, even on the one row that
supplies a NULL for it; a bare `null` carries no type information at all,
while `{"$decimal": null}` still names the column's declared family
(`numeric`) while leaving its value genuinely NULL — exactly the shape the
three-valued-law vectors (`row-three-valued.json`) need. A bare JSON
`null` is used instead wherever the column's family does not matter to
grammar validation (a membership check has no `expr`/no family typing at
all, §4.2 — `membership.json`'s NULL-key vectors use bare `null`).

**Evaluation is the REAL production interpreter, end to end**
(`frames.business_checks.compile_business_checks` + `evaluate` — §7.1/§7.2,
the same functions `stages/post_check.py` composes) — never a
reimplementation of the pass/fail rule. Every vector's `check` is wrapped
as the SOLE authored check of a synthetic single-check `ChecksModel` bound
against a `FactSchemaModel` inferred from `input`'s own column shapes (a
`domain_id` column is always present and non-null in every vector, so the
framework-reserved implicit check — entry 0, D-6 — never fires and never
confounds the verdict under test); `admitted_candidates().count() == 1`
means `"pass"`, `business_violations().count() == 1` means `"fail"` — the
two are complementary by construction (§8.1's own count-identity law), so
either is a sufficient verdict projection.

**[DS-5] synthetic-only**: every value below is fabricated for this suite
alone — no partner-delivery or lake content of any kind.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spine.core.model import ChecksModel, FactSchemaModel, MembershipCheckModel, RowCheckModel
from spine.frames import business_checks as bc

_FIXTURES_DIR = Path(__file__).resolve().parents[3] / "contracts" / "fixtures" / "check-verdicts"

_FACT_TYPE = "vec"


def _untag(raw: Any) -> tuple[DataType, str, Any]:
    """This file's own untagging parser (module docstring). Returns
    `(spark_dtype, fact_column_kind, python_value)`."""
    if isinstance(raw, dict):
        (tag, inner) = next(iter(raw.items()))
        if tag == "$decimal":
            return (
                DecimalType(20, 6),
                "decimal(20,6)",
                Decimal(inner) if inner is not None else None,
            )
        if tag == "$date":
            return DateType(), "date", (date.fromisoformat(inner) if inner is not None else None)
        if tag == "$timestamp":
            return (
                TimestampType(),
                "timestamp",
                datetime.fromisoformat(inner) if inner is not None else None,
            )
        raise ValueError(f"check-verdicts fixture: unknown tag {tag!r}")
    if isinstance(raw, bool):
        return BooleanType(), "bool", raw
    if isinstance(raw, int):
        return LongType(), "long", raw
    if isinstance(raw, str) or raw is None:
        return StringType(), "string", raw
    raise ValueError(f"check-verdicts fixture: cannot untag {raw!r}")


def _row_schema_and_values(row: dict[str, Any]) -> tuple[FactSchemaModel, StructType, tuple]:
    columns, fields, values = [], [], []
    for name, raw in row.items():
        dtype, kind, value = _untag(raw)
        columns.append({"name": name, "type": kind})
        fields.append(StructField(name, dtype, True))
        values.append(value)
    schema = FactSchemaModel(
        columns=columns, domain_id_col="domain_id", record_key=["domain_id"], ordering=[]
    )
    return schema, StructType(fields), tuple(values)


def _evaluate_row_vector(spark: SparkSession, check_json: dict, row: dict[str, Any]) -> str:
    check = RowCheckModel(**check_json)
    schema, struct, values = _row_schema_and_values(row)
    compiled = bc.compile_business_checks(ChecksModel(checks=[check]), _FACT_TYPE, schema)
    df = spark.createDataFrame([values], struct)
    evaluated = bc.evaluate(df, compiled, {})
    admitted = bc.admitted_candidates(evaluated).count()
    violated = bc.business_violations(evaluated).count()
    assert admitted + violated == 1, "exactly one of admitted/violated per single-row vector"
    return "pass" if admitted == 1 else "fail"


def _evaluate_membership_vector(
    spark: SparkSession, check_json: dict, row: dict[str, Any], ref_rows: list[list[Any]]
) -> str:
    check = MembershipCheckModel(**check_json)
    schema, struct, values = _row_schema_and_values(row)
    compiled = bc.compile_business_checks(ChecksModel(checks=[check]), _FACT_TYPE, schema)
    df = spark.createDataFrame([values], struct)

    ref_fields = []
    for i, rc in enumerate(check.ref_columns):
        dtype, _kind, _v = _untag(ref_rows[0][i])
        ref_fields.append(StructField(rc, dtype, True))
    ref_struct = StructType(ref_fields)
    ref_values = [tuple(_untag(v)[2] for v in r) for r in ref_rows]
    ref_df = spark.createDataFrame(ref_values, ref_struct)

    evaluated = bc.evaluate(df, compiled, {check.co_effect: ref_df})
    admitted = bc.admitted_candidates(evaluated).count()
    violated = bc.business_violations(evaluated).count()
    assert admitted + violated == 1, "exactly one of admitted/violated per single-row vector"
    return "pass" if admitted == 1 else "fail"


def _load_vectors() -> list[tuple[str, dict[str, Any]]]:
    vectors: list[tuple[str, dict[str, Any]]] = []
    for path in sorted(_FIXTURES_DIR.glob("*.json")):
        for entry in json.loads(path.read_text(encoding="utf-8")):
            vectors.append((path.name, entry))
    return vectors


_VECTORS = _load_vectors()


def test_check_verdict_fixtures_exist() -> None:
    # Zero-cases guard, same convention as test_canonical.py's own.
    assert _FIXTURES_DIR.is_dir()
    assert _VECTORS


@pytest.mark.parametrize(
    "filename,entry", _VECTORS, ids=[f"{fn}#{i}" for i, (fn, _) in enumerate(_VECTORS)]
)
def test_check_verdicts_reproduce_every_committed_vector(
    spark: SparkSession, filename: str, entry: dict[str, Any]
) -> None:
    """G-10: "the spine evaluates every committed vector" — the REAL
    `business_checks.compile_business_checks`/`evaluate` interpreter,
    exercised against every committed `(check, input, verdict)` triple."""
    check_json = entry["check"]
    row = entry["input"]["row"]
    ref_rows = entry["input"].get("ref_rows")
    if check_json["kind"] == "row":
        actual = _evaluate_row_vector(spark, check_json, row)
    elif check_json["kind"] == "membership":
        assert ref_rows is not None, f"{filename}: membership vector missing ref_rows"
        actual = _evaluate_membership_vector(spark, check_json, row, ref_rows)
    else:
        raise AssertionError(f"{filename}: unsupported check kind {check_json['kind']!r} for G-10")
    assert actual == entry["verdict"], f"{filename}: expected {entry['verdict']!r}, got {actual!r}"


# --- coverage bullets named at §13.3, asserted directly (not just implied
# by "some vector somewhere happens to exercise it") ---------------------


def test_coverage_includes_three_valued_null_expr_passes() -> None:
    entries = [
        e
        for _, e in _VECTORS
        if "-tv" in e["check"]["id"] or e["check"]["id"].startswith("known-status-tv")
    ]
    assert entries, "expected at least one three-valued (NULL expr -> pass) vector"
    assert all(e["verdict"] == "pass" for e in entries)


def test_coverage_includes_membership_null_key_passes() -> None:
    entries = [
        e
        for _, e in _VECTORS
        if e["check"]["kind"] == "membership"
        and any(v is None for v in e["input"]["row"].values() if e["input"]["row"])
    ]
    assert entries, "expected at least one membership vector with a NULL key column"
    assert all(e["verdict"] == "pass" for e in entries)


def test_coverage_includes_a_decimal_comparison() -> None:
    entries = [
        e
        for _, e in _VECTORS
        if e["check"]["kind"] == "row"
        and any(isinstance(v, dict) and "$decimal" in v for v in e["input"]["row"].values())
    ]
    assert len(entries) >= 2, "expected decimal-comparison coverage (pass and fail)"
    assert {e["verdict"] for e in entries} == {"pass", "fail"}


def test_coverage_includes_temporal_functions() -> None:
    entries = [
        e
        for _, e in _VECTORS
        if e["check"]["kind"] == "row"
        and any(fn in e["check"]["expr"] for fn in ("datediff", "year", "date_add"))
    ]
    assert len(entries) >= 3, "expected temporal-function coverage"
    assert {e["verdict"] for e in entries} == {"pass", "fail"}
