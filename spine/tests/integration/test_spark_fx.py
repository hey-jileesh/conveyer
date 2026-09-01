"""`effects/spark.py` — reads, guards, one-commit append, `MERGE INTO`. LLD
§7.6, I-3, I-4, I-6, I-11, I-19, [S-6][T-4][T-6][T-9][T-10][T-19].

Uses `local_runner_fx` (the REAL production assembly, `effects.build.
make_runner_fx`, over the session's `spine_cat` Hadoop catalog) for every fx
call under test — no parallel test-only builder. `unique_table` returns a
fully-qualified `spine_cat.<db>.<table>` identifier; every `RunnerFx`
callable takes the BARE `<db>.<table>` form (`core.naming.qualified`
reconstructs the qualified name internally, matching `CoEffectDecl.table`'s
own documented shape) — `_bare` strips the fixture's `spine_cat.` prefix once
per table, so every call site below reads as "the fx call under test," not
"prefix-stripping boilerplate."

The n2-reader section (bead conveyer-azr.16, 005.1 §5; wired into `RunnerFx`/
`SparkFx` by bead conveyer-azr.19, n3-admission-cut) is the one exception to
"every fx call under test goes through `local_runner_fx`": its own tests
still invoke `_build_read_objects_admission` directly against the shared
`spark` session (three-arg `(object_uris, ReadSpecModel, RawContractModel)`
signature, §5.8) rather than through `local_runner_fx.read_objects` — a
deliberate choice kept even now that it IS the wired implementation, since
these tests want a bare `ReadSpecModel`/`RawContractModel` per case, not a
full `PipelineSpecModel`/`BatchContext`. The provisional I-P1 reader
(`_build_read_objects`, CSV/UTF-8/header/FAILFAST) this section used to sit
alongside is deleted (n3-admission-cut) — its own tests are gone with it.
"""

from __future__ import annotations

import dataclasses
import gzip
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from py4j.protocol import Py4JJavaError
from pydantic import ValidationError
from pyspark.sql import SparkSession

# `tests/` has no `__init__.py` anywhere (deliberate, see `tests/conftest.py`'s
# own docstring on per-subdir collision-avoidance): pytest's default
# "prepend" import mode puts THIS file's own directory (`tests/integration/`)
# on `sys.path`, so a sibling module in the same directory is a bare
# top-level import, never a `tests.integration.snapshot_asserts` dotted path.
from snapshot_asserts import (
    assert_no_new_snapshot,
    assert_stamped_batch,
    snapshot_delta,
    snapshot_ids,
)
from spine.core.merge import MergeSpec
from spine.core.model import (
    ColumnSpec,
    DialectModel,
    PipelineSpecModel,
    RawContractModel,
    ReadSpecModel,
)
from spine.effects import build
from spine.effects import spark as spark_fx
from spine.effects.records import MergeResult, RunnerFx, TransientError

_CATALOG_PREFIX = "spine_cat."


def _bare(qualified_table: str) -> str:
    assert qualified_table.startswith(_CATALOG_PREFIX)
    return qualified_table.removeprefix(_CATALOG_PREFIX)


def _create_raw_like_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(f"CREATE TABLE {qualified_table} (id STRING, batch_id STRING) USING iceberg")


def _create_quarantine_like_table(spark: SparkSession, qualified_table: str) -> None:
    spark.sql(
        f"CREATE TABLE {qualified_table} "
        "(id STRING, batch_id STRING, check_stage STRING) USING iceberg"
    )


def _create_state_table(spark: SparkSession, qualified_table: str) -> None:
    # write.merge.mode=merge-on-read: see effects/spark.py's module docstring
    # -- this bead's own empirical finding on why COW-mode MERGE INTO can
    # never report a logical no-op via summary inspection.
    spark.sql(
        f"CREATE TABLE {qualified_table} "
        "(domain_id STRING, event_time TIMESTAMP, source_ts TIMESTAMP, "
        "content_hash STRING, payload STRING) USING iceberg "
        "TBLPROPERTIES ('write.merge.mode'='merge-on-read')"
    )


# --- n2-reader (005.1 §5): _build_read_objects_admission (wired into ------
# `RunnerFx`/`SparkFx`, bead conveyer-azr.19, n3-admission-cut) --------------
#
# The real reader (A-1, A-3, A-4) -- every test below builds it directly via
# `spark_fx._build_read_objects_admission(spark)` (a bare `ReadSpecModel`/
# `RawContractModel` per case), never `local_runner_fx` (module docstring).
# Fixtures (§12.5's reader-level slice) are committed plaintext under
# `tests/exemplar/identity/fixtures/reader/`; gzip variants are generated at
# test setup (`_gzip_bytes`) -- no binaries in git.

_READER_FIXTURES_DIR = (
    Path(__file__).resolve().parent.parent / "exemplar" / "identity" / "fixtures" / "reader"
)


def _reader_fixture_text(name: str) -> str:
    return (_READER_FIXTURES_DIR / name).read_text(encoding="utf-8")


def _gzip_bytes(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"))


def _contract(*columns: ColumnSpec) -> RawContractModel:
    return RawContractModel(columns=list(columns))


def _read_spec(
    *, compression: str = "none", skip_leading_lines: int = 0, **dialect_kw: object
) -> ReadSpecModel:
    return ReadSpecModel(
        compression=compression,
        dialect=DialectModel(format="csv", **dialect_kw),  # type: ignore[arg-type]
        skip_leading_lines=skip_leading_lines,
    )


def test_read_objects_admission_clean_multi_object_with_locators_and_extras(
    spark: SparkSession, tmp_path: Path
) -> None:
    read_objects = spark_fx._build_read_objects_admission(spark)
    path_a = tmp_path / "clean_object_a.csv"
    path_a.write_text(_reader_fixture_text("clean_object_a.csv"))
    path_b = tmp_path / "clean_object_b.csv"
    path_b.write_text(_reader_fixture_text("clean_object_b.csv"))
    contract = _contract(
        ColumnSpec(name="domain_id", required=True, nullable=False), ColumnSpec(name="payload")
    )

    df = read_objects((str(path_a), str(path_b)), _read_spec(), contract)

    assert df.schema.fieldNames() == [
        "source_uri",
        "object_seq",
        "row_index",
        "malformed_text",
        "domain_id",
        "payload",
        "extras",
    ]
    rows = sorted(
        (
            r["object_seq"],
            r["row_index"],
            r["source_uri"],
            r["domain_id"],
            r["payload"],
            dict(r["extras"]),
        )
        for r in df.collect()
    )
    # codepoint-sorted object_uris (A-3): "clean_object_a.csv" < "..._b.csv"
    assert rows == [
        (1, 1, str(path_a), "id-001", "alpha", {"extra_col": "zeta"}),
        (1, 2, str(path_a), "id-002", "bravo", {"extra_col": "yankee"}),
        (2, 1, str(path_b), "id-003", "charlie", {}),
    ]


def test_read_objects_admission_captures_malformed_rows_verbatim(
    spark: SparkSession, tmp_path: Path
) -> None:
    """§5.3/§5.4: unterminated quote, a ragged (too-many-fields) row, and
    junk-after-closing-quote all shape identically -- `malformed_text` the
    decoded line verbatim, every declared column NULL, `extras = {}`."""
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "malformed_rows.csv"
    path.write_text(_reader_fixture_text("malformed_rows.csv"))
    contract = _contract(ColumnSpec(name="domain_id"), ColumnSpec(name="payload"))

    df = read_objects((str(path),), _read_spec(), contract)

    rows = {
        r["row_index"]: (r["malformed_text"], r["domain_id"], r["payload"], dict(r["extras"]))
        for r in df.collect()
    }
    assert rows[1] == (None, "id-1", "ok", {})
    assert rows[2] == ('id-2,"unterminated', None, None, {})
    assert rows[3] == ("id-3,too,many,fields", None, None, {})
    assert rows[4] == ('id-4,"abc"junk,x', None, None, {})


def test_read_objects_admission_gzip_variant_with_bom_and_skip_leading_lines(
    spark: SparkSession, tmp_path: Path
) -> None:
    """The gzip variant is generated HERE, at test setup, from the committed
    plaintext fixture (§12.5) -- no `.gz` binary in git."""
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "bom_preamble.csv.gz"
    path.write_bytes(_gzip_bytes(_reader_fixture_text("bom_preamble.csv")))
    contract = _contract(ColumnSpec(name="domain_id"), ColumnSpec(name="payload"))

    df = read_objects((str(path),), _read_spec(compression="gzip", skip_leading_lines=1), contract)

    rows = sorted((r["row_index"], r["domain_id"], r["payload"]) for r in df.collect())
    assert rows == [(1, "id-1", "alpha"), (2, "id-2", "bravo")]


# --- A-06: BOM strip, skip_leading_lines, row_index/physical-line mapping ---


def test_read_objects_admission_bom_stripped_and_skipped_lines_never_become_records(
    spark: SparkSession, tmp_path: Path
) -> None:
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "bom_preamble.csv"
    path.write_text(_reader_fixture_text("bom_preamble.csv"))
    contract = _contract(ColumnSpec(name="domain_id"), ColumnSpec(name="payload"))

    df = read_objects((str(path),), _read_spec(skip_leading_lines=1), contract)

    # If the leading U+FEFF had NOT been stripped, "domain_id" would carry
    # it and required-column-missing would raise before this point -- a
    # stronger proof than inspecting the header token directly.
    rows = sorted((r["row_index"], r["domain_id"], r["payload"]) for r in df.collect())
    assert rows == [(1, "id-1", "alpha"), (2, "id-2", "bravo")]


def test_read_objects_admission_row_index_maps_to_physical_line_per_a3_formula(
    spark: SparkSession, tmp_path: Path
) -> None:
    """A-3: `physical_line = skip_leading_lines + header + row_index`. Lines
    (0-based): 0=preamble (skipped), 1=header, 2=row_index 1, 3=blank (no
    record, no ordinal), 4=row_index 2."""
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "mapping.csv"
    path.write_text("PREAMBLE\ndomain_id,payload\nid-1,a\n\nid-2,b\n")
    contract = _contract(ColumnSpec(name="domain_id"), ColumnSpec(name="payload"))

    df = read_objects((str(path),), _read_spec(skip_leading_lines=1), contract)

    rows = sorted((r["row_index"], r["domain_id"], r["payload"]) for r in df.collect())
    assert rows == [(1, "id-1", "a"), (2, "id-2", "b")]


# --- A-12: header:false positional binding -----------------------------


def test_read_objects_admission_header_false_positional_binding(
    spark: SparkSession, tmp_path: Path
) -> None:
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "headerless.csv"
    path.write_text("id-1,alpha\nid-2,bravo\n")
    contract = _contract(ColumnSpec(name="domain_id"), ColumnSpec(name="payload"))

    df = read_objects((str(path),), _read_spec(header=False), contract)

    rows = sorted(
        (r["row_index"], r["domain_id"], r["payload"], dict(r["extras"])) for r in df.collect()
    )
    assert rows == [(1, "id-1", "alpha", {}), (2, "id-2", "bravo", {})]


def test_read_objects_admission_header_false_short_and_long_rows_are_malformed(
    spark: SparkSession, tmp_path: Path
) -> None:
    """[DC-2]: `extras` is always `{}` for headerless feeds -- longer AND
    shorter rows are both malformed against the declared width, never
    positionally re-bound or padded."""
    read_objects = spark_fx._build_read_objects_admission(spark)
    contract = _contract(ColumnSpec(name="domain_id"), ColumnSpec(name="payload"))
    short_path = tmp_path / "short.csv"
    short_path.write_text("id-1\n")
    long_path = tmp_path / "long.csv"
    long_path.write_text("id-2,alpha,extra\n")

    short_row = read_objects((str(short_path),), _read_spec(header=False), contract).collect()[0]
    long_row = read_objects((str(long_path),), _read_spec(header=False), contract).collect()[0]

    assert (short_row["malformed_text"], short_row["domain_id"], short_row["payload"]) == (
        "id-1",
        None,
        None,
    )
    assert dict(short_row["extras"]) == {}
    assert (long_row["malformed_text"], long_row["domain_id"], long_row["payload"]) == (
        "id-2,alpha,extra",
        None,
        None,
    )
    assert dict(long_row["extras"]) == {}


def test_header_false_contract_with_a_required_column_is_rejected_at_spec_parse() -> None:
    """A-12: the cross-model validator built in n0-spec-migration (`core/
    model.py::PipelineSpecModel._check_header_false_forbids_required_
    columns`) -- re-asserted at the reader-frame level per this bead's
    brief (§14: "a `header: false` contract declaring any `required` column
    is a spec-parse defect")."""
    with pytest.raises(ValidationError, match=r"required:true column.*header: false"):
        PipelineSpecModel(
            pipeline="pipelines/reader-probe",
            transforms_module="pipelines.reader_probe.transforms",
            raw_table="db.reader_probe__raw",
            quarantine_table="db.reader_probe__quarantine",
            # 006.1 P-1: singular fact_table/state_table replaced by a
            # per-type `fact_types` mapping -- this fixture just needs SOME
            # valid declaration, not to exercise fact-type semantics.
            fact_types={
                "probe": {
                    "fact_table": "db.reader_probe__facts",
                    "state_table": "db.reader_probe__state",
                    "schema": {
                        "columns": [{"name": "domain_id", "type": "string"}],
                        "domain_id_col": "domain_id",
                        "record_key": ["domain_id"],
                    },
                }
            },
            fold="default-lww",
            domain_id_col="domain_id",
            co_effects={},
            serialize=False,
            read=_read_spec(header=False),
            raw_contract=_contract(ColumnSpec(name="domain_id", required=True, nullable=False)),
            sla_minutes=60,
        )


# --- A-13: multiline: true -- embedded newline, unterminated-quote cost ----


def test_read_objects_admission_multiline_embedded_newline_parses_as_one_record(
    spark: SparkSession, tmp_path: Path
) -> None:
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "multiline.csv"
    path.write_text('domain_id,payload\nid-1,"hello\nworld"\nid-2,plain\n')
    contract = _contract(ColumnSpec(name="domain_id"), ColumnSpec(name="payload"))

    df = read_objects((str(path),), _read_spec(multiline=True), contract)

    rows = sorted((r["row_index"], r["domain_id"], r["payload"]) for r in df.collect())
    assert rows == [(1, "id-1", "hello\nworld"), (2, "id-2", "plain")]


def test_read_objects_admission_multiline_unterminated_quote_consumes_remainder(
    spark: SparkSession, tmp_path: Path
) -> None:
    """§5.5's declared trade-off: an unterminated quote consumes the file's
    remainder into ONE field, rather than raising (unlike the `multiline:
    false` per-line path's `strict=True`)."""
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "unterminated_multiline.csv"
    path.write_text('domain_id,payload\nid-1,"hello\nid-2,plain\n')
    contract = _contract(ColumnSpec(name="domain_id"), ColumnSpec(name="payload"))

    df = read_objects((str(path),), _read_spec(multiline=True), contract)

    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["row_index"] == 1
    assert rows[0]["domain_id"] == "id-1"
    assert rows[0]["payload"] == "hello\nid-2,plain\n"


# --- §5.7 tier-1 defects: exact A-10 grammar, no URIs/filenames ([S-8]) ----


def test_read_objects_admission_undecodable_object_defect(
    spark: SparkSession, tmp_path: Path
) -> None:
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "corrupt.csv.gz"
    path.write_bytes(b"not a real gzip stream")
    contract = _contract(ColumnSpec(name="domain_id"))

    with pytest.raises(ValueError) as exc_info:
        read_objects((str(path),), _read_spec(compression="gzip"), contract)

    message = str(exc_info.value)
    assert message == "admission-defect/undecodable-object: object_seq=1"
    assert str(path) not in message
    assert path.name not in message
    # conveyer-azr.32/S-8: the underlying Py4JJavaError's text embeds the
    # full JVM stack, including this object's URI -- `from None` must
    # suppress it from the raised exception's `__cause__`, not just keep it
    # out of the message string (a chained cause still rides an unhandled
    # traceback into driver/executor logs).
    assert exc_info.value.__cause__ is None


def test_read_objects_admission_duplicate_header_column_defect(
    spark: SparkSession, tmp_path: Path
) -> None:
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "dup.csv"
    path.write_text("domain_id,domain_id,payload\n1,2,3\n")
    contract = _contract(ColumnSpec(name="domain_id"), ColumnSpec(name="payload"))

    with pytest.raises(ValueError) as exc_info:
        read_objects((str(path),), _read_spec(), contract)

    message = str(exc_info.value)
    assert message == (
        "admission-defect/duplicate-header-column: object_seq=1 positions=[0, 1] name='domain_id'"
    )
    assert str(path) not in message
    assert path.name not in message


def test_read_objects_admission_duplicate_header_column_defect_hides_undeclared_token(
    spark: SparkSession, tmp_path: Path
) -> None:
    """[DC-8]: an undeclared duplicated token's own text never appears in
    the raised message, only its positions -- a mis-uploaded partner file's
    first-line-is-data cell value must never ride the ledger's
    `error_message`."""
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "dup_undeclared.csv"
    path.write_text("domain_id,mystery,mystery\n1,a,b\n")
    contract = _contract(ColumnSpec(name="domain_id"))

    with pytest.raises(ValueError) as exc_info:
        read_objects((str(path),), _read_spec(), contract)

    message = str(exc_info.value)
    assert message == "admission-defect/duplicate-header-column: object_seq=1 positions=[1, 2]"
    assert "mystery" not in message


def test_read_objects_admission_required_column_missing_defect(
    spark: SparkSession, tmp_path: Path
) -> None:
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "missing.csv"
    path.write_text("payload\nfoo\n")
    contract = _contract(
        ColumnSpec(name="domain_id", required=True, nullable=False), ColumnSpec(name="payload")
    )

    with pytest.raises(ValueError) as exc_info:
        read_objects((str(path),), _read_spec(), contract)

    message = str(exc_info.value)
    assert message == "admission-defect/required-column-missing: object_seq=1 columns=['domain_id']"
    assert str(path) not in message
    assert path.name not in message


def test_read_objects_admission_compression_extension_mismatch_defect(
    spark: SparkSession, tmp_path: Path
) -> None:
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "plain.csv"
    path.write_text("domain_id\n1\n")
    contract = _contract(ColumnSpec(name="domain_id"))

    with pytest.raises(ValueError) as exc_info:
        read_objects((str(path),), _read_spec(compression="gzip"), contract)

    message = str(exc_info.value)
    assert message == "admission-defect/compression-extension-mismatch: object_seq=1"
    assert str(path) not in message
    assert path.name not in message


def test_read_objects_admission_compression_extension_mismatch_reverse_direction(
    spark: SparkSession, tmp_path: Path
) -> None:
    """`compression: none` forbids every Hadoop-actionable compression
    extension, not just `.gz` -- the reverse direction from the previous
    test (declared `gzip` against a plain file)."""
    read_objects = spark_fx._build_read_objects_admission(spark)
    path = tmp_path / "actually_plain.csv.gz"
    path.write_bytes(_gzip_bytes("domain_id\n1\n"))
    contract = _contract(ColumnSpec(name="domain_id"))

    with pytest.raises(ValueError) as exc_info:
        read_objects((str(path),), _read_spec(compression="none"), contract)

    message = str(exc_info.value)
    assert message == "admission-defect/compression-extension-mismatch: object_seq=1"
    assert str(path) not in message


def test_reserved_ladder_value_defect_at_spec_parse() -> None:
    """§5.7's fifth code, `reserved-ladder-value`, is raised at `ReadSpecModel`
    parse (A-2), never by the reader itself -- `zstd` can never construct a
    `ReadSpecModel` for `read_objects` to be called with."""
    with pytest.raises(ValidationError) as exc_info:
        ReadSpecModel(compression="zstd", dialect=DialectModel(format="csv"))

    assert "admission-defect/reserved-ladder-value: compression='zstd'" in str(exc_info.value)


# --- Property test: locator determinism (A-3, §12.4) ------------------------

_ROW_COUNTS_PER_OBJECT = st.lists(st.integers(min_value=0, max_value=4), min_size=1, max_size=3)


@given(row_counts=_ROW_COUNTS_PER_OBJECT)
@settings(max_examples=10, deadline=None)
def test_read_objects_admission_locator_determinism(
    tmp_path_factory: pytest.TempPathFactory, spark: SparkSession, row_counts: list[int]
) -> None:
    """Same object(s), read twice, MUST assign identical `(object_seq,
    row_index)` locators both times (A-3) -- varying object count and
    per-object row count via hypothesis, `tmp_path_factory` (session-scoped)
    rather than the function-scoped `tmp_path` fixture, since each example
    needs its own fresh directory."""
    read_objects = spark_fx._build_read_objects_admission(spark)
    base = tmp_path_factory.mktemp("locator_determinism")
    uris = []
    for object_index, row_count in enumerate(row_counts):
        path = base / f"object_{object_index}.csv"
        lines = ["domain_id,payload"] + [f"id-{object_index}-{i},v{i}" for i in range(row_count)]
        path.write_text("\n".join(lines) + "\n")
        uris.append(str(path))
    contract = _contract(ColumnSpec(name="domain_id"), ColumnSpec(name="payload"))

    first = read_objects(tuple(uris), _read_spec(), contract)
    second = read_objects(tuple(uris), _read_spec(), contract)

    locators_first = sorted((r["object_seq"], r["row_index"]) for r in first.collect())
    locators_second = sorted((r["object_seq"], r["row_index"]) for r in second.collect())
    assert locators_first == locators_second


# --- read_table: pinned read (I-6), zero-snapshot sentinel [T-19] -----------


def test_read_table_zero_snapshot_returns_sentinel(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("zero_snapshot")
    _create_raw_like_table(spark, qt)

    df, sid = local_runner_fx.read_table(_bare(qt))

    assert sid == -1
    assert df.count() == 0


def test_read_table_resolves_current_snapshot_id(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("current_snapshot")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)

    df, sid = local_runner_fx.read_table(bare)

    assert sid == spark_fx._current_snapshot_id(spark, qt)
    assert sorted(r["id"] for r in df.collect()) == ["1"]


def test_read_table_pin_is_perceived_under_a_concurrent_append(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    """I-6: a pinned read must NOT see a sibling commit that lands after the
    read was pinned, even though Spark's plan is lazy and the actual scan
    executes later (`.count()` below, well after the second append)."""
    qt = unique_table("pin_under_concurrent_append")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)
    sid_at_pin = spark_fx._current_snapshot_id(spark, qt)

    df, sid = local_runner_fx.read_table(bare)
    assert sid == sid_at_pin

    # a sibling commit lands AFTER the pin was taken, BEFORE the pinned df's
    # own lazy plan is ever executed
    sibling = spark.createDataFrame([("2", "b2")], ["id", "batch_id"])
    local_runner_fx.append(bare, sibling, "b2", None)

    assert df.count() == 1  # still just the pre-pin row -- recorded == perceived
    assert sorted(r["id"] for r in df.collect()) == ["1"]
    # the CURRENT (unpinned) table now has both rows -- proves the second
    # append really did commit, so the assertion above is meaningful
    assert spark.table(qt).count() == 2
    assert spark_fx._current_snapshot_id(spark, qt) != sid_at_pin


# --- read_batch: current-snapshot, column-object batch_id predicate [S-6] ---


def test_read_batch_filters_to_the_named_batch_only(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("read_batch")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    batch_a = spark.createDataFrame([("1", "A"), ("2", "A")], ["id", "batch_id"])
    batch_b = spark.createDataFrame([("3", "B")], ["id", "batch_id"])
    local_runner_fx.append(bare, batch_a, "A", None)
    local_runner_fx.append(bare, batch_b, "B", None)

    df = local_runner_fx.read_batch(bare, "A")

    assert sorted(r["id"] for r in df.collect()) == ["1", "2"]


# --- table_has_batch: I-3 guard, incl. quarantine stage_key disambiguation --


def test_table_has_batch_true_and_false_on_real_data(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("guard")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "present")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "present", None)

    assert local_runner_fx.table_has_batch(bare, "present", None) is True
    assert local_runner_fx.table_has_batch(bare, "absent", None) is False


def test_table_has_batch_stage_key_disambiguates_quarantine_substreams(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("quarantine_guard")
    _create_quarantine_like_table(spark, qt)
    bare = _bare(qt)
    df = spark.createDataFrame([("1", "b1", "pre_check")], ["id", "batch_id", "check_stage"])
    local_runner_fx.append(bare, df, "b1", "pre_check")

    assert local_runner_fx.table_has_batch(bare, "b1", "pre_check") is True
    assert local_runner_fx.table_has_batch(bare, "b1", "post_check") is False
    assert local_runner_fx.table_has_batch(bare, "other_batch", "pre_check") is False


# --- append: summary added-records + stamps present; one-commit (R-13) -----


def test_append_returns_added_records_and_stamped_summary(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("append_summary")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    df = spark.createDataFrame([("1", "b1"), ("2", "b1")], ["id", "batch_id"])

    rows_appended, summary = local_runner_fx.append(bare, df, "b1", None)

    assert rows_appended == 2
    assert_stamped_batch(summary, "b1")


def test_append_stamps_stage_key_when_given(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("append_stage_stamp")
    _create_quarantine_like_table(spark, qt)
    bare = _bare(qt)
    df = spark.createDataFrame([("1", "b1", "post_check")], ["id", "batch_id", "check_stage"])

    _rows, summary = local_runner_fx.append(bare, df, "b1", "post_check")

    assert_stamped_batch(summary, "b1", "post_check")


def test_append_repartitions_before_write_when_shuffle_partitions_configured(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    """`RunConfig.repartition_before_write=True` + an explicit
    `shuffle_partitions` together fire the repartition-before-write branch
    (a coalesce/repartition changes file layout, never row content)."""
    qt = unique_table("append_repartition")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    run_config_json = json.dumps({"repartition_before_write": True, "shuffle_partitions": 2})
    config = dataclasses.replace(local_runner_fx.config, run_config_json=run_config_json)
    fx = build.make_runner_fx(spark, config)
    df = spark.createDataFrame([("1", "b1"), ("2", "b1"), ("3", "b1")], ["id", "batch_id"])

    rows_appended, summary = fx.append(bare, df, "b1", None)

    assert rows_appended == 3
    assert sorted(r["id"] for r in spark.table(qt).collect()) == ["1", "2", "3"]
    assert_stamped_batch(summary, "b1")


def test_append_is_exactly_one_commit_r13_harness_self_test(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("one_commit")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    before = snapshot_ids(spark, qt)

    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)

    new_id, summary = snapshot_delta(spark, qt, before)
    assert_stamped_batch(summary, "b1")
    assert summary.get("added-records") == "1"
    assert new_id == spark_fx._current_snapshot_id(spark, qt)


def test_snapshot_delta_harness_raises_on_zero_new_snapshots(
    spark: SparkSession, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("harness_zero")
    _create_raw_like_table(spark, qt)
    before = snapshot_ids(spark, qt)

    with pytest.raises(AssertionError, match="one-commit invariant"):
        snapshot_delta(spark, qt, before)  # nothing committed since `before`


def test_assert_no_new_snapshot_passes_on_a_true_guard_skip(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("guard_skip_zero_commits")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)

    before = snapshot_ids(spark, qt)
    if local_runner_fx.table_has_batch(bare, "b1", None):
        pass  # the stage would skip its append entirely -- no fx.append call happens
    assert_no_new_snapshot(spark, qt, before)


# --- resolve_batch_snapshot: hit / miss / stage-filtered (I-19, lineage) ----


def test_resolve_batch_snapshot_hit(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("resolve_hit")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)

    sid = local_runner_fx.resolve_batch_snapshot(bare, "b1", None)

    assert sid == spark_fx._current_snapshot_id(spark, qt)


def test_resolve_batch_snapshot_miss_returns_none(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("resolve_miss")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    seed = spark.createDataFrame([("1", "b1")], ["id", "batch_id"])
    local_runner_fx.append(bare, seed, "b1", None)

    assert local_runner_fx.resolve_batch_snapshot(bare, "never-written", None) is None


def test_resolve_batch_snapshot_stage_filtered(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("resolve_stage_filtered")
    _create_quarantine_like_table(spark, qt)
    bare = _bare(qt)
    local_runner_fx.append(
        bare,
        spark.createDataFrame([("1", "b1", "pre_check")], ["id", "batch_id", "check_stage"]),
        "b1",
        "pre_check",
    )
    local_runner_fx.append(
        bare,
        spark.createDataFrame([("2", "b1", "post_check")], ["id", "batch_id", "check_stage"]),
        "b1",
        "post_check",
    )

    pre_sid = local_runner_fx.resolve_batch_snapshot(bare, "b1", "pre_check")
    post_sid = local_runner_fx.resolve_batch_snapshot(bare, "b1", "post_check")

    assert pre_sid is not None
    assert post_sid is not None
    assert pre_sid != post_sid


# --- merge: fresh insert, ordering-conditional update, no-op, unique-child --


def _row(domain_id: str, dt: datetime, content_hash: str, payload: str) -> tuple[str, ...]:
    return (domain_id, dt, dt, content_hash, payload)


_T1 = datetime(2024, 1, 1, tzinfo=UTC)
_T2 = datetime(2024, 1, 2, tzinfo=UTC)

_STATE_COLS = ["domain_id", "event_time", "source_ts", "content_hash", "payload"]


def test_merge_fresh_insert(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("merge_insert")
    _create_state_table(spark, qt)
    spec = MergeSpec(
        target_table=_bare(qt),
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    source = spark.createDataFrame([_row("a", _T1, "h1", "new-a")], _STATE_COLS)

    result = local_runner_fx.merge(spec, source)

    assert result.snapshot_id is not None
    assert result.summary is not None
    rows = spark.table(qt).collect()
    assert [(r["domain_id"], r["payload"]) for r in rows] == [("a", "new-a")]


def test_merge_ordering_conditional_update_newer_wins(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("merge_newer_wins")
    _create_state_table(spark, qt)
    spark.createDataFrame([_row("a", _T1, "h1", "old-a")], _STATE_COLS).writeTo(qt).append()
    spec = MergeSpec(
        target_table=_bare(qt),
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    source = spark.createDataFrame([_row("a", _T2, "h2", "new-a")], _STATE_COLS)

    result = local_runner_fx.merge(spec, source)

    assert result.snapshot_id is not None
    rows = spark.table(qt).collect()
    assert [(r["domain_id"], r["payload"]) for r in rows] == [("a", "new-a")]


def test_merge_older_loses_and_ties_are_no_op(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("merge_older_and_tie")
    _create_state_table(spark, qt)
    spark.createDataFrame([_row("a", _T2, "h2", "current-a")], _STATE_COLS).writeTo(qt).append()
    spec = MergeSpec(
        target_table=_bare(qt),
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    older_source = spark.createDataFrame([_row("a", _T1, "h1", "older-a")], _STATE_COLS)

    older_result = local_runner_fx.merge(spec, older_source)

    assert older_result == MergeResult(None, None)
    rows = spark.table(qt).collect()
    assert [(r["domain_id"], r["payload"]) for r in rows] == [("a", "current-a")]

    tie_source = spark.createDataFrame([_row("a", _T2, "h2", "current-a")], _STATE_COLS)
    tie_result = local_runner_fx.merge(spec, tie_source)

    assert tie_result == MergeResult(None, None)


def test_merge_unique_child_of_before_id(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("merge_unique_child")
    _create_state_table(spark, qt)
    spec = MergeSpec(
        target_table=_bare(qt),
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    before_id = spark_fx._current_snapshot_id(spark, qt)
    assert before_id is None  # zero-snapshot state table, pre-capture

    source = spark.createDataFrame([_row("a", _T1, "h1", "a")], _STATE_COLS)
    result = local_runner_fx.merge(spec, source)

    assert result.snapshot_id is not None
    assert result.snapshot_id != before_id
    assert result.snapshot_id == spark_fx._current_snapshot_id(spark, qt)


# --- nvh.40 [F1]: own-commit attribution under a concurrent sibling fold ---
#
# `_merge_race_probe` is `merge`'s one test seam (module docstring): a
# no-op in production, monkeypatched here to commit a SIBLING write to the
# same state table at a named point around our own `MERGE INTO` --
# reproducing, deterministically and single-threaded, the two races this
# bead's own scratch probe found empirically against a real local Iceberg
# table (see `spine/effects/spark.py`'s module docstring for the full
# account).


def test_merge_survives_a_sibling_commit_between_our_commit_and_resolution(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sibling committing AFTER our own `MERGE INTO` succeeds, BEFORE
    own-commit resolution runs, must never have its snapshot id/summary
    attributed to our batch -- unique-child-of-`before_id` survives this
    race (unlike the original "read current after MERGE" implementation,
    which would have returned the sibling's snapshot here)."""
    qt = unique_table("merge_race_post_commit")
    _create_state_table(spark, qt)
    bare = _bare(qt)
    spec = MergeSpec(
        target_table=bare,
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    spark.createDataFrame([_row("a", _T1, "h1", "seed-a")], _STATE_COLS).writeTo(qt).append()

    def fake_probe(probe_spark: SparkSession, probe_qt: str, point: str) -> None:
        if point == spark_fx._MERGE_POST_COMMIT:
            probe_spark.createDataFrame([_row("b", _T1, "h1", "sibling-b")], _STATE_COLS).writeTo(
                probe_qt
            ).append()

    monkeypatch.setattr(spark_fx, "_merge_race_probe", fake_probe)

    source = spark.createDataFrame([_row("a", _T2, "h2", "our-update-a")], _STATE_COLS)
    result = local_runner_fx.merge(spec, source)

    assert result.attributable is True
    assert result.snapshot_id is not None
    assert result.summary is not None
    # "current" (after both our commit AND the sibling's) must NOT be what
    # we attributed to ourselves -- proves the fix isn't accidentally
    # degenerating back to reading "current".
    assert result.snapshot_id != spark_fx._current_snapshot_id(spark, qt)
    rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(qt).collect())
    assert rows == [("a", "our-update-a"), ("b", "sibling-b")]  # both commits really landed


def test_merge_reports_unattributable_when_a_sibling_commits_before_our_statement_executes(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sibling that commits BEFORE our own `MERGE INTO` executes (using
    the same `before_id` as its own base -- Spark's MERGE reads *live*
    table state, never a value pinned at `before_id`-capture time) shifts
    our own eventual commit's parent off `before_id`; chain topology alone
    cannot then tell the sibling's commit apart from ours. Caught instead
    by the pre-commit base-shift check -- reported as an explicit
    unattributable result, never a fabricated (wrong) attribution, and
    never conflated with a logical no-op (real rows DID merge)."""
    qt = unique_table("merge_race_pre_commit")
    _create_state_table(spark, qt)
    bare = _bare(qt)
    spec = MergeSpec(
        target_table=bare,
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    spark.createDataFrame([_row("a", _T1, "h1", "seed-a")], _STATE_COLS).writeTo(qt).append()

    def fake_probe(probe_spark: SparkSession, probe_qt: str, point: str) -> None:
        if point == spark_fx._MERGE_PRE_COMMIT:
            probe_spark.createDataFrame([_row("b", _T1, "h1", "sibling-b")], _STATE_COLS).writeTo(
                probe_qt
            ).append()

    monkeypatch.setattr(spark_fx, "_merge_race_probe", fake_probe)

    source = spark.createDataFrame([_row("a", _T2, "h2", "our-update-a")], _STATE_COLS)
    result = local_runner_fx.merge(spec, source)

    assert result == MergeResult(None, None, attributable=False)
    # our own MERGE still genuinely ran and converged the state table --
    # unattributable is about NAMING the commit, not about it not happening.
    rows = sorted((r["domain_id"], r["payload"]) for r in spark.table(qt).collect())
    assert rows == [("a", "our-update-a"), ("b", "sibling-b")]


def test_merge_unattributable_path_is_distinguishable_from_a_logical_no_op(
    spark: SparkSession,
    local_runner_fx: RunnerFx,
    unique_table: Callable[[str], str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard against conflating the two `(None, None)` states
    (`MergeResult`'s own docstring): a genuine no-op (`test_merge_older_
    loses_and_ties_are_no_op`) and an unattributable race must remain
    distinguishable via `attributable`, not identical values."""
    qt = unique_table("merge_race_vs_no_op")
    _create_state_table(spark, qt)
    bare = _bare(qt)
    spec = MergeSpec(
        target_table=bare,
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload"),
    )
    spark.createDataFrame([_row("a", _T2, "h2", "current-a")], _STATE_COLS).writeTo(qt).append()
    older_source = spark.createDataFrame([_row("a", _T1, "h1", "older-a")], _STATE_COLS)
    no_op_result = local_runner_fx.merge(spec, older_source)
    assert no_op_result == MergeResult(None, None)
    assert no_op_result.attributable is True

    def fake_probe(probe_spark: SparkSession, probe_qt: str, point: str) -> None:
        if point == spark_fx._MERGE_PRE_COMMIT:
            probe_spark.createDataFrame([_row("c", _T1, "h1", "sibling-c")], _STATE_COLS).writeTo(
                probe_qt
            ).append()

    monkeypatch.setattr(spark_fx, "_merge_race_probe", fake_probe)
    fresh_source = spark.createDataFrame([_row("a", _T2, "h2", "newer-again-a")], _STATE_COLS)
    unattributable_result = local_runner_fx.merge(spec, fresh_source)

    assert unattributable_result.attributable is False
    assert unattributable_result != no_op_result  # same (None, None) shape, different meaning
    assert unattributable_result == MergeResult(None, None, attributable=False)


# --- CommitFailed/Validation/CommitStateUnknown -> TransientError [T-10] ----
#
# A genuine concurrent-commit conflict needs a second live writer racing the
# same table -- impractical from one local[2] JVM. Two complementary checks
# instead: (1) the exported predicate directly, against a `Py4JJavaError`
# built with a duck-typed fake `java_exception` (real `Py4JJavaError.
# __init__` only touches `._target_id`; no live py4j gateway needed -- see
# effects/spark.py's module docstring); (2) `append`/`merge` really do let a
# NON-matching real local failure (a genuine schema mismatch) propagate
# UNTOUCHED, proving the "else raise" branch isn't accidentally swallowing
# everything.


class _FakeJavaClass:
    def __init__(self, name: str) -> None:
        self._name = name

    def getName(self) -> str:
        return self._name


class _FakeJavaException:
    def __init__(self, name: str) -> None:
        self._name = name
        self._target_id = "o1"  # Py4JJavaError.__init__ needs this attribute

    def getClass(self) -> _FakeJavaClass:
        return _FakeJavaClass(self._name)


@pytest.mark.parametrize(
    "class_name",
    [
        "org.apache.iceberg.exceptions.CommitFailedException",
        "org.apache.iceberg.exceptions.CommitStateUnknownException",
        "org.apache.iceberg.exceptions.ValidationException",
    ],
)
def test_is_transient_iceberg_failure_true_for_the_three_mapped_exceptions(
    class_name: str,
) -> None:
    fake_exc = Py4JJavaError("An error occurred", _FakeJavaException(class_name))
    assert spark_fx.is_transient_iceberg_failure(fake_exc) is True


@pytest.mark.parametrize(
    "class_name",
    [
        "org.apache.spark.sql.AnalysisException",
        "java.lang.IllegalStateException",
        "org.apache.iceberg.exceptions.NoSuchTableException",
    ],
)
def test_is_transient_iceberg_failure_false_for_other_exceptions(class_name: str) -> None:
    fake_exc = Py4JJavaError("An error occurred", _FakeJavaException(class_name))
    assert spark_fx.is_transient_iceberg_failure(fake_exc) is False


def test_is_transient_iceberg_failure_false_for_a_plain_python_exception() -> None:
    assert spark_fx.is_transient_iceberg_failure(ValueError("boom")) is False


def test_append_reraises_a_non_transient_failure_untouched(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("append_non_transient")
    _create_raw_like_table(spark, qt)
    bare = _bare(qt)
    # a genuine, locally-reproducible failure that is NOT one of the three
    # mapped Iceberg exceptions: the target table has no `extra_col` column.
    bad_df = spark.createDataFrame([("1", "b1", "x")], ["id", "batch_id", "extra_col"])

    with pytest.raises(Exception) as exc_info:
        local_runner_fx.append(bare, bad_df, "b1", None)
    assert not isinstance(exc_info.value, TransientError)


def test_merge_reraises_a_non_transient_failure_untouched(
    spark: SparkSession, local_runner_fx: RunnerFx, unique_table: Callable[[str], str]
) -> None:
    qt = unique_table("merge_non_transient")
    _create_state_table(spark, qt)
    spec = MergeSpec(
        target_table=_bare(qt),
        key_cols=("domain_id",),
        ordering_cols=("event_time", "source_ts", "content_hash"),
        update_cols=("event_time", "source_ts", "content_hash", "payload", "extra_col"),
    )
    # `extra_col` is neither in the state table's schema nor the source's --
    # a genuine, locally-reproducible AnalysisException, not a transient one.
    source = spark.createDataFrame([_row("a", _T1, "h1", "a")], _STATE_COLS)

    with pytest.raises(Exception) as exc_info:
        local_runner_fx.merge(spec, source)
    assert not isinstance(exc_info.value, TransientError)
