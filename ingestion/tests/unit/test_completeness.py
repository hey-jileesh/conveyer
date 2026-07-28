"""Unit tests for `ingestion.core.completeness` — LLD §7.3.

Example-based coverage of every verdict branch named in the LLD:
`parse_manifest` (ManifestV1 | Defect), `evaluate_manifest` (complete /
incomplete / defective, each named cause), `evaluate_trailer` (complete /
incomplete / defective), and `quiet_window_satisfied`.
"""

import json
from datetime import UTC, datetime, timedelta

from ingestion.core import completeness, model

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_FEED_ID = "carrier-x/commission-statements"


def _manifest_json(**overrides: object) -> bytes:
    payload: dict[str, object] = {
        "manifest_version": 1,
        "manifest_id": "m1",
        "feed_id": _FEED_ID,
        "files": [{"name": "a.csv", "bytes": 10, "sha256": _SHA_A}],
    }
    payload.update(overrides)
    return json.dumps(payload).encode("utf-8")


def _manifest(*files: model.ManifestFile, feed_id: str = _FEED_ID) -> model.ManifestV1:
    return model.ManifestV1(
        manifest_version=1, manifest_id="m1", feed_id=feed_id, files=list(files)
    )


# --- parse_manifest -----------------------------------------------------------


def test_parse_manifest_valid_bytes_returns_manifest_v1() -> None:
    result = completeness.parse_manifest(_manifest_json())
    assert isinstance(result, model.ManifestV1)
    assert result.manifest_id == "m1"


def test_parse_manifest_malformed_json_returns_defect() -> None:
    result = completeness.parse_manifest(b"not json at all")
    assert isinstance(result, completeness.Defect)
    assert result.reason  # non-empty, human-readable


def test_parse_manifest_schema_violation_returns_defect() -> None:
    # missing required field `feed_id`
    result = completeness.parse_manifest(
        json.dumps({"manifest_version": 1, "manifest_id": "m1", "files": []}).encode("utf-8")
    )
    assert isinstance(result, completeness.Defect)


def test_parse_manifest_invalid_utf8_returns_defect_not_raise() -> None:
    result = completeness.parse_manifest(b"\xff\xfe not valid utf8 { ")
    assert isinstance(result, completeness.Defect)


# --- H-4 (security-gate): parse_manifest must never leak untrusted manifest --
# content (partner-controlled) into `Defect.reason` -- that reason becomes
# ledger `notes`, append-only and Athena-queryable forever.

_PII_MARKER = "ssn-078-05-1120-jane.doe@example.com"


def test_parse_manifest_schema_violation_never_leaks_field_content_into_reason() -> None:
    # `files.0.bytes` must be an int; feeding it a distinctive PII-shaped
    # string is exactly the leak vector pydantic's default `str(exc)`
    # rendering has (`input_value=<the offending string>`).
    bad = json.dumps(
        {
            "manifest_version": 1,
            "manifest_id": "m1",
            "feed_id": _FEED_ID,
            "files": [{"name": "a.csv", "bytes": _PII_MARKER, "sha256": _SHA_A}],
        }
    ).encode("utf-8")

    result = completeness.parse_manifest(bad)

    assert isinstance(result, completeness.Defect)
    assert _PII_MARKER not in result.reason
    assert "files.0.bytes" in result.reason  # still diagnostic: (loc, type) only
    assert "int_parsing" in result.reason


def test_parse_manifest_reason_is_capped_regardless_of_manifest_size() -> None:
    # A 1000-file manifest (the schema's own cap) where every file is
    # defective would otherwise produce an unbounded `reason` string.
    files = [
        {"name": f"f{i}.csv", "bytes": "not-an-int", "sha256": _SHA_A, "record_count": None}
        for i in range(1000)
    ]
    bad = json.dumps(
        {"manifest_version": 1, "manifest_id": "m1", "feed_id": _FEED_ID, "files": files}
    ).encode("utf-8")

    result = completeness.parse_manifest(bad)

    assert isinstance(result, completeness.Defect)
    assert len(result.reason) < 500
    assert "more violation(s)" in result.reason


# --- evaluate_manifest ---------------------------------------------------------


def test_evaluate_manifest_complete_with_full_record_count() -> None:
    manifest = _manifest(
        model.ManifestFile(name="a.csv", bytes=10, sha256=_SHA_A, record_count=5),
        model.ManifestFile(name="b.csv", bytes=20, sha256=_SHA_B, record_count=7),
    )
    present = [
        completeness.ObjectStat("a.csv", 10, _SHA_A),
        completeness.ObjectStat("b.csv", 20, _SHA_B),
    ]
    result = completeness.evaluate_manifest(manifest, present, _FEED_ID)
    assert result.verdict == "complete"
    assert result.reason is None
    assert result.asserted_record_count == 12
    assert result.data_object_names == ("a.csv", "b.csv")


def test_evaluate_manifest_complete_record_count_none_when_any_file_lacks_it() -> None:
    manifest = _manifest(
        model.ManifestFile(name="a.csv", bytes=10, sha256=_SHA_A, record_count=5),
        model.ManifestFile(name="b.csv", bytes=20, sha256=_SHA_B),  # no record_count
    )
    present = [
        completeness.ObjectStat("a.csv", 10, _SHA_A),
        completeness.ObjectStat("b.csv", 20, _SHA_B),
    ]
    result = completeness.evaluate_manifest(manifest, present, _FEED_ID)
    assert result.verdict == "complete"
    assert result.asserted_record_count is None


def test_evaluate_manifest_incomplete_when_declared_file_absent() -> None:
    manifest = _manifest(model.ManifestFile(name="a.csv", bytes=10, sha256=_SHA_A))
    result = completeness.evaluate_manifest(manifest, [], _FEED_ID)
    assert result.verdict == "incomplete"
    assert result.reason is not None and "a.csv" in result.reason


def test_evaluate_manifest_incomplete_on_byte_size_mismatch() -> None:
    manifest = _manifest(model.ManifestFile(name="a.csv", bytes=10, sha256=_SHA_A))
    present = [completeness.ObjectStat("a.csv", 999, _SHA_A)]
    result = completeness.evaluate_manifest(manifest, present, _FEED_ID)
    assert result.verdict == "incomplete"
    assert result.reason is not None and "byte-size mismatch" in result.reason


def test_evaluate_manifest_defective_on_feed_id_mismatch() -> None:
    manifest = _manifest(
        model.ManifestFile(name="a.csv", bytes=10, sha256=_SHA_A), feed_id="other/feed"
    )
    present = [completeness.ObjectStat("a.csv", 10, _SHA_A)]
    result = completeness.evaluate_manifest(manifest, present, _FEED_ID)
    assert result.verdict == "defective"
    assert result.reason is not None and "feed_id" in result.reason


def test_evaluate_manifest_defective_on_duplicate_names() -> None:
    manifest = _manifest(
        model.ManifestFile(name="a.csv", bytes=10, sha256=_SHA_A),
        model.ManifestFile(name="a.csv", bytes=10, sha256=_SHA_A),
    )
    present = [completeness.ObjectStat("a.csv", 10, _SHA_A)]
    result = completeness.evaluate_manifest(manifest, present, _FEED_ID)
    assert result.verdict == "defective"
    assert result.reason is not None and "duplicate" in result.reason


def test_evaluate_manifest_defective_on_sha256_mismatch_when_observed() -> None:
    manifest = _manifest(model.ManifestFile(name="a.csv", bytes=10, sha256=_SHA_A))
    present = [completeness.ObjectStat("a.csv", 10, "c" * 64)]
    result = completeness.evaluate_manifest(manifest, present, _FEED_ID)
    assert result.verdict == "defective"
    assert result.reason is not None and "sha256 mismatch" in result.reason


def test_evaluate_manifest_sha256_none_observed_is_not_a_defect() -> None:
    # observed sha256 is None (listed, not yet streamed) — never a mismatch trigger.
    manifest = _manifest(model.ManifestFile(name="a.csv", bytes=10, sha256=_SHA_A))
    present = [completeness.ObjectStat("a.csv", 10, None)]
    result = completeness.evaluate_manifest(manifest, present, _FEED_ID)
    assert result.verdict == "complete"


def test_evaluate_manifest_defective_takes_priority_over_incomplete() -> None:
    manifest = _manifest(
        model.ManifestFile(name="a.csv", bytes=10, sha256=_SHA_A),
        model.ManifestFile(name="b.csv", bytes=20, sha256=_SHA_B),
    )
    present = [completeness.ObjectStat("a.csv", 10, "c" * 64)]  # sha mismatch; b.csv absent too
    result = completeness.evaluate_manifest(manifest, present, _FEED_ID)
    assert result.verdict == "defective"


# --- evaluate_trailer -----------------------------------------------------------


def test_evaluate_trailer_complete_with_integer_count() -> None:
    spec = model.TrailerSpec(pattern=r"TRAILER\|(?P<count>\d+)", count_group="count")
    result = completeness.evaluate_trailer("row1\nrow2\nTRAILER|100\n", spec)
    assert result.verdict == "complete"
    assert result.reason is None
    assert result.asserted_record_count == 100


def test_evaluate_trailer_complete_without_count_group_configured() -> None:
    spec = model.TrailerSpec(pattern=r"TRAILER\|END")
    result = completeness.evaluate_trailer("row1\nTRAILER|END\n", spec)
    assert result.verdict == "complete"
    assert result.asserted_record_count is None


def test_evaluate_trailer_incomplete_on_empty_tail() -> None:
    spec = model.TrailerSpec(pattern=r"TRAILER\|(?P<count>\d+)", count_group="count")
    result = completeness.evaluate_trailer("", spec)
    assert result.verdict == "incomplete"
    assert result.reason == "trailer missing or malformed"


def test_evaluate_trailer_incomplete_when_only_blank_lines() -> None:
    spec = model.TrailerSpec(pattern=r"TRAILER\|(?P<count>\d+)", count_group="count")
    result = completeness.evaluate_trailer("\n\n   \n", spec)
    assert result.verdict == "incomplete"


def test_evaluate_trailer_incomplete_when_final_line_does_not_match() -> None:
    spec = model.TrailerSpec(pattern=r"TRAILER\|(?P<count>\d+)", count_group="count")
    result = completeness.evaluate_trailer("row1\nnot a trailer\n", spec)
    assert result.verdict == "incomplete"
    assert result.reason == "trailer missing or malformed"


def test_evaluate_trailer_defective_when_count_group_is_non_integer() -> None:
    spec = model.TrailerSpec(pattern=r"TRAILER\|(?P<count>\w+)", count_group="count")
    result = completeness.evaluate_trailer("row1\nTRAILER|ABC\n", spec)
    assert result.verdict == "defective"
    assert result.reason is not None and "not an integer" in result.reason


def test_evaluate_trailer_ignores_trailing_blank_lines_after_trailer() -> None:
    spec = model.TrailerSpec(pattern=r"TRAILER\|(?P<count>\d+)", count_group="count")
    result = completeness.evaluate_trailer("row1\nTRAILER|55\n\n\n", spec)
    assert result.verdict == "complete"
    assert result.asserted_record_count == 55


def test_evaluate_trailer_handles_crlf_line_endings() -> None:
    spec = model.TrailerSpec(pattern=r"TRAILER\|(?P<count>\d+)", count_group="count")
    result = completeness.evaluate_trailer("row1\r\nTRAILER|77\r\n", spec)
    assert result.verdict == "complete"
    assert result.asserted_record_count == 77


# --- quiet_window_satisfied -----------------------------------------------------


def test_quiet_window_satisfied_when_mtime_well_before_window() -> None:
    now = datetime(2026, 7, 24, 12, 0, 0, tzinfo=UTC)
    assert completeness.quiet_window_satisfied(now - timedelta(minutes=10), now, 5) is True


def test_quiet_window_not_satisfied_when_mtime_too_recent() -> None:
    now = datetime(2026, 7, 24, 12, 0, 0, tzinfo=UTC)
    assert completeness.quiet_window_satisfied(now - timedelta(minutes=3), now, 5) is False


def test_quiet_window_satisfied_at_exact_boundary() -> None:
    now = datetime(2026, 7, 24, 12, 0, 0, tzinfo=UTC)
    assert completeness.quiet_window_satisfied(now - timedelta(minutes=5), now, 5) is True
