"""Unit tests for `spine.core.naming` — LLD §5, §7.7, I-22, [S-11].

Covers: `slug`/`unslug` round-trip over the grammar plus an injection corpus
(path traversal, `--` tricks, unicode); `qualified` composing `spine_cat.
<db>.<table>` and rejecting a malformed table; `execution_name`/
`rerun_execution_name`/`is_rerun_execution_name` (§5's `--rN` grammar,
structurally disjoint from routed `batch_id`s, I-13); `check_object_uris`
accept/reject corpus (I-22), including BOTH M0 `delivery-registered`
fixtures, forged-feed/forged-delivery/forged-bucket rejections, and the
path-traversal-as-suffix corpus (security HIGH, conveyer-nvh.46) -- every
suffix past a genuinely-matching prefix must still be exactly one clean
object-name segment.
"""

import json
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from spine.core import naming

_FIXTURES_DIR = (
    Path(__file__).resolve().parents[3]
    / "contracts"
    / "fixtures"
    / "events"
    / "delivery-registered"
)

_U5 = str(uuid.uuid5(uuid.NAMESPACE_DNS, "batch"))

# --- slug/unslug: examples ----------------------------------------------------


def test_slug_replaces_slash_with_double_dash() -> None:
    assert naming.slug("pipelines/commissions") == "pipelines--commissions"


def test_slug_handles_multi_segment_pipeline() -> None:
    assert naming.slug("a/b/c") == "a--b--c"


def test_unslug_is_slugs_inverse() -> None:
    assert naming.unslug("pipelines--commissions") == "pipelines/commissions"


@pytest.mark.parametrize(
    "bad_pipeline",
    [
        "a--b",  # "--" inside a segment
        "A/b",  # uppercase
        "-a/b",  # segment can't start with "-"
        "a/-b",
        "a//b",  # empty segment
        "",  # empty pipeline
        "a/",  # trailing slash -> empty segment
        "/a",  # leading slash -> empty segment
        "a_b/c",  # underscore not in grammar
        "a/b ",  # trailing space
        "../../etc/passwd",  # path traversal attempt
        "a/../b",
        "héllo/world",  # unicode
        "a/b\x00c",  # embedded null byte
        "😀/pipeline",  # emoji
    ],
)
def test_slug_rejects_non_conforming_pipeline(bad_pipeline: str) -> None:
    with pytest.raises(ValueError, match="pipeline must be"):
        naming.slug(bad_pipeline)


# --- slug/unslug: property-based round trip + injection corpus [S-11] -------

_ALPHANUM = "abcdefghijklmnopqrstuvwxyz0123456789"


@st.composite
def _segment(draw: st.DrawFn) -> str:
    first = draw(st.sampled_from(_ALPHANUM))
    length = draw(st.integers(min_value=0, max_value=12))
    out = [first]
    prev_dash = False
    for _ in range(length):
        alphabet = _ALPHANUM if prev_dash else _ALPHANUM + "-"
        ch = draw(st.sampled_from(alphabet))
        out.append(ch)
        prev_dash = ch == "-"
    return "".join(out)


@st.composite
def _pipeline(draw: st.DrawFn) -> str:
    n_segments = draw(st.integers(min_value=1, max_value=4))
    segments = [draw(_segment()) for _ in range(n_segments)]
    return "/".join(segments)


@given(pipeline=_pipeline())
@settings(max_examples=300)
def test_unslug_slug_round_trips_over_the_grammar(pipeline: str) -> None:
    assert naming.unslug(naming.slug(pipeline)) == pipeline


@given(pipeline=_pipeline())
@settings(max_examples=300)
def test_slug_is_injective_no_stray_double_dash_from_a_single_slash(pipeline: str) -> None:
    # every "--" in the output marks exactly one "/" in the input -- i.e. the
    # slugged form has exactly as many "--" occurrences as the input has "/"
    slugged = naming.slug(pipeline)
    assert slugged.count("--") == pipeline.count("/")


# --- qualified ----------------------------------------------------------------


def test_qualified_prefixes_spine_cat() -> None:
    assert naming.qualified("lake.commissions__facts") == "spine_cat.lake.commissions__facts"


def test_qualified_rejects_bare_unqualified_table() -> None:
    with pytest.raises(ValueError):
        naming.qualified("no_dot_here")


def test_qualified_rejects_invalid_identifier_component() -> None:
    with pytest.raises(ValueError):
        naming.qualified("lake.bad table")


def test_check_qualified_table_accepts_a_component_at_exactly_the_length_cap() -> None:
    # F-6 (security gate `wf_c9aadeb2-8eb`, LOW): 255 is the AWS Glue Data
    # Catalog table/database name limit -- exactly at the cap still admits.
    value = f"lake.{'a' * 255}"
    assert naming.check_qualified_table(value) == value


def test_check_qualified_table_rejects_a_component_one_over_the_length_cap() -> None:
    with pytest.raises(ValueError, match="exceed 255 chars"):
        naming.check_qualified_table(f"lake.{'a' * 256}")


def test_check_qualified_table_rejects_an_over_long_database_component_too() -> None:
    # Every dot-component is checked, not just the trailing one.
    with pytest.raises(ValueError, match="exceed 255 chars"):
        naming.check_qualified_table(f"{'a' * 256}.facts")


# --- execution_name / rerun grammar -------------------------------------------


def test_execution_name_is_the_batch_id_exactly() -> None:
    assert naming.execution_name(_U5) == _U5


def test_execution_name_rejects_non_uuidv5() -> None:
    with pytest.raises(ValueError):
        naming.execution_name(str(uuid.uuid4()))


def test_rerun_execution_name_appends_r_n() -> None:
    assert naming.rerun_execution_name(_U5, 1) == f"{_U5}--r1"
    assert naming.rerun_execution_name(_U5, 42) == f"{_U5}--r42"


def test_rerun_execution_name_rejects_non_uuidv5_batch_id() -> None:
    with pytest.raises(ValueError):
        naming.rerun_execution_name(str(uuid.uuid4()), 1)


def test_rerun_execution_name_rejects_non_positive_n() -> None:
    with pytest.raises(ValueError):
        naming.rerun_execution_name(_U5, 0)
    with pytest.raises(ValueError):
        naming.rerun_execution_name(_U5, -1)


def test_is_rerun_execution_name_true_for_rerun_shape() -> None:
    assert naming.is_rerun_execution_name(naming.rerun_execution_name(_U5, 3)) is True


def test_is_rerun_execution_name_false_for_plain_batch_id() -> None:
    # I-13: a routed batch_id (UUIDv5) can never itself look rerun-shaped
    assert naming.is_rerun_execution_name(naming.execution_name(_U5)) is False


def test_is_rerun_execution_name_false_for_arbitrary_string() -> None:
    assert naming.is_rerun_execution_name("not-a-rerun-name") is False


# --- check_object_uris: fixtures + forged corpus (I-22) -----------------------


def _load_fixtures() -> Iterator[dict[str, Any]]:
    for path in sorted(_FIXTURES_DIR.glob("*.json")):
        yield json.loads(path.read_text())


@pytest.mark.parametrize("fixture", list(_load_fixtures()), ids=lambda f: f["delivery_id"])
def test_check_object_uris_accepts_both_m0_fixtures(fixture: dict[str, Any]) -> None:
    naming.check_object_uris(
        feed_id=fixture["feed_id"],
        delivery_id=fixture["delivery_id"],
        received_at=datetime.fromisoformat(fixture["received_at"]),
        object_uris=fixture["object_uris"],
        landing_bucket="conveyer-dev-lake",
    )


def _minimal_fixture() -> dict[str, Any]:
    return json.loads((_FIXTURES_DIR / "v1-minimal.json").read_text())


def test_check_object_uris_rejects_forged_feed_id() -> None:
    fixture = _minimal_fixture()
    with pytest.raises(ValueError, match="I-22"):
        naming.check_object_uris(
            feed_id="carrier-z/other-feed",
            delivery_id=fixture["delivery_id"],
            received_at=datetime.fromisoformat(fixture["received_at"]),
            object_uris=fixture["object_uris"],
            landing_bucket="conveyer-dev-lake",
        )


def test_check_object_uris_rejects_forged_delivery_id() -> None:
    fixture = _minimal_fixture()
    with pytest.raises(ValueError, match="I-22"):
        naming.check_object_uris(
            feed_id=fixture["feed_id"],
            delivery_id="ffffffff-ffff-4fff-8fff-ffffffffffff",
            received_at=datetime.fromisoformat(fixture["received_at"]),
            object_uris=fixture["object_uris"],
            landing_bucket="conveyer-dev-lake",
        )


def test_check_object_uris_rejects_wrong_landing_bucket() -> None:
    fixture = _minimal_fixture()
    with pytest.raises(ValueError, match="I-22"):
        naming.check_object_uris(
            feed_id=fixture["feed_id"],
            delivery_id=fixture["delivery_id"],
            received_at=datetime.fromisoformat(fixture["received_at"]),
            object_uris=fixture["object_uris"],
            landing_bucket="some-other-bucket",
        )


def test_check_object_uris_rejects_uri_with_no_name_past_prefix() -> None:
    fixture = _minimal_fixture()
    received_at = datetime.fromisoformat(fixture["received_at"])
    prefix_only = fixture["object_uris"][0].rsplit("/", 1)[0] + "/"
    with pytest.raises(ValueError, match="I-22"):
        naming.check_object_uris(
            feed_id=fixture["feed_id"],
            delivery_id=fixture["delivery_id"],
            received_at=received_at,
            object_uris=[prefix_only],
            landing_bucket="conveyer-dev-lake",
        )


def test_check_object_uris_accepts_same_instant_different_offset_received_at() -> None:
    # self-consistency is instant equality, not string-format coincidence
    fixture = _minimal_fixture()
    received_at = datetime.fromisoformat(fixture["received_at"])
    shifted = received_at.astimezone(timezone(timedelta(hours=2)))
    naming.check_object_uris(
        feed_id=fixture["feed_id"],
        delivery_id=fixture["delivery_id"],
        received_at=shifted,
        object_uris=fixture["object_uris"],
        landing_bucket="conveyer-dev-lake",
    )


def test_check_object_uris_rejects_wrong_received_at() -> None:
    fixture = _minimal_fixture()
    wrong_time = datetime(2099, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="I-22"):
        naming.check_object_uris(
            feed_id=fixture["feed_id"],
            delivery_id=fixture["delivery_id"],
            received_at=wrong_time,
            object_uris=fixture["object_uris"],
            landing_bucket="conveyer-dev-lake",
        )


# --- `_format_received_at` overflow guard (conveyer-azr.25, the azr.24 -----
# arithmetic pre-check idiom applied here too): an aware `received_at` at/
# near `datetime.min`/`datetime.max` with a UTC offset that walks the
# instant outside `[MINYEAR, MAXYEAR]` must reject via `ValueError`, never
# propagate `.astimezone(UTC)`'s bare `OverflowError` -- exercised through
# the public `check_object_uris` boundary (mirrors `test_canonical.py`'s own
# choice to test `_timestamp_str`'s identical guard through the public
# `canonical_json`, never the private helper directly).


def test_check_object_uris_rejects_received_at_overflowing_past_minyear() -> None:
    # An aware value AT `datetime.min` with a positive UTC offset walks the
    # UTC-converted instant below MINYEAR (there is no year 0).
    dt = datetime(1, 1, 1, 0, 0, tzinfo=timezone(timedelta(hours=5)))
    with pytest.raises(ValueError, match="out of representable range"):
        naming.check_object_uris(
            feed_id="carrier-a/feed-1",
            delivery_id=_U5,
            received_at=dt,
            object_uris=[],
            landing_bucket="conveyer-dev-lake",
        )


def test_check_object_uris_rejects_received_at_overflowing_past_maxyear() -> None:
    # Symmetric edge: an aware value AT `datetime.max` with a negative UTC
    # offset walks the UTC-converted instant above MAXYEAR.
    dt = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=timezone(timedelta(hours=-5)))
    with pytest.raises(ValueError, match="out of representable range"):
        naming.check_object_uris(
            feed_id="carrier-a/feed-1",
            delivery_id=_U5,
            received_at=dt,
            object_uris=[],
            landing_bucket="conveyer-dev-lake",
        )


def test_check_object_uris_accepts_received_at_exactly_at_the_representable_boundary() -> None:
    # The exact-microsecond boundary (offset consuming the remaining span
    # EXACTLY) must NOT reject -- the pre-check's `>`/`<` (not `>=`/`<=`)
    # comparisons are the load-bearing detail (conveyer-azr.24's own
    # kernel-verified boundary case, reapplied here).
    offset = timedelta(hours=2)
    naive = datetime.min + offset
    dt = naive.replace(tzinfo=timezone(offset))
    naming.check_object_uris(
        feed_id="carrier-a/feed-1",
        delivery_id=_U5,
        received_at=dt,
        object_uris=[],
        landing_bucket="conveyer-dev-lake",
    )


def test_check_object_uris_rejects_one_bad_uri_among_many() -> None:
    fixture = json.loads((_FIXTURES_DIR / "v1-multi-object.json").read_text())
    received_at = datetime.fromisoformat(fixture["received_at"])
    tampered = [*fixture["object_uris"], "s3://conveyer-dev-lake/some/other/path/x.csv"]
    with pytest.raises(ValueError, match="I-22"):
        naming.check_object_uris(
            feed_id=fixture["feed_id"],
            delivery_id=fixture["delivery_id"],
            received_at=received_at,
            object_uris=tampered,
            landing_bucket="conveyer-dev-lake",
        )


# --- check_object_uris: path-traversal-as-suffix corpus (security HIGH, ------
# conveyer-nvh.46) -- the prefix match alone is not sufficient; everything
# past a genuinely-matching prefix must be exactly ONE clean object-name
# segment. Each case below builds a URI with a REAL matching prefix (from
# the `v1-minimal` fixture) and only varies the suffix, so a pass here would
# mean the traversal defect, not a forged prefix, let the URI through.


def _uri_with_suffix(fixture: dict[str, Any], suffix: str) -> str:
    prefix = fixture["object_uris"][0].rsplit("/", 1)[0] + "/"
    return prefix + suffix


@pytest.mark.parametrize(
    "suffix",
    [
        "..",
        "../x",
        "../../incoming/attacker.csv",
        ".",
        "./x",
        "a/b",
        "a/b/c",
        "",
        "%2e%2e/x",
        "%2e%2e%2fattacker.csv",
        "a%2fb",
        "a%5cb",
        "statement-2026-07.csv/",
        "a\\b",
        "..\\x",
    ],
    ids=[
        "dotdot",
        "dotdot-slash-x",
        "dotdot-traversal-to-incoming",
        "dot",
        "dot-slash-x",
        "multi-segment",
        "multi-segment-3",
        "empty",
        "percent-encoded-dotdot",
        "percent-encoded-dotdot-slash",
        "percent-encoded-slash",
        "percent-encoded-backslash",
        "trailing-slash",
        "backslash-segment",
        "backslash-traversal",
    ],
)
def test_check_object_uris_rejects_traversal_suffixes(suffix: str) -> None:
    fixture = _minimal_fixture()
    received_at = datetime.fromisoformat(fixture["received_at"])
    with pytest.raises(ValueError, match="I-22"):
        naming.check_object_uris(
            feed_id=fixture["feed_id"],
            delivery_id=fixture["delivery_id"],
            received_at=received_at,
            object_uris=[_uri_with_suffix(fixture, suffix)],
            landing_bucket="conveyer-dev-lake",
        )


def test_check_object_uris_accepts_single_clean_object_name() -> None:
    fixture = _minimal_fixture()
    received_at = datetime.fromisoformat(fixture["received_at"])
    naming.check_object_uris(
        feed_id=fixture["feed_id"],
        delivery_id=fixture["delivery_id"],
        received_at=received_at,
        object_uris=[_uri_with_suffix(fixture, "some-other-clean-name.csv")],
        landing_bucket="conveyer-dev-lake",
    )


# --- `table_slug`/`markers_table`/`table_class_inventory_uri` (007.1 §6.3/
# §6.5, bead conveyer-6pg.18, B7) -- identifier-grammar-safe table naming,
# deliberately DIFFERENT from `slug()` (see `table_slug`'s own docstring for
# why `slug()`'s "--"-joined form must never compose an actual identifier).


def test_table_slug_takes_the_trailing_segment() -> None:
    assert naming.table_slug("pipelines/identity") == "identity"


def test_table_slug_is_a_noop_for_a_single_segment_pipeline() -> None:
    assert naming.table_slug("commissions") == "commissions"


def test_table_slug_takes_the_last_segment_of_a_deeper_path() -> None:
    assert naming.table_slug("a/b/c") == "c"


def test_table_slug_never_contains_a_dash_separator_for_the_multi_segment_case() -> None:
    # The whole point: unlike `slug()`, no NEW "--" is ever introduced by
    # the join -- `table_slug`'s output is always a legal `check_qualified_
    # table` identifier component for a pipeline whose trailing segment
    # itself contains no hyphen.
    assert "--" not in naming.table_slug("pipelines/identity")


def test_table_slug_rejects_a_malformed_pipeline() -> None:
    with pytest.raises(ValueError):
        naming.table_slug("Bad/Pipeline")


def test_markers_table_derives_db_from_raw_table_and_slug_from_pipeline() -> None:
    assert (
        naming.markers_table("conveyer_dev_lake.identity__raw", "pipelines/identity")
        == "conveyer_dev_lake.identity__markers"
    )


def test_markers_table_output_is_a_legal_qualified_table_identifier() -> None:
    result = naming.markers_table("conveyer_dev_lake.identity__raw", "pipelines/identity")
    assert naming.check_qualified_table(result) == result  # does not raise


def test_markers_table_rejects_a_malformed_raw_table() -> None:
    with pytest.raises(ValueError):
        naming.markers_table("not-a-qualified-table", "pipelines/identity")


def test_table_class_inventory_uri_replaces_the_spec_filename() -> None:
    assert (
        naming.table_class_inventory_uri(
            "s3://some-artifacts-bucket/spine/specs/pipelines--identity/pipeline.yaml"
        )
        == "s3://some-artifacts-bucket/spine/specs/pipelines--identity/table-classes.json"
    )


def test_table_class_inventory_uri_works_for_file_scheme() -> None:
    assert (
        naming.table_class_inventory_uri("file:///tmp/specs/pipelines--identity/pipeline.yaml")
        == "file:///tmp/specs/pipelines--identity/table-classes.json"
    )


def test_commit_completion_sentinel_is_outside_the_identifier_grammar() -> None:
    # 007.1 §6.3 answer 1: "by grammar, not by convention" -- proven here
    # against the SAME `check_qualified_table` a real table name must pass,
    # not a re-derived regex.
    with pytest.raises(ValueError):
        naming.check_qualified_table(f"lake.{naming.COMMIT_COMPLETION_SENTINEL}")
