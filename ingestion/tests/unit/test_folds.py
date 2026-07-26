"""Unit tests for `ingestion.core.folds` — LLD §7.4.

Covers: the fold-idempotence hypothesis property (§12.3), the disposition
rank tiebreak, and the set semantics of `acquired_final`/`observed_defective`
(superseded stays acquired; manifest-mode excluded from defective).
"""

from datetime import UTC, datetime, timedelta

from hypothesis import given, settings
from hypothesis import strategies as st
from ingestion.core import folds
from ingestion.core.model import DeliveryObject, DeliveryRecord

_DISPOSITIONS = ("registered", "duplicate", "superseded", "incomplete", "unreadable")


def _make_row(
    delivery_id: str,
    disposition: str,
    recorded_at: datetime,
    *,
    received_at: datetime | None = None,
    delivery_key: str = "k1",
    objects: list[DeliveryObject] | None = None,
    completeness_mode: str = "trailer",
    feed_id: str = "src/feed",
    supersedes: str | None = None,
) -> DeliveryRecord:
    if objects is None:
        objects = [
            DeliveryObject(name="a.csv", role="data", uri="s3://x/a.csv", bytes=10, sha256="a" * 64)
        ]
    is_content_bearing = disposition in ("registered", "duplicate", "superseded")
    return DeliveryRecord(
        delivery_id=delivery_id,
        feed_id=feed_id,
        delivery_key=delivery_key,
        batch_id="b1" if is_content_bearing else None,
        content_hash="sha256:" + "a" * 64 if is_content_bearing else None,
        size_bytes=10 if is_content_bearing else None,
        object_uris=[o.uri for o in objects if o.role == "data" and o.uri is not None],
        objects=objects,
        manifest_ref=None,
        asserted_record_count=None,
        completeness_mode=completeness_mode,  # type: ignore[arg-type]
        received_at=received_at or recorded_at,
        recorded_at=recorded_at,
        disposition=disposition,  # type: ignore[arg-type]
        supersedes=supersedes,
        driver="sftp-pull",
        driver_run_id="run1",
        notes=None,
    )


# --- latest_dispositions -----------------------------------------------------


def test_latest_dispositions_keeps_max_recorded_at_per_delivery_id() -> None:
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    t1 = t0 + timedelta(hours=1)
    older = _make_row("d1", "incomplete", t0)
    newer = _make_row("d1", "registered", t1)
    result = folds.latest_dispositions([older, newer])
    assert result == {"d1": newer}


def test_latest_dispositions_rank_tiebreak_on_exact_recorded_at() -> None:
    """LLD §7.4: rank breaks EXACT recorded_at ties deterministically —
    unreadable > superseded > duplicate > incomplete > registered."""
    t = datetime(2026, 1, 1, tzinfo=UTC)
    rows_by_rank = [
        _make_row("d1", "registered", t),
        _make_row("d1", "incomplete", t),
        _make_row("d1", "duplicate", t),
        _make_row("d1", "superseded", t),
        _make_row("d1", "unreadable", t),
    ]
    # unreadable should win regardless of list order.
    for ordering in (rows_by_rank, list(reversed(rows_by_rank))):
        result = folds.latest_dispositions(ordering)
        assert result["d1"].disposition == "unreadable"


def test_latest_dispositions_rank_order_is_total() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    expected_order = ["registered", "incomplete", "duplicate", "superseded", "unreadable"]
    for i in range(len(expected_order) - 1):
        lower = _make_row("d1", expected_order[i], t)
        higher = _make_row("d1", expected_order[i + 1], t)
        result = folds.latest_dispositions([lower, higher])
        assert result["d1"].disposition == expected_order[i + 1]


def test_latest_dispositions_groups_independently_by_delivery_id() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    d1 = _make_row("d1", "registered", t)
    d2 = _make_row("d2", "incomplete", t)
    result = folds.latest_dispositions([d1, d2])
    assert set(result) == {"d1", "d2"}


@given(
    rows=st.lists(
        st.builds(
            _make_row,
            delivery_id=st.sampled_from(["d1", "d2", "d3"]),
            disposition=st.sampled_from(_DISPOSITIONS),
            recorded_at=st.builds(
                lambda offset: datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=offset),
                st.integers(min_value=0, max_value=5),
            ),
        ),
        min_size=0,
        max_size=20,
    )
)
@settings(max_examples=100)
def test_latest_dispositions_is_idempotent_under_duplication(rows: list[DeliveryRecord]) -> None:
    """§12.3: `latest_dispositions(rows + rows) == latest_dispositions(rows)`."""
    assert folds.latest_dispositions(rows + rows) == folds.latest_dispositions(rows)


# --- registered_deliveries / feed_watermark -----------------------------------


def test_registered_deliveries_filters_and_sorts() -> None:
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    t1 = t0 + timedelta(days=1)
    reg_late = _make_row("d1", "registered", t1, received_at=t1)
    reg_early = _make_row("d2", "registered", t0, received_at=t0)
    incomplete = _make_row("d3", "incomplete", t0)
    result = folds.registered_deliveries([reg_late, reg_early, incomplete])
    assert [r.delivery_id for r in result] == ["d2", "d1"]


def test_feed_watermark_is_max_received_at_of_registered() -> None:
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    t1 = t0 + timedelta(days=1)
    reg_early = _make_row("d1", "registered", t0, received_at=t0)
    reg_late = _make_row("d2", "registered", t1, received_at=t1)
    assert folds.feed_watermark([reg_early, reg_late]) == t1


def test_feed_watermark_none_when_no_registered_deliveries() -> None:
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    incomplete = _make_row("d1", "incomplete", t0)
    assert folds.feed_watermark([incomplete]) is None


def test_feed_watermark_empty_rows() -> None:
    assert folds.feed_watermark([]) is None


# --- acquired_final ------------------------------------------------------------


def test_acquired_final_includes_registered_duplicate_superseded() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    obj_a = [
        DeliveryObject(name="a.csv", role="data", uri="s3://x/a.csv", bytes=10, sha256="a" * 64)
    ]
    obj_b = [
        DeliveryObject(name="b.csv", role="data", uri="s3://x/b.csv", bytes=20, sha256="b" * 64)
    ]
    obj_c = [
        DeliveryObject(name="c.csv", role="data", uri="s3://x/c.csv", bytes=30, sha256="c" * 64)
    ]
    registered = _make_row("d1", "registered", t, objects=obj_a)
    duplicate = _make_row("d2", "duplicate", t, objects=obj_b)
    superseded = _make_row("d3", "superseded", t, objects=obj_c)
    result = folds.acquired_final([registered, duplicate, superseded])
    assert result == frozenset({("a.csv", 10), ("b.csv", 20), ("c.csv", 30)})


def test_acquired_final_excludes_incomplete_and_unreadable() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    incomplete = _make_row("d1", "incomplete", t)
    unreadable = _make_row("d2", "unreadable", t)
    assert folds.acquired_final([incomplete, unreadable]) == frozenset()


def test_acquired_final_includes_manifest_role_objects() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    objects = [
        DeliveryObject(
            name="part.csv", role="data", uri="s3://x/part.csv", bytes=10, sha256="a" * 64
        ),
        DeliveryObject(
            name="m.json", role="manifest", uri="s3://x/m.json", bytes=5, sha256="b" * 64
        ),
    ]
    registered = _make_row("d1", "registered", t, objects=objects, completeness_mode="manifest")
    result = folds.acquired_final([registered])
    assert result == frozenset({("part.csv", 10), ("m.json", 5)})


def test_acquired_final_reflects_only_latest_disposition() -> None:
    """A delivery that was `incomplete` then later `registered` (new delivery_id,
    but re-using this test's simplification of one delivery_id per attempt)
    is acquired once its LATEST disposition qualifies."""
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    t1 = t0 + timedelta(hours=1)
    obj = [DeliveryObject(name="a.csv", role="data", uri="s3://x/a.csv", bytes=10, sha256="a" * 64)]
    first = _make_row("d1", "incomplete", t0, objects=obj)
    corrected = _make_row("d1", "registered", t1, objects=obj)
    assert folds.acquired_final([first, corrected]) == frozenset({("a.csv", 10)})


# --- observed_defective ---------------------------------------------------------


def test_observed_defective_includes_trailer_and_timer_unreadable_incomplete() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    obj_a = [DeliveryObject(name="a.csv", role="data", uri=None, bytes=10, sha256=None)]
    obj_b = [DeliveryObject(name="b.csv", role="data", uri=None, bytes=20, sha256=None)]
    incomplete = _make_row("d1", "incomplete", t, objects=obj_a, completeness_mode="trailer")
    unreadable = _make_row("d2", "unreadable", t, objects=obj_b, completeness_mode="timer")
    result = folds.observed_defective([incomplete, unreadable])
    assert result == frozenset({("a.csv", 10), ("b.csv", 20)})


def test_observed_defective_excludes_manifest_mode() -> None:
    """LLD §7.4: manifest-mode deliveries are deliberately EXCLUDED — they are
    re-examined manifest-first each run, never via this set."""
    t = datetime(2026, 1, 1, tzinfo=UTC)
    obj = [DeliveryObject(name="m.json", role="manifest", uri=None, bytes=5, sha256=None)]
    incomplete_manifest = _make_row(
        "d1", "incomplete", t, objects=obj, completeness_mode="manifest"
    )
    assert folds.observed_defective([incomplete_manifest]) == frozenset()


def test_observed_defective_excludes_registered_and_duplicate() -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    registered = _make_row("d1", "registered", t)
    duplicate = _make_row("d2", "duplicate", t)
    assert folds.observed_defective([registered, duplicate]) == frozenset()
