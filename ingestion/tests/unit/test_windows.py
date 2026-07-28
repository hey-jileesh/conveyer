"""Unit tests for `ingestion.core.windows` — LLD §7.4.

Covers `select_candidates` (pattern filter, acquired/defective drop, force
bypass, timer quiet-window, sort order) and `select_manifests` (acquired
drop, force bypass, non-final re-examination is implicit — manifests never
enter `acquired` until they are final).
"""

from datetime import UTC, datetime, timedelta

from ingestion.core import windows
from ingestion.core.model import Completeness, TimerSpec, TrailerSpec

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
TRAILER_COMPLETENESS = Completeness(mode="trailer", trailer=TrailerSpec(pattern=".*"))


def _rf(name: str, bytes_: int, mtime: datetime) -> windows.RemoteFile:
    return windows.RemoteFile(name=name, bytes=bytes_, mtime=mtime)


# --- select_candidates: pattern filter -----------------------------------------


def test_select_candidates_filters_by_fnmatch_pattern() -> None:
    listing = [
        _rf("COMM_a.csv", 10, NOW - timedelta(hours=2)),
        _rf("other.txt", 10, NOW - timedelta(hours=2)),
    ]
    result = windows.select_candidates(
        listing, frozenset(), frozenset(), "COMM_*", TRAILER_COMPLETENESS, NOW
    )
    assert [f.name for f in result] == ["COMM_a.csv"]


# --- select_candidates: acquired/defective drop + force ------------------------


def test_select_candidates_drops_acquired_and_defective() -> None:
    listing = [
        _rf("COMM_new.csv", 10, NOW - timedelta(hours=2)),
        _rf("COMM_acquired.csv", 20, NOW - timedelta(hours=2)),
        _rf("COMM_defective.csv", 30, NOW - timedelta(hours=2)),
    ]
    acquired = frozenset({("COMM_acquired.csv", 20)})
    defective = frozenset({("COMM_defective.csv", 30)})
    result = windows.select_candidates(
        listing, acquired, defective, "COMM_*", TRAILER_COMPLETENESS, NOW
    )
    assert [f.name for f in result] == ["COMM_new.csv"]


def test_select_candidates_force_bypasses_acquired_and_defective_only() -> None:
    """D-16: force=True bypasses ONLY step 2 (acquired/defective drop) — the
    pattern filter still applies."""
    listing = [
        _rf("COMM_new.csv", 10, NOW - timedelta(hours=2)),
        _rf("COMM_acquired.csv", 20, NOW - timedelta(hours=2)),
        _rf("other.txt", 10, NOW - timedelta(hours=2)),  # still filtered by pattern
    ]
    acquired = frozenset({("COMM_acquired.csv", 20)})
    result = windows.select_candidates(
        listing, acquired, frozenset(), "COMM_*", TRAILER_COMPLETENESS, NOW, force=True
    )
    assert {f.name for f in result} == {"COMM_new.csv", "COMM_acquired.csv"}


def test_select_candidates_byte_change_is_not_dropped() -> None:
    """A defective-then-fixed file has a DIFFERENT byte count, so it is not
    in the `(name, bytes)` defective set and is naturally re-selected."""
    listing = [_rf("COMM_a.csv", 15, NOW - timedelta(hours=2))]  # fixed: was 10 bytes
    defective = frozenset({("COMM_a.csv", 10)})
    result = windows.select_candidates(
        listing, frozenset(), defective, "COMM_*", TRAILER_COMPLETENESS, NOW
    )
    assert [f.name for f in result] == ["COMM_a.csv"]


# --- select_candidates: timer quiet-window --------------------------------------


def test_select_candidates_timer_mode_quiet_window() -> None:
    timer_completeness = Completeness(
        mode="timer", timer=TimerSpec(quiet_window_minutes=60, accepted_risk="x" * 25)
    )
    listing = [
        _rf("t_fresh.csv", 10, NOW - timedelta(minutes=10)),
        _rf("t_old.csv", 10, NOW - timedelta(minutes=90)),
    ]
    result = windows.select_candidates(
        listing, frozenset(), frozenset(), "t_*", timer_completeness, NOW
    )
    assert [f.name for f in result] == ["t_old.csv"]


def test_select_candidates_trailer_mode_ignores_quiet_window() -> None:
    listing = [_rf("COMM_fresh.csv", 10, NOW - timedelta(minutes=1))]
    result = windows.select_candidates(
        listing, frozenset(), frozenset(), "COMM_*", TRAILER_COMPLETENESS, NOW
    )
    assert [f.name for f in result] == ["COMM_fresh.csv"]


# --- select_candidates: sort order -----------------------------------------------


def test_select_candidates_sorts_by_mtime_then_name_ascending() -> None:
    listing = [
        _rf("COMM_b.csv", 10, NOW - timedelta(hours=1)),
        _rf("COMM_a.csv", 10, NOW - timedelta(hours=2)),
        _rf("COMM_c.csv", 10, NOW - timedelta(hours=1)),  # same mtime as b, name breaks tie
    ]
    result = windows.select_candidates(
        listing, frozenset(), frozenset(), "COMM_*", TRAILER_COMPLETENESS, NOW
    )
    assert [f.name for f in result] == ["COMM_a.csv", "COMM_b.csv", "COMM_c.csv"]


# --- select_manifests --------------------------------------------------------------


def test_select_manifests_filters_pattern_and_drops_acquired() -> None:
    listing = [
        _rf("2026-01-01.manifest.json", 5, NOW - timedelta(hours=1)),
        _rf("2026-01-02.manifest.json", 6, NOW - timedelta(hours=3)),
        _rf("other.csv", 7, NOW),
    ]
    acquired = frozenset({("2026-01-02.manifest.json", 6)})
    result = windows.select_manifests(listing, acquired, "*.manifest.json")
    assert [f.name for f in result] == ["2026-01-01.manifest.json"]


def test_select_manifests_force_bypasses_acquired() -> None:
    listing = [
        _rf("2026-01-01.manifest.json", 5, NOW - timedelta(hours=1)),
        _rf("2026-01-02.manifest.json", 6, NOW - timedelta(hours=3)),
    ]
    acquired = frozenset({("2026-01-02.manifest.json", 6)})
    result = windows.select_manifests(listing, acquired, "*.manifest.json", force=True)
    assert [f.name for f in result] == ["2026-01-02.manifest.json", "2026-01-01.manifest.json"]


def test_select_manifests_non_final_manifest_is_never_acquired_so_always_reexamined() -> None:
    """§7.4/§9.2: a non-final (incomplete/unreadable) manifest never enters
    `acquired_final`, so passing an empty acquired set here simulates it
    being re-selected on the next run regardless of prior examination."""
    listing = [_rf("stuck.manifest.json", 5, NOW - timedelta(days=3))]
    result = windows.select_manifests(listing, frozenset(), "*.manifest.json")
    assert [f.name for f in result] == ["stuck.manifest.json"]
