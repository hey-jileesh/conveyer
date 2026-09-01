"""Tests for `content_hash` at pure-derivation grade -- LLD 007.1 F-1 §5.1
(mechanics), §5.4 (the committed vector family), K-02 (§13.1: "the commit
UDF reproduces every `contracts/fixtures/fact-hash/` vector -- activates
when the build epic commits the family"), K-03 (§13.1: "stamp-insensitivity
property... pinned as a property").

**Grade note (milestone B8).** `content_hash` is designed (§5.1's
completion block) as one scalar UDF wrapping `core.canonical` over a
pre-rendered, bind-derived declared-column struct (`frames/facts.py`,
milestone B9). This suite validates the *resident serializer itself*
against the committed vectors and the stamp-exclusion property at the
plain-value grade -- `spine.core.canonical.row_hash` directly, no Spark, no
`FactSchemaModel`-derived column list. The UDF-grade re-assertion (the real
`frames/facts.py` selection-and-hash pipeline against the same committed
vectors) completes at B9a; nothing here is superseded by that milestone,
both stay green (§13.3: "both families are reproduce-or-fail CI gates").

**Fixture vector format** (`contracts/fixtures/fact-hash/*.json`, one JSON
array of `{"value": ..., "canonical": ..., "sha256": ...}` entries per
file, §5.4): identical convention to §5.3's record-key vectors and 005.1
§15.2's tagged-JSON convention; the `sha256` field **is** `content_hash`.
`_parse_fixture_value` below is this module's **own** untagging parser --
NOT imported from `test_canonical.py` or `test_identity.py` (§5.4: "every
consumer writes its own untagging parser... shared vectors, never shared
code", 004 D-13).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from spine.core import canonical, identity
from spine.core.record import FACT_STAMP_COLUMNS

_FIXTURES_DIR = Path(__file__).resolve().parents[3] / "contracts" / "fixtures" / "fact-hash"
_RECORD_KEY_DIR = Path(__file__).resolve().parents[3] / "contracts" / "fixtures" / "record-key"


def _parse_fixture_value(raw: Any) -> Any:
    if isinstance(raw, dict):
        if set(raw.keys()) == {"$decimal"}:
            return Decimal(raw["$decimal"])
        if set(raw.keys()) == {"$date"}:
            return date.fromisoformat(raw["$date"])
        if set(raw.keys()) == {"$timestamp"}:
            return datetime.fromisoformat(raw["$timestamp"])
        return {k: _parse_fixture_value(v) for k, v in raw.items()}
    if isinstance(raw, list):
        return [_parse_fixture_value(item) for item in raw]
    return raw


def _load_vectors() -> list[tuple[str, dict[str, Any]]]:
    vectors: list[tuple[str, dict[str, Any]]] = []
    for path in sorted(_FIXTURES_DIR.glob("*.json")):
        for entry in json.loads(path.read_text(encoding="utf-8")):
            vectors.append((path.name, entry))
    return vectors


_VECTORS = _load_vectors()


def test_fact_hash_fixtures_exist() -> None:
    # Zero-cases guard (test_canonical.py's / test_identity.py's own
    # convention) -- fails loudly if the fixtures directory moves or empties
    # rather than letting the parametrized test below silently collect zero
    # cases.
    assert _FIXTURES_DIR.is_dir()
    assert _VECTORS


def test_fact_hash_fixtures_total_28_vectors() -> None:
    # §5.4's four files: 8 (basic) + 3 (null-and-empty) + 12 (edge-values)
    # + 5 (coverage: one shared base + three single-column-class variants
    # + one deliberate record-key coincidence entry).
    assert len(_VECTORS) == 28


@pytest.mark.parametrize(
    "filename,entry",
    _VECTORS,
    ids=[f"{filename}#{i}" for i, (filename, _) in enumerate(_VECTORS)],
)
def test_content_hash_reproduces_every_committed_vector(
    filename: str, entry: dict[str, Any]
) -> None:
    """K-02 (pure-derivation grade): the resident serializer reproduces
    every `contracts/fixtures/fact-hash/` vector (28). The UDF-grade
    re-assertion against `frames/facts.py`'s real bind-derived declared-
    column selection completes at B9a."""
    value = _parse_fixture_value(entry["value"])
    assert canonical.canonical_json(value) == entry["canonical"]
    assert canonical.row_hash(value) == entry["sha256"]


def test_coverage_json_coincidence_entry_matches_record_key_basic() -> None:
    # §5.4's `coverage.json`: "one deliberate coincidence entry -- a map
    # value identical to a committed record-key/basic.json entry, same
    # sha256" -- F-2's benign-coincidence note (§5.2) made a living, cross-
    # family assertion: content_hash and record_key are the SAME function
    # (row_hash) over a map, so an identical map produces an identical hash
    # by construction, never a domain-separation guarantee.
    coverage = json.loads((_FIXTURES_DIR / "coverage.json").read_text(encoding="utf-8"))
    coincidence = next(e for e in coverage if e["value"] == {"policy_no": "POL-0042"})

    record_key_basic = json.loads((_RECORD_KEY_DIR / "basic.json").read_text(encoding="utf-8"))
    twin = next(e for e in record_key_basic if e["value"] == {"policy_no": "POL-0042"})

    assert coincidence["sha256"] == twin["sha256"]
    assert coincidence["sha256"] == identity.derive_record_key({"policy_no": "POL-0042"})
    assert coincidence["sha256"] == canonical.row_hash({"policy_no": "POL-0042"})


# --- K-03: stamp-insensitivity property -------------------------------------
#
# "What the exclusion vectors cannot express" (§5.4): a committed vector
# pins ONE fixed input to ONE fixed hash: it cannot show that content_hash
# is unaffected by EVERY possible stamp value, because that is a universal
# claim over an unbounded domain, not a fact about one input. This is
# exactly what a property test is for.


def _content_hash_reference(full_row: Mapping[str, Any]) -> str:
    """D-1's rule at plain-value grade: "the hashed object built from the
    declaration, never the frame" (§5.1's completion block) -- select every
    column NOT in `FACT_STAMP_COLUMNS`, then hash. Pure-derivation grade
    only (no bind-derived `FactSchemaModel` column list, no Spark UDF); the
    real `frames/facts.py::stamp_fact_identity` plan-builder (B9) selects
    from the declaration rather than by exclusion, but the two agree
    exactly where the declaration and the frame are disjoint by
    construction (006.1's F3 bind check) -- which is always, on evaluated
    paths."""
    return canonical.row_hash({k: v for k, v in full_row.items() if k not in FACT_STAMP_COLUMNS})


@st.composite
def _aware_datetime(draw: st.DrawFn) -> datetime:
    # Bounded a day off each end, same idiom as test_canonical.py's own
    # `_timestamp_leaf` -- keeps generated instants inside
    # `canonical_json`'s representable domain regardless of the sampled
    # offset.
    naive = draw(
        st.datetimes(
            min_value=datetime.min + timedelta(days=1),
            max_value=datetime.max - timedelta(days=1),
        )
    )
    tz = draw(st.sampled_from([UTC, timezone(timedelta(hours=5)), timezone(timedelta(hours=-8))]))
    return naive.replace(tzinfo=tz)


# The five D-1 hash-excluded framework stamps (§5.1 fragment 4); `content_
# hash`/`record_key` themselves are derived OUTPUT stamps that never exist
# on a candidate frame prior to derivation, so they are not modeled here.
_STAMPS = st.fixed_dictionaries(
    {
        "batch_id": st.text(min_size=1, max_size=8),
        "delivery_id": st.text(min_size=1, max_size=8),
        "feed_id": st.text(min_size=1, max_size=8),
        "received_at": _aware_datetime(),
        "source_ts": st.one_of(st.none(), _aware_datetime()),
    }
)

# Declared-column keys are generated disjoint from FACT_STAMP_COLUMNS --
# 006.1's F3 bind check enforces exactly this disjointness on every real
# declaration, so a generated key colliding with a stamp name would model
# an unreachable state, not a real one.
_DECLARED_KEY = st.text(min_size=1, max_size=8).filter(lambda s: s not in FACT_STAMP_COLUMNS)
_DECLARED_LEAF = st.one_of(st.none(), st.booleans(), st.integers(), st.text(max_size=8))
_DECLARED = st.dictionaries(_DECLARED_KEY, _DECLARED_LEAF, max_size=6)


@given(declared=_DECLARED, stamps_a=_STAMPS, stamps_b=_STAMPS)
@settings(max_examples=200)
def test_content_hash_is_insensitive_to_stamp_values(
    declared: dict[str, Any], stamps_a: dict[str, Any], stamps_b: dict[str, Any]
) -> None:
    """K-03: `content_hash` unchanged under any stamp values -- two candidate
    rows sharing the same declared columns but carrying arbitrary (and
    arbitrarily different) framework stamp values hash identically, and
    both equal hashing the declared columns alone."""
    row_a = {**stamps_a, **declared}
    row_b = {**stamps_b, **declared}
    expected = canonical.row_hash(declared)
    assert _content_hash_reference(row_a) == expected
    assert _content_hash_reference(row_b) == expected


def test_content_hash_stamp_insensitivity_concrete_example() -> None:
    # A readable, non-generated instance of K-03: same declared columns,
    # two batches worth of completely different stamp values -- same hash.
    declared = {"amount": "100.00", "domain_id": "POL-0042"}
    stamps_batch_1 = {
        "batch_id": "batch-1",
        "delivery_id": "del-1",
        "feed_id": "feed-a",
        "received_at": datetime(2026, 1, 1, tzinfo=UTC),
        "source_ts": None,
    }
    stamps_batch_2 = {
        "batch_id": "batch-2",
        "delivery_id": "del-9",
        "feed_id": "feed-b",
        "received_at": datetime(2027, 6, 6, tzinfo=UTC),
        "source_ts": datetime(2026, 5, 5, tzinfo=UTC),
    }
    h1 = _content_hash_reference({**stamps_batch_1, **declared})
    h2 = _content_hash_reference({**stamps_batch_2, **declared})
    assert h1 == h2 == canonical.row_hash(declared)


def test_content_hash_still_depends_on_declared_columns() -> None:
    # Sanity twin of K-03: the reference function is not vacuously constant
    # -- changing a DECLARED column (stamps held fixed) does change the hash.
    stamps = {
        "batch_id": "batch-1",
        "delivery_id": "del-1",
        "feed_id": "feed-a",
        "received_at": datetime(2026, 1, 1, tzinfo=UTC),
        "source_ts": None,
    }
    h1 = _content_hash_reference({**stamps, "amount": "100.00"})
    h2 = _content_hash_reference({**stamps, "amount": "999.00"})
    assert h1 != h2
