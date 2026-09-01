"""Unit tests for `spine.core.identity.derive_record_key` — LLD 007.1 F-2
§5.2 (mechanics), §5.3 (the committed vector surface), K-01 (§13.1: "the CI
gate active since v0.1-seams").

**Fixture vector format** (`contracts/fixtures/record-key/*.json`, one JSON
array of `{"value": ..., "canonical": ..., "sha256": ...}` entries per
file, §5.3): identical convention to 005.1 §15.2's tagged-JSON fixtures.
`_parse_fixture_value` below is this module's **own** untagging parser --
NOT imported from `spine.core.canonical`'s test suite or anywhere else
(007.1 §5.3: "every consumer... writes its own untagging parser against
these committed files -- shared vectors, never shared code", 004 D-13;
`test_canonical.py::_parse_fixture_value` is the reference implementation
to READ, per this bead's brief, never to import)."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from spine.core import identity

_FIXTURES_DIR = Path(__file__).resolve().parents[3] / "contracts" / "fixtures" / "record-key"


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
        for entry in json.loads(path.read_text()):
            vectors.append((path.name, entry))
    return vectors


_VECTORS = _load_vectors()


def test_record_key_fixtures_exist() -> None:
    # Zero-cases guard (test_canonical.py's own convention) -- fails loudly
    # if the fixtures directory moves or empties rather than letting the
    # parametrized test below silently collect zero cases.
    assert _FIXTURES_DIR.is_dir()
    assert _VECTORS


def test_record_key_fixtures_total_25_vectors() -> None:
    # K-01's own count (007.1 §5.3's generation-provenance note): 8 (basic)
    # + 3 (null-and-empty) + 14 (edge-values).
    assert len(_VECTORS) == 25


@pytest.mark.parametrize(
    "filename,entry",
    _VECTORS,
    ids=[f"{filename}#{i}" for i, (filename, _) in enumerate(_VECTORS)],
)
def test_derive_record_key_reproduces_every_committed_vector(
    filename: str, entry: dict[str, Any]
) -> None:
    """K-01: `derive_record_key` must reproduce every `contracts/fixtures/
    record-key/` vector (25) -- the CI gate active since v0.1-seams."""
    value = _parse_fixture_value(entry["value"])
    assert identity.derive_record_key(value) == entry["sha256"]


# --- targeted properties beyond the fixture reproduction --------------------


def test_derive_record_key_is_insensitive_to_declared_key_order() -> None:
    # 007.1 §5.2: canonical rendering sorts keys bytewise -- reordering a
    # `record_key:` declaration never changes derived keys.
    a = identity.derive_record_key({"agent_code": "A7", "statement_period": "2026-07"})
    b = identity.derive_record_key({"statement_period": "2026-07", "agent_code": "A7"})
    assert a == b


def test_derive_record_key_is_total_over_none_values() -> None:
    # 007.1 §5.2: total over complete maps -- `None` renders as canonical
    # `null`, a value, never a refusal (the commit-site totality rule).
    key = identity.derive_record_key({"agent_code": None, "statement_period": "2026-07"})
    assert len(key) == 64
    assert all(c in "0123456789abcdef" for c in key)


def test_derive_record_key_column_names_participate_in_identity() -> None:
    # 007.1 §5.2: the hashed object is the map, not a positional value
    # tuple -- a renamed key column changes the derived key.
    a = identity.derive_record_key({"agent_code": "x"})
    b = identity.derive_record_key({"agent_id": "x"})
    assert a != b


def test_derive_record_key_distinguishes_bool_from_string() -> None:
    # [EM-11]: bool is in the input domain; its canonical rendering must
    # not collide with the string "true"/"false".
    a = identity.derive_record_key({"flag": True})
    b = identity.derive_record_key({"flag": "true"})
    assert a != b


def test_derive_record_key_output_is_64_lowercase_hex() -> None:
    key = identity.derive_record_key({"policy_no": "POL-0042"})
    assert len(key) == 64
    assert key == key.lower()
    int(key, 16)  # raises ValueError if not valid hex
