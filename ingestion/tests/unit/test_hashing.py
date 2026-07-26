"""Unit tests for `ingestion.core.hashing` — LLD §6.5 (D-6).

`canonical_content_hash` is example-tested against a hand-computed
sha256sum-style digest, then property-tested per §12.3: invariance under
permutation of the input objects, and sensitivity to any single name/sha256
change.
"""

import hashlib

from hypothesis import given, settings
from hypothesis import strategies as st
from ingestion.core import hashing

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_SHA_C = "c" * 64

_name_strategy = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\n\r"),
    min_size=1,
    max_size=20,
)
_sha_strategy = st.text(alphabet="0123456789abcdef", min_size=64, max_size=64)
_objects_strategy = st.lists(
    st.tuples(_name_strategy, _sha_strategy), min_size=0, max_size=8, unique_by=lambda x: x[0]
)


# --- example-based -----------------------------------------------------------


def test_empty_sequence_hashes_empty_string() -> None:
    expected = "sha256:" + hashlib.sha256(b"").hexdigest()
    assert hashing.canonical_content_hash([]) == expected


def test_single_object_matches_sha256sum_style_line() -> None:
    line = f"{_SHA_A}  x.csv"
    expected = "sha256:" + hashlib.sha256(line.encode("utf-8")).hexdigest()
    assert hashing.canonical_content_hash([("x.csv", _SHA_A)]) == expected


def test_multiple_objects_sorted_bytewise_before_hashing() -> None:
    # Deliberately supplied out of sorted order; the function must sort.
    objects = [("b.csv", _SHA_B), ("a.csv", _SHA_A), ("c.csv", _SHA_C)]
    lines = sorted([f"{_SHA_A}  a.csv", f"{_SHA_B}  b.csv", f"{_SHA_C}  c.csv"])
    expected = "sha256:" + hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()
    assert hashing.canonical_content_hash(objects) == expected


def test_upload_order_does_not_affect_hash() -> None:
    objects = [("b.csv", _SHA_B), ("a.csv", _SHA_A)]
    reordered = [("a.csv", _SHA_A), ("b.csv", _SHA_B)]
    assert hashing.canonical_content_hash(objects) == hashing.canonical_content_hash(reordered)


def test_renamed_but_identical_bytes_is_a_different_delivery() -> None:
    # Names participate in the hash deliberately (LLD §6.5): a rename changes
    # the content hash even though the underlying bytes (sha256) match.
    original = [("a.csv", _SHA_A)]
    renamed = [("a-renamed.csv", _SHA_A)]
    assert hashing.canonical_content_hash(original) != hashing.canonical_content_hash(renamed)


# --- property-based (§12.3) --------------------------------------------------


@given(data=st.data())
@settings(max_examples=200)
def test_canonical_content_hash_invariant_under_permutation(data: st.DataObject) -> None:
    objects = data.draw(_objects_strategy)
    permuted = data.draw(st.permutations(objects))
    assert hashing.canonical_content_hash(objects) == hashing.canonical_content_hash(permuted)


@given(data=st.data())
@settings(max_examples=200)
def test_canonical_content_hash_sensitive_to_any_name_or_hash_change(data: st.DataObject) -> None:
    objects = data.draw(
        st.lists(
            st.tuples(_name_strategy, _sha_strategy),
            min_size=1,
            max_size=8,
            unique_by=lambda x: x[0],
        )
    )
    idx = data.draw(st.integers(min_value=0, max_value=len(objects) - 1))
    existing_names = {name for name, _ in objects}
    field = data.draw(st.sampled_from(["name", "sha"]))

    changed = list(objects)
    if field == "name":
        new_name = data.draw(_name_strategy.filter(lambda n: n not in existing_names))
        changed[idx] = (new_name, objects[idx][1])
    else:
        new_sha = data.draw(_sha_strategy.filter(lambda s: s != objects[idx][1]))
        changed[idx] = (objects[idx][0], new_sha)

    assert hashing.canonical_content_hash(objects) != hashing.canonical_content_hash(changed)
