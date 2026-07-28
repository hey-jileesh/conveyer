"""Unit tests for `ingestion.core.minting` — LLD §6.6 (D-4).

`mint_batch_id` is example-tested against a hand-computed `uuid5` value, then
property-tested per §12.3 for determinism (same inputs, same batch_id, always)
and sensitivity (any content_hash change mints a different batch_id).
"""

import uuid

from hypothesis import given, settings
from hypothesis import strategies as st
from ingestion.core import minting

_feed_id_strategy = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789-/", min_size=3, max_size=40
)
_content_hash_strategy = st.text(alphabet="0123456789abcdef", min_size=64, max_size=64).map(
    lambda h: f"sha256:{h}"
)


# --- example-based -----------------------------------------------------------


def test_namespace_constant_is_the_lld_fixed_value() -> None:
    assert minting.CONVEYER_INGESTION_NS == uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")


def test_mint_batch_id_matches_manual_uuid5_computation() -> None:
    feed_id = "carrier-x/commission-statements"
    content_hash = "sha256:" + "a" * 64
    expected = str(uuid.uuid5(minting.CONVEYER_INGESTION_NS, f"{feed_id}\n{content_hash}"))
    assert minting.mint_batch_id(feed_id, content_hash) == expected


def test_mint_batch_id_returns_a_valid_uuid_string() -> None:
    minted = minting.mint_batch_id("carrier-x/commission-statements", "sha256:" + "a" * 64)
    assert uuid.UUID(minted).version == 5


# --- property-based (§12.3) --------------------------------------------------


@given(feed_id=_feed_id_strategy, content_hash=_content_hash_strategy)
@settings(max_examples=200)
def test_mint_batch_id_is_deterministic(feed_id: str, content_hash: str) -> None:
    assert minting.mint_batch_id(feed_id, content_hash) == minting.mint_batch_id(
        feed_id, content_hash
    )


@given(feed_id=_feed_id_strategy, h1=_content_hash_strategy, h2=_content_hash_strategy)
@settings(max_examples=200)
def test_mint_batch_id_sensitive_to_content_hash_change(feed_id: str, h1: str, h2: str) -> None:
    if h1 != h2:
        assert minting.mint_batch_id(feed_id, h1) != minting.mint_batch_id(feed_id, h2)


@given(f1=_feed_id_strategy, f2=_feed_id_strategy, content_hash=_content_hash_strategy)
@settings(max_examples=200)
def test_mint_batch_id_sensitive_to_feed_id_change(f1: str, f2: str, content_hash: str) -> None:
    if f1 != f2:
        assert minting.mint_batch_id(f1, content_hash) != minting.mint_batch_id(f2, content_hash)
