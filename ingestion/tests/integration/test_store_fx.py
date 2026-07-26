"""Integration tests (factory level, LLD §12.1) for `effects.s3.make_store_fx`
against moto S3.

Covers: streaming SHA-256 correctness, `get_bytes`'s max-bytes cap -> `Defect`,
ranged tail reads, `list_prefix`, `copy_verbatim` (single-shot only -- the
>5 GiB multipart-copy branch is not exercised: moto cannot practically
simulate a multi-gigabyte object, §12.5 documented exclusion), and
`stream_upload` including the mandatory multipart-abort-on-iterator-failure
path (state lives inside one function activation, §7.7).
"""

import hashlib
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from ingestion.core.completeness import Defect
from ingestion.effects.records import ObjectSummary, TransientError
from ingestion.effects.s3 import make_store_fx
from moto import mock_aws

_BUCKET = "conveyer-test-store-fx"


@pytest.fixture
def s3_client() -> Iterator[Any]:
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=_BUCKET)
        yield client


# --- list_prefix -------------------------------------------------------------


def test_list_prefix_returns_object_summaries(s3_client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="incoming/a.csv", Body=b"12345")
    s3_client.put_object(Bucket=_BUCKET, Key="incoming/b.csv", Body=b"1234567")
    s3_client.put_object(Bucket=_BUCKET, Key="other/c.csv", Body=b"x")
    fx = make_store_fx(s3_client)

    summaries = fx.list_prefix(_BUCKET, "incoming/")

    assert {s.name for s in summaries} == {"a.csv", "b.csv"}
    assert all(isinstance(s, ObjectSummary) for s in summaries)
    by_name = {s.name: s for s in summaries}
    assert by_name["a.csv"].bytes == 5
    assert by_name["a.csv"].key == "incoming/a.csv"
    assert by_name["b.csv"].bytes == 7


# --- stream_sha256 -------------------------------------------------------------


def test_stream_sha256_matches_hashlib_across_multiple_chunks(s3_client) -> None:
    # spans several 1 MiB streaming chunks -- exercises the accumulation loop,
    # not just a single read.
    content = b"x" * (3 * 1024 * 1024 + 17)
    s3_client.put_object(Bucket=_BUCKET, Key="obj", Body=content)
    fx = make_store_fx(s3_client)

    digest, total_bytes, version_id, etag = fx.stream_sha256(_BUCKET, "obj")

    assert digest == hashlib.sha256(content).hexdigest()
    assert total_bytes == len(content)
    assert version_id is None  # bucket is not versioned
    assert etag is not None


# --- H-1 (security-gate): stream_sha256 captures VersionId/ETag; copy_verbatim ---
# pins the server-side copy to them, closing the TOCTOU window between hashing
# (T0) and the copy (T1).


def test_stream_sha256_captures_version_id_on_a_versioned_bucket(s3_client) -> None:
    s3_client.put_bucket_versioning(Bucket=_BUCKET, VersioningConfiguration={"Status": "Enabled"})
    put_response = s3_client.put_object(Bucket=_BUCKET, Key="versioned-obj", Body=b"v1-bytes")
    fx = make_store_fx(s3_client)

    _digest, _total, version_id, _etag = fx.stream_sha256(_BUCKET, "versioned-obj")

    assert version_id == put_response["VersionId"]


def test_copy_verbatim_pinned_to_version_id_copies_the_hashed_bytes_not_a_later_mutation(
    s3_client,
) -> None:
    s3_client.put_bucket_versioning(Bucket=_BUCKET, VersioningConfiguration={"Status": "Enabled"})
    s3_client.put_object(Bucket=_BUCKET, Key="src/a.csv", Body=b"hashed-bytes")
    fx = make_store_fx(s3_client)

    _digest, _total, version_id, etag = fx.stream_sha256(_BUCKET, "src/a.csv")
    assert version_id is not None

    # The partner mutates the SAME key between hash-time (T0) and copy-time (T1).
    s3_client.put_object(Bucket=_BUCKET, Key="src/a.csv", Body=b"mutated-bytes-swapped-in-later")

    fx.copy_verbatim(_BUCKET, "src/a.csv", _BUCKET, "canonical/pinned.csv", version_id, etag)

    copied = s3_client.get_object(Bucket=_BUCKET, Key="canonical/pinned.csv")["Body"].read()
    assert copied == b"hashed-bytes"  # the HASHED bytes, not the later mutation


def test_copy_verbatim_unpinned_reproduces_old_unversioned_behavior(s3_client) -> None:
    # `src_version_id`/`src_etag` both `None` (the sftp-pull / manifest-object call
    # shape) must behave exactly like the pre-H-1 `copy_verbatim` -- copies whatever
    # is CURRENT at copy time, no pinning.
    s3_client.put_object(Bucket=_BUCKET, Key="src/b.csv", Body=b"first-bytes")
    fx = make_store_fx(s3_client)

    s3_client.put_object(Bucket=_BUCKET, Key="src/b.csv", Body=b"second-bytes")
    fx.copy_verbatim(_BUCKET, "src/b.csv", _BUCKET, "canonical/unpinned.csv")

    copied = s3_client.get_object(Bucket=_BUCKET, Key="canonical/unpinned.csv")["Body"].read()
    assert copied == b"second-bytes"


# --- get_bytes -----------------------------------------------------------------


def test_get_bytes_under_cap_returns_bytes(s3_client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="manifest.json", Body=b'{"a": 1}')
    fx = make_store_fx(s3_client)

    result = fx.get_bytes(_BUCKET, "manifest.json", 1024 * 1024)

    assert result == b'{"a": 1}'


def test_get_bytes_over_cap_returns_defect(s3_client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="huge.json", Body=b"x" * 2000)
    fx = make_store_fx(s3_client)

    result = fx.get_bytes(_BUCKET, "huge.json", 1024)

    assert isinstance(result, Defect)
    assert "2000" in result.reason
    assert "1024" in result.reason


# --- get_bytes_pinned -- H-1 RESIDUAL (security-gate): manifest-read variant -----
# that ALSO captures VersionId/ETag from the SAME GetObject response the bytes are
# read from, so the manifest's own vestibule->canonical copy can be pinned exactly
# like `stream_sha256`'s data-object case.


def test_get_bytes_pinned_under_cap_returns_bytes_and_etag(s3_client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="manifest.json", Body=b'{"a": 1}')
    fx = make_store_fx(s3_client)

    raw, version_id, etag = fx.get_bytes_pinned(_BUCKET, "manifest.json", 1024 * 1024)

    assert raw == b'{"a": 1}'
    assert version_id is None  # bucket is not versioned
    assert etag is not None


def test_get_bytes_pinned_over_cap_returns_defect(s3_client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="huge-manifest.json", Body=b"x" * 2000)
    fx = make_store_fx(s3_client)

    result = fx.get_bytes_pinned(_BUCKET, "huge-manifest.json", 1024)

    assert isinstance(result, Defect)
    assert "2000" in result.reason
    assert "1024" in result.reason


def test_get_bytes_pinned_captures_version_id_on_a_versioned_bucket(s3_client) -> None:
    s3_client.put_bucket_versioning(Bucket=_BUCKET, VersioningConfiguration={"Status": "Enabled"})
    put_response = s3_client.put_object(
        Bucket=_BUCKET, Key="versioned-manifest.json", Body=b'{"v": 1}'
    )
    fx = make_store_fx(s3_client)

    _raw, version_id, _etag = fx.get_bytes_pinned(_BUCKET, "versioned-manifest.json", 1024 * 1024)

    assert version_id == put_response["VersionId"]


# --- get_tail --------------------------------------------------------------------


def test_get_tail_returns_last_n_bytes(s3_client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="trailer.txt", Body=b"header\nTRAILER:42\n")
    fx = make_store_fx(s3_client)

    tail = fx.get_tail(_BUCKET, "trailer.txt", 11)

    assert tail == b"TRAILER:42\n"


def test_get_tail_n_larger_than_object_returns_whole_object(s3_client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="small.txt", Body=b"tiny")
    fx = make_store_fx(s3_client)

    tail = fx.get_tail(_BUCKET, "small.txt", 4096)

    assert tail == b"tiny"


# --- copy_verbatim ---------------------------------------------------------------


def test_copy_verbatim_single_shot_copies_bytes(s3_client) -> None:
    s3_client.put_object(Bucket=_BUCKET, Key="src/a.csv", Body=b"delivered-bytes")
    fx = make_store_fx(s3_client)

    fx.copy_verbatim(_BUCKET, "src/a.csv", _BUCKET, "canonical/a.csv")

    copied = s3_client.get_object(Bucket=_BUCKET, Key="canonical/a.csv")["Body"].read()
    assert copied == b"delivered-bytes"


# --- stream_upload -----------------------------------------------------------------


def test_stream_upload_accumulates_hash_and_bytes(s3_client) -> None:
    chunks = [b"a" * 4, b"b" * 6, b"c" * 3]
    fx = make_store_fx(s3_client)

    digest, total_bytes = fx.stream_upload(iter(chunks), _BUCKET, "uploaded/obj.bin")

    expected = b"".join(chunks)
    assert digest == hashlib.sha256(expected).hexdigest()
    assert total_bytes == len(expected)
    uploaded = s3_client.get_object(Bucket=_BUCKET, Key="uploaded/obj.bin")["Body"].read()
    assert uploaded == expected


def test_stream_upload_spans_multiple_multipart_parts(s3_client) -> None:
    # 20 MiB across 1 MiB input chunks crosses the 8 MiB part-size boundary twice.
    chunk = b"q" * (1024 * 1024)
    fx = make_store_fx(s3_client)

    digest, total_bytes = fx.stream_upload((chunk for _ in range(20)), _BUCKET, "big/obj.bin")

    expected = chunk * 20
    assert total_bytes == len(expected)
    assert digest == hashlib.sha256(expected).hexdigest()
    uploaded = s3_client.get_object(Bucket=_BUCKET, Key="big/obj.bin")["Body"].read()
    assert uploaded == expected


def test_stream_upload_aborts_multipart_on_iterator_failure(s3_client) -> None:
    def failing_chunks() -> Iterator[bytes]:
        yield b"partial-bytes"
        raise RuntimeError("sftp connection dropped mid-stream")

    fx = make_store_fx(s3_client)

    with pytest.raises(TransientError):
        fx.stream_upload(failing_chunks(), _BUCKET, "never-completes")

    uploads = s3_client.list_multipart_uploads(Bucket=_BUCKET).get("Uploads", [])
    assert uploads == []
    with pytest.raises(s3_client.exceptions.NoSuchKey):
        s3_client.get_object(Bucket=_BUCKET, Key="never-completes")
