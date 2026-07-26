"""make_store_fx (boto3 closures) -- LLD S7.7.

Each public function below is a plain module-level function taking the
boto3 S3 client as its first argument; `make_store_fx` closes each one over
one client via `functools.partial`, matching `StoreFx`'s field order in
`effects/records.py`. `ClientError` at any boundary becomes `TransientError`
(S7.3) -- the framework's Lambda retry / DLQ / alarm path handles it
uniformly, whatever the underlying S3 failure.
"""

from __future__ import annotations

import functools
import hashlib
from collections.abc import Iterator
from typing import Any

from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from ingestion.core.completeness import Defect
from ingestion.effects.records import ObjectSummary, StoreFx, TransientError

_STREAM_CHUNK_BYTES = 1024 * 1024  # 1 MiB
_UPLOAD_PART_BYTES = 8 * 1024 * 1024  # 8 MiB -- safely above S3's 5 MiB multipart minimum
_COPY_MULTIPART_THRESHOLD_BYTES = 5 * 1024**3  # 5 GiB -- CopyObject's single-shot limit
_COPY_PART_BYTES = 512 * 1024**2  # 512 MiB per part for the multipart-copy branch


def _list_prefix(client: Any, bucket: str, prefix: str) -> list[ObjectSummary]:
    try:
        summaries: list[ObjectSummary] = []
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                summaries.append(
                    ObjectSummary(
                        key=key,
                        name=key.rsplit("/", 1)[-1],
                        bytes=obj["Size"],
                        mtime=obj["LastModified"],
                    )
                )
        return summaries
    except ClientError as exc:
        raise TransientError(f"list_prefix failed for s3://{bucket}/{prefix}: {exc}") from exc


def _stream_sha256(client: Any, bucket: str, key: str) -> tuple[str, int, str | None, str | None]:
    """Streaming SHA-256 -- never spools to disk; one chunk at a time.

    H-1 (security-gate, TOCTOU): `VersionId`/`ETag` are read off the SAME
    `GetObject` response the hash is streamed from -- capturing "the exact
    bytes just hashed" identity at this call's T0, for `copy_verbatim` to
    pin at its own, later, T1 (see `StoreFx.stream_sha256`'s docstring).
    """
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        digest = hashlib.sha256()
        total = 0
        for chunk in response["Body"].iter_chunks(chunk_size=_STREAM_CHUNK_BYTES):
            digest.update(chunk)
            total += len(chunk)
        return digest.hexdigest(), total, response.get("VersionId"), response.get("ETag")
    except ClientError as exc:
        raise TransientError(f"stream_sha256 failed for s3://{bucket}/{key}: {exc}") from exc


def _read_object(
    client: Any, bucket: str, key: str, max_bytes: int
) -> tuple[bytes, str | None, str | None] | Defect:
    """Shared capped-read body for `_get_bytes`/`_get_bytes_pinned`: `Defect`
    (a value, S7.0 rule 4 -- caller decides what to do with it), never an
    exception, if the object exceeds `max_bytes`. A `head_object` first
    avoids ever pulling more than `max_bytes` into memory. `VersionId`/`ETag`
    are read off the SAME `GetObject` response the bytes are returned from --
    the H-1-residual (security-gate, TOCTOU) capture point for a manifest
    read, mirroring `_stream_sha256`'s T0 capture for hashed data objects.
    """
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        size = head["ContentLength"]
        if size > max_bytes:
            return Defect(
                reason=f"s3://{bucket}/{key} is {size} bytes, exceeds max_bytes cap {max_bytes}"
            )
        response = client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read(), response.get("VersionId"), response.get("ETag")
    except ClientError as exc:
        raise TransientError(f"get_bytes failed for s3://{bucket}/{key}: {exc}") from exc


def _get_bytes(client: Any, bucket: str, key: str, max_bytes: int) -> bytes | Defect:
    """Registry read (`effects/registry.py`'s `feeds.json`): plain bytes, no
    pinning need -- this contract is UNCHANGED by the H-1-residual fix.
    """
    result = _read_object(client, bucket, key, max_bytes)
    if isinstance(result, Defect):
        return result
    raw, _version_id, _etag = result
    return raw


def _get_bytes_pinned(
    client: Any, bucket: str, key: str, max_bytes: int
) -> tuple[bytes, str | None, str | None] | Defect:
    """Manifest read (H-1 residual, security-gate, TOCTOU): same capped read
    as `_get_bytes`, but ALSO returns the `VersionId`/`ETag` captured from
    the SAME `GetObject` response the bytes came from, so the caller can
    thread them onto the manifest `StagedObject.src_version_id`/`src_etag`
    exactly as `_stream_sha256`'s callers do for data objects -- pinning the
    manifest's own vestibule->canonical copy to the exact bytes that were
    parsed, not whatever is current when `copy_verbatim` later runs.
    """
    return _read_object(client, bucket, key, max_bytes)


def _get_tail(client: Any, bucket: str, key: str, n: int) -> bytes:
    """Ranged GET for the trailer check -- last `n` bytes (or the whole object
    if it is smaller than `n`; S3's suffix-range semantics already do this).
    """
    try:
        response = client.get_object(Bucket=bucket, Key=key, Range=f"bytes=-{n}")
        return response["Body"].read()
    except ClientError as exc:
        raise TransientError(f"get_tail failed for s3://{bucket}/{key}: {exc}") from exc


def _copy_verbatim(
    client: Any,
    src_bucket: str,
    src_key: str,
    dst_bucket: str,
    dst_key: str,
    src_version_id: str | None = None,
    src_etag: str | None = None,
) -> None:
    """Server-side copy; CopyObject's single-shot limit is 5 GiB, so objects
    above that use a manual multipart UploadPartCopy loop instead.

    H-1 (security-gate, TOCTOU): `src_version_id`/`src_etag` -- captured
    when the object was hashed (`stream_sha256`'s T0), threaded here via
    `StagedObject`/`CopySpec` -- pin every read this function makes (the
    `head_object` size check AND the copy itself) to those exact bytes, so
    a partner's retained `PutObject` on the vestibule between hash-time and
    this call (T1) cannot silently substitute different content. Both
    `None` (sftp-pull's `copy_verbatim` calls, and s3-push's own manifest-
    object copy, never set them) reproduces the old unpinned behavior
    exactly. `VersionId` is preferred when available (the landing bucket is
    versioned, §10.1); `CopySourceIfMatch=<etag>` is the fallback when it
    is not -- S3 returns a precondition-failure `ClientError` (-> below ->
    `TransientError`) if the object no longer matches, i.e. the registration
    fails closed rather than silently copying the wrong bytes.
    """
    try:
        head_kwargs: dict[str, Any] = {"Bucket": src_bucket, "Key": src_key}
        copy_source: dict[str, Any] = {"Bucket": src_bucket, "Key": src_key}
        copy_precondition: dict[str, Any] = {}
        if src_version_id is not None:
            head_kwargs["VersionId"] = src_version_id
            copy_source["VersionId"] = src_version_id
        elif src_etag is not None:
            head_kwargs["IfMatch"] = src_etag
            copy_precondition["CopySourceIfMatch"] = src_etag

        head = client.head_object(**head_kwargs)
        size = head["ContentLength"]
        if size <= _COPY_MULTIPART_THRESHOLD_BYTES:
            client.copy_object(
                Bucket=dst_bucket, Key=dst_key, CopySource=copy_source, **copy_precondition
            )
            return
        upload_id = client.create_multipart_upload(Bucket=dst_bucket, Key=dst_key)["UploadId"]
        parts: list[dict[str, Any]] = []
        part_number = 1
        start = 0
        while start < size:
            end = min(start + _COPY_PART_BYTES, size) - 1
            part = client.upload_part_copy(
                Bucket=dst_bucket,
                Key=dst_key,
                UploadId=upload_id,
                PartNumber=part_number,
                CopySource=copy_source,
                CopySourceRange=f"bytes={start}-{end}",
                **copy_precondition,
            )
            parts.append({"ETag": part["CopyPartResult"]["ETag"], "PartNumber": part_number})
            start = end + 1
            part_number += 1
        client.complete_multipart_upload(
            Bucket=dst_bucket,
            Key=dst_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except ClientError as exc:
        raise TransientError(
            f"copy_verbatim failed for s3://{src_bucket}/{src_key} "
            f"-> s3://{dst_bucket}/{dst_key}: {exc}"
        ) from exc


def _stream_upload(client: Any, chunks: Iterator[bytes], bucket: str, key: str) -> tuple[str, int]:
    """Chunks in -> multipart upload out; sha256 accumulated en route so the
    object is never buffered whole in memory or spooled to disk. State (the
    running digest, byte count, part buffer, part number) lives entirely in
    this one function activation (S7.7) -- there is no stateful "sink"
    object with an identity across calls. MUST abort the multipart upload on
    any failure mid-stream (iterator failure or a failing `upload_part`
    call) so a partial upload never lingers.
    """
    upload_id = client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
    digest = hashlib.sha256()
    total_bytes = 0
    parts: list[dict[str, Any]] = []
    part_number = 1
    buffer = bytearray()
    try:
        for chunk in chunks:
            digest.update(chunk)
            total_bytes += len(chunk)
            buffer.extend(chunk)
            while len(buffer) >= _UPLOAD_PART_BYTES:
                part_bytes = bytes(buffer[:_UPLOAD_PART_BYTES])
                del buffer[:_UPLOAD_PART_BYTES]
                part = client.upload_part(
                    Bucket=bucket,
                    Key=key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=part_bytes,
                )
                parts.append({"ETag": part["ETag"], "PartNumber": part_number})
                part_number += 1
        if buffer or not parts:  # final (possibly short) part; S3 allows this for the last part
            part = client.upload_part(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=bytes(buffer),
            )
            parts.append({"ETag": part["ETag"], "PartNumber": part_number})
    except Exception as exc:
        client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise TransientError(f"stream_upload aborted for s3://{bucket}/{key}: {exc}") from exc

    try:
        client.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
        )
    except ClientError as exc:
        client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise TransientError(f"stream_upload aborted for s3://{bucket}/{key}: {exc}") from exc
    return digest.hexdigest(), total_bytes


def make_store_fx(client: Any) -> StoreFx:
    return StoreFx(
        list_prefix=functools.partial(_list_prefix, client),
        stream_sha256=functools.partial(_stream_sha256, client),
        get_bytes=functools.partial(_get_bytes, client),
        get_bytes_pinned=functools.partial(_get_bytes_pinned, client),
        get_tail=functools.partial(_get_tail, client),
        copy_verbatim=functools.partial(_copy_verbatim, client),
        stream_upload=functools.partial(_stream_upload, client),
    )
