"""Effects + sub-record type definitions (records of functions) -- LLD S7.7.

A capability is a frozen dataclass whose fields are `Callable`s -- the Python
spelling of a Clojure map of fns (S7.0 rule 3). Production records are built
by `make_*_fx` factories in the sibling `effects/*.py` modules, whose
closures capture boto3/paramiko clients; test doubles are the SAME record
shapes built from plain local functions (no mocking framework, ever, S12.2).
`core/` imports none of this (purity-linter enforced) -- it only ever sees
the plain values these functions return.

`TransientError` is defined here -- the ONLY exception type in the codebase
(S7.3 restated at S7.0 rule 4): infra hiccups (S3 5xx, DynamoDB throttle,
Iceberg commit conflict after retries, SFTP failures) that should retry then
alarm. Raised by effect functions only; `core/` never raises (defects are
values there, S7.3).

`Effects.invoke_async` is the m5-absence bead's ONE sanctioned additive
field (§9.3 needs a Lambda-invoke capability §7.7's original record shape
omits): fire-and-forget async invoke, `(function_name, payload) -> None`.
Not a general invocation abstraction -- `absence/detector.py` is its only
production caller (stuck-claim sweep re-driving the registrar / a per-feed
driver), built in `effects/build.py` and, for tests, a plain recording
local function in `tests/conftest.py` (no mocking framework, ever).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from ingestion.config import RuntimeConfig
from ingestion.core.completeness import Defect
from ingestion.core.decisions import RegistrationRequest
from ingestion.core.model import ClaimItem, ClaimResult, DeliveryRecord
from ingestion.core.windows import RemoteFile


class TransientError(Exception):
    """Infra failure that should retry / alarm (S7.3). Raised only by effect
    functions in `effects/*.py` -- Lambda retry -> DLQ on exhaustion -> alarm.
    """


@dataclass(frozen=True)
class ObjectSummary:
    key: str
    name: str  # basename
    bytes: int
    mtime: datetime


@dataclass(frozen=True)
class StoreFx:
    list_prefix: Callable[[str, str], list[ObjectSummary]]  # (bucket, prefix)
    stream_sha256: Callable[[str, str], tuple[str, int, str | None, str | None]]
    # (bucket, key) -> (hex, bytes, version_id, etag). H-1 (security-gate, TOCTOU): the
    # trailing `version_id`/`etag` are captured from the SAME GetObject response the hash
    # is streamed from (T0) -- `version_id` when the bucket is versioned (None otherwise,
    # e.g. a bucket predating versioning), `etag` always. Callers thread both through
    # `StagedObject.src_version_id`/`src_etag` -> `CopySpec` so `copy_verbatim` can pin the
    # server-side copy (T1) to the EXACT bytes that were hashed, closing the window where a
    # partner's retained `PutObject` on `incoming/*` could swap the object in between.
    get_bytes: Callable[[str, str, int], bytes | Defect]  # registry read; Defect if > max_bytes
    get_bytes_pinned: Callable[[str, str, int], tuple[bytes, str | None, str | None] | Defect]
    # (bucket, key, max_bytes) -> (raw_bytes, version_id, etag) | Defect -- H-1 RESIDUAL
    # (security-gate, TOCTOU): the manifest-read variant of `get_bytes`, used ONLY for the
    # s3-push manifest object (never the feeds.json registry read, which stays on plain
    # `get_bytes` -- no pinning need there). `version_id`/`etag` are captured from the SAME
    # GetObject response the bytes are read from (T0), exactly like `stream_sha256`'s capture
    # for data objects -- callers thread both through `StagedObject.src_version_id`/`src_etag`
    # -> `CopySpec` so the manifest's OWN vestibule->canonical copy is pinned identically to a
    # data object's, closing the residual TOCTOU window on the delivery's own source-of-truth
    # assertion document.
    get_tail: Callable[[str, str, int], bytes]  # ranged GET, trailer check
    copy_verbatim: Callable[[str, str, str, str, str | None, str | None], None]
    # (src_bucket, src_key, dst_bucket, dst_key, src_version_id, src_etag) -- server-side;
    # multipart > 5 GiB. H-1: `src_version_id`/`src_etag` are None-tolerant (both None ->
    # unpinned copy of whatever is current, the pre-fix behavior -- e.g. the sftp-pull path
    # never sets these, and never calls `copy_verbatim` at all, since `src_key is None`
    # there). When `src_version_id` is set, `CopySource` carries it (`{Bucket, Key,
    # VersionId}`); otherwise, when only `src_etag` is available, `CopySourceIfMatch=<etag>`
    # is the fallback -- the copy fails closed (`ClientError` -> `TransientError`) if the
    # object changed since it was hashed.
    stream_upload: Callable[[Iterator[bytes], str, str], tuple[str, int]]
    # chunks in -> multipart upload to (bucket, key), sha256 accumulated en route ->
    # (sha256_hex, total_bytes); aborts the multipart on iterator failure. Replaces any
    # stateful "sink" object: state lives inside one function activation, not an identity.


@dataclass(frozen=True)
class LedgerFx:
    append: Callable[[Sequence[DeliveryRecord]], None]
    # pyarrow table <- rows; table.append(); on CommitFailedException retry (fresh table
    # load) up to 5 attempts, jittered backoff 0.5-8 s; then TransientError.
    # Metric LedgerCommitRetries per retry.
    scan_feed: Callable[[str, datetime | None], list[DeliveryRecord]]
    # row_filter feed_id (+ received_at >= since); returns RAW rows always --
    # folding is the caller's job via core.folds (S6.2).


@dataclass(frozen=True)
class CasFx:  # semantics specified in S8.4
    claim: Callable[[RegistrationRequest, str, str, dict, datetime], ClaimResult]
    complete: Callable[[str, str, str, datetime], None]
    sweep_stale: Callable[[datetime], list[ClaimItem]]
    marker_exists: Callable[[str], bool]
    mark: Callable[[str, datetime, int], bool]
    get_claim: Callable[[str, str], ClaimItem | None]  # (feed_id, batch_id); GetItem on the
    # exact pk -- M-1's sanctioned additive field (security-gate): the sftp-pull resume path
    # (§9.3) must look up ONE claim by its known (feed_id, batch_id), never `sweep_stale`'s
    # DynamoDB Scan -- the per-feed driver role does not (and must not) hold `dynamodb:Scan`
    # (LeadingKeys can constrain GetItem/Query but not Scan; granting it would break the
    # per-feed CAS blast-radius wall). `sweep_stale` stays exclusively the absence detector's.


@dataclass(frozen=True)
class SftpFx:
    listdir: Callable[[str], list[RemoteFile]]
    read_chunks: Callable[[str], Iterator[bytes]]  # 1 MiB chunks


@dataclass(frozen=True)
class Effects:
    store: StoreFx
    ledger: LedgerFx
    cas: CasFx
    emit: Callable[[str, BaseModel], None]  # PutEvents; failure -> TransientError
    sftp_fx_for: Callable[[str], SftpFx]  # secret ARN -> connected SftpFx
    # (paramiko closures; host-key check per S6.7; connect 30 s / read 60 s
    #  timeouts; failures -> TransientError)
    invoke_async: Callable[[str, dict[str, Any]], None]  # (function_name, payload);
    # fire-and-forget Lambda invoke (S9.3 stuck-claim resume); failure -> TransientError
    now: Callable[[], datetime]  # aware UTC
    new_delivery_id: Callable[[], str]  # uuid4
    config: RuntimeConfig
