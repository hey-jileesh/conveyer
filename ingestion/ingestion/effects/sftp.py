"""make_sftp_fx (paramiko closures) -- LLD S7.7 / S6.7.

Real paramiko is not exercisable under moto -- no AWS mock covers SFTP
(S12.5). This module therefore has no `tests/integration` counterpart; the
sftp path is instead covered by the in-memory `SftpFx` record (plain local
functions, no real network) used in the golden suite (S12.1). Document that
exclusion here, per §12.5's "document each exclusion inline."

`make_sftp_fx(secret)` performs the actual connect (S7.7: "make_<x>_fx
factory returning the record with those functions' clients closed over") --
by the time it returns, the `paramiko.SFTPClient` inside its closures is
live. `effects/build.py` (out of this bead's scope) composes this with
`effects/secrets.py::make_secret_fn` to build `Effects.sftp_fx_for: str ->
SftpFx` (secret ARN -> connected SftpFx).
"""

from __future__ import annotations

import base64
import functools
import hashlib
import io
import logging
import socket
import stat
from collections.abc import Iterator
from datetime import UTC, datetime

import paramiko  # type: ignore[import-untyped]

from ingestion.core.model import PrivateKeyAuth, SftpSecret
from ingestion.core.windows import RemoteFile
from ingestion.effects.records import SftpFx, TransientError

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT_S = 30
_READ_TIMEOUT_S = 60
_READ_CHUNK_BYTES = 1024 * 1024  # 1 MiB

# Auto-detect order for `private_key_pem` -- the S6.7 secret schema carries no
# explicit key-type field, so each class is tried in turn; a wrong-type
# attempt raises `paramiko.SSHException`, not a crash (LLD does not specify
# an order; DSA/`DSSKey` is absent from paramiko 5 -- deprecated/removed).
_PRIVATE_KEY_CLASSES: tuple[type[paramiko.PKey], ...] = (
    paramiko.RSAKey,
    paramiko.Ed25519Key,
    paramiko.ECDSAKey,
)


def _load_private_key(pem: str, passphrase: str | None) -> paramiko.PKey:
    last_exc: Exception | None = None
    for key_cls in _PRIVATE_KEY_CLASSES:
        try:
            return key_cls.from_private_key(io.StringIO(pem), password=passphrase)
        except paramiko.SSHException as exc:
            last_exc = exc
            continue
    raise TransientError(f"unable to parse SFTP private key: {last_exc}")


def _host_key_fingerprint(key: paramiko.PKey) -> str:
    """SHA-256 base64 OpenSSH format: `SHA256:<base64-no-padding>` of the raw
    key bytes (matches `ssh-keygen -E sha256 -lf` output, S6.7).
    """
    digest = hashlib.sha256(key.asbytes()).digest()
    return "SHA256:" + base64.b64encode(digest).decode("ascii").rstrip("=")


def _verify_host_key(transport: paramiko.Transport, secret: SftpSecret) -> None:
    server_key = transport.get_remote_server_key()
    observed = _host_key_fingerprint(server_key)
    if secret.host_key_fingerprint is None:
        logger.warning(
            "sftp first-connection trust: no host_key_fingerprint configured for "
            "%s:%s; trusting host key %s on first connection",
            secret.host,
            secret.port,
            observed,
        )
        return
    if observed != secret.host_key_fingerprint:
        raise TransientError(
            f"sftp host key fingerprint mismatch for {secret.host}:{secret.port}: "
            f"expected {secret.host_key_fingerprint}, observed {observed}"
        )


def _authenticate(transport: paramiko.Transport, secret: SftpSecret) -> None:
    if isinstance(secret.auth, PrivateKeyAuth):
        pkey = _load_private_key(secret.auth.private_key_pem, secret.auth.passphrase)
        transport.auth_publickey(secret.username, pkey)
    else:
        transport.auth_password(secret.username, secret.auth.password)


def _connect(secret: SftpSecret) -> paramiko.SFTPClient:
    endpoint = f"{secret.host}:{secret.port}"
    try:
        sock = socket.create_connection((secret.host, secret.port), timeout=_CONNECT_TIMEOUT_S)
    except OSError as exc:
        raise TransientError(f"sftp TCP connect failed for {endpoint}: {exc}") from exc

    transport = paramiko.Transport(sock)
    try:
        transport.banner_timeout = _CONNECT_TIMEOUT_S
        transport.start_client(timeout=_CONNECT_TIMEOUT_S)
        _verify_host_key(transport, secret)
        _authenticate(transport, secret)
        sftp = paramiko.SFTPClient.from_transport(transport)
    except (paramiko.SSHException, OSError, EOFError) as exc:
        transport.close()
        raise TransientError(f"sftp connect failed for {endpoint}: {exc}") from exc

    if sftp is None:
        transport.close()
        raise TransientError(f"sftp connect failed for {endpoint}: no SFTP session")
    sftp.get_channel().settimeout(_READ_TIMEOUT_S)  # type: ignore[union-attr]
    return sftp


def _listdir(sftp: paramiko.SFTPClient, path: str) -> list[RemoteFile]:
    try:
        entries = sftp.listdir_attr(path)
    except (paramiko.SSHException, OSError) as exc:
        raise TransientError(f"sftp listdir failed for {path}: {exc}") from exc
    files: list[RemoteFile] = []
    for entry in entries:
        if entry.st_mode is not None and stat.S_ISDIR(entry.st_mode):
            continue
        files.append(
            RemoteFile(
                name=entry.filename,
                bytes=entry.st_size or 0,
                mtime=datetime.fromtimestamp(entry.st_mtime or 0, tz=UTC),
            )
        )
    return files


def _read_chunks(sftp: paramiko.SFTPClient, path: str) -> Iterator[bytes]:
    try:
        with sftp.open(path, "rb") as f:
            while True:
                chunk = f.read(_READ_CHUNK_BYTES)
                if not chunk:
                    break
                yield chunk
    except (paramiko.SSHException, OSError) as exc:
        raise TransientError(f"sftp read failed for {path}: {exc}") from exc


def make_sftp_fx(secret: SftpSecret) -> SftpFx:
    sftp = _connect(secret)
    return SftpFx(
        listdir=functools.partial(_listdir, sftp),
        read_chunks=functools.partial(_read_chunks, sftp),
    )
