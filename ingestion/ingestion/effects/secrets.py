"""make_secret_fn (Secrets Manager) -- LLD S7.7 / S6.7.

Named `make_secret_fn` (not `make_secret_fx`): there is no `SecretsFx`
sub-record in `effects/records.py` -- `Effects.sftp_fx_for` composes this
function's output (an `SftpSecret`) with `effects/sftp.py::make_sftp_fx` in
`effects/build.py` (out of this bead's scope). This factory returns the
bare `Callable[[str], SftpSecret]` directly, same pattern as
`effects/events.py::make_event_fx`.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any

from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from pydantic import ValidationError

from ingestion.core.model import SftpSecret
from ingestion.effects.records import TransientError


def _format_validation_errors(exc: ValidationError) -> str:
    """(loc, type) pairs only -- H-2 (security-gate): pydantic v2's default
    `str(ValidationError)` rendering includes `input_value=...` for each
    error, which for a top-level failure on this model (e.g. an unknown
    `auth.kind` tag) is the WHOLE decoded secret dict, including
    `auth.password`. Mirrors `tools/put_secret.py::_format_errors`'
    (loc, message) shape but drops `msg` too -- a custom validator's
    message could itself echo part of the input -- keeping only pydantic's
    own closed-vocabulary `type` string, which never carries payload data.
    """
    parts = []
    for err in exc.errors():
        loc = ".".join(str(part) for part in err["loc"])
        parts.append(f"{loc}: {err['type']}" if loc else err["type"])
    return "; ".join(parts)


def _get_sftp_secret(client: Any, secret_arn: str) -> SftpSecret:
    """GetSecretValue -> `SftpSecret` (S6.7). Both an AWS-side failure
    (throttling, missing secret) and a malformed `SecretString` (invalid
    JSON, schema violation) become `TransientError` -- config is
    platform-owned, so an invalid secret is an ops page, not a data defect.
    """
    try:
        response = client.get_secret_value(SecretId=secret_arn)
    except ClientError as exc:
        raise TransientError(f"GetSecretValue failed for {secret_arn}: {exc}") from exc
    try:
        return SftpSecret.model_validate_json(response["SecretString"])
    except ValidationError as exc:
        # NEVER interpolate `exc`/`str(exc)` here -- see `_format_validation_errors`.
        raise TransientError(
            f"malformed SFTP secret at {secret_arn}: {_format_validation_errors(exc)}"
        ) from exc
    except (KeyError, UnicodeError) as exc:
        # Structural failures only (missing `SecretString` key / bad JSON bytes) --
        # neither carries the secret's own field values, unlike `ValidationError`.
        raise TransientError(f"malformed SFTP secret at {secret_arn}: {exc}") from exc


def make_secret_fn(client: Any) -> Callable[[str], SftpSecret]:
    return functools.partial(_get_sftp_secret, client)
