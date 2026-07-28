"""Put the SFTP secret JSON for a feed into Secrets Manager -- LLD §6.7,
§10.7, §10.8 step 7 (`make put-secret`).

Terraform creates the per-feed Secrets Manager secret *shell* only (D-13);
this tool sets its VALUE out-of-band, so the secret payload never touches
Terraform state. The secret NAME this tool writes to is deterministic
(`${p}/sftp/<source>/<feed>`, §10.7) -- the SAME name Terraform's
`aws_secretsmanager_secret` resource creates, and the convention §15.1's
committed `source.yaml` `secret_ref` placeholder ARN relies on
(`name_prefix` read from `CONVEYER_NAME_PREFIX`, default `"conveyer"` --
`RuntimeConfig` carries no `name_prefix` field, §7.2, so this is the same
effect-side env-var gap-fill pattern `absence/detector.py::_function_prefix`
established for the identical `${p}` gap).

Input is read via `getpass.getpass` -- NEVER echoed to the terminal, and
never printed anywhere in this module, including on validation failure
(only structural pydantic error messages -- location + message -- are ever
printed, never the payload itself). The payload is PARSED, not trusted:
`SftpSecret.model_validate_json` (§6.7) must succeed before
`PutSecretValue` is ever called against AWS.
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from dataclasses import dataclass
from typing import Any

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from ingestion.config import from_env
from ingestion.core.model import SftpSecret
from pydantic import ValidationError

_NAME_PREFIX_ENV = "CONVEYER_NAME_PREFIX"
_DEFAULT_NAME_PREFIX = "conveyer"  # Terraform's own `name_prefix` default, §10.1


def secret_name_for(feed_id: str, env: str) -> str:
    """Deterministic Secrets Manager secret NAME (not ARN) -- LLD §10.7:
    `${p}/sftp/<source>/<feed>` where `${p} = ${name_prefix}-${env}`.
    `PutSecretValue`'s `SecretId` accepts a friendly name in place of a full
    ARN, so no account-id/region-qualified ARN is needed here (§15.1's
    committed `secret_ref` placeholder ARN is illustrative only -- "the ARN
    pattern is deterministic from the naming rule in §10.7").
    """
    name_prefix = os.environ.get(_NAME_PREFIX_ENV, _DEFAULT_NAME_PREFIX)
    return f"{name_prefix}-{env}/sftp/{feed_id}"


@dataclass(frozen=True)
class SecretValidationFailure:
    """A schema violation (§6.7) -- `errors` never includes the raw payload,
    only pydantic's `(location, message)` pairs: parse, don't trust, and
    never leak secret material into logs on a bad paste.
    """

    errors: tuple[str, ...]


def _format_errors(exc: ValidationError) -> tuple[str, ...]:
    formatted = []
    for err in exc.errors():
        loc = ".".join(str(part) for part in err["loc"])
        location = f"{loc}: " if loc else ""
        formatted.append(f"{location}{err['msg']}")
    return tuple(formatted)


def put_sftp_secret(
    client: Any, secret_id: str, raw_json: str
) -> SftpSecret | SecretValidationFailure:
    """Parse+validate `raw_json` as `SftpSecret` (§6.7) THEN `PutSecretValue`
    -- a malformed payload never reaches AWS. Returns the validated
    `SftpSecret` on success, or a `SecretValidationFailure` on a schema
    violation. `ClientError` (e.g. the secret shell doesn't exist yet --
    D-13 says Terraform must create it first) propagates to the caller
    unchanged; `main()` turns it into a clear operator-facing message.
    """
    try:
        secret = SftpSecret.model_validate_json(raw_json)
    except ValidationError as exc:
        return SecretValidationFailure(_format_errors(exc))
    client.put_secret_value(SecretId=secret_id, SecretString=raw_json)
    return secret


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Put the SFTP secret JSON for a feed into Secrets Manager "
            "(LLD §6.7, §10.8 step 7). Input is read hidden, never echoed."
        )
    )
    parser.add_argument(
        "--feed", required=True, help="feed_id, e.g. carrier-x/commission-statements"
    )
    parser.add_argument(
        "--env", required=True, help="Deploy environment (must match CONVEYER_ENV)."
    )
    args = parser.parse_args()

    config = from_env()
    if args.env != config.env:
        raise SystemExit(f"--env {args.env!r} does not match CONVEYER_ENV={config.env!r}")

    secret_id = secret_name_for(args.feed, config.env)
    raw_json = getpass.getpass(
        f"Paste the SFTP secret JSON for {args.feed!r} (§6.7 shape, one line, input hidden): "
    )

    client = boto3.client("secretsmanager", region_name=config.aws_region)
    try:
        result = put_sftp_secret(client, secret_id, raw_json)
    except ClientError as exc:
        raise SystemExit(f"PutSecretValue failed for {secret_id!r}: {exc}") from exc

    if isinstance(result, SecretValidationFailure):
        for err in result.errors:
            print(f"  {err}", file=sys.stderr)
        raise SystemExit(f"invalid SFTP secret JSON for {args.feed!r} -- nothing written")

    print(f"secret stored at {secret_id!r}")


if __name__ == "__main__":
    main()
