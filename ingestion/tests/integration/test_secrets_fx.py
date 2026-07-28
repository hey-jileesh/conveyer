"""Integration tests (factory level, LLD §12.1) for
`effects.secrets.make_secret_fn` against moto Secrets Manager.

Covers the S6.7 secret schema round-trip (password and private-key auth
variants), and both failure modes collapsing to `TransientError`: a missing
secret (AWS-side `ClientError`) and a malformed `SecretString` (invalid
JSON / schema violation, S7.3's "config is platform-owned; invalid config
is an ops page, not a data defect").
"""

import json
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from ingestion.core.model import PasswordAuth, PrivateKeyAuth, SftpSecret
from ingestion.effects.records import TransientError
from ingestion.effects.secrets import make_secret_fn
from moto import mock_aws


@pytest.fixture
def secrets_client() -> Iterator[Any]:
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="us-east-1")


def test_get_sftp_secret_password_auth_round_trips(secrets_client) -> None:
    arn = secrets_client.create_secret(
        Name="conveyer/sftp/carrier-x",
        SecretString=json.dumps(
            {
                "host": "sftp.carrier-x.example.com",
                "port": 22,
                "username": "conveyer",
                "auth": {"kind": "password", "password": "hunter2"},
                "host_key_fingerprint": "SHA256:" + "a" * 43,
            }
        ),
    )["ARN"]
    fetch = make_secret_fn(secrets_client)

    secret = fetch(arn)

    assert secret == SftpSecret(
        host="sftp.carrier-x.example.com",
        port=22,
        username="conveyer",
        auth=PasswordAuth(kind="password", password="hunter2"),
        host_key_fingerprint="SHA256:" + "a" * 43,
    )


def test_get_sftp_secret_private_key_auth_and_null_fingerprint(secrets_client) -> None:
    arn = secrets_client.create_secret(
        Name="conveyer/sftp/carrier-y",
        SecretString=json.dumps(
            {
                "host": "sftp.carrier-y.example.com",
                "username": "conveyer",
                "auth": {
                    "kind": "private_key",
                    "private_key_pem": "-----BEGIN OPENSSH PRIVATE KEY-----\n...\n",
                    "passphrase": None,
                },
                "host_key_fingerprint": None,
            }
        ),
    )["ARN"]
    fetch = make_secret_fn(secrets_client)

    secret = fetch(arn)

    assert isinstance(secret.auth, PrivateKeyAuth)
    assert secret.port == 22  # default
    assert secret.host_key_fingerprint is None


def test_get_sftp_secret_missing_secret_raises_transient_error(secrets_client) -> None:
    fetch = make_secret_fn(secrets_client)

    with pytest.raises(TransientError):
        fetch("arn:aws:secretsmanager:us-east-1:123456789012:secret:does-not-exist-abcdef")


def test_get_sftp_secret_malformed_json_raises_transient_error(secrets_client) -> None:
    arn = secrets_client.create_secret(Name="conveyer/sftp/broken", SecretString="not valid json")[
        "ARN"
    ]
    fetch = make_secret_fn(secrets_client)

    with pytest.raises(TransientError):
        fetch(arn)


def test_get_sftp_secret_schema_violation_raises_transient_error(secrets_client) -> None:
    arn = secrets_client.create_secret(
        Name="conveyer/sftp/incomplete", SecretString=json.dumps({"host": "h"})
    )["ARN"]
    fetch = make_secret_fn(secrets_client)

    with pytest.raises(TransientError):
        fetch(arn)


# --- H-2 (security-gate): the raised TransientError must never leak secret ---
# material. Pydantic v2's default `str(ValidationError)` rendering includes
# `input_value=...` for a top-level union-tag failure -- which for this model
# is the WHOLE decoded secret dict, including `auth.password`. This scenario
# (an unrecognized `auth.kind` discriminator) fails validation at the model's
# top level specifically so the leaked `input_value` would be the entire dict,
# not just the offending field.


_KNOWN_PASSWORD = "hunter2-leak-probe"  # distinctive, and short enough to survive
# pydantic's own `input_value=...` truncation in its default `__str__` (a long
# password's middle gets elided by pydantic itself in that rendering, which
# would make the "still leaks via `__cause__`" sanity assertion below
# unreliable) -- must never leak into the raised `TransientError`'s message.


def test_get_sftp_secret_schema_violation_never_leaks_password_into_transient_error(
    secrets_client,
) -> None:
    arn = secrets_client.create_secret(
        Name="conveyer/sftp/leak-probe",
        SecretString=json.dumps(
            {
                "host": "sftp.example.com",
                "username": "conveyer",
                "auth": {"kind": "not-a-real-auth-kind", "password": _KNOWN_PASSWORD},
            }
        ),
    )["ARN"]
    fetch = make_secret_fn(secrets_client)

    with pytest.raises(TransientError) as exc_info:
        fetch(arn)

    assert _KNOWN_PASSWORD not in str(exc_info.value)
    assert _KNOWN_PASSWORD not in repr(exc_info.value)
    # the underlying pydantic ValidationError is still chained (`from exc`) --
    # confirm ITS default rendering DOES contain the password (proving this
    # scenario is a genuine leak vector the fix had to specifically avoid
    # re-surfacing via `str(exc)`/`{exc}` interpolation), while the raised
    # `TransientError` message itself stays clean.
    assert exc_info.value.__cause__ is not None
    assert _KNOWN_PASSWORD in str(exc_info.value.__cause__)
