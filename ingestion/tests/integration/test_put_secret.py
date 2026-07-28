"""Integration tests (factory level, LLD §12.1) for `tools.put_secret`
against moto Secrets Manager -- `make put-secret`, LLD §6.7/§10.7.

Exercises the full `main()` CLI flow: `getpass.getpass` is monkeypatched to
feed a plain string (the "feed input a string" technique -- this test never
touches a real terminal, so it cannot observe terminal echo directly; the
non-echo guarantee is that production code calls ONLY `getpass.getpass`,
never `input()`, reviewed in the module). The secret shell is pre-created
via `create_secret` before each `put_sftp_secret`/`main()` call, mirroring
D-13's "Terraform creates the shell, this tool only sets the value out of
band" split -- `PutSecretValue` against a name with no shell is a genuine
`ResourceNotFoundException` (verified live in the REPL), not something this
tool ever provisions.

Also covers: `secret_name_for`'s deterministic naming (§10.7, `${p}/sftp/
<source>/<feed>`, `CONVEYER_NAME_PREFIX` gap-fill matching
`absence/detector.py::_function_prefix`); a schema violation never reaching
`PutSecretValue` (secret stays at its pre-existing value); and that no
secret material -- including a value pydantic's own `ValidationError`
carries in its `input` key for a type-mismatch error -- is ever printed to
stdout/stderr, on success OR failure.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from ingestion.core.model import PasswordAuth, SftpSecret
from moto import mock_aws
from tools import put_secret

_ENV_VARS = {
    "CONVEYER_ENV": "dev",
    "CONVEYER_AWS_REGION": "us-east-1",
    "CONVEYER_LANDING_BUCKET": "conveyer-dev-landing",
    "CONVEYER_LAKE_BUCKET": "conveyer-dev-lake",
    "CONVEYER_ARTIFACTS_BUCKET": "conveyer-dev-artifacts",
    "CONVEYER_GLUE_DATABASE": "conveyer_dev_ingestion",
    "CONVEYER_LEDGER_TABLE": "delivery_ledger",
    "CONVEYER_CAS_TABLE": "conveyer-dev-cas",
    "CONVEYER_EVENT_BUS": "conveyer-dev-bus",
    "CONVEYER_REGISTRY_URI": "s3://conveyer-dev-artifacts/registry/feeds.json",
    "CONVEYER_ATHENA_WORKGROUP": "conveyer-dev-ingestion",
    "CONVEYER_ATHENA_OUTPUT_URI": "s3://conveyer-dev-artifacts/athena-results/",
}

_FEED_ID = "carrier-x/commission-statements"
_SECRET_PASSWORD = "hunter2"  # nosec: fixture value only, asserted NEVER to be printed
_SECRET_JSON = json.dumps(
    {
        "host": "sftp.carrier-x.example.com",
        "port": 22,
        "username": "conveyer",
        "auth": {"kind": "password", "password": _SECRET_PASSWORD},
        "host_key_fingerprint": None,
    }
)


@pytest.fixture(autouse=True)
def _runtime_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in _ENV_VARS.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("CONVEYER_NAME_PREFIX", raising=False)


@pytest.fixture
def secrets_client() -> Iterator[Any]:
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="us-east-1")


def test_secret_name_for_matches_terraforms_naming_rule(monkeypatch: pytest.MonkeyPatch) -> None:
    assert put_secret.secret_name_for(_FEED_ID, "dev") == f"conveyer-dev/sftp/{_FEED_ID}"

    monkeypatch.setenv("CONVEYER_NAME_PREFIX", "acme")

    assert put_secret.secret_name_for(_FEED_ID, "dev") == f"acme-dev/sftp/{_FEED_ID}"


def test_put_sftp_secret_round_trips_through_the_model(secrets_client: Any) -> None:
    name = put_secret.secret_name_for(_FEED_ID, "dev")
    secrets_client.create_secret(Name=name, SecretString="{}")

    result = put_secret.put_sftp_secret(secrets_client, name, _SECRET_JSON)

    assert result == SftpSecret(
        host="sftp.carrier-x.example.com",
        port=22,
        username="conveyer",
        auth=PasswordAuth(kind="password", password=_SECRET_PASSWORD),
        host_key_fingerprint=None,
    )
    stored = secrets_client.get_secret_value(SecretId=name)["SecretString"]
    assert stored == _SECRET_JSON
    assert SftpSecret.model_validate_json(stored) == result


def test_put_sftp_secret_missing_shell_raises_client_error(secrets_client: Any) -> None:
    from botocore.exceptions import ClientError

    name = put_secret.secret_name_for(_FEED_ID, "dev")  # never created -- D-13

    with pytest.raises(ClientError):
        put_secret.put_sftp_secret(secrets_client, name, _SECRET_JSON)


def test_put_sftp_secret_invalid_payload_never_reaches_aws(secrets_client: Any) -> None:
    name = put_secret.secret_name_for(_FEED_ID, "dev")
    secrets_client.create_secret(Name=name, SecretString="{}")

    result = put_secret.put_sftp_secret(secrets_client, name, json.dumps({"host": "h"}))

    assert isinstance(result, put_secret.SecretValidationFailure)
    assert result.errors
    # secret is untouched -- still the pre-existing shell value, not overwritten
    assert secrets_client.get_secret_value(SecretId=name)["SecretString"] == "{}"


def test_put_sftp_secret_type_mismatch_error_never_carries_the_value(secrets_client: Any) -> None:
    """Pydantic's `ValidationError.errors()` carries the offending value in
    an `input` key for type-mismatch errors (verified live) -- `msg`/`loc`
    never do. `_format_errors` (and therefore `SecretValidationFailure`)
    must only ever surface `loc`/`msg`.
    """
    name = put_secret.secret_name_for(_FEED_ID, "dev")
    secrets_client.create_secret(Name=name, SecretString="{}")
    bad_auth = {"kind": "password", "password": 12345}
    bad = json.dumps({"host": "h", "username": "u", "auth": bad_auth})

    result = put_secret.put_sftp_secret(secrets_client, name, bad)

    assert isinstance(result, put_secret.SecretValidationFailure)
    assert not any("12345" in err for err in result.errors)


def test_main_prompts_hidden_parses_and_stores(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    name = put_secret.secret_name_for(_FEED_ID, "dev")
    monkeypatch.setattr(sys, "argv", ["put_secret.py", "--feed", _FEED_ID, "--env", "dev"])
    monkeypatch.setattr(put_secret.getpass, "getpass", lambda prompt="": _SECRET_JSON)

    with mock_aws():
        client = boto3.client("secretsmanager", region_name="us-east-1")
        client.create_secret(Name=name, SecretString="{}")

        put_secret.main()

        stored = client.get_secret_value(SecretId=name)["SecretString"]
        assert stored == _SECRET_JSON

    out, err = capsys.readouterr()
    assert "secret stored" in out
    assert _SECRET_PASSWORD not in out
    assert _SECRET_PASSWORD not in err


def test_main_invalid_payload_exits_nonzero_and_never_prints_it(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    name = put_secret.secret_name_for(_FEED_ID, "dev")
    monkeypatch.setattr(sys, "argv", ["put_secret.py", "--feed", _FEED_ID, "--env", "dev"])
    monkeypatch.setattr(put_secret.getpass, "getpass", lambda prompt="": '{"host": "h"}')

    with mock_aws():
        client = boto3.client("secretsmanager", region_name="us-east-1")
        client.create_secret(Name=name, SecretString="{}")

        with pytest.raises(SystemExit):
            put_secret.main()

        assert client.get_secret_value(SecretId=name)["SecretString"] == "{}"

    out, err = capsys.readouterr()
    assert '{"host": "h"}' not in out
    assert '{"host": "h"}' not in err


def test_main_env_mismatch_exits_without_prompting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["put_secret.py", "--feed", _FEED_ID, "--env", "prod"])
    prompted: list[str] = []
    monkeypatch.setattr(
        put_secret.getpass, "getpass", lambda prompt="": prompted.append(prompt) or ""
    )

    with pytest.raises(SystemExit):
        put_secret.main()

    assert prompted == []  # never got as far as prompting -- fails fast on the env check
