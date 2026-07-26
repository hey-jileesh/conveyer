"""Integration tests (factory level, LLD §12.1) for `effects.build.build_effects`
-- the ONE production `Effects` assembly point (§7.7). Never directly
exercised before this file (only referenced from entrypoint handlers and
`conftest.py`'s docstring) -- part of the F4/F5 coverage shortfall (LLD
§12.5).

**Documented exclusion, this file only** (mirrors `effects/ledger.py`'s own
§12.5 note for the identical reason): `build_effects` always constructs its
`LedgerConfig` with `catalog_kind="glue"`, and `make_ledger_fx` eagerly calls
`catalog.load_table(...)` at construction time. Under moto, a `GlueCatalog`
can be *constructed* (verified in the kernel -- no network call happens
until an API method is invoked), but *loading* a real Iceberg table needs
pyarrow's own S3 filesystem to write/read `metadata.json`, which bypasses
moto's boto3-level mock entirely (the same limitation `effects/ledger.py`'s
module docstring already documents for its own tests). So every test here
that calls `build_effects` end-to-end monkeypatches `effects.build.
make_ledger_fx` to a lightweight fake `LedgerFx` -- these tests assert
`build_effects`'s *wiring* (which config it hands to which factory, which
client goes to which capability, how `sftp_fx_for`/`invoke_async` behave),
never the ledger's own internals (already covered by `test_ledger_fx.py`
and the golden suite via `tests/conftest.py`'s `SqlCatalog`-backed
`local_effects`).

`ledger.build_catalog`'s actual glue-vs-sql SELECTION logic (§6.2/§7.7,
D-7) -- the two-branch `if config.catalog_kind == "sql": ... else: ...`
`GlueCatalog`/`SqlCatalog` dispatch, plus both `ValueError` guards -- IS
fully exercised here directly (no monkeypatch needed: constructing either
catalog class, or hitting a guard, never touches the network), closing a
real branch-coverage gap `test_ledger_fx.py` left (it only ever builds a
`catalog_kind="sql"` `LedgerConfig`).
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from ingestion.config import RuntimeConfig
from ingestion.core.model import PasswordAuth, SftpSecret
from ingestion.effects.ledger import LedgerConfig, build_catalog
from ingestion.effects.records import Effects, LedgerFx, SftpFx, TransientError
from moto import mock_aws
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.catalog.sql import SqlCatalog

# --- shared moto scaffolding: everything `build_effects`'s clients need to --
# --- construct without erroring, EXCEPT the ledger (see module docstring) --


@pytest.fixture
def aws_resources(runtime_config: RuntimeConfig) -> Iterator[None]:
    with mock_aws():
        s3_client = boto3.client("s3", region_name=runtime_config.aws_region)
        for bucket in (
            runtime_config.landing_bucket,
            runtime_config.lake_bucket,
            runtime_config.artifacts_bucket,
        ):
            s3_client.create_bucket(Bucket=bucket)

        dynamodb_client = boto3.client("dynamodb", region_name=runtime_config.aws_region)
        dynamodb_client.create_table(
            TableName=runtime_config.cas_table,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        events_client = boto3.client("events", region_name=runtime_config.aws_region)
        events_client.create_event_bus(Name=runtime_config.event_bus)

        yield


def _fake_ledger_fx() -> LedgerFx:
    return LedgerFx(append=lambda rows: None, scan_feed=lambda feed_id, since: [])


def _patch_ledger_capture(monkeypatch: pytest.MonkeyPatch) -> dict[str, LedgerConfig]:
    """Monkeypatches `effects.build.make_ledger_fx` (the module docstring's
    documented exclusion) with a fake that records the `LedgerConfig`
    `build_effects` passed it, so a test can assert build_effects's own
    wiring decision without needing a working Iceberg table.
    """
    import ingestion.effects.build as build_mod

    captured: dict[str, LedgerConfig] = {}

    def fake_make_ledger_fx(config: LedgerConfig) -> LedgerFx:
        captured["config"] = config
        return _fake_ledger_fx()

    monkeypatch.setattr(build_mod, "make_ledger_fx", fake_make_ledger_fx)
    return captured


# --- build_effects: ledger wiring / glue selection ---------------------------


def test_build_effects_wires_a_glue_ledger_config_derived_from_runtime_config(
    aws_resources: None, runtime_config: RuntimeConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ingestion.effects.build as build_mod

    captured = _patch_ledger_capture(monkeypatch)

    effects = build_mod.build_effects(runtime_config)

    ledger_config = captured["config"]
    assert ledger_config.catalog_kind == "glue"
    assert ledger_config.glue_database == runtime_config.glue_database
    assert ledger_config.table_name == runtime_config.ledger_table
    assert ledger_config.warehouse_uri == f"s3://{runtime_config.lake_bucket}/ledger/"
    assert ledger_config.sql_uri is None  # tests-only field, never set in production

    # The rest of the record is wired too, not just the ledger.
    assert effects.config is runtime_config
    assert effects.store.list_prefix(runtime_config.landing_bucket, "no-such-prefix/") == []
    assert effects.ledger.scan_feed("no-such-feed", None) == []  # the fake, but reachable


def test_build_catalog_selects_glue_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    # `GlueCatalog.__init__` builds a boto3 client eagerly, which needs a
    # resolvable region -- Lambda always sets `AWS_REGION` at cold start; a
    # local run needs the equivalent env var (no network call happens at
    # construction time, verified in the kernel).
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    config = LedgerConfig(
        catalog_kind="glue",
        glue_database="db",
        table_name="delivery_ledger",
        warehouse_uri="s3://some-lake-bucket/ledger/",
    )

    catalog = build_catalog(config)

    assert isinstance(catalog, GlueCatalog)


def test_build_catalog_selects_sql_catalog(tmp_path: Any) -> None:
    config = LedgerConfig(
        catalog_kind="sql",
        glue_database="db",
        table_name="delivery_ledger",
        warehouse_uri=f"file://{tmp_path}/warehouse",
        sql_uri=f"sqlite:///{tmp_path}/catalog.db",
    )

    catalog = build_catalog(config)

    assert isinstance(catalog, SqlCatalog)


def test_build_catalog_glue_requires_warehouse_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    config = LedgerConfig(
        catalog_kind="glue", glue_database="db", table_name="delivery_ledger", warehouse_uri=None
    )

    with pytest.raises(ValueError, match="catalog_kind='glue' requires warehouse_uri"):
        build_catalog(config)


def test_build_catalog_sql_requires_both_sql_uri_and_warehouse_uri(tmp_path: Any) -> None:
    missing_sql_uri = LedgerConfig(
        catalog_kind="sql",
        glue_database="db",
        table_name="delivery_ledger",
        warehouse_uri=f"file://{tmp_path}/warehouse",
        sql_uri=None,
    )
    missing_warehouse_uri = LedgerConfig(
        catalog_kind="sql",
        glue_database="db",
        table_name="delivery_ledger",
        warehouse_uri=None,
        sql_uri=f"sqlite:///{tmp_path}/catalog.db",
    )

    for config in (missing_sql_uri, missing_warehouse_uri):
        with pytest.raises(ValueError, match="catalog_kind='sql' requires both"):
            build_catalog(config)


# --- build_effects: sftp_fx_for memoization ----------------------------------


def _seed_sftp_secret(runtime_config: RuntimeConfig) -> str:
    secrets_client = boto3.client("secretsmanager", region_name=runtime_config.aws_region)
    payload = SftpSecret(
        host="sftp.carrier-x.example.com",
        username="conveyer",
        auth=PasswordAuth(kind="password", password="hunter2"),
    )
    return secrets_client.create_secret(
        Name="conveyer-dev/sftp/carrier-x/commission-statements",
        SecretString=payload.model_dump_json(),
    )["ARN"]


def test_sftp_fx_for_memoizes_per_secret_arn(
    aws_resources: None, runtime_config: RuntimeConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ingestion.effects.build as build_mod
    import ingestion.effects.sftp as sftp_mod

    _patch_ledger_capture(monkeypatch)
    connect_calls: list[SftpSecret] = []

    def fake_make_sftp_fx(secret: SftpSecret) -> SftpFx:
        connect_calls.append(secret)
        return SftpFx(listdir=lambda path: [], read_chunks=lambda path: iter(()))

    monkeypatch.setattr(sftp_mod, "make_sftp_fx", fake_make_sftp_fx)

    arn_a = _seed_sftp_secret(runtime_config)
    secrets_client = boto3.client("secretsmanager", region_name=runtime_config.aws_region)
    arn_b = secrets_client.create_secret(
        Name="conveyer-dev/sftp/carrier-y/renewal-statements",
        SecretString=SftpSecret(
            host="sftp.carrier-y.example.com",
            username="conveyer",
            auth=PasswordAuth(kind="password", password="s3cr3t"),
        ).model_dump_json(),
    )["ARN"]

    effects = build_mod.build_effects(runtime_config)

    fx_a_first = effects.sftp_fx_for(arn_a)
    fx_a_second = effects.sftp_fx_for(arn_a)
    fx_b = effects.sftp_fx_for(arn_b)

    assert fx_a_first is fx_a_second  # same secret ARN -> the cached connection, not a new one
    assert fx_a_first is not fx_b  # a different secret ARN -> a genuinely new connection
    assert len(connect_calls) == 2  # one real "connect" per distinct ARN, not per call


# --- build_effects: invoke_async ---------------------------------------------


def test_invoke_async_translates_client_error_to_transient_error(
    aws_resources: None, runtime_config: RuntimeConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ingestion.effects.build as build_mod

    _patch_ledger_capture(monkeypatch)
    effects: Effects = build_mod.build_effects(runtime_config)

    with pytest.raises(TransientError, match="does-not-exist"):
        effects.invoke_async("does-not-exist", {"resume_batch_id": "b-1"})


def test_invoke_async_fires_a_real_invoke_without_raising(
    aws_resources: None, runtime_config: RuntimeConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ingestion.effects.build as build_mod

    _patch_ledger_capture(monkeypatch)
    lambda_client = boto3.client("lambda", region_name=runtime_config.aws_region)
    iam_client = boto3.client("iam", region_name=runtime_config.aws_region)
    role_arn = iam_client.create_role(
        RoleName="conveyer-test-lambda-role", AssumeRolePolicyDocument="{}"
    )["Role"]["Arn"]
    package = io.BytesIO()
    with zipfile.ZipFile(package, "w") as zf:
        zf.writestr("lambda_function.py", "def handler(event, context):\n    return event\n")
    lambda_client.create_function(
        FunctionName="conveyer-test-resume-target",
        Runtime="python3.12",
        Role=role_arn,
        Handler="lambda_function.handler",
        Code={"ZipFile": package.getvalue()},
    )

    effects: Effects = build_mod.build_effects(runtime_config)

    # No exception is the assertion -- `invoke_async` is fire-and-forget and
    # always returns `None` on success (§7.7); moto's `Invoke` response
    # itself is not inspected, since `_invoke_async` never reads it either.
    effects.invoke_async("conveyer-test-resume-target", {"resume_batch_id": "b-1"})
