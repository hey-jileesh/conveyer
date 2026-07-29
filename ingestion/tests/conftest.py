"""Local-stack fixtures -- LLD §12.5.

`local_effects` is the central fixture: the SAME `Effects` record shape
`effects/build.py::build_effects` assembles for production, built instead
from moto clients (S3, DynamoDB, EventBridge, Secrets Manager), a
`SqlCatalog` ledger (bootstrap-created via
`ingestion.bootstrap.create_ledger.bootstrap_ledger` -- so every test using
this fixture also exercises the bootstrap path), an in-memory sftp store
behind `sftp_fx_for`, a controllable `now()`, and a seeded deterministic
`new_delivery_id` sequence. No mocking framework anywhere (§12.2 IDIOM
rule); every test double here is a plain function or a mutable container a
test can seed directly -- never a patched/mocked object.

Function-scoped (pytest's default): every test gets a fresh moto account,
fresh SqlCatalog-backed ledger, and empty CAS table/sftp store, so tests
never see another test's state.
"""

from __future__ import annotations

import functools
import itertools
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import boto3
import pytest
from ingestion.bootstrap.create_ledger import bootstrap_ledger
from ingestion.config import RuntimeConfig
from ingestion.core.windows import RemoteFile
from ingestion.effects import cas as cas_mod
from ingestion.effects import events as events_mod
from ingestion.effects import s3 as s3_mod
from ingestion.effects.ledger import LedgerConfig, build_catalog, make_ledger_fx
from ingestion.effects.records import Effects, SftpFx
from moto import mock_aws

LANDING_BUCKET = "conveyer-test-landing"
LAKE_BUCKET = "conveyer-test-lake"
ARTIFACTS_BUCKET = "conveyer-test-artifacts"
GLUE_DATABASE = "conveyer_test_ingestion"
LEDGER_TABLE = "delivery_ledger"
CAS_TABLE = "conveyer-test-cas"
EVENT_BUS = "conveyer-test-bus"

# In-memory SFTP store, exposed as its own fixture so tests can seed remote
# files before invoking a driver: `secret_arn -> {remote_path: (content, mtime)}`.
SftpStore = dict[str, dict[str, tuple[bytes, datetime]]]


def _fake_sftp_fx(store: SftpStore, secret_arn: str) -> SftpFx:
    files = store.setdefault(secret_arn, {})

    def _listdir(path: str) -> list[RemoteFile]:
        prefix = path if path.endswith("/") else path + "/"
        return [
            RemoteFile(name=name[len(prefix) :], bytes=len(content), mtime=mtime)
            for name, (content, mtime) in files.items()
            if name.startswith(prefix) and "/" not in name[len(prefix) :]
        ]

    def _read_chunks(path: str) -> Iterator[bytes]:
        content, _mtime = files[path]
        yield content

    return SftpFx(listdir=_listdir, read_chunks=_read_chunks)


@pytest.fixture
def sftp_store() -> SftpStore:
    return {}


@pytest.fixture
def clock_box() -> list[datetime]:
    """A one-element mutable box holding "now" -- tests control time by
    assigning `clock_box[0] = ...`; `Effects.now` always reads the current
    value (the closure captures the list, not a snapshot).
    """
    return [datetime(2026, 1, 1, tzinfo=UTC)]


@pytest.fixture
def invoke_log() -> list[tuple[str, dict[str, Any]]]:
    """Records every `Effects.invoke_async` call a test's driver code makes
    -- `(function_name, payload)` tuples, in call order. No mocking
    framework (§12.2 IDIOM): `local_effects` wires a plain closure over this
    list as `invoke_async`, mirroring `sftp_store`/`clock_box`'s "mutable
    box a test can both seed and read" shape.
    """
    return []


@pytest.fixture
def runtime_config() -> RuntimeConfig:
    return RuntimeConfig(
        env="test",
        aws_region="us-east-1",
        landing_bucket=LANDING_BUCKET,
        lake_bucket=LAKE_BUCKET,
        artifacts_bucket=ARTIFACTS_BUCKET,
        glue_database=GLUE_DATABASE,
        ledger_table=LEDGER_TABLE,
        cas_table=CAS_TABLE,
        event_bus=EVENT_BUS,
        registry_uri=f"s3://{ARTIFACTS_BUCKET}/registry/feeds.json",
        athena_workgroup="conveyer-test-workgroup",
        athena_output_uri=f"s3://{ARTIFACTS_BUCKET}/athena-output/",
        maintenance_tables=(f"{GLUE_DATABASE}.{LEDGER_TABLE}",),
        feed_id=None,
    )


def _record_invoke(
    log: list[tuple[str, dict[str, Any]]], function_name: str, payload: dict[str, Any]
) -> None:
    log.append((function_name, payload))


@pytest.fixture
def local_effects(
    tmp_path,
    runtime_config: RuntimeConfig,
    sftp_store: SftpStore,
    clock_box: list[datetime],
    invoke_log: list[tuple[str, dict[str, Any]]],
) -> Iterator[Effects]:
    with mock_aws():
        s3_client = boto3.client("s3", region_name=runtime_config.aws_region)
        for bucket in (LANDING_BUCKET, LAKE_BUCKET, ARTIFACTS_BUCKET):
            s3_client.create_bucket(Bucket=bucket)
        # LLD S10.1: "${p}-landing | Versioning on." -- the lake bucket is
        # explicitly versioning OFF ("Iceberg manages its own history"), so only
        # the landing bucket gets this. H-1 (security-gate, TOCTOU): the vestibule
        # object's VersionId is only available to `stream_sha256`/`copy_verbatim`
        # when the underlying bucket is actually versioned, matching production.
        s3_client.put_bucket_versioning(
            Bucket=LANDING_BUCKET, VersioningConfiguration={"Status": "Enabled"}
        )

        dynamodb_client = boto3.client("dynamodb", region_name=runtime_config.aws_region)
        dynamodb_client.create_table(
            TableName=CAS_TABLE,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        events_client = boto3.client("events", region_name=runtime_config.aws_region)
        events_client.create_event_bus(Name=EVENT_BUS)

        ledger_config = LedgerConfig(
            catalog_kind="sql",
            glue_database=GLUE_DATABASE,
            table_name=LEDGER_TABLE,
            warehouse_uri=f"file://{tmp_path}/warehouse",
            sql_uri=f"sqlite:///{tmp_path}/catalog.db",
        )
        bootstrap_ledger(build_catalog(ledger_config), GLUE_DATABASE, LEDGER_TABLE)

        counter = itertools.count(1)

        def _new_delivery_id() -> str:
            return str(uuid.UUID(int=next(counter)))

        yield Effects(
            store=s3_mod.make_store_fx(s3_client),
            ledger=make_ledger_fx(ledger_config),
            cas=cas_mod.make_cas_fx(dynamodb_client, CAS_TABLE),
            emit=events_mod.make_event_fx(events_client, EVENT_BUS),
            sftp_fx_for=lambda arn: _fake_sftp_fx(sftp_store, arn),
            invoke_async=functools.partial(_record_invoke, invoke_log),
            now=lambda: clock_box[0],
            new_delivery_id=_new_delivery_id,
            config=runtime_config,
        )
