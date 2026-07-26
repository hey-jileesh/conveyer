"""Integration tests (LLD §12.1) for `ingestion/entrypoints/*.py` -- the four
Lambda handlers (`registrar_s3`, `sftp_pull`, `absence`, `maintenance`),
driven with fake events + monkeypatched `CONVEYER_*` env under moto.
Wiring-only assertions, mirroring the golden fixtures minimally (§7.1:
"Handlers contain wiring only") -- none of these are new golden scenarios;
each just proves the handler's own env-parsing/`build_effects`/driver-call
plumbing actually connects, which no test exercised before this file (all
four were 0% covered).

**Documented exclusion, this file only** (same reason as `test_build.py`'s
module docstring): `build_effects` always builds a `catalog_kind="glue"`
`LedgerConfig`, and the glue path cannot construct a working Iceberg table
under moto (real S3 metadata writes bypass moto's boto3-level mock -- see
`effects/ledger.py`'s own §12.5 note). Every test here monkeypatches
`effects.build.make_ledger_fx` to hand back a REAL `SqlCatalog`-backed
`LedgerFx` instead (the same substitution `tests/conftest.py::local_effects`
performs, just reached through `build_effects` itself) -- so, unlike
`test_build.py`, these handlers get a genuinely working ledger and can
exercise real registration/read-back, not just a capturing stub.

`entrypoints/maintenance.py` additionally needs `build_athena_fx` patched
(no moto Athena at all, `maintenance/optimize.py`'s own §12.5 exclusion) --
patched with a minimal `AthenaFx` whose three queries all succeed
immediately with zero result rows, so `run_maintenance` completes without
ever touching a real Athena client or sleeping.
"""

from __future__ import annotations

import json
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
import pytest
from ingestion.bootstrap.create_ledger import bootstrap_ledger
from ingestion.config import RuntimeConfig
from ingestion.core.model import (
    Completeness,
    FeedConfig,
    PasswordAuth,
    S3PushConnection,
    SftpConnection,
    SftpSecret,
    TrailerSpec,
    Trigger,
)
from ingestion.effects.ledger import LedgerConfig, build_catalog, make_ledger_fx
from ingestion.effects.records import SftpFx
from ingestion.maintenance.optimize import AthenaFx
from moto import mock_aws

_QUEUE = "conveyer-test-entrypoints-capture"


# --- CONVEYER_* env (the handlers call `config.from_env()` -- real os.environ,
# --- not the `runtime_config` fixture value directly) + moto scaffolding -----


@pytest.fixture(autouse=True)
def _runtime_env(monkeypatch: pytest.MonkeyPatch, runtime_config: RuntimeConfig) -> None:
    """Every handler builds its `Effects` from `config.from_env()` (real
    `os.environ`, §7.2) -- `runtime_config` (from `tests/conftest.py`) is
    only a plain value other fixtures/tests use to seed moto resources at
    matching bucket/table names. This fixture is the bridge: it mirrors
    `runtime_config`'s own fields into `CONVEYER_*` env vars, autouse so
    every test in this file gets a consistent, from_env()-parseable
    environment without repeating the mapping per test.
    """
    env_map = {
        "CONVEYER_ENV": runtime_config.env,
        "CONVEYER_AWS_REGION": runtime_config.aws_region,
        "CONVEYER_LANDING_BUCKET": runtime_config.landing_bucket,
        "CONVEYER_LAKE_BUCKET": runtime_config.lake_bucket,
        "CONVEYER_ARTIFACTS_BUCKET": runtime_config.artifacts_bucket,
        "CONVEYER_GLUE_DATABASE": runtime_config.glue_database,
        "CONVEYER_LEDGER_TABLE": runtime_config.ledger_table,
        "CONVEYER_CAS_TABLE": runtime_config.cas_table,
        "CONVEYER_EVENT_BUS": runtime_config.event_bus,
        "CONVEYER_REGISTRY_URI": runtime_config.registry_uri,
        "CONVEYER_ATHENA_WORKGROUP": runtime_config.athena_workgroup,
        "CONVEYER_ATHENA_OUTPUT_URI": runtime_config.athena_output_uri,
    }
    for key, value in env_map.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("CONVEYER_FEED_ID", raising=False)  # per-feed driver functions only


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


@pytest.fixture
def sql_ledger_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, runtime_config: RuntimeConfig
) -> LedgerConfig:
    """See module docstring: substitutes a real `SqlCatalog`-backed ledger
    for `build_effects`'s production glue wiring, for every test in this
    file. Returns the `LedgerConfig` so a test can re-open the same
    catalog+table afterward to assert on ledger contents.
    """
    import ingestion.effects.build as build_mod

    ledger_config = LedgerConfig(
        catalog_kind="sql",
        glue_database=runtime_config.glue_database,
        table_name=runtime_config.ledger_table,
        warehouse_uri=f"file://{tmp_path}/warehouse",
        sql_uri=f"sqlite:///{tmp_path}/catalog.db",
    )
    bootstrap_ledger(
        build_catalog(ledger_config), runtime_config.glue_database, runtime_config.ledger_table
    )

    def fake_make_ledger_fx(config: LedgerConfig) -> Any:
        del config  # build_effects's glue-typed config; the real substitution is fixed above
        return make_ledger_fx(ledger_config)

    monkeypatch.setattr(build_mod, "make_ledger_fx", fake_make_ledger_fx)
    return ledger_config


def _fake_context() -> Any:
    return types.SimpleNamespace(aws_request_id="test-request-id")


def _seed_registry(runtime_config: RuntimeConfig, feeds: list[FeedConfig]) -> None:
    s3_client = boto3.client("s3", region_name=runtime_config.aws_region)
    bucket, key = runtime_config.registry_uri.removeprefix("s3://").split("/", 1)
    payload = {"registry_version": 1, "feeds": [json.loads(f.model_dump_json()) for f in feeds]}
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode("utf-8"))


@pytest.fixture
def capture_queue_url(aws_resources: None, runtime_config: RuntimeConfig) -> str:
    del aws_resources  # ordering only: guarantees moto is active before this fixture's boto3 calls
    region = runtime_config.aws_region
    events_client = boto3.client("events", region_name=region)
    sqs = boto3.client("sqs", region_name=region)
    url = sqs.create_queue(QueueName=_QUEUE)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    events_client.put_rule(
        Name="capture-all",
        EventBusName=runtime_config.event_bus,
        EventPattern=json.dumps({"source": ["conveyer.ingestion"]}),
    )
    events_client.put_targets(
        Rule="capture-all",
        EventBusName=runtime_config.event_bus,
        Targets=[{"Id": "1", "Arn": queue_arn}],
    )
    return url


def _drain_events(runtime_config: RuntimeConfig, queue_url: str) -> list[dict[str, Any]]:
    sqs = boto3.client("sqs", region_name=runtime_config.aws_region)
    messages = sqs.receive_message(
        QueueUrl=queue_url, WaitTimeSeconds=1, MaxNumberOfMessages=10
    ).get("Messages", [])
    return [json.loads(m["Body"]) for m in messages]


# --- registrar_s3.handler -----------------------------------------------------

_TRAILER_FEED_ID = "carrier-z/trailer-feed"


def _trailer_s3_push_feed() -> FeedConfig:
    return FeedConfig(
        feed_id=_TRAILER_FEED_ID,
        driver="s3-push",
        pipeline="pipelines/trailer",
        connection=S3PushConnection(
            partner_principal_arns=["arn:aws:iam::111111111111:role/carrier-z-uploader"]
        ),
        completeness=Completeness(mode="trailer", trailer=TrailerSpec(pattern=r"TOTAL:\d+")),
    )


def test_registrar_s3_handler_registers_a_delivery_end_to_end(
    aws_resources: None,
    sql_ledger_config: LedgerConfig,
    runtime_config: RuntimeConfig,
    capture_queue_url: str,
) -> None:
    import ingestion.entrypoints.registrar_s3 as ep

    _seed_registry(runtime_config, [_trailer_s3_push_feed()])
    key = f"{_TRAILER_FEED_ID}/incoming/data.txt"
    content = b"policy_id,premium\nP1,100.00\nTOTAL:1\n"
    boto3.client("s3", region_name=runtime_config.aws_region).put_object(
        Bucket=runtime_config.landing_bucket, Key=key, Body=content
    )
    event = {"detail": {"bucket": {"name": runtime_config.landing_bucket}, "object": {"key": key}}}

    result = ep.handler(event, _fake_context())

    assert result == {"registered": 1}
    rows = make_ledger_fx(sql_ledger_config).scan_feed(_TRAILER_FEED_ID, None)
    assert len(rows) == 1
    assert rows[0].disposition == "registered"
    events = _drain_events(runtime_config, capture_queue_url)
    assert len(events) == 1
    assert events[0]["detail-type"] == "delivery-registered"


def test_registrar_s3_handler_raises_when_a_required_env_var_is_missing(
    aws_resources: None, sql_ledger_config: LedgerConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ingestion.entrypoints.registrar_s3 as ep

    # `from_env()` is the ONLY place that validates `CONVEYER_*` presence
    # (§7.2); a handler never re-validates it -- this proves the handler
    # doesn't swallow that failure, letting it fail loud into Lambda's
    # retry/DLQ/alarm path (an uncaught exception IS the alarm path, §9.4).
    monkeypatch.delenv("CONVEYER_LANDING_BUCKET", raising=False)
    event = {"detail": {"bucket": {"name": "irrelevant"}, "object": {"key": "irrelevant"}}}

    with pytest.raises(RuntimeError, match="CONVEYER_LANDING_BUCKET"):
        ep.handler(event, _fake_context())


# --- sftp_pull.handler ---------------------------------------------------------

_SFTP_FEED_ID = "carrier-x/commission-statements"
_SECRET_NAME = "conveyer-dev/sftp/carrier-x/commission-statements"


def _sftp_trailer_feed() -> FeedConfig:
    return FeedConfig(
        feed_id=_SFTP_FEED_ID,
        driver="sftp-pull",
        pipeline="pipelines/commissions",
        connection=SftpConnection(
            secret_ref=f"arn:aws:secretsmanager:us-east-1:000000000000:secret:{_SECRET_NAME}",
            remote_path="/outbound/commissions/",
        ),
        trigger=Trigger(schedule="cron(0 13 ? * MON-FRI *)", timezone="America/New_York"),
        completeness=Completeness(mode="trailer", trailer=TrailerSpec(pattern=r"TOTAL:\d+")),
    )


@pytest.fixture
def sftp_pull_env(
    aws_resources: None, monkeypatch: pytest.MonkeyPatch, runtime_config: RuntimeConfig
) -> Iterator[None]:
    del aws_resources  # ordering only: guarantees moto is active before this fixture's boto3 calls
    import ingestion.effects.sftp as sftp_mod

    monkeypatch.setenv("CONVEYER_FEED_ID", _SFTP_FEED_ID)
    monkeypatch.setattr(
        sftp_mod,
        "make_sftp_fx",
        lambda secret: SftpFx(listdir=lambda path: [], read_chunks=lambda path: iter(())),
    )
    _seed_registry(runtime_config, [_sftp_trailer_feed()])
    secret = SftpSecret(
        host="sftp.carrier-x.example.com",
        username="conveyer",
        auth=PasswordAuth(kind="password", password="hunter2"),
    )
    boto3.client("secretsmanager", region_name=runtime_config.aws_region).create_secret(
        Name=_SECRET_NAME, SecretString=secret.model_dump_json()
    )
    yield


def test_sftp_pull_handler_scheduled_run_with_empty_remote_listing(
    aws_resources: None, sql_ledger_config: LedgerConfig, sftp_pull_env: None
) -> None:
    import ingestion.entrypoints.sftp_pull as ep

    result = ep.handler({}, _fake_context())

    assert result == {"acquired": 0}


def test_sftp_pull_handler_parses_an_operator_window_and_force_payload(
    aws_resources: None, sql_ledger_config: LedgerConfig, sftp_pull_env: None
) -> None:
    import ingestion.entrypoints.sftp_pull as ep

    payload = {
        "window": {"start": "2026-07-01T00:00:00+00:00", "end": "2026-07-25T00:00:00+00:00"},
        "force": True,
    }

    result = ep.handler(payload, _fake_context())

    assert result == {"acquired": 0}  # still empty remote; proves parsing didn't raise


def test_sftp_pull_handler_requires_feed_id_env_var(
    aws_resources: None,
    sql_ledger_config: LedgerConfig,
    sftp_pull_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ingestion.entrypoints.sftp_pull as ep

    monkeypatch.delenv("CONVEYER_FEED_ID", raising=False)

    with pytest.raises(RuntimeError, match="CONVEYER_FEED_ID"):
        ep.handler({}, _fake_context())


def test_sftp_pull_handler_requires_a_registered_feed(
    aws_resources: None,
    sql_ledger_config: LedgerConfig,
    sftp_pull_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import ingestion.entrypoints.sftp_pull as ep

    monkeypatch.setenv("CONVEYER_FEED_ID", "no-such-feed/at-all")

    with pytest.raises(RuntimeError, match="no registered feed"):
        ep.handler({}, _fake_context())


# --- absence.handler -----------------------------------------------------------


def test_absence_handler_empty_registry_returns_zero_counts(
    aws_resources: None, sql_ledger_config: LedgerConfig, runtime_config: RuntimeConfig
) -> None:
    import ingestion.entrypoints.absence as ep

    _seed_registry(runtime_config, [])

    result = ep.handler({}, _fake_context())

    assert result == {"overdue_emitted": 0, "claims_recovered": 0}


# --- M-9 (security-gate): every entrypoint installs the JSON log handler,
# idempotently across warm-container (repeat) invocations. --------------------


@pytest.fixture
def _clean_root_logger():
    """Snapshot + restore the REAL root logger around the test -- `handler()`
    installs `observability.install_json_handler()` on `logging.getLogger()`
    itself (not a fixture-scoped logger, unlike `test_observability.py`'s
    unit tests), so this test must not leak a handler into the rest of the
    suite.
    """
    import logging

    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    root.handlers = []
    try:
        yield root
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)


def test_absence_handler_installs_json_log_handler_exactly_once_across_two_invocations(
    aws_resources: None,
    sql_ledger_config: LedgerConfig,
    runtime_config: RuntimeConfig,
    _clean_root_logger,
) -> None:
    import ingestion.entrypoints.absence as ep
    from ingestion.observability import _JSON_HANDLER_NAME

    _seed_registry(runtime_config, [])

    ep.handler({}, _fake_context())
    ep.handler({}, _fake_context())  # warm-container repeat invocation

    installed = [h for h in _clean_root_logger.handlers if h.name == _JSON_HANDLER_NAME]
    assert len(installed) == 1


# --- maintenance.handler --------------------------------------------------------


def test_maintenance_handler_runs_the_weekly_job_and_returns_zero_reconciled(
    aws_resources: None, sql_ledger_config: LedgerConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    import ingestion.entrypoints.maintenance as ep

    queries_started: list[str] = []

    def fake_build_athena_fx(config: RuntimeConfig) -> AthenaFx:
        del config
        return AthenaFx(
            start_query=lambda sql: queries_started.append(sql) or "qid",  # type: ignore[func-returns-value]
            poll=lambda query_execution_id: "SUCCEEDED",
            get_results=lambda query_execution_id: [],
        )

    monkeypatch.setattr(ep, "build_athena_fx", fake_build_athena_fx)

    result = ep.handler({}, _fake_context())

    assert result == {"supersessions_reconciled": 0}
    assert len(queries_started) == 3  # OPTIMIZE, VACUUM, live-duplicates
