"""build_effects(RuntimeConfig) -> Effects -- the ONLY production assembly
point (LLD §7.7). Every boto3 client and the pyiceberg catalog are built
exactly once, here, and closed over by the `make_*_fx` factories; nothing
else in the codebase constructs a client. `tests/conftest.py`'s
`local_effects` fixture assembles the same `Effects` record shape from moto
clients + a `SqlCatalog` instead of calling this function (§12.5).

`Effects.sftp_fx_for` is this bead's composition of `effects/secrets.py`'s
`make_secret_fn` (ARN -> `SftpSecret`) with `effects/sftp.py`'s
`make_sftp_fx` (`SftpSecret` -> connected `SftpFx`) -- flagged as
build.py's job by the m2-effects-stack bead that built those two modules.
Connections are memoized per secret ARN in a plain `dict` closure (state
lives inside this one `build_effects` call's activation, §7.7's
`stream_upload` precedent) so a driver iterating many deliveries against
the same feed within one warm Lambda container reuses one SFTP session
instead of reconnecting per delivery.

`Effects.invoke_async` (m5-absence's sanctioned additive field, §9.3) is
wired directly here -- a private `_invoke_async` closes over a `boto3
lambda` client, same "one private module-level function + TransientError
translation" shape `effects/*.py` uses throughout (see
[[m2-effects-stack-design-notes]]), kept in `build.py` rather than a new
`effects/lambda_.py` module per the brief's "minimal, not a general
invocation abstraction" instruction.
"""

from __future__ import annotations

import functools
import json
import uuid
from datetime import UTC, datetime
from typing import Any

import boto3  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from ingestion.config import RuntimeConfig
from ingestion.effects import cas, events, s3, secrets, sftp
from ingestion.effects.ledger import LedgerConfig, make_ledger_fx
from ingestion.effects.records import Effects, SftpFx, TransientError


def _now() -> datetime:
    return datetime.now(UTC)


def _new_delivery_id() -> str:
    return str(uuid.uuid4())


def _sftp_fx_for(secrets_client: object, cache: dict[str, SftpFx], secret_arn: str) -> SftpFx:
    if secret_arn not in cache:
        secret = secrets.make_secret_fn(secrets_client)(secret_arn)
        cache[secret_arn] = sftp.make_sftp_fx(secret)
    return cache[secret_arn]


def _invoke_async(client: Any, function_name: str, payload: dict[str, Any]) -> None:
    """Fire-and-forget Lambda invoke (`InvocationType="Event"`) -- §9.3's
    stuck-claim resume. Any `ClientError` becomes `TransientError` (§7.3),
    same translation every other `effects/*.py` capability uses.
    """
    try:
        client.invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=json.dumps(payload).encode("utf-8"),
        )
    except ClientError as exc:
        raise TransientError(f"async invoke of {function_name!r} failed: {exc}") from exc


def build_effects(config: RuntimeConfig) -> Effects:
    s3_client = boto3.client("s3", region_name=config.aws_region)
    dynamodb_client = boto3.client("dynamodb", region_name=config.aws_region)
    events_client = boto3.client("events", region_name=config.aws_region)
    secrets_client = boto3.client("secretsmanager", region_name=config.aws_region)
    lambda_client = boto3.client("lambda", region_name=config.aws_region)

    ledger_config = LedgerConfig(
        catalog_kind="glue",
        glue_database=config.glue_database,
        table_name=config.ledger_table,
        warehouse_uri=f"s3://{config.lake_bucket}/ledger/",
    )
    sftp_cache: dict[str, SftpFx] = {}

    return Effects(
        store=s3.make_store_fx(s3_client),
        ledger=make_ledger_fx(ledger_config),
        cas=cas.make_cas_fx(dynamodb_client, config.cas_table),
        emit=events.make_event_fx(events_client, config.event_bus),
        sftp_fx_for=functools.partial(_sftp_fx_for, secrets_client, sftp_cache),
        invoke_async=functools.partial(_invoke_async, lambda_client),
        now=_now,
        new_delivery_id=_new_delivery_id,
        config=config,
    )
