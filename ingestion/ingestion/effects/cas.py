"""make_cas_fx -- DynamoDB conditional-write turnstile -- LLD §8.4.

Single-table design, two `pk` shapes (§8.4): `batch#<feed_id>#<batch_id>`
(batch claim items, built/read by `claim`/`complete`/`sweep_stale`) and an
opaque caller-supplied `pk` for `marker_exists`/`mark` (the overdue-marker
shape `overdue#<feed_id>#<expectation_date>` is the absence detector's
concern, M5 -- this module does not need to know that format).

Uses the low-level `dynamodb` boto3 client (matching `effects/s3.py`'s
convention) with `boto3.dynamodb.types.TypeSerializer`/`TypeDeserializer`
for the Python-value <-> DynamoDB-AttributeValue conversion, rather than
hand-writing `{"S": ...}`/`{"N": ...}` for every attribute.

**TAKEN_OVER item semantics (verified live against moto, load-bearing for
`core/decisions.py::_plan_taken_over`, which builds the resumed row/event
from `claim.item` "with NOTHING re-derived" so it is byte-identical to what
the dead run would have written, §8.5):** the `ClaimResult.item` returned on
`TAKEN_OVER` carries the identity **as observed BEFORE** the takeover
`UpdateItem` -- in particular `item.owner_run_id` is the DEAD run's id, not
the resuming run's -- while the DynamoDB row's `owner_run_id` attribute IS
updated to the resuming run's id (so `complete`'s fencing condition
`owner_run_id = run_id` later admits the resuming run and rejects the dead
one). These two facts (returned value vs. stored row) are deliberately
different values.
"""

from __future__ import annotations

import dataclasses
import functools
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from ingestion.core.decisions import RegistrationRequest
from ingestion.core.hashing import canonical_content_hash
from ingestion.core.model import ClaimItem, ClaimResult, StagedObject
from ingestion.effects.records import CasFx, TransientError

_STALE_THRESHOLD_S = 1200  # > max Lambda duration (900 s): provably no live owner
_CLAIM_TTL_DAYS = 30

_serializer = TypeSerializer()
_deserializer = TypeDeserializer()


def _batch_pk(feed_id: str, batch_id: str) -> str:
    return f"batch#{feed_id}#{batch_id}"


def _is_conditional_failure(exc: ClientError) -> bool:
    return exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException"


def _put_raw(client: Any, table: str, item: Mapping[str, Any], condition: str) -> None:
    client.put_item(
        TableName=table,
        Item={k: _serializer.serialize(v) for k, v in item.items()},
        ConditionExpression=condition,
    )


def _get_raw(client: Any, table: str, pk: str) -> dict[str, Any] | None:
    response = client.get_item(TableName=table, Key={"pk": {"S": pk}}, ConsistentRead=True)
    if "Item" not in response:
        return None
    return {k: _deserializer.deserialize(v) for k, v in response["Item"].items()}


def _build_claim_item_dict(
    req: RegistrationRequest, batch_id: str, run_id: str, trigger: dict[str, Any], now: datetime
) -> dict[str, Any]:
    """Everything a takeover needs to finish the registration without
    re-deriving anything (§8.4). `content_hash`/`size_bytes` are computed
    here from `req.objects`' data objects -- the same pure algorithm
    `core.decisions._plan_won` independently applies to the same objects
    when it later builds the ledger row, so the two derivations always
    agree.
    """
    data_objects = [o for o in req.objects if o.role == "data"]
    content_hash = canonical_content_hash([(o.name, o.sha256) for o in data_objects])
    size_bytes = sum(o.bytes for o in data_objects)
    claimed_at = int(now.timestamp())
    return {
        "pk": _batch_pk(req.feed.feed_id, batch_id),
        "feed_id": req.feed.feed_id,
        "batch_id": batch_id,
        "delivery_id": req.delivery_id,
        "driver": req.driver,
        "received_at": req.received_at.isoformat(),
        "delivery_key": req.delivery_key,
        "content_hash": content_hash,
        "size_bytes": size_bytes,
        "objects_inventory": json.dumps([dataclasses.asdict(o) for o in req.objects]),
        "asserted_record_count": req.completeness.asserted_record_count,
        "completeness_mode": req.feed.completeness.mode,
        "trigger": json.dumps(trigger),
        "owner_run_id": run_id,
        "status": "in_progress",
        "claimed_at": claimed_at,
        "completed_at": None,
        "expires_at": claimed_at + _CLAIM_TTL_DAYS * 86400,
    }


def _item_to_claim_item(raw: Mapping[str, Any]) -> ClaimItem:
    inventory = tuple(StagedObject(**o) for o in json.loads(raw["objects_inventory"]))
    completed_at = raw["completed_at"]
    return ClaimItem(
        feed_id=raw["feed_id"],
        batch_id=raw["batch_id"],
        delivery_id=raw["delivery_id"],
        driver=raw["driver"],
        received_at=datetime.fromisoformat(raw["received_at"]),
        delivery_key=raw["delivery_key"],
        content_hash=raw["content_hash"],
        size_bytes=int(raw["size_bytes"]),
        objects_inventory=inventory,
        asserted_record_count=(
            int(raw["asserted_record_count"]) if raw["asserted_record_count"] is not None else None
        ),
        completeness_mode=raw["completeness_mode"],
        trigger=json.loads(raw["trigger"]),
        owner_run_id=raw["owner_run_id"],
        status=raw["status"],
        claimed_at=int(raw["claimed_at"]),
        completed_at=(int(completed_at) if completed_at is not None else None),
    )


def _claim(
    client: Any,
    table: str,
    req: RegistrationRequest,
    batch_id: str,
    run_id: str,
    trigger: dict[str, Any],
    now: datetime,
) -> ClaimResult:
    item_dict = _build_claim_item_dict(req, batch_id, run_id, trigger, now)
    pk = item_dict["pk"]
    try:
        _put_raw(client, table, item_dict, condition="attribute_not_exists(pk)")
        return ClaimResult(kind="WON", item=None)
    except ClientError as exc:
        if not _is_conditional_failure(exc):
            raise TransientError(f"cas claim PutItem failed for {pk}: {exc}") from exc

    raw = _get_raw(client, table, pk)
    if raw is None:
        # The PutItem lost to a competing writer's item, but a consistent
        # GetItem immediately after found nothing -- only possible if that
        # item was deleted between the two calls (e.g. a TTL reap racing
        # us). Genuinely transient; the platform's Lambda retry resolves it.
        raise TransientError(f"cas claim: item at {pk} vanished between PutItem and GetItem")
    observed = _item_to_claim_item(raw)

    if observed.status == "completed":
        return ClaimResult(kind="LOST_COMPLETED", item=observed)

    age_s = int(now.timestamp()) - observed.claimed_at
    if age_s <= _STALE_THRESHOLD_S:
        return ClaimResult(kind="LOST_IN_PROGRESS", item=observed)

    new_claimed_at = int(now.timestamp())
    try:
        client.update_item(
            TableName=table,
            Key={"pk": {"S": pk}},
            UpdateExpression="SET owner_run_id = :new_owner, claimed_at = :new_claimed",
            ConditionExpression="owner_run_id = :observed_owner AND #st = :in_progress",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":new_owner": {"S": run_id},
                ":new_claimed": {"N": str(new_claimed_at)},
                ":observed_owner": {"S": observed.owner_run_id},
                ":in_progress": {"S": "in_progress"},
            },
        )
        return ClaimResult(kind="TAKEN_OVER", item=observed)
    except ClientError as exc:
        if _is_conditional_failure(exc):
            return ClaimResult(kind="LOST_IN_PROGRESS", item=observed)
        raise TransientError(f"cas takeover UpdateItem failed for {pk}: {exc}") from exc


def _complete(
    client: Any, table: str, feed_id: str, batch_id: str, run_id: str, now: datetime
) -> None:
    """Fenced on `owner_run_id`: a taken-over zombie's late `complete` call
    fails the condition and is silently absorbed (§8.4 -- "its late writes
    are harmless duplicates by design"), not raised.
    """
    pk = _batch_pk(feed_id, batch_id)
    completed_at = int(now.timestamp())
    expires_at = completed_at + _CLAIM_TTL_DAYS * 86400
    try:
        client.update_item(
            TableName=table,
            Key={"pk": {"S": pk}},
            UpdateExpression="SET #st = :completed, completed_at = :ca, expires_at = :ea",
            ConditionExpression="owner_run_id = :run_id",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":completed": {"S": "completed"},
                ":ca": {"N": str(completed_at)},
                ":ea": {"N": str(expires_at)},
                ":run_id": {"S": run_id},
            },
        )
    except ClientError as exc:
        if not _is_conditional_failure(exc):
            raise TransientError(f"cas complete failed for {pk}: {exc}") from exc


def _sweep_stale(client: Any, table: str, now: datetime) -> list[ClaimItem]:
    """Scan w/ filter -- the table is tiny (§8.4)."""
    threshold = int(now.timestamp()) - _STALE_THRESHOLD_S
    items: list[ClaimItem] = []
    scan_kwargs: dict[str, Any] = {
        "TableName": table,
        "FilterExpression": (
            "begins_with(pk, :prefix) AND #st = :in_progress AND claimed_at <= :threshold"
        ),
        "ExpressionAttributeNames": {"#st": "status"},
        "ExpressionAttributeValues": {
            ":prefix": {"S": "batch#"},
            ":in_progress": {"S": "in_progress"},
            ":threshold": {"N": str(threshold)},
        },
    }
    try:
        while True:
            response = client.scan(**scan_kwargs)
            for raw_item in response.get("Items", []):
                deserialized = {k: _deserializer.deserialize(v) for k, v in raw_item.items()}
                items.append(_item_to_claim_item(deserialized))
            if "LastEvaluatedKey" not in response:
                return items
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    except ClientError as exc:
        raise TransientError(f"cas sweep_stale scan failed: {exc}") from exc


def _get_claim(client: Any, table: str, feed_id: str, batch_id: str) -> ClaimItem | None:
    """GetItem on the exact `(feed_id, batch_id)` pk -- M-1 (security-gate):
    the sftp-pull resume path's ONLY legitimate way to look up a single
    known-stale claim, instead of reusing `sweep_stale`'s Scan (which the
    per-feed driver role does not, and must not, hold -- see this field's
    docstring in `effects/records.py::CasFx`).
    """
    pk = _batch_pk(feed_id, batch_id)
    try:
        raw = _get_raw(client, table, pk)
    except ClientError as exc:
        raise TransientError(f"cas get_claim failed for {pk}: {exc}") from exc
    if raw is None:
        return None
    return _item_to_claim_item(raw)


def _marker_exists(client: Any, table: str, pk: str) -> bool:
    try:
        return _get_raw(client, table, pk) is not None
    except ClientError as exc:
        raise TransientError(f"cas marker_exists failed for {pk}: {exc}") from exc


def _mark(client: Any, table: str, pk: str, now: datetime, ttl_days: int) -> bool:
    """Conditional put; `True` iff this call was the first writer (§9.3's
    at-least-once emit-then-mark -- a lost race here is expected and fine).
    """
    marked_at = int(now.timestamp())
    item = {"pk": pk, "marked_at": marked_at, "expires_at": marked_at + ttl_days * 86400}
    try:
        _put_raw(client, table, item, condition="attribute_not_exists(pk)")
        return True
    except ClientError as exc:
        if _is_conditional_failure(exc):
            return False
        raise TransientError(f"cas mark failed for {pk}: {exc}") from exc


def make_cas_fx(client: Any, table: str) -> CasFx:
    return CasFx(
        claim=functools.partial(_claim, client, table),
        complete=functools.partial(_complete, client, table),
        sweep_stale=functools.partial(_sweep_stale, client, table),
        marker_exists=functools.partial(_marker_exists, client, table),
        mark=functools.partial(_mark, client, table),
        get_claim=functools.partial(_get_claim, client, table),
    )
