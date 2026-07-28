"""Maintenance: Athena OPTIMIZE/VACUUM submitter + supersession reconciliation
-- LLD §9.4.

Weekly job, three steps, each polled to completion; ANY failure raises (the
only exception type in the codebase, `TransientError` -- §7.3) so Lambda's
retry/DLQ/alarm path takes over (§9.4: "each polled to completion (failure ->
raise -> alarm)"):

1. `OPTIMIZE <db>.delivery_ledger REWRITE DATA USING BIN_PACK` (`optimize_sql`).
2. `VACUUM <db>.delivery_ledger` (`vacuum_sql`) -- retention is governed
   entirely by the `vacuum_max_snapshot_age_seconds` / `vacuum_min_snapshots_to_keep`
   table properties `bootstrap/create_ledger.py` already set at bootstrap
   (via `effects.ledger.LEDGER_TABLE_PROPERTIES`); nothing set here.
3. Supersession reconciliation, repairing §8.3's named concurrent-correction
   race: find `delivery_key`s with more than one live `registered` delivery,
   feed the grouped rows to the ALREADY-BUILT pure planner
   `core.decisions.plan_reconciliation` (§8.3/§9.4, built by `m1-folds-planners`),
   then exactly one `fx.ledger.append` of the resulting `superseded` accretion
   rows (`reconcile_supersessions`). Deterministic and idempotent
   (append-on-change): a delivery_key with its correction already reconciled
   no longer appears in a fresh "current registered" fold, so a second run
   over unchanged ledger content appends nothing (verified in
   `tests/golden/test_reconciliation.py`).

**§12.5 documented exclusion -- Athena has no moto coverage.** As of this
writing moto ships no Athena backend at all (not even a partial one), and
the brief confirms this ("moto cannot simulate Athena adequately"). So:

* `AthenaFx` (below) is a small LOCAL record of functions -- the same
  records-of-functions idiom as every capability in `effects/records.py`
  (§7.0 rule 3) -- kept in THIS module rather than added to
  `effects/records.py` (outside this bead's FILES ownership; also the brief
  says either location is fine). `make_athena_fx` is the production factory
  (closes over a real boto3 `athena` client); tests build the SAME record
  from a plain local recording/fake function -- no mocking framework, ever.
* Only the POLLING/SEQUENCING logic that consumes an `AthenaFx` (`run_query`)
  and the pure Athena-result-row -> `DeliveryRecord` parsing
  (`_row_to_delivery_record` and friends) are exercised by tests, against a
  fake `AthenaFx` and hand-built result-row dicts respectively -- never
  against a real `athena` boto3 client. `make_athena_fx`'s own boto3 call
  bodies (`_start_query`/`_poll`/`_get_results`) are therefore UNTESTED
  (same shape as `effects/sftp.py`'s and `effects/ledger.py`'s glue-catalog
  path documented exclusions).
* The reconciliation step's "get the current candidate rows" input is
  deliberately factored so PRODUCTION goes through Athena
  (`live_duplicates_sql` + `_row_to_delivery_record` parsing) while TESTS can
  substitute a plain local ledger scan folded in Python
  (`live_duplicates_from_rows`, applied directly to `fx.ledger.scan_feed(...)`
  output) -- both paths feed the identical `reconcile_supersessions`
  function, so the golden test exercises the real `plan_reconciliation` +
  `fx.ledger.append` path end-to-end without needing Athena at all (brief:
  "run step 3 with the query replaced by the local fold").
* `_row_to_delivery_record`'s parsing of the `objects`/`object_uris` list<>/
  struct<> columns relies on Trino/Presto's documented (if non-obvious)
  `CAST(... AS JSON)` behavior for ROW values: there is no standard ROW ->
  JSON-*object* mapping, so Trino renders a ROW as a POSITIONAL JSON array
  (verified against Trino's own docs; NOT verified against a real Athena
  response, since none can be produced in this test environment) --
  `_parse_objects_json` parses each element positionally, in
  `LEDGER_ICEBERG_SCHEMA`'s declared struct field order
  (name, role, uri, bytes, sha256). This is the one part of this module with
  no test coverage against the real service; flagged here per §12.5's
  "document each exclusion inline" and again at its definition below.

This function's execution role is (per Terraform, M6 -- nothing to do here)
the ONLY principal holding `s3:DeleteObject` on `${p}-lake/ledger/*` (D-9).
"""

from __future__ import annotations

import functools
import json
import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, cast

from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from ingestion import observability
from ingestion.config import RuntimeConfig
from ingestion.core import decisions, folds
from ingestion.core.model import DeliveryObject, DeliveryRecord
from ingestion.effects.records import Effects, TransientError

_logger = logging.getLogger(__name__)

QueryState = Literal["QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"]
_TERMINAL_STATES: frozenset[str] = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})

_DEFAULT_POLL_INTERVAL_S = 5.0
_DEFAULT_MAX_ATTEMPTS = 120  # ~10 min at 5 s/attempt


# --- LLD §7.0 rule 3 / §9.4: Athena as a local record of functions ---------


@dataclass(frozen=True)
class AthenaFx:
    """Not part of `effects.records.Effects` (deliberately -- see module
    docstring): maintenance is the only caller of Athena in this codebase.
    """

    start_query: Callable[[str], str]  # sql -> query_execution_id
    poll: Callable[[str], str]  # query_execution_id -> QueryState
    get_results: Callable[[str], list[dict[str, str | None]]]
    # query_execution_id -> rows, each a column-name -> value dict
    # (Athena's own shape: every value is a string, or None/absent if NULL)


def _start_query(client: object, workgroup: str, database: str, output_uri: str, sql: str) -> str:
    try:
        response = client.start_query_execution(  # type: ignore[attr-defined]
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_uri},
            WorkGroup=workgroup,
        )
    except ClientError as exc:
        raise TransientError(f"athena start_query_execution failed: {exc}") from exc
    query_execution_id: str = response["QueryExecutionId"]
    return query_execution_id


def _poll(client: object, query_execution_id: str) -> str:
    try:
        response = client.get_query_execution(  # type: ignore[attr-defined]
            QueryExecutionId=query_execution_id
        )
    except ClientError as exc:
        raise TransientError(f"athena get_query_execution failed: {exc}") from exc
    state: str = response["QueryExecution"]["Status"]["State"]
    return state


def _get_results(client: object, query_execution_id: str) -> list[dict[str, str | None]]:
    try:
        paginator = client.get_paginator("get_query_results")  # type: ignore[attr-defined]
        rows: list[dict[str, str | None]] = []
        columns: list[str] | None = None
        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            for row in page["ResultSet"]["Rows"]:
                values = [cell.get("VarCharValue") for cell in row["Data"]]
                if columns is None:
                    columns = values  # type: ignore[assignment] # header row, always non-null
                    continue
                rows.append(dict(zip(columns, values, strict=True)))
        return rows
    except ClientError as exc:
        raise TransientError(f"athena get_query_results failed: {exc}") from exc


def make_athena_fx(client: object, workgroup: str, database: str, output_uri: str) -> AthenaFx:
    """Production factory -- closes over a real boto3 `athena` client. UNTESTED
    against a real service (module docstring, §12.5 exclusion)."""
    return AthenaFx(
        start_query=functools.partial(_start_query, client, workgroup, database, output_uri),
        poll=functools.partial(_poll, client),
        get_results=functools.partial(_get_results, client),
    )


def build_athena_fx(config: RuntimeConfig) -> AthenaFx:
    """Entrypoint-facing convenience: builds the real boto3 `athena` client
    and wires it into an `AthenaFx`, from `RuntimeConfig` alone -- mirrors
    `effects.build.build_effects`'s client-construction shape, kept HERE
    (not in `effects/build.py`, outside this bead's FILES ownership) since
    `Effects` deliberately carries no Athena capability.
    """
    import boto3  # type: ignore[import-untyped]  # local import: only this factory needs it

    client = boto3.client("athena", region_name=config.aws_region)
    return make_athena_fx(
        client, config.athena_workgroup, config.glue_database, config.athena_output_uri
    )


def run_query(
    athena: AthenaFx,
    sql: str,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
    poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
) -> str:
    """Start `sql`, poll to a terminal state, return the query_execution_id
    on SUCCEEDED. `TransientError` on FAILED/CANCELLED, or if `max_attempts`
    polls never reach a terminal state (LLD §9.4: "each polled to completion
    (failure -> raise -> alarm)"). `sleep_fn`/`max_attempts`/`poll_interval_s`
    are injected so tests never sleep for real (kernel-hygiene requirement) --
    production default is a real `time.sleep` at ~10 min total budget.
    """
    query_execution_id = athena.start_query(sql)
    for attempt in range(1, max_attempts + 1):
        state = athena.poll(query_execution_id)
        if state == "SUCCEEDED":
            return query_execution_id
        if state in _TERMINAL_STATES:  # FAILED or CANCELLED
            raise TransientError(f"athena query {query_execution_id} ended in {state}: {sql!r}")
        if attempt == max_attempts:
            raise TransientError(
                f"athena query {query_execution_id} timed out after {max_attempts} polls: {sql!r}"
            )
        sleep_fn(poll_interval_s)
    raise AssertionError("unreachable")  # loop always returns or raises above


# --- LLD §9.4 steps 1/2: OPTIMIZE / VACUUM ----------------------------------


def _ledger_identifier(config: RuntimeConfig) -> str:
    return f"{config.glue_database}.{config.ledger_table}"


def optimize_sql(config: RuntimeConfig) -> str:
    return f"OPTIMIZE {_ledger_identifier(config)} REWRITE DATA USING BIN_PACK"


def vacuum_sql(config: RuntimeConfig) -> str:
    # Retention is governed by the table properties `create_ledger.py` set
    # at bootstrap (`effects.ledger.LEDGER_TABLE_PROPERTIES`) -- nothing set
    # here (LLD §9.4 step 2).
    return f"VACUUM {_ledger_identifier(config)}"


# --- LLD §9.4 step 3: supersession reconciliation ---------------------------


def live_duplicates_sql(config: RuntimeConfig) -> str:
    """§11.4's "latest-disposition fold in SQL, window by `delivery_id` over
    `recorded_at`" (same shape as the `current-dispositions` named query),
    pre-filtered to `delivery_key`s appearing more than once among CURRENT
    `registered` rows -- an OPTIMIZATION (less data returned), not the sole
    correctness mechanism: `live_duplicates_from_rows` (below) re-derives the
    authoritative per-`(feed_id, delivery_key)` grouping in Python from
    whatever rows this query returns, so a coarser SQL-side filter (this one
    groups by `delivery_key` alone, matching §9.4's literal wording, not by
    `(feed_id, delivery_key)`) cannot cause an incorrect reconciliation --
    only, at worst, a slightly larger candidate set fetched from Athena.

    `objects`/`object_uris` (Iceberg list<>/struct<> columns) are cast to
    JSON so `_row_to_delivery_record` can parse them without Presto's
    unparseable default `array`/`row` VARCHAR rendering -- see the module
    docstring's §12.5 note on `CAST(... AS JSON)`'s positional-array ROW
    behavior. Timestamp columns are cast to VARCHAR (Athena/Trino renders a
    `timestamp` as `'YYYY-MM-DD HH:MM:SS[.fraction]'`, session timezone UTC
    by default -- Iceberg `timestamptz` values are UTC instants, so this is
    unambiguous) since raw `GetQueryResults` values are strings regardless.
    """
    identifier = _ledger_identifier(config)
    return (
        "WITH latest AS (\n"
        "    SELECT *, ROW_NUMBER() OVER "
        "(PARTITION BY delivery_id ORDER BY recorded_at DESC) AS rn\n"
        f"    FROM {identifier}\n"
        "),\n"
        "current_registered AS (\n"
        "    SELECT * FROM latest WHERE rn = 1 AND disposition = 'registered'\n"
        "),\n"
        "dup_keys AS (\n"
        "    SELECT delivery_key FROM current_registered "
        "GROUP BY delivery_key HAVING COUNT(*) > 1\n"
        ")\n"
        "SELECT\n"
        "    cr.delivery_id, cr.feed_id, cr.delivery_key, cr.batch_id, cr.content_hash,\n"
        "    cr.size_bytes,\n"
        "    CAST(cr.object_uris AS JSON) AS object_uris_json,\n"
        "    CAST(cr.objects AS JSON) AS objects_json,\n"
        "    cr.manifest_ref, cr.asserted_record_count, cr.completeness_mode,\n"
        "    CAST(cr.received_at AS VARCHAR) AS received_at,\n"
        "    CAST(cr.recorded_at AS VARCHAR) AS recorded_at,\n"
        "    cr.disposition, cr.supersedes, cr.driver, cr.driver_run_id, cr.notes\n"
        "FROM current_registered cr\n"
        "JOIN dup_keys dk ON cr.delivery_key = dk.delivery_key\n"
        "ORDER BY cr.delivery_key, cr.delivery_id"
    )


def _parse_athena_timestamp(value: str) -> datetime:
    """See `live_duplicates_sql`'s docstring for the assumed format.
    `.fraction` may be 0-9 digits (Trino supports nanosecond precision);
    `datetime.strptime`'s `%f` only accepts up to 6, so it is truncated to
    microseconds.
    """
    date_part, _, frac = value.partition(".")
    micros = (frac + "000000")[:6]
    return datetime.strptime(f"{date_part}.{micros}", "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=UTC)


def _parse_objects_json(raw: str) -> list[DeliveryObject]:
    """POSITIONAL parsing -- see module docstring's §12.5 note: Trino's
    `CAST(row AS JSON)` has no ROW -> JSON-object mapping, only a JSON array
    in declared-field order (name, role, uri, bytes, sha256, matching
    `effects.ledger.LEDGER_ICEBERG_SCHEMA`).
    """
    return [
        DeliveryObject(name=item[0], role=item[1], uri=item[2], bytes=item[3], sha256=item[4])
        for item in json.loads(raw)
    ]


def _optional_int(value: str | None) -> int | None:
    return int(value) if value is not None else None


def _row_to_delivery_record(row: Mapping[str, str | None]) -> DeliveryRecord:
    """One Athena `live_duplicates_sql` result row -> `DeliveryRecord`.
    `DeliveryRecord.model_validate(dict)` (not the keyword constructor) is
    used deliberately: it takes `Any` per field, so it does not require the
    `Literal`-narrowing helpers `core/decisions.py` needs for its
    keyword-constructor calls (this module is effects-side, not `core/`, so
    the pure-core purity/idiom rules don't force that shape here) --
    pydantic still validates every field at construction, same as any other
    boundary parse in this codebase.
    """
    payload: dict[str, object] = {
        "delivery_id": row["delivery_id"],
        "feed_id": row["feed_id"],
        "delivery_key": row["delivery_key"],
        "batch_id": row.get("batch_id"),
        "content_hash": row.get("content_hash"),
        "size_bytes": _optional_int(row.get("size_bytes")),
        "object_uris": json.loads(cast(str, row["object_uris_json"])),
        "objects": _parse_objects_json(cast(str, row["objects_json"])),
        "manifest_ref": row.get("manifest_ref"),
        "asserted_record_count": _optional_int(row.get("asserted_record_count")),
        "completeness_mode": row["completeness_mode"],
        "received_at": _parse_athena_timestamp(cast(str, row["received_at"])),
        "recorded_at": _parse_athena_timestamp(cast(str, row["recorded_at"])),
        "disposition": row["disposition"],
        "supersedes": row.get("supersedes"),
        "driver": row["driver"],
        "driver_run_id": row["driver_run_id"],
        "notes": row.get("notes"),
    }
    return DeliveryRecord.model_validate(payload)


def live_duplicates_from_rows(
    rows: Sequence[DeliveryRecord],
) -> dict[str, Sequence[DeliveryRecord]]:
    """PURE grouping helper, the shared "decide what `plan_reconciliation`
    should see" step for BOTH call paths (production: Athena-sourced rows;
    tests: a local ledger scan -- the brief's "query replaced by the local
    fold"): folds `rows` via `core.folds.registered_deliveries` (the SAME
    fold used everywhere else, LLD §7.4 -- for Athena-sourced input this is
    a safe no-op re-application, since `live_duplicates_sql`'s own
    `rn = 1 AND disposition = 'registered'` filter already computed the
    identical "latest disposition per delivery_id" result), then groups by
    `(feed_id, delivery_key)` -- NOT `delivery_key` alone, to avoid two
    different feeds' deliveries ever being (mis)grouped together merely
    because they happen to share a `delivery_key` string -- keeping only
    groups with more than one record. The map key is opaque to
    `plan_reconciliation` (it only iterates `.values()`, per
    `core/decisions.py`), so encoding `feed_id` into it is safe.
    """
    by_key: dict[str, list[DeliveryRecord]] = {}
    for record in folds.registered_deliveries(rows):
        key = f"{record.feed_id}\x00{record.delivery_key}"
        by_key.setdefault(key, []).append(record)
    return {key: records for key, records in by_key.items() if len(records) > 1}


def _count_by_feed(rows: Sequence[DeliveryRecord]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.feed_id] = counts.get(row.feed_id, 0) + 1
    return counts


def reconcile_supersessions(
    fx: Effects, live_duplicates: Mapping[str, Sequence[DeliveryRecord]], now: datetime
) -> tuple[DeliveryRecord, ...]:
    """LLD §9.4 step 3's pure-planner + one-append pair: `decide, then do`
    (§7.0 rule 5). Idempotent via append-on-change --
    `core.decisions.plan_reconciliation` returns `()` once every
    `delivery_key`'s correction is already the ledger's newest fact, so a
    second call fed a freshly recomputed `live_duplicates` is a no-op
    append (`fx.ledger.append(())` is itself a no-op, `effects/ledger.py`).
    """
    rows = decisions.plan_reconciliation(live_duplicates, now)
    fx.ledger.append(rows)
    for feed_id, count in _count_by_feed(rows).items():
        observability.emit_metric("SupersessionsReconciled", count, feed_id)
    _logger.info("reconciliation: appended %d superseded row(s)", len(rows))
    return rows


# --- LLD §9.4: the full weekly job ------------------------------------------


def run_maintenance(
    fx: Effects,
    athena: AthenaFx,
    *,
    sleep_fn: Callable[[float], None] = time.sleep,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
    poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
) -> tuple[DeliveryRecord, ...]:
    """The weekly maintenance job (LLD §9.4): OPTIMIZE, then VACUUM, then
    supersession reconciliation via Athena. Any step's failure raises
    (`run_query`) and aborts the remaining steps -- matching §9.4's "each
    polled to completion (failure -> raise -> alarm)"; the entrypoint adds
    no additional try/except (an uncaught exception here IS the alarm path).
    """
    config = fx.config
    _logger.info("maintenance: starting OPTIMIZE on %s", _ledger_identifier(config))
    run_query(
        athena,
        optimize_sql(config),
        sleep_fn=sleep_fn,
        max_attempts=max_attempts,
        poll_interval_s=poll_interval_s,
    )
    _logger.info("maintenance: OPTIMIZE succeeded; starting VACUUM")
    run_query(
        athena,
        vacuum_sql(config),
        sleep_fn=sleep_fn,
        max_attempts=max_attempts,
        poll_interval_s=poll_interval_s,
    )
    _logger.info("maintenance: VACUUM succeeded; starting supersession reconciliation")
    execution_id = run_query(
        athena,
        live_duplicates_sql(config),
        sleep_fn=sleep_fn,
        max_attempts=max_attempts,
        poll_interval_s=poll_interval_s,
    )
    rows = [_row_to_delivery_record(row) for row in athena.get_results(execution_id)]
    live_duplicates = live_duplicates_from_rows(rows)
    return reconcile_supersessions(fx, live_duplicates, fx.now())
