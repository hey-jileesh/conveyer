"""`RunnerConfig` — framework-owned tuning surface; `from_args` is a pure parse. §6.4.

Deviation/assumption note (recorded for the LLD's own errata process, not
silently baked in): §6.4/§8.2/§8.3 pin the argv *key names* actually used
elsewhere in the doc verbatim -- `--conveyer-delivery` (§8.2), `--conveyer-
sfn-retry-count` / `--conveyer-sfn-redrive-count` (§8.2), `--conveyer-run-
config` / `--conveyer-pipeline-spec-uri` / `--conveyer-sla-minutes` (§10.4),
`--conveyer-attempt-id` / `--JOB_RUN_ID` (I-5). The doc does not enumerate a
key name for every other `RunnerConfig` field (`env`, `aws_region`,
`catalog_kind`, `warehouse_uri`, `ledger_catalog_kind`, `ledger_sql_uri`,
`spine_db`, `run_ledger_table`, `event_bus`, `landing_bucket`) -- those are
Terraform-authored default job arguments (009/010 territory). This module
extends the same `--conveyer-<kebab-field-name>` convention the pinned flags
already follow; `_ARGV_KEYS` below is the single place that convention lives,
so a future LLD errata pinning the real names is a one-table edit here.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final, Literal, cast

from pydantic import BaseModel, ConfigDict

from spine.core import args as core_args

_ARGV_KEYS: Final[dict[str, str]] = {
    "env": "conveyer-env",
    "aws_region": "conveyer-aws-region",
    "catalog_kind": "conveyer-catalog-kind",
    "warehouse_uri": "conveyer-warehouse-uri",  # optional (tests only)
    "ledger_catalog_kind": "conveyer-ledger-catalog-kind",
    "ledger_sql_uri": "conveyer-ledger-sql-uri",  # optional (SqlCatalog, tests only)
    "spine_db": "conveyer-spine-db",
    "run_ledger_table": "conveyer-run-ledger-table",
    "event_bus": "conveyer-event-bus",
    "landing_bucket": "conveyer-landing-bucket",
    "pipeline_spec_uri": "conveyer-pipeline-spec-uri",
    "delivery_json": "conveyer-delivery",  # §8.2, SFN-injected (irregular vs. the
    # kebab-field-name convention -- pinned verbatim by the doc)
    "sfn_retry_count": "conveyer-sfn-retry-count",  # §8.2, SFN-injected
    "sfn_redrive_count": "conveyer-sfn-redrive-count",  # §8.2, SFN-injected
    "run_config_json": "conveyer-run-config",
    "sla_minutes": "conveyer-sla-minutes",
}


@dataclass(frozen=True)
class RunnerConfig:  # effect-side wiring; spine/config.py
    env: str
    aws_region: str
    catalog_kind: Literal["glue", "hadoop"]  # Spark data-path catalog (I-2)
    warehouse_uri: str | None  # hadoop only (tests)
    ledger_catalog_kind: Literal["glue", "sql"]
    ledger_sql_uri: str | None  # SqlCatalog, tests only
    spine_db: str
    run_ledger_table: str
    event_bus: str
    landing_bucket: str  # for the I-22 object-URI shape check
    pipeline_spec_uri: str  # validated against the specs root (I-23)
    delivery_json: str  # allowlisted detail passed by SFN
    attempt_id: str
    sfn_retry_count: int
    sfn_redrive_count: int
    run_config_json: str  # framework-owned RunConfig (below)
    # the DEPLOYED per-attempt budget (TF default arg); entrypoint asserts
    # == spec.sla_minutes [H-5]
    sla_minutes: int


class RunConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    shuffle_partitions: int | None = None  # None -> AQE decides
    target_file_size_mb: int = 512  # applied as table write property at append
    repartition_before_write: bool = True  # coalesce/repartition inside effects, pre-append
    # v0.1's `spark_conf` escape hatch is DELETED -- no current need, and an
    # unbounded conf map invites tuning-into-logic erosion; re-adding it later
    # is additive [C-2]. The capacity axis (worker_type/number_of_workers)
    # lives in IaC, not here [T-7].


def _check_literal(value: str, options: tuple[str, ...], key: str) -> str:
    if value not in options:
        raise ValueError(f"{key} must be one of {options!r}, got {value!r}")
    return value


def from_args(argv: Sequence[str]) -> RunnerConfig:
    """Wraps `core.args.parse_args` (pure: `["--k", "v", ...] -> dict`;
    unknown keys ignored -- Glue injects its own); missing required key ->
    `KeyError` with the key named. No `awsglue` (I-14)."""
    parsed = core_args.parse_args(argv)

    def required(field: str) -> str:
        return parsed[_ARGV_KEYS[field]]

    def optional(field: str) -> str | None:
        return parsed.get(_ARGV_KEYS[field])

    # I-5: `--conveyer-attempt-id` override -> `--JOB_RUN_ID` fallback. A
    # plain `parsed[key]` (not `.get`) so a missing fallback still raises
    # `KeyError` naming the exact key that was needed.
    attempt_id = (
        parsed["conveyer-attempt-id"] if "conveyer-attempt-id" in parsed else parsed["JOB_RUN_ID"]
    )

    return RunnerConfig(
        env=required("env"),
        aws_region=required("aws_region"),
        catalog_kind=cast(
            Literal["glue", "hadoop"],
            _check_literal(required("catalog_kind"), ("glue", "hadoop"), "catalog_kind"),
        ),
        warehouse_uri=optional("warehouse_uri"),
        ledger_catalog_kind=cast(
            Literal["glue", "sql"],
            _check_literal(required("ledger_catalog_kind"), ("glue", "sql"), "ledger_catalog_kind"),
        ),
        ledger_sql_uri=optional("ledger_sql_uri"),
        spine_db=required("spine_db"),
        run_ledger_table=required("run_ledger_table"),
        event_bus=required("event_bus"),
        landing_bucket=required("landing_bucket"),
        pipeline_spec_uri=required("pipeline_spec_uri"),
        delivery_json=required("delivery_json"),
        attempt_id=attempt_id,
        sfn_retry_count=int(required("sfn_retry_count")),
        sfn_redrive_count=int(required("sfn_redrive_count")),
        run_config_json=required("run_config_json"),
        sla_minutes=int(required("sla_minutes")),
    )
