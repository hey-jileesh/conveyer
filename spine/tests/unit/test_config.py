"""Unit tests for `spine.config` — LLD §6.4.

`RunConfig` forbids unknown keys (no `spark_conf` escape hatch, [C-2]);
`RunnerConfig.from_args` wraps `core.args.parse_args`, picks known keys, and
raises `KeyError` naming the exact missing key -- including the `--conveyer-
attempt-id` override -> `--JOB_RUN_ID` fallback chain (I-5).
"""

import pytest
from pydantic import ValidationError
from spine import config

_FULL_ARGV = [
    "--conveyer-env", "dev",
    "--conveyer-aws-region", "us-east-1",
    "--conveyer-catalog-kind", "glue",
    "--conveyer-ledger-catalog-kind", "glue",
    "--conveyer-spine-db", "conveyer_dev_spine",
    "--conveyer-run-ledger-table", "run_ledger",
    "--conveyer-event-bus", "conveyer-dev-bus",
    "--conveyer-landing-bucket", "conveyer-dev-landing",
    "--conveyer-artifacts-bucket", "artifacts",
    "--conveyer-pipeline-spec-uri", "s3://artifacts/spine/specs/commissions/pipeline.yaml",
    "--conveyer-delivery", "{}",
    "--conveyer-sfn-retry-count", "0",
    "--conveyer-sfn-redrive-count", "0",
    "--conveyer-run-config", "{}",
    "--conveyer-sla-minutes", "480",
    "--JOB_RUN_ID", "jr_abc123",
]  # fmt: skip


# --- RunConfig: extra="forbid" [C-2] -----------------------------------------


def test_run_config_defaults() -> None:
    run_config = config.RunConfig()
    assert run_config.shuffle_partitions is None
    assert run_config.target_file_size_mb == 512
    assert run_config.repartition_before_write is True


def test_run_config_rejects_spark_conf_escape_hatch() -> None:
    with pytest.raises(ValidationError):
        config.RunConfig(spark_conf={"spark.sql.shuffle.partitions": "10"})  # type: ignore[call-arg]


def test_run_config_rejects_any_unknown_field() -> None:
    with pytest.raises(ValidationError):
        config.RunConfig(unexpected_field=True)  # type: ignore[call-arg]


# --- RunnerConfig.from_args ---------------------------------------------------


def test_from_args_happy_path() -> None:
    runner_config = config.from_args(_FULL_ARGV)
    assert runner_config.env == "dev"
    assert runner_config.aws_region == "us-east-1"
    assert runner_config.catalog_kind == "glue"
    assert runner_config.warehouse_uri is None
    assert runner_config.delivery_json == "{}"
    assert runner_config.sfn_retry_count == 0
    assert runner_config.sla_minutes == 480
    assert runner_config.attempt_id == "jr_abc123"  # JOB_RUN_ID fallback, I-5


def test_from_args_missing_required_key_raises_keyerror_naming_it() -> None:
    argv = [
        token
        for i, token in enumerate(_FULL_ARGV)
        if not (token == "--conveyer-env" or _FULL_ARGV[i - 1] == "--conveyer-env")
    ]
    with pytest.raises(KeyError) as exc_info:
        config.from_args(argv)
    assert exc_info.value.args[0] == "conveyer-env"


def test_from_args_attempt_id_override_takes_precedence_over_job_run_id() -> None:
    argv = _FULL_ARGV + ["--conveyer-attempt-id", "override-attempt-id"]
    runner_config = config.from_args(argv)
    assert runner_config.attempt_id == "override-attempt-id"


def test_from_args_missing_attempt_id_and_job_run_id_raises_keyerror_naming_job_run_id() -> None:
    argv = [
        token
        for i, token in enumerate(_FULL_ARGV)
        if not (token == "--JOB_RUN_ID" or _FULL_ARGV[i - 1] == "--JOB_RUN_ID")
    ]
    with pytest.raises(KeyError) as exc_info:
        config.from_args(argv)
    assert exc_info.value.args[0] == "JOB_RUN_ID"


def test_from_args_optional_warehouse_and_ledger_sql_uri_default_none() -> None:
    runner_config = config.from_args(_FULL_ARGV)
    assert runner_config.warehouse_uri is None
    assert runner_config.ledger_sql_uri is None


def test_from_args_rejects_bad_catalog_kind_literal() -> None:
    argv = [tok if tok != "glue" else "not-a-real-catalog" for tok in _FULL_ARGV]
    with pytest.raises(ValueError):
        config.from_args(argv)
