# athena.tf -- the four spine named queries (LLD S11.5 / 004 S13.2), run in
# the EXISTING ingestion Athena workgroup (var.athena_workgroup_name), not a
# new one -- S10.2: "Athena named queries (§11.5) in the existing workgroup."
# Queries run against `<spine db>.run_ledger` per the S6.5 schema.

locals {
  run_ledger_identifier = "${local.spine_glue_database}.run_ledger"

  # spine-run-status: latest outcome per (batch_id, stage), folded by
  # started_at (S11.5: "latest outcome per (batch, stage) -- fold ordered by
  # started_at").
  spine_run_status_sql = <<-SQL
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY batch_id, stage ORDER BY started_at DESC
        ) AS rn
        FROM ${local.run_ledger_identifier}
    )
    SELECT
        batch_id, pipeline, feed_id, attempt_id, stage, outcome,
        started_at, finished_at, error_type, error_message
    FROM latest
    WHERE rn = 1
    ORDER BY batch_id, stage
  SQL

  # spine-attempts-per-batch: distinct attempt_id counts, flags > 1.
  spine_attempts_per_batch_sql = <<-SQL
    SELECT
        batch_id, pipeline, feed_id,
        COUNT(DISTINCT attempt_id) AS attempt_count,
        COUNT(DISTINCT attempt_id) > 1 AS multiple_attempts
    FROM ${local.run_ledger_identifier}
    GROUP BY batch_id, pipeline, feed_id
    ORDER BY attempt_count DESC, batch_id
  SQL

  # spine-stage-durations-30d: p50/p95 finished_at - started_at by stage,
  # trailing 30 days, successful transitions only.
  spine_stage_durations_30d_sql = <<-SQL
    WITH recent AS (
        SELECT
            stage,
            date_diff('millisecond', started_at, finished_at) AS duration_ms
        FROM ${local.run_ledger_identifier}
        WHERE recorded_at >= date_add('day', -30, current_timestamp)
          AND outcome = 'ok'
    )
    SELECT
        stage,
        approx_percentile(duration_ms, 0.5) AS p50_ms,
        approx_percentile(duration_ms, 0.95) AS p95_ms,
        COUNT(*) AS sample_count
    FROM recent
    GROUP BY stage
    ORDER BY stage
  SQL

  # spine-rerun-noop-rate: share of attempts whose every row is
  # skipped-guard with facts_appended = 0 (003's phase-gate criterion as a
  # standing query, S11.5).
  spine_rerun_noop_rate_sql = <<-SQL
    WITH per_attempt AS (
        SELECT
            batch_id, attempt_id,
            COUNT(*) AS row_count,
            SUM(CASE WHEN outcome = 'skipped-guard' THEN 1 ELSE 0 END) AS skipped_count,
            SUM(COALESCE(facts_appended, 0)) AS total_facts_appended
        FROM ${local.run_ledger_identifier}
        GROUP BY batch_id, attempt_id
    ),
    classified AS (
        SELECT
            *,
            (skipped_count = row_count AND total_facts_appended = 0) AS is_noop
        FROM per_attempt
    )
    SELECT
        COUNT_IF(is_noop) AS noop_attempts,
        COUNT(*) AS total_attempts,
        CAST(COUNT_IF(is_noop) AS DOUBLE) / NULLIF(COUNT(*), 0) AS noop_rate
    FROM classified
  SQL
}

resource "aws_athena_named_query" "spine_run_status" {
  name        = "spine-run-status"
  workgroup   = var.athena_workgroup_name
  database    = local.spine_glue_database
  description = "Latest outcome per (batch_id, stage), folded by started_at."
  query       = local.spine_run_status_sql
}

resource "aws_athena_named_query" "spine_attempts_per_batch" {
  name        = "spine-attempts-per-batch"
  workgroup   = var.athena_workgroup_name
  database    = local.spine_glue_database
  description = "Distinct attempt_id counts per batch; flags batches with more than one attempt."
  query       = local.spine_attempts_per_batch_sql
}

resource "aws_athena_named_query" "spine_stage_durations_30d" {
  name        = "spine-stage-durations-30d"
  workgroup   = var.athena_workgroup_name
  database    = local.spine_glue_database
  description = "p50/p95 stage duration (finished_at - started_at) over the trailing 30 days."
  query       = local.spine_stage_durations_30d_sql
}

resource "aws_athena_named_query" "spine_rerun_noop_rate" {
  name        = "spine-rerun-noop-rate"
  workgroup   = var.athena_workgroup_name
  database    = local.spine_glue_database
  description = "Share of attempts whose every row is skipped-guard with facts_appended = 0 (003 phase-gate criterion)."
  query       = local.spine_rerun_noop_rate_sql
}
