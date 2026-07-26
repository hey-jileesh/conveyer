# athena.tf -- the operator-console workgroup + four named queries (LLD
# S10.4 / S11.4). Results write to `s3://${p}-artifacts/athena-results/`
# (lifecycle-expired after 30 d, s3.tf); workgroup config is enforced so no
# per-query override can escape it.

resource "aws_athena_workgroup" "ingestion" {
  name = "${local.p}-ingestion"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.artifacts.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

locals {
  ledger_identifier = "${local.glue_database}.${local.ledger_table}"

  # Latest-disposition fold in SQL (S7.4's `folds.latest_dispositions`,
  # expressed for Athena): window by `delivery_id` over `recorded_at`.
  current_dispositions_sql = <<-SQL
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY delivery_id ORDER BY recorded_at DESC
        ) AS rn
        FROM ${local.ledger_identifier}
    )
    SELECT
        delivery_id, feed_id, delivery_key, batch_id, content_hash, size_bytes,
        manifest_ref, asserted_record_count, completeness_mode,
        received_at, recorded_at, disposition, supersedes, driver,
        driver_run_id, notes
    FROM latest
    WHERE rn = 1
    ORDER BY feed_id, delivery_key
  SQL

  feed_watermarks_sql = <<-SQL
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY delivery_id ORDER BY recorded_at DESC
        ) AS rn
        FROM ${local.ledger_identifier}
    ),
    current AS (
        SELECT * FROM latest WHERE rn = 1
    )
    SELECT
        feed_id,
        MAX(CASE WHEN disposition = 'registered' THEN received_at END) AS last_registered_received_at,
        MAX(recorded_at) AS last_ledger_activity_at
    FROM current
    GROUP BY feed_id
    ORDER BY feed_id
  SQL

  duplicate_rate_30d_sql = <<-SQL
    WITH recent AS (
        SELECT *
        FROM ${local.ledger_identifier}
        WHERE recorded_at >= date_add('day', -30, current_timestamp)
    )
    SELECT
        feed_id,
        COUNT_IF(disposition = 'duplicate') AS duplicates,
        COUNT_IF(disposition = 'registered') AS registered,
        CAST(COUNT_IF(disposition = 'duplicate') AS DOUBLE)
            / NULLIF(COUNT_IF(disposition IN ('registered', 'duplicate')), 0) AS duplicate_rate
    FROM recent
    GROUP BY feed_id
    ORDER BY feed_id
  SQL

  # Lineage: batch_id -> objects/driver run. Operator replaces <BATCH_ID>
  # before running (Athena named queries are templates, not parameterized
  # prepared statements).
  deliveries_for_batch_sql = <<-SQL
    SELECT
        delivery_id, feed_id, delivery_key, disposition, driver, driver_run_id,
        received_at, recorded_at, object_uris, objects, notes
    FROM ${local.ledger_identifier}
    WHERE batch_id = '<BATCH_ID>'
    ORDER BY recorded_at
  SQL
}

resource "aws_athena_named_query" "current_dispositions" {
  name        = "current-dispositions"
  workgroup   = aws_athena_workgroup.ingestion.id
  database    = local.glue_database
  description = "Latest disposition per delivery_id (S7.4 fold, in SQL)."
  query       = local.current_dispositions_sql
}

resource "aws_athena_named_query" "feed_watermarks" {
  name        = "feed-watermarks"
  workgroup   = aws_athena_workgroup.ingestion.id
  database    = local.glue_database
  description = "Per-feed last registered delivery + last ledger activity."
  query       = local.feed_watermarks_sql
}

resource "aws_athena_named_query" "duplicate_rate_30d" {
  name        = "duplicate-rate-30d"
  workgroup   = aws_athena_workgroup.ingestion.id
  database    = local.glue_database
  description = "Per-feed duplicate rate over the trailing 30 days."
  query       = local.duplicate_rate_30d_sql
}

resource "aws_athena_named_query" "deliveries_for_batch" {
  name        = "deliveries-for-batch"
  workgroup   = aws_athena_workgroup.ingestion.id
  database    = local.glue_database
  description = "Lineage: batch_id -> objects/driver run. Replace <BATCH_ID> before running."
  query       = local.deliveries_for_batch_sql
}
