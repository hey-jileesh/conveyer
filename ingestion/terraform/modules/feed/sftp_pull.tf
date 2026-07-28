# sftp-pull feeds -- LLD S10.7. Per-feed secret shell, driver Lambda (same
# shared image, this feed's own IAM role from iam.tf), and its Scheduler
# trigger from source.yaml's `trigger` block.

resource "aws_secretsmanager_secret" "sftp" {
  count = local.is_sftp_pull ? 1 : 0

  # "${p}/sftp/<source>/<feed>" (LLD S5, S6.7) -- the ARN this naming rule
  # produces is deterministic, which is why source.yaml plugins can commit
  # the well-known ARN as a placeholder before the secret exists (S15.1).
  name                    = "${local.p}/sftp/${var.feed.feed_id}"
  description             = "SFTP connection secret for feed ${var.feed.feed_id} (LLD S6.7)."
  recovery_window_in_days = 7

  # Value is set out-of-band via `make put-secret` (D-13) -- Terraform
  # only creates the shell; the SecretString is never written here and
  # never appears in Terraform state.
}

# SECURITY-GATE FIX (M-9-tf): declared explicitly with a bounded retention --
# left undeclared, Lambda auto-creates `/aws/lambda/<name>` on first
# invocation with retention NEVER EXPIRE, and this function logs delivery
# keys, filenames, ClaimItem reprs, and error strings (S11.1) into it
# indefinitely. Named by the static function-name string (not
# `aws_lambda_function.driver[0].function_name`) to avoid a dependency
# cycle; the function `depends_on` its log group below so Lambda never races
# the auto-create against our own explicit creation.
resource "aws_cloudwatch_log_group" "driver" {
  count = local.is_sftp_pull ? 1 : 0

  name              = "/aws/lambda/${local.p}-driver-${local.slug}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "driver" {
  count = local.is_sftp_pull ? 1 : 0

  function_name = "${local.p}-driver-${local.slug}"
  role          = aws_iam_role.feed[0].arn

  package_type = "Image"
  image_uri    = var.image_uri

  image_config {
    command = ["ingestion.entrypoints.sftp_pull.handler"]
  }

  timeout     = 900
  memory_size = 2048

  environment {
    variables = merge(
      var.platform.common_env,
      {
        CONVEYER_FEED_ID = var.feed.feed_id
        # S9.2 step 5's per-run byte budget; read directly via os.environ
        # by drivers/sftp_pull.py::_budget_bytes(), NOT a RuntimeConfig
        # field (see ingestion agent-memory m4-sftp-pull-design-notes.md).
        CONVEYER_DRIVER_BYTES_BUDGET = tostring(var.driver_bytes_budget)
      }
    )
  }

  dead_letter_config {
    target_arn = var.platform.dlq_arn
  }

  depends_on = [aws_cloudwatch_log_group.driver]
}

resource "aws_scheduler_schedule" "driver" {
  count = local.is_sftp_pull ? 1 : 0

  name = "${local.p}-driver-${local.slug}"

  # Same FLEXIBLE-15m / retry-2 / DLQ shape as the platform-level absence
  # and maintenance schedules (LLD S10.5); S10.7 only calls out the
  # feed-specific fields (schedule_expression[_timezone], empty input).
  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 15
  }

  schedule_expression          = var.feed.trigger.schedule
  schedule_expression_timezone = var.feed.trigger.timezone

  target {
    arn      = aws_lambda_function.driver[0].arn
    role_arn = var.platform.scheduler_role_arn
    # {} -- window defaults to the ledger fold (LLD S10.7, S9.2 payload
    # shape 1).
    input = jsonencode({})

    retry_policy {
      maximum_retry_attempts = 2
    }

    dead_letter_config {
      arn = var.platform.dlq_arn
    }
  }
}
