# monitoring.tf -- LLD S11.4's PER-PIPELINE alarm rows (the platform-scoped
# rows -- router errors, RunLedgerLoss -- live in the sibling
# `modules/spine-platform`, per that module's own monitoring.tf header,
# which explicitly defers these four rows here).
#
# Ambiguity 4 (main.tf header): `modules/spine-platform` owns its own
# conditional SNS topic but does not output its ARN, so this module owns
# an independent optional `alert_email` -> SNS topic, mirroring the same
# pattern both `ingestion/terraform/modules/platform` and
# `modules/spine-platform` already use independently.

resource "aws_sns_topic" "alerts" {
  count = var.alert_email != "" ? 1 : 0
  name  = "${local.job_name}-alerts"
}

resource "aws_sns_topic_subscription" "alerts_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

locals {
  alarm_actions = var.alert_email != "" ? [aws_sns_topic.alerts[0].arn] : []
}

# "Per-pipeline ExecutionsFailed >= 1, 5 m" -- native AWS/States metric,
# dimensioned by this pipeline's own state machine ARN (a real, known-at-
# apply-time dimension value, unlike the Metrics Insights alarms below).
resource "aws_cloudwatch_metric_alarm" "executions_failed" {
  alarm_name          = "${local.job_name}-executions-failed"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.this.arn
  }

  alarm_actions = local.alarm_actions
}

# "Per-pipeline ExecutionsTimedOut >= 1, 5 m" -- retries exhausted by time
# (I-18): a healthy retry is never killed mid-run (T-2's timeout
# arithmetic), so this alarm firing genuinely means a stuck batch.
resource "aws_cloudwatch_metric_alarm" "executions_timed_out" {
  alarm_name          = "${local.job_name}-executions-timed-out"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1
  metric_name         = "ExecutionsTimedOut"
  namespace           = "AWS/States"
  period              = 300
  statistic           = "Sum"
  treat_missing_data  = "notBreaching"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.this.arn
  }

  alarm_actions = local.alarm_actions
}

# "Quarantine rate: QuarantinedRows / RawRows > 0.05, 1 h, per-pipeline" --
# metric math over two Metrics Insights SELECTs, each filtered to THIS
# pipeline's own `pipeline` dimension value (S11.1: `Conveyer/Spine`,
# dims `pipeline`, `feed_id` [+ `stage` on stage metrics] -- QuarantinedRows
# carries the `stage` dimension too).
resource "aws_cloudwatch_metric_alarm" "quarantine_rate" {
  alarm_name          = "${local.job_name}-quarantine-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0.05
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  metric_query {
    id          = "quarantined"
    expression  = "SELECT SUM(QuarantinedRows) FROM SCHEMA(\"Conveyer/Spine\", pipeline, feed_id, stage) WHERE pipeline = '${var.pipeline}'"
    period      = 3600
    return_data = false
  }

  metric_query {
    id          = "raw"
    expression  = "SELECT SUM(RawRows) FROM SCHEMA(\"Conveyer/Spine\", pipeline, feed_id) WHERE pipeline = '${var.pipeline}'"
    period      = 3600
    return_data = false
  }

  metric_query {
    id          = "rate"
    expression  = "quarantined / raw"
    period      = 3600
    return_data = true
  }
}

# "JobAttempts > 10, 1 h, per-pipeline" -- storm-throttle signal (S-13):
# `max_concurrent_runs` bounds spend, this alarms an operator instead of
# letting a partner-driven storm run unnoticed.
resource "aws_cloudwatch_metric_alarm" "job_attempts" {
  alarm_name          = "${local.job_name}-job-attempts"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 10
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  metric_query {
    id          = "m1"
    expression  = "SELECT SUM(JobAttempts) FROM SCHEMA(\"Conveyer/Spine\", pipeline, feed_id) WHERE pipeline = '${var.pipeline}'"
    period      = 3600
    return_data = true
  }
}
