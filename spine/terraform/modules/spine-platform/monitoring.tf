# monitoring.tf -- LLD S11.4's PLATFORM-scoped alarm rows only, verbatim
# thresholds. Per-pipeline rows (`ExecutionsFailed`, `ExecutionsTimedOut`,
# quarantine rate, `JobAttempts`) live in the sibling `modules/spine-pipeline`
# -- they alarm on a specific state machine / pipeline dimension this module
# has no per-pipeline resource to attach to. `SingleFlightCollisions` is a
# metric (S11.1), not an alarm row in S11.4 -- no alarm resource for it.
#
# Actions -> SNS if var.alert_email is set (ingestion 002.1 S11.3 pattern).

resource "aws_sns_topic" "spine_alerts" {
  count = var.alert_email != "" ? 1 : 0
  name  = "${local.p}-spine-alerts"
}

resource "aws_sns_topic_subscription" "spine_alerts_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.spine_alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

locals {
  spine_alarm_actions = var.alert_email != "" ? [aws_sns_topic.spine_alerts[0].arn] : []
}

# "Router errors / DLQ visible >= 1, 5 m" -- S11.4's single table row,
# implemented as the two underlying signals (mirrors ingestion's own split
# between `function_errors` and `dlq_messages_visible`, modules/platform
# monitoring.tf).

resource "aws_cloudwatch_metric_alarm" "router_errors" {
  alarm_name          = "${local.p}-spine-router-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.router.function_name
  }

  alarm_actions = local.spine_alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "spine_dlq_messages_visible" {
  alarm_name          = "${local.p}-spine-dlq-messages-visible"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.spine_dlq.name
  }

  alarm_actions = local.spine_alarm_actions
}

# "RunLedgerLoss >= 1, 1 h". `Conveyer/Spine` metrics are dimensioned by
# `pipeline`/`feed_id` (S11.1) -- values unknown at platform-apply time
# (pipelines come and go, same reasoning as ingestion's `feed_id`-dimensioned
# alarms). A CloudWatch Metrics Insights SELECT, not a fixed-dimension
# metric reference, lets the alarm fire on ANY pipeline without enumerating
# them here.
resource "aws_cloudwatch_metric_alarm" "run_ledger_loss" {
  alarm_name          = "${local.p}-spine-run-ledger-loss"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.spine_alarm_actions

  metric_query {
    id          = "m1"
    expression  = "SELECT SUM(RunLedgerLoss) FROM SCHEMA(\"Conveyer/Spine\", pipeline, feed_id)"
    period      = 3600
    return_data = true
  }
}
