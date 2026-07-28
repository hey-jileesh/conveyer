# monitoring.tf -- alarms per LLD S11.3, actions -> SNS if var.alert_email
# is set.

resource "aws_sns_topic" "alerts" {
  count = var.alert_email != "" ? 1 : 0
  name  = "${local.p}-ingestion-alerts"
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

# DLQ ApproximateNumberOfMessagesVisible > 0.
resource "aws_cloudwatch_metric_alarm" "dlq_messages_visible" {
  alarm_name          = "${local.p}-ingestion-dlq-messages-visible"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }

  alarm_actions = local.alarm_actions
}

# Per-function Errors > 0 (5 m).
resource "aws_cloudwatch_metric_alarm" "function_errors" {
  for_each = {
    registrar   = aws_lambda_function.registrar.function_name
    absence     = aws_lambda_function.absence.function_name
    maintenance = aws_lambda_function.maintenance.function_name
  }

  alarm_name          = "${local.p}-${each.key}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  threshold           = 0
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = each.value
  }

  alarm_actions = local.alarm_actions
}

# Unreadable >= 1 (1 h). `Conveyer/Ingestion` metrics are dimensioned by
# `feed_id` (S11.2) -- a value unknown at platform-apply time (feeds come
# and go). A CloudWatch Metrics Insights SELECT, not a fixed-dimension
# metric reference, is what lets the alarm fire on ANY feed without
# enumerating feed_ids here.
resource "aws_cloudwatch_metric_alarm" "unreadable" {
  alarm_name          = "${local.p}-ingestion-unreadable"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  metric_query {
    id          = "m1"
    expression  = "SELECT SUM(Unreadable) FROM SCHEMA(\"Conveyer/Ingestion\", feed_id)"
    period      = 3600
    return_data = true
  }
}

# StuckClaimsRecovered >= 1 (1 h).
resource "aws_cloudwatch_metric_alarm" "stuck_claims_recovered" {
  alarm_name          = "${local.p}-ingestion-stuck-claims-recovered"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  metric_query {
    id          = "m1"
    expression  = "SELECT SUM(StuckClaimsRecovered) FROM SCHEMA(\"Conveyer/Ingestion\", feed_id)"
    period      = 3600
    return_data = true
  }
}

# OverdueEmitted >= 1 (1 h) -- an operational page, distinct from the
# `delivery-overdue` event itself (which downstream consumers act on).
resource "aws_cloudwatch_metric_alarm" "overdue_emitted" {
  alarm_name          = "${local.p}-ingestion-overdue-emitted"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  metric_query {
    id          = "m1"
    expression  = "SELECT SUM(OverdueEmitted) FROM SCHEMA(\"Conveyer/Ingestion\", feed_id)"
    period      = 3600
    return_data = true
  }
}
