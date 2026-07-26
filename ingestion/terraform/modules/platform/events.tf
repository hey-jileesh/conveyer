# events.tf -- custom bus, Phase 1 observability rule -> log group, SQS DLQ
# (LLD S10.5).

resource "aws_cloudwatch_event_bus" "ingestion" {
  name = "${local.p}-bus"
}

resource "aws_cloudwatch_log_group" "events" {
  name              = "/conveyer/${var.env}/ingestion/events"
  retention_in_days = 14
}

resource "aws_cloudwatch_event_rule" "observability" {
  name           = "${local.p}-ingestion-observability"
  event_bus_name = aws_cloudwatch_event_bus.ingestion.name
  event_pattern = jsonencode({
    source = ["conveyer.ingestion"]
  })
}

resource "aws_cloudwatch_event_target" "observability_logs" {
  rule           = aws_cloudwatch_event_rule.observability.name
  event_bus_name = aws_cloudwatch_event_bus.ingestion.name
  arn            = aws_cloudwatch_log_group.events.arn
}

# Required for the EventBridge rule above to actually be able to write into
# the log group target (not itself a named LLD line item, but the target
# wiring is inert without it).
data "aws_iam_policy_document" "events_to_logs" {
  statement {
    sid     = "EventBridgeToCloudWatchLogs"
    effect  = "Allow"
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = ["${aws_cloudwatch_log_group.events.arn}:*"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.observability.arn]
    }
  }
}

resource "aws_cloudwatch_log_resource_policy" "events" {
  policy_name     = "${local.p}-ingestion-events-to-logs"
  policy_document = data.aws_iam_policy_document.events_to_logs.json
}

# SQS DLQ -- KMS-SSE, 14 d retention. Uses the AWS-managed `alias/aws/sqs`
# key: unlike a customer-managed key, it requires no extra key-policy grants
# for principals that already hold `sqs:SendMessage`/`sqs:ReceiveMessage`,
# which keeps the S10.6 IAM table's plain `sqs:SendMessage on DLQ` grant
# sufficient on its own.
resource "aws_sqs_queue" "dlq" {
  name                      = "${local.p}-ingestion-dlq"
  message_retention_seconds = 14 * 24 * 60 * 60
  kms_master_key_id         = "alias/aws/sqs"
}

# M-6 completion: an EventBridge RULE target's `dead_letter_config` (unlike
# a Lambda function's own DLQ, or an EventBridge Scheduler target's DLQ --
# both identity-based, granted via the IAM `DlqSend` statements elsewhere in
# this module) is delivered by the EventBridge SERVICE itself, which is not
# an IAM principal this account controls. AWS requires a queue
# resource-based policy granting `events.amazonaws.com` permission before
# any rule target's dead-letter delivery can succeed; without it, the
# `dead_letter_config` added to the s3-push EventBridge target (modules/feed
# s3_push.tf, M-6) would be silently inert -- the exact "no ledger row, no
# DLQ, no alarm" failure mode the fix exists to close. Scoped by
# `aws:SourceAccount` (not a specific rule ARN) because the rules that use
# this DLQ are created per-feed by the sibling `modules/feed`, which this
# module cannot reference forward -- same naming-convention-wildcard
# rationale already used elsewhere in this codebase (s3.tf's landing bucket
# policy, this module's own `scheduler` role grant).
data "aws_iam_policy_document" "dlq_policy" {
  statement {
    sid       = "AllowEventBridgeSameAccount"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_sqs_queue_policy" "dlq" {
  queue_url = aws_sqs_queue.dlq.id
  policy    = data.aws_iam_policy_document.dlq_policy.json
}
