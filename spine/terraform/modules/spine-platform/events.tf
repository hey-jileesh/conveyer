# events.tf -- the delivery-registered -> router rule/target, the bus
# resource policy (I-22), and the spine DLQ (LLD S10.2).

# --- rule + target -----------------------------------------------------

resource "aws_cloudwatch_event_rule" "delivery_registered" {
  name           = "${local.p}-spine-delivery-registered"
  event_bus_name = var.event_bus_name
  event_pattern = jsonencode({
    source      = ["conveyer.ingestion"]
    detail-type = ["delivery-registered"]
  })
}

resource "aws_cloudwatch_event_target" "router" {
  rule           = aws_cloudwatch_event_rule.delivery_registered.name
  event_bus_name = var.event_bus_name
  arn            = aws_lambda_function.router.arn

  retry_policy {
    maximum_retry_attempts = 2
  }

  dead_letter_config {
    arn = aws_sqs_queue.spine_dlq.arn
  }
}

# --- bus resource policy (I-22) -----------------------------------------
#
# See main.tf's header for the single-writer assumption this rests on: this
# module is the bus's ONLY resource-policy owner as of 2026-07-29.
#
# Critique F2: for SAME-ACCOUNT principals, EventBridge authorizes on the
# UNION of identity policy and resource policy -- an Allow-only resource
# policy (below) therefore blocks nothing. ingestion's registrar/absence/
# feed-driver roles (ingestion/terraform/modules/{platform,feed}/iam.tf)
# each hold a plain, UNCONDITIONED `events:PutEvents` identity grant on this
# bus (no `events:source` restriction at the identity layer), so any one of
# them -- or any other same-account role someone later grants
# `events:PutEvents` to -- could already forge a `delivery-registered` with
# hostile `object_uris` or squat a future `batch_id` (I-22's own rationale)
# purely on the strength of its identity policy, with this resource policy
# never in a position to say no. The three Deny statements below close
# that: an explicit Deny in a resource policy applies to same-account
# principals too and always wins over any Allow (identity- or
# resource-side), which is what makes the by-construction claim in I-22
# true rather than aspirational.
#
# Mirrors this module's own s3.tf `DenySpinePrefixWriteExceptDeploy` /
# ingestion's `DenyCanonicalPutExceptFramework` shape: Deny-except-the-
# known-principals, not Allow-only.
#
# Third statement (`DenyUnknownSource`, below): I-22's own wording --
# "events:PutEvents only for the ingestion producer roles and the spine job
# role" -- names exactly two legitimate sources and no others, so a blanket
# deny of any third `events:source` value is in scope, not an
# over-tightening. It is safe to add here specifically because `${p}-bus`
# is a **custom** event bus (`aws_cloudwatch_event_bus.ingestion` in
# ingestion/terraform/modules/platform/events.tf), not the account's
# default bus: AWS service events (S3 notifications, CloudWatch alarm
# state changes, Health events, etc.) are delivered to the default bus
# only, and nothing in ingestion's or spine's Terraform configures
# cross-bus rule targets or a partner event source onto this bus (verified
# by inspection, 2026-07-29 -- the bus's only `events:PutEvents` callers
# are the registrar, absence, and feed-driver roles, all same-account IAM
# identities, all publishing `conveyer.ingestion`). A blanket
# deny-unknown-source would be unsafe on the default bus; it is not on this
# one.
#
# Known residual: `events:source` (like `aws:PrincipalArn`) is evaluated
# per API call, and `PutEvents` accepts up to 10 entries per call with
# potentially different `Source` values -- a caller batching one
# legitimate-source entry with one forged-source entry in a single call
# sees the condition key take on multiple values, and multi-valued
# StringEquals/StringNotEquals context keys have documented, easy-to-get-
# wrong evaluation semantics (AWS: "Creating a Condition That Tests
# Multiple Values"). None of this module's callers (registrar, absence,
# feed drivers, the future spine job role) are known to batch mixed-source
# entries in one call; if a future producer starts doing so, this
# statement set should be re-audited against the batched-call semantics
# before being trusted for it.

locals {
  # `StringNotLike` requires >= 1 value. Phase 1 defaults
  # var.spine_job_role_arns to [] (no spine job role exists yet -- I-21's
  # sibling module isn't built by this bead). Rather than omit
  # `DenyForgedSpineSource` entirely in that case (which would leave
  # events:source = conveyer.spine wide open to any same-account
  # identity-policy grant until the first spine job role ships) or emit an
  # invalid empty-values condition, fall back to an unreachable sentinel
  # ARN: no real IAM principal will ever match it, so the Deny applies to
  # EVERY principal -- correct, since no principal is legitimately entitled
  # to that source yet.
  spine_source_deny_principal_arns = (
    length(var.spine_job_role_arns) > 0
    ? var.spine_job_role_arns
    : ["arn:aws:iam::000000000000:role/unreachable-sentinel-no-spine-job-role-yet"]
  )
}

data "aws_iam_policy_document" "bus_policy" {
  statement {
    sid       = "AllowIngestionProducers"
    effect    = "Allow"
    actions   = ["events:PutEvents"]
    resources = [var.event_bus_arn]

    principals {
      type        = "AWS"
      identifiers = var.ingestion_producer_role_arns
    }

    condition {
      test     = "StringEquals"
      variable = "events:source"
      values   = ["conveyer.ingestion"]
    }
  }

  # Deny events:source = conveyer.ingestion for anyone other than the
  # declared ingestion producer roles -- closes the identity-policy-union
  # gap for THIS source value regardless of what any role's own identity
  # policy grants.
  statement {
    sid       = "DenyForgedIngestionSource"
    effect    = "Deny"
    actions   = ["events:PutEvents"]
    resources = [var.event_bus_arn]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "StringEquals"
      variable = "events:source"
      values   = ["conveyer.ingestion"]
    }

    condition {
      test     = "StringNotLike"
      variable = "aws:PrincipalArn"
      values   = var.ingestion_producer_role_arns
    }
  }

  # Omitted entirely (not an empty-principal statement) until at least one
  # per-pipeline spine-job role exists -- var.spine_job_role_arns defaults
  # to [].
  dynamic "statement" {
    for_each = length(var.spine_job_role_arns) > 0 ? [1] : []
    content {
      sid       = "AllowSpineJobRoles"
      effect    = "Allow"
      actions   = ["events:PutEvents"]
      resources = [var.event_bus_arn]

      principals {
        type        = "AWS"
        identifiers = var.spine_job_role_arns
      }

      condition {
        test     = "StringEquals"
        variable = "events:source"
        values   = ["conveyer.spine"]
      }
    }
  }

  # Deny events:source = conveyer.spine for anyone other than the declared
  # spine job roles -- the conveyer.spine counterpart of
  # DenyForgedIngestionSource above. UNLIKE the AllowSpineJobRoles
  # statement, this is never omitted: it must hold even before any spine
  # job role exists (see local.spine_source_deny_principal_arns).
  statement {
    sid       = "DenyForgedSpineSource"
    effect    = "Deny"
    actions   = ["events:PutEvents"]
    resources = [var.event_bus_arn]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "StringEquals"
      variable = "events:source"
      values   = ["conveyer.spine"]
    }

    condition {
      test     = "StringNotLike"
      variable = "aws:PrincipalArn"
      values   = local.spine_source_deny_principal_arns
    }
  }

  # Deny any events:source value other than the two I-22 names. Safe on
  # THIS bus because it is custom, not the account default bus -- see the
  # header comment above this data source for the verification this rests
  # on.
  statement {
    sid       = "DenyUnknownSource"
    effect    = "Deny"
    actions   = ["events:PutEvents"]
    resources = [var.event_bus_arn]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "StringNotEquals"
      variable = "events:source"
      values   = ["conveyer.ingestion", "conveyer.spine"]
    }
  }
}

resource "aws_cloudwatch_event_bus_policy" "spine" {
  event_bus_name = var.event_bus_name
  policy         = data.aws_iam_policy_document.bus_policy.json
}

# --- spine DLQ -----------------------------------------------------------
#
# KMS-SSE (AWS-managed `alias/aws/sqs`, same choice as ingestion's DLQ --
# no extra key-policy grants needed for principals that already hold
# sqs:SendMessage/ReceiveMessage), 14 d retention.

resource "aws_sqs_queue" "spine_dlq" {
  name                      = "${local.p}-spine-dlq"
  message_retention_seconds = 14 * 24 * 60 * 60
  kms_master_key_id         = "alias/aws/sqs"
}

# Queue policy [S-14]: TLS-only deny, plus sqs:SendMessage for the
# EventBridge rule's dead-letter delivery (scoped to THIS rule's ARN --
# unlike ingestion's DLQ, this module knows the exact rule ARN, so no
# account-wide fallback is needed) and for the router role's own DLQ send
# (defense in depth alongside the identity-based DlqSend grant in iam.tf --
# a Lambda function's own dead_letter_config is delivered using the
# function's execution role, so the identity-based grant is what actually
# makes it work; the resource-policy statement documents the same intent
# on the queue side).
data "aws_iam_policy_document" "spine_dlq_policy" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["sqs:*"]
    resources = [aws_sqs_queue.spine_dlq.arn]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  statement {
    sid       = "AllowEventBridgeRuleSend"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.spine_dlq.arn]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.delivery_registered.arn]
    }
  }

  statement {
    sid       = "AllowRouterRoleSend"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.spine_dlq.arn]

    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.spine_router.arn]
    }
  }
}

resource "aws_sqs_queue_policy" "spine_dlq" {
  queue_url = aws_sqs_queue.spine_dlq.id
  policy    = data.aws_iam_policy_document.spine_dlq_policy.json
}
