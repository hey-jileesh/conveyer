# s3-push feeds -- LLD S10.7. EventBridge rule on the DEFAULT bus (S3
# notifications land there, not the custom ${p}-bus) matches partner
# uploads under this feed's vestibule and invokes the shared registrar.
# No per-feed compute, role, or secret exists on this path -- the
# registrar (platform-owned) is the only code that runs.

locals {
  # Manifest mode: the rule must fire ONLY on manifest_pattern-suffixed
  # keys (LLD S8.2 step 2: "rule fires only on manifest_pattern-suffixed
  # keys") -- a single combined wildcard, not an OR of prefix-and-suffix,
  # so trailer/data-object uploads under the same vestibule never invoke
  # the registrar. manifest_pattern is validated elsewhere (S6.1) to be
  # "*<literal-suffix>" with no other wildcards, so concatenating the
  # prefix directly onto it folds its leading "*" into the one wildcard
  # (LLD S10.7's "folding manifest_pattern's leading * into the wildcard").
  # `try()` guards feeds outside manifest mode, where the key may be
  # absent from the raw YAML entirely.
  s3_push_manifest_suffix = try(var.feed.completeness.manifest_pattern, "*.manifest.json")

  s3_push_event_pattern_manifest = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [var.platform.landing_bucket_name] }
      object = {
        key = [
          { wildcard = "${local.incoming_prefix}${local.s3_push_manifest_suffix}" },
        ]
      }
    }
  })

  # Trailer mode (and any non-manifest mode reachable by s3-push, i.e.
  # trailer -- D-10 rejects s3-push + timer at config validation): rule
  # fires on every incoming/ object (LLD S8.2 step 2).
  s3_push_event_pattern_prefix = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [var.platform.landing_bucket_name] }
      object = {
        key = [
          { prefix = local.incoming_prefix },
        ]
      }
    }
  })

  # completeness.mode is a required FeedConfig field (S6.1) -- always
  # present, unlike manifest_pattern above -- so no try() needed here.
  s3_push_event_pattern = (
    var.feed.completeness.mode == "manifest"
    ? local.s3_push_event_pattern_manifest
    : local.s3_push_event_pattern_prefix
  )
}

resource "aws_cloudwatch_event_rule" "s3_push" {
  count = local.is_s3_push ? 1 : 0

  name           = "${local.p}-s3-push-${local.slug}"
  description    = "S3 Object Created (vestibule) -> registrar for feed ${var.feed.feed_id} (LLD S10.7)"
  event_bus_name = "default"
  event_pattern  = local.s3_push_event_pattern
}

resource "aws_cloudwatch_event_target" "s3_push_registrar" {
  count = local.is_s3_push ? 1 : 0

  rule           = aws_cloudwatch_event_rule.s3_push[0].name
  event_bus_name = "default"
  target_id      = "registrar"
  arn            = var.platform.registrar_function_arn

  # SECURITY-GATE FIX (M-6): under burst + the registrar's reserved
  # concurrency of 10, EventBridge throttles, retries for up to
  # maximum_event_age_in_seconds, then DROPS the event with no ledger row,
  # no DLQ entry, and no alarm -- a silent drop, violating the project's
  # "quarantine with reasons, never silent drops" invariant. Every other
  # target in this codebase (the platform schedules, S10.5) already carries
  # this shape; this was the one target that didn't.
  dead_letter_config {
    arn = var.platform.dlq_arn
  }

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 5
  }
}

resource "aws_lambda_permission" "s3_push_registrar" {
  count = local.is_s3_push ? 1 : 0

  statement_id  = "AllowEventBridge-${local.slug}"
  action        = "lambda:InvokeFunction"
  function_name = var.platform.registrar_function_name
  principal     = "events.amazonaws.com"
  # Scoped to this feed's rule ARN only (LLD S10.7) -- one feed's rule
  # cannot be used to forge an invocation on behalf of another.
  source_arn = aws_cloudwatch_event_rule.s3_push[0].arn
}
