# Security-gate regression tests -- C-1, M-10, M-3, M-5, M-6, M-9-tf (platform
# half; see modules/feed/tests/security_fixes.tftest.hcl for the feed half).
#
# Every assertion in the single `security_fixes` run block below was
# verified, by temporarily reverting the corresponding source file with
# `git stash` and re-running `terraform test`, to FAIL against the pre-fix
# code -- see the bead handoff report for the per-finding evidence.
#
# Mechanics: `data.aws_iam_policy_document.*.json` is a provider-computed
# attribute, so it is only known post-`apply` (a data source with any input
# derived from a not-yet-created resource's ARN is unknown during `plan` --
# ordinary Terraform behavior, not a test-framework quirk). We therefore use
# a REAL (non-mock) `provider "aws"` -- `aws_iam_policy_document` makes no
# network call regardless -- with `skip_credentials_validation` etc. so the
# provider itself never calls STS, `override_data` for the one genuine API
# call (`data.aws_caller_identity.current`), and `override_resource` for
# every OTHER resource in the module so `apply` never touches real AWS. This
# is the only combination that yields the real, HCL-computed policy JSON
# without live credentials (mock_provider substitutes zero-valued/garbage
# data for data sources too, which breaks downstream IAM-JSON-shape
# validation -- confirmed empirically, not by inference, before settling on
# this shape).

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
}

variables {
  name_prefix = "conveyer"
  env         = "test"
  region      = "us-east-1"
  image_uri   = "123456789012.dkr.ecr.us-east-1.amazonaws.com/conveyer-ingestion:abc123"
  feeds_json  = jsonencode({ registry_version = 1, feeds = [] })
}

run "security_fixes" {
  command = apply

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:root"
      user_id    = "AIDAEXAMPLE"
    }
  }

  override_resource {
    target = aws_s3_bucket.landing
    values = { id = "conveyer-test-landing", arn = "arn:aws:s3:::conveyer-test-landing" }
  }
  override_resource {
    target = aws_s3_bucket.lake
    values = { id = "conveyer-test-lake", arn = "arn:aws:s3:::conveyer-test-lake" }
  }
  override_resource {
    target = aws_s3_bucket.artifacts
    values = { id = "conveyer-test-artifacts", arn = "arn:aws:s3:::conveyer-test-artifacts" }
  }
  override_resource { target = aws_s3_bucket_server_side_encryption_configuration.landing }
  override_resource { target = aws_s3_bucket_server_side_encryption_configuration.lake }
  override_resource { target = aws_s3_bucket_server_side_encryption_configuration.artifacts }
  override_resource { target = aws_s3_bucket_public_access_block.landing }
  override_resource { target = aws_s3_bucket_public_access_block.lake }
  override_resource { target = aws_s3_bucket_public_access_block.artifacts }
  override_resource { target = aws_s3_bucket_versioning.landing }
  override_resource { target = aws_s3_bucket_notification.landing }
  override_resource { target = aws_s3_bucket_lifecycle_configuration.landing }
  override_resource { target = aws_s3_bucket_lifecycle_configuration.artifacts }
  override_resource { target = aws_s3_bucket_policy.landing }
  override_resource { target = aws_s3_bucket_policy.lake }
  override_resource { target = aws_s3_bucket_policy.artifacts }
  override_resource { target = aws_s3_object.feeds_json }
  override_resource {
    target = aws_dynamodb_table.cas
    values = { name = "conveyer-test-ingestion-cas", arn = "arn:aws:dynamodb:us-east-1:123456789012:table/conveyer-test-ingestion-cas" }
  }
  override_resource { target = aws_glue_catalog_database.ingestion }
  override_resource {
    target = aws_athena_workgroup.ingestion
    values = { id = "conveyer-test-ingestion", name = "conveyer-test-ingestion", arn = "arn:aws:athena:us-east-1:123456789012:workgroup/conveyer-test-ingestion" }
  }
  override_resource { target = aws_athena_named_query.current_dispositions }
  override_resource { target = aws_athena_named_query.feed_watermarks }
  override_resource { target = aws_athena_named_query.duplicate_rate_30d }
  override_resource { target = aws_athena_named_query.deliveries_for_batch }
  override_resource {
    target = aws_cloudwatch_event_bus.ingestion
    values = { name = "conveyer-test-bus", arn = "arn:aws:events:us-east-1:123456789012:event-bus/conveyer-test-bus" }
  }
  override_resource {
    target = aws_cloudwatch_log_group.events
    values = { name = "/conveyer/test/ingestion/events", arn = "arn:aws:logs:us-east-1:123456789012:log-group:/conveyer/test/ingestion/events" }
  }
  override_resource {
    target = aws_cloudwatch_log_group.registrar
    values = { name = "/aws/lambda/conveyer-test-registrar", arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/conveyer-test-registrar" }
  }
  override_resource {
    target = aws_cloudwatch_log_group.absence
    values = { name = "/aws/lambda/conveyer-test-absence", arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/conveyer-test-absence" }
  }
  override_resource {
    target = aws_cloudwatch_log_group.maintenance
    values = { name = "/aws/lambda/conveyer-test-maintenance", arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/conveyer-test-maintenance" }
  }
  override_resource {
    target = aws_cloudwatch_event_rule.observability
    values = { name = "conveyer-test-ingestion-observability", arn = "arn:aws:events:us-east-1:123456789012:rule/conveyer-test-bus/conveyer-test-ingestion-observability" }
  }
  override_resource { target = aws_cloudwatch_event_target.observability_logs }
  override_resource { target = aws_cloudwatch_log_resource_policy.events }
  override_resource {
    target = aws_sqs_queue.dlq
    values = {
      id   = "https://sqs.us-east-1.amazonaws.com/123456789012/conveyer-test-ingestion-dlq"
      name = "conveyer-test-ingestion-dlq"
      arn  = "arn:aws:sqs:us-east-1:123456789012:conveyer-test-ingestion-dlq"
    }
  }
  override_resource { target = aws_sqs_queue_policy.dlq }
  override_resource {
    target = aws_iam_role.registrar
    values = { name = "conveyer-test-registrar", arn = "arn:aws:iam::123456789012:role/conveyer-test-registrar" }
  }
  override_resource {
    target = aws_iam_role.absence
    values = { name = "conveyer-test-absence", arn = "arn:aws:iam::123456789012:role/conveyer-test-absence" }
  }
  override_resource {
    target = aws_iam_role.maintenance
    values = { name = "conveyer-test-maintenance", arn = "arn:aws:iam::123456789012:role/conveyer-test-maintenance" }
  }
  override_resource {
    target = aws_iam_role.scheduler
    values = { name = "conveyer-test-scheduler", arn = "arn:aws:iam::123456789012:role/conveyer-test-scheduler" }
  }
  override_resource { target = aws_iam_role_policy_attachment.registrar_basic }
  override_resource { target = aws_iam_role_policy_attachment.absence_basic }
  override_resource { target = aws_iam_role_policy_attachment.maintenance_basic }
  override_resource { target = aws_iam_role_policy.registrar }
  override_resource { target = aws_iam_role_policy.absence }
  override_resource { target = aws_iam_role_policy.maintenance }
  override_resource { target = aws_iam_role_policy.scheduler }
  override_resource {
    target = aws_lambda_function.registrar
    values = { arn = "arn:aws:lambda:us-east-1:123456789012:function:conveyer-test-registrar", function_name = "conveyer-test-registrar" }
  }
  override_resource {
    target = aws_lambda_function.absence
    values = { arn = "arn:aws:lambda:us-east-1:123456789012:function:conveyer-test-absence", function_name = "conveyer-test-absence" }
  }
  override_resource {
    target = aws_lambda_function.maintenance
    values = { arn = "arn:aws:lambda:us-east-1:123456789012:function:conveyer-test-maintenance", function_name = "conveyer-test-maintenance" }
  }
  override_resource { target = aws_ecr_repository.ingestion }
  override_resource { target = aws_ecr_lifecycle_policy.ingestion }
  override_resource { target = aws_scheduler_schedule.absence }
  override_resource { target = aws_scheduler_schedule.maintenance }
  override_resource { target = aws_cloudwatch_metric_alarm.dlq_messages_visible }
  override_resource { target = aws_cloudwatch_metric_alarm.function_errors }
  override_resource { target = aws_cloudwatch_metric_alarm.unreadable }
  override_resource { target = aws_cloudwatch_metric_alarm.stuck_claims_recovered }
  override_resource { target = aws_cloudwatch_metric_alarm.overdue_emitted }

  # --- C-1 / M-10: the copy-paste class audit ---------------------------
  #
  # Across EVERY role-policy document in iam.tf (ledger_write, ledger_read,
  # registrar, absence, maintenance, scheduler), any statement naming a bare
  # bucket ARN (lake or landing) must be restricted to s3:ListBucket alone.
  # Pre-fix, ledger_write/ledger_read and registrar's old LandingIncomingRead
  # statement mixed the bucket ARN into a resource list shared with
  # s3:Get*/s3:Put*/s3:GetObject -- this assertion fails against that shape.
  assert {
    condition = alltrue([
      for s in concat(
        jsondecode(data.aws_iam_policy_document.ledger_write.json).Statement,
        jsondecode(data.aws_iam_policy_document.ledger_read.json).Statement,
        jsondecode(data.aws_iam_policy_document.registrar.json).Statement,
        jsondecode(data.aws_iam_policy_document.absence.json).Statement,
        jsondecode(data.aws_iam_policy_document.maintenance.json).Statement,
        jsondecode(data.aws_iam_policy_document.scheduler.json).Statement,
      ) :
      (
        !contains(flatten([s.Resource]), aws_s3_bucket.lake.arn) &&
        !contains(flatten([s.Resource]), aws_s3_bucket.landing.arn)
      ) || length(setsubtract(flatten([s.Action]), ["s3:ListBucket"])) == 0
    ])
    error_message = "C-1/M-10 class: a platform IAM statement mixes a bare bucket ARN with a non-ListBucket action (bucket-level write/read exposure via wildcarded or object actions)."
  }

  # --- C-1: no wildcarded s3 actions anywhere in the ledger composites ---
  #
  # Directly targets the pre-fix "s3:Get*"/"s3:Put*"/"s3:List*" actions.
  assert {
    condition = alltrue([
      for s in concat(
        jsondecode(data.aws_iam_policy_document.ledger_write.json).Statement,
        jsondecode(data.aws_iam_policy_document.ledger_read.json).Statement,
      ) :
      alltrue([for a in flatten([s.Action]) : !endswith(a, "*")])
    ])
    error_message = "C-1: ledger_write/ledger_read must grant explicit object actions only, never s3:*-style wildcards."
  }

  # --- C-1: ledger ListBucket is conditioned to ledger/* only ------------
  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.ledger_write.json).Statement :
      contains(flatten([s.Resource]), aws_s3_bucket.lake.arn) &&
      flatten([s.Action]) == ["s3:ListBucket"] &&
      try(contains(flatten([s.Condition.StringLike["s3:prefix"]]), "ledger/*"), false)
    ])
    error_message = "C-1: ledger_write must grant s3:ListBucket on the lake bucket conditioned to prefix ledger/* only."
  }

  # --- M-10: registrar's landing ListBucket is conditioned to */incoming/* -
  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.registrar.json).Statement :
      contains(flatten([s.Resource]), aws_s3_bucket.landing.arn) &&
      flatten([s.Action]) == ["s3:ListBucket"] &&
      try(contains(flatten([s.Condition.StringLike["s3:prefix"]]), "*/incoming/*"), false)
    ])
    error_message = "M-10: registrar must grant s3:ListBucket on the landing bucket conditioned to prefix */incoming/* only -- unconditioned, it lets a compromised registrar enumerate every feed's received_at= tree."
  }

  # --- M-3: registrar holds Abort/List-parts on the received_at= resources
  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.registrar.json).Statement :
      contains(flatten([s.Resource]), "${aws_s3_bucket.landing.arn}/*/received_at=*") &&
      contains(flatten([s.Action]), "s3:AbortMultipartUpload") &&
      contains(flatten([s.Action]), "s3:ListMultipartUploadParts")
    ])
    error_message = "M-3: registrar must hold s3:AbortMultipartUpload + s3:ListMultipartUploadParts on the received_at= resources, or stream_upload's own failure-path abort raises AccessDenied and orphaned parts bill forever."
  }

  # --- M-3: landing lifecycle config aborts incomplete multipart uploads --
  assert {
    condition = anytrue([
      for r in aws_s3_bucket_lifecycle_configuration.landing.rule :
      length(r.abort_incomplete_multipart_upload) > 0 &&
      r.abort_incomplete_multipart_upload[0].days_after_initiation == 7
    ])
    error_message = "M-3: the landing lifecycle configuration must include an abort_incomplete_multipart_upload rule with days_after_initiation = 7."
  }

  # --- M-5: lambda_assume / scheduler_assume are account-scoped ----------
  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.lambda_assume.json).Statement :
      try(s.Condition.StringEquals["aws:SourceAccount"], null) == "123456789012"
    ])
    error_message = "M-5: lambda_assume's trust must carry a StringEquals aws:SourceAccount condition (bare service-principal trust is a confused-deputy risk)."
  }

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.scheduler_assume.json).Statement :
      try(s.Condition.StringEquals["aws:SourceAccount"], null) == "123456789012" &&
      try(contains(flatten([s.Condition.ArnLike["aws:SourceArn"]]), "arn:aws:scheduler:us-east-1:123456789012:schedule/*"), false)
    ])
    error_message = "M-5: scheduler_assume's trust must carry both a StringEquals aws:SourceAccount condition AND an ArnLike aws:SourceArn condition scoped to this account's schedules."
  }

  # --- M-9-tf: platform Lambda log groups exist with bounded retention ---
  assert {
    condition     = aws_cloudwatch_log_group.registrar.retention_in_days == 30
    error_message = "M-9-tf: registrar's log group must be declared with a bounded (default 30 d) retention -- undeclared, it auto-creates with NEVER EXPIRE while logging delivery keys/filenames/error strings."
  }
  assert {
    condition     = aws_cloudwatch_log_group.absence.retention_in_days == 30
    error_message = "M-9-tf: absence's log group must be declared with a bounded (default 30 d) retention."
  }
  assert {
    condition     = aws_cloudwatch_log_group.maintenance.retention_in_days == 30
    error_message = "M-9-tf: maintenance's log group must be declared with a bounded (default 30 d) retention."
  }
  assert {
    condition     = aws_cloudwatch_log_group.registrar.name == "/aws/lambda/conveyer-test-registrar"
    error_message = "M-9-tf: registrar's log group must be named /aws/lambda/<function-name> so Lambda adopts it instead of auto-creating its own."
  }

  # --- M-6 completion: DLQ resource policy admits EventBridge -------------
  #
  # Without this, the dead_letter_config added to the s3-push EventBridge
  # target (modules/feed s3_push.tf) is inert: EventBridge Rule targets
  # deliver dead-letters via the EventBridge SERVICE principal, which
  # requires a queue resource policy (distinct from the IAM-identity-based
  # DlqSend grants used by the Lambda functions' own DLQs).
  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.dlq_policy.json).Statement :
      contains(flatten([try(s.Principal.Service, [])]), "events.amazonaws.com") &&
      contains(flatten([s.Action]), "sqs:SendMessage") &&
      try(s.Condition.StringEquals["aws:SourceAccount"], null) == "123456789012"
    ])
    error_message = "M-6 completion: the DLQ resource policy must grant events.amazonaws.com sqs:SendMessage, scoped to this account, or EventBridge rule targets' dead_letter_config is silently inert."
  }
}
