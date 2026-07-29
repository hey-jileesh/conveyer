# Invariant tests -- mirrors
# ingestion/terraform/modules/platform/tests/security_fixes.tftest.hcl's
# mechanics exactly (see that file's header comment for the full rationale;
# restated briefly below).
#
# Mechanics: `data.aws_iam_policy_document.*.json` is only known
# post-`apply` (a data source depending on a not-yet-created resource's ARN
# is unknown during `plan`). We therefore use a REAL (non-mock)
# `provider "aws"` (aws_iam_policy_document makes no network call) with
# `skip_credentials_validation` etc., `override_data` for the one genuine
# API call (`data.aws_caller_identity.current`), and `override_resource` for
# every OTHER resource so `apply` never touches real AWS. `mock_provider`
# was NOT used -- it substitutes zero-valued/garbage data-source output,
# which breaks the IAM-JSON-shape assertions below (confirmed against the
# ingestion module's own test file rationale).

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
}

variables {
  name_prefix                    = "conveyer"
  env                            = "test"
  region                         = "us-east-1"
  event_bus_name                 = "conveyer-test-bus"
  event_bus_arn                  = "arn:aws:events:us-east-1:123456789012:event-bus/conveyer-test-bus"
  artifacts_bucket_name          = "conveyer-test-artifacts"
  artifacts_bucket_arn           = "arn:aws:s3:::conveyer-test-artifacts"
  artifacts_deploy_principal_arn = "arn:aws:iam::123456789012:role/conveyer-test-deploy"
  athena_workgroup_name          = "conveyer-test-ingestion"
  router_zip_path                = "./tests/fixtures/router.zip"
  ingestion_producer_role_arns = [
    "arn:aws:iam::123456789012:role/conveyer-test-registrar",
  ]
}

run "spine_platform" {
  command = apply

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:root"
      user_id    = "AIDAEXAMPLE"
    }
  }

  override_resource { target = aws_glue_catalog_database.spine }

  override_resource {
    target = aws_cloudwatch_log_group.router
    values = {
      name = "/aws/lambda/conveyer-test-spine-router"
      arn  = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/conveyer-test-spine-router"
    }
  }

  override_resource {
    target = aws_lambda_function.router
    values = {
      arn           = "arn:aws:lambda:us-east-1:123456789012:function:conveyer-test-spine-router"
      function_name = "conveyer-test-spine-router"
    }
  }

  override_resource { target = aws_lambda_permission.eventbridge_invoke_router }

  override_resource {
    target = aws_cloudwatch_event_rule.delivery_registered
    values = {
      name = "conveyer-test-spine-delivery-registered"
      arn  = "arn:aws:events:us-east-1:123456789012:rule/conveyer-test-bus/conveyer-test-spine-delivery-registered"
    }
  }
  override_resource { target = aws_cloudwatch_event_target.router }
  override_resource { target = aws_cloudwatch_event_bus_policy.spine }

  override_resource {
    target = aws_sqs_queue.spine_dlq
    values = {
      id   = "https://sqs.us-east-1.amazonaws.com/123456789012/conveyer-test-spine-dlq"
      name = "conveyer-test-spine-dlq"
      arn  = "arn:aws:sqs:us-east-1:123456789012:conveyer-test-spine-dlq"
    }
  }
  override_resource { target = aws_sqs_queue_policy.spine_dlq }

  override_resource {
    target = aws_iam_role.spine_router
    values = {
      name = "conveyer-test-spine-router"
      arn  = "arn:aws:iam::123456789012:role/conveyer-test-spine-router"
    }
  }
  override_resource { target = aws_iam_role_policy_attachment.spine_router_basic }
  override_resource { target = aws_iam_role_policy.spine_router }

  override_resource {
    target = aws_iam_role.spine_sfn
    values = {
      name = "conveyer-test-spine-sfn"
      arn  = "arn:aws:iam::123456789012:role/conveyer-test-spine-sfn"
    }
  }
  override_resource { target = aws_iam_role_policy.spine_sfn }

  override_resource { target = aws_s3_bucket_versioning.artifacts }

  override_resource { target = aws_athena_named_query.spine_run_status }
  override_resource { target = aws_athena_named_query.spine_attempts_per_batch }
  override_resource { target = aws_athena_named_query.spine_stage_durations_30d }
  override_resource { target = aws_athena_named_query.spine_rerun_noop_rate }

  override_resource { target = aws_cloudwatch_metric_alarm.router_errors }
  override_resource { target = aws_cloudwatch_metric_alarm.spine_dlq_messages_visible }
  override_resource { target = aws_cloudwatch_metric_alarm.run_ledger_loss }

  # --- naming (LLD S5) ---------------------------------------------------

  assert {
    condition     = aws_glue_catalog_database.spine.name == "conveyer_test_spine"
    error_message = "S5: spine Glue database must be named <name_prefix>_<env>_spine."
  }

  assert {
    condition     = aws_glue_catalog_database.spine.location_uri == "s3://conveyer-test-lake/spine/"
    error_message = "conveyer-nvh.47: the spine Glue database's location_uri must be pinned under s3://<p>-lake/spine/ -- a bootstrap-created run_ledger table with no location of its own must land inside the job-role/maintenance-Lambda grants' exact-prefix s3://<p>-lake/spine/run_ledger/*."
  }

  assert {
    condition     = local.spine_sfn_arn_prefix == "arn:aws:states:us-east-1:123456789012:stateMachine:conveyer-test-spine-"
    error_message = "S5/router.py: CONVEYER_SFN_ARN_PREFIX must be everything up to and including the trailing '-spine-'."
  }

  # --- router (S10.2, S8.1) -----------------------------------------------

  assert {
    condition     = aws_lambda_function.router.handler == "spine.entrypoints.router.handler"
    error_message = "the router zip preserves the spine/ package layout -- handler must be spine.entrypoints.router.handler, not a flattened path."
  }

  assert {
    condition     = aws_lambda_function.router.runtime == "python3.11"
    error_message = "S10.2: router Lambda runtime must be python3.11 (I-1 engine pin)."
  }

  assert {
    condition     = aws_lambda_function.router.memory_size == 512 && aws_lambda_function.router.timeout == 30
    error_message = "S10.2: router Lambda must be 512 MB / 30 s timeout."
  }

  assert {
    condition     = aws_lambda_function.router.reserved_concurrent_executions == 10
    error_message = "S10.2: router Lambda reserved concurrency must be 10."
  }

  assert {
    condition     = aws_lambda_function.router.environment[0].variables["CONVEYER_SFN_ARN_PREFIX"] == "arn:aws:states:us-east-1:123456789012:stateMachine:conveyer-test-spine-"
    error_message = "router.py reads CONVEYER_SFN_ARN_PREFIX verbatim -- env var must match the module's own naming-convention prefix exactly."
  }

  assert {
    condition     = aws_lambda_function.router.environment[0].variables["CONVEYER_ARGV_BUDGET_BYTES"] == "8192"
    error_message = "router.py reads CONVEYER_ARGV_BUDGET_BYTES -- must be set (default mirrors the code's own _DEFAULT_ARGV_BUDGET_BYTES)."
  }

  assert {
    condition     = aws_lambda_function.router.dead_letter_config[0].target_arn == aws_sqs_queue.spine_dlq.arn
    error_message = "router Lambda must be wired to the spine DLQ."
  }

  assert {
    condition     = aws_cloudwatch_log_group.router.retention_in_days == 30
    error_message = "[S-18]: the router's explicitly-created log group must have bounded (default 30 d) retention."
  }

  # --- EventBridge rule / bus policy (I-22) -------------------------------

  assert {
    condition     = jsondecode(aws_cloudwatch_event_rule.delivery_registered.event_pattern).source[0] == "conveyer.ingestion"
    error_message = "the delivery-registered rule must match source = conveyer.ingestion."
  }

  assert {
    condition     = jsondecode(aws_cloudwatch_event_rule.delivery_registered.event_pattern)["detail-type"][0] == "delivery-registered"
    error_message = "the delivery-registered rule must match detail-type = delivery-registered."
  }

  assert {
    condition     = aws_cloudwatch_event_target.router.retry_policy[0].maximum_retry_attempts == 2
    error_message = "S10.2: the router target must retry 2 times before DLQ."
  }

  assert {
    condition     = aws_cloudwatch_event_target.router.dead_letter_config[0].arn == aws_sqs_queue.spine_dlq.arn
    error_message = "the router target's dead_letter_config must point at the spine DLQ."
  }

  # I-22: producer statement present, conditioned on events:source =
  # conveyer.ingestion, restricted to the supplied producer role ARNs.
  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.bus_policy.json).Statement :
      contains(flatten([try(s.Principal.AWS, [])]), "arn:aws:iam::123456789012:role/conveyer-test-registrar") &&
      contains(flatten([s.Action]), "events:PutEvents") &&
      try(s.Condition.StringEquals["events:source"], null) == "conveyer.ingestion"
    ])
    error_message = "I-22: the bus policy must grant events:PutEvents to the ingestion producer roles conditioned events:source = conveyer.ingestion."
  }

  # I-22: with var.spine_job_role_arns left at its [] default, the ALLOW
  # conveyer.spine statement must be OMITTED entirely (not an
  # empty-principal statement, which IAM rejects). The DENY conveyer.spine
  # statement (F2, asserted below) is a different statement and must
  # remain present even in this case.
  assert {
    condition = alltrue([
      for s in jsondecode(data.aws_iam_policy_document.bus_policy.json).Statement :
      s.Effect == "Allow" ? try(s.Condition.StringEquals["events:source"], null) != "conveyer.spine" : true
    ])
    error_message = "I-22: with no spine job roles supplied, the bus policy must omit the ALLOW conveyer.spine statement entirely, not emit one with zero principals."
  }

  # F2: Deny events:source = conveyer.ingestion for any principal not in
  # the declared ingestion producer role list -- closes the
  # identity-policy-union gap (an Allow-only resource policy blocks
  # nothing for same-account principals).
  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.bus_policy.json).Statement :
      s.Effect == "Deny" &&
      contains(flatten([s.Action]), "events:PutEvents") &&
      try(s.Condition.StringEquals["events:source"], null) == "conveyer.ingestion" &&
      contains(flatten([try(s.Condition.StringNotLike["aws:PrincipalArn"], [])]), "arn:aws:iam::123456789012:role/conveyer-test-registrar")
    ])
    error_message = "F2: the bus policy must deny events:source = conveyer.ingestion for any principal not in the declared ingestion producer role list."
  }

  # F2: Deny events:source = conveyer.spine for any principal -- with
  # var.spine_job_role_arns at its [] default, no real spine job role ARN
  # exists yet, so this Deny must hold unconditionally (the sentinel
  # fallback denies every principal, never the empty-values statement IAM
  # would reject).
  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.bus_policy.json).Statement :
      s.Effect == "Deny" &&
      contains(flatten([s.Action]), "events:PutEvents") &&
      try(s.Condition.StringEquals["events:source"], null) == "conveyer.spine" &&
      length(flatten([try(s.Condition.StringNotLike["aws:PrincipalArn"], [])])) > 0
    ])
    error_message = "F2: the bus policy must deny events:source = conveyer.spine for any principal, even with no spine job roles supplied."
  }

  # F2: Deny any events:source outside the two I-22 producer sources
  # entirely (custom bus -- see events.tf header for why this is safe).
  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.bus_policy.json).Statement :
      s.Effect == "Deny" &&
      contains(flatten([s.Action]), "events:PutEvents") &&
      try(toset(s.Condition.StringNotEquals["events:source"]), null) == toset(["conveyer.ingestion", "conveyer.spine"])
    ])
    error_message = "F2: the bus policy must deny events:PutEvents for any events:source other than conveyer.ingestion / conveyer.spine."
  }

  # --- spine DLQ (S10.2, [S-14]) -------------------------------------------

  assert {
    condition     = aws_sqs_queue.spine_dlq.message_retention_seconds == 1209600
    error_message = "S10.2: spine DLQ retention must be 14 days."
  }

  assert {
    condition     = aws_sqs_queue.spine_dlq.kms_master_key_id == "alias/aws/sqs"
    error_message = "S10.2: spine DLQ must be KMS-SSE."
  }

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.spine_dlq_policy.json).Statement :
      s.Effect == "Deny" &&
      try(s.Condition.Bool["aws:SecureTransport"], null) == "false"
    ])
    error_message = "[S-14]: the spine DLQ queue policy must deny non-TLS transport."
  }

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.spine_dlq_policy.json).Statement :
      contains(flatten([try(s.Principal.Service, [])]), "events.amazonaws.com") &&
      contains(flatten([s.Action]), "sqs:SendMessage") &&
      try(s.Condition.ArnEquals["aws:SourceArn"], null) == "arn:aws:events:us-east-1:123456789012:rule/conveyer-test-bus/conveyer-test-spine-delivery-registered"
    ])
    error_message = "the spine DLQ queue policy must grant events.amazonaws.com sqs:SendMessage conditioned on THIS rule's ARN."
  }

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.spine_dlq_policy.json).Statement :
      contains(flatten([try(s.Principal.AWS, [])]), "arn:aws:iam::123456789012:role/conveyer-test-spine-router") &&
      contains(flatten([s.Action]), "sqs:SendMessage")
    ])
    error_message = "the spine DLQ queue policy must grant the router role sqs:SendMessage."
  }

  # --- IAM trust policies [S-14] -------------------------------------------

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.spine_router_assume.json).Statement :
      try(s.Condition.StringEquals["aws:SourceAccount"], null) == "123456789012"
    ])
    error_message = "[S-14]: spine-router's trust must carry a StringEquals aws:SourceAccount condition."
  }

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.spine_sfn_assume.json).Statement :
      try(s.Condition.StringEquals["aws:SourceAccount"], null) == "123456789012" &&
      try(contains(flatten([s.Condition.ArnLike["aws:SourceArn"]]), "arn:aws:states:us-east-1:123456789012:stateMachine:conveyer-test-spine-*"), false)
    ])
    error_message = "[S-14]: spine-sfn's trust must carry both aws:SourceAccount AND an ArnLike aws:SourceArn scoped to this account's conveyer-test-spine-* state machines."
  }

  # --- spine-router grants --------------------------------------------------

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.spine_router.json).Statement :
      contains(flatten([s.Action]), "states:StartExecution") &&
      contains(flatten([s.Resource]), "arn:aws:states:us-east-1:123456789012:stateMachine:conveyer-test-spine-*")
    ])
    error_message = "spine-router must hold states:StartExecution on conveyer-test-spine-* state machines only."
  }

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.spine_router.json).Statement :
      contains(flatten([s.Action]), "sqs:SendMessage") &&
      contains(flatten([s.Resource]), aws_sqs_queue.spine_dlq.arn)
    ])
    error_message = "spine-router must hold sqs:SendMessage on the spine DLQ."
  }

  # --- spine-sfn: EXACTLY the four actions, no events:* [S-17] -------------

  assert {
    condition = alltrue([
      for s in jsondecode(data.aws_iam_policy_document.spine_sfn.json).Statement :
      toset(flatten([s.Action])) == toset(["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"])
    ])
    error_message = "[S-17]: spine-sfn must hold EXACTLY glue:StartJobRun/GetJobRun/GetJobRuns/BatchStopJobRun -- no more, no fewer, and never events:*."
  }

  assert {
    condition = alltrue([
      for s in jsondecode(data.aws_iam_policy_document.spine_sfn.json).Statement :
      alltrue([for a in flatten([s.Action]) : !startswith(a, "events:")])
    ])
    error_message = "[S-17]: spine-sfn must never hold any events:* action."
  }

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.spine_sfn.json).Statement :
      contains(flatten([s.Resource]), "arn:aws:glue:us-east-1:123456789012:job/conveyer-test-spine-*")
    ])
    error_message = "spine-sfn must be scoped to job/conveyer-test-spine-* only."
  }

  # --- S10.3 append-only posture: neither platform role holds any delete --

  assert {
    condition = alltrue([
      for s in concat(
        jsondecode(data.aws_iam_policy_document.spine_router.json).Statement,
        jsondecode(data.aws_iam_policy_document.spine_sfn.json).Statement,
      ) :
      alltrue([for a in flatten([s.Action]) : !strcontains(lower(a), "delete")])
    ])
    error_message = "S10.3: no spine platform role may hold any delete permission."
  }

  # --- I-23 artifacts protection (exposed as a document, not a resource) --

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.artifacts_spine_protection.json).Statement :
      s.Effect == "Deny" &&
      contains(flatten([s.Action]), "s3:PutObject") &&
      contains(flatten([s.Action]), "s3:DeleteObject") &&
      contains(flatten([s.Resource]), "arn:aws:s3:::conveyer-test-artifacts/spine/*") &&
      try(contains(flatten([s.Condition.StringNotLike["aws:PrincipalArn"]]), "arn:aws:iam::123456789012:role/conveyer-test-deploy"), false)
    ])
    error_message = "I-23: the artifacts spine/* protection document must deny Put/Delete except the deploy principal."
  }

  assert {
    condition     = aws_s3_bucket_versioning.artifacts.versioning_configuration[0].status == "Enabled"
    error_message = "I-23: the artifacts bucket must have versioning enabled."
  }

  # --- alarms (S11.4, verbatim thresholds) ---------------------------------

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.router_errors.threshold == 0 &&
      aws_cloudwatch_metric_alarm.router_errors.period == 300 &&
      aws_cloudwatch_metric_alarm.router_errors.comparison_operator == "GreaterThanThreshold"
    )
    error_message = "S11.4: router errors alarm must be > 0 over 5 m."
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.spine_dlq_messages_visible.threshold == 0 &&
      aws_cloudwatch_metric_alarm.spine_dlq_messages_visible.period == 300 &&
      aws_cloudwatch_metric_alarm.spine_dlq_messages_visible.comparison_operator == "GreaterThanThreshold"
    )
    error_message = "S11.4: DLQ-visible alarm must be > 0 over 5 m."
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.run_ledger_loss.threshold == 1 &&
      aws_cloudwatch_metric_alarm.run_ledger_loss.comparison_operator == "GreaterThanOrEqualToThreshold" &&
      anytrue([for mq in aws_cloudwatch_metric_alarm.run_ledger_loss.metric_query : mq.period == 3600])
    )
    error_message = "S11.4: RunLedgerLoss alarm must be >= 1 over 1 h."
  }

  # --- Athena named queries (S11.5) ----------------------------------------

  assert {
    condition     = aws_athena_named_query.spine_run_status.workgroup == "conveyer-test-ingestion"
    error_message = "S10.2: spine named queries must run in the EXISTING ingestion workgroup, not a new one."
  }

  assert {
    condition = alltrue([
      for q in [
        aws_athena_named_query.spine_run_status,
        aws_athena_named_query.spine_attempts_per_batch,
        aws_athena_named_query.spine_stage_durations_30d,
        aws_athena_named_query.spine_rerun_noop_rate,
      ] : q.database == "conveyer_test_spine"
    ])
    error_message = "S11.5: all four named queries must target the spine Glue database."
  }
}
