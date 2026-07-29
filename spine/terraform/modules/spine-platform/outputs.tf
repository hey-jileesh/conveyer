# outputs.tf -- flat outputs (house style, mirrors
# ingestion/terraform/modules/platform/outputs.tf), for the sibling
# `modules/spine-pipeline` and the env root.

output "name_prefix" {
  value = var.name_prefix
}

output "env" {
  value = var.env
}

output "region" {
  value = var.region
}

# --- Glue ------------------------------------------------------------------

output "glue_database_name" {
  value = local.spine_glue_database
}

output "run_ledger_table_name" {
  value = "run_ledger"
}

output "run_ledger_s3_uri" {
  value = "s3://${local.p}-lake/spine/run_ledger/"
}

# --- naming-convention ARN patterns -- `modules/spine-pipeline` composes
# its own state-machine/Glue-job resources against these SAME prefixes; ---
# exposed so that module's role-trust conditions don't have to re-derive
# `${p}` independently.

output "spine_state_machine_arn_pattern" {
  value = local.spine_state_machine_arn_pattern
}

output "spine_glue_job_arn_pattern" {
  value = local.spine_glue_job_arn_pattern
}

output "spine_sfn_arn_prefix" {
  value = local.spine_sfn_arn_prefix
}

# --- router ------------------------------------------------------------

output "router_function_name" {
  value = aws_lambda_function.router.function_name
}

output "router_function_arn" {
  value = aws_lambda_function.router.arn
}

output "router_log_group_name" {
  value = aws_cloudwatch_log_group.router.name
}

output "router_log_group_arn" {
  value = aws_cloudwatch_log_group.router.arn
}

# --- IAM ---------------------------------------------------------------

output "spine_router_role_name" {
  value = aws_iam_role.spine_router.name
}

output "spine_router_role_arn" {
  value = aws_iam_role.spine_router.arn
}

output "spine_sfn_role_name" {
  value = aws_iam_role.spine_sfn.name
}

output "spine_sfn_role_arn" {
  value = aws_iam_role.spine_sfn.arn
}

# --- DLQ -----------------------------------------------------------------

output "spine_dlq_arn" {
  value = aws_sqs_queue.spine_dlq.arn
}

output "spine_dlq_url" {
  value = aws_sqs_queue.spine_dlq.id
}

output "spine_dlq_name" {
  value = aws_sqs_queue.spine_dlq.name
}

# --- EventBridge -----------------------------------------------------------

output "delivery_registered_rule_arn" {
  value = aws_cloudwatch_event_rule.delivery_registered.arn
}

output "delivery_registered_rule_name" {
  value = aws_cloudwatch_event_rule.delivery_registered.name
}

# --- artifacts (pass-through + the I-23 policy document merge input;
# see main.tf/s3.tf for why no aws_s3_bucket_policy resource exists here) --

output "artifacts_spine_policy_document_json" {
  description = "Deny-PutObject/DeleteObject*-under-spine/*-except-deploy-principal statement (I-23), for the env root to merge with ingestion's own artifacts bucket policy document into ONE aws_s3_bucket_policy."
  value       = data.aws_iam_policy_document.artifacts_spine_protection.json
}

# --- Athena named query ids (convenience; the queries themselves live in
# the workgroup passed in via var.athena_workgroup_name) -------------------

output "athena_named_query_ids" {
  value = {
    spine_run_status          = aws_athena_named_query.spine_run_status.id
    spine_attempts_per_batch  = aws_athena_named_query.spine_attempts_per_batch.id
    spine_stage_durations_30d = aws_athena_named_query.spine_stage_durations_30d.id
    spine_rerun_noop_rate     = aws_athena_named_query.spine_rerun_noop_rate.id
  }
}
