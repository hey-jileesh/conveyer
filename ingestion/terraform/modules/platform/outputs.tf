# outputs.tf -- the platform contract per LLD S10.6's enumeration, exposed
# as FLAT top-level outputs (one per field), NOT a single wrapped object.
#
# This shape is load-bearing, not a style choice: the root env module
# (`terraform/envs/dev/main.tf`, sibling bead) wires `module.feed`'s
# `platform` input as `platform = module.platform` -- a bare module
# reference, which Terraform evaluates as the object of ALL of a module's
# top-level outputs. `modules/feed/variables.tf`'s `variable "platform"`
# is typed as a flat `object({name_prefix, landing_bucket_name, ...,
# common_env, ...})`. Both were independently built from the same LLD S10.6
# prose and agree with each other on: flat (no nesting), and the env-var
# template field named `common_env` (not `env_vars`). Do not reintroduce a
# nesting level or rename a key below without updating both of those files
# in lockstep.
#
# Every key `modules/feed/variables.tf`'s `object({...})` type constraint
# requires is present below; a few extra convenience outputs are included
# too (athena_*, ecr_*, log_groups, dlq_url, ledger_s3_uri) -- Terraform's
# object type conversion silently drops attributes a stricter target type
# doesn't ask for, so these are harmless additions, not a compatibility risk.

output "name_prefix" {
  value = var.name_prefix
}

output "env" {
  value = var.env
}

output "region" {
  value = var.region
}

# --- buckets: names/ARNs ----------------------------------------------------

output "landing_bucket_name" {
  value = aws_s3_bucket.landing.id
}

output "landing_bucket_arn" {
  value = aws_s3_bucket.landing.arn
}

output "lake_bucket_name" {
  value = aws_s3_bucket.lake.id
}

output "lake_bucket_arn" {
  value = aws_s3_bucket.lake.arn
}

output "artifacts_bucket_name" {
  value = aws_s3_bucket.artifacts.id
}

output "artifacts_bucket_arn" {
  value = aws_s3_bucket.artifacts.arn
}

# --- event bus ---------------------------------------------------------------

output "event_bus_name" {
  value = aws_cloudwatch_event_bus.ingestion.name
}

output "event_bus_arn" {
  value = aws_cloudwatch_event_bus.ingestion.arn
}

# --- CAS table -----------------------------------------------------------

output "cas_table_name" {
  value = aws_dynamodb_table.cas.name
}

output "cas_table_arn" {
  value = aws_dynamodb_table.cas.arn
}

# --- Glue db + ledger identifiers (table itself is bootstrap-owned, D-7) ---

output "glue_catalog_arn" {
  value = local.glue_catalog_arn
}

output "glue_database_name" {
  value = local.glue_database
}

output "glue_database_arn" {
  value = local.glue_database_arn
}

output "ledger_table_name" {
  value = local.ledger_table
}

output "ledger_table_arn" {
  value = local.glue_table_arn
}

output "ledger_s3_uri" {
  value = "s3://${aws_s3_bucket.lake.id}/ledger/"
}

# --- Athena ------------------------------------------------------------------

output "athena_workgroup_name" {
  value = aws_athena_workgroup.ingestion.name
}

output "athena_workgroup_arn" {
  value = aws_athena_workgroup.ingestion.arn
}

output "athena_output_uri" {
  value = "s3://${aws_s3_bucket.artifacts.id}/athena-results/"
}

# --- DLQ -----------------------------------------------------------------

output "dlq_arn" {
  value = aws_sqs_queue.dlq.arn
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.id
}

# --- registrar function ---------------------------------------------------

output "registrar_function_name" {
  value = aws_lambda_function.registrar.function_name
}

output "registrar_function_arn" {
  value = aws_lambda_function.registrar.arn
}

# --- scheduler role -- every per-feed pull schedule (S10.7) reuses this ---

output "scheduler_role_arn" {
  value = aws_iam_role.scheduler.arn
}

# --- ECR (shared image; feed module's driver functions build from the same
# var.image_uri passed at the root, but the repo itself lives here) --------

output "ecr_repository_url" {
  value = aws_ecr_repository.ingestion.repository_url
}

output "ecr_repository_arn" {
  value = aws_ecr_repository.ingestion.arn
}

# --- log groups ------------------------------------------------------------

output "log_groups" {
  value = {
    events_name = aws_cloudwatch_log_group.events.name
    events_arn  = aws_cloudwatch_log_group.events.arn
  }
}

# --- CONVEYER_* env-var template shared by every function (S7.2) ----------
# The feed module merges in CONVEYER_FEED_ID (+ any driver-specific env
# vars, e.g. CONVEYER_SFTP_LOOKBACK_DAYS/CONVEYER_DRIVER_BYTES_BUDGET) per
# function.

output "common_env" {
  value = local.base_env
}
