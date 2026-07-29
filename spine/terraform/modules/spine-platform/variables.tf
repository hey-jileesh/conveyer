variable "name_prefix" {
  description = "Physical-name prefix for every resource (LLD S5); root default is \"conveyer\"."
  type        = string
  default     = "conveyer"
}

variable "env" {
  description = "Environment name, e.g. \"dev\" (LLD S5: `$${p} = $${name_prefix}-$${env}`)."
  type        = string
}

variable "region" {
  description = "AWS region to deploy into."
  type        = string
}

# --- consumed from the ingestion platform module's flat outputs -----------

variable "event_bus_name" {
  description = "The ingestion platform's shared event bus name (`module.platform.event_bus_name`) -- the router listens on it, and this module's I-22 bus resource policy is applied to it."
  type        = string
}

variable "event_bus_arn" {
  description = "The ingestion platform's shared event bus ARN (`module.platform.event_bus_arn`)."
  type        = string
}

variable "artifacts_bucket_name" {
  description = "The ingestion platform's artifacts bucket name (`module.platform.artifacts_bucket_name`) -- versioning is enabled here (I-23); the bucket POLICY deny is exposed as an output only (see main.tf header for why)."
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "The ingestion platform's artifacts bucket ARN (`module.platform.artifacts_bucket_arn`)."
  type        = string
}

variable "athena_workgroup_name" {
  description = "The ingestion platform's existing Athena workgroup name (`module.platform.athena_workgroup_name`) -- the spine named queries (S11.5) run in this SAME workgroup, not a new one."
  type        = string
}

# --- IAM inputs the env root must supply (cross-account-boundary lists) ---

variable "ingestion_producer_role_arns" {
  description = <<-EOT
    Role ARNs allowed `events:PutEvents` on the bus conditioned
    `events:source = conveyer.ingestion` (I-22): the ingestion registrar +
    per-feed driver roles. Required -- ingestion's own producers must keep
    working once this module's bus resource policy is applied.
  EOT
  type        = list(string)
}

variable "spine_job_role_arns" {
  description = <<-EOT
    Per-pipeline `spine-job-<slug>` role ARNs (created by the sibling
    `modules/spine-pipeline`, I-21) allowed `events:PutEvents` on the bus
    conditioned `events:source = conveyer.spine` (I-22). Defaults empty --
    Phase 1 wires this once the identity exemplar's job role exists; an
    empty list omits the statement entirely rather than emitting an
    empty-principal IAM statement.
  EOT
  type        = list(string)
  default     = []
}

variable "artifacts_deploy_principal_arn" {
  description = "The single principal (deploy role/user) excepted from the spine/* PutObject/DeleteObject* deny (I-23)."
  type        = string
}

# --- router packaging --------------------------------------------------

variable "router_zip_path" {
  description = "Path to the zip built by `make -C spine package-router` (spine/dist/router.zip) -- stdlib + boto3 only (I-8)."
  type        = string
}

variable "argv_budget_bytes" {
  description = <<-EOT
    CONVEYER_ARGV_BUDGET_BYTES -- bounds the serialized, allowlisted detail
    forwarded into Glue's `--conveyer-delivery` argument (S8.2 [T-5]).
    Default mirrors the router's own hardcoded fallback
    (`_DEFAULT_ARGV_BUDGET_BYTES` in router.py) so the deployed value is
    visible in Terraform rather than only in code; override once M6 pins
    the real Glue argv ceiling.
  EOT
  type        = number
  default     = 8192
}

# --- alerting / log retention (ingestion 002.1 S11.3 pattern) --------------

variable "alert_email" {
  description = "Email address subscribed to the spine alarm SNS topic; empty disables SNS entirely."
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for the router Lambda's explicitly-created log group [S-18]."
  type        = number
  default     = 30
}
