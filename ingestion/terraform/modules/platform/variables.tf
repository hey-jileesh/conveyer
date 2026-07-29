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
  description = "AWS region to deploy into; also written verbatim into CONVEYER_AWS_REGION (S7.2)."
  type        = string
}

variable "image_uri" {
  description = "Container image URI (\"<ecr>:<tag>\") shared by all four Lambda functions (S7.1)."
  type        = string
}

variable "alert_email" {
  description = "Email address subscribed to the alarm SNS topic (S11.3); empty disables SNS entirely."
  type        = string
  default     = ""
}

variable "feeds_json" {
  description = <<-EOT
    `jsonencode({registry_version = 1, feeds = [FeedConfig, ...]})` -- the
    rendered feed registry (S6.8). Drives the landing bucket's per-s3-push-feed
    lifecycle/policy statements and is published verbatim to
    `s3://$${p}-artifacts/registry/feeds.json`.
  EOT
  type        = string
}

variable "landing_glacier_days" {
  description = "Days after which canonical landing objects transition to GLACIER_IR (S10.2); never expire."
  type        = number
  default     = 90
}

variable "log_retention_days" {
  description = <<-EOT
    CloudWatch Logs retention for the platform Lambda functions' log groups
    (M-9-tf security fix): left undeclared, a function's log group
    auto-creates with NEVER EXPIRE retention on first invocation, and these
    functions log delivery keys, filenames, ClaimItem reprs, and error
    strings indefinitely. Declaring the log group ourselves with an explicit
    retention closes that gap.
  EOT
  type        = number
  default     = 30
}

variable "extra_artifacts_policy_statements_json" {
  description = <<-EOT
    LLD 004.1 S10.1/I-23: additional `aws_iam_policy_document` JSON
    documents to merge (via `source_policy_documents`) into this module's
    SOLE `aws_s3_bucket_policy.artifacts` resource -- a bucket policy is one
    document per resource, so a second module (e.g. `spine-platform`, whose
    `artifacts_spine_policy_document_json` output is exactly this shape)
    must never create its own `aws_s3_bucket_policy` against the same
    bucket (see this module's own house-style note in the platform
    module's header comments / the terraform-house-style memory). Default
    empty list -- a standalone apply of this module is unaffected.
  EOT
  type        = list(string)
  default     = []
}

variable "spine_run_ledger_identifier" {
  description = <<-EOT
    LLD 004.1 S12.6(3)/I-17 [E-7]: the spine run ledger's Glue-catalog
    identifier (`"<spine glue db>.run_ledger"`, e.g.
    `module.spine_platform.glue_database_name` + `.run_ledger` composed by
    the env root) -- added to `CONVEYER_MAINTENANCE_TABLES` so ONE
    maintenance Lambda sweeps both ledgers. Default "" (empty) omits it
    entirely from the table list, so a standalone apply of this module
    (no spine wired yet, e.g. this module's own tests) is unaffected.
  EOT
  type        = string
  default     = ""
}
