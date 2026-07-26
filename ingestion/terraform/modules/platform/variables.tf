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
