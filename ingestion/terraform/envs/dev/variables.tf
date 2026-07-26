# LLD S10.1: "Root variables: name_prefix (default conveyer), env, region,
# image_uri, alert_email (default ""), landing_glacier_days (default 90),
# driver_bytes_budget (default 5368709120)."

variable "name_prefix" {
  description = "First half of \"$${p}\" throughout the LLD (S5): all physical resource names derive from \"$${name_prefix}-$${env}\"."
  type        = string
  default     = "conveyer"
}

variable "env" {
  description = "Second half of \"$${p}\" (S5), e.g. \"dev\"."
  type        = string
}

variable "region" {
  description = "AWS region for every resource in this env."
  type        = string
}

variable "image_uri" {
  description = "Container image URI (\"<ecr>:<tag>\") shared by every Lambda function (D-2). See the deployment runbook, S10.8: the ECR repo must exist and be pushed to before this can point at a real tag."
  type        = string
}

variable "alert_email" {
  description = "Optional email for the platform's SNS alarm topic (S11.3). Empty disables the SNS subscription."
  type        = string
  default     = ""
}

variable "landing_glacier_days" {
  description = "Days before canonical landing data transitions to GLACIER_IR (S10.2). Canonical data is never expired, only transitioned -- verbatim-forever (arch S12)."
  type        = number
  default     = 90
}

variable "driver_bytes_budget" {
  description = "Default per-run acquisition byte budget for sftp-pull drivers, in bytes (S9.2 step 5; default 5 GiB). Wired to each sftp-pull driver's CONVEYER_DRIVER_BYTES_BUDGET env var."
  type        = number
  default     = 5368709120
}
