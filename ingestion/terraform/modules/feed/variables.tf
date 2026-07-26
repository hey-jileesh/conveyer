variable "feed" {
  description = <<-EOT
    One decoded `source.yaml` (`FeedConfig`-shaped, LLD S6.1), passed
    verbatim from the env root's `yamldecode` over `sources/**/source.yaml`
    (D-12). Left untyped (`any`) deliberately: shape varies by
    `driver`/`completeness.mode` (e.g. `trigger` and `connection` differ
    between s3-push and sftp-pull; `completeness.manifest_pattern` may be
    absent outside manifest mode), and Terraform is only expected to check
    YAML well-formedness here -- the authoritative `FeedConfig` validation
    gate is `make registry` / CI (LLD S6.1, S12.6).
  EOT
  type        = any
}

variable "platform" {
  description = <<-EOT
    Platform module output object (LLD S10.6: "Outputs (consumed by
    modules/feed as one object): bucket names/ARNs, bus name/ARN, CAS table
    name/ARN, Glue db, ledger identifiers, DLQ ARN, registrar function
    name/ARN, scheduler role ARN, log-group names, env-var map template.").

    Field names confirmed against the landed modules/platform/outputs.tf
    (joint-gate seam, both beads built independently against the same
    S10.6 prose): flat top-level outputs (module.platform is directly the
    contract object -- no wrapper key), `common_env` for the env-var
    template. Extra attributes modules/platform exposes beyond what's
    declared here (ledger_s3_uri, athena_*, dlq_url, ecr_*, log_groups,
    ...) are dropped harmlessly by Terraform's object-type conversion;
    only what this module actually consumes is declared.

    `common_env` must carry every `CONVEYER_*` key `ingestion/config.py`'s
    `from_env()` requires (env, aws_region, landing_bucket, lake_bucket,
    artifacts_bucket, glue_database, ledger_table, cas_table, event_bus,
    registry_uri, athena_workgroup, athena_output_uri) -- ALL of them,
    including the athena_* pair the driver never reads, because
    `from_env()` fails loudly on any missing required var regardless of
    which function is cold-starting. This module adds only
    `CONVEYER_FEED_ID` (and, for sftp-pull, `CONVEYER_DRIVER_BYTES_BUDGET`)
    on top.
  EOT
  type = object({
    name_prefix = string
    env         = string
    region      = string

    landing_bucket_name   = string
    landing_bucket_arn    = string
    lake_bucket_name      = string
    lake_bucket_arn       = string
    artifacts_bucket_name = string
    artifacts_bucket_arn  = string

    event_bus_name = string
    event_bus_arn  = string

    cas_table_name = string
    cas_table_arn  = string

    glue_catalog_arn   = string
    glue_database_name = string
    glue_database_arn  = string
    ledger_table_name  = string
    ledger_table_arn   = string

    dlq_arn = string

    registrar_function_name = string
    registrar_function_arn  = string

    scheduler_role_arn = string

    # CONVEYER_* env-var template shared by every function (LLD S7.2);
    # this module merges in CONVEYER_FEED_ID (+ CONVEYER_DRIVER_BYTES_BUDGET
    # for sftp-pull drivers) on top.
    common_env = map(string)
  })
}

variable "image_uri" {
  description = "Container image URI (\"<ecr>:<tag>\") shared by every Lambda function (D-2, LLD S10.8)."
  type        = string
}

variable "driver_bytes_budget" {
  description = <<-EOT
    Default per-run acquisition byte budget for sftp-pull drivers (LLD
    S9.2 step 5). Wired to the `CONVEYER_DRIVER_BYTES_BUDGET` env var that
    `drivers/sftp_pull.py::_budget_bytes()` reads directly (NOT a
    `RuntimeConfig`/`config.py` field -- see ingestion agent-memory
    m4-sftp-pull-design-notes.md). Ignored for s3-push feeds.
  EOT
  type        = number
}

variable "log_retention_days" {
  description = <<-EOT
    CloudWatch Logs retention for each sftp-pull driver function's log group
    (M-9-tf security fix): left undeclared, a function's log group
    auto-creates with NEVER EXPIRE retention on first invocation, and these
    functions log delivery keys, filenames, ClaimItem reprs, and error
    strings indefinitely. Declared independently of `modules/platform`'s own
    `log_retention_days` (not plumbed through `var.platform`, to avoid
    widening that module's load-bearing output-object contract for a
    same-default convenience value); operators wanting a non-default value
    must set both.
  EOT
  type        = number
  default     = 30
}
