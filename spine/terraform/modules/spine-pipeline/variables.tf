# variables.tf -- LLD 004.1 S10.4. Grouped: pipeline identity, shared
# platform inputs (ingestion's bus/buckets + the sibling spine-platform
# module's outputs), artifact supply chain (I-23), runner tuning (S6.4,
# T-7), IAM grant-generation inputs (I-21/S-15), alerting/log retention.

# --- pipeline identity -----------------------------------------------------

variable "pipeline" {
  description = <<-EOT
    The pipeline identifier exactly as it appears in `DeliveryRegisteredV1.
    pipeline` (S6.1) and `PipelineSpecModel.pipeline` (S6.2), e.g.
    "pipelines/identity" (S10.4's own exemplar value). `local.slug` derives
    `slug(pipeline)` (S5: replace "/" -> "--") for every resource name/ARN
    this module composes.
  EOT
  type        = string

  validation {
    # S5 grammar per segment: ^[a-z0-9]([a-z0-9]|-(?!-))*$ -- "--" forbidden
    # WITHIN a segment, making slug() injective. Terraform's regex engine
    # (RE2) has no negative-lookahead support, so `(?!-)` cannot be
    # transliterated literally; the equivalent lookahead-free form is
    # "one or more alnum runs joined by single dashes, never a double
    # dash" -- exactly what `[a-z0-9]+(-[a-z0-9]+)*` expresses.
    #
    # MUST be checked on each ORIGINAL "/"-split segment individually, not
    # on the replaced (`/` -> `--`) string as a single joined pattern: a
    # joined check cannot tell "two clean segments" apart from "one
    # segment containing a literal '--'" (e.g. "a/b" and "a/b--c" both
    # collapse to indistinguishable neighborhoods once '--' is already in
    # the string) -- exactly the ambiguity injectivity exists to rule out.
    # Caught by this module's own `terraform test` suite (`tests/
    # pipeline_slug_validation.tftest.hcl`): a first draft that validated
    # `replace(var.pipeline, "/", "--")` against a joined-segments pattern
    # SILENTLY ACCEPTED "pipelines/bad--name" (the replaced string
    # "pipelines--bad--name" parses as three clean segments, hiding the
    # illegal embedded "--"). Validating each split segment separately
    # closes that gap structurally.
    condition = alltrue([
      for segment in split("/", var.pipeline) :
      can(regex("^[a-z0-9]+(-[a-z0-9]+)*$", segment))
    ])
    error_message = "pipeline must be '/'-separated segments each matching ^[a-z0-9]+(-[a-z0-9]+)*$ -- lowercase alnum, single dashes only, no leading/trailing/double dash, no empty segment."
  }
}

# --- naming ------------------------------------------------------------

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

# --- ingestion platform's shared bus + buckets (module.platform outputs) --

variable "landing_bucket_name" {
  description = "The ingestion platform's landing bucket name (`module.platform.landing_bucket_name`) -- read-only for this pipeline's own routed feeds (I-21)."
  type        = string
}

variable "landing_bucket_arn" {
  description = "The ingestion platform's landing bucket ARN (`module.platform.landing_bucket_arn`)."
  type        = string
}

variable "lake_bucket_name" {
  description = "The ingestion platform's lake bucket name (`module.platform.lake_bucket_name`) -- holds both `tables/<slug>/*` (this pipeline's own data) and `spine/run_ledger/*`."
  type        = string
}

variable "lake_bucket_arn" {
  description = "The ingestion platform's lake bucket ARN (`module.platform.lake_bucket_arn`)."
  type        = string
}

variable "artifacts_bucket_name" {
  description = "The ingestion platform's artifacts bucket name (`module.platform.artifacts_bucket_name`) -- this pipeline's job role reads `spine/*` only (wheel + spec + entrypoint script, I-23)."
  type        = string
}

variable "artifacts_bucket_arn" {
  description = "The ingestion platform's artifacts bucket ARN (`module.platform.artifacts_bucket_arn`)."
  type        = string
}

variable "event_bus_name" {
  description = "The shared event bus NAME (`module.platform.event_bus_name`) -- wired to `--conveyer-event-bus`; `effects/events.py::_emit` puts `EventBusName` as a bus NAME (or ARN), and matches the `CONVEYER_EVENT_BUS` convention ingestion's own platform module already uses (bus name, not ARN)."
  type        = string
}

variable "event_bus_arn" {
  description = "The shared event bus ARN (`module.platform.event_bus_arn`) -- this pipeline's job role gets `events:PutEvents` on it, conditioned `events:source = conveyer.spine` (I-21/S-2)."
  type        = string
}

# --- sibling modules/spine-platform's outputs -----------------------------

variable "spine_database_name" {
  description = "The spine Glue database name (`module.spine_platform.glue_database_name`, `$${name_prefix}_$${env}_spine`) -- holds `run_ledger`; this pipeline's job role gets `GetDatabase` on it (never database-wide writes, I-21)."
  type        = string
}

variable "run_ledger_table_name" {
  description = "The run-ledger table name (`module.spine_platform.run_ledger_table_name`); default matches that module's own hardcoded output value."
  type        = string
  default     = "run_ledger"
}

variable "spine_sfn_role_arn" {
  description = "The shared `spine-sfn` role ARN (`module.spine_platform.spine_sfn_role_arn`) -- assumed by every pipeline's state machine (S10.3: platform-level, NOT per-pipeline, unlike the job role)."
  type        = string
}

# --- artifact supply chain (I-23) ------------------------------------------

variable "spine_wheel_uri" {
  description = "Content-addressed wheel key: `s3://$${p}-artifacts/spine/<git-sha>/conveyer_spine-....whl` (I-23). Wired to `--additional-python-modules`."
  type        = string

  validation {
    condition     = startswith(var.spine_wheel_uri, "s3://")
    error_message = "spine_wheel_uri must be an s3:// URI (I-23: content-addressed, immutable)."
  }
}

variable "glue_entrypoint_script_uri" {
  description = <<-EOT
    S3 location of the thin Glue driver script Glue's `command.
    script_location` executes (a hard Glue API requirement, distinct from
    the wheel itself, which only makes the `spine` package importable via
    `--additional-python-modules`). Deploy-pushed, content-addressed
    alongside the wheel (ambiguity 2, main.tf header) -- expected content
    is a 3-line shim: `import sys; from spine.entrypoints.glue_main import
    main; main(sys.argv[1:])`.
  EOT
  type        = string

  validation {
    condition     = startswith(var.glue_entrypoint_script_uri, "s3://")
    error_message = "glue_entrypoint_script_uri must be an s3:// URI."
  }
}

variable "pipeline_spec_uri" {
  description = "`s3://$${p}-artifacts/spine/specs/<slug>/pipeline.yaml` (I-23 allowlist root). Wired to `--conveyer-pipeline-spec-uri`."
  type        = string

  validation {
    condition     = startswith(var.pipeline_spec_uri, "s3://")
    error_message = "pipeline_spec_uri must be an s3:// URI."
  }
}

# --- runner tuning (S6.4, T-7) ---------------------------------------------

variable "sla_minutes" {
  description = "Per-ATTEMPT budget (I-18): Glue job `Timeout`, and the entrypoint's [H-5] drift assert against `PipelineSpecModel.sla_minutes`. Also drives the SFN `TimeoutSeconds` arithmetic (T-2)."
  type        = number
  default     = 480

  validation {
    condition     = var.sla_minutes > 0
    error_message = "sla_minutes must be positive."
  }
}

variable "max_concurrent_runs" {
  description = "Glue `execution_property.max_concurrent_runs` (C-1/S-13): the AWS default of 1 would serialize-and-strand parallel same-feed batches, silently un-implementing D-12."
  type        = number
  default     = 4

  validation {
    condition     = var.max_concurrent_runs >= 1
    error_message = "max_concurrent_runs must be >= 1."
  }
}

variable "worker_type" {
  description = "Glue job worker type (T-7: the capacity axis lives in IaC, not RunConfig)."
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Glue job worker count (T-7)."
  type        = number
  default     = 2

  validation {
    condition     = var.number_of_workers >= 2
    error_message = "number_of_workers must be >= 2 (Glue's own minimum for standard/G.1X-class workers)."
  }
}

variable "run_config" {
  description = <<-EOT
    Framework-owned `RunConfig` JSON (S6.4) -- NOT a pipeline-spec field;
    wired to `--conveyer-run-config`. Default matches the model's own
    defaults: `shuffle_partitions=null` (AQE decides), `target_file_size_mb
    =512`, `repartition_before_write=true`.
  EOT
  type        = string
  # Variable defaults must be constant literals (no function calls) --
  # this is the literal `jsonencode({shuffle_partitions: null, target_
  # file_size_mb: 512, repartition_before_write: true})` output, kept in
  # sync by hand; `RunConfig`'s own pydantic defaults are the source of
  # truth this must match (S6.4).
  default = "{\"shuffle_partitions\":null,\"target_file_size_mb\":512,\"repartition_before_write\":true}"
}

# --- IAM grant-generation inputs --------------------------------------------

variable "landing_feed_prefixes" {
  description = <<-EOT
    Landing prefixes ("<source>/<feed>", S5) routed to THIS pipeline --
    read-only S3 grant scoped to exactly these feeds' canonical
    `received_at=` trees (I-21); no vestibule/`incoming/` access, no write,
    no delete. An empty list is valid (a pipeline provisioned before its
    first feed is wired) and simply omits the landing-read statements.
  EOT
  type        = list(string)
  default     = []
}

variable "co_effect_tables" {
  description = <<-EOT
    One entry per declared co-effect (`PipelineSpecModel.co_effects`,
    S6.2) -- read-only Glue + S3 grants GENERATED from this list, so
    reading another pipeline's data is a provisioning event, not a YAML
    edit (I-21/S-15). `s3_prefix` is a KEY PREFIX WITHIN the shared lake
    bucket (e.g. "tables/other-pipeline/state/"), not a full `s3://` URI --
    every co-effect table this LLD's tables live in the same
    `$${p}-lake` bucket as this pipeline's own (S5), just under a
    different `tables/<other-slug>/` prefix.
  EOT
  type = list(object({
    database  = string
    table     = string
    s3_prefix = string
  }))
  default = []
}

# --- alerting / log retention -----------------------------------------------

variable "alert_email" {
  description = "Email address subscribed to this pipeline's own alarm SNS topic; empty disables SNS entirely (ambiguity 4, main.tf header: `modules/spine-platform` does not output its own topic ARN for reuse)."
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for this job's explicitly-created continuous-logging log group [S-18]."
  type        = number
  default     = 30
}
