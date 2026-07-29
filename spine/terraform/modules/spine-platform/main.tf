# modules/spine-platform -- LLD 004.1 S10.2 + the platform-role rows of S10.3
# + S11.4 alarms + S11.5 Athena named queries.
#
# Shared spine infrastructure: the spine Glue database, the router Lambda +
# its EventBridge wiring, the spine DLQ, the bus resource policy (I-22), the
# artifacts-bucket spine/* write protection (I-23), the platform-level
# alarms, the Athena named queries, and the two platform IAM roles
# (`spine-router`, `spine-sfn`). Per-pipeline resources (state machine, Glue
# job, the per-pipeline job IAM role, per-pipeline alarms -- I-21, S10.4) are
# a SIBLING module, `modules/spine-pipeline` (not built by this bead).
#
# Like `ingestion/terraform/modules/platform`, this module deliberately does
# NOT declare a `provider "aws" {}` block -- it is always called from a root
# module that configures the provider (incl. `default_tags`) once.
#
# --- Cross-module state hazards (read before touching s3.tf/events.tf) -----
#
# This module consumes the ingestion platform's bus and artifacts bucket as
# VARIABLES (name/arn), not as resources it owns. Two consequences:
#
#   1. Bus resource policy (I-22): `ingestion/terraform/modules/platform`
#      does NOT currently declare an `aws_cloudwatch_event_bus_policy` on
#      `${p}-bus` (verified by inspection, 2026-07-29) -- so this module is
#      the bus policy's SOLE owner. If a future ingestion-side change adds
#      its own bus policy resource, the two will fight (last apply wins,
#      because a bus/queue/bucket resource-based policy is a single
#      document, not additive across Terraform states). Documented here so
#      the next person touching either module sees the assumption before
#      breaking it.
#
#   2. Artifacts bucket policy (I-23): `ingestion/terraform/modules/platform`
#      s3.tf ALREADY owns `aws_s3_bucket_policy.artifacts` (a TLS-only-deny
#      statement, no `spine/*` awareness). A bucket has exactly ONE bucket
#      policy; a second `aws_s3_bucket_policy` resource in THIS module
#      pointed at the same bucket would not merge with ingestion's -- it
#      would silently overwrite it (or vice versa, depending on apply
#      order), reintroducing the exact class of security gap ingestion's
#      own TLS-deny fix was for. This module therefore does NOT create an
#      `aws_s3_bucket_policy` resource at all. It exposes the spine-specific
#      deny statement as a `data "aws_iam_policy_document"` and outputs its
#      JSON (`artifacts_spine_policy_document_json`) for the env root to
#      merge into ONE combined document (`source_policy_documents`) applied
#      as ONE `aws_s3_bucket_policy` -- an env-root concern, out of scope for
#      this bead (see s3.tf for the document itself).
#      Bucket *versioning*, by contrast, is a distinct resource type that
#      ingestion's module does NOT declare for the artifacts bucket (only
#      for landing) -- this module safely owns
#      `aws_s3_bucket_versioning.artifacts` with no such conflict.

terraform {
  required_version = ">= 1.7"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.60"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # `${p}` throughout the LLD == `${name_prefix}-${env}` (S5).
  p          = "${var.name_prefix}-${var.env}"
  account_id = data.aws_caller_identity.current.account_id

  spine_glue_database = "${var.name_prefix}_${var.env}_spine"

  # S5: "State machine | ${p}-spine-<slug>"; "Glue job | ${p}-spine-<slug>".
  # Both `spine-router` (StartExecution) and `spine-sfn` (StartJobRun) scope
  # their grants by this SAME naming-convention wildcard -- neither role can
  # reference the per-pipeline resources `modules/spine-pipeline` (a sibling
  # module, not built by this bead) will create.
  spine_state_machine_arn_pattern = "arn:aws:states:${var.region}:${local.account_id}:stateMachine:${local.p}-spine-*"
  spine_glue_job_arn_pattern      = "arn:aws:glue:${var.region}:${local.account_id}:job/${local.p}-spine-*"

  # Handed to the router as CONVEYER_SFN_ARN_PREFIX -- everything up to and
  # including the trailing "-spine-"; the router appends `slug(pipeline)`
  # itself (spine/entrypoints/router.py's own docstring, S5).
  spine_sfn_arn_prefix = "arn:aws:states:${var.region}:${local.account_id}:stateMachine:${local.p}-spine-"
}
