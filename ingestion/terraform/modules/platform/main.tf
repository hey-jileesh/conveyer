# modules/platform -- LLD S10.2-S10.6.
#
# Shared infrastructure: three S3 buckets, the CAS DynamoDB table, the Glue
# database, Athena workgroup + named queries, the event bus, three Lambda
# functions (registrar/absence/maintenance -- per-feed driver functions are
# `modules/feed`'s concern), the four IAM roles, and monitoring.
#
# This module deliberately does NOT declare a `provider "aws" {}` block: it
# is always called from a root module (`terraform/envs/<env>/main.tf`, owned
# by a sibling bead) that configures the provider (incl. `default_tags`, LLD
# S10's `{project, component, env}` tag set) once and lets it propagate.
# Declaring a provider block here would violate Terraform's rule that a
# non-root module used with `for_each`/`count`/`depends_on` at the call site
# must not configure its own providers.

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
  p             = "${var.name_prefix}-${var.env}"
  glue_database = "${var.name_prefix}_${var.env}_ingestion"
  ledger_table  = "delivery_ledger" # bootstrap-owned (D-7); Terraform never creates it

  account_id = data.aws_caller_identity.current.account_id

  glue_catalog_arn  = "arn:aws:glue:${var.region}:${local.account_id}:catalog"
  glue_database_arn = "arn:aws:glue:${var.region}:${local.account_id}:database/${local.glue_database}"
  glue_table_arn    = "arn:aws:glue:${var.region}:${local.account_id}:table/${local.glue_database}/${local.ledger_table}"
  glue_ledger_arns  = [local.glue_catalog_arn, local.glue_database_arn, local.glue_table_arn]

  # `var.feeds_json` == `jsonencode({registry_version, feeds})`, root S10.1.
  feeds           = jsondecode(var.feeds_json).feeds
  s3_push_feeds   = [for f in local.feeds : f if f.driver == "s3-push"]
  sftp_pull_feeds = [for f in local.feeds : f if f.driver == "sftp-pull"]

  # CONVEYER_* env vars every function needs (S7.2); `CONVEYER_FEED_ID` is
  # added only by modules/feed's per-feed driver functions.
  base_env = {
    CONVEYER_ENV               = var.env
    CONVEYER_AWS_REGION        = var.region
    CONVEYER_LANDING_BUCKET    = aws_s3_bucket.landing.id
    CONVEYER_LAKE_BUCKET       = aws_s3_bucket.lake.id
    CONVEYER_ARTIFACTS_BUCKET  = aws_s3_bucket.artifacts.id
    CONVEYER_GLUE_DATABASE     = local.glue_database
    CONVEYER_LEDGER_TABLE      = local.ledger_table
    CONVEYER_CAS_TABLE         = aws_dynamodb_table.cas.name
    CONVEYER_EVENT_BUS         = aws_cloudwatch_event_bus.ingestion.name
    CONVEYER_REGISTRY_URI      = "s3://${aws_s3_bucket.artifacts.id}/registry/feeds.json"
    CONVEYER_ATHENA_WORKGROUP  = aws_athena_workgroup.ingestion.name
    CONVEYER_ATHENA_OUTPUT_URI = "s3://${aws_s3_bucket.artifacts.id}/athena-results/"
  }
}
