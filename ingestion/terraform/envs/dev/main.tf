# Dev env root -- LLD S10.1. Terraform reads sources/**/source.yaml
# directly (D-12: no codegen); one `feed` instance per file, keyed by
# feed_id, wired against the platform module's output object (S10.6).

terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.60"
    }
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      project   = "conveyer"
      component = "ingestion"
      env       = var.env
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  source_files = fileset("${path.module}/../../../sources", "*/*/source.yaml")
  feeds = { for f in local.source_files :
    yamldecode(file("${path.module}/../../../sources/${f}")).feed_id
    => yamldecode(file("${path.module}/../../../sources/${f}"))
  }

  # --- Runner Spine (D-1) -----------------------------------------------

  p = "${var.name_prefix}-${var.env}"

  # `modules/feed`'s per-feed sftp-pull driver role ARNs (null for s3-push
  # feeds, S10.7) -- ingestion's own events:PutEvents producers, LLD I-22.
  feed_driver_role_arns = compact([for f in module.feed : f.driver_role_arn])

  ingestion_producer_role_arns = concat(
    [module.platform.registrar_role_arn, module.platform.absence_role_arn],
    local.feed_driver_role_arns,
  )

  # The identity exemplar's job-role ARN, PREDICTED from
  # `modules/spine-pipeline`'s own deterministic naming (`local.job_name =
  # "${p}-spine-${slug}"`, reused there for BOTH the Glue job/state machine
  # AND the job role -- verified by reading that module's iam.tf/main.tf
  # directly, not assumed) -- computed here, NOT read back from
  # `module.spine_pipeline_identity.job_role_arn`, because that would be a
  # genuine module dependency CYCLE: spine_platform's bus policy needs the
  # job role ARN, while spine_pipeline_identity needs spine_platform's own
  # outputs (spine_database_name, spine_sfn_role_arn). IAM ARNs are string
  # patterns, not existence-checked at plan/apply time (the same technique
  # `modules/spine-platform` itself uses for `spine_glue_job_arn_pattern`/
  # `spine_sfn_arn_prefix`) -- this duplicates a naming fact across a
  # module boundary; flagged in the bead handoff for architect review, not
  # silently fixed here (spine-pipeline is a previously-validated module
  # outside this bead's file list).
  identity_pipeline      = "pipelines/identity"
  identity_pipeline_slug = replace(local.identity_pipeline, "/", "--")
  identity_job_role_arn  = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${local.p}-spine-${local.identity_pipeline_slug}"
}

module "platform" {
  source = "../../modules/platform"

  name_prefix = var.name_prefix
  env         = var.env
  region      = var.region
  image_uri   = var.image_uri   # "<ecr>:<tag>"; see runbook S10.8
  alert_email = var.alert_email # optional; empty disables SNS subscription

  landing_glacier_days = var.landing_glacier_days

  feeds_json = jsonencode({ registry_version = 1, feeds = values(local.feeds) })

  # LLD 004.1 D-1/S12.6(3)/I-17 [E-7]: register the spine run ledger with
  # this same maintenance Lambda's table list + delete-grant extension.
  spine_run_ledger_identifier = "${module.spine_platform.glue_database_name}.${module.spine_platform.run_ledger_table_name}"

  # LLD 004.1 S10.1/I-23: merge spine-platform's spine/*-prefix protection
  # statement into the ONE artifacts bucket policy this module owns (see
  # that module's variable description + modules/platform/s3.tf comment --
  # single-writer cross-module hazard, house-style memory).
  extra_artifacts_policy_statements_json = [module.spine_platform.artifacts_spine_policy_document_json]
}

module "feed" {
  source = "../../modules/feed"

  for_each = local.feeds

  feed                = each.value
  platform            = module.platform # object output: names, ARNs (S10.6 outputs)
  image_uri           = var.image_uri
  driver_bytes_budget = var.driver_bytes_budget
}

# --- Runner Spine (LLD 004.1 D-1, S10.1-S10.4) ------------------------------

module "spine_platform" {
  source = "../../../../spine/terraform/modules/spine-platform"

  name_prefix = var.name_prefix
  env         = var.env
  region      = var.region

  event_bus_name        = module.platform.event_bus_name
  event_bus_arn         = module.platform.event_bus_arn
  artifacts_bucket_name = module.platform.artifacts_bucket_name
  artifacts_bucket_arn  = module.platform.artifacts_bucket_arn
  athena_workgroup_name = module.platform.athena_workgroup_name

  ingestion_producer_role_arns = local.ingestion_producer_role_arns
  # See locals.identity_job_role_arn's comment: PREDICTED, not read back
  # from module.spine_pipeline_identity, to avoid a module dependency cycle.
  spine_job_role_arns = [local.identity_job_role_arn]

  # conveyer-nvh.47: no root-side fallback -- var.artifacts_deploy_
  # principal_arn is now REQUIRED and must be the deploy ROLE's own ARN
  # (see variables.tf's own docstring for why a caller-identity fallback
  # was wrong: aws:PrincipalArn evaluates to the ROLE arn for an assumed
  # role, never the session arn data.aws_caller_identity.current.arn
  # would have resolved to).
  artifacts_deploy_principal_arn = var.artifacts_deploy_principal_arn

  router_zip_path   = "${path.module}/${var.spine_router_zip_path}"
  argv_budget_bytes = var.spine_argv_budget_bytes

  alert_email        = var.spine_alert_email
  log_retention_days = var.spine_log_retention_days
}

# Phase 1 instantiates exactly one pipeline (LLD S10.4): the identity
# exemplar. 009 will drive one `module "spine_pipeline_<slug>"` block per
# pipeline from each pipeline's own `pipeline.yaml` -- not a `for_each`
# (mirrors this LLD's own "Phase 1 instantiates exactly one" framing, and
# the module's per-instance `for_each`-forbidding-provider-block
# constraint is satisfied either way).
module "spine_pipeline_identity" {
  source = "../../../../spine/terraform/modules/spine-pipeline"

  pipeline    = local.identity_pipeline
  name_prefix = var.name_prefix
  env         = var.env
  region      = var.region

  landing_bucket_name   = module.platform.landing_bucket_name
  landing_bucket_arn    = module.platform.landing_bucket_arn
  lake_bucket_name      = module.platform.lake_bucket_name
  lake_bucket_arn       = module.platform.lake_bucket_arn
  artifacts_bucket_name = module.platform.artifacts_bucket_name
  artifacts_bucket_arn  = module.platform.artifacts_bucket_arn
  event_bus_name        = module.platform.event_bus_name
  event_bus_arn         = module.platform.event_bus_arn

  spine_database_name   = module.spine_platform.glue_database_name
  run_ledger_table_name = module.spine_platform.run_ledger_table_name
  spine_sfn_role_arn    = module.spine_platform.spine_sfn_role_arn

  spine_wheel_uri            = var.spine_wheel_uri
  glue_entrypoint_script_uri = var.glue_entrypoint_script_uri
  pipeline_spec_uri          = var.spine_pipeline_spec_uri

  # LLD S10.7's own feed-routing convention, applied once the identity feed
  # (LLD 004.1 S12.6(4)) exists: only that feed's landing prefix is granted
  # read to this pipeline's job role (I-21 -- no vestibule, no write).
  landing_feed_prefixes = ["conveyer-internal/identity-smoke"]

  alert_email        = var.spine_alert_email
  log_retention_days = var.spine_log_retention_days
}

# --- Operator role (LLD 004.1 S10.3, I-20's governed `--rN` escape hatch) ---
#
# No-assume-by-default: the trust condition below can only be satisfied by
# a principal ARN present in `var.spine_operator_principal_arns` (empty by
# default), so this role exists (and holds its grants) but literally no one
# can assume it until an operator's real principal is wired in -- a
# deliberate "create it, defer who can use it" choice per this bead's task
# framing (document-and-defer via a variable stub).

data "aws_iam_policy_document" "spine_operator_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:PrincipalArn"
      # A syntactically valid trust policy needs a non-empty condition
      # values list; the sentinel ARN below matches no real principal, so
      # an empty `var.spine_operator_principal_arns` is genuinely
      # no-assume-by-default rather than an invalid/rejected policy.
      values = (
        length(var.spine_operator_principal_arns) > 0
        ? var.spine_operator_principal_arns
        : ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/__no-spine-operator-configured__"]
      )
    }
  }
}

resource "aws_iam_role" "spine_operator" {
  name               = "${local.p}-spine-operator"
  assume_role_policy = data.aws_iam_policy_document.spine_operator_assume.json
}

data "aws_iam_policy_document" "spine_operator" {
  statement {
    sid = "SpineOperatorStartExecution"
    # I-20's runbook precondition (S10.6) needs a liveness check BEFORE
    # starting a `--rN` execution -- states:DescribeExecution here, plus
    # glue:GetJobRuns/GetJobRun below.
    actions   = ["states:StartExecution", "states:DescribeExecution"]
    resources = ["${module.spine_platform.spine_sfn_arn_prefix}*"]
  }

  statement {
    sid       = "SpineOperatorGlueJobLiveness"
    actions   = ["glue:GetJobRuns", "glue:GetJobRun"]
    resources = [module.spine_platform.spine_glue_job_arn_pattern]
  }
}

resource "aws_iam_role_policy" "spine_operator" {
  name   = "${local.p}-spine-operator"
  role   = aws_iam_role.spine_operator.id
  policy = data.aws_iam_policy_document.spine_operator.json
}
