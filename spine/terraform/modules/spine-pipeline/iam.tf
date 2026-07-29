# iam.tf -- the per-pipeline `spine-job-<slug>` role, LLD S10.3's row
# VERBATIM (I-21/S-4/S-5/S-15). This is the ONLY IAM role this module
# creates; the state machine reuses the platform-shared `spine-sfn` role
# (`var.spine_sfn_role_arn`, from the sibling `modules/spine-platform`) --
# S10.3 is explicit that the SFN role stays platform-level while the JOB
# role is per-pipeline.
#
# Posture restated precisely, as grep-able facts about THIS file, not just
# prose (S10.3's "restated precisely" instruction):
#   - No `s3:Delete*` action appears anywhere below.
#   - No statement anywhere below references `"${var.lake_bucket_arn}/
#     ledger/*"` (ingestion's own ledger prefix) -- I-21's "no grant exists
#     in any spine role" is implemented as absence, not an explicit Deny
#     (an explicit Deny would be redundant defense-in-depth over IAM's own
#     implicit-deny default; documented as a deliberate choice, not an
#     oversight).
#   - No `glue:CreateTable`/`UpdateTable`/`Get*` resource list below ever
#     contains a bare database ARN -- every Glue Catalog data statement is
#     scoped to specific table ARNs (or, for `GetDatabase`, to the exact
#     two databases this pipeline legitimately needs to resolve) plus the
#     mandatory catalog-ARN resource-hierarchy entry (Glue Catalog API
#     requirement, not a scope expansion -- same note as ingestion's own
#     `modules/platform/iam.tf`).
#   - No `iam:PassRole` statement appears (S10.3: PassRole on `${p}-
#     spine-*` roles belongs to the deploy principal only).

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    # S-14 confused-deputy guard, mirroring ingestion's `lambda_assume`/
    # `scheduler_assume` shape and `modules/spine-platform`'s own
    # `spine_sfn_assume` (ArnLike scoped to this job's own ARN, not a
    # wildcard -- tighter than the platform module's own pattern-scoped
    # trust, since this role belongs to exactly one job).
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [local.glue_job_arn]
    }
  }
}

resource "aws_iam_role" "job" {
  name               = local.job_name
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "job" {
  # --- landing: read-only, scoped to feeds routed to THIS pipeline (I-21) --
  # No vestibule (`incoming/`) access, no write, no delete -- this is the
  # runner reading already-landed canonical objects only.
  dynamic "statement" {
    for_each = var.landing_feed_prefixes
    content {
      sid       = "LandingFeedRead${statement.key}"
      actions   = ["s3:GetObject"]
      resources = ["${var.landing_bucket_arn}/${statement.value}/received_at=*"]
    }
  }

  dynamic "statement" {
    for_each = length(var.landing_feed_prefixes) > 0 ? [1] : []
    content {
      sid       = "LandingFeedList"
      actions   = ["s3:ListBucket"]
      resources = [var.landing_bucket_arn]

      condition {
        test     = "StringLike"
        variable = "s3:prefix"
        values   = [for p in var.landing_feed_prefixes : "${p}/received_at=*"]
      }
    }
  }

  # --- lake: Get/Put/List, NO Delete, own tables + run_ledger ONLY (I-21) --
  statement {
    sid     = "LakeReadWriteObjects"
    actions = ["s3:GetObject", "s3:PutObject"]
    resources = [
      "${var.lake_bucket_arn}/tables/${local.slug}/*",
      "${var.lake_bucket_arn}/spine/run_ledger/*",
    ]
  }

  statement {
    sid       = "LakeList"
    actions   = ["s3:ListBucket"]
    resources = [var.lake_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["tables/${local.slug}/*", "spine/run_ledger/*"]
    }
  }

  # --- artifacts: read spine/* only (wheel + spec + entrypoint script) ----
  statement {
    sid       = "ArtifactsRead"
    actions   = ["s3:GetObject"]
    resources = ["${var.artifacts_bucket_arn}/spine/*"]
  }

  statement {
    sid       = "ArtifactsList"
    actions   = ["s3:ListBucket"]
    resources = [var.artifacts_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["spine/*"]
    }
  }

  # --- Glue catalog: PER-TABLE only, never database-wide (I-21/S-5) ------
  # `glue:Get*` (GetTable/GetTables/GetPartitions/...) + UpdateTable +
  # CreateTable, but the RESOURCE list is exactly this pipeline's own four
  # tables plus run_ledger -- never a database ARN, so `glue:UpdateTable`
  # on another pipeline's table (a metadata-pointer attack that swaps
  # table contents with no S3 write, S-5) is structurally unreachable.
  statement {
    sid     = "PipelineTablesReadWrite"
    actions = ["glue:Get*", "glue:UpdateTable", "glue:CreateTable"]
    resources = concat(
      [local.glue_catalog_arn],
      local.pipeline_table_arns,
      [local.run_ledger_table_arn],
    )
  }

  statement {
    sid       = "PipelineDatabasesGetOnly"
    actions   = ["glue:GetDatabase"]
    resources = [local.glue_catalog_arn, local.lake_database_arn, local.spine_database_arn]
  }

  # --- co-effect tables: read-only, GENERATED from var.co_effect_tables --
  # (S-15) -- reading another pipeline's data is a provisioning event
  # (a Terraform apply that adds a list entry), not a YAML edit.
  dynamic "statement" {
    for_each = var.co_effect_tables
    content {
      sid     = "CoEffectGlueRead${statement.key}"
      actions = ["glue:GetTable"]
      resources = [
        local.glue_catalog_arn,
        "arn:aws:glue:${var.region}:${local.account_id}:database/${statement.value.database}",
        "arn:aws:glue:${var.region}:${local.account_id}:table/${statement.value.database}/${statement.value.table}",
      ]
    }
  }

  dynamic "statement" {
    for_each = var.co_effect_tables
    content {
      sid       = "CoEffectS3Read${statement.key}"
      actions   = ["s3:GetObject"]
      resources = ["${var.lake_bucket_arn}/${statement.value.s3_prefix}*"]
    }
  }

  dynamic "statement" {
    for_each = length(var.co_effect_tables) > 0 ? [1] : []
    content {
      sid       = "CoEffectS3List"
      actions   = ["s3:ListBucket"]
      resources = [var.lake_bucket_arn]

      condition {
        test     = "StringLike"
        variable = "s3:prefix"
        values   = [for c in var.co_effect_tables : "${c.s3_prefix}*"]
      }
    }
  }

  # --- events: PutEvents, conditioned events:source=conveyer.spine (S-2) --
  # (Defense in depth: the bus resource policy, `modules/spine-platform`'s
  # own concern, enforces the same condition from the resource side.)
  statement {
    sid       = "EmitSpineEvents"
    actions   = ["events:PutEvents"]
    resources = [var.event_bus_arn]

    condition {
      test     = "StringEquals"
      variable = "events:source"
      values   = ["conveyer.spine"]
    }
  }

  # --- logs / metrics ------------------------------------------------------
  statement {
    sid     = "JobLogs"
    actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [
      aws_cloudwatch_log_group.job.arn,
      "${aws_cloudwatch_log_group.job.arn}:*",
    ]
  }

  # `cloudwatch:PutMetricData` has no ARN-addressable resource (AWS
  # requirement: resource must be "*"); scoped instead by namespace
  # condition -- the EMF-extraction-failure fallback path (T-14).
  statement {
    sid       = "EmfMetricsFallback"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Conveyer/Spine"]
    }
  }
}

resource "aws_iam_role_policy" "job" {
  name   = local.job_name
  role   = aws_iam_role.job.id
  policy = data.aws_iam_policy_document.job.json
}
