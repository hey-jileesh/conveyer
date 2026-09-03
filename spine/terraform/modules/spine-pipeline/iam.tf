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
#     contains a `table/<db>/*` WILDCARD or a database-wide grant of those
#     actions (I-21/S-5's own "never database-wide" -- table actions stay
#     scoped to specific per-table ARNs). `PipelineTablesReadWrite` DOES
#     also list `local.lake_database_arn` alongside those per-table ARNs
#     (F-1 fix, security gate `wf_c9aadeb2-8eb`, MEDIUM) -- the Glue
#     Catalog API requires the DATABASE ancestor ARN, not just the
#     catalog-ARN resource-hierarchy entry, for any action on a table
#     inside it ("Actions on a table ... require permission on the table,
#     database, and catalog", AWS's own `glue-specifying-resource-arns`
#     doc); omitting it made every `glue:UpdateTable`/`CreateTable`/`Get*`
#     call fail closed with `AccessDenied` against a real Glue catalog. The
#     invariant this file restates is "no wildcard, no database-wide
#     GRANT" -- not "no database ARN ever appears" (same note as
#     ingestion's own `modules/platform/iam.tf`, whose `glue_ledger_arns`
#     already includes catalog + database + table for the same reason).
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
  # F-1 fix: `tables/${local.table_slug}/*`, NOT `local.slug` -- the S3
  # object prefix under `tables/` mirrors the TABLE-name slug (same
  # `local.table_slug`/`main.tf` reasoning: the physical per-pipeline data
  # prefix is keyed by the same trailing-segment slug the table names
  # themselves use, not the "--"-joined ARN/exec-path form).
  statement {
    sid     = "LakeReadWriteObjects"
    actions = ["s3:GetObject", "s3:PutObject"]
    resources = [
      "${var.lake_bucket_arn}/tables/${local.table_slug}/*",
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
      values   = ["tables/${local.table_slug}/*", "spine/run_ledger/*"]
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
  # CreateTable, but the RESOURCE list is exactly this pipeline's own
  # per-table ARNs plus run_ledger -- never a `table/<db>/*` wildcard or a
  # database-wide grant, so `glue:UpdateTable` on another pipeline's table
  # (a metadata-pointer attack that swaps table contents with no S3 write,
  # S-5) is structurally unreachable.
  #
  # F-1 fix (security gate `wf_c9aadeb2-8eb`, MEDIUM): also includes
  # `local.lake_database_arn`, the DATABASE ancestor of every entry in
  # `local.pipeline_table_arns` -- the Glue Catalog API requires the
  # database (and catalog) ARN, not just the table ARN, for any
  # `Get*`/`UpdateTable`/`CreateTable` call against a table inside it (AWS's
  # own `glue-specifying-resource-arns` doc: "Actions on a table ... require
  # permission on the table, database, and catalog"). Without it, EVERY
  # first-touch of this pipeline's own tables against a real Glue catalog
  # fails closed with `AccessDenied` -- not a scope expansion (this is one
  # specific, named database ARN, still zero `table/<db>/*` wildcards and
  # zero database-WIDE grant of these actions).
  statement {
    sid     = "PipelineTablesReadWrite"
    actions = ["glue:Get*", "glue:UpdateTable", "glue:CreateTable"]
    resources = concat(
      [local.glue_catalog_arn, local.lake_database_arn],
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
