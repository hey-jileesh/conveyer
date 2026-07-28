# iam.tf -- the four platform roles, inline policies per LLD S10.6's table.
#
# Two deviations from the table's literal text, both required for features
# the table's OWN sibling sections (S10.4/S10.5) mandate; each is called out
# where it appears below (also summarized in the bead handoff report):
#   1. `athena:GetQueryResults` added to `maintenance` -- the table lists
#      only StartQueryExecution/GetQueryExecution, but
#      `maintenance/optimize.py::_get_results` calls the paginated
#      `get_query_results` API to fetch reconciliation candidate rows; without
#      it step 3 (S9.4) cannot run at all.
#   2. `sqs:SendMessage` on the DLQ added to `absence` and `maintenance` --
#      the table's rows for those two roles omit it, but S10.5 wires a
#      `dead_letter_config` to the same DLQ on both functions, and a Lambda's
#      own execution role (not a separate role) must hold `sqs:SendMessage`
#      on its DLQ target for that wiring to function.
#
# Also: Glue actions require the catalog-level ARN in the resource list
# alongside the database/table ARNs (a Glue Data Catalog resource-hierarchy
# requirement, not a scope expansion) -- `local.glue_ledger_arns` (main.tf)
# already includes it.
#
# SECURITY-GATE FIX (C-1, M-10): every statement below that names a bucket
# ARN (`aws_s3_bucket.lake.arn` / `aws_s3_bucket.landing.arn`) directly, NOT
# an object-prefix under it, MUST be restricted to `s3:ListBucket` alone,
# with a `StringLike`/`s3:prefix` condition scoping which keys the listing
# may enumerate. The pre-fix `ledger_write`/`ledger_read` composites put the
# bare lake bucket ARN in the SAME resource list as `s3:Get*`/`s3:Put*`/
# `s3:List*` -- since IAM resource lists apply to every action in the
# statement, `s3:Put*` (a wildcard covering bucket-level actions like
# PutBucketPolicy/PutBucketAcl/PutBucketPublicAccessBlock/
# PutEncryptionConfiguration/PutBucketVersioning) resolved against the
# BUCKET ARN itself, not just objects under `ledger/*`: a compromised
# registrar could have made the ledger bucket public or stripped its
# encryption without ever holding `s3:DeleteObject`, defeating D-9 entirely.
# `registrar`'s own `LandingIncomingRead` statement had the identical
# shape (M-10) with the added consequence that the unconditioned
# `s3:ListBucket` on the whole landing bucket let a compromised registrar
# enumerate every feed's canonical `received_at=` tree, not just its own
# incoming/ vestibule. Shape copied from `modules/feed/iam.tf`'s
# `LedgerObjects`/`LedgerList` statements, which were already correct.
# Every other S3 statement in this file was audited for the same
# bucket-ARN-in-object-statement pattern; none of the others exhibit it
# (`LedgerObjectsReadWriteDelete`, `RegistryRead`, `AthenaResultsWrite`,
# `LandingCanonicalReadWrite` all resolve to object-prefix resources only,
# no bare bucket ARN).

# --- reusable ledger-write / ledger-read composites (S10.6's own vocabulary) -

data "aws_iam_policy_document" "ledger_write" {
  statement {
    sid       = "LedgerWriteS3Objects"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.lake.arn}/ledger/*"]
  }

  statement {
    sid       = "LedgerWriteS3List"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["ledger/*"]
    }
  }

  statement {
    sid       = "LedgerWriteGlue"
    actions   = ["glue:GetTable", "glue:GetDatabase", "glue:UpdateTable"]
    resources = local.glue_ledger_arns
  }
}

data "aws_iam_policy_document" "ledger_read" {
  statement {
    sid       = "LedgerReadS3Objects"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/ledger/*"]
  }

  statement {
    sid       = "LedgerReadS3List"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["ledger/*"]
    }
  }

  statement {
    sid       = "LedgerReadGlue"
    actions   = ["glue:GetTable"]
    resources = local.glue_ledger_arns
  }
}

# --- trust policies ----------------------------------------------------------
#
# SECURITY-GATE FIX (M-5): bare service-principal trusts let ANY account's
# Lambda/Scheduler service invocation of `sts:AssumeRole` on behalf of that
# service succeed, confused-deputy style, unless scoped back to this
# account. `aws:SourceAccount` pins the trust to resources owned by this
# account; `scheduler_assume` additionally pins `aws:SourceArn` to this
# account's own schedules (`schedule/*` matches any group, since IAM
# wildcard matching is not path-segment-aware -- it matches the literal
# `schedule/<group>/<name>` string with `*` covering `<group>/<name>`).

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:scheduler:${var.region}:${local.account_id}:schedule/*"]
    }
  }
}

# --- registrar -----------------------------------------------------------

resource "aws_iam_role" "registrar" {
  name               = "${local.p}-registrar"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "registrar_basic" {
  role       = aws_iam_role.registrar.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "registrar" {
  source_policy_documents = [
    data.aws_iam_policy_document.ledger_write.json,
    data.aws_iam_policy_document.ledger_read.json,
  ]

  statement {
    sid       = "LandingIncomingReadObjects"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.landing.arn}/*/incoming/*"]
  }

  # M-10: ListBucket must be conditioned to the incoming/ vestibule prefix --
  # unconditioned, it let a compromised registrar enumerate every feed's
  # canonical received_at= tree, not just vestibule uploads.
  statement {
    sid       = "LandingIncomingList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.landing.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["*/incoming/*"]
    }
  }

  # M-3: Abort/List-parts added alongside Put/Get -- effects/s3.py's
  # stream_upload aborts an in-progress multipart upload on failure; without
  # this the abort call itself raises AccessDenied (masking the real error)
  # and the orphaned parts bill forever (no role held AbortMultipartUpload
  # before this fix).
  statement {
    sid = "LandingCanonicalReadWrite"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.landing.arn}/*/received_at=*"]
  }

  statement {
    sid       = "CasReadWrite"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.cas.arn]
  }

  statement {
    sid       = "EmitEvents"
    actions   = ["events:PutEvents"]
    resources = [aws_cloudwatch_event_bus.ingestion.arn]
  }

  statement {
    sid       = "RegistryRead"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.artifacts.arn}/registry/*"]
  }

  statement {
    sid       = "DlqSend"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }
}

resource "aws_iam_role_policy" "registrar" {
  name   = "${local.p}-registrar"
  role   = aws_iam_role.registrar.id
  policy = data.aws_iam_policy_document.registrar.json
}

# --- absence ---------------------------------------------------------------

resource "aws_iam_role" "absence" {
  name               = "${local.p}-absence"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "absence_basic" {
  role       = aws_iam_role.absence.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "absence" {
  source_policy_documents = [data.aws_iam_policy_document.ledger_read.json]

  statement {
    sid       = "CasReadWriteScan"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:Scan"]
    resources = [aws_dynamodb_table.cas.arn]
  }

  statement {
    sid       = "EmitEvents"
    actions   = ["events:PutEvents"]
    resources = [aws_cloudwatch_event_bus.ingestion.arn]
  }

  statement {
    sid       = "RegistryRead"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.artifacts.arn}/registry/*"]
  }

  statement {
    sid     = "InvokeResumers"
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.registrar.arn,
      "arn:aws:lambda:${var.region}:${local.account_id}:function:${local.p}-driver-*",
    ]
  }

  # Deviation 2 (see file header): required by S10.5's DLQ wiring on this
  # function, missing from the table's `absence` row.
  statement {
    sid       = "DlqSend"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }
}

resource "aws_iam_role_policy" "absence" {
  name   = "${local.p}-absence"
  role   = aws_iam_role.absence.id
  policy = data.aws_iam_policy_document.absence.json
}

# --- maintenance -------------------------------------------------------------

resource "aws_iam_role" "maintenance" {
  name               = "${local.p}-maintenance"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "maintenance_basic" {
  role       = aws_iam_role.maintenance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "maintenance" {
  statement {
    sid = "AthenaQuery"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      # Deviation 1 (see file header): not in the S10.6 table's literal
      # text, but required by `optimize.py::_get_results`.
      "athena:GetQueryResults",
    ]
    resources = [aws_athena_workgroup.ingestion.arn]
  }

  statement {
    sid       = "LedgerGlueReadUpdate"
    actions   = ["glue:GetTable", "glue:UpdateTable"]
    resources = local.glue_ledger_arns
  }

  # The sole ledger-prefix Delete grant in the platform (D-9).
  statement {
    sid       = "LedgerObjectsReadWriteDelete"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/ledger/*"]
  }

  statement {
    sid       = "AthenaResultsWrite"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.artifacts.arn}/athena-results/*"]
  }

  # Deviation 2 (see file header): required by S10.5's DLQ wiring on this
  # function, missing from the table's `maintenance` row.
  statement {
    sid       = "DlqSend"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }
}

resource "aws_iam_role_policy" "maintenance" {
  name   = "${local.p}-maintenance"
  role   = aws_iam_role.maintenance.id
  policy = data.aws_iam_policy_document.maintenance.json
}

# --- scheduler ---------------------------------------------------------------
#
# Assumed by `scheduler.amazonaws.com` for EventBridge Scheduler targets
# (absence + maintenance schedules here, plus every per-feed pull schedule
# `modules/feed` creates, S10.7) -- not a Lambda execution role, so no
# `AWSLambdaBasicExecutionRole` attachment.

resource "aws_iam_role" "scheduler" {
  name               = "${local.p}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    sid       = "InvokeIngestionFunctions"
    actions   = ["lambda:InvokeFunction"]
    resources = ["arn:aws:lambda:${var.region}:${local.account_id}:function:${local.p}-*"]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  name   = "${local.p}-scheduler"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}
