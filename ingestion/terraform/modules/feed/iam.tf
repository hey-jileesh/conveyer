# Per-feed IAM role -- LLD S10.7. This is the blast-radius wall: the role
# must not be able to name another feed's landing prefix, secret, or CAS
# keyspace. sftp-pull only -- s3-push feeds run entirely inside the
# platform-owned registrar's role.

# SECURITY-GATE FIX (M-5): own account-id lookup, scoped count-gated the
# same way as the rest of this sftp-pull-only file, so an s3-push feed
# instance makes no extra STS call. `modules/platform`'s own equivalent
# lookup (`data.aws_caller_identity.current` in main.tf) cannot be reused
# here -- this module has no forward reference into a sibling module beyond
# `var.platform`'s declared output object, which does not carry account_id.
data "aws_caller_identity" "current" {
  count = local.is_sftp_pull ? 1 : 0
}

data "aws_iam_policy_document" "feed_assume_role" {
  count = local.is_sftp_pull ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current[0].account_id]
    }
  }
}

resource "aws_iam_role" "feed" {
  count = local.is_sftp_pull ? 1 : 0

  name               = "${local.p}-feed-${local.slug}"
  assume_role_policy = data.aws_iam_policy_document.feed_assume_role[0].json
}

resource "aws_iam_role_policy_attachment" "feed_basic_execution" {
  count = local.is_sftp_pull ? 1 : 0

  role       = aws_iam_role.feed[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "feed_inline" {
  count = local.is_sftp_pull ? 1 : 0

  # Landing: PutObject/GetObject ONLY under this feed's own received_at=
  # prefix -- never another feed's prefix, never the vestibule (S10.7).
  #
  # M-3: Abort/List-parts added alongside Put/Get -- effects/s3.py's
  # stream_upload aborts an in-progress multipart upload on failure; without
  # this the abort call itself raises AccessDenied (masking the real error)
  # and the orphaned parts bill forever.
  statement {
    sid = "LandingObjects"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${var.platform.landing_bucket_arn}/${local.received_prefix}*"]
  }

  statement {
    sid       = "LandingList"
    actions   = ["s3:ListBucket"]
    resources = [var.platform.landing_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.received_prefix}*"]
    }
  }

  # Secrets: GetSecretValue ONLY on this feed's own secret (S10.7).
  statement {
    sid       = "SftpSecret"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.sftp[0].arn]
  }

  # Ledger write + read, table-scoped -- D-8: Iceberg's shared metadata
  # tree cannot be partition-scoped by IAM, so this is feed-scoped only in
  # the sense that landing/secrets/CAS are; the ledger table itself is
  # shared. Composite per S10.6's "ledger-write" / "ledger-read" grants.
  statement {
    sid       = "LedgerObjects"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${var.platform.lake_bucket_arn}/ledger/*"]
  }

  statement {
    sid       = "LedgerList"
    actions   = ["s3:ListBucket"]
    resources = [var.platform.lake_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["ledger/*"]
    }
  }

  statement {
    sid     = "LedgerCatalog"
    actions = ["glue:GetTable", "glue:GetDatabase", "glue:UpdateTable"]
    resources = [
      var.platform.glue_catalog_arn,
      var.platform.glue_database_arn,
      var.platform.ledger_table_arn,
    ]
  }

  # CAS turnstile: this feed's own keyspace only, via dynamodb:LeadingKeys
  # -- the mechanism that makes the CAS grant a blast-radius wall too
  # (S8.4 key format `batch#<feed_id>#<batch_id>`, S10.7).
  statement {
    sid       = "CasTurnstile"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"]
    resources = [var.platform.cas_table_arn]

    condition {
      test     = "StringLike"
      variable = "dynamodb:LeadingKeys"
      values   = ["batch#${var.feed.feed_id}#*"]
    }
  }

  statement {
    sid       = "Events"
    actions   = ["events:PutEvents"]
    resources = [var.platform.event_bus_arn]
  }

  # Registry read -- same grant shape as the registrar's (S10.6): the
  # driver resolves its own FeedConfig from the rendered registry object,
  # never the filesystem (S6.8).
  statement {
    sid       = "RegistryRead"
    actions   = ["s3:GetObject"]
    resources = ["${var.platform.artifacts_bucket_arn}/registry/*"]
  }

  statement {
    sid       = "DlqSend"
    actions   = ["sqs:SendMessage"]
    resources = [var.platform.dlq_arn]
  }
}

resource "aws_iam_role_policy" "feed" {
  count = local.is_sftp_pull ? 1 : 0

  name   = "${local.p}-feed-${local.slug}-inline"
  role   = aws_iam_role.feed[0].id
  policy = data.aws_iam_policy_document.feed_inline[0].json
}
