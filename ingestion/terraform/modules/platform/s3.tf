# s3.tf -- landing/lake/artifacts buckets (LLD S10.2).
#
# All three: force_destroy = false, SSE-S3, public-access-block all true,
# TLS-only deny statement. Extras per bucket below.

resource "aws_s3_bucket" "landing" {
  bucket        = "${local.p}-landing"
  force_destroy = false
}

resource "aws_s3_bucket" "lake" {
  bucket        = "${local.p}-lake"
  force_destroy = false
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = "${local.p}-artifacts"
  force_destroy = false
}

# --- SSE-S3 --------------------------------------------------------------

resource "aws_s3_bucket_server_side_encryption_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# --- public access block (all-true) ---------------------------------------

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- landing: versioning ON, EventBridge notifications on ------------------

resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_notification" "landing" {
  bucket      = aws_s3_bucket.landing.id
  eventbridge = true
}

# --- landing lifecycle: ONE resource owning all rules -----------------------
#
# (a) one rule per s3-push feed, id `vestibule-<feed_id>`: expire the
#     `incoming/` vestibule after 14 d.
# (b) one rule for canonical data: transition to GLACIER_IR after
#     `var.landing_glacier_days`, never expire.
#
# S3 lifecycle `filter.prefix` is leading-prefix-only -- there is no way to
# express "any `<source>/<feed>/received_at=*` subtree" as a single prefix
# without enumerating every feed, and the LLD calls for exactly ONE canonical
# rule. So (b) is bucket-wide (empty filter) with no expiration action at
# all; vestibule objects are already removed by (a)'s 14 d rule long before
# the default 90 d transition threshold, so they are never actually affected
# by (b) in practice.

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id

  dynamic "rule" {
    for_each = { for f in local.s3_push_feeds : f.feed_id => f }
    content {
      id     = "vestibule-${rule.key}"
      status = "Enabled"
      filter {
        prefix = "${rule.key}/incoming/"
      }
      expiration {
        days = 14
      }
    }
  }

  rule {
    id     = "canonical-glacier"
    status = "Enabled"
    filter {}
    transition {
      days          = var.landing_glacier_days
      storage_class = "GLACIER_IR"
    }
  }

  # SECURITY-GATE FIX (M-3): effects/s3.py's stream_upload aborts an
  # in-progress multipart upload on failure -- without this rule (and the
  # AbortMultipartUpload/ListMultipartUploadParts grants added to the
  # registrar/feed roles), a failed abort call, or any multipart upload that
  # is simply never resumed, leaves orphaned parts billing forever. Bucket-
  # wide (no filter): multipart uploads happen on canonical received_at=
  # writes, and there is no reason to let ANY stalled multipart upload,
  # anywhere in this bucket, persist indefinitely.
  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# --- landing bucket policy ---------------------------------------------------
#
# One policy, four statement families:
#   1. TLS-only deny (no exceptions).
#   2. Deny Delete* to everyone, always -- nothing legitimately deletes
#      landing objects; lifecycle acts via S3 itself.
#   3. Deny PutObject on canonical `*/received_at=*` keys unless the
#      principal is the registrar role or a driver role. Driver roles
#      (`${p}-feed-<slug>`) are created by the sibling `modules/feed`, which
#      this module cannot reference forward -- so, like the scheduler role's
#      own grant (S10.6), this is expressed as a naming-convention wildcard
#      rather than an enumerated resource list.
#   4. Per s3-push feed: Allow that feed's `partner_principal_arns` to
#      PutObject on `<feed_id>/incoming/*` and to ListBucket conditioned on
#      that same prefix -- and nothing else (D-15).

data "aws_iam_policy_document" "landing" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.landing.arn, "${aws_s3_bucket.landing.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  statement {
    sid       = "DenyDeleteAlways"
    effect    = "Deny"
    actions   = ["s3:DeleteObject", "s3:DeleteObjectVersion"]
    resources = ["${aws_s3_bucket.landing.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }
  }

  statement {
    sid       = "DenyCanonicalPutExceptFramework"
    effect    = "Deny"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.landing.arn}/*/received_at=*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "StringNotLike"
      variable = "aws:PrincipalArn"
      values = [
        aws_iam_role.registrar.arn,
        "arn:aws:iam::${local.account_id}:role/${local.p}-feed-*",
      ]
    }
  }

  dynamic "statement" {
    for_each = { for idx, f in local.s3_push_feeds : tostring(idx) => f }
    content {
      sid       = "S3PushPut${statement.key}"
      effect    = "Allow"
      actions   = ["s3:PutObject"]
      resources = ["${aws_s3_bucket.landing.arn}/${statement.value.feed_id}/incoming/*"]

      principals {
        type        = "AWS"
        identifiers = statement.value.connection.partner_principal_arns
      }
    }
  }

  dynamic "statement" {
    for_each = { for idx, f in local.s3_push_feeds : tostring(idx) => f }
    content {
      sid       = "S3PushList${statement.key}"
      effect    = "Allow"
      actions   = ["s3:ListBucket"]
      resources = [aws_s3_bucket.landing.arn]

      principals {
        type        = "AWS"
        identifiers = statement.value.connection.partner_principal_arns
      }

      condition {
        test     = "StringLike"
        variable = "s3:prefix"
        values   = ["${statement.value.feed_id}/incoming/*"]
      }
    }
  }
}

resource "aws_s3_bucket_policy" "landing" {
  bucket = aws_s3_bucket.landing.id
  policy = data.aws_iam_policy_document.landing.json
}

# --- lake: versioning off, Deny Delete except maintenance (D-9) ------------

data "aws_iam_policy_document" "lake" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  statement {
    sid       = "DenyDeleteExceptMaintenance"
    effect    = "Deny"
    actions   = ["s3:DeleteObject", "s3:DeleteObjectVersion"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "StringNotLike"
      variable = "aws:PrincipalArn"
      values   = [aws_iam_role.maintenance.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket = aws_s3_bucket.lake.id
  policy = data.aws_iam_policy_document.lake.json
}

# --- artifacts: TLS-only, athena-results/ 30 d expiry, feeds.json object ---

data "aws_iam_policy_document" "artifacts" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  policy = data.aws_iam_policy_document.artifacts.json
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 30
    }
  }
}

resource "aws_s3_object" "feeds_json" {
  bucket       = aws_s3_bucket.artifacts.id
  key          = "registry/feeds.json"
  content      = var.feeds_json
  content_type = "application/json"
  etag         = md5(var.feeds_json)
}
