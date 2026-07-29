# s3.tf -- artifacts-bucket spine/* protection (I-23, LLD S10.2).
#
# See main.tf's header for why this file does NOT declare an
# `aws_s3_bucket_policy` resource: ingestion's platform module already owns
# the artifacts bucket's policy (TLS-only deny), and a bucket has exactly
# one. Versioning is a separate resource type ingestion does not declare for
# this bucket, so it is safe to own here.

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = var.artifacts_bucket_name
  versioning_configuration {
    status = "Enabled"
  }
}

# Deny PutObject/DeleteObject* under `spine/*` for everyone except the
# deploy principal -- protects the content-addressed wheel key
# (`spine/<git-sha>/…whl`) and pipeline specs (`spine/specs/<slug>/…yaml`)
# from being overwritten or removed post-publish (I-23's supply-chain
# argument: `glue:StartJobRun` on a spine job IS code execution as the job
# role, so a mutable artifact under this prefix is an RCE path).
#
# NOT applied as a resource here -- exposed as JSON only; see main.tf's
# header. The env root must merge this into ONE combined
# `aws_s3_bucket_policy` alongside ingestion's own artifacts TLS-deny
# document (e.g. via `source_policy_documents` on a new
# `data.aws_iam_policy_document`), then apply that single merged document.
data "aws_iam_policy_document" "artifacts_spine_protection" {
  statement {
    sid    = "DenySpinePrefixWriteExceptDeploy"
    effect = "Deny"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion",
    ]
    resources = ["${var.artifacts_bucket_arn}/spine/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "StringNotLike"
      variable = "aws:PrincipalArn"
      values   = [var.artifacts_deploy_principal_arn]
    }
  }
}
