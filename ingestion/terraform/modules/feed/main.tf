# Per-feed resources -- LLD S10.7. `for_each`'d once per source.yaml by the
# env root (terraform/envs/dev/main.tf); each instance builds exactly the
# resources its `driver` needs (s3_push.tf / sftp_pull.tf, both count-gated
# below) plus the shared per-feed IAM role (iam.tf, sftp-pull only -- the
# s3-push path is served by the platform-owned registrar, no per-feed
# compute or role).

terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.60"
    }
  }
}

locals {
  # Common: slug = replace(feed_id, "/", "--") (LLD S10.7, S5).
  slug = replace(var.feed.feed_id, "/", "--")

  # feed_id is always "<source>/<feed>" (S5); Terraform-side validation is
  # YAML-well-formedness only (S6.1), so this split is trusted the same way
  # the rest of this module trusts var.feed's shape.
  feed_id_parts = split("/", var.feed.feed_id)
  source_name   = local.feed_id_parts[0]
  feed_name     = local.feed_id_parts[1]

  # "${p}" throughout the LLD == "${name_prefix}-${env}" (S5).
  p = "${var.platform.name_prefix}-${var.platform.env}"

  is_s3_push   = var.feed.driver == "s3-push"
  is_sftp_pull = var.feed.driver == "sftp-pull"

  # Vestibule prefix (s3-push) and canonical prefix (both drivers write
  # here; only sftp-pull's per-feed role needs it scoped in IAM) -- S5.
  incoming_prefix = "${local.source_name}/${local.feed_name}/incoming/"
  received_prefix = "${local.source_name}/${local.feed_name}/received_at="
}
