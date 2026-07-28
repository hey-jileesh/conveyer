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

locals {
  source_files = fileset("${path.module}/../../../sources", "*/*/source.yaml")
  feeds = { for f in local.source_files :
    yamldecode(file("${path.module}/../../../sources/${f}")).feed_id
    => yamldecode(file("${path.module}/../../../sources/${f}"))
  }
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
}

module "feed" {
  source = "../../modules/feed"

  for_each = local.feeds

  feed                = each.value
  platform            = module.platform # object output: names, ARNs (S10.6 outputs)
  image_uri           = var.image_uri
  driver_bytes_budget = var.driver_bytes_budget
}
