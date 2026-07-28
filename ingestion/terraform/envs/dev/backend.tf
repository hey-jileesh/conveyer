# Local state by default (this scaffold). Before any shared/team apply,
# comment out the `local` block below and uncomment + fill in the `s3`
# block (bucket/table must already exist -- Terraform cannot bootstrap its
# own state backend). See the deployment runbook, LLD S10.8.

terraform {
  backend "local" {
    path = "terraform.tfstate"
  }

  # backend "s3" {
  #   bucket         = "conveyer-tfstate"
  #   key            = "ingestion/dev/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "conveyer-tfstate-lock"
  #   encrypt        = true
  # }
}
