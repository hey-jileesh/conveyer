# dynamodb.tf -- the CAS turnstile table (LLD S10.3 / S8.4). Nothing else --
# the pointer set stays tiny; every other attribute is schemaless.

resource "aws_dynamodb_table" "cas" {
  name         = "${local.p}-ingestion-cas"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  deletion_protection_enabled = true
}
