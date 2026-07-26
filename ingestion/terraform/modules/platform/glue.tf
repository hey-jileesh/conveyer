# glue.tf -- Glue Catalog database only (LLD S10.4 / D-7). The
# `delivery_ledger` table is bootstrap-owned (`ingestion/bootstrap/create_ledger.py`,
# `make bootstrap-ledger`) -- Terraform's Iceberg table support drifts on
# schema; a pyiceberg bootstrap is exact and re-runnable.

resource "aws_glue_catalog_database" "ingestion" {
  name = local.glue_database
}
