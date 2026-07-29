# glue.tf -- the spine Glue Catalog database only (LLD S5, S10.2). The
# `run_ledger` table itself is bootstrap-owned (`make -C spine
# bootstrap-ledger`, S10.5) -- same D-7 rationale as ingestion's
# `delivery_ledger`: Terraform's Iceberg table support drifts on schema, a
# pyiceberg bootstrap script is exact and re-runnable.
#
# `location_uri` (conveyer-nvh.47): previously unset, so a new table created
# under this database with no location of its own falls back to whatever
# Glue's own catalog-level default warehouse is -- NOT guaranteed to be
# anywhere near `s3://${p}-lake/spine/run_ledger/`, the exact prefix
# `modules/spine-pipeline/iam.tf`'s job-role write grant AND `modules/
# platform/iam.tf`'s maintenance-Lambda delete grant are both scoped to
# (`"${var.lake_bucket_arn}/spine/run_ledger/*"`). Pinning the DATABASE's
# own `location_uri` to `s3://${p}-lake/spine/` means `bootstrap/
# create_run_ledger.py`'s `create_table_if_not_exists` (which passes no
# explicit table-level location) inherits a table location of `s3://${p}-
# lake/spine/run_ledger/` by Glue's own standard "table location defaults
# to `<database location>/<table name>/`" behavior -- landing squarely
# inside the granted prefix, with no change needed to the bootstrap script
# itself. `"${local.p}-lake"` (not a new module variable) reuses the exact
# same naming-convention string this module's own `run_ledger_s3_uri`
# output (outputs.tf) already derives -- `modules/platform/s3.tf` names the
# ingestion platform's lake bucket literally `"${local.p}-lake"`, and this
# module's `local.p` is computed identically (`"${var.name_prefix}-${var.
# env}"`, main.tf) from the SAME `name_prefix`/`env` the env root passes to
# both modules -- so no cross-module lookup or new bucket-name var is
# needed for a purely deterministic physical name (the same class of
# PREDICTED-not-read-back derivation `ingestion/terraform/envs/dev/main.tf`
# already uses for `local.identity_job_role_arn`).
resource "aws_glue_catalog_database" "spine" {
  name         = local.spine_glue_database
  location_uri = "s3://${local.p}-lake/spine/"
}
