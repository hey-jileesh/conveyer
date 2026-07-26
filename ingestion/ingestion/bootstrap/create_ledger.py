"""Idempotent ledger table creation (make bootstrap-ledger) -- LLD §6.2.

`bootstrap_ledger` is this module's one real function -- the ONLY place
`catalog.create_table_if_not_exists` is called (§6.2: "Terraform creates the
Glue *database*; the bootstrap script owns the *table*", D-7). The Iceberg
schema/partition-spec/table-properties constants it builds the table from
live in `effects/ledger.py` (imported, not redeclared here) because
`effects/ledger.py::make_ledger_fx`'s `append` needs the identical shape --
see that module's docstring for why one authoritative definition matters.

`main()` (the `make bootstrap-ledger` / `python -m ingestion.bootstrap.create_ledger
--env ...` entrypoint) is NOT covered by `tests/integration` -- it talks to a
real AWS Glue Catalog via `effects.ledger.build_catalog`'s glue path, which
is untestable under moto (`moto[glue]` is not an installed test dependency;
same documented-exclusion shape as `effects/sftp.py`, §12.5). `bootstrap_ledger`
itself -- the actual logic -- IS tested: `tests/conftest.py`'s `local_effects`
fixture builds a `SqlCatalog` and calls this function directly, and
`tests/integration/test_bootstrap.py` asserts idempotence against it.
"""

from __future__ import annotations

import argparse

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from ingestion.config import from_env
from ingestion.effects.ledger import (
    LEDGER_ICEBERG_SCHEMA,
    LEDGER_PARTITION_SPEC,
    LEDGER_TABLE_PROPERTIES,
    LedgerConfig,
    build_catalog,
)


def bootstrap_ledger(catalog: Catalog, database: str, table_name: str) -> Table:
    """Idempotent (LLD §6.2/§9.4): safe to call repeatedly with no error and
    a stable schema -- `catalog.create_table_if_not_exists` returns the
    existing table unchanged on every call after the first. Also
    idempotently ensures the namespace exists: Terraform creates the Glue
    database in production, but tests' `SqlCatalog` has no Terraform
    equivalent, so this call is the only thing that creates it there (a
    harmless no-op against an already-Terraform-created Glue database).
    """
    catalog.create_namespace_if_not_exists(database)
    return catalog.create_table_if_not_exists(
        identifier=f"{database}.{table_name}",
        schema=LEDGER_ICEBERG_SCHEMA,
        partition_spec=LEDGER_PARTITION_SPEC,
        properties=LEDGER_TABLE_PROPERTIES,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Idempotently create the delivery_ledger Iceberg table."
    )
    parser.add_argument(
        "--env", required=True, help="Deploy environment (must match CONVEYER_ENV)."
    )
    args = parser.parse_args()

    config = from_env()
    if args.env != config.env:
        raise SystemExit(f"--env {args.env!r} does not match CONVEYER_ENV={config.env!r}")

    ledger_config = LedgerConfig(
        catalog_kind="glue",
        glue_database=config.glue_database,
        table_name=config.ledger_table,
        warehouse_uri=f"s3://{config.lake_bucket}/ledger/",
    )
    catalog = build_catalog(ledger_config)
    table = bootstrap_ledger(catalog, config.glue_database, config.ledger_table)
    print(f"delivery_ledger ready at {table.location()}")  # noqa: T201 -- CLI status output


if __name__ == "__main__":
    main()
