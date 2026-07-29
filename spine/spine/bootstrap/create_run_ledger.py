"""Idempotent run-ledger Iceberg table creation (`make bootstrap-ledger`). LLD §6.5, I-17.

`create_run_ledger` is this module's one real function — the ONLY place
`catalog.create_table_if_not_exists` is called for the run ledger (mirrors
ingestion's `bootstrap/create_ledger.py::bootstrap_ledger`, 002.1 §6.2's
"Terraform creates the Glue *database*; the bootstrap script owns the
*table*" split, D-7). The Iceberg schema / partition spec / table properties
constants it builds the table from live in `effects/ledger.py` (imported,
not redeclared here) — that module's own docstring explains why one
authoritative definition matters: the append path's pyarrow conversion must
match the bootstrap-created shape exactly.

`main()` (the `make -C spine bootstrap-ledger ENV=...` /
`python -m spine.bootstrap.create_run_ledger --env ...` entrypoint) is NOT
covered by `tests/unit` — it talks to a real AWS Glue Catalog via
`effects.ledger.build_catalog`'s glue path, the same documented exclusion
shape as ingestion's `bootstrap/create_ledger.py::main` (§12.5).
`create_run_ledger` itself — the actual idempotent-creation logic — IS
tested directly against a `SqlCatalog` (`tests/unit/test_create_run_ledger.py`).

**Recorded gap, not silently patched**: `spine/Makefile`'s `bootstrap-ledger`
recipe currently invokes this module with only `--env $(ENV)` on the command
line, while `spine.config.from_args` (the one argv parser this package owns,
§6.4) expects the full per-attempt Glue argv contract (`pipeline_spec_uri`,
`delivery_json`, `sfn_retry_count`, …) that a one-shot bootstrap invocation
has no natural reason to supply. `main()` below calls `from_args` anyway —
matching the shape a real invocation needs once 009/010's Terraform wiring
passes bootstrap its own argv slice (a superset containing at least the
ledger/catalog fields `from_args` requires) — so `make bootstrap-ledger`
will fail fast today on a missing required key, which is correct behavior
for an under-specified argv, not a defect of this module. Editing the
Makefile recipe (out of this bead's file scope; also excluded by the task's
own instructions) or inventing a second, parallel env-var config reader
(diverging from this package's one argv-parsing convention, I-14) are both
avoided; flagged here for whichever bead wires 009/010's real bootstrap
invocation.
"""

from __future__ import annotations

import argparse
import sys

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from spine import observability
from spine.config import from_args
from spine.effects.ledger import (
    RUN_LEDGER_PARTITION_SPEC,
    RUN_LEDGER_SCHEMA,
    RUN_LEDGER_TABLE_PROPERTIES,
    build_catalog,
)


def create_run_ledger(catalog: Catalog, database: str, table_name: str) -> Table:
    """Idempotent (§6.5/I-17): safe to call repeatedly with no error and a
    stable schema — `create_table_if_not_exists` returns the existing table
    unchanged on every call after the first. Also idempotently ensures the
    namespace exists: Terraform creates the Glue database in production,
    but tests' `SqlCatalog` has no Terraform equivalent, so this call is the
    only thing that creates it there (a harmless no-op against an
    already-Terraform-created Glue database).
    """
    catalog.create_namespace_if_not_exists(database)
    return catalog.create_table_if_not_exists(
        identifier=f"{database}.{table_name}",
        schema=RUN_LEDGER_SCHEMA,
        partition_spec=RUN_LEDGER_PARTITION_SPEC,
        properties=RUN_LEDGER_TABLE_PROPERTIES,
    )


def main() -> None:
    # conveyer-nvh.47: installed first, before any logging (including
    # argparse/from_args failures) -- idempotent (`observability.
    # install_json_handler`'s own docstring), so a warm/repeated invocation
    # never accumulates duplicate handlers.
    observability.install_json_handler()

    parser = argparse.ArgumentParser(
        description="Idempotently create the spine run_ledger Iceberg table."
    )
    parser.add_argument("--env", required=True, help="Deploy environment (cross-checked below).")
    known_args, _unknown = parser.parse_known_args()

    argv = sys.argv[1:]
    config = from_args(argv)
    if known_args.env != config.env:
        raise SystemExit(f"--env {known_args.env!r} does not match parsed env {config.env!r}")

    catalog = build_catalog(config)
    table = create_run_ledger(catalog, config.spine_db, config.run_ledger_table)
    print(f"run_ledger ready at {table.location()}")  # noqa: T201 -- CLI status output


if __name__ == "__main__":
    main()
