# Conveyer

Large-volume batch data processing lane — an immutable-fact pipeline on Iceberg
that reuses the event lane's programming model (raw in, pure transforms,
append-only facts, derived current state) on a substrate priced for volume.

Architecture: [design/001_batch_data_processing_architecture.md](design/001_batch_data_processing_architecture.md)

## Modules

| Module | What | Design |
|---|---|---|
| [`ingestion/`](ingestion/) | Delivery ledger, registration, `s3-push` / `sftp-pull` drivers, absence detection | [design/002.1_data_ingestion_lld.md](design/002.1_data_ingestion_lld.md) |

Future modules (runner / stage sequence, pipeline packages) join as further
uv-workspace members.

## Development

uv workspace, Python 3.12. `make setup` (runs `uv sync`), then per-module
targets: `make lint schemas registry test` — that sequence green is the
definition of mergeable (LLD §12.6).
