# conveyer-ingestion

The Conveyer ingestion module: delivery ledger (append-only Iceberg),
registration core (CAS turnstile), `s3-push` and `sftp-pull` drivers, and
absence detection.

**The build spec is [design/002.1_data_ingestion_lld.md](../design/002.1_data_ingestion_lld.md)**
— every schema, signature, resource, and algorithm there is normative.
Implement in the milestone order of §13. Deployment runbook: LLD §10.8.

The full acceptance loop runs locally (no AWS account): injectable Iceberg
catalog (SQLite in tests, Glue in prod), moto for S3/DynamoDB/EventBridge/
Secrets Manager, in-memory SFTP record of functions. CI = `make lint schemas
registry test`.
