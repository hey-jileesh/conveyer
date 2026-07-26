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

## Deploy runbook (LLD §10.8 — order is load-bearing)

```
1. make lint test                                    # full local gate
2. terraform -chdir=terraform/envs/dev init
3. terraform ... apply -target=module.platform.aws_ecr_repository.ingestion
4. make push-image ENV=dev                           # docker build + push, tag = git SHA
5. terraform ... apply -var image_uri=<ecr>:<sha>    # everything else
6. make bootstrap-ledger ENV=dev                     # idempotent table creation
7. make put-secret FEED=carrier-x/commission-statements ENV=dev   # operator pastes JSON §6.7
8. smoke: make smoke ENV=dev                         # §12.5
```

`make bootstrap-ledger`, `make put-secret`, and `make smoke` (steps 6–8) all
call `ingestion.config.from_env()` — the same `CONVEYER_*` runtime config a
deployed Lambda reads (§7.2). Export that env-var set into your shell
(values from `terraform output` after step 5) before running steps 6–8.

`make smoke ENV=dev` is real-AWS only, never part of `make test`, and safe
to re-run against the same environment (each run mints a fresh delivery).
