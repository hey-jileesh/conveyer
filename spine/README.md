# conveyer-spine

The Conveyer runner spine: `BatchContext`, the eight-stage sequence driver
(`land → pre_check → pull → apply → post_check → commit → fold → publish`),
`RunnerFx` effects, router Lambda + Step Functions + Glue entrypoint, the
Iceberg run ledger, and the identity exemplar pipeline proving it all end
to end, locally, with no AWS account.

**The build spec is [design/004.1_runner_spine_lld.md](../design/004.1_runner_spine_lld.md)**
— every schema, signature, resource, and algorithm there is normative.
Implement in the milestone order of §13. Deployment runbook: LLD §10.6.

CI = `make lint test` (root) / `make -C spine setup lint test` (this
package alone) — both run under Python 3.11 (`uv run -p 3.11 --package
conveyer-spine`, LLD I-1), matching Glue 5.0's engine pin even though the
workspace default is 3.12.

## Deployment runbook (LLD §10.6, M6)

D-1: spine has **no terraform env root of its own** — `modules/spine-
platform` and `modules/spine-pipeline` are consumed from the existing
ingestion env root (`ingestion/terraform/envs/dev`), which also owns the
two cross-module decisions this wiring needed (both documented at their
point of use, `ingestion/terraform/envs/dev/main.tf`):

- **Artifacts bucket-policy merge (I-23)**: `modules/spine-platform` only
  *outputs* its `spine/*` protection statement
  (`artifacts_spine_policy_document_json`) rather than owning its own
  `aws_s3_bucket_policy` — a bucket policy is one document per resource,
  and ingestion's `modules/platform` already owns
  `aws_s3_bucket_policy.artifacts`. The env root merges the two via a new
  `extra_artifacts_policy_statements_json` variable on
  `modules/platform` (`source_policy_documents`), keeping a single writer.
- **Operator role (§10.3, I-20)**: the env root creates `${p}-spine-
  operator` with `states:StartExecution`/`states:DescribeExecution` on the
  spine state machines and `glue:GetJobRun(s)` for liveness checks, but its
  trust policy is **no-assume-by-default** — the `aws:PrincipalArn`
  condition only matches ARNs in `var.spine_operator_principal_arns`
  (empty by default). The role exists and holds its grants from the first
  apply; wiring a real operator identity is a separate, later `-var`
  decision (document-and-defer, per this bead's own task framing).

`make -C spine plan|apply|bootstrap-ledger|smoke` all delegate into that
env root (`terraform -chdir=../ingestion/terraform/envs/$(ENV) ...`) — this
Makefile has no terraform of its own to run.

### Steps

```
1. make lint test                                # full local gate (spine + ingestion + contracts)
2. make -C spine package && make -C spine package-router
                                                   # `package-router` MUST run before any
                                                   # `plan`/`apply` below: modules/spine-platform's
                                                   # router Lambda resource evaluates
                                                   # filebase64sha256(var.spine_router_zip_path) at
                                                   # PLAN time, so the zip must already exist on disk
                                                   # ("build before plan").
3. make -C spine push-wheel ENV=dev               # -> s3://${p}-artifacts/spine/<git-sha>/…whl
                                                   # prints the pushed URI -- pass it as
                                                   # -var spine_wheel_uri=<printed URI> below.
4. terraform -chdir=ingestion/terraform/envs/dev apply \
     -var spine_wheel_uri=<pushed wheel URI, step 3> \
     -var glue_entrypoint_script_uri=<pushed launcher URI, see note below> \
     -var spine_pipeline_spec_uri=<pushed pipeline.yaml URI, see note below>
                                                   # platform + identity pipeline. Without the three
                                                   # -var overrides, apply succeeds against
                                                   # PLACEHOLDER deploy-time-artifact defaults
                                                   # (envs/dev/variables.tf) -- fine for a first
                                                   # `plan`/`validate` pass, but the Glue job will
                                                   # fail at run time until real objects exist at
                                                   # those URIs (glue_entrypoint_script_uri's launcher
                                                   # is a known, documented gap: no Makefile target
                                                   # pushes it yet -- modules/spine-pipeline's own
                                                   # "ambiguity 2", main.tf header -- so it must be
                                                   # uploaded by hand: a 3-line shim, `import sys;
                                                   # from spine.entrypoints.glue_main import main;
                                                   # main(sys.argv[1:])`, at
                                                   # s3://${p}-artifacts/spine/<git-sha>/glue_driver.py).
5. make -C spine bootstrap-ledger ENV=dev
5b. make -C spine bootstrap-admission ENV=dev SPEC=<pipeline spec URI>
                                                   # PER PIPELINE, once per deploy, after the wheel
                                                   # is pushed and before smoke (005.1 LLD §4.4/§11.3):
                                                   # idempotent, additive-only raw/quarantine Iceberg
                                                   # table creation. `SPEC` is that pipeline's own
                                                   # `pipeline.yaml` URI (s3:// in a real deploy,
                                                   # file:// for a local/dev dry run) -- run once per
                                                   # pipeline being deployed, not once per environment.
5c. make -C spine bootstrap-record-tables ENV=dev SPEC=<pipeline spec URI>
                                                   # PER PIPELINE, once per deploy, AFTER 5b and before
                                                   # smoke (007.1 LLD §6.5): idempotent, additive-only
                                                   # fact/state/marker Iceberg table creation, per the
                                                   # pipeline's `fact_types` mapping (006.1 P-1). Also
                                                   # emits/refreshes `table-classes.json` BESIDE the
                                                   # deployed spec (same `SPEC` directory, F-10) -- the
                                                   # bind-time authority `entrypoints/glue_main.py`
                                                   # loads for [DC-1]'s marker-table probe; re-run
                                                   # whenever `fact_types` changes shape, not only on a
                                                   # brand-new pipeline.
6. make -C spine smoke ENV=dev                    # via the identity FEED (ingestion front door,
                                                   # conveyer-internal/identity-smoke, §12.6); polls
                                                   # the run ledger for a publish/ok row, the facts
                                                   # table for that batch_id, and the Glue job's
                                                   # continuous-logging group for an EMF-marked line
                                                   # (T-14). Skips cleanly without --env/credentials
                                                   # (never fails a bare `pytest`/CI sweep); fails
                                                   # loudly (`make smoke` with no ENV=) with a clear
                                                   # message.
```

### Admission tables — bootstrap, promotion, and the dev-only N5 note (005.1 LLD §4.4/§11.3)

```
Per pipeline, at deploy (after push-wheel, before smoke):
  make -C spine bootstrap-admission ENV=dev SPEC=<pipeline spec URI>   # §4.4; idempotent; additive-only

Contract adds a column:
  edit contract (raw_contract.columns in pipeline.yaml) -> bootstrap-admission again
  -> the new column lands NATIVE on every row from the next batch on; history for
     that column stays inside `extras` on every row landed before the promotion.
     Author the coalescing view (an Athena view UNIONing pre/post-promotion reads,
     `extras['col']` for old rows, the native column for new ones) BY HAND at
     promotion time (D-11) -- this is an operator step, not something
     bootstrap-admission builds for you.

Dev-only migration (N5), identity exemplar only:
  the exemplar's raw/quarantine tables predate this LLD and carry a PROVISIONAL
  schema (the pre-005.1 9-column shape). Once the writers flip (n3-admission-cut),
  DROP those two tables and re-run bootstrap-admission to recreate them under the
  real §4.1/§4.2 shape, then re-run smoke. Dev-only disposable data -- a real
  pipeline's production tables never carry the provisional shape, so this step
  is never needed for one.
```

### B5 deploy-refresh checklist (006.1 LLD §14 B5)

Local half shipped by `conveyer-6pg.15`; executed by `conveyer-6pg.16`
(B5-gate) — human-supervised, LEAVE OPEN, mirrors the `conveyer-nvh.32`/
`conveyer-4ot.27`/N5 precedent above: no AWS mutation by any agent.

```
1. Spec redeploy -- no NEW authoring needed: `tests/exemplar/identity/
   pipeline.yaml` already carries the per-type `fact_types` shape (006.1
   P-1, migrated at B3/`conveyer-6pg.13`) -- push it to
   s3://${p}-artifacts/spine/specs/pipelines--identity/pipeline.yaml and
   `terraform apply -var spine_pipeline_spec_uri=<pushed URI>` (steps 3-4
   above); confirms the deployed Glue job's `--conveyer-pipeline-spec-uri`
   argv resolves the new shape, not a stale singular `fact_table`/
   `state_table` one.

2. Dev quarantine re-create (P-7), identity exemplar only -- DROP
   `<lake_db>.identity__quarantine` ONLY (not `identity__raw` -- P-7 never
   touches raw's shape) and re-run step 5b (`bootstrap-admission`) to
   recreate it. Why: P-7 adds one reserved key (`_conveyer_fact_type`) to
   every NEW quarantine row's `row_snapshot` JSON so `row_hash` can
   discriminate which fact type a quarantined row belongs to (§8.1's
   per-type rerun subtraction); rows quarantined by a pre-P-7 deploy lack
   that key. No DDL changes -- the tag lives inside the existing
   `row_snapshot` column's JSON value, never a new column, so
   `bootstrap-admission`'s own exact-schema assertion is untouched by this
   step; it is a DATA disposal, not a schema migration. P-7's own decision
   record (006.1 §2): "accepting ... that pre-tag durable quarantine rows
   in the dev exemplar are re-created rather than migrated (dev-disposable,
   §14)". Dev-only, same framing as N5 above -- a real pipeline's
   quarantine table is never pre-tag, so this step is never needed for one.

3. Record tables + inventory refresh -- step 5c above
   (`bootstrap-record-tables`), idempotent; re-run whenever `fact_types`
   changed shape since the last deploy (harmless no-op otherwise). Confirms
   `table-classes.json` (F-10) is content-current beside the redeployed
   spec -- the bind-time authority `entrypoints/glue_main.py` loads before
   ANY stage runs ([DC-1]'s marker-table probe).

4. `make -C spine smoke ENV=dev` (step 6 above) -- confirms the redeployed
   spec + refreshed table set round-trips one real batch end to end
   (publish/ok ledger row, a facts-table row, an EMF-marked log line).

5. Glue G-08 parity -- `make -C spine glue-parity` (Makefile target added
   this bead), run the SAME way against the deployed Glue job's own
   wheel/environment (a `spark-submit`/ad hoc job run against a live Glue
   5.0 cluster -- the probe needs no seed/delivery/catalog access of its
   own, `spine/probes/g08_parity.py`'s own docstring) rather than a
   laptop's local Spark. Expect `55/55 discriminator rows passed on this
   engine` in the job's continuous-logging output and exit code 0 -- a
   LOCAL rehearsal of the identical command already passed 55/55 under
   local Spark (originally 45/45, `conveyer-6pg.15`; extended to 55/55 with
   the §6.3 aggregate-position engine rows, `conveyer-swb.12`/A006-4); this
   step is what settles
   whether Glue 5.0's real JVM agrees (§13.1's own class of claim: "only a
   real account can confirm").

This bead (`conveyer-6pg.15`, B5-local) ships every artifact steps 1-5 need
— the already-migrated spec, this checklist's own instructions for step 2's
DROP (never the DROP itself), the `bootstrap-record-tables` step 5c wiring,
and the `glue-parity` probe/Makefile target — each validated locally end to
end (every command above runs clean against a local/dry-run substrate; only
the AWS-account-specific parts of steps 1-5 are deferred to B5-gate).
```

### Reader cost note: `multiline: true` parses on the driver (005.1 LLD §5.5, critique F3)

`_shape_multiline_object` (`effects/spark.py`) reads an object's ENTIRE body
into one driver-side string (`spark.read.text(uri, wholetext=True).collect()
[0]["value"]`) before handing it to `core.reading.multiline_records`'s
`csv.reader` — unlike the `multiline: false` per-line path (one Hadoop
split, `mapPartitions`-distributed across executors), this whole-object
read+parse happens entirely on the driver, in one JVM->Python round trip
per object. §5.5's own "whole-object memory cost" is therefore a DRIVER
memory/CPU cost under this implementation, not the "one task" a naive
reading might suggest — operationally: a `multiline: true` feed whose
individual objects can grow large can stall or OOM the driver even while
executors sit idle. Size `spark.driver.memory` for the largest single
object any `multiline: true` feed can land, and prefer `multiline: false`
for a feed whose per-object size is unbounded.

### Interim quarantine escalation: `spine-quarantine-reasons-30d` (005.1 LLD §10)

Until 012's remediation queue exists, the per-pipeline quarantine-rate alarm
(004.1 §11.4) is the *run*-level escalation path and this Athena named query
is the *reason*-level one: `reason_code` counts by (pipeline, feed_id,
check_stage, day) over the trailing 30 days, run ad hoc or saved as a named
query in the Athena workgroup. Quarantine carries no `pipeline` column by
design (005 D-7: the schema is pipeline-independent) — each pipeline has its
own `<db>.<pipeline>__quarantine` table (§4.2/§5), so the query supplies the
pipeline name as a literal per `UNION ALL` branch. This is documentation
only — no Terraform changes (§11.1); add a branch by hand whenever
`bootstrap-admission` creates a new pipeline's quarantine table.

```sql
-- spine-quarantine-reasons-30d -- interim escalation surface until 012's
-- queue (005.1 LLD §10/§8.3). One UNION branch per bootstrapped pipeline.
SELECT
  pipeline,
  feed_id,
  check_stage,
  date(quarantined_at) AS day,
  reason_code,
  count(*)             AS reason_count
FROM (
  SELECT 'identity' AS pipeline, feed_id, check_stage, quarantined_at, reason_code
  FROM conveyer_dev_lake.identity__quarantine
  -- UNION ALL
  -- SELECT '<pipeline>' AS pipeline, feed_id, check_stage, quarantined_at, reason_code
  -- FROM <db>.<pipeline>__quarantine
)
WHERE quarantined_at >= date_add('day', -30, current_date)
GROUP BY pipeline, feed_id, check_stage, date(quarantined_at), reason_code
ORDER BY day DESC, pipeline, feed_id, check_stage, reason_count DESC;
```

### Deliberate rerun (pick one) — I-20's governed escape hatch

```
redrive:   aws stepfunctions redrive-execution --execution-arn <execution arn>
           (eligible ~14 days after close only [T-21] -- after that, use --rN)

fresh run: PRECONDITION (I-20) -- BOTH of the following, checked in order:
             1. aws stepfunctions describe-execution --execution-arn <arn>
                shows a TERMINAL status (not RUNNING)
             2. aws glue get-job-runs --job-name ${p}-spine-<slug>
                shows NO job run in state RUNNING/STARTING/STOPPING for this batch_id
           Only once both hold:
             aws stepfunctions start-execution \
               --state-machine-arn <arn> \
               --name "<batch_id>--r1" \
               --input "$(cat detail.json)"
           Startable only by the named operator role (§10.3) -- no one else holds
           states:StartExecution on the spine state machines besides the router.
```

**Note [T-22]**: a failed/killed Spark write logs S3 `AccessDenied` errors
during its own abort-cleanup attempt — **expected noise** under the
append-only bucket policy (no spine role holds `s3:DeleteObject`, §10.3);
orphaned files are never visible to any snapshot and are swept by 008's
maintenance design (§15.2). Do not chase these as a real failure signal.

### Out-of-band rebuild (interim) — I-20's governed escape hatch (007.1 §9)

Full rebuild recomputes a pipeline's state tables from ALL committed facts
through the SAME per-type fold (`core.merge.merge_spec` +
`frames.fold.reduce_batch_winners`, §8.2's normative plan — bit-for-bit the
plan `stages/fold.py` runs per batch), then atomically swaps each result
in via an Iceberg conditional overwrite (`effects.rebuild.swap_with_retry`
— `validate-from-snapshot-id` + `isolation-level=serializable`, BOTH
options always, §9.2). It is a separate run mode, invoked directly —
**there is no `--force` flag anywhere in this path, by construction
(RB-2)**: a refused swap re-pins facts and recomputes, retrying up to a
budget before failing loudly (`TransientError`, D-1's ordinary job-
failure channel); the write is never issued past a refusal.

```
spine/spine/entrypoints/rebuild_main.py::main(argv) — a SEPARATE entrypoint
from the Glue job's own glue_main.py (that composition hard-requires a
seed/delivery event a rebuild invocation does not have). Reuses glue_main's
own I-23 spec-URI allowlist and file:// / s3:// spec fetch (imported, not
duplicated).

Required argv (Terraform/operator-supplied, same --conveyer-<kebab> shape
glue_main's own job args use):
  --conveyer-pipeline               <pipeline slug, e.g. "pipelines/identity">
  --conveyer-pipeline-spec-uri      <s3:// or file:// URI under .../spine/specs/<slug>/…>
  --conveyer-artifacts-bucket       <pinned bucket name; s3:// pipeline-spec-uri must be in it (6pg.35 item 4)>
  --conveyer-env                    <env name>
  --conveyer-aws-region             <region>
  --conveyer-catalog-kind           glue | hadoop
  --conveyer-warehouse-uri          <hadoop only; omit for glue>
  --conveyer-ledger-catalog-kind    glue | sql
  --conveyer-ledger-sql-uri         <sql only; omit for glue>
  --conveyer-spine-db               <ledger's Glue/SQL database name>
  --conveyer-run-ledger-table       <run-ledger table name>

A never-folded (virgin) state table converges too (M4, bead conveyer-
swb.25): `rebuild_state_table` genesis-seeds it first, pushing an empty
reduce of the fact table's own schema through the SAME `effects/spark.py::
build_merge` closure production `RunnerFx.merge` uses (no second `MERGE
INTO`/`.overwrite(` call site) — a zero-row MERGE still commits a real
first Iceberg snapshot on this runtime, establishing the lineage §9.2's
swap needs, then the normal swap proceeds. Bootstrap-record-tables' own
DDL creation alone still carries no snapshot on its own; this run mode's
own FIRST invocation against a bootstrapped-but-never-folded table is what
now supplies one, in the SAME call, rather than requiring a prior ordinary
fold or a hand-run genesis step first.

Observability today (the interim, until 004.1's rebuild stage-vocabulary
accretion lands, §16): one `stage="rebuild"` run-ledger row per swap
attempt (`outcome="ok"|"failed"`, `error_type` the OBSERVED wrapped Iceberg
exception class on a refusal) and a `RebuildSwapRetries` EMF metric per
refused attempt, dimensioned by `pipeline` and `state_table`.
```

**The manual re-materialization step — discipline, and says so (§9.3).**
`rebuild_main.main`'s successful return means every declared fact type's
state table now reflects `fold(all facts)` — nothing more. No
`RebuildCompletedV1` event exists yet (004.1's own proposed contract,
`effects/rebuild.py`'s module docstring), so **this run mode announces
nothing**: any downstream materialization outside spine's own state tables
(in particular `domainDB`, or any other consumer that reads spine's state
tables directly rather than reacting to an event) must be re-triggered BY
HAND after a successful out-of-band rebuild — re-run whatever job/query
populates it, the same way you would after any other out-of-band state
change. A kill between a successful swap and this manual step leaves state
already correct (§11's K-27 kill-matrix row) — only the announcement is
stale, closed by re-running this step (or `rebuild_main.main` again:
idempotent by content, §9.5).
