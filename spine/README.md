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
