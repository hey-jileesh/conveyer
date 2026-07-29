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
