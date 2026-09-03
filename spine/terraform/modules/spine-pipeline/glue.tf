# glue.tf -- the runner job itself, LLD S10.4. `--datalake-formats iceberg`
# lets Glue 5.0 pre-install the matching Iceberg runtime jar (no
# `spark.jars.packages` needed, unlike the local test substrate's own
# Maven-coordinate pull, `tests/conftest.py::_ICEBERG_PACKAGE` -- Glue 5.0
# ships it). `--additional-python-modules` + `--python-modules-installer-
# option "--no-index"` (I-23): the wheel is the sole source of `spine` +
# its pinned transitive deps; a job start never resolves from PyPI.
#
# Every `--conveyer-*` argument below is one of `spine/config.py::
# _ARGV_KEYS` this pipeline's job needs (verified against that file's
# literal key table): env, aws-region, catalog-kind, ledger-catalog-kind,
# spine-db, run-ledger-table, event-bus, landing-bucket, artifacts-bucket,
# pipeline-spec-uri, run-config, sla-minutes. NOT set here (deliberately):
# `--conveyer-warehouse-uri` / `--conveyer-ledger-sql-uri` (test-only,
# `RunnerConfig` marks both optional and this job runs `catalog_kind="glue"` /
# `ledger_catalog_kind="glue"`, neither of which reaches those branches);
# `--conveyer-delivery` / `--conveyer-sfn-retry-count` / `--conveyer-sfn-
# redrive-count` (SFN-injected PER EXECUTION via the state machine's Task
# `Arguments`, state_machine.tf -- a default job argument would be a
# constant, which is wrong for per-batch/per-retry values); `--conveyer-
# attempt-id` (I-5's fallback path: Glue injects `--JOB_RUN_ID`
# automatically, so no override is set here for the deployed job).

resource "aws_cloudwatch_log_group" "job" {
  # Explicit group + bounded retention (S-18 pattern, mirrors ingestion's
  # own log-group-per-function convention) -- an auto-created Glue
  # continuous-logging group never expires by default, and this job's logs
  # carry batch/feed/attempt identifiers per S11.2.
  name              = "/aws-glue/spine/${local.job_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_glue_job" "this" {
  name     = local.job_name
  role_arn = aws_iam_role.job.arn

  glue_version      = "5.0" # I-1: Spark 3.5.x, Python 3.11, Iceberg 1.6.1 runtime
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.sla_minutes # I-18: per-ATTEMPT budget

  execution_property {
    # C-1/E-4: the AWS default of 1 would serialize-and-strand parallel
    # same-feed batches, silently un-implementing D-12.
    max_concurrent_runs = var.max_concurrent_runs
  }

  command {
    name            = "glueetl"
    script_location = var.glue_entrypoint_script_uri # ambiguity 2, main.tf header
    python_version  = "3"
  }

  default_arguments = {
    "--additional-python-modules"       = var.spine_wheel_uri
    "--python-modules-installer-option" = "--no-index"
    "--datalake-formats"                = "iceberg"

    # T-16: Iceberg SQL extensions + spine_cat catalog conf, set here AND
    # in code (`glue_main.py::_catalog_conf`) -- see main.tf's
    # `local.iceberg_conf` comment for why the double-set is load-bearing,
    # not redundant.
    "--conf" = local.iceberg_conf

    "--conveyer-env"                 = var.env
    "--conveyer-aws-region"          = var.region
    "--conveyer-catalog-kind"        = "glue"
    "--conveyer-ledger-catalog-kind" = "glue"
    "--conveyer-spine-db"            = var.spine_database_name
    "--conveyer-run-ledger-table"    = var.run_ledger_table_name
    "--conveyer-event-bus"           = var.event_bus_name
    "--conveyer-landing-bucket"      = var.landing_bucket_name
    "--conveyer-artifacts-bucket"    = var.artifacts_bucket_name
    "--conveyer-pipeline-spec-uri"   = var.pipeline_spec_uri
    "--conveyer-run-config"          = var.run_config
    "--conveyer-sla-minutes"         = tostring(var.sla_minutes)

    # Continuous logging, on (H-5/S11.1's EMF-via-continuous-logging
    # dependency; M6 must assert extraction end-to-end, T-14 -- this
    # module wires the job-side half of that obligation).
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.job.name
  }
}
