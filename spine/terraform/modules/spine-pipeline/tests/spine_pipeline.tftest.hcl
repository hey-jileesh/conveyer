# spine_pipeline.tftest.hcl -- validates the module's normative LLD
# properties (S10.3 IAM table verbatim, S10.4 Glue job wiring, S8.2 state
# machine template, S11.4 alarm thresholds) against a REAL (non-mock)
# `provider "aws"` with `skip_credentials_validation` etc., `override_data`
# for the one genuine API call (`data.aws_caller_identity.current`), and
# `override_resource` for every other resource -- the exact mechanics
# `ingestion/terraform/modules/platform/tests/security_fixes.tftest.hcl`
# documents and this file reuses verbatim (a mocked provider substitutes
# garbage for data sources too, which breaks IAM-JSON-shape assertions;
# confirmed there, not re-derived here).

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
}

variables {
  name_prefix = "conveyer"
  env         = "test"
  region      = "us-east-1"

  pipeline = "pipelines/identity"

  landing_bucket_name   = "conveyer-test-landing"
  landing_bucket_arn    = "arn:aws:s3:::conveyer-test-landing"
  lake_bucket_name      = "conveyer-test-lake"
  lake_bucket_arn       = "arn:aws:s3:::conveyer-test-lake"
  artifacts_bucket_name = "conveyer-test-artifacts"
  artifacts_bucket_arn  = "arn:aws:s3:::conveyer-test-artifacts"

  event_bus_name = "conveyer-test-spine-bus"
  event_bus_arn  = "arn:aws:events:us-east-1:123456789012:event-bus/conveyer-test-bus"

  spine_database_name = "conveyer_test_spine"
  spine_sfn_role_arn  = "arn:aws:iam::123456789012:role/conveyer-test-spine-sfn"

  spine_wheel_uri            = "s3://conveyer-test-artifacts/spine/abc123/conveyer_spine-0.1.0-py3-none-any.whl"
  glue_entrypoint_script_uri = "s3://conveyer-test-artifacts/spine/abc123/glue_main.py"
  pipeline_spec_uri          = "s3://conveyer-test-artifacts/spine/specs/pipelines--identity/pipeline.yaml"

  sla_minutes         = 480
  max_concurrent_runs = 4

  landing_feed_prefixes = ["carrier-y/renewal-statements", "carrier-z/monthly-statements"]

  co_effect_tables = [
    {
      database  = "conveyer_test_lake"
      table     = "pipelines--other__state"
      s3_prefix = "tables/pipelines--other/state/"
    }
  ]
}

run "spine_pipeline" {
  command = apply

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:root"
      user_id    = "AIDAEXAMPLE"
    }
  }

  override_resource {
    target = aws_cloudwatch_log_group.job
    values = {
      name = "/aws-glue/spine/conveyer-test-spine-pipelines--identity"
      arn  = "arn:aws:logs:us-east-1:123456789012:log-group:/aws-glue/spine/conveyer-test-spine-pipelines--identity"
    }
  }
  override_resource {
    target = aws_iam_role.job
    values = {
      name = "conveyer-test-spine-pipelines--identity"
      arn  = "arn:aws:iam::123456789012:role/conveyer-test-spine-pipelines--identity"
    }
  }
  override_resource { target = aws_iam_role_policy.job }
  override_resource {
    target = aws_glue_job.this
    values = {
      name = "conveyer-test-spine-pipelines--identity"
      arn  = "arn:aws:glue:us-east-1:123456789012:job/conveyer-test-spine-pipelines--identity"
    }
  }
  override_resource {
    target = aws_sfn_state_machine.this
    values = {
      arn = "arn:aws:states:us-east-1:123456789012:stateMachine:conveyer-test-spine-pipelines--identity"
      id  = "arn:aws:states:us-east-1:123456789012:stateMachine:conveyer-test-spine-pipelines--identity"
    }
  }
  override_resource {
    target = aws_cloudwatch_metric_alarm.executions_failed
    values = { arn = "arn:aws:cloudwatch:us-east-1:123456789012:alarm:conveyer-test-spine-pipelines--identity-executions-failed" }
  }
  override_resource {
    target = aws_cloudwatch_metric_alarm.executions_timed_out
    values = { arn = "arn:aws:cloudwatch:us-east-1:123456789012:alarm:conveyer-test-spine-pipelines--identity-executions-timed-out" }
  }
  override_resource {
    target = aws_cloudwatch_metric_alarm.quarantine_rate
    values = { arn = "arn:aws:cloudwatch:us-east-1:123456789012:alarm:conveyer-test-spine-pipelines--identity-quarantine-rate" }
  }
  override_resource {
    target = aws_cloudwatch_metric_alarm.job_attempts
    values = { arn = "arn:aws:cloudwatch:us-east-1:123456789012:alarm:conveyer-test-spine-pipelines--identity-job-attempts" }
  }

  # --- naming (S5) ---------------------------------------------------------

  assert {
    condition     = aws_glue_job.this.name == "conveyer-test-spine-pipelines--identity"
    error_message = "S5: Glue job must be named $${p}-spine-<slug> with slug(pipeline) = pipeline with '/' -> '--'."
  }

  assert {
    condition     = aws_sfn_state_machine.this.name == aws_glue_job.this.name
    error_message = "S5: the state machine must share the EXACT SAME name string as the Glue job."
  }

  # --- IAM: trust policy confused-deputy guard (S-14) -----------------------

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.glue_assume.json).Statement :
      try(s.Condition.StringEquals["aws:SourceAccount"], null) == "123456789012" &&
      try(contains(flatten([s.Condition.ArnLike["aws:SourceArn"]]), "arn:aws:glue:us-east-1:123456789012:job/conveyer-test-spine-pipelines--identity"), false)
    ])
    error_message = "S-14: the job role's trust policy must carry both aws:SourceAccount and an aws:SourceArn scoped to this job's own ARN."
  }

  # --- IAM: append-only posture -- no delete anywhere (S10.3) ---------------

  assert {
    condition = alltrue([
      for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
      alltrue([for a in flatten([s.Action]) : !startswith(lower(a), "s3:delete") && a != "iam:PassRole"])
    ])
    error_message = "S10.3: no spine role holds any delete permission, and iam:PassRole is deliberately absent from every runtime role."
  }

  # --- IAM: no grant under ${p}-lake/ledger/* exists (I-21) -----------------

  assert {
    condition = alltrue([
      for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
      alltrue([for r in flatten([s.Resource]) : !can(regex("/ledger/", r))])
    ])
    error_message = "I-21: no grant under $${p}-lake/ledger/* may exist in the job role -- ingestion's ledger must stay unreachable even under a misconfigured spec."
  }

  # --- IAM: landing read scoped to routed feeds only, no vestibule (I-21) --

  assert {
    condition = alltrue([
      for prefix in var.landing_feed_prefixes :
      anytrue([
        for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
        contains(flatten([s.Resource]), "arn:aws:s3:::conveyer-test-landing/${prefix}/received_at=*") &&
        flatten([s.Action]) == ["s3:GetObject"]
      ])
    ])
    error_message = "I-21: a s3:GetObject-only statement must exist per routed feed prefix, scoped to its own received_at= tree."
  }

  assert {
    condition = alltrue([
      for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
      contains(flatten([s.Resource]), "arn:aws:s3:::conveyer-test-landing") ? !can(regex("incoming", jsonencode(s))) : true
    ])
    error_message = "I-21: the job role must never reference the incoming/ vestibule prefix -- no per-feed compute holds vestibule access."
  }

  # --- IAM: lake Get/Put/List, no Delete, own tables + run_ledger only ----

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
      contains(flatten([s.Resource]), "arn:aws:s3:::conveyer-test-lake/tables/pipelines--identity/*") &&
      contains(flatten([s.Resource]), "arn:aws:s3:::conveyer-test-lake/spine/run_ledger/*") &&
      length(setsubtract(flatten([s.Action]), ["s3:GetObject", "s3:PutObject"])) == 0
    ])
    error_message = "I-21: lake Get/Put (no Delete) must be scoped to exactly tables/<slug>/* and spine/run_ledger/*."
  }

  # --- IAM: artifacts read spine/* only (I-23) ------------------------------

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
      flatten([s.Resource]) == ["arn:aws:s3:::conveyer-test-artifacts/spine/*"] &&
      flatten([s.Action]) == ["s3:GetObject"]
    ])
    error_message = "I-23: the job role must read exactly $${p}-artifacts/spine/* and nothing else in that bucket."
  }

  # --- IAM: Glue catalog PER-TABLE only, never database-wide (I-21/S-5) ---

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
      contains(flatten([s.Action]), "glue:UpdateTable") &&
      contains(flatten([s.Resource]), "arn:aws:glue:us-east-1:123456789012:table/conveyer_test_lake/pipelines--identity__facts") &&
      !contains(flatten([s.Resource]), "arn:aws:glue:us-east-1:123456789012:database/conveyer_test_lake")
    ])
    error_message = "S-5: glue:UpdateTable must be scoped to this pipeline's own table ARNs (including __facts), never a bare database ARN."
  }

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
      flatten([s.Action]) == ["glue:GetDatabase"] &&
      contains(flatten([s.Resource]), "arn:aws:glue:us-east-1:123456789012:database/conveyer_test_lake") &&
      contains(flatten([s.Resource]), "arn:aws:glue:us-east-1:123456789012:database/conveyer_test_spine")
    ])
    error_message = "I-21: GetDatabase must be granted on exactly the lake db and the spine db, nothing else."
  }

  # --- IAM: run_ledger table reachable, per-table (I-21) --------------------

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
      contains(flatten([s.Resource]), "arn:aws:glue:us-east-1:123456789012:table/conveyer_test_spine/run_ledger")
    ])
    error_message = "I-21: the job role must reach its own run_ledger table entry."
  }

  # --- IAM: co-effect grants GENERATED from var.co_effect_tables (S-15) ----

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
      flatten([s.Action]) == ["glue:GetTable"] &&
      contains(flatten([s.Resource]), "arn:aws:glue:us-east-1:123456789012:table/conveyer_test_lake/pipelines--other__state")
    ])
    error_message = "S-15: a read-only glue:GetTable statement must be generated for each var.co_effect_tables entry."
  }

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
      flatten([s.Action]) == ["s3:GetObject"] &&
      contains(flatten([s.Resource]), "arn:aws:s3:::conveyer-test-lake/tables/pipelines--other/state/*")
    ])
    error_message = "S-15: a read-only s3:GetObject statement must be generated for each var.co_effect_tables entry's s3_prefix."
  }

  # --- IAM: events:PutEvents conditioned events:source=conveyer.spine (S-2) -

  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.job.json).Statement :
      contains(flatten([s.Action]), "events:PutEvents") &&
      try(s.Condition.StringEquals["events:source"], null) == "conveyer.spine"
    ])
    error_message = "S-2: events:PutEvents must be conditioned events:source = conveyer.spine."
  }

  # --- Glue job wiring (S10.4) -----------------------------------------------

  assert {
    condition     = aws_glue_job.this.glue_version == "5.0"
    error_message = "I-1: Glue job must pin glue_version 5.0."
  }

  assert {
    condition     = aws_glue_job.this.timeout == var.sla_minutes
    error_message = "I-18: Glue job Timeout must equal the per-attempt sla_minutes budget."
  }

  assert {
    condition     = aws_glue_job.this.execution_property[0].max_concurrent_runs == var.max_concurrent_runs
    error_message = "C-1/E-4: execution_property.max_concurrent_runs must be wired from the variable, never left at the AWS default of 1."
  }

  assert {
    condition     = aws_glue_job.this.command[0].name == "glueetl" && aws_glue_job.this.command[0].script_location == var.glue_entrypoint_script_uri
    error_message = "S10.4: the Glue job's command must be glueetl pointed at the deploy-pushed entrypoint script."
  }

  assert {
    condition = (
      aws_glue_job.this.default_arguments["--additional-python-modules"] == var.spine_wheel_uri &&
      aws_glue_job.this.default_arguments["--python-modules-installer-option"] == "--no-index" &&
      aws_glue_job.this.default_arguments["--datalake-formats"] == "iceberg"
    )
    error_message = "I-23: the wheel must be wired via --additional-python-modules with --no-index, and --datalake-formats=iceberg set."
  }

  assert {
    condition = (
      strcontains(aws_glue_job.this.default_arguments["--conf"], "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") &&
      strcontains(aws_glue_job.this.default_arguments["--conf"], "spark.sql.catalog.spine_cat=org.apache.iceberg.spark.SparkCatalog") &&
      strcontains(aws_glue_job.this.default_arguments["--conf"], "spark.sql.catalog.spine_cat.type=glue")
    )
    error_message = "T-16: the Iceberg SQL extensions + spine_cat catalog conf must be set in the Glue job's default arguments, mirroring glue_main.py::_catalog_conf verbatim."
  }

  assert {
    condition = alltrue([
      for k in [
        "--conveyer-env", "--conveyer-aws-region", "--conveyer-catalog-kind",
        "--conveyer-ledger-catalog-kind", "--conveyer-spine-db", "--conveyer-run-ledger-table",
        "--conveyer-event-bus", "--conveyer-landing-bucket", "--conveyer-pipeline-spec-uri",
        "--conveyer-run-config", "--conveyer-sla-minutes",
      ] :
      contains(keys(aws_glue_job.this.default_arguments), k)
    ])
    error_message = "S6.4/S10.4: every --conveyer-* default argument the entrypoint's from_args needs (excluding the SFN-injected/attempt-id ones) must be present."
  }

  assert {
    condition = (
      aws_glue_job.this.default_arguments["--enable-continuous-cloudwatch-log"] == "true" &&
      aws_glue_job.this.default_arguments["--continuous-log-logGroup"] == aws_cloudwatch_log_group.job.name
    )
    error_message = "S11.1: continuous logging must be enabled and pointed at this job's own explicit log group."
  }

  # --- state machine (S8.2, VERBATIM) -----------------------------------

  assert {
    condition     = aws_sfn_state_machine.this.type == "STANDARD"
    error_message = "S8.2: the state machine must be a Standard workflow."
  }

  assert {
    condition     = aws_sfn_state_machine.this.role_arn == var.spine_sfn_role_arn
    error_message = "S10.3: the state machine must assume the platform-shared spine-sfn role, not a role this module creates."
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.this.definition).TimeoutSeconds == (2 + 1) * var.sla_minutes * 60 + 900
    error_message = "I-18/T-2: SFN TimeoutSeconds must equal (MaxAttempts+1) * sla_minutes * 60 + 900."
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.this.definition).States.RunBatch.Resource == "arn:aws:states:::glue:startJobRun.sync"
    error_message = "S8.2: the single Task state must use glue:startJobRun.sync."
  }

  assert {
    condition = (
      jsondecode(aws_sfn_state_machine.this.definition).States.RunBatch.Retry[0].ErrorEquals == ["States.ALL"] &&
      jsondecode(aws_sfn_state_machine.this.definition).States.RunBatch.Retry[0].IntervalSeconds == 120 &&
      jsondecode(aws_sfn_state_machine.this.definition).States.RunBatch.Retry[0].MaxAttempts == 2 &&
      jsondecode(aws_sfn_state_machine.this.definition).States.RunBatch.Retry[0].BackoffRate == 2.0
    )
    error_message = "S8.2: the Retry block must be States.ALL x2, 120 s interval, 2.0 backoff -- VERBATIM."
  }

  assert {
    condition     = !can(jsondecode(aws_sfn_state_machine.this.definition).States.RunBatch.Catch)
    error_message = "S8.2/I-18: no Catch may exist -- retry exhaustion IS the alarm signal."
  }

  assert {
    condition = (
      jsondecode(aws_sfn_state_machine.this.definition).States.RunBatch.Parameters.Arguments["--conveyer-delivery.$"] == "States.JsonToString($)" &&
      jsondecode(aws_sfn_state_machine.this.definition).States.RunBatch.Parameters.Arguments["--conveyer-sfn-retry-count.$"] == "States.Format('{}', $$.State.RetryCount)" &&
      jsondecode(aws_sfn_state_machine.this.definition).States.RunBatch.Parameters.Arguments["--conveyer-sfn-redrive-count.$"] == "States.Format('{}', $$.Execution.RedriveCount)"
    )
    error_message = "S8.2: the Task's Arguments must pass --conveyer-delivery via States.JsonToString($) plus retry/redrive counts from the context object, VERBATIM."
  }

  # --- alarms (S11.4) ---------------------------------------------------

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.executions_failed.metric_name == "ExecutionsFailed" &&
      aws_cloudwatch_metric_alarm.executions_failed.threshold == 1 &&
      aws_cloudwatch_metric_alarm.executions_failed.period == 300
    )
    error_message = "S11.4: ExecutionsFailed >= 1, 5 m."
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.executions_timed_out.metric_name == "ExecutionsTimedOut" &&
      aws_cloudwatch_metric_alarm.executions_timed_out.threshold == 1 &&
      aws_cloudwatch_metric_alarm.executions_timed_out.period == 300
    )
    error_message = "S11.4: ExecutionsTimedOut >= 1, 5 m."
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.quarantine_rate.threshold == 0.05 &&
      aws_cloudwatch_metric_alarm.quarantine_rate.comparison_operator == "GreaterThanThreshold"
    )
    error_message = "S11.4: quarantine rate > 0.05 default, 1 h."
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.job_attempts.threshold == 10 &&
      aws_cloudwatch_metric_alarm.job_attempts.comparison_operator == "GreaterThanThreshold"
    )
    error_message = "S11.4: JobAttempts > 10, 1 h."
  }

  # --- outputs -----------------------------------------------------------

  assert {
    condition     = output.job_role_arn == aws_iam_role.job.arn
    error_message = "Flat outputs must include the job role ARN."
  }
}
