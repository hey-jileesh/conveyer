# pipeline_slug_validation.tftest.hcl -- S5 grammar rejection, split into its
# OWN file (own implicit state) deliberately: `terraform test` run blocks
# within ONE file share sequential state by default, so a plan-only run
# placed after `spine_pipeline.tftest.hcl`'s apply-with-overrides run would
# try to REFRESH that run's (fake, override_resource-only) resources
# against real AWS and fail on network/credential errors that have nothing
# to do with the property under test here. Confirmed empirically before
# splitting.
#
# Every run block below needs the FULL override set (`override_data` for
# `data.aws_caller_identity.current` AND `override_resource` for every
# managed resource, including `aws_sfn_state_machine.this`) even though
# `command = plan` never creates anything: confirmed empirically that (a)
# Terraform evaluates data sources as part of building a plan regardless
# of a variable `validation` block's outcome, and (b) the AWS provider's
# `aws_sfn_state_machine` resource calls the real `sfn:
# ValidateStateMachineDefinition` API as a plan-time side effect -- a
# provider behavior distinct from resource creation, that `override_
# resource` (which intercepts the provider's plan RPC for that resource
# entirely, the same mechanism that lets `spine_pipeline.tftest.hcl`'s
# `command = apply` run avoid all real AWS calls) is required to suppress.

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
}

run "rejects_double_dash_within_segment" {
  command = plan

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:root"
      user_id    = "AIDAEXAMPLE"
    }
  }
  override_resource { target = aws_cloudwatch_log_group.job }
  override_resource { target = aws_iam_role.job }
  override_resource { target = aws_iam_role_policy.job }
  override_resource { target = aws_glue_job.this }
  override_resource { target = aws_sfn_state_machine.this }
  override_resource { target = aws_cloudwatch_metric_alarm.executions_failed }
  override_resource { target = aws_cloudwatch_metric_alarm.executions_timed_out }
  override_resource { target = aws_cloudwatch_metric_alarm.quarantine_rate }
  override_resource { target = aws_cloudwatch_metric_alarm.job_attempts }

  variables {
    pipeline = "pipelines/bad--name"
  }

  expect_failures = [
    var.pipeline,
  ]
}

run "rejects_underscore" {
  command = plan

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:root"
      user_id    = "AIDAEXAMPLE"
    }
  }
  override_resource { target = aws_cloudwatch_log_group.job }
  override_resource { target = aws_iam_role.job }
  override_resource { target = aws_iam_role_policy.job }
  override_resource { target = aws_glue_job.this }
  override_resource { target = aws_sfn_state_machine.this }
  override_resource { target = aws_cloudwatch_metric_alarm.executions_failed }
  override_resource { target = aws_cloudwatch_metric_alarm.executions_timed_out }
  override_resource { target = aws_cloudwatch_metric_alarm.quarantine_rate }
  override_resource { target = aws_cloudwatch_metric_alarm.job_attempts }

  variables {
    pipeline = "pipelines/bad_name"
  }

  expect_failures = [
    var.pipeline,
  ]
}

run "rejects_leading_dash" {
  command = plan

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:root"
      user_id    = "AIDAEXAMPLE"
    }
  }
  override_resource { target = aws_cloudwatch_log_group.job }
  override_resource { target = aws_iam_role.job }
  override_resource { target = aws_iam_role_policy.job }
  override_resource { target = aws_glue_job.this }
  override_resource { target = aws_sfn_state_machine.this }
  override_resource { target = aws_cloudwatch_metric_alarm.executions_failed }
  override_resource { target = aws_cloudwatch_metric_alarm.executions_timed_out }
  override_resource { target = aws_cloudwatch_metric_alarm.quarantine_rate }
  override_resource { target = aws_cloudwatch_metric_alarm.job_attempts }

  variables {
    pipeline = "pipelines/-bad"
  }

  expect_failures = [
    var.pipeline,
  ]
}

run "rejects_uppercase" {
  command = plan

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:root"
      user_id    = "AIDAEXAMPLE"
    }
  }
  override_resource { target = aws_cloudwatch_log_group.job }
  override_resource { target = aws_iam_role.job }
  override_resource { target = aws_iam_role_policy.job }
  override_resource { target = aws_glue_job.this }
  override_resource { target = aws_sfn_state_machine.this }
  override_resource { target = aws_cloudwatch_metric_alarm.executions_failed }
  override_resource { target = aws_cloudwatch_metric_alarm.executions_timed_out }
  override_resource { target = aws_cloudwatch_metric_alarm.quarantine_rate }
  override_resource { target = aws_cloudwatch_metric_alarm.job_attempts }

  variables {
    pipeline = "pipelines/Bad"
  }

  expect_failures = [
    var.pipeline,
  ]
}

run "accepts_the_exemplar_value" {
  command = plan

  override_data {
    target = data.aws_caller_identity.current
    values = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:root"
      user_id    = "AIDAEXAMPLE"
    }
  }
  override_resource { target = aws_cloudwatch_log_group.job }
  override_resource { target = aws_iam_role.job }
  override_resource { target = aws_iam_role_policy.job }
  override_resource { target = aws_glue_job.this }
  override_resource { target = aws_sfn_state_machine.this }
  override_resource { target = aws_cloudwatch_metric_alarm.executions_failed }
  override_resource { target = aws_cloudwatch_metric_alarm.executions_timed_out }
  override_resource { target = aws_cloudwatch_metric_alarm.quarantine_rate }
  override_resource { target = aws_cloudwatch_metric_alarm.job_attempts }

  variables {
    pipeline = "pipelines/identity"
  }
}
