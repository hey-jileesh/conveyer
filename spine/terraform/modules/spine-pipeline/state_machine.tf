# state_machine.tf -- LLD S8.2's template, VERBATIM: Standard workflow, one
# Task state (`glue:startJobRun.sync`), whole-job Retry (States.ALL x2,
# 120 s interval, 2.0 backoff), NO Catch (retry exhaustion IS the alarm
# signal -- I-18; a Catch here would swallow the failed-execution signal
# `ExecutionsFailed`/`ExecutionsTimedOut` depend on). Uses the platform-
# shared `spine-sfn` role (`var.spine_sfn_role_arn`), never a role this
# module creates -- S10.3 keeps the SFN role platform-level while the JOB
# role (iam.tf) is per-pipeline.

locals {
  state_machine_definition = {
    Comment        = "Runner spine per-pipeline state machine (LLD 004.1 S8.2) for pipeline ${var.pipeline}."
    StartAt        = "RunBatch"
    TimeoutSeconds = local.sfn_timeout_seconds

    States = {
      RunBatch = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"

        Parameters = {
          JobName = local.job_name

          Arguments = {
            # The whole (allowlisted, router-forwarded) execution input is
            # passed through verbatim as a JSON string -- S8.2 verbatim.
            "--conveyer-delivery.$"          = "States.JsonToString($)"
            "--conveyer-sfn-retry-count.$"   = "States.Format('{}', $$.State.RetryCount)"
            "--conveyer-sfn-redrive-count.$" = "States.Format('{}', $$.Execution.RedriveCount)"
          }
        }

        Retry = [
          {
            ErrorEquals     = ["States.ALL"]
            IntervalSeconds = 120
            MaxAttempts     = local.sfn_max_attempts
            BackoffRate     = 2.0
          }
        ]

        End = true
      }
    }
  }
}

resource "aws_sfn_state_machine" "this" {
  name     = local.job_name
  role_arn = var.spine_sfn_role_arn
  type     = "STANDARD"

  definition = jsonencode(local.state_machine_definition)

  # Not a functional requirement (JobName above is a name-convention
  # string match, not a live resource reference) -- ordering the Glue job
  # into existence first keeps a first-ever `terraform apply` from racing
  # a start_execution against a not-yet-created job.
  depends_on = [aws_glue_job.this]
}
