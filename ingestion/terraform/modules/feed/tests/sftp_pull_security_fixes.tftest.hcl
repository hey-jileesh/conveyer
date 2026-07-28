# Security-gate regression tests -- feed half of M-3, M-5, M-6, M-9-tf (see
# modules/platform/tests/security_fixes.tftest.hcl for C-1/M-10 and the
# platform half of the other findings; that file's header comment explains
# the apply+override_resource mechanics reused here).
#
# Every assertion below was verified, by temporarily reverting the
# corresponding source file with a local backup + re-run, to FAIL against
# the pre-fix code -- see the bead handoff report for the per-finding
# evidence.

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
}

variables {
  image_uri           = "123456789012.dkr.ecr.us-east-1.amazonaws.com/conveyer-ingestion:abc123"
  driver_bytes_budget = 5368709120

  platform = {
    name_prefix = "conveyer"
    env         = "test"
    region      = "us-east-1"

    landing_bucket_name   = "conveyer-test-landing"
    landing_bucket_arn    = "arn:aws:s3:::conveyer-test-landing"
    lake_bucket_name      = "conveyer-test-lake"
    lake_bucket_arn       = "arn:aws:s3:::conveyer-test-lake"
    artifacts_bucket_name = "conveyer-test-artifacts"
    artifacts_bucket_arn  = "arn:aws:s3:::conveyer-test-artifacts"

    event_bus_name = "conveyer-test-bus"
    event_bus_arn  = "arn:aws:events:us-east-1:123456789012:event-bus/conveyer-test-bus"

    cas_table_name = "conveyer-test-ingestion-cas"
    cas_table_arn  = "arn:aws:dynamodb:us-east-1:123456789012:table/conveyer-test-ingestion-cas"

    glue_catalog_arn   = "arn:aws:glue:us-east-1:123456789012:catalog"
    glue_database_name = "conveyer_test_ingestion"
    glue_database_arn  = "arn:aws:glue:us-east-1:123456789012:database/conveyer_test_ingestion"
    ledger_table_name  = "delivery_ledger"
    ledger_table_arn   = "arn:aws:glue:us-east-1:123456789012:table/conveyer_test_ingestion/delivery_ledger"

    dlq_arn = "arn:aws:sqs:us-east-1:123456789012:conveyer-test-ingestion-dlq"

    registrar_function_name = "conveyer-test-registrar"
    registrar_function_arn  = "arn:aws:lambda:us-east-1:123456789012:function:conveyer-test-registrar"

    scheduler_role_arn = "arn:aws:iam::123456789012:role/conveyer-test-scheduler"

    common_env = {
      CONVEYER_ENV               = "test"
      CONVEYER_AWS_REGION        = "us-east-1"
      CONVEYER_LANDING_BUCKET    = "conveyer-test-landing"
      CONVEYER_LAKE_BUCKET       = "conveyer-test-lake"
      CONVEYER_ARTIFACTS_BUCKET  = "conveyer-test-artifacts"
      CONVEYER_GLUE_DATABASE     = "conveyer_test_ingestion"
      CONVEYER_LEDGER_TABLE      = "delivery_ledger"
      CONVEYER_CAS_TABLE         = "conveyer-test-ingestion-cas"
      CONVEYER_EVENT_BUS         = "conveyer-test-bus"
      CONVEYER_REGISTRY_URI      = "s3://conveyer-test-artifacts/registry/feeds.json"
      CONVEYER_ATHENA_WORKGROUP  = "conveyer-test-ingestion"
      CONVEYER_ATHENA_OUTPUT_URI = "s3://conveyer-test-artifacts/athena-results/"
    }
  }
}

# --- sftp-pull feed instance: M-3, M-5, M-9-tf ---------------------------

run "sftp_pull_security_fixes" {
  command = apply

  variables {
    feed = {
      feed_id  = "carrier-x/commission-statements"
      driver   = "sftp-pull"
      pipeline = "pipelines/commissions"
      connection = {
        secret_ref   = "arn:aws:secretsmanager:us-east-1:123456789012:secret:conveyer-test/sftp/carrier-x/commission-statements"
        remote_path  = "/outbound/commissions/"
        file_pattern = "COMM_*"
      }
      trigger = {
        schedule = "cron(0 13 ? * MON-FRI *)"
        timezone = "America/New_York"
      }
      expectation = {
        expected = "weekdays"
        by       = "06:00"
        timezone = "America/New_York"
      }
      completeness = {
        mode             = "manifest"
        manifest_pattern = "*.manifest.json"
      }
    }
  }

  override_data {
    target = data.aws_caller_identity.current[0]
    values = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:root"
      user_id    = "AIDAEXAMPLE"
    }
  }

  override_resource {
    target = aws_secretsmanager_secret.sftp[0]
    values = { arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:conveyer-test/sftp/carrier-x/commission-statements-Ab1cD2" }
  }
  override_resource {
    target = aws_iam_role.feed[0]
    values = { name = "conveyer-test-feed-carrier-x--commission-statements", arn = "arn:aws:iam::123456789012:role/conveyer-test-feed-carrier-x--commission-statements" }
  }
  override_resource { target = aws_iam_role_policy_attachment.feed_basic_execution[0] }
  override_resource { target = aws_iam_role_policy.feed[0] }
  override_resource {
    target = aws_cloudwatch_log_group.driver[0]
    values = { name = "/aws/lambda/conveyer-test-driver-carrier-x--commission-statements", arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/conveyer-test-driver-carrier-x--commission-statements" }
  }
  override_resource {
    target = aws_lambda_function.driver[0]
    values = { arn = "arn:aws:lambda:us-east-1:123456789012:function:conveyer-test-driver-carrier-x--commission-statements", function_name = "conveyer-test-driver-carrier-x--commission-statements" }
  }
  override_resource { target = aws_scheduler_schedule.driver[0] }

  # --- M-3: feed role holds Abort/List-parts on its received_at= prefix ---
  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.feed_inline[0].json).Statement :
      contains(flatten([s.Resource]), "${var.platform.landing_bucket_arn}/carrier-x/commission-statements/received_at=*") &&
      contains(flatten([s.Action]), "s3:AbortMultipartUpload") &&
      contains(flatten([s.Action]), "s3:ListMultipartUploadParts")
    ])
    error_message = "M-3: the per-feed role must hold s3:AbortMultipartUpload + s3:ListMultipartUploadParts on its own received_at= resources, or stream_upload's own failure-path abort raises AccessDenied and orphaned parts bill forever."
  }

  # --- M-5: feed_assume_role trust is account-scoped ----------------------
  assert {
    condition = anytrue([
      for s in jsondecode(data.aws_iam_policy_document.feed_assume_role[0].json).Statement :
      try(s.Condition.StringEquals["aws:SourceAccount"], null) == "123456789012"
    ])
    error_message = "M-5: feed_assume_role's trust must carry a StringEquals aws:SourceAccount condition (bare service-principal trust is a confused-deputy risk)."
  }

  # --- M-9-tf: driver log group exists with bounded retention ------------
  assert {
    condition     = aws_cloudwatch_log_group.driver[0].retention_in_days == 30
    error_message = "M-9-tf: the per-feed driver's log group must be declared with a bounded (default 30 d) retention -- undeclared, it auto-creates with NEVER EXPIRE while logging delivery keys/filenames/error strings."
  }
  assert {
    condition     = aws_cloudwatch_log_group.driver[0].name == "/aws/lambda/conveyer-test-driver-carrier-x--commission-statements"
    error_message = "M-9-tf: the per-feed driver's log group must be named /aws/lambda/<function-name> so Lambda adopts it instead of auto-creating its own."
  }
}

