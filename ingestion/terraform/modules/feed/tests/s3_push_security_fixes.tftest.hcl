# Security-gate regression test -- feed half of M-6 (see
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

run "s3_push_security_fixes" {
  command = apply

  variables {
    feed = {
      feed_id  = "carrier-y/renewal-statements"
      driver   = "s3-push"
      pipeline = "pipelines/renewals"
      connection = {
        partner_principal_arns = ["arn:aws:iam::111111111111:role/carrier-y-uploader"]
      }
      expectation = {
        expected = "weekly:mon"
        by       = "09:00"
        timezone = "America/New_York"
      }
      completeness = {
        mode             = "manifest"
        manifest_pattern = "*.manifest.json"
      }
    }
  }

  override_resource {
    target = aws_cloudwatch_event_rule.s3_push[0]
    values = { name = "conveyer-test-s3-push-carrier-y--renewal-statements", arn = "arn:aws:events:us-east-1:123456789012:rule/conveyer-test-s3-push-carrier-y--renewal-statements" }
  }
  override_resource { target = aws_cloudwatch_event_target.s3_push_registrar[0] }
  override_resource { target = aws_lambda_permission.s3_push_registrar[0] }

  # --- M-6: EventBridge target carries dead_letter_config + retry_policy --
  assert {
    condition     = aws_cloudwatch_event_target.s3_push_registrar[0].dead_letter_config[0].arn == var.platform.dlq_arn
    error_message = "M-6: the s3-push EventBridge target must carry dead_letter_config pointing at the platform DLQ, or a throttled/failed delivery is silently dropped after 24 h with no ledger row, no DLQ entry, and no alarm."
  }
  assert {
    condition = (
      aws_cloudwatch_event_target.s3_push_registrar[0].retry_policy[0].maximum_event_age_in_seconds == 3600 &&
      aws_cloudwatch_event_target.s3_push_registrar[0].retry_policy[0].maximum_retry_attempts == 5
    )
    error_message = "M-6: the s3-push EventBridge target must carry retry_policy { maximum_event_age_in_seconds = 3600, maximum_retry_attempts = 5 }, matching every other target in this codebase."
  }
}
