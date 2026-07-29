# lambda.tf -- ECR repo, the three platform Lambda functions, and their
# EventBridge Scheduler schedules (LLD S10.5). Per-feed driver functions are
# `modules/feed`'s concern.

resource "aws_ecr_repository" "ingestion" {
  name                 = "${local.p}-ingestion"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "ingestion" {
  repository = aws_ecr_repository.ingestion.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep last 20 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 20
      }
      action = {
        type = "expire"
      }
    }]
  })
}

# --- log groups ------------------------------------------------------------
#
# SECURITY-GATE FIX (M-9-tf): declared explicitly with a bounded retention --
# left undeclared, Lambda auto-creates `/aws/lambda/<name>` on first
# invocation with retention NEVER EXPIRE, and these functions log delivery
# keys, filenames, ClaimItem reprs, and error strings (S11.1) into it
# indefinitely. Named by the static function-name string (not
# `aws_lambda_function.*.function_name`) to avoid a dependency cycle; each
# function `depends_on` its log group below so Lambda never races the
# auto-create against our own explicit creation.

resource "aws_cloudwatch_log_group" "registrar" {
  name              = "/aws/lambda/${local.p}-registrar"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "absence" {
  name              = "/aws/lambda/${local.p}-absence"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "maintenance" {
  name              = "/aws/lambda/${local.p}-maintenance"
  retention_in_days = var.log_retention_days
}

# --- functions -----------------------------------------------------------

resource "aws_lambda_function" "registrar" {
  function_name = "${local.p}-registrar"
  role          = aws_iam_role.registrar.arn
  package_type  = "Image"
  image_uri     = var.image_uri
  timeout       = 900
  memory_size   = 2048

  reserved_concurrent_executions = 10

  image_config {
    command = ["ingestion.entrypoints.registrar_s3.handler"]
  }

  environment {
    variables = local.base_env
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.dlq.arn
  }

  depends_on = [aws_cloudwatch_log_group.registrar]
}

resource "aws_lambda_function" "absence" {
  function_name = "${local.p}-absence"
  role          = aws_iam_role.absence.arn
  package_type  = "Image"
  image_uri     = var.image_uri
  timeout       = 900
  memory_size   = 512

  reserved_concurrent_executions = 1

  image_config {
    command = ["ingestion.entrypoints.absence.handler"]
  }

  environment {
    # `CONVEYER_NAME_PREFIX` -- absence/detector.py::_function_prefix reads
    # this directly (default "conveyer" if unset, matching var.name_prefix's
    # own default) to build the `${p}-registrar` / `${p}-driver-<slug>`
    # stuck-claim resume targets. Not part of `RuntimeConfig`/`base_env`.
    variables = merge(local.base_env, {
      CONVEYER_NAME_PREFIX = var.name_prefix
    })
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.dlq.arn
  }

  depends_on = [aws_cloudwatch_log_group.absence]
}

resource "aws_lambda_function" "maintenance" {
  function_name = "${local.p}-maintenance"
  role          = aws_iam_role.maintenance.arn
  package_type  = "Image"
  image_uri     = var.image_uri
  timeout       = 900
  memory_size   = 512

  reserved_concurrent_executions = 1

  image_config {
    command = ["ingestion.entrypoints.maintenance.handler"]
  }

  environment {
    # `CONVEYER_MAINTENANCE_TABLES` -- LLD 004.1 S12.6(3)/I-17 [E-7]: the
    # OPTIMIZE+VACUUM table list, additive to `RuntimeConfig`/`base_env`
    # (parsed by `ingestion.config._parse_maintenance_tables`; unset default
    # there resolves to the single ledger identifier this var itself always
    # includes).
    variables = merge(local.base_env, {
      CONVEYER_MAINTENANCE_TABLES = join(",", local.maintenance_tables)
    })
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.dlq.arn
  }

  depends_on = [aws_cloudwatch_log_group.maintenance]
}

# --- schedules -------------------------------------------------------------

resource "aws_scheduler_schedule" "absence" {
  name                         = "${local.p}-absence"
  schedule_expression          = "rate(1 hour)"
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 15
  }

  target {
    arn      = aws_lambda_function.absence.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts = 2
    }

    dead_letter_config {
      arn = aws_sqs_queue.dlq.arn
    }
  }
}

resource "aws_scheduler_schedule" "maintenance" {
  name                         = "${local.p}-maintenance"
  schedule_expression          = "cron(0 8 ? * SUN *)"
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 15
  }

  target {
    arn      = aws_lambda_function.maintenance.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts = 2
    }

    dead_letter_config {
      arn = aws_sqs_queue.dlq.arn
    }
  }
}
