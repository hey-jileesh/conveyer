# lambda.tf -- the router Lambda (LLD S8.1, S10.2). Zip package (stdlib +
# boto3 only, I-8), built by `make -C spine package-router`; this module
# only consumes the built artifact's path.

# SECURITY-GATE pattern (mirrors ingestion M-9-tf): declared explicitly with
# a bounded retention [S-18] -- left undeclared, Lambda auto-creates
# `/aws/lambda/<name>` with retention NEVER EXPIRE. Named by the static
# function-name string (not `aws_lambda_function.router.function_name`) to
# avoid a dependency cycle; the function `depends_on` this log group so
# Lambda never races its own auto-create against ours.
resource "aws_cloudwatch_log_group" "router" {
  name              = "/aws/lambda/${local.p}-spine-router"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "router" {
  function_name = "${local.p}-spine-router"
  role          = aws_iam_role.spine_router.arn

  filename         = var.router_zip_path
  source_code_hash = filebase64sha256(var.router_zip_path)

  # `make -C spine package-router` preserves the `spine/` package layout
  # inside the zip (spine/entrypoints/router.py + spine/core/naming.py +
  # the __init__.py's needed to import them) -- the handler path matches
  # that layout, not a flattened one.
  handler = "spine.entrypoints.router.handler"
  runtime = "python3.11"

  memory_size = 512
  timeout     = 30

  reserved_concurrent_executions = 10

  environment {
    variables = {
      # Everything up to and including the trailing "-spine-"; the router
      # appends `slug(pipeline)` itself (router.py docstring, S5) -- handing
      # it one fully-formed prefix means it never re-derives `${p}` and
      # can't drift from this module's own naming convention.
      CONVEYER_SFN_ARN_PREFIX = local.spine_sfn_arn_prefix

      CONVEYER_ARGV_BUDGET_BYTES = tostring(var.argv_budget_bytes)
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.spine_dlq.arn
  }

  depends_on = [aws_cloudwatch_log_group.router]
}

resource "aws_lambda_permission" "eventbridge_invoke_router" {
  statement_id  = "AllowEventBridgeInvokeRouter"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.router.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.delivery_registered.arn
}
