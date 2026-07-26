output "feed_id" {
  description = "This instance's feed_id, echoed back for the env root's for_each key."
  value       = var.feed.feed_id
}

output "driver_function_name" {
  description = "sftp-pull driver Lambda function name; null for s3-push feeds, which have no per-feed compute (served entirely by the shared registrar)."
  value       = local.is_sftp_pull ? aws_lambda_function.driver[0].function_name : null
}

output "driver_function_arn" {
  description = "sftp-pull driver Lambda ARN; null for s3-push feeds."
  value       = local.is_sftp_pull ? aws_lambda_function.driver[0].arn : null
}

output "driver_role_arn" {
  description = "sftp-pull per-feed IAM role ARN (LLD S10.7 blast-radius wall); null for s3-push feeds."
  value       = local.is_sftp_pull ? aws_iam_role.feed[0].arn : null
}
