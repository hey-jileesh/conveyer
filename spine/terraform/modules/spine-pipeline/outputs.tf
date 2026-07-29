# outputs.tf -- flat outputs (house style: modules/platform/outputs.tf,
# modules/spine-platform/outputs.tf), one per field, no wrapper object.

output "job_name" {
  value = aws_glue_job.this.name
}

output "job_arn" {
  value = aws_glue_job.this.arn
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.this.arn
}

output "job_role_arn" {
  value = aws_iam_role.job.arn
}
