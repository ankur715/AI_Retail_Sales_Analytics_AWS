output "s3_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "redshift_copy_role_arn" {
  value = aws_iam_role.redshift_copy.arn
}

output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.this.endpoint[0].address
}

output "redshift_port" {
  value = aws_redshiftserverless_workgroup.this.endpoint[0].port
}

output "pipeline_access_key_id" {
  value = aws_iam_access_key.pipeline.id
}

output "pipeline_secret_access_key" {
  description = "terraform output -raw pipeline_secret_access_key | pipe into `aws configure set` -- never print it into a shared log."
  value       = aws_iam_access_key.pipeline.secret
  sensitive   = true
}
