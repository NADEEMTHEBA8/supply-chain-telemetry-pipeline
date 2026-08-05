output "s3_bucket_name" {
  description = "S3 bucket name hosting the Medallion Delta Lakehouse"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "kinesis_stream_name" {
  description = "Amazon Kinesis Data Stream name for telemetry ingestion"
  value       = "${var.project_name}-telemetry"
}

output "databricks_access_key_id" {
  description = "IAM Access Key ID for Databricks object store integration"
  value       = aws_iam_access_key.databricks_pipeline.id
  sensitive   = true
}

output "databricks_secret_access_key" {
  description = "IAM Secret Access Key for Databricks object store integration"
  value       = aws_iam_access_key.databricks_pipeline.secret
  sensitive   = true
}
