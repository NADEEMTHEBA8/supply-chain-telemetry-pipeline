terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "us-east-1"
}

variable "project_name" {
  default = "te-supply-chain-telemetry"
}

# -----------------------------------------------------------------------------
# S3 — Delta Lakehouse Storage (Bronze / Silver / Gold)
# All Databricks Delta tables land here. Replaces the self-hosted MinIO
# instance in the original fraud pipeline.
# -----------------------------------------------------------------------------
resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.project_name}-lake"

  tags = {
    Project     = var.project_name
    Layer       = "lakehouse"
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Pre-create the folder prefixes so Databricks Auto Loader can resolve paths
resource "aws_s3_object" "raw_prefix" {
  bucket  = aws_s3_bucket.lakehouse.id
  key     = "raw/machine-telemetry/"
  content = ""
}

resource "aws_s3_object" "bronze_prefix" {
  bucket  = aws_s3_bucket.lakehouse.id
  key     = "delta/bronze_machine_telemetry/"
  content = ""
}

resource "aws_s3_object" "silver_prefix" {
  bucket  = aws_s3_bucket.lakehouse.id
  key     = "delta/silver_telemetry/"
  content = ""
}

resource "aws_s3_object" "gold_risk_prefix" {
  bucket  = aws_s3_bucket.lakehouse.id
  key     = "delta/gold_supply_risk/"
  content = ""
}

resource "aws_s3_object" "checkpoint_prefix" {
  bucket  = aws_s3_bucket.lakehouse.id
  key     = "checkpoints/"
  content = ""
}

# -----------------------------------------------------------------------------
# Amazon Kinesis Data Streams — Real-time telemetry ingestion
# Replaces the self-hosted Kafka cluster. Free tier: 1 shard.
# Production equivalent: Amazon MSK with multiple brokers.
# -----------------------------------------------------------------------------
resource "aws_kinesis_stream" "machine_telemetry" {
  name        = "${var.project_name}-telemetry"
  shard_count = 1

  retention_period = 24  # hours

  tags = {
    Project   = var.project_name
    Purpose   = "machine-telemetry-ingestion"
    ManagedBy = "terraform"
  }
}

# -----------------------------------------------------------------------------
# IAM — Databricks access to S3 and Kinesis
# Databricks Community Edition uses Access Key + Secret for S3 access.
# -----------------------------------------------------------------------------
resource "aws_iam_user" "databricks_pipeline" {
  name = "${var.project_name}-databricks-user"
}

resource "aws_iam_access_key" "databricks_pipeline" {
  user = aws_iam_user.databricks_pipeline.name
}

resource "aws_iam_policy" "pipeline_policy" {
  name        = "${var.project_name}-policy"
  description = "Allows Databricks to read/write S3 lakehouse and produce to Kinesis"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3LakehouseAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.lakehouse.arn,
          "${aws_s3_bucket.lakehouse.arn}/*",
        ]
      },
      {
        Sid    = "KinesisProducerAccess"
        Effect = "Allow"
        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:ListStreams",
        ]
        Resource = aws_kinesis_stream.machine_telemetry.arn
      },
    ]
  })
}

resource "aws_iam_user_policy_attachment" "databricks_attach" {
  user       = aws_iam_user.databricks_pipeline.name
  policy_arn = aws_iam_policy.pipeline_policy.arn
}

# -----------------------------------------------------------------------------
# Outputs — values needed to configure Databricks and the generator
# -----------------------------------------------------------------------------
output "s3_bucket_name" {
  description = "S3 bucket name for the Delta Lakehouse"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "kinesis_stream_name" {
  description = "Kinesis stream name for the telemetry producer"
  value       = aws_kinesis_stream.machine_telemetry.name
}

output "databricks_access_key_id" {
  description = "IAM Access Key ID for Databricks S3 mount"
  value       = aws_iam_access_key.databricks_pipeline.id
  sensitive   = true
}

output "databricks_secret_access_key" {
  description = "IAM Secret Access Key for Databricks S3 mount"
  value       = aws_iam_access_key.databricks_pipeline.secret
  sensitive   = true
}

output "mount_command" {
  description = "Run this in a Databricks notebook to mount the S3 bucket"
  value = <<-EOT
    dbutils.fs.mount(
      source="s3a://${aws_s3_bucket.lakehouse.bucket}",
      mount_point="/mnt/te-supply-chain",
      extra_configs={
        "fs.s3a.access.key": "<ACCESS_KEY_ID>",
        "fs.s3a.secret.key": "<SECRET_ACCESS_KEY>"
      }
    )
  EOT
}
