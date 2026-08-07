terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.80.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    Owner       = var.owner
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.project_name}-lake"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "raw_prefix" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "raw/machine-telemetry/"
}

resource "aws_s3_object" "bronze_prefix" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "delta/bronze_machine_telemetry/"
}

resource "aws_s3_object" "silver_prefix" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "delta/silver_telemetry/"
}

resource "aws_s3_object" "gold_risk_prefix" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "delta/gold_supply_risk/"
}

resource "aws_s3_object" "checkpoint_prefix" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "checkpoints/"
}

# resource "aws_kinesis_stream" "machine_telemetry" {
#   name             = "${var.project_name}-telemetry"
#   shard_count      = 1
#   retention_period = 24
#   tags             = local.common_tags
# }

resource "aws_iam_user" "databricks_pipeline" {
  name = "${var.project_name}-databricks-user"
  tags = local.common_tags
}

resource "aws_iam_access_key" "databricks_pipeline" {
  user = aws_iam_user.databricks_pipeline.name
}

resource "aws_iam_policy" "pipeline_policy" {
  name        = "${var.project_name}-policy"
  description = "Least-privilege policy for Databricks S3 and Kinesis integration"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3BucketAccess"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [aws_s3_bucket.lakehouse.arn]
      },
      {
        Sid    = "S3ObjectOperations"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = ["${aws_s3_bucket.lakehouse.arn}/*"]
      },
      {
        Sid    = "KinesisStreamOperations"
        Effect = "Allow"
        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream"
        ]
        Resource = ["arn:aws:kinesis:*:*:stream/${var.project_name}-telemetry"]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "databricks_attach" {
  user       = aws_iam_user.databricks_pipeline.name
  policy_arn = aws_iam_policy.pipeline_policy.arn
}
