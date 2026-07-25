variable "aws_region" {
  type        = string
  description = "AWS region for infrastructure deployment"
  default     = "us-east-1"
}

variable "project_name" {
  type        = string
  description = "Project name prefix for resource naming and tagging"
  default     = "te-supply-chain-telemetry"
}

variable "environment" {
  type        = string
  description = "Deployment environment stage"
  default     = "Production"
}

variable "owner" {
  type        = string
  description = "Team owner tag for compliance"
  default     = "DataEngineering"
}
