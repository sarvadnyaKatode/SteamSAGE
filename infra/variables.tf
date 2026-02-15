
# Global

variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix for resources"
  type        = string
  default     = "steam-analytics"
}

variable "environment" {
  description = "Deployment environment (dev, prod)"
  type        = string
  default     = "dev"
}


# S3

variable "data_bucket_name" {
  description = "S3 bucket for raw, silver data and scripts"
  type        = string
}


# Glue

variable "glue_version" {
  description = "Glue version"
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type"
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of workers for all Glue jobs"
  type        = number
  default     = 2
}

variable "glue_timeout" {
  description = "Glue job timeout in minutes"
  type        = number
  default     = 60
}


# Athena

variable "athena_database_name" {
  description = "Athena database name"
  type        = string
  default     = "steam_analytics"
}

variable "github_repo" {
  description = "The GitHub repository (Org/Name) to trust. passed automatically by GitHub Actions."
  type        = string
}


