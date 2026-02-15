# Glue Jobs

locals {
  glue_scripts_path = "s3://${aws_s3_bucket.data_lake.bucket}/glue-scripts"
}

# Applications Job

resource "aws_glue_job" "applications_job" {
  name     = "${var.project_name}-applications-job-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  glue_version       = var.glue_version
  worker_type        = var.glue_worker_type
  number_of_workers  = var.glue_number_of_workers
  timeout            = var.glue_timeout

  command {
    name            = "glueetl"
    script_location = "${local.glue_scripts_path}/applications_job.py"
    python_version  = "3"
  }

  default_arguments = {
    # INPUT: Reading from permanent existing bucket (Fixed Name)
    "--RAW_BASE"    = "s3://steam-analytics-steam-analytics-aman-2026/raw"

    # OUTPUT: Writing to the new dynamic bucket
    "--SILVER_BASE" = "s3://${aws_s3_bucket.data_lake.bucket}/silver"
    "--job-language" = "python"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# Reviews Job

resource "aws_glue_job" "reviews_job" {
  name     = "${var.project_name}-reviews-job-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  glue_version       = var.glue_version
  worker_type        = var.glue_worker_type
  number_of_workers  = var.glue_number_of_workers
  timeout            = var.glue_timeout

  command {
    name            = "glueetl"
    script_location = "${local.glue_scripts_path}/reviews_job.py"
    python_version  = "3"
  }

  default_arguments = {
    # INPUT 1: Raw Data (Old Bucket)
    "--RAW_BASE"    = "s3://steam-analytics-steam-analytics-aman-2026/raw"

    # INPUT 2: Scores Data (Old Bucket - Silver Folder)
    "--SCORES_BASE" = "s3://steam-analytics-steam-analytics-aman-2026/silver"

    # OUTPUT: Writing to the new dynamic bucket
    "--SILVER_BASE" = "s3://${aws_s3_bucket.data_lake.bucket}/silver"
    "--job-language" = "python"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# Dimensions Job

resource "aws_glue_job" "dimensions_job" {
  name     = "${var.project_name}-dimensions-job-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  glue_version       = var.glue_version
  worker_type        = var.glue_worker_type
  number_of_workers  = var.glue_number_of_workers
  timeout            = var.glue_timeout

  command {
    name            = "glueetl"
    script_location = "${local.glue_scripts_path}/dimensions_job.py"
    python_version  = "3"
  }

  default_arguments = {
    # INPUT: Reading from permanent existing bucket
    "--RAW_BASE"    = "s3://steam-analytics-steam-analytics-aman-2026/raw"

    # OUTPUT: Writing to the new dynamic bucket
    "--SILVER_BASE" = "s3://${aws_s3_bucket.data_lake.bucket}/silver"
    "--job-language" = "python"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# Glue Crawler (Silver Layer)

resource "aws_glue_crawler" "silver_crawler" {
  name          = "${var.project_name}-silver-crawler-${var.environment}"
  database_name = aws_athena_database.steam_db.name
  role          = aws_iam_role.glue_role.arn

  # Target 1: Applications
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/applications/"
  }

  # Target 2: Reviews
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/reviews/"
  }

  # Target 3: Dimensions
  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/silver/dimensions/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
