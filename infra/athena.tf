
# Athena Database

resource "aws_athena_database" "steam_db" {
  name   = var.athena_database_name
  bucket = aws_s3_bucket.data_lake.bucket

  comment = "Athena database for Steam analytics curated datasets"
}


# Athena Workgroup

resource "aws_athena_workgroup" "steam_workgroup" {
  name = "${var.project_name}-workgroup-${var.environment}"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.bucket}/athena-results/"
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

