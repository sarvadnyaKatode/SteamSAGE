
# S3 Bucket (Data Lake)

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "data_lake" {
  # Creates a unique name like: steam-analytics-dev-a9b3c4
  bucket = "${var.project_name}-${var.environment}-${random_id.bucket_suffix.hex}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}


# Block ALL public access

resource "aws_s3_bucket_public_access_block" "data_lake_block" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


# Enable versioning

resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}


# Server-side encryption

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_encryption" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}


# Logical prefixes (folders)

resource "aws_s3_object" "raw_prefix" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "raw/"
}

resource "aws_s3_object" "silver_prefix" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "silver/"
}

resource "aws_s3_object" "glue_scripts_prefix" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "glue-scripts/"
}

resource "aws_s3_object" "athena_results_prefix" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "athena-results/"
}



