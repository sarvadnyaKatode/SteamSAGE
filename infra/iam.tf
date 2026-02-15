# Glue IAM Role

resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# Glue IAM Policy

resource "aws_iam_policy" "glue_policy" {
  name        = "${var.project_name}-glue-policy-${var.environment}"
  description = "Permissions for AWS Glue jobs to access S3, logs, and Glue resources"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [

      # S3 access (data + scripts + athena results)
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },

      # CloudWatch Logs
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },

      # Glue service permissions
      {
        Effect = "Allow"
        Action = [
          "glue:GetJob",
          "glue:GetJobs",
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchCreatePartition",
          "glue:BatchGetPartition",
          "glue:BatchUpdatePartition"
        ]
        Resource = "*"
      }
    ]
  })
}


# Attach policy to role

resource "aws_iam_role_policy_attachment" "glue_policy_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_policy.arn
}


# Grant Access to the EXISTING Data Bucket (S3 + KMS)

resource "aws_iam_role_policy" "glue_access_existing_data" {
  name = "glue-access-existing-data"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # 1. Allow Reading Files
      {
        Sid    = "AllowReadOldBucket"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::steam-analytics-steam-analytics-aman-2026",
          "arn:aws:s3:::steam-analytics-steam-analytics-aman-2026/*"
        ]
      },
      # 2. Allow Decrypting Files (The Missing Piece)
      {
        Sid    = "AllowKMSDecrypt"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = "*"  
      }
    ]
  })
}






