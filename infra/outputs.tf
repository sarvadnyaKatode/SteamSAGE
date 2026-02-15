output "DEPLOY_BUCKET_NAME" {
  value = aws_s3_bucket.data_lake.bucket
}

output "DEPLOY_ROLE_ARN" {
  value = aws_iam_role.github_actions_role.arn
}
