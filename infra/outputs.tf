output "site_url" {
  description = "The public dashboard"
  value       = "https://${aws_cloudfront_distribution.site.domain_name}"
}

output "api_endpoint" {
  description = "API Gateway URL (bypasses CloudFront; useful for load tests)"
  value       = aws_apigatewayv2_api.api.api_endpoint
}

output "distribution_id" {
  value = aws_cloudfront_distribution.site.id
}

output "ecr_repository_url" {
  value = aws_ecr_repository.api.repository_url
}

output "ui_bucket" {
  value = aws_s3_bucket.this["ui"].bucket
}

output "lake_bucket" {
  value = aws_s3_bucket.this["lake"].bucket
}
