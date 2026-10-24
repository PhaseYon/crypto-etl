# The dashboard API: the api/ Docker image on Lambda behind an HTTP API with a request rate limit.
# The account allows 10 concurrent Lambda executions in total, which also caps what abuse can cost.

resource "aws_ecr_repository" "api" {
  name                 = "${var.project}-api"
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true
  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "api" {
  repository = aws_ecr_repository.api.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the 5 most recent images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/lambda/${var.project}-api"
  retention_in_days = var.log_retention_days
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "api" {
  name               = "${var.project}-api"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# Read-only access to the lake, plus writing its own logs
data "aws_iam_policy_document" "api" {
  statement {
    sid       = "ListLake"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.this["lake"].arn]
  }
  statement {
    sid       = "ReadLake"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.this["lake"].arn}/*"]
  }
  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.api.arn}:*"]
  }
}

resource "aws_iam_role_policy" "api" {
  role   = aws_iam_role.api.id
  policy = data.aws_iam_policy_document.api.json
}

resource "aws_lambda_function" "api" {
  function_name = "${var.project}-api"
  role          = aws_iam_role.api.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.api.repository_url}:${var.api_image_tag}"
  architectures = ["x86_64"]
  memory_size   = 1024
  timeout       = 20

  environment {
    variables = {
      SNAPSHOT_AT = var.snapshot_at
      LAKE_BUCKET = aws_s3_bucket.this["lake"].bucket
      PRODUCTS    = var.products
      # The snapshot never changes, so reads can be cached for the life of the instance
      CACHE_TTL_SECONDS = "3600"
      # S3 reads from a cold instance are slower than from the local object store
      SOURCE_TIMEOUT_SECONDS = "10"
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.api.name
  }

  lifecycle {
    precondition {
      condition     = var.api_image_tag != null
      error_message = "Set api_image_tag: push an image first with scripts/push-api.sh."
    }
  }

  depends_on = [aws_iam_role_policy.api]
}

resource "aws_apigatewayv2_api" "api" {
  name          = "${var.project}-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}

# Only reads under /api reach Lambda; everything else is a 404 from API Gateway
resource "aws_apigatewayv2_route" "api" {
  api_id    = aws_apigatewayv2_api.api.id
  route_key = "GET /api/{proxy+}"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_cloudwatch_log_group" "api_access" {
  name              = "/aws/apigateway/${var.project}-api"
  retention_in_days = var.log_retention_days
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.api.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_rate_limit  = var.api_rate_limit
    throttling_burst_limit = var.api_burst_limit
  }

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_access.arn
    format = jsonencode({
      time     = "$context.requestTime"
      ip       = "$context.identity.sourceIp"
      method   = "$context.httpMethod"
      path     = "$context.path"
      status   = "$context.status"
      latency  = "$context.responseLatency"
      lambdaMs = "$context.integrationLatency"
      error    = "$context.integrationErrorMessage"
    })
  }
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowApiGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.api.execution_arn}/*/*"
}
