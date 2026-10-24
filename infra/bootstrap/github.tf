# Lets GitHub Actions deploy without stored AWS keys: a workflow on main presents GitHub's OIDC
# token and gets short-lived credentials for this role.
#
# The role can roll out new code (API image, Lambda update, UI files, cache invalidation) and read
# what terraform plan needs. It can't create or delete infrastructure; structural changes to the
# main stack are applied locally with an admin login.

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = "us-east-2"
  project    = "crypto-etl"
  # GitHub issues this repository's tokens with immutable subjects: owner and repo carry their
  # numeric IDs, so a renamed or re-created repo with the same name can't assume the role
  # (gh api repos/PhaseYon/crypto-etl/actions/oidc/customization/sub)
  github_repo  = "PhaseYon@122104297/crypto-etl@1399268791"
  ui_bucket    = "arn:aws:s3:::${local.project}-ui-${local.account_id}"
  lake_bucket  = "arn:aws:s3:::${local.project}-lake-${local.account_id}"
  api_function = "arn:aws:lambda:${local.region}:${local.account_id}:function:${local.project}-api"
  api_repo     = "arn:aws:ecr:${local.region}:${local.account_id}:repository/${local.project}-api"
  api_role     = "arn:aws:iam::${local.account_id}:role/${local.project}-api"
}

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_policy_document" "github_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    # Only workflows running on main in this repository (not PRs, forks or other branches)
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${local.github_repo}:ref:refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "github_deploy" {
  name                 = "${local.project}-github-deploy"
  assume_role_policy   = data.aws_iam_policy_document.github_trust.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "github_deploy" {
  statement {
    sid       = "TerraformState"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.state.arn]
  }
  statement {
    sid       = "TerraformStateObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.state.arn}/dashboard/*"]
  }

  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"] # account-wide by design; the token alone grants nothing
  }
  statement {
    sid = "EcrPushApiImage"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:InitiateLayerUpload",
      "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload",
      "ecr:PutImage",
      "ecr:BatchGetImage",
      "ecr:GetDownloadUrlForLayer",
      "ecr:Describe*",
      "ecr:List*",
      "ecr:GetLifecyclePolicy",
      "ecr:GetRepositoryPolicy",
    ]
    resources = [local.api_repo]
  }

  statement {
    sid       = "UpdateApiFunction"
    actions   = ["lambda:Get*", "lambda:List*", "lambda:UpdateFunctionCode", "lambda:UpdateFunctionConfiguration"]
    resources = [local.api_function]
  }
  statement {
    sid       = "PassApiRoleToLambda"
    actions   = ["iam:PassRole"]
    resources = [local.api_role]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["lambda.amazonaws.com"]
    }
  }

  statement {
    sid       = "DeployUi"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${local.ui_bucket}/*"]
  }
  statement {
    sid       = "InvalidateCdn"
    actions   = ["cloudfront:CreateInvalidation", "cloudfront:GetInvalidation"]
    resources = ["arn:aws:cloudfront::${local.account_id}:distribution/*"]
  }

  # Read-only access for terraform plan's refresh of the main stack
  statement {
    sid       = "ReadBuckets"
    actions   = ["s3:Get*", "s3:List*"]
    resources = [local.ui_bucket, "${local.ui_bucket}/*", local.lake_bucket]
  }
  statement {
    sid       = "ReadApiRole"
    actions   = ["iam:GetRole", "iam:GetRolePolicy", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies", "iam:ListRoleTags"]
    resources = [local.api_role]
  }
  statement {
    sid = "ReadAccountScoped"
    # These services don't support narrower resources for their read calls (or the IDs only exist
    # after apply). Read-only.
    actions = [
      "cloudfront:Get*",
      "cloudfront:List*",
      "logs:Describe*",
      "logs:ListTagsForResource",
      "logs:ListTagsLogGroup",
    ]
    resources = ["*"]
  }
  statement {
    sid       = "ReadHttpApi"
    actions   = ["apigateway:GET"]
    resources = ["arn:aws:apigateway:${local.region}::/apis*", "arn:aws:apigateway:${local.region}::/tags/*"]
  }
}

resource "aws_iam_role_policy" "github_deploy" {
  role   = aws_iam_role.github_deploy.id
  policy = data.aws_iam_policy_document.github_deploy.json
}

output "github_deploy_role_arn" {
  value = aws_iam_role.github_deploy.arn
}
