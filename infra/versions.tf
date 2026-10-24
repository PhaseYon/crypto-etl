terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Created by infra/bootstrap. use_lockfile stops two applies from running at once.
  backend "s3" {
    bucket       = "crypto-etl-tfstate-071026217372"
    key          = "dashboard/terraform.tfstate"
    region       = "us-east-2"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  # The organization's SCP only allows regional services in us-east-2
  region = var.region
  default_tags {
    tags = { Project = var.project, ManagedBy = "terraform" }
  }
}

data "aws_caller_identity" "current" {}
