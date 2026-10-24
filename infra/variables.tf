variable "region" {
  type    = string
  default = "us-east-2"
}

variable "project" {
  type    = string
  default = "crypto-etl"
}

variable "api_image_tag" {
  description = "Tag of the API image in ECR to run (scripts/push-api.sh prints it)"
  type        = string
  default     = null
}

variable "snapshot_at" {
  description = "Moment the lake snapshot represents; the API measures freshness from it"
  type        = string
  default     = "2026-10-22T08:30:00Z"
}

variable "products" {
  type    = string
  default = "BTC-USD,ETH-USD,SOL-USD"
}

# Requests per second across all clients, before API Gateway answers 429.
# CloudFront caches snapshot responses, so real visitors rarely reach this.
variable "api_rate_limit" {
  type    = number
  default = 10
}

variable "api_burst_limit" {
  type    = number
  default = 20
}

variable "log_retention_days" {
  type    = number
  default = 14
}
