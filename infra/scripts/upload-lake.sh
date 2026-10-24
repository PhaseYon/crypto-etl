#!/usr/bin/env bash
# Copy the Delta tables from the local object store (MinIO/RustFS) to the lake bucket in S3.
# Spark checkpoints are left out: the API only reads the tables.
# Usage: AWS_PROFILE=crypto-etl infra/scripts/upload-lake.sh   (with `docker compose up -d minio` running)
set -euo pipefail
cd "$(dirname "$0")/../.."

dest=$(terraform -chdir=infra output -raw lake_bucket)
staging=$(mktemp -d)
trap 'rm -rf "$staging"' EXIT

# The local store's keys from .env are used only in this subshell, so they can't reach AWS
(
  set -a; source .env; set +a
  unset AWS_PROFILE
  aws s3 sync "s3://${LAKE_BUCKET:-crypto-lake}" "$staging" \
    --endpoint-url "http://localhost:${MINIO_API_HOST_PORT:-9000}" --region us-east-1 \
    --exclude "_checkpoints/*" --only-show-errors
)
du -sh "$staging"
aws s3 sync "$staging" "s3://$dest" --delete --only-show-errors
echo "uploaded to s3://$dest"
