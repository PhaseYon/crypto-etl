#!/usr/bin/env bash
# Copy the Delta tables from the local object store (MinIO/RustFS) to the lake bucket in S3.
# Spark checkpoints are left out: the API only reads the tables.
# Usage: AWS_PROFILE=crypto-etl infra/scripts/upload-lake.sh   (with `docker compose up -d minio` running)
set -euo pipefail
cd "$(dirname "$0")/../.."

set -a; source .env; set +a
local_bucket=${LAKE_BUCKET:-crypto-lake}
endpoint="http://localhost:${MINIO_API_HOST_PORT:-9000}"
dest=$(terraform -chdir=infra output -raw lake_bucket)
staging=$(mktemp -d)
trap 'rm -rf "$staging"' EXIT

# The local store uses the .env keys; real S3 uses the AWS profile
AWS_PROFILE= AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  aws s3 sync "s3://$local_bucket" "$staging" --endpoint-url "$endpoint" --region us-east-1 \
  --exclude "_checkpoints/*" --only-show-errors
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
du -sh "$staging"
aws s3 sync "$staging" "s3://$dest" --delete --only-show-errors
echo "uploaded to s3://$dest"
