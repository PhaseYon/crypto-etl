#!/usr/bin/env bash
# Build the UI, upload it to the UI bucket and invalidate CloudFront.
# Usage: AWS_PROFILE=crypto-etl infra/scripts/deploy-ui.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

bucket=$(terraform -chdir=infra output -raw ui_bucket)
distribution=$(terraform -chdir=infra output -raw distribution_id)

# Builds in the pinned Node image so the result doesn't depend on the host's Node
docker run --rm -v "$PWD/ui:/app" -w /app -u "$(id -u):$(id -g)" -e HOME=/tmp \
  node:24.22.0-bookworm-slim sh -c "npm ci --no-audit --no-fund && npm run build"

# Hashed assets never change; index.html must be revalidated so new deploys show up
aws s3 sync ui/dist/assets "s3://$bucket/assets" --delete \
  --cache-control "public, max-age=31536000, immutable" --only-show-errors
aws s3 sync ui/dist "s3://$bucket" --delete --exclude "assets/*" \
  --cache-control "no-cache" --only-show-errors
aws cloudfront create-invalidation --distribution-id "$distribution" --paths "/index.html" "/" \
  --query 'Invalidation.Id' --output text
echo "deployed: $(terraform -chdir=infra output -raw site_url)"
