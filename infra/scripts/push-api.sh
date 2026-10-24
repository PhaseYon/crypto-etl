#!/usr/bin/env bash
# Build the API image and push it to ECR, tagged with the current commit. Prints the tag.
# Usage: AWS_PROFILE=crypto-etl infra/scripts/push-api.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

region=$(aws configure get region || echo us-east-2)
repo=$(terraform -chdir=infra output -raw ecr_repository_url)
registry=${repo%%/*}
tag=$(git rev-parse --short=12 HEAD)
if [[ -n $(git status --porcelain -- api) ]]; then
  # Tags are immutable in ECR, so uncommitted changes get their own tag
  tag="$tag-dirty-$(date +%s)"
fi

if aws ecr describe-images --repository-name "${repo#*/}" --image-ids imageTag="$tag" >/dev/null 2>&1; then
  echo "already pushed: $repo:$tag" >&2
else
  aws ecr get-login-password --region "$region" | docker login --username AWS --password-stdin "$registry" >&2
  # Lambda needs a single-platform image manifest, not a multi-arch index
  docker build --platform linux/amd64 --provenance=false -t "$repo:$tag" api >&2
  docker push "$repo:$tag" >&2
fi
echo "$tag"
