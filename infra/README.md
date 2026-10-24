# infra: the public dashboard on AWS

The dashboard runs as a read-only demo over a frozen week of lake data (snapshot mode, see `api/`).
Kafka, Spark and the ML jobs aren't deployed.

```
browser → CloudFront ─┬─ /*      → S3 (built UI, private, origin access control)
                      └─ /api/*  → API Gateway HTTP API (10 req/s, burst 20) → Lambda (api/ image) → S3 lake snapshot
```

- **Region:** everything regional runs in **us-east-2**. The organization's SCP allows only global services in us-east-1.
- **Caching:** snapshot API responses send `Cache-Control: public, max-age=300`, so CloudFront answers repeat requests itself.
- **Abuse limits:** the account's Lambda concurrency (10) and the API Gateway rate limit cap the cost of abuse. The org's $20 monthly budget alert backs them up.

## Deploy

Log in first: `aws login --profile crypto-etl` (with the browser on the Blank Slate account), then `export AWS_PROFILE=crypto-etl`.

```bash
terraform -chdir=infra/bootstrap init && terraform -chdir=infra/bootstrap apply   # once: state bucket
terraform -chdir=infra init
terraform -chdir=infra apply -target=aws_ecr_repository.api -var api_image_tag=x  # first deploy only
tag=$(infra/scripts/push-api.sh)                 # build + push the API image
terraform -chdir=infra apply -var api_image_tag=$tag
docker compose up -d minio && infra/scripts/upload-lake.sh   # copy the local lake to S3
infra/scripts/deploy-ui.sh                       # build the UI, upload, invalidate CloudFront
```

Smoke test the live site:

```bash
docker run --rm -v "$PWD/ui:/app" -w /app -u "$(id -u):$(id -g)" -e HOME=/tmp \
  -e E2E_SNAPSHOT=1 -e E2E_BASE_URL=$(terraform -chdir=infra output -raw site_url) \
  mcr.microsoft.com/playwright:v1.64.1-noble npx playwright test
```

## Load test

`loadtest/api.js` is a k6 script that sends a mix of dashboard requests. To load Lambda, run it against `terraform output -raw api_endpoint`, after raising `api_rate_limit` and `api_burst_limit` for the run. To load the CDN, run it against `site_url`.

```bash
docker run --rm -i -e BASE_URL=<url> -e VUS=50 -e DURATION=30s grafana/k6:1.3.0 run - < infra/loadtest/api.js
```

Results from 2026-10-24, from one home connection:

| Target | Throughput | Latency p50 / p95 | Errors |
|---|---|---|---|
| CloudFront (cache hits) | 1,146 req/s, limited by the client's 32 MB/s link | 30 / 110 ms | 0% |
| API Gateway → Lambda | ~100 req/s, the account's Lambda ceiling (10 concurrent × 10 req/s each) | 33 / 47 ms | throttled above the ceiling |
