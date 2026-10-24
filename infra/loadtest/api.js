// Load test for the dashboard API with k6: a fixed number of virtual users, each requesting the
// dashboard's endpoints in a loop. Run against API Gateway to load Lambda, or the CloudFront URL
// to load the CDN. Usage (see infra/README.md):
//   docker run --rm -i -e BASE_URL=... -e VUS=50 -e DURATION=60s grafana/k6:<version> run - < infra/loadtest/api.js
import http from "k6/http";
import { check } from "k6";
import { Counter } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL;
const throttled = new Counter("throttled_429");

export const options = {
  vus: Number(__ENV.VUS || 50),
  duration: __ENV.DURATION || "60s",
  summaryTrendStats: ["avg", "p(50)", "p(95)", "p(99)", "max"],
  thresholds: { http_req_failed: ["rate<0.01"] },
};

const PATHS = [
  "/api/meta",
  "/api/pipeline",
  "/api/quality",
  "/api/market/ohlcv?product=BTC-USD&minutes=360",
  "/api/market/ohlcv?product=ETH-USD&minutes=360",
  "/api/market/ohlcv?product=SOL-USD&minutes=360",
];

export default function () {
  const res = http.get(BASE_URL + PATHS[Math.floor(Math.random() * PATHS.length)]);
  if (res.status === 429) throttled.add(1);
  check(res, { "status 200": (r) => r.status === 200 });
}
