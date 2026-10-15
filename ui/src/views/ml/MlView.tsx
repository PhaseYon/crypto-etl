import { useMemo } from "react";

import { useMeta, usePredictions } from "../../api/queries";
import type { PredictionsResponse } from "../../api/types";
import { HealthBadge } from "../../components/HealthBadge";
import { Panel } from "../../components/Panel";
import { formatAgo, formatTime } from "../../lib/format";
import { latestByProduct, MAX_CHART_SERIES, scoreSeries } from "./predictions";
import { ScoreChart } from "./ScoreChart";

export function MlView() {
  const query = usePredictions();
  const products = useMeta().data?.products ?? [];
  return (
    <div className="stack">
      <Panel
        title="Model predictions"
        subtitle="Batch inference output (ml/predictions)"
        query={query}
        isEmpty={emptyMessage}
        actions={query.data ? <HealthBadge status={query.data.status} /> : null}
      >
        {(data) => <PredictionsBody data={data} products={products} />}
      </Panel>
    </div>
  );
}

function emptyMessage(data: PredictionsResponse): string | null {
  if (data.status === "down") return `Couldn't read predictions: ${data.error ?? "unknown error"}`;
  if (data.status === "inactive" || data.predictions.length === 0) {
    return `${data.reason}. Predictions appear here once the ML batch job has written ml/predictions.`;
  }
  return null;
}

function PredictionsBody({ data, products }: { data: PredictionsResponse; products: string[] }) {
  // Products the API knows about first (stable colors), then any others found in the data
  const order = useMemo(() => {
    const extra = [...new Set(data.predictions.map((p) => p.product_id))].filter(
      (p) => !products.includes(p),
    );
    return [...products, ...extra];
  }, [data.predictions, products]);
  const series = useMemo(() => scoreSeries(data.predictions, order), [data.predictions, order]);
  const latest = latestByProduct(data.predictions, order);
  const hidden = latest.length - series.length;

  return (
    <div className="stack">
      <p className="panel-subtitle">{data.reason}</p>
      {series.length > 0 ? <ScoreChart series={series} /> : null}
      {hidden > 0 ? (
        <p className="footer">
          {hidden} more product{hidden === 1 ? "" : "s"} in the table (the chart shows up to{" "}
          {MAX_CHART_SERIES}).
        </p>
      ) : null}
      <div className="table-wrap">
        <table className="data">
          <caption className="sr-only">Latest prediction per product</caption>
          <thead>
            <tr>
              <th scope="col">Product</th>
              <th scope="col" className="num">
                Score
              </th>
              <th scope="col">Window</th>
              <th scope="col">Model</th>
              <th scope="col">Written</th>
            </tr>
          </thead>
          <tbody>
            {latest.map((p) => (
              <tr key={p.product_id}>
                <td>{p.product_id}</td>
                <td className="num">{p.score === null ? "—" : p.score.toFixed(3)}</td>
                <td>{formatTime(p.window_start)}</td>
                <td>{p.model_version ?? "—"}</td>
                <td>{formatAgo(p.created_at)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
