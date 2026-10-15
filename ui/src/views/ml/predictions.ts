import type { LineData, UTCTimestamp } from "lightweight-charts";

import type { Prediction } from "../../api/types";

export const MAX_CHART_SERIES = 3;

/** Newest prediction per product, in the given product order. */
export function latestByProduct(predictions: Prediction[], order: string[]): Prediction[] {
  const latest = new Map<string, Prediction>();
  for (const p of predictions) {
    const current = latest.get(p.product_id);
    if (!current || sortKey(p) > sortKey(current)) latest.set(p.product_id, p);
  }
  const rank = (id: string) => {
    const i = order.indexOf(id);
    return i === -1 ? Number.MAX_SAFE_INTEGER : i;
  };
  return [...latest.values()].sort(
    (a, b) => rank(a.product_id) - rank(b.product_id) || a.product_id.localeCompare(b.product_id),
  );
}

function sortKey(p: Prediction): number {
  return Date.parse(p.window_start ?? p.created_at ?? "") || 0;
}

/**
 * Score series per product for the chart. Colors follow the product (its position in the
 * configured product list), never its rank in the current data, so a product keeps its color.
 */
export function scoreSeries(
  predictions: Prediction[],
  products: string[],
): { product: string; slot: number; data: LineData<UTCTimestamp>[] }[] {
  const byProduct = new Map<string, Map<number, number>>();
  for (const p of predictions) {
    if (p.score === null || !p.window_start) continue;
    const time = Math.floor(Date.parse(p.window_start) / 1000);
    if (Number.isNaN(time)) continue;
    const points = byProduct.get(p.product_id) ?? new Map<number, number>();
    points.set(time, p.score);
    byProduct.set(p.product_id, points);
  }
  return products
    .map((product, slot) => ({ product, slot }))
    .filter(({ product, slot }) => slot < MAX_CHART_SERIES && byProduct.has(product))
    .map(({ product, slot }) => ({
      product,
      slot,
      data: [...(byProduct.get(product) ?? new Map<number, number>()).entries()]
        .sort(([a], [b]) => a - b)
        .map(([time, value]) => ({ time: time as UTCTimestamp, value })),
    }));
}
