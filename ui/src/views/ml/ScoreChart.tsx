import {
  ColorType,
  createChart,
  CrosshairMode,
  LineSeries,
  type IChartApi,
  type ISeriesApi,
} from "lightweight-charts";
import { useEffect, useRef } from "react";

import { useChartColors } from "../../lib/theme";
import type { scoreSeries } from "./predictions";

type Series = ReturnType<typeof scoreSeries>;

/** Model score over time, one 2px line per product, colored by the product's fixed slot. */
export function ScoreChart({ series }: { series: Series }) {
  const container = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const lines = useRef(new Map<string, ISeriesApi<"Line">>());
  const colors = useChartColors();

  useEffect(() => {
    if (!container.current) return;
    const instance = createChart(container.current, {
      autoSize: true,
      crosshair: { mode: CrosshairMode.Normal },
      timeScale: { timeVisible: true, secondsVisible: false, borderVisible: false },
      rightPriceScale: { borderVisible: false },
    });
    chart.current = instance;
    const owned = lines.current;
    return () => {
      instance.remove();
      chart.current = null;
      owned.clear();
    };
  }, []);

  useEffect(() => {
    const instance = chart.current;
    if (!instance) return;
    instance.applyOptions({
      layout: {
        background: { type: ColorType.Solid, color: colors.surface },
        textColor: colors.muted,
        fontFamily: 'system-ui, -apple-system, "Segoe UI", sans-serif',
      },
      grid: { vertLines: { color: colors.grid }, horzLines: { color: colors.grid } },
    });
    const wanted = new Set(series.map((s) => s.product));
    for (const [product, line] of lines.current) {
      if (!wanted.has(product)) {
        instance.removeSeries(line);
        lines.current.delete(product);
      }
    }
    let added = false;
    for (const s of series) {
      let line = lines.current.get(s.product);
      if (!line) {
        added = true;
        line = instance.addSeries(LineSeries, { lineWidth: 2, priceLineVisible: false, title: s.product });
        lines.current.set(s.product, line);
      }
      line.applyOptions({ color: colors.series[s.slot] ?? colors.series[0] });
      line.setData(s.data);
    }
    // Fit when products appear, not on every poll (that would undo the user's zoom)
    if (added) instance.timeScale().fitContent();
  }, [series, colors]);

  return (
    <div>
      <ul className="legend" aria-label="Legend">
        {series.map((s) => (
          <li key={s.product}>
            <span className="key-line" style={{ background: colors.series[s.slot] }} aria-hidden="true" />
            {s.product}
          </li>
        ))}
      </ul>
      <div
        ref={container}
        className="chart-box small"
        role="img"
        aria-label="Model score over time per product; exact values are in the table below"
      />
    </div>
  );
}
