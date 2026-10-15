import {
  CandlestickSeries,
  ColorType,
  createChart,
  CrosshairMode,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type MouseEventParams,
  type UTCTimestamp,
} from "lightweight-charts";
import { useEffect, useRef, useState } from "react";

import type { Candle } from "../../api/types";
import { formatPrice, formatVolume } from "../../lib/format";
import { useChartColors } from "../../lib/theme";
import { toCandleData, toVolumeData } from "./candles";

type Readout = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number | null;
};

type Bar = { open: number; high: number; low: number; close: number };

function isBar(data: unknown): data is Bar {
  return typeof data === "object" && data !== null && "open" in data && "close" in data;
}

function isValue(data: unknown): data is { value: number } {
  return typeof data === "object" && data !== null && "value" in data;
}

/** Candles (price pane) over volume (its own pane, so there's never a second y-axis on one plot). */
export function CandleChart({ candles, productId }: { candles: Candle[]; productId: string }) {
  const container = useRef<HTMLDivElement>(null);
  const chart = useRef<IChartApi | null>(null);
  const price = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volume = useRef<ISeriesApi<"Histogram"> | null>(null);
  const colors = useChartColors();
  const [hovered, setHovered] = useState<Readout | null>(null);

  // Create once; theme changes and data updates are applied below without recreating
  useEffect(() => {
    if (!container.current) return;
    const instance = createChart(container.current, {
      autoSize: true,
      crosshair: { mode: CrosshairMode.Magnet },
      timeScale: { timeVisible: true, secondsVisible: false, borderVisible: false },
      rightPriceScale: { borderVisible: false },
    });
    price.current = instance.addSeries(CandlestickSeries, { borderVisible: false, priceLineVisible: false });
    volume.current = instance.addSeries(
      HistogramSeries,
      { priceFormat: { type: "volume" }, priceLineVisible: false, lastValueVisible: false },
      1,
    );
    instance.panes()[1]?.setHeight(90);
    const onMove = (param: MouseEventParams) => {
      const bar = price.current ? param.seriesData.get(price.current) : undefined;
      const vol = volume.current ? param.seriesData.get(volume.current) : undefined;
      if (!isBar(bar) || param.time === undefined) {
        setHovered(null);
        return;
      }
      setHovered({
        time: param.time as UTCTimestamp,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: isValue(vol) ? vol.value : null,
      });
    };
    instance.subscribeCrosshairMove(onMove);
    chart.current = instance;
    return () => {
      instance.unsubscribeCrosshairMove(onMove);
      instance.remove();
      chart.current = null;
      price.current = null;
      volume.current = null;
    };
  }, []);

  useEffect(() => {
    chart.current?.applyOptions({
      layout: {
        background: { type: ColorType.Solid, color: colors.surface },
        textColor: colors.muted,
        fontFamily: 'system-ui, -apple-system, "Segoe UI", sans-serif',
        panes: { separatorColor: colors.grid, separatorHoverColor: colors.axis },
      },
      grid: { vertLines: { color: colors.grid }, horzLines: { color: colors.grid } },
    });
    price.current?.applyOptions({
      upColor: colors.up,
      downColor: colors.down,
      wickUpColor: colors.up,
      wickDownColor: colors.down,
    });
    volume.current?.applyOptions({ color: colors.volume });
  }, [colors]);

  useEffect(() => {
    price.current?.setData(toCandleData(candles));
    volume.current?.setData(toVolumeData(candles));
  }, [candles]);

  // Fit the whole range when the product changes or data first arrives, not on every poll
  const isEmpty = candles.length === 0;
  useEffect(() => {
    chart.current?.timeScale().fitContent();
  }, [productId, isEmpty]);

  const latest = candles[candles.length - 1];
  const readout: Readout | null =
    hovered ??
    (latest
      ? {
          time: Date.parse(latest.time) / 1000,
          open: latest.open,
          high: latest.high,
          low: latest.low,
          close: latest.close,
          volume: latest.volume,
        }
      : null);

  return (
    <div>
      <ul className="legend" aria-label="Legend">
        <li>
          <span className="key-rect" style={{ background: colors.up }} aria-hidden="true" />
          Close ≥ open
        </li>
        <li>
          <span className="key-rect" style={{ background: colors.down }} aria-hidden="true" />
          Close &lt; open
        </li>
        <li>
          <span className="key-rect" style={{ background: colors.volume }} aria-hidden="true" />
          Volume (lower pane)
        </li>
      </ul>
      <div className="chart-readout" aria-live="off" data-testid="chart-readout">
        {readout ? (
          <>
            <span>{new Date(readout.time * 1000).toISOString().slice(0, 16).replace("T", " ")} UTC</span>
            <span>
              O <strong>{formatPrice(readout.open)}</strong>
            </span>
            <span>
              H <strong>{formatPrice(readout.high)}</strong>
            </span>
            <span>
              L <strong>{formatPrice(readout.low)}</strong>
            </span>
            <span>
              C <strong>{formatPrice(readout.close)}</strong>
            </span>
            <span>
              Vol <strong>{formatVolume(readout.volume)}</strong>
            </span>
          </>
        ) : null}
      </div>
      <div
        ref={container}
        className="chart-box"
        role="img"
        aria-label={`${productId} 1-minute candlestick chart with volume; use the table view for exact values`}
      />
    </div>
  );
}
