import { vi } from "vitest";

/** Stand-in for lightweight-charts (canvas isn't available in jsdom). Records what charts receive. */
export const charts: { series: { type: string; setData: ReturnType<typeof vi.fn> }[] }[] = [];

export function lightweightChartsMock() {
  const createChart = vi.fn(() => {
    const record: (typeof charts)[number] = { series: [] };
    charts.push(record);
    const pane = { setHeight: vi.fn() };
    return {
      addSeries: vi.fn((definition: { type: string }) => {
        const series = { type: definition.type, setData: vi.fn(), applyOptions: vi.fn() };
        record.series.push(series);
        return series;
      }),
      panes: () => [pane, pane],
      applyOptions: vi.fn(),
      subscribeCrosshairMove: vi.fn(),
      unsubscribeCrosshairMove: vi.fn(),
      timeScale: () => ({ fitContent: vi.fn() }),
      remove: vi.fn(),
    };
  });
  return {
    createChart,
    CandlestickSeries: { type: "Candlestick" },
    HistogramSeries: { type: "Histogram" },
    LineSeries: { type: "Line" },
    ColorType: { Solid: "solid" },
    CrosshairMode: { Magnet: 1, Normal: 0 },
    LineStyle: { Solid: 0 },
  };
}
