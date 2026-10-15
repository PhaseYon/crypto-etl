import { screen, waitFor, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { Prediction } from "../../api/types";
import { charts, lightweightChartsMock } from "../../test/chartMock";
import * as fixtures from "../../test/fixtures";
import { renderWithClient, stubApi } from "../../test/render";
import { MlView } from "./MlView";
import { latestByProduct, scoreSeries } from "./predictions";

vi.mock("lightweight-charts", () => lightweightChartsMock());

const pred = (product_id: string, minute: number, score: number | null): Prediction => ({
  product_id,
  window_start: new Date(Date.UTC(2026, 9, 15, 12, minute)).toISOString(),
  score,
  model_version: "1",
  created_at: null,
});

describe("prediction helpers", () => {
  it("latest per product follows the configured order", () => {
    const latest = latestByProduct(
      [pred("ETH-USD", 1, 0.4), pred("BTC-USD", 1, 0.5), pred("BTC-USD", 3, 0.6)],
      ["BTC-USD", "ETH-USD"],
    );
    expect(latest.map((p) => [p.product_id, p.score])).toEqual([
      ["BTC-USD", 0.6],
      ["ETH-USD", 0.4],
    ]);
  });

  it("colors follow the product, not its rank in the data", () => {
    const series = scoreSeries([pred("SOL-USD", 1, 0.5)], ["BTC-USD", "ETH-USD", "SOL-USD"]);
    expect(series).toEqual([{ product: "SOL-USD", slot: 2, data: [{ time: 1_792_065_660, value: 0.5 }] }]);
  });

  it("drops null scores, sorts points and caps the number of series", () => {
    const series = scoreSeries(
      [pred("A-USD", 2, 0.2), pred("A-USD", 1, 0.1), pred("A-USD", 3, null), pred("D-USD", 1, 0.9)],
      ["A-USD", "B-USD", "C-USD", "D-USD"],
    );
    expect(series.map((s) => s.product)).toEqual(["A-USD"]);
    expect(series[0]?.data.map((d) => d.value)).toEqual([0.1, 0.2]);
  });
});

describe("MlView", () => {
  beforeEach(() => {
    charts.length = 0;
  });

  it("charts scores per product and lists the latest prediction", async () => {
    stubApi({ "/api/meta": fixtures.meta, "/api/ml/predictions": fixtures.predictions() });
    renderWithClient(<MlView />);
    const table = await screen.findByRole("table");
    expect(within(table).getAllByRole("row")).toHaveLength(3); // header + BTC + ETH
    expect(within(table).getByText("0.600")).toBeInTheDocument();
    await waitFor(() => {
      expect(charts.at(-1)?.series.map((s) => s.type)).toEqual(["Line", "Line"]);
    });
    const legend = screen.getByRole("list", { name: "Legend" });
    expect(
      within(legend)
        .getAllByRole("listitem")
        .map((li) => li.textContent),
    ).toEqual(["BTC-USD", "ETH-USD"]);
  });

  it("says when predictions aren't available yet", async () => {
    stubApi({
      "/api/meta": fixtures.meta,
      "/api/ml/predictions": fixtures.predictions({
        status: "inactive",
        reason: "ML predictions not available yet",
        predictions: [],
      }),
    });
    renderWithClient(<MlView />);
    expect(await screen.findByText(/ML predictions not available yet/)).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("shows read failures", async () => {
    stubApi({
      "/api/meta": fixtures.meta,
      "/api/ml/predictions": fixtures.predictions({ status: "down", error: "boom", predictions: [] }),
    });
    renderWithClient(<MlView />);
    expect(await screen.findByText("Couldn't read predictions: boom")).toBeInTheDocument();
  });
});
