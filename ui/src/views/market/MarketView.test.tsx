import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { charts, lightweightChartsMock } from "../../test/chartMock";
import * as fixtures from "../../test/fixtures";
import { renderWithClient, stubApi } from "../../test/render";
import { MarketView } from "./MarketView";

vi.mock("lightweight-charts", () => lightweightChartsMock());

function marketCalls(fetchMock: ReturnType<typeof stubApi>) {
  return fetchMock.mock.calls
    .map(([input]) => new URL(input as string))
    .filter((url) => url.pathname === "/api/market/ohlcv")
    .map((url) => `${url.searchParams.get("product") ?? ""}:${url.searchParams.get("minutes") ?? ""}`);
}

describe("MarketView", () => {
  beforeEach(() => {
    charts.length = 0;
  });

  it("loads the first product and feeds candles and volume to the chart", async () => {
    const fetchMock = stubApi({ "/api/meta": fixtures.meta, "/api/market": fixtures.market() });
    renderWithClient(<MarketView />);
    expect(await screen.findByText("Last price")).toBeInTheDocument();
    expect(marketCalls(fetchMock)).toEqual(["BTC-USD:360"]);
    await waitFor(() => {
      expect(charts.at(-1)?.series.map((s) => s.type)).toEqual(["Candlestick", "Histogram"]);
    });
    const [candleSeries, volumeSeries] = charts.at(-1)?.series ?? [];
    expect(candleSeries?.setData).toHaveBeenLastCalledWith(
      expect.arrayContaining([expect.objectContaining({ open: 100 })]),
    );
    expect(volumeSeries?.setData).toHaveBeenLastCalledWith(
      expect.arrayContaining([expect.objectContaining({ value: 3 })]),
    );
    // The readout shows the latest candle without hovering
    expect(within(screen.getByTestId("chart-readout")).getByText("104.00")).toBeInTheDocument();
  });

  it("switching product or range refetches", async () => {
    const user = userEvent.setup();
    const fetchMock = stubApi({ "/api/meta": fixtures.meta, "/api/market": fixtures.market() });
    renderWithClient(<MarketView />);
    await screen.findByText("Last price");
    await user.selectOptions(screen.getByRole("combobox", { name: "Product" }), "ETH-USD");
    await user.click(screen.getByRole("button", { name: "24h" }));
    await waitFor(() => {
      expect(marketCalls(fetchMock)).toEqual(["BTC-USD:360", "ETH-USD:360", "ETH-USD:1440"]);
    });
    expect(screen.getByRole("button", { name: "24h" })).toHaveAttribute("aria-pressed", "true");
  });

  it("offers a table view with every value", async () => {
    const user = userEvent.setup();
    stubApi({ "/api/meta": fixtures.meta, "/api/market": fixtures.market() });
    renderWithClient(<MarketView />);
    await user.click(await screen.findByRole("button", { name: "Show table" }));
    const table = screen.getByRole("table");
    expect(within(table).getAllByRole("row")).toHaveLength(6); // header + 5 candles
  });

  it.each([
    [
      fixtures.market({ status: "inactive", candles: [] }),
      "No candles yet: the gold table hasn't been created.",
    ],
    [
      fixtures.market({ status: "down", error: "timed out after 4s", candles: [] }),
      "Couldn't read candles: timed out after 4s",
    ],
    [fixtures.market({ candles: [] }), "No trades in this range."],
  ])("empty and degraded states", async (response, message) => {
    stubApi({ "/api/meta": fixtures.meta, "/api/market": response });
    renderWithClient(<MarketView />);
    expect(await screen.findByText(message)).toBeInTheDocument();
  });

  it("shows an error when the API fails", async () => {
    stubApi({ "/api/meta": fixtures.meta, "/api/market": 500 });
    renderWithClient(<MarketView />);
    expect(await screen.findByText(/API returned 500/)).toBeInTheDocument();
  });
});
