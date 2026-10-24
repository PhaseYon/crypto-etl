import { expect, test } from "@playwright/test";

// Smoke test against the API in mock mode (DATA_SOURCE=mock): deterministic, no lake needed.
// E2E_SNAPSHOT=1 runs it against a deployed snapshot instead (real data, no Kafka, no predictions).
const snapshot = process.env.E2E_SNAPSHOT === "1";

test("pipeline view renders every stage", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "crypto-etl" })).toBeVisible();
  await expect(page.getByText(snapshot ? /^Snapshot · / : "Mock data")).toBeVisible();
  const stages = page.getByRole("list", { name: /pipeline stages/i }).getByRole("listitem");
  await expect(stages).toHaveCount(7);
  for (const name of [
    "Coinbase WebSocket",
    "Producer",
    "Kafka raw.trades",
    "Bronze",
    "Silver",
    "Gold",
    "ML",
  ]) {
    await expect(page.getByRole("heading", { name, exact: true })).toBeVisible();
  }
  await expect(page.getByTestId("stage-gold").getByText("OK")).toBeVisible();
  await expect(page.getByRole("region", { name: "Lake tables" }).getByText("gold/ohlcv_1m")).toBeVisible();
});

test("switching product updates the chart", async ({ page }) => {
  await page.goto("/#/market");
  await expect(page.getByRole("heading", { name: "BTC-USD · 1-minute candles" })).toBeVisible();
  const lastPrice = page.locator(".tile").filter({ hasText: "Last price" }).locator(".tile-value");
  const btc = Number((await lastPrice.textContent())?.replace(/,/g, ""));
  expect(btc).toBeGreaterThan(10_000);
  await expect(page.locator(".chart-box canvas").first()).toBeVisible();

  await page.getByRole("combobox", { name: "Product" }).selectOption("ETH-USD");
  await expect(page.getByRole("heading", { name: "ETH-USD · 1-minute candles" })).toBeVisible();
  await expect
    .poll(async () => Number((await lastPrice.textContent())?.replace(/,/g, "")))
    .toBeLessThan(10_000);

  // The table view is the chart's accessible twin
  await page.getByRole("button", { name: "Show table" }).click();
  await expect(page.getByRole("table", { name: "Candles, newest first" })).toBeVisible();
});

test("data quality and ML views load", async ({ page }) => {
  await page.goto("/#/quality");
  if (snapshot) {
    // The snapshot passed validation, so the reasons list is an empty state
    await expect(
      page.getByText("No quarantined records. Every record passed validation.").first(),
    ).toBeVisible();
  } else {
    await expect(page.getByRole("list", { name: "Quarantined records by reason" })).toBeVisible();
  }
  await page.getByRole("tab", { name: "ML" }).click();
  await expect(page).toHaveURL(/#\/ml$/);
  await expect(page.getByRole("heading", { name: "Model predictions" })).toBeVisible();
  if (!snapshot) {
    // Named lookup: the chart library lays out its canvas with an internal <table>
    await expect(page.getByRole("table", { name: "Latest prediction per product" })).toBeVisible();
  }
});

test("theme toggle switches to dark", async ({ page }) => {
  await page.goto("/");
  const toggle = page.getByRole("button", { name: /Theme:/ });
  await toggle.click(); // system → light
  await toggle.click(); // light → dark
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
});
