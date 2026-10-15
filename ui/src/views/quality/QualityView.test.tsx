import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import * as fixtures from "../../test/fixtures";
import { renderWithClient, stubApi } from "../../test/render";
import { QualityView } from "./QualityView";

describe("QualityView", () => {
  it("shows tiles, reasons, gaps and recent records", async () => {
    stubApi({ "/api/meta": fixtures.meta, "/api/quality": fixtures.quality() });
    renderWithClient(<QualityView />);
    const tiles = await screen.findByRole("region", { name: "Data quality" });
    expect(await within(tiles).findByText("37")).toBeInTheDocument();
    expect(within(tiles).getByText("0.40%")).toBeInTheDocument();
    expect(within(tiles).getByText("60 trades missing")).toBeInTheDocument();

    const bars = screen.getByRole("list", { name: "Quarantined records by reason" });
    expect(
      within(bars)
        .getAllByRole("listitem")
        .map((li) => li.textContent),
    ).toEqual(["unparseable time22", "missing price12", "invalid side3"]);
    const gaps = screen.getByRole("region", { name: "Open trade_id gaps" });
    expect(within(gaps).getByText("SOL-USD")).toBeInTheDocument();
    expect(screen.getByText('{"type":"match","time":"nope"}')).toBeInTheDocument();
  });

  it("each part degrades on its own", async () => {
    const base = fixtures.quality();
    stubApi({
      "/api/meta": fixtures.meta,
      "/api/quality": fixtures.quality({
        gaps: { status: "inactive", error: null, detected_at: null, gaps: [] },
        quarantine: { ...base.quarantine, status: "down", error: "timed out after 4s", total: null },
      }),
    });
    renderWithClient(<QualityView />);
    expect(
      await screen.findByText("The data-quality job hasn't run yet.", { selector: "span" }),
    ).toBeInTheDocument();
    expect(screen.getAllByText("Couldn't read the quarantine table: timed out after 4s")).toHaveLength(2);
    // Backfill share still renders
    expect(screen.getByText("0.40%")).toBeInTheDocument();
  });

  it("celebrates a clean pipeline", async () => {
    stubApi({
      "/api/meta": fixtures.meta,
      "/api/quality": fixtures.quality({
        quarantine: { status: "inactive", error: null, total: 0, reasons: [], recent: [] },
        gaps: { status: "ok", error: null, detected_at: fixtures.NOW, gaps: [] },
      }),
    });
    renderWithClient(<QualityView />);
    expect(
      await screen.findAllByText("No quarantined records. Every record passed validation."),
    ).toHaveLength(2);
    expect(screen.getByText("No missing trade_ids in the latest scan.")).toBeInTheDocument();
  });

  it("shows an error state when the API fails", async () => {
    stubApi({ "/api/meta": fixtures.meta, "/api/quality": new TypeError("Failed to fetch") });
    renderWithClient(<QualityView />);
    expect((await screen.findAllByText(/Couldn't load/)).length).toBe(4);
  });
});
