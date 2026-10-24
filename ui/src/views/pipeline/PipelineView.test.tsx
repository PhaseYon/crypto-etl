import { act, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import * as fixtures from "../../test/fixtures";
import { renderWithClient, stubApi } from "../../test/render";
import { PipelineView } from "./PipelineView";

describe("PipelineView", () => {
  it("shows a loading state first", () => {
    stubApi({ "/api/meta": fixtures.meta, "/api/pipeline": () => new Promise(() => undefined) });
    renderWithClient(<PipelineView />);
    expect(screen.getByText("Loading Pipeline")).toBeInTheDocument();
  });

  it("renders every stage in flow order with status and reason", async () => {
    stubApi({ "/api/meta": fixtures.meta, "/api/pipeline": fixtures.pipeline() });
    renderWithClient(<PipelineView />);
    const flow = await screen.findByRole("list", { name: /pipeline stages/i });
    const names = within(flow)
      .getAllByRole("heading")
      .map((h) => h.textContent);
    expect(names).toEqual([
      "Coinbase WebSocket",
      "Producer",
      "Kafka raw.trades",
      "Bronze",
      "Silver",
      "Gold",
      "ML",
    ]);
    const gold = screen.getByTestId("stage-gold");
    expect(within(gold).getByText("Down")).toBeInTheDocument();
    expect(within(gold).getByText("OSError: connection refused")).toBeInTheDocument();
    expect(
      within(screen.getByTestId("stage-ml")).getByText("ML predictions not available yet"),
    ).toBeInTheDocument();
    // Overall status is the worst active stage
    expect(
      within(screen.getByRole("region", { name: "Pipeline" })).getAllByText("Down").length,
    ).toBeGreaterThan(1);
  });

  it("shows partition lag and lake tables", async () => {
    stubApi({ "/api/meta": fixtures.meta, "/api/pipeline": fixtures.pipeline() });
    renderWithClient(<PipelineView />);
    const kafka = await screen.findByRole("region", { name: "Kafka → bronze" });
    expect(await within(kafka).findByText("2,093")).toBeInTheDocument();
    expect(within(kafka).getByText(/Total lag/)).toBeInTheDocument();
    const tables = screen.getByRole("region", { name: "Lake tables" });
    expect(within(tables).getByText("bronze/trades")).toBeInTheDocument();
    expect(within(tables).getByText("Not yet")).toBeInTheDocument();
  });

  it("shows Kafka as unreachable but still renders the lake", async () => {
    const data = fixtures.pipeline({
      kafka: { ...fixtures.pipeline().kafka, status: "down", error: "timed out after 4s", partitions: [] },
    });
    stubApi({ "/api/meta": fixtures.meta, "/api/pipeline": data });
    renderWithClient(<PipelineView />);
    expect(await screen.findByText("Kafka unreachable: timed out after 4s")).toBeInTheDocument();
    expect(screen.getByText("bronze/trades")).toBeInTheDocument();
  });

  it("shows an error state when the API is unreachable", async () => {
    stubApi({ "/api/meta": fixtures.meta, "/api/pipeline": new TypeError("Failed to fetch") });
    renderWithClient(<PipelineView />);
    expect((await screen.findAllByText(/Couldn't load/)).length).toBe(3);
    expect(screen.getAllByText("API unreachable: Failed to fetch").length).toBe(3);
  });

  it("keeps the last data visible when a refresh fails", async () => {
    let fail = false;
    stubApi({
      "/api/meta": fixtures.meta,
      "/api/pipeline": () => (fail ? 503 : fixtures.pipeline()),
    });
    const { client } = renderWithClient(<PipelineView />);
    await screen.findByTestId("stage-gold");
    fail = true;
    await act(async () => {
      await client.refetchQueries({ queryKey: ["pipeline"] });
    });
    expect((await screen.findAllByText(/refresh failing/)).length).toBeGreaterThan(0);
    expect(screen.getByTestId("stage-gold")).toBeInTheDocument();
  });

  it("labels a snapshot instead of claiming to be live", async () => {
    const snapshot = { ...fixtures.meta, data_source: "real" as const, snapshot_at: "2026-10-22T08:10:00Z" };
    stubApi({ "/api/meta": snapshot, "/api/pipeline": fixtures.pipeline() });
    renderWithClient(<PipelineView />);
    expect(await screen.findByText(/^Snapshot taken /)).toBeInTheDocument();
    expect(screen.getByText("State of each stage when the snapshot was taken")).toBeInTheDocument();
    expect(screen.queryByText(/^Updated /)).not.toBeInTheDocument();
  });
});
