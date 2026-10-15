import { describe, expect, it } from "vitest";

import { candles } from "../../test/fixtures";
import { summarize, toCandleData, toTimestamp, toVolumeData } from "./candles";

describe("candle mapping", () => {
  it("converts ISO times to UTC seconds", () => {
    expect(toTimestamp("2026-10-15T12:00:00Z")).toBe(1_792_065_600);
  });

  it("sorts by time and drops duplicate minutes (last write wins)", () => {
    const [a, b] = candles(2);
    if (!a || !b) throw new Error("fixture");
    const rows = toCandleData([b, a, { ...b, close: 999 }]);
    expect(rows.map((r) => r.time)).toEqual([toTimestamp(a.time), toTimestamp(b.time)]);
    expect(rows[1]?.close).toBe(999);
  });

  it("skips rows with unparseable times", () => {
    const [a] = candles(1);
    if (!a) throw new Error("fixture");
    expect(toCandleData([a, { ...a, time: "garbage" }])).toHaveLength(1);
    expect(toVolumeData([{ ...a, time: "garbage" }])).toHaveLength(0);
  });

  it("maps volume", () => {
    expect(toVolumeData(candles(2)).map((v) => v.value)).toEqual([3, 4]);
  });
});

describe("summarize", () => {
  it("computes last, change, range and totals", () => {
    const s = summarize(candles(3));
    expect(s).toEqual({ last: 103, change: 103 / 100 - 1, high: 104, low: 99, volume: 12, trades: 33 });
  });

  it("is null without candles", () => {
    expect(summarize([])).toBeNull();
  });
});
