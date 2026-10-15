import { describe, expect, it } from "vitest";

import {
  formatAgo,
  formatCount,
  formatDuration,
  formatMetric,
  formatPercent,
  formatPrice,
  formatRate,
  formatSignedPercent,
  formatTime,
  formatVolume,
} from "./format";

describe("formatCount", () => {
  it.each([
    [0, "0"],
    [1284, "1,284"],
    [9999, "9,999"],
    [12_900, "12.9K"],
    [4_200_000, "4.2M"],
    [null, "—"],
    [Number.NaN, "—"],
  ])("%s → %s", (value, expected) => {
    expect(formatCount(value)).toBe(expected);
  });
});

describe("formatDuration", () => {
  it.each([
    [0, "0s"],
    [59.4, "59s"],
    [61, "1m 1s"],
    [3600, "1h 0m"],
    [90_061, "1d 1h"],
    [-5, "0s"],
    [null, "—"],
  ])("%s → %s", (value, expected) => {
    expect(formatDuration(value)).toBe(expected);
  });
});

describe("number formats", () => {
  it("rate", () => {
    expect(formatRate(4.36)).toBe("4.4/s");
    expect(formatRate(1234)).toBe("1,234/s");
    expect(formatRate(null)).toBe("—");
  });

  it("percent", () => {
    expect(formatPercent(0.004)).toBe("0.4%");
    expect(formatSignedPercent(0.0123)).toBe("+1.23%");
    expect(formatSignedPercent(-0.0123)).toBe("−1.23%");
    expect(formatSignedPercent(0)).toBe("0.00%");
  });

  it("volume keeps fractional precision", () => {
    expect(formatVolume(0.73559)).toBe("0.7356");
    expect(formatVolume(174.44)).toBe("174.4");
    expect(formatVolume(12_900)).toBe("12.9K");
    expect(formatVolume(null)).toBe("—");
  });

  it("price precision follows magnitude", () => {
    expect(formatPrice(84123.456)).toBe("84,123.46");
    expect(formatPrice(0.0045123)).toBe("0.004512");
    expect(formatPrice(undefined)).toBe("—");
  });
});

describe("formatMetric", () => {
  it("uses the unit", () => {
    expect(formatMetric({ label: "Rows", value: 12_900, unit: "count" })).toBe("12.9K");
    expect(formatMetric({ label: "Rate", value: 2, unit: "per_second" })).toBe("2.0/s");
    expect(formatMetric({ label: "Age", value: 75, unit: "seconds" })).toBe("1m 15s");
    expect(formatMetric({ label: "Retention", value: 168, unit: "hours" })).toBe("168h");
    expect(formatMetric({ label: "Share", value: 0.25, unit: "ratio" })).toBe("25.0%");
    expect(formatMetric({ label: "Version", value: 42, unit: "version" })).toBe("v42");
    expect(formatMetric({ label: "Version", value: null, unit: "version" })).toBe("—");
  });
});

describe("time", () => {
  it("formats relative time", () => {
    const now = Date.parse("2026-10-15T12:00:00Z");
    expect(formatAgo("2026-10-15T11:59:30Z", now)).toBe("30s ago");
    expect(formatAgo(null, now)).toBe("—");
    expect(formatAgo("garbage", now)).toBe("—");
  });

  it("guards invalid timestamps", () => {
    expect(formatTime("garbage")).toBe("—");
    expect(formatTime(null)).toBe("—");
  });
});
