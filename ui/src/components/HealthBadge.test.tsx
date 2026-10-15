import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { overallHealth } from "../lib/health";
import { HealthBadge } from "./HealthBadge";

describe("HealthBadge", () => {
  it.each([
    ["ok", "OK"],
    ["stale", "Stale"],
    ["down", "Down"],
    ["inactive", "Inactive"],
  ] as const)("%s shows a text label, not color alone", (status, label) => {
    render(<HealthBadge status={status} />);
    const badge = screen.getByText(label);
    expect(badge).toHaveAttribute("data-status", status);
  });

  it("accepts a custom label", () => {
    render(<HealthBadge status="inactive" label="Not yet" />);
    expect(screen.getByText("Not yet")).toBeInTheDocument();
  });
});

describe("overallHealth", () => {
  it("worst active status wins", () => {
    expect(overallHealth(["ok", "stale", "ok"])).toBe("stale");
    expect(overallHealth(["ok", "down", "stale"])).toBe("down");
    expect(overallHealth(["ok", "inactive"])).toBe("ok");
  });

  it("all inactive is inactive", () => {
    expect(overallHealth(["inactive", "inactive"])).toBe("inactive");
    expect(overallHealth([])).toBe("inactive");
  });
});
