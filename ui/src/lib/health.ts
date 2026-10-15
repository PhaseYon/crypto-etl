import type { Health } from "../api/types";

const SEVERITY: Record<Health, number> = { inactive: 0, ok: 1, stale: 2, down: 3 };

/** Overall status of a set of stages: the worst one wins; inactive stages don't count. */
export function overallHealth(statuses: Health[]): Health {
  const active = statuses.filter((s) => s !== "inactive");
  if (active.length === 0) return "inactive";
  return active.reduce((worst, s) => (SEVERITY[s] > SEVERITY[worst] ? s : worst), "ok");
}
