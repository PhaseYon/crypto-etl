import type { Health } from "../api/types";
import { HEALTH_LABEL } from "../lib/format";

const ICON: Record<Health, string> = { ok: "✓", stale: "!", down: "✕", inactive: "" };

/** Status is never color alone: every badge carries an icon and a text label. */
export function HealthBadge({ status, label }: { status: Health; label?: string | undefined }) {
  return (
    <span className="badge" data-status={status}>
      <span className="badge-icon" aria-hidden="true">
        {ICON[status]}
      </span>
      {label ?? HEALTH_LABEL[status]}
    </span>
  );
}
