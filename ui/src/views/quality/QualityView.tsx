import { useQuality } from "../../api/queries";
import type { BackfillShare, GapsSummary, QualityResponse, QuarantineSummary } from "../../api/types";
import { HealthBadge } from "../../components/HealthBadge";
import { Panel } from "../../components/Panel";
import { StatTile } from "../../components/StatTile";
import { formatAgo, formatCount, formatPercent, formatTime } from "../../lib/format";

export function QualityView() {
  const query = useQuality();
  return (
    <div className="stack">
      <Panel
        title="Data quality"
        subtitle="Silver validation, trade_id continuity and backfill"
        query={query}
      >
        {(data) => <QualityTiles data={data} />}
      </Panel>
      <div className="grid-2">
        <Panel
          title="Quarantine reasons"
          subtitle="Records that failed parsing or validation (silver/trades_quarantine)"
          query={query}
          isEmpty={(data) => quarantineEmpty(data.quarantine)}
        >
          {(data) => <ReasonBars quarantine={data.quarantine} />}
        </Panel>
        <Panel
          title="Open trade_id gaps"
          subtitle={
            query.data?.gaps.detected_at ? `Latest scan ${formatAgo(query.data.gaps.detected_at)}` : undefined
          }
          query={query}
          isEmpty={(data) => gapsEmpty(data.gaps)}
        >
          {(data) => <GapsTable gaps={data.gaps} />}
        </Panel>
      </div>
      <Panel
        title="Recently quarantined"
        query={query}
        isEmpty={(data) =>
          quarantineEmpty(data.quarantine) ?? (data.quarantine.recent.length === 0 ? "Nothing recent." : null)
        }
      >
        {(data) => <RecentQuarantine quarantine={data.quarantine} />}
      </Panel>
    </div>
  );
}

function quarantineEmpty(q: QuarantineSummary): string | null {
  if (q.status === "down") return `Couldn't read the quarantine table: ${q.error ?? "unknown error"}`;
  if (q.status === "inactive" || q.total === 0)
    return "No quarantined records. Every record passed validation.";
  return null;
}

function gapsEmpty(g: GapsSummary): string | null {
  if (g.status === "down") return `Couldn't read ops/trade_gaps: ${g.error ?? "unknown error"}`;
  if (g.status === "inactive") return "The data-quality job hasn't run yet.";
  if (g.gaps.length === 0) return "No missing trade_ids in the latest scan.";
  return null;
}

function backfillNote(b: BackfillShare): string {
  if (b.status === "down") return `unavailable: ${b.error ?? "unknown error"}`;
  if (b.status === "inactive") return "silver table not created yet";
  return `${formatCount(b.backfilled_rows)} of ${formatCount(b.total_rows)} rows · last ${b.lookback_days}d`;
}

function QualityTiles({ data }: { data: QualityResponse }) {
  const { quarantine, gaps, backfill } = data;
  const missing = gaps.gaps.reduce((sum, g) => sum + g.missing_count, 0);
  return (
    <div className="tiles">
      <StatTile
        label="Quarantined records"
        value={quarantine.status === "down" ? "—" : formatCount(quarantine.total ?? 0)}
        note={
          quarantine.status === "down" ? (
            <HealthBadge status="down" />
          ) : (
            `${quarantine.reasons.length} distinct reasons`
          )
        }
      />
      <StatTile
        label="Open trade_id gaps"
        value={gaps.status === "ok" ? formatCount(gaps.gaps.length) : "—"}
        note={gaps.status === "ok" ? `${formatCount(missing)} trades missing` : gapsEmpty(gaps)}
      />
      <StatTile
        label="Backfilled share"
        value={backfill.status === "ok" ? formatPercent(backfill.share, 2) : "—"}
        note={backfillNote(backfill)}
      />
    </div>
  );
}

/** One series, so one color (slot 1) for every bar; the value sits at the bar's end in ink. */
function ReasonBars({ quarantine }: { quarantine: QuarantineSummary }) {
  const max = Math.max(1, ...quarantine.reasons.map((r) => r.count));
  return (
    <ul className="bars" aria-label="Quarantined records by reason">
      {quarantine.reasons.map((r) => (
        <li key={r.error} className="bar-row">
          <span className="bar-label">{r.error}</span>
          <span className="bar-track">
            <span className="bar" style={{ width: `${(r.count / max) * 80}%` }} aria-hidden="true" />
            <span className="bar-value">{formatCount(r.count)}</span>
          </span>
        </li>
      ))}
    </ul>
  );
}

function GapsTable({ gaps }: { gaps: GapsSummary }) {
  return (
    <>
      <div className="table-wrap">
        <table className="data">
          <caption className="sr-only">Missing trade_id ranges</caption>
          <thead>
            <tr>
              <th scope="col">Product</th>
              <th scope="col" className="num">
                First missing
              </th>
              <th scope="col" className="num">
                Last missing
              </th>
              <th scope="col" className="num">
                Missing
              </th>
            </tr>
          </thead>
          <tbody>
            {gaps.gaps.map((g) => (
              <tr key={`${g.product_id}:${g.gap_start_id}`}>
                <td>{g.product_id}</td>
                <td className="num">{g.gap_start_id}</td>
                <td className="num">{g.gap_end_id}</td>
                <td className="num">{formatCount(g.missing_count)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="footer">These ranges can be fed to the producer's backfill CLI.</p>
    </>
  );
}

function RecentQuarantine({ quarantine }: { quarantine: QuarantineSummary }) {
  return (
    <div className="table-wrap">
      <table className="data">
        <caption className="sr-only">Most recently quarantined records</caption>
        <thead>
          <tr>
            <th scope="col">When</th>
            <th scope="col">Reason</th>
            <th scope="col">Kafka position</th>
            <th scope="col">Raw value</th>
          </tr>
        </thead>
        <tbody>
          {quarantine.recent.map((r, i) => (
            <tr key={`${String(r.kafka_partition)}:${String(r.kafka_offset)}:${String(i)}`}>
              <td>{formatTime(r.quarantined_at)}</td>
              <td>{r.error}</td>
              <td>
                {r.kafka_partition !== null && r.kafka_offset !== null
                  ? `p${String(r.kafka_partition)} @ ${String(r.kafka_offset)}`
                  : "—"}
              </td>
              <td className="wrap">
                <code>{r.value_preview}</code>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
