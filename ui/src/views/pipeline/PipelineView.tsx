import { usePipeline, useSnapshotAt } from "../../api/queries";
import type { PipelineResponse } from "../../api/types";
import { HealthBadge } from "../../components/HealthBadge";
import { overallHealth } from "../../lib/health";
import { Panel } from "../../components/Panel";
import { formatAgo, formatCount, formatDuration, formatRate, formatTime } from "../../lib/format";
import { PipelineDiagram } from "./PipelineDiagram";

export function PipelineView() {
  const query = usePipeline();
  const snapshotAt = useSnapshotAt();
  return (
    <div className="stack">
      <Panel
        title="Pipeline"
        subtitle={
          snapshotAt
            ? "State of each stage when the snapshot was taken"
            : "Live state of each stage, refreshed every few seconds"
        }
        query={query}
        actions={
          query.data ? (
            <span className="field">
              Overall <HealthBadge status={overallHealth(query.data.stages.map((s) => s.status))} />
            </span>
          ) : null
        }
      >
        {(data) => (
          <>
            <PipelineDiagram stages={data.stages} />
            <p className="footer">
              {snapshotAt
                ? `Snapshot taken ${formatTime(snapshotAt)}`
                : `Updated ${formatAgo(data.generated_at)}`}
            </p>
          </>
        )}
      </Panel>
      <div className="stack">
        <Panel
          title="Kafka → bronze"
          subtitle={query.data ? kafkaSubtitle(query.data, snapshotAt !== null) : undefined}
          query={query}
          isEmpty={(data) =>
            snapshotAt
              ? "Kafka isn't deployed for the snapshot; partitions and lag exist only while the pipeline runs"
              : data.kafka.status === "down"
                ? `Kafka unreachable: ${data.kafka.error ?? "unknown error"}`
                : data.kafka.partitions.length === 0
                  ? "The topic has no partitions"
                  : null
          }
        >
          {(data) => <PartitionTable data={data} />}
        </Panel>
        <Panel title="Lake tables" subtitle="Delta tables in the lake (from the Delta log)" query={query}>
          {(data) => <TablesTable data={data} />}
        </Panel>
      </div>
    </div>
  );
}

function kafkaSubtitle(data: PipelineResponse, snapshot: boolean): string {
  const k = data.kafka;
  if (snapshot) return `topic ${k.topic} · not deployed`;
  const parts = [`topic ${k.topic}`, `${formatRate(k.ingest_rate_per_second)} ingest`];
  if (k.retention_hours !== null) parts.push(`${formatCount(k.retention_hours)}h retention`);
  return parts.join(" · ");
}

function PartitionTable({ data }: { data: PipelineResponse }) {
  const lagByPartition = new Map(data.lag.partitions.map((p) => [p.partition, p]));
  return (
    <>
      {data.lag.error ? (
        <div className="notice" role="status">
          <span aria-hidden="true">⚠</span>
          <span>Lag unknown: {data.lag.error}</span>
        </div>
      ) : null}
      <div className="table-wrap">
        <table className="data">
          <caption className="sr-only">Kafka partitions and bronze lag</caption>
          <thead>
            <tr>
              <th scope="col">Partition</th>
              <th scope="col" className="num">
                Low
              </th>
              <th scope="col" className="num">
                High
              </th>
              <th scope="col" className="num">
                Retained
              </th>
              <th scope="col" className="num">
                Bronze next
              </th>
              <th scope="col" className="num">
                Lag
              </th>
            </tr>
          </thead>
          <tbody>
            {data.kafka.partitions.map((p) => {
              const lag = lagByPartition.get(p.partition);
              return (
                <tr key={p.partition}>
                  <td>{p.partition}</td>
                  <td className="num">{formatCount(p.low_watermark)}</td>
                  <td className="num">{formatCount(p.high_watermark)}</td>
                  <td className="num">{formatCount(p.high_watermark - p.low_watermark)}</td>
                  <td className="num">
                    {lag?.bronze_next_offset != null ? formatCount(lag.bronze_next_offset) : "—"}
                  </td>
                  <td className="num">{lag ? formatCount(lag.lag) : "—"}</td>
                </tr>
              );
            })}
          </tbody>
          {data.lag.total_lag !== null ? (
            <tfoot>
              <tr>
                <td colSpan={5}>
                  Total lag <HealthBadge status={data.lag.status} />
                </td>
                <td className="num">{formatCount(data.lag.total_lag)}</td>
              </tr>
            </tfoot>
          ) : null}
        </table>
      </div>
    </>
  );
}

function TablesTable({ data }: { data: PipelineResponse }) {
  return (
    <div className="table-wrap">
      <table className="data">
        <caption className="sr-only">Lake tables</caption>
        <thead>
          <tr>
            <th scope="col">Table</th>
            <th scope="col">Status</th>
            <th scope="col" className="num">
              Rows
            </th>
            <th scope="col" className="num">
              Freshness
            </th>
            <th scope="col" className="num">
              Version
            </th>
            <th scope="col">Last commit</th>
          </tr>
        </thead>
        <tbody>
          {data.tables.map((t) => (
            <tr key={t.id}>
              <td>
                {t.label}
                <br />
                <code>{t.path}</code>
              </td>
              <td className="wrap">
                <HealthBadge
                  status={t.status}
                  label={t.exists ? undefined : t.status === "down" ? "Down" : "Not yet"}
                />
                <div className="muted">{t.reason}</div>
              </td>
              <td className="num">{formatCount(t.row_count)}</td>
              <td className="num">{formatDuration(t.freshness_seconds)}</td>
              <td className="num">{t.version !== null ? `v${t.version}` : "—"}</td>
              <td>{formatTime(t.last_commit_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
