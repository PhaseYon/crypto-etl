import type { Stage } from "../../api/types";
import { HealthBadge } from "../../components/HealthBadge";
import { formatMetric } from "../../lib/format";

/** Coinbase → producer → Kafka → bronze → silver → gold → ML, one card per stage. */
export function PipelineDiagram({ stages }: { stages: Stage[] }) {
  return (
    <ol className="flow" aria-label="Pipeline stages, in data-flow order">
      {stages.map((stage) => (
        <li key={stage.id} className="flow-step">
          <StageCard stage={stage} />
        </li>
      ))}
    </ol>
  );
}

export function StageCard({ stage }: { stage: Stage }) {
  return (
    <article className="stage" data-status={stage.status} data-testid={`stage-${stage.id}`}>
      <div>
        <h3 className="stage-name">{stage.label}</h3>
      </div>
      <HealthBadge status={stage.status} />
      <p className="stage-reason">{stage.reason}</p>
      {stage.metrics.length > 0 ? (
        <dl className="stage-metrics">
          {stage.metrics.map((metric) => (
            <div key={metric.label}>
              <dt>{metric.label}</dt>
              <dd>{formatMetric(metric)}</dd>
            </div>
          ))}
        </dl>
      ) : null}
    </article>
  );
}
