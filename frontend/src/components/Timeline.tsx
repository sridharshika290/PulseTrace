import type { AnomalyRecord, AuditEvent } from '../types';

type TimelineProps = {
  events: AuditEvent[];
  anomalies: AnomalyRecord[];
  title?: string;
  eyebrow?: string;
  topicFilter?: string | null;
};

export function Timeline({ events, anomalies, title = 'Recent flow', eyebrow = 'Timeline', topicFilter = null }: TimelineProps) {
  const items = (topicFilter ? events.filter((e) => e.topic === topicFilter) : events).slice(0, 8);

  return (
    <section className="panel timeline-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">{eyebrow}</p>
          <h2>{title}</h2>
        </div>
      </div>
      <div className="timeline">
        {items.map((event) => {
          const hit = anomalies.find((anomaly) => anomaly.topic === event.topic && anomaly.partition === event.partition);
          return (
            <article className="timeline-item" key={`${event.message_id}-${event.timestamp}`}>
              <div className={`timeline-dot ${event.kind}`} />
              <div className="timeline-copy">
                <div className="timeline-topline">
                  <strong>{event.message_id}</strong>
                  <span>{event.topic} / p{event.partition}</span>
                </div>
                <p>
                  Seq {event.sequence} · {event.kind} · {Math.round(event.processing_ms ?? 0)}ms processing
                </p>
                {hit ? <small className="alert-chip">{hit.message}</small> : null}
              </div>
            </article>
          );
        })}
      </div>
    </section>
  );
}
