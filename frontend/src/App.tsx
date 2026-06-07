import { useEffect, useMemo, useState } from 'react';
import {
  fetchAnomalies,
  fetchEvents,
  fetchHeatmap,
  fetchReplayAnomalies,
  fetchReplayEvents,
  fetchSummary,
  postEvent,
} from './api';
import { MetricCard } from './components/MetricCard';
import { Sidebar } from './components/Sidebar';
import { Timeline } from './components/Timeline';
import type { AnomalyRecord, AuditEvent, HeatmapPoint, Summary } from './types';

const defaultSummary: Summary = {
  total_messages: 0,
  duplicates: 0,
  out_of_order: 0,
  lag_spikes: 0,
  dlq_bursts: 0,
  slow_processes: 0,
  active_topics: 0,
};

function formatBucketedHeatmap(points: HeatmapPoint[]) {
  const buckets = Array.from(new Set(points.map((point) => point.bucket))).slice(-8);
  const topics = Array.from(new Set(points.map((point) => point.topic)));
  return { buckets, topics };
}

function createBaseEvent(overrides: Partial<AuditEvent>): AuditEvent {
  return {
    message_id: `test-${crypto.randomUUID()}`,
    topic: 'payments',
    partition: 0,
    sequence: 1,
    kind: 'producer',
    source: 'dashboard-test',
    timestamp: new Date().toISOString(),
    processing_ms: 120,
    lag_ms: 120,
    payload: { amount: 42 },
    ...overrides,
  };
}

export default function App() {
  const [summary, setSummary] = useState<Summary>(defaultSummary);
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [anomalies, setAnomalies] = useState<AnomalyRecord[]>([]);
  const [heatmap, setHeatmap] = useState<HeatmapPoint[]>([]);
  const [replayEvents, setReplayEvents] = useState<AuditEvent[]>([]);
  const [replayAnomalies, setReplayAnomalies] = useState<AnomalyRecord[]>([]);
  const [replayLoaded, setReplayLoaded] = useState(false);
  const [status, setStatus] = useState('warming up');
  const [testStatus, setTestStatus] = useState('');
  const [isTesting, setIsTesting] = useState(false);
  const [deltas, setDeltas] = useState<Record<string, string>>({});

  useEffect(() => {
    let mounted = true;

    const hydrate = async () => {
      try {
        const [summaryData, eventData, anomalyData, heatmapData] = await Promise.all([
          fetchSummary(),
          fetchEvents(),
          fetchAnomalies(),
          fetchHeatmap(),
        ]);

        if (!mounted) {
          return;
        }

        // summaryData may include deltas: { ...summaryFields, deltas: { messages: '%'}} or {summary: {...}, deltas: {...}}
        if (summaryData?.summary) {
          setSummary(summaryData.summary);
          setDeltas(summaryData.deltas ?? {});
        } else {
          setSummary(summaryData);
          setDeltas((summaryData as any)?.deltas ?? {});
        }
        setEvents(eventData);
        setAnomalies(anomalyData);
        setHeatmap(heatmapData);
        setStatus('live');
      } catch {
        if (mounted) {
          setStatus('offline');
        }
      }
    };

    void hydrate();
    const interval = window.setInterval(() => {
      void hydrate();
    }, 2500);

    const socketUrl = (import.meta.env.VITE_API_URL?.replace('http', 'ws') ?? 'ws://localhost:8000') + '/ws/live';

    let backoff = 500;
    let closed = false;

    const connect = () => {
      if (!mounted || closed) return;
      const socket = new WebSocket(socketUrl);

      socket.onopen = () => {
        backoff = 500;
        if (mounted) setStatus('live');
      };

      socket.onmessage = (msg) => {
        try {
          const payload = JSON.parse(msg.data) as any;
          // summary update
          if (payload.summary) {
            const s = payload.summary;
            setSummary(() => s);
            setDeltas(() => s.deltas ?? {});
          }
          // events: prepend new events to the timeline
          if (payload.events) {
            setEvents((prev) => {
              const merged = Array.isArray(payload.events) ? [...payload.events, ...prev] : prev;
              return merged.slice(0, 200);
            });
          }
          // anomalies: prepend anomalies
          if (payload.anomalies) {
            setAnomalies((prev) => {
              const merged = Array.isArray(payload.anomalies) ? [...payload.anomalies, ...prev] : prev;
              return merged.slice(0, 200);
            });
          }
          // heatmap refresh
          if (payload.heatmap) {
            setHeatmap(() => payload.heatmap);
          }
        } catch (e) {
          // ignore malformed payloads
        }
      };

      socket.onerror = () => {
        if (mounted) setStatus('polling');
      };

      socket.onclose = () => {
        if (!mounted || closed) return;
        setStatus('reconnecting');
        setTimeout(() => {
          backoff = Math.min(30000, backoff * 1.6 + 100);
          connect();
        }, backoff);
      };
    };

    connect();

    return () => {
      mounted = false;
      window.clearInterval(interval);
      closed = true;
      // closing any open socket is handled by letting the socket instance go out of scope
    };
  }, []);

  const heatmapView = useMemo(() => formatBucketedHeatmap(heatmap), [heatmap]);
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);
  const computeDlqWritesPerMin = (topic?: string | null) => {
    if (!heatmap || heatmap.length === 0) return 0;
    const buckets = Array.from(new Set(heatmap.map((p) => p.bucket))).slice(-8);
    const bucketCount = buckets.length || 1;
    const total = heatmap.filter((p) => (topic ? p.topic === topic : true)).reduce((s, p) => s + p.count, 0);
    const minutes = bucketCount * 5; // assume 5-min buckets
    return Math.round((total / Math.max(1, minutes)));
  };

  const computeAnomalyDelta = (kind: string, topic?: string | null) => {
    const now = Date.now();
    const windowMs = 15 * 60 * 1000;
    const lastStart = now - windowMs;
    const prevStart = now - windowMs * 2;
    const lastCount = anomalies.filter((a) => a.kind === kind && (!topic || a.topic === topic) && new Date(a.created_at).getTime() >= lastStart).length;
    const prevCount = anomalies.filter((a) => a.kind === kind && (!topic || a.topic === topic) && new Date(a.created_at).getTime() >= prevStart && new Date(a.created_at).getTime() < lastStart).length;
    if (prevCount === 0) return lastCount > 0 ? '+100%' : '0%';
    const pct = ((lastCount - prevCount) / prevCount) * 100;
    return `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%`;
  };

  const refreshSnapshot = async () => {
    const [summaryData, eventData, anomalyData, heatmapData] = await Promise.all([
      fetchSummary(),
      fetchEvents(),
      fetchAnomalies(),
      fetchHeatmap(),
    ]);
    setSummary(summaryData);
    setEvents(eventData);
    setAnomalies(anomalyData);
    setHeatmap(heatmapData);
  };

  const loadReplay = async () => {
    const [replayEventData, replayAnomalyData] = await Promise.all([fetchReplayEvents(), fetchReplayAnomalies()]);
    setReplayEvents(replayEventData);
    setReplayAnomalies(replayAnomalyData);
    setReplayLoaded(true);
  };

  const sendTestEvents = async (eventsToSend: AuditEvent[], label: string) => {
    setIsTesting(true);
    setTestStatus(`running ${label} test...`);
    try {
      for (const event of eventsToSend) {
        await postEvent(event);
      }
      setTestStatus(`${label} test sent`);
      await refreshSnapshot();
    } catch (error) {
      setTestStatus(error instanceof Error ? error.message : 'test failed');
    } finally {
      setIsTesting(false);
    }
  };

  const runDuplicateTest = async () => {
    const event = createBaseEvent({ message_id: `dup-${crypto.randomUUID()}` });
    await sendTestEvents([event, { ...event, timestamp: new Date(Date.now() + 1000).toISOString() }], 'duplicate');
  };

  const runOutOfOrderTest = async () => {
    await sendTestEvents(
      [
        createBaseEvent({ message_id: `seq-${crypto.randomUUID()}`, sequence: 20, topic: 'contracts', partition: 1 }),
        createBaseEvent({ message_id: `seq-${crypto.randomUUID()}`, sequence: 19, topic: 'contracts', partition: 1 }),
      ],
      'out-of-order'
    );
  };

  const runLagSpikeTest = async () => {
    await sendTestEvents(
      [
        createBaseEvent({ message_id: `lag-${crypto.randomUUID()}`, topic: 'notifications', lag_ms: 150 }),
        createBaseEvent({ message_id: `lag-${crypto.randomUUID()}`, topic: 'notifications', lag_ms: 180 }),
        createBaseEvent({ message_id: `lag-${crypto.randomUUID()}`, topic: 'notifications', lag_ms: 3200 }),
      ],
      'lag spike'
    );
  };

  const runDlqTest = async () => {
    await sendTestEvents([createBaseEvent({ message_id: `dlq-${crypto.randomUUID()}`, kind: 'dlq', topic: 'payments', partition: 0 })], 'dlq');
  };

  const runSlowConsumerTest = async () => {
    await sendTestEvents(
      [createBaseEvent({ message_id: `slow-${crypto.randomUUID()}`, topic: 'payments', processing_ms: 2400, lag_ms: 2400 })],
      'slow consumer'
    );
  };

  return (
    <div className="app-shell">
      <Sidebar
        topics={heatmapView.topics}
        selected={selectedTopic}
        onSelect={setSelectedTopic}
        onNavSelect={(id: string) => {
          const el = document.getElementById(id);
          if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }}
      />
      <div className="shell">
      <div className="backdrop backdrop-a" />
      <div className="backdrop backdrop-b" />

      <header id="hero" className="hero panel">
        <div>
          <p className="eyebrow">PulseTrace</p>
          <h1>Audit every message path without touching producer or consumer code.</h1>
          {selectedTopic ? <div className="selected-topic">Viewing: {selectedTopic}</div> : null}
          <p className="lede">
            Duplicate detection, ordering checks, lag spikes, DLQ bursts, and slow-processing outliers in one
            self-contained sidecar.
          </p>
        </div>
        <div className="status-pill">
          <span className={`status-dot ${status}`} />
          <span>{status}</span>
        </div>
        <button className="replay-button" type="button" onClick={() => void loadReplay()}>
          Load replay
        </button>
      </header>

      <section id="metrics-cards" className="metrics-grid">
        <MetricCard label="Messages observed" value={summary.total_messages} accent="#f59e0b" delta={deltas.messages} />
        <MetricCard label="Duplicates" value={summary.duplicates} accent="#fb7185" delta={computeAnomalyDelta('duplicate', selectedTopic)} />
        <MetricCard label="Out of order" value={summary.out_of_order} accent="#f97316" delta={computeAnomalyDelta('out_of_order', selectedTopic)} />
        <MetricCard label="Lag spikes" value={summary.lag_spikes} accent="#38bdf8" delta={computeAnomalyDelta('lag_spike', selectedTopic)} />
        <MetricCard label="DLQ writes / min" value={computeDlqWritesPerMin(selectedTopic)} accent="#a78bfa" subtitle={`${computeDlqWritesPerMin(selectedTopic)} writes / min`} />
        <MetricCard label="Slow consumers" value={summary.slow_processes} accent="#22c55e" delta={computeAnomalyDelta('processing_outlier', selectedTopic)} />
      </section>


      <section className="main-grid">
        <div id="timeline">
          <Timeline events={events} anomalies={anomalies} topicFilter={selectedTopic} />
        </div>

        <section id="heatmap" className="panel chart-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">DLQ heatmap</p>
              <h2>Failure concentration</h2>
            </div>
            <span className="subtle">{summary.active_topics} active topics</span>
          </div>
          <div className="heatmap-legend">
            {heatmapView.topics.map((topic) => (
              <span key={topic}>{topic}</span>
            ))}
          </div>
          <div className="heatmap-grid">
            {heatmapView.buckets.map((bucket) =>
              heatmapView.topics.map((topic) => {
                const point = heatmap.find((entry) => entry.bucket === bucket && entry.topic === topic);
                const intensity = Math.min((point?.count ?? 0) / 5, 1);
                return (
                  <div
                    className="heat-cell"
                    key={`${bucket}-${topic}`}
                    title={`${bucket} · ${topic} · ${point?.count ?? 0}`}
                    data-active={intensity > 0 ? 'true' : 'false'}
                    style={{ opacity: 0.18 + intensity * 0.82, ['--pulse' as any]: intensity }}
                  >
                    <span>{point?.count ?? 0}</span>
                  </div>
                );
              })
            )}
          </div>
        </section>
      </section>

      <section id="anomalies" className="panel anomaly-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Anomalies</p>
            <h2>Most recent violations</h2>
          </div>
        </div>
        <div className="anomaly-list">
          {anomalies.slice(0, 5).map((anomaly) => (
            <article className={`anomaly-item severity-${anomaly.severity}`} key={anomaly.id}>
              <strong>{anomaly.kind}</strong>
              <p>{anomaly.message}</p>
              <small>
                {anomaly.topic} / p{anomaly.partition}
              </small>
            </article>
          ))}
        </div>
      </section>

      <section id="test" className="panel test-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Test harness</p>
            <h2>Generate anomalies</h2>
          </div>
          <span className="subtle">{isTesting ? 'sending test traffic' : testStatus || 'ready'}</span>
        </div>
        <div className="test-actions">
          <button type="button" className="test-button" onClick={() => void runDuplicateTest()} disabled={isTesting}>
            Duplicate
          </button>
          <button type="button" className="test-button" onClick={() => void runOutOfOrderTest()} disabled={isTesting}>
            Out of order
          </button>
          <button type="button" className="test-button" onClick={() => void runLagSpikeTest()} disabled={isTesting}>
            Lag spike
          </button>
          <button type="button" className="test-button" onClick={() => void runDlqTest()} disabled={isTesting}>
            DLQ
          </button>
          <button type="button" className="test-button" onClick={() => void runSlowConsumerTest()} disabled={isTesting}>
            Slow consumer
          </button>
        </div>
        <p className="test-note">Each action posts crafted events to the backend and should update the live anomaly feed immediately.</p>
      </section>

      {replayLoaded ? (
        <section className="main-grid replay-grid">
          <Timeline events={replayEvents} anomalies={replayAnomalies} title="Replay feed" eyebrow="Replay" />
          <section className="panel anomaly-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Replay anomalies</p>
                <h2>Historical violations</h2>
              </div>
            </div>
            <div className="anomaly-list">
              {replayAnomalies.slice(0, 5).map((anomaly) => (
                <article className={`anomaly-item severity-${anomaly.severity}`} key={anomaly.id}>
                  <strong>{anomaly.kind}</strong>
                  <p>{anomaly.message}</p>
                  <small>
                    {anomaly.topic} / p{anomaly.partition}
                  </small>
                </article>
              ))}
            </div>
          </section>
        </section>
      ) : null}
      </div>
    </div>
  );
}
