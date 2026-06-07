type MetricCardProps = {
  label: string;
  value: string | number;
  accent?: string;
  delta?: string | number;
  subtitle?: string;
};

export function MetricCard({ label, value, accent = '#7dd3fc', delta, subtitle }: MetricCardProps) {
  return (
    <div className="metric-card" style={{ ['--accent' as string]: accent }}>
      <span className="metric-label">{label}</span>
      <div className="metric-body">
        <strong className="metric-value">{value}</strong>
        {delta ? (
          <span className={`metric-delta ${String(delta).startsWith('-') ? 'negative' : 'positive'}`}>
            {String(delta).startsWith('-') ? (
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" aria-hidden>
                <path d="M6 15l6-6 6 6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            ) : (
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" aria-hidden>
                <path d="M18 9l-6 6-6-6" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            )}
            <span className="metric-delta-text">{delta}</span>
          </span>
        ) : null}
      </div>
      {subtitle ? <div className="metric-subtitle">{subtitle}</div> : null}
    </div>
  );
}
