import React from 'react';
import { LiveIcon, LagIcon, DlqIcon, TopologyIcon } from '../icons';

type Props = {
  topics?: string[];
  selected?: string | null;
  onSelect?: (topic: string | null) => void;
  onNavSelect?: (id: string) => void;
};

export function Sidebar({ topics = [], selected = null, onSelect, onNavSelect }: Props) {
  return (
    <aside className="sidebar">
      <div className="sidebar-top">
        <div className="logo">PulseTrace</div>
        <div className="version">v1.0 · sidecar mode</div>
      </div>

      <nav className="nav">
        <h4 className="nav-section">Monitor</h4>
        <ul>
          <li>
            <button className="nav-btn nav-item active" aria-pressed="true" onClick={() => onNavSelect?.('timeline')}>
              <LiveIcon className="nav-icon" /> Live feed <span className="badge">3</span>
            </button>
          </li>
          <li>
            <button className="nav-btn nav-item" aria-pressed="false" onClick={() => onNavSelect?.('metrics-cards')}>
              <LagIcon className="nav-icon" /> Consumer lag
            </button>
          </li>
          <li>
            <button className="nav-btn nav-item" aria-pressed="false" onClick={() => onNavSelect?.('heatmap')}>
              <DlqIcon className="nav-icon" /> DLQ heatmap <span className="badge warning">!</span>
            </button>
          </li>
          <li>
            <button className="nav-btn nav-item" aria-pressed="false" onClick={() => onNavSelect?.('timeline')}>
              <TopologyIcon className="nav-icon" /> Topology
            </button>
          </li>
        </ul>

        <h4 className="nav-section">Topics</h4>
        <ul className="topics">
          <li>
            <button
              className={`topic ${selected === null ? 'topic-active' : ''}`}
              onClick={() => onSelect?.(null)}
              onKeyDown={(e) => (e.key === 'Enter' || e.key === ' ' ? onSelect?.(null) : undefined)}
            >
              All topics
            </button>
          </li>
          {topics.map((t) => (
            <li key={t}>
              <button
                className={`topic ${selected === t ? 'topic-active' : ''}`}
                onClick={() => onSelect?.(t)}
                onKeyDown={(e) => (e.key === 'Enter' || e.key === ' ' ? onSelect?.(t) : undefined)}
              >
                {t}
              </button>
            </li>
          ))}
        </ul>
      </nav>

      <div className="sidebar-footer">
        <label className="toggle"><input type="checkbox" /> Thresholds</label>
      </div>
    </aside>
  );
}
