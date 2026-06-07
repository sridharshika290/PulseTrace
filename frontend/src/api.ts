import type { AnomalyRecord, AuditEvent, HeatmapPoint, Summary } from './types';

const baseUrl = import.meta.env.VITE_API_URL ?? 'http://localhost:8000';

async function request<T>(path: string): Promise<T> {
  const response = await fetch(`${baseUrl}${path}`);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json() as Promise<T>;
}

async function send<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json() as Promise<T>;
}

export function fetchSummary() {
  return request<any>('/api/summary');
}

export function fetchEvents() {
  return request<AuditEvent[]>('/api/events');
}

export function fetchAnomalies() {
  return request<AnomalyRecord[]>('/api/anomalies');
}

export function fetchHeatmap() {
  return request<HeatmapPoint[]>('/api/heatmap');
}

export function fetchReplayEvents(limit = 24) {
  return request<AuditEvent[]>(`/api/replay/events?limit=${limit}`);
}

export function fetchReplayAnomalies(limit = 24) {
  return request<AnomalyRecord[]>(`/api/replay/anomalies?limit=${limit}`);
}

export function postEvent(event: AuditEvent) {
  return send<{ ok: boolean; anomalies: AnomalyRecord[] }>('/api/events', event);
}
