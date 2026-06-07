export type Summary = {
  total_messages: number;
  duplicates: number;
  out_of_order: number;
  lag_spikes: number;
  dlq_bursts: number;
  slow_processes: number;
  active_topics: number;
};

export type AuditEvent = {
  message_id: string;
  topic: string;
  partition: number;
  sequence: number;
  kind: 'producer' | 'consumer' | 'dlq';
  source: string;
  timestamp: string;
  processed_at?: string | null;
  committed_at?: string | null;
  processing_ms?: number | null;
  lag_ms?: number | null;
  payload: Record<string, unknown>;
};

export type AnomalyRecord = {
  id: string;
  kind: 'duplicate' | 'out_of_order' | 'lag_spike' | 'dlq_burst' | 'processing_outlier';
  severity: 'low' | 'medium' | 'high';
  message: string;
  topic: string;
  partition: number;
  created_at: string;
  details: Record<string, unknown>;
};

export type HeatmapPoint = {
  bucket: string;
  topic: string;
  count: number;
};
