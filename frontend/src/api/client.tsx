// frontend/src/types/trace.ts
// Updated: Stage enum replaced with ProbeType strings to match the probe-based backend.

// ProbeType mirrors stacktracer/core/event_schema.py ProbeType literal
export type ProbeType =
  | "request.entry"
  | "request.exit"
  | "django.middleware.enter"
  | "django.middleware.exit"
  | "django.url.resolve"
  | "django.view.enter"
  | "django.view.exit"
  | "asyncio.task.create"
  | "asyncio.task.block"
  | "asyncio.task.wakeup"
  | "asyncio.loop.tick"
  | "asyncio.timer.schedule"
  | "function.call"
  | "function.return"
  | "function.exception"
  | "syscall.enter"
  | "syscall.exit"
  | "tcp.send"
  | "tcp.recv"
  | "db.query.start"
  | "db.query.end"
  | "custom";

// One stage in a critical path (returned by /api/v1/traces/:id and TRACE DSL)
export interface PathStage {
  probe: ProbeType;
  service: string;
  name: string;
  timestamp: number;
  wall_time: number;
  duration_ms: number | null;  // null for first stage
  metadata: Record<string, unknown>;
}

// Trace summary row (returned by list queries)
export interface TraceSummary {
  trace_id: string;
  timestamp: string;
  duration_ms: number;
  method: string;
  path: string;
  status: number;
}

// Full trace (returned by /api/v1/traces/:id)
export interface Trace extends TraceSummary {
  stages: number;
  total_ms: number;
  path: PathStage[];         // note: renamed from events[] to path[] to match backend
}

// Graph node (returned by SHOW graph / /api/v1/graph)
export interface GraphNode {
  id: string;
  service: string;
  type: string;
  call_count: number;
}

export interface GraphEdge {
  source: string;
  target: string;
  type: string;
  call_count: number;
}

export interface RuntimeGraph {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

// Causal match (returned by CAUSAL / /api/v1/causal)
export interface CausalMatch {
  rule: string;
  confidence: number;
  explanation: string;
  evidence: Record<string, unknown>;
  timestamp: number;
}

// Hotspot node (returned by HOTSPOT / /api/v1/hotspots)
export interface HotspotNode {
  node: string;
  service: string;
  type: string;
  call_count: number;
  avg_duration_ms: number | null;
}

// Summary stats (unchanged endpoint, same shape)
export interface SummaryStats {
  total_requests: number;
  avg_duration_ms: number;
  p50_duration_ms: number;
  p95_duration_ms: number;
  p99_duration_ms: number;
}

// DSL query result (generic — shape depends on verb)
export interface QueryResult {
  verb?: string;
  metric?: string;
  data?: unknown;
  error?: string;
  [key: string]: unknown;
}