// Points at the OriginTracer FastAPI backend.
// Set VITE_API_URL and VITE_API_KEY in a .env file at the project root:
//
//  VITE_API_URL=http://localhost:8001
//  VITE_API_KEY=test-key-123


const BASE = import.meta.env.VITE_API_URL || "http://localhost:8001";
const KEY  = import.meta.env.VITE_API_KEY  || "test-key-123";

async function request(path, options = {}) {
  const { method = "GET", data } = options;
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: {
      "Authorization": `Bearer ${KEY}`,
      ...(data ? { "Content-Type": "application/json" } : {}),
    },
    ...(data ? { body: JSON.stringify(data) } : {}),
  });
  const payload = await res.json().catch(() => null);
  if (!res.ok) {
    const detail = payload?.detail || payload?.error || `HTTP ${res.status}`;
    throw new Error(detail);
  }
  return payload;
}

export const api = {
  nodes: (workerInstanceId) => request(`/api/v1/nodes?worker_instance_id=${encodeURIComponent(workerInstanceId)}`),
  edges: (workerInstanceId) => request(`/api/v1/edges?worker_instance_id=${encodeURIComponent(workerInstanceId)}`),
  events: (workerInstanceId, limit = 30) => request(
    `/api/v1/events?worker_instance_id=${encodeURIComponent(workerInstanceId)}&limit=${limit}`,
  ),
  capturedContent: (workerInstanceId, kind, limit = 100) => request(
    `/api/v1/events/content?worker_instance_id=${encodeURIComponent(workerInstanceId)}&kind=${encodeURIComponent(kind)}&limit=${limit}`,
  ),
  trace: (id, workerInstanceId) => request(
    `/api/v1/traces/${encodeURIComponent(id)}?worker_instance_id=${encodeURIComponent(workerInstanceId)}`,
  ),
  stitchTrace: (id) => request(`/api/v1/traces/${encodeURIComponent(id)}/stitch`),
  status: (workerInstanceId) => request(`/api/v1/status?worker_instance_id=${encodeURIComponent(workerInstanceId)}`),
  // hardcoded label for testing
  diff: (workerInstanceId, label = "deployment") => request(`/api/v1/graph/diff?worker_instance_id=${encodeURIComponent(workerInstanceId)}&since=${encodeURIComponent(label)}`),
  graph: (workerInstanceId) => request(`/api/v1/graph?worker_instance_id=${encodeURIComponent(workerInstanceId)}`),
  causal: (workerInstanceId, tags) => request(`/api/v1/causal?worker_instance_id=${encodeURIComponent(workerInstanceId)}${tags ? `&tags=${encodeURIComponent(tags)}` : ''}`),
  hotspots: (workerInstanceId, top = 10) => request(
    `/api/v1/hotspots?worker_instance_id=${encodeURIComponent(workerInstanceId)}&top=${top}`,
  ),
  causalHistory: (workerInstanceId, limit = 50) => request(`/api/v1/causal/history?worker_instance_id=${encodeURIComponent(workerInstanceId)}&limit=${limit}`),
  snapshots: () => request("/api/v1/snapshots"),
  snapshot: (workerInstanceId) => request(`/api/v1/snapshots/${encodeURIComponent(workerInstanceId)}`),
};
