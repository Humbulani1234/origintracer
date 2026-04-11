// src/api/client.js
//
// Points at the OriginTracer FastAPI backend.
// Set VITE_API_URL and VITE_API_KEY in a .env file at the project root:
//
//   VITE_API_URL=http://localhost:8000
//   VITE_API_KEY=test-key-123
//
// When the backend is unavailable, all calls throw and App.jsx
// catches silently — mock data stays visible with no errors shown.

const BASE = import.meta.env.VITE_API_URL || "http://localhost:8001";
const KEY  = import.meta.env.VITE_API_KEY  || "test-key-123";

async function request(path) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Authorization": `Bearer ${KEY}` },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export const api = {
  nodes:  ()           => request("/api/v1/nodes"),
  edges:  ()           => request("/api/v1/edges"),
  events: (limit = 30) => request(`/api/v1/events?limit=${limit}`),
  trace:  (id)         => request(`/api/v1/traces/${id}`),
  status: ()           => request("/api/v1/status"),
  diff:   (label = "origintracer-snapshot") => request(`/api/v1/graph/diff?since=${label}`),
  graph: ()           => request("/api/v1/graph"),
  causal: (since = "deployment", tags = null) => {
    const params = new URLSearchParams({ since });
    if (tags) params.append("tags", tags);
    return request(`/api/v1/causal?${params}`);
  },
};