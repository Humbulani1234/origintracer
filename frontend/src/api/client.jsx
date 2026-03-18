// src/api/client.js
//
// Talks to the HTTP bridge that wraps the Unix socket local_server.
// Add to your stacktracer package:
//
//   stacktracer/bridge.py  — tiny FastAPI app:
//
//     from fastapi import FastAPI
//     from stacktracer.scripts.repl import query
//     import glob, os
//
//     app = FastAPI()
//
//     def _sockets():
//         return glob.glob("/tmp/stacktracer-*.sock")
//
//     @app.get("/api/nodes")
//     def nodes():
//         sock = _sockets()[0]
//         return query(sock, "SHOW nodes")
//
//     @app.get("/api/edges")
//     def edges():
//         sock = _sockets()[0]
//         return query(sock, "SHOW edges")
//
//     @app.get("/api/events")
//     def events(limit: int = 20):
//         sock = _sockets()[0]
//         return query(sock, f"SHOW events LIMIT {limit}")
//
//     @app.get("/api/trace/{trace_id}")
//     def trace(trace_id: str):
//         socks = _sockets()
//         results = []
//         for sock in socks:
//             r = query(sock, f"TRACE {trace_id}")
//             if r.get("ok"):
//                 results.append(r)
//         return {"ok": True, "data": results}
//
//     @app.get("/api/status")
//     def status():
//         socks = _sockets()
//         results = []
//         for sock in socks:
//             r = query(sock, "STATUS")
//             if r.get("ok"):
//                 results.append(r)
//         return {"ok": True, "sockets": len(socks), "data": results}
//
// Run: uvicorn stacktracer.bridge:app --port 7123

const BASE = import.meta.env.VITE_API_URL || "http://localhost:7123";

async function request(path) {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

export const api = {
  nodes:  ()           => request("/api/nodes"),
  edges:  ()           => request("/api/edges"),
  events: (limit = 30) => request(`/api/events?limit=${limit}`),
  trace:  (id)         => request(`/api/trace/${id}`),
  status: ()           => request("/api/status"),
};