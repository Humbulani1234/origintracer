// src/components/StatusView.jsx
export default function StatusView({ nodes, edges, events, status, worker }) {
  const workerRows = [
    ["process", worker?.pid ? `pid ${worker.pid}` : status?.pid],
    ["socket", worker?.socket ?? status?.socket],
    ["uptime", status?.uptime],
    ["nodes (live)", status?.graph_nodes ?? nodes?.length],
    ["edges (live)", status?.graph_edges ?? edges?.length],
    ["events (live)", status?.event_log_size ?? events?.length],
    ["active requests", status?.active_requests],
    ["probes active", Array.isArray(status?.probes_active)
      ? status.probes_active.join(", ") : status?.probes_active],
    ["semantic labels", status?.semantic_labels],
  ];
  const snapshotRows = [
    ["customer", status?.customer_id],
    ["storage", status?.storage],
    ["timestamp", status?.timestamp
      ? new Date(status.timestamp * 1000).toLocaleTimeString() : null],
    ["nodes", nodes?.length],
    ["edges", edges?.length],
    ["events", events?.length],
    ["snapshot", status?.snapshot?.available ? "available" : "unavailable"],
    ["snap nodes", status?.snapshot?.nodes],
    ["snap edges", status?.snapshot?.edges],
    ["last updated", status?.snapshot?.last_updated
      ? new Date(status.snapshot.last_updated * 1000).toLocaleTimeString() : null],
  ];
  const rows = status?.mode === "worker" ? workerRows : snapshotRows;

  return (
    <div style={{ padding:14, fontFamily:"monospace", fontSize:11 }}>
      {rows.map(([label, value]) => (
        <div key={label} style={{
          display:"grid", gridTemplateColumns:"140px 1fr",
          gap:12, padding:"5px 0",
          borderBottom:"0.5px solid rgba(42,42,42,0.4)",
        }}>
          <span style={{ color:"var(--muted)" }}>{label}</span>
          <span style={{ color:"var(--amber)" }}>{value ?? "—"}</span>
        </div>
      ))}
    </div>
  );
}
