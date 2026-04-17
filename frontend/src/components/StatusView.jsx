// src/components/StatusView.jsx
export default function StatusView({ nodes, edges, events, status }) {
  return (
    <div style={{ padding:14, fontFamily:"monospace", fontSize:11 }}>
      {[
        ["customer", status?.customer_id],
        ["storage", status?.storage],
        ["timestamp", status?.timestamp
            ? new Date(status.timestamp * 1000).toLocaleTimeString() : null],
        ["nodes (live)", nodes?.length],
        ["edges (live)", edges?.length],
        ["events (live)", events?.length],
        ["snapshot", status?.snapshot?.available ? "available" : "unavailable"],
        ["snap nodes", status?.snapshot?.nodes],
        ["snap edges", status?.snapshot?.edges],
        ["last updated", status?.snapshot?.last_updated
            ? new Date(status.snapshot.last_updated * 1000).toLocaleTimeString() : null],
      ].map(([label, value]) => (
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