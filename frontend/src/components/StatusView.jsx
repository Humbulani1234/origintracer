// src/components/StatusView.jsx
export default function StatusView({ nodes, edges, events, status, worker }) {
  const snapshotInfo = status?.snapshot ?? {};
  const runtime = snapshotInfo.runtime_status ?? {};
  const snapshotRows = [
    ["process", worker?.worker_pid ? `pid ${worker.worker_pid}` : "—"],
    ["worker instance", worker?.worker_instance_id],
    ["customer", status?.customer_id],
    ["storage", status?.storage],
    ["snapshot updated", snapshotInfo.last_updated
      ? new Date(snapshotInfo.last_updated * 1000).toLocaleTimeString() : null],
    ["snapshot received", worker?.received_at
      ? new Date(worker.received_at * 1000).toLocaleTimeString() : null],
    ["reporting", snapshotInfo.online ? "yes" : "no / stale heartbeat"],
    ["last heartbeat age", snapshotInfo.heartbeat_age_s == null
      ? null : `${snapshotInfo.heartbeat_age_s}s`],
    ["uptime", runtime.uptime_s == null
      ? null : `${Math.floor(runtime.uptime_s / 3600)}h ${Math.floor((runtime.uptime_s % 3600) / 60)}m`],
    ["active requests", runtime.active_requests],
    ["active probes", runtime.probes_active?.join(", ")],
    ["active rules", runtime.rules_active?.join(", ")],
    ["nodes", nodes?.length],
    ["edges", edges?.length],
    ["events", events?.length],
    ["snapshot", snapshotInfo.available ? "available" : "unavailable"],
  ];

  return (
    <div style={{ padding:14, fontFamily:"monospace", fontSize:11 }}>
      {snapshotRows.map(([label, value]) => (
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
