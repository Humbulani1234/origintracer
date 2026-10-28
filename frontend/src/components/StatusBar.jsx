export default function StatusBar({
  nodes, edges, events, status, worker, workerInventoryLoaded,
}) {
  const customer = status?.customer_id ?? "—";
  const stored = status?.storage?.event_count ?? null;
  const snapLabel = status?.snapshot?.label ?? null;
  const hasLiveWorker = Boolean(worker?.pid);
  const connectionLabel = hasLiveWorker
    ? "live"
    : workerInventoryLoaded ? "no worker" : "discovering";

  return (
    <div style={{ display:"flex", gap:20, alignItems:"center", padding:"5px 14px",
      borderTop:"1px solid var(--border)", background:"var(--bg2)",
      fontFamily:"monospace", fontSize:10, color:"var(--muted)" }}>
      <span>
        <span style={{ display:"inline-block", width:5, height:5, borderRadius:"50%",
          background:hasLiveWorker ? "#3c9" : "#555",
          marginRight:4, verticalAlign:"middle" }} />
        {connectionLabel}
      </span>
      <span><span style={{color:"var(--text)"}}>{nodes.length}</span> nodes</span>
      <span><span style={{color:"var(--text)"}}>{edges.length}</span> edges</span>
      <span><span style={{color:"var(--text)"}}>{events.length}</span> events</span>
      {stored != null && (
        <span><span style={{color:"var(--text)"}}>{stored}</span> stored</span>
      )}
      {snapLabel && (
        <span>snap: <span style={{color:"var(--text)"}}>{snapLabel}</span></span>
      )}
      <span style={{ marginLeft:"auto" }}>
        {worker?.pid ? `pid ${worker.pid}` : customer}
      </span>
    </div>
  );
}
