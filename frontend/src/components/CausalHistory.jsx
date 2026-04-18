function fmtTime(ts) {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString([], {
    month: "short", day: "numeric",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

export default function CausalHistory({ history }) {
  if (!history?.length) {
    return (
      <div style={{ padding:"32px 14px", fontFamily:"monospace",
          fontSize:11, color:"var(--muted)", textAlign:"center" }}>
        No causal matches recorded yet.
      </div>
    );
  }

  return (
    <div>
      {history.map((entry, i) => {
        const color = entry.matches?.some(m => m.confidence >= 0.8)
          ? "var(--red, #e05252)" : "var(--amber, #d4a843)";
        return (
          <div key={i} style={{
            padding:"8px 14px", borderBottom:"0.5px solid rgba(42,42,42,0.4)",
            borderLeft:`2px solid ${color}`, fontFamily:"monospace", fontSize:10,
          }}>
            <div style={{ display:"flex", gap:12, marginBottom:4 }}>
              <span style={{ color:"var(--muted)" }}>{fmtTime(entry.timestamp)}</span>
              <span style={{ color:"var(--muted)", opacity:0.5 }}>{entry.label ?? "—"}</span>
              <span style={{ color }}>{entry.matches?.length} rule{entry.matches?.length !== 1 ? "s" : ""}</span>
            </div>
            {entry.matches?.map((m, j) => (
              <div key={j} style={{ color:"var(--muted)", fontSize:9, marginLeft:8 }}>
                · {m.name} ({Math.round((m.confidence ?? 0) * 100)}%)
                <div style={{ opacity:0.6, marginLeft:8, lineHeight:1.5 }}>
                  {m.explanation}
                </div>
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}