// src/components/StatusBar.jsx
export default function StatusBar({ nodes, edges, events, status }) {
  return (
    <div style={{ display:"flex", gap:20, alignItems:"center", padding:"5px 14px",
      borderTop:"1px solid var(--border)", background:"var(--bg2)",
      fontFamily:"monospace", fontSize:10, color:"var(--muted)" }}>
      <span>
        <span style={{ display:"inline-block", width:5, height:5, borderRadius:"50%",
          background:"#3c9", marginRight:4, verticalAlign:"middle" }} />
        live
      </span>
      <span><span style={{color:"var(--amber)"}}>{nodes.length}</span> nodes</span>
      <span><span style={{color:"var(--amber)"}}>{edges.length}</span> edges</span>
      <span><span style={{color:"var(--amber)"}}>{events.length}</span> events</span>
      {status && (
        <span><span style={{color:"var(--amber)"}}>{status.sockets}</span> sockets</span>
      )}
    </div>
  );
}