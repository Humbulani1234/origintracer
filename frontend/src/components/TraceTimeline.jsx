// src/components/TraceTimeline.jsx
import { svcColor, short } from "./NodeTable";

export default function TraceTimeline({ trace }) {
  if (!trace) {
    return (
      <div style={{ padding:"32px 14px", fontFamily:"monospace", fontSize:11,
        color:"var(--muted)", textAlign:"center" }}>
        run <span style={{color:"var(--amber)"}}>\ stitch &lt;trace_id&gt;</span> to view a trace
      </div>
    );
  }

  const { id, stages } = trace;
  const maxDur = Math.max(...stages.filter(s => s.duration_ms).map(s => s.duration_ms), 1);
  const totalMs = stages.reduce((a, s) => a + (s.duration_ms || 0), 0);

  return (
    <div style={{ padding:14 }}>
      <div style={{ fontFamily:"monospace", fontSize:10, color:"var(--muted)",
        marginBottom:12, letterSpacing:"0.06em" }}>
        trace <span style={{color:"var(--amber)"}}>{id.slice(0,8)}…</span>
      </div>
      {stages.map((s, i) => {
        const barW = s.duration_ms ? Math.max(2, (s.duration_ms / maxDur) * 100) : 0;
        const c    = svcColor(s.service);
        return (
          <div key={i} style={{ display:"grid", gridTemplateColumns:"64px 1fr 220px",
            gap:10, alignItems:"center", padding:"3px 0",
            borderBottom:"1px solid rgba(42,42,42,0.4)" }}>
            <div style={{ textAlign:"right", fontFamily:"monospace", fontSize:10,
              color: s.duration_ms ? "var(--amber)" : "var(--muted)" }}>
              {s.duration_ms ? s.duration_ms.toFixed(1)+"ms" : "—"}
            </div>
            <div style={{ height:8, background:"var(--bg3)", borderRadius:1 }}>
              <div style={{ height:"100%", width:`${barW}%`, background:c,
                borderRadius:1, minWidth:2 }} />
            </div>
            <div style={{ fontFamily:"monospace", fontSize:10, color:"var(--muted)",
              overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>
              <span style={{color:"var(--text)"}}>{s.probe}</span>
              <span style={{color:"#555", fontSize:9, marginLeft:4}}>
                {s.service}::{short(s.name, 28)}
              </span>
            </div>
          </div>
        );
      })}
      <div style={{ marginTop:12, fontFamily:"monospace", fontSize:10, color:"var(--muted)" }}>
        end-to-end <span style={{color:"var(--amber)"}}>{totalMs.toFixed(1)}ms</span>
        {" · "}
        <span style={{color:"var(--amber)"}}>{stages.length}</span> stages
      </div>
    </div>
  );
}