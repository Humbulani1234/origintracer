// src/components/EventLog.jsx
import { svcColor, short } from "./NodeTable";

export default function EventLog({ events }) {
  return (
    <div>
      {events.map((e, i) => {
        const c = svcColor(e.service);
        return (
          <div key={i} style={{ display:"grid", gridTemplateColumns:"80px 120px 1fr auto",
            gap:8, alignItems:"center", padding:"5px 14px",
            borderBottom:"1px solid rgba(42,42,42,0.4)",
            fontFamily:"monospace", fontSize:10 }}>
            <span style={{color:"var(--muted)"}}>{e.wall_time || e.timestamp}</span>
            <span style={{color:c}}>{e.probe}</span>
            <span className="muted" title={e.name}>{short(e.name || "", 32)}</span>
            <span style={{color:"var(--amber)"}}>
              {e.duration_ms ? e.duration_ms.toFixed(1)+"ms" : "—"}
            </span>
          </div>
        );
      })}
    </div>
  );
}