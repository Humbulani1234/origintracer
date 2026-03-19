// src/components/QueryBar.jsx
import { useState } from "react";

export default function QueryBar({ onRun, loading }) {
  const [val, setVal] = useState("");

  const run = () => {
    if (val.trim()) { onRun(val); setVal(""); }
  };

  return (
    <div style={{ display:"flex", alignItems:"center", gap:8, padding:"7px 14px",
      borderBottom:"1px solid var(--border)", background:"var(--bg)" }}>
      <span style={{ fontFamily:"monospace", fontSize:11,
        color:"var(--amber)", flexShrink:0 }}>›</span>
      <input
        value={val}
        onChange={e => setVal(e.target.value)}
        onKeyDown={e => e.key === "Enter" && run()}
        placeholder="SHOW nodes  ·  SHOW edges  ·  \stitch <trace_id>"
        style={{ flex:1, background:"none", border:"none", outline:"none",
          fontFamily:"monospace", fontSize:11, color:"var(--text)",
          caretColor:"var(--amber)" }}
      />
      <button onClick={run} disabled={loading}
        style={{ fontFamily:"monospace", fontSize:10, color:"var(--amber)",
          background:"rgba(232,160,32,0.08)", border:"1px solid rgba(232,160,32,0.2)",
          borderRadius:3, padding:"2px 8px", cursor:"pointer" }}>
        {loading ? "…" : "run ↵"}
      </button>
    </div>
  );
}