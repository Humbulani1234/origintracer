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
        color:"var(--accent)", flexShrink:0 }}>›</span>
      <input
        value={val}
        onChange={e => setVal(e.target.value)}
        onKeyDown={e => e.key === "Enter" && run()}
        placeholder="SHOW nodes · SHOW llm_content · SHOW tool_content · TRACE <id>"
        style={{ flex:1, background:"none", border:"none", outline:"none",
          fontFamily:"monospace", fontSize:11, color:"var(--text)",
          caretColor:"var(--accent)" }}
      />
      <button onClick={run} disabled={loading}
        style={{ fontFamily:"monospace", fontSize:10, color:"var(--accent)",
          background:"rgba(69,196,176,0.08)", border:"1px solid rgba(69,196,176,0.2)",
          borderRadius:3, padding:"2px 8px", cursor:"pointer" }}>
        {loading ? "…" : "run ↵"}
      </button>
    </div>
  );
}
