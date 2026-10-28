function EvidenceValue({ value }) {
  if (Array.isArray(value)) {
    return (
      <div style={{ display: "grid", gap: 8 }}>
        {value.map((item, index) => (
          <EvidenceValue key={index} value={item} />
        ))}
      </div>
    );
  }

  if (value && typeof value === "object") {
    if (typeof value.node === "string" && value.call_count != null) {
      return (
        <div style={{ padding: "8px 10px", background: "var(--bg2)",
            border: "1px solid var(--border)", borderRadius: 4 }}>
          <div style={{ color: "var(--text)", overflowWrap: "anywhere",
              lineHeight: 1.6 }}>{value.node}</div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "4px 16px",
              marginTop: 4, color: "var(--muted)" }}>
            <span>Calls: <b style={{ color: "var(--text)" }}>{value.call_count}</b></span>
            {value.pct != null && <span>Share: <b style={{ color: "var(--text)" }}>{value.pct}%</b></span>}
            {value.avg_ms != null && <span>Average: <b style={{ color: "var(--text)" }}>{value.avg_ms} ms</b></span>}
          </div>
        </div>
      );
    }
    return (
      <div style={{ display: "grid", gap: 6 }}>
        {Object.entries(value).map(([key, item]) => (
          <div key={key}>
            <span style={{ color: "var(--muted)" }}>{key.replaceAll("_", " ")}: </span>
            <EvidenceValue value={item} />
          </div>
        ))}
      </div>
    );
  }

  return <span style={{ overflowWrap: "anywhere" }}>{String(value ?? "—")}</span>;
}

function ConfidenceBar({ pct }) {
  const filled = Math.round(pct / 10);
  const color = pct >= 80 ? "var(--red, #e05252)" : "var(--amber, #e8a020)";
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{ display: "flex", gap: 2 }}>
        {Array.from({ length: 10 }, (_, i) => (
          <div key={i} style={{
            width: 8, height: 14, borderRadius: 2,
            background: i < filled ? color : "rgba(80,80,80,0.3)",
          }} />
        ))}
      </div>
      <span style={{ fontFamily: "monospace", fontSize: 11,
          fontWeight: 600, color }}>
        {pct}%
      </span>
    </div>
  );
}

export default function CausalView({ causal }) {
  if (!causal?.length) {
    return (
      <div style={{ padding: "32px 14px", fontFamily: "monospace",
          fontSize: 11, color: "var(--muted)", textAlign: "center" }}>
        No causal patterns matched.
      </div>
    );
  }

  return (
    <div style={{ padding: 14 }}>
      {causal.map((m, i) => {
        const pct = Math.round((m.confidence ?? 0) * 100);
        const color = pct >= 80 ? "var(--red, #e05252)" : "var(--amber, #e8a020)";
        const evidence = m.evidence && Object.keys(m.evidence).length
          ? m.evidence : null;

        return (
          <div key={i} style={{
            marginBottom: 16, padding: "10px 14px",
            borderLeft: `2px solid ${color}`,
            borderBottom: "0.5px solid rgba(42,42,42,0.4)",
            borderRadius: "0 4px 4px 0",
          }}>
            <div style={{ display: "flex", alignItems: "center",
                gap: 12, marginBottom: 6 }}>
              <ConfidenceBar pct={pct} />
              <span style={{ fontFamily: "monospace", fontSize: 11,
                  fontWeight: 600, color }}>
                {m.rule}
              </span>
            </div>
            <div style={{ fontFamily: "monospace", fontSize: 10,
                color: "var(--muted)", lineHeight: 1.6,
                marginBottom: evidence ? 6 : 0 }}>
              {m.explanation}
            </div>
            {evidence && (
              <div style={{ fontFamily: "monospace", fontSize: 11,
                  color: "var(--text)", lineHeight: 1.6, marginTop: 8 }}>
                <div style={{ color: "var(--accent)", marginBottom: 5 }}>Evidence</div>
                <EvidenceValue value={evidence} />
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
