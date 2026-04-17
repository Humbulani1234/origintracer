function ConfidenceBar({ pct }) {
  const filled = Math.round(pct / 10);
  const color = pct >= 80 ? "var(--red, #e05252)" : "var(--amber, #d4a843)";
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
        const color = pct >= 80 ? "var(--red, #e05252)" : "var(--amber, #d4a843)";
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
              <div style={{ fontFamily: "monospace", fontSize: 9,
                  color: "var(--muted)", opacity: 0.6, marginTop: 4 }}>
                Evidence: {JSON.stringify(evidence)}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}