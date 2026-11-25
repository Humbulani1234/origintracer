function formatLabel(key) {
  return key.replaceAll("_", " ").replace(/\b\w/g, char => char.toUpperCase());
}

function EvidenceValue({ value }) {
  if (Array.isArray(value)) {
    const objectRows = value.length > 0 && value.every(
      item => item && typeof item === "object" && !Array.isArray(item),
    );

    if (objectRows) {
      return (
        <details className="causal-evidence-list" open={value.length <= 3}>
          <summary>{value.length} records · expand to inspect</summary>
          <div className="causal-evidence-records">
            {value.map((item, index) => (
              <EvidenceValue key={index} value={item} />
            ))}
          </div>
        </details>
      );
    }

    return (
      <div className="causal-evidence-values">
        {value.map((item, index) => (
          <EvidenceValue key={index} value={item} />
        ))}
      </div>
    );
  }

  if (value && typeof value === "object") {
    if (typeof value.node === "string" && value.call_count != null) {
      return (
        <div className="causal-evidence-node">
          <div className="causal-evidence-primary">{value.node}</div>
          <div className="causal-evidence-metrics">
            <span>Calls <b>{value.call_count}</b></span>
            {value.pct != null && <span>Share <b>{value.pct}%</b></span>}
            {value.avg_ms != null && <span>Average <b>{value.avg_ms} ms</b></span>}
          </div>
        </div>
      );
    }

    return (
      <div className="causal-evidence-object">
        {Object.entries(value).map(([key, item]) => (
          <div className="causal-evidence-field" key={key}>
            <span className="causal-evidence-label">{formatLabel(key)}</span>
            <EvidenceValue value={item} />
          </div>
        ))}
      </div>
    );
  }

  return <span className="causal-evidence-value">{String(value ?? "—")}</span>;
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
    <div className="causal-view">
      {causal.map((m, i) => {
        const pct = Math.round((m.confidence ?? 0) * 100);
        const color = pct >= 80 ? "var(--red, #e05252)" : "var(--amber, #e8a020)";
        const evidence = m.evidence && Object.keys(m.evidence).length
          ? m.evidence : null;

        return (
          <section key={i} className="causal-card" style={{ "--causal-color": color }}>
            <div className="causal-card-heading">
              <ConfidenceBar pct={pct} />
              <span className="causal-rule-name">{m.rule}</span>
            </div>
            <p className="causal-explanation">{m.explanation}</p>
            {evidence && (
              <div className="causal-evidence">
                <div className="causal-evidence-heading">Evidence</div>
                <EvidenceValue value={evidence} />
              </div>
            )}
          </section>
        );
      })}
    </div>
  );
}
