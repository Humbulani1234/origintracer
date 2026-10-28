function fmtTime(ts) {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString([], {
    month: "short", day: "numeric",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

function MatchRows({ matches }) {
  return matches?.map((match, index) => (
    <div key={index} style={{
      marginTop:8, padding:"8px 10px", background:"var(--bg)",
      border:"1px solid var(--border)", borderRadius:4,
    }}>
      <div style={{ color:"var(--text)", fontSize:11, fontWeight:600 }}>
        {match.rule ?? match.name ?? "Causal rule"}
        <span style={{ color:"var(--muted)", fontWeight:400, marginLeft:8 }}>
          {Math.round((match.confidence ?? 0) * 100)}% confidence
        </span>
      </div>
      <div style={{ color:"var(--muted)", marginTop:5, lineHeight:1.6 }}>
        {match.explanation}
      </div>
    </div>
  ));
}

function MatchGroup({ matches, label, timestamp }) {
  const color = matches?.some(match => match.confidence >= 0.8)
    ? "var(--red, #e05252)" : "var(--amber, #e8a020)";
  return (
    <div style={{
      margin:"6px 12px 12px", padding:"12px 14px",
      background:"var(--bg2)", border:"1px solid var(--border)", borderRadius:5,
      fontFamily:"monospace", fontSize:10,
    }}>
      <div style={{ display:"flex", alignItems:"center", flexWrap:"wrap", gap:8 }}>
        <span style={{ color:"var(--text)", fontSize:11, fontWeight:600 }}>
          {timestamp ? fmtTime(timestamp) : label}
        </span>
        {timestamp && <span style={{ color:"var(--muted)" }}>{label}</span>}
        <span style={{
          marginLeft:"auto", padding:"3px 7px", borderRadius:10,
          color, background:"var(--bg)", border:"1px solid var(--border)",
        }}>
          {matches?.length ?? 0} rule{matches?.length !== 1 ? "s" : ""}
        </span>
      </div>
      <MatchRows matches={matches} />
    </div>
  );
}

function SectionHeading({ children }) {
  return (
    <div style={{
      margin:"12px 12px 0", padding:"8px 10px",
      color:"var(--accent)", background:"rgba(69,196,176,0.08)",
      border:"1px solid rgba(69,196,176,0.22)", borderRadius:4,
      fontFamily:"monospace", fontSize:10, fontWeight:700,
      letterSpacing:"0.06em", textTransform:"uppercase",
    }}>
      {children}
    </div>
  );
}

export default function CausalHistory({ history, currentMatches = [] }) {
  if (!history?.length && !currentMatches?.length) {
    return (
      <div style={{ padding:"32px 14px", fontFamily:"monospace",
          fontSize:11, color:"var(--muted)", textAlign:"center" }}>
        No current matches or saved causal history.
      </div>
    );
  }

  return (
    <div>
      {currentMatches?.length > 0 && (
        <>
          <SectionHeading>
            Current live matches
          </SectionHeading>
          <MatchGroup matches={currentMatches} label="Live worker" />
        </>
      )}
      {history?.length > 0 && (
        <SectionHeading>
          Saved history
        </SectionHeading>
      )}
      {(history ?? []).map((entry, i) => {
        return (
          <MatchGroup
            key={i}
            matches={entry.matches ?? []}
            timestamp={entry.timestamp}
            label={entry.label ?? "Saved snapshot"}
          />
        );
      })}
    </div>
  );
}
