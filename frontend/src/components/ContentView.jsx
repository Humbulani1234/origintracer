function shortTrace(traceId) {
  if (!traceId) return "—";
  return traceId.length > 18 ? `${traceId.slice(0, 18)}…` : traceId;
}

function parseToolValue(content) {
  if (typeof content !== "string") return undefined;
  try {
    return JSON.parse(content);
  } catch {
    // JSON cannot represent undefined, so it is a safe parse-failure marker.
    return undefined;
  }
}

function ToolValue({ value, depth = 0 }) {
  const nested = value !== null && typeof value === "object";
  if (!nested) {
    return (
      <span style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
        {value == null ? "null" : String(value)}
      </span>
    );
  }

  const entries = Array.isArray(value)
    ? value.map((item, index) => [index, item])
    : Object.entries(value);

  if (!entries.length) return <span>{Array.isArray(value) ? "[]" : "{}"}</span>;

  return (
    <div style={{ marginLeft: depth ? 12 : 0 }}>
      {entries.map(([key, item]) => (
        <div key={key} style={{ display: "flex", gap: 8, padding: "2px 0" }}>
          <span style={{ minWidth: 72, color: "var(--amber)", flexShrink: 0 }}>
            {Array.isArray(value) ? `[${key}]` : key}
          </span>
          <div style={{ minWidth: 0 }}>
            <ToolValue value={item} depth={depth + 1} />
          </div>
        </div>
      ))}
    </div>
  );
}

function ContentBody({ isTool, content }) {
  // `undefined` is the explicit "not a parsed tool value" marker.  Keeping
  // it for the LLM path ensures LLM text always uses the plain-text renderer.
  const parsedToolValue = isTool ? parseToolValue(content) : undefined;
  const isParsedToolValue = parsedToolValue !== undefined;

  if (isParsedToolValue) {
    return (
      <div style={{ margin: 0, padding: 10, overflowX: "auto",
        fontFamily: "var(--mono)", fontSize: 11, lineHeight: 1.55,
        color: "var(--text)", background: "var(--bg)" }}>
        <ToolValue value={parsedToolValue} />
      </div>
    );
  }

  return (
    <pre style={{ margin: 0, padding: 10, overflowX: "auto",
      whiteSpace: "pre-wrap", overflowWrap: "anywhere",
      fontFamily: "var(--mono)", fontSize: 11, lineHeight: 1.55,
      color: "var(--text)", background: "var(--bg)" }}>
      {content ?? ""}
    </pre>
  );
}

/**
 * Display opt-in LangGraph content captures without widening the event table.
 * The worker query decides what content is available; this view only renders
 * already-redacted, bounded previews returned by the selected live process.
 */
export default function ContentView({ kind, rows }) {
  const isTool = kind === "tool";
  const title = isTool ? "tool content" : "llm content";
  const empty = isTool
    ? "No captured tool content for this worker."
    : "No captured LLM content for this worker.";

  if (!rows?.length) {
    return (
      <div style={{ padding: "32px 14px", fontFamily: "monospace",
        fontSize: 11, color: "var(--muted)", textAlign: "center" }}>
        {empty}
        <div style={{ marginTop: 8, fontSize: 10 }}>
          Enable capture, run the agent, then select this view again.
        </div>
      </div>
    );
  }

  return (
    <div style={{ padding: 14 }}>
      <div style={{ marginBottom: 12, fontFamily: "monospace", fontSize: 10,
        color: "var(--muted)", letterSpacing: "0.06em" }}>
        {rows.length} captured {title} {rows.length === 1 ? "entry" : "entries"}
      </div>
      {rows.map((row, index) => {
        const label = isTool ? row.kind ?? "content" : row.probe ?? "content";
        const truncated = row.truncated ? " · truncated" : "";
        return (
          <article key={`${row.span_id ?? index}-${index}`} style={{
            marginBottom: 12, border: "1px solid var(--border)",
            borderRadius: 4, overflow: "hidden", background: "var(--bg2)",
          }}>
            <header style={{ display: "flex", gap: 10, alignItems: "center",
              padding: "7px 10px", borderBottom: "1px solid var(--border)",
              fontFamily: "monospace", fontSize: 10 }}>
              <span style={{ color: "var(--amber)" }}>{label}</span>
              <span style={{ color: "var(--text)" }}>{row.name ?? "—"}</span>
              <span style={{ marginLeft: "auto", color: "var(--muted)" }}
                title={row.trace_id}>
                {shortTrace(row.trace_id)}{truncated}
              </span>
            </header>
            <ContentBody isTool={isTool} content={row.content} />
          </article>
        );
      })}
    </div>
  );
}
