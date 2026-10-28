function shortTrace(traceId) {
  if (!traceId) return "—";
  return traceId.length > 18 ? `${traceId.slice(0, 18)}…` : traceId;
}

function parseStructuredValue(content) {
  if (typeof content !== "string") return undefined;
  try {
    return JSON.parse(content);
  } catch {
    // JSON cannot represent undefined, so it is a safe parse-failure marker.
    return undefined;
  }
}

function extractStructuredBlock(content) {
  if (typeof content !== "string") return null;
  for (let start = 0; start < content.length; start += 1) {
    const opening = content[start];
    if (opening !== "{" && opening !== "[") continue;

    const stack = [];
    let inString = false;
    let escaped = false;
    for (let end = start; end < content.length; end += 1) {
      const char = content[end];
      if (inString) {
        if (escaped) escaped = false;
        else if (char === "\\") escaped = true;
        else if (char === '"') inString = false;
        continue;
      }
      if (char === '"') inString = true;
      else if (char === "{" || char === "[") stack.push(char);
      else if (char === "}" || char === "]") {
        const expected = char === "}" ? "{" : "[";
        if (stack.pop() !== expected) break;
        if (!stack.length) {
          const raw = content.slice(start, end + 1);
          try {
            return {
              before: content.slice(0, start),
              value: JSON.parse(raw),
              after: content.slice(end + 1),
            };
          } catch {
            break;
          }
        }
      }
    }
    // Do not treat braces inside a failed or truncated outer block as a new
    // top-level JSON fragment. The caller can still render this whole prefix
    // with the tolerant parser.
    return null;
  }
  return null;
}

function parseJsonPrefix(source) {
  let index = 0;
  const skipSpace = () => {
    while (/\s/.test(source[index] ?? "")) index += 1;
  };
  const readString = () => {
    const start = index++;
    let escaped = false;
    while (index < source.length) {
      const char = source[index++];
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') {
        try { return JSON.parse(source.slice(start, index)); }
        catch { return source.slice(start + 1, index - 1); }
      }
    }
    let raw = source.slice(start);
    if (escaped) raw = raw.slice(0, -1);
    try { return JSON.parse(`${raw}"`); }
    catch { return raw.slice(1).replaceAll("\\\"", '"'); }
  };
  const readValue = () => {
    skipSpace();
    const char = source[index];
    if (char === '"') return readString();
    if (char === "{") {
      const result = {};
      index += 1;
      while (index < source.length) {
        skipSpace();
        if (source[index] === "}") { index += 1; break; }
        if (source[index] !== '"') break;
        const key = readString();
        skipSpace();
        if (source[index] !== ":") break;
        index += 1;
        result[key] = readValue();
        skipSpace();
        if (source[index] === ",") { index += 1; continue; }
        if (source[index] === "}") { index += 1; break; }
        break;
      }
      return result;
    }
    if (char === "[") {
      const result = [];
      index += 1;
      while (index < source.length) {
        skipSpace();
        if (source[index] === "]") { index += 1; break; }
        const before = index;
        result.push(readValue());
        skipSpace();
        if (source[index] === ",") { index += 1; continue; }
        if (source[index] === "]") { index += 1; break; }
        if (index === before || index >= source.length) break;
        break;
      }
      return result;
    }
    const rest = source.slice(index);
    const literal = /^(true|false|null|-?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)/.exec(rest);
    if (literal) {
      index += literal[0].length;
      return literal[0] === "true" ? true
        : literal[0] === "false" ? false
          : literal[0] === "null" ? null : Number(literal[0]);
    }
    return undefined;
  };

  const value = readValue();
  return value !== undefined && value !== null && typeof value === "object"
    ? value : undefined;
}

function parseNestedJson(value) {
  let candidate = value;
  // Metadata can contain JSON encoded as a string more than once: each
  // serialization layer adds another level of escaped quotes and slashes.
  for (let depth = 0; depth < 4 && typeof candidate === "string"; depth += 1) {
    const trimmed = candidate.trim();
    if (trimmed.startsWith("{\\\"") || trimmed.startsWith("[\\\"")) {
      try {
        candidate = JSON.parse(trimmed.replaceAll('\\"', '"'));
        if (candidate !== null && typeof candidate === "object") {
          return candidate;
        }
        continue;
      } catch {
        // Try the regular JSON decoding path below.
      }
    }
    if (!trimmed.startsWith("{") && !trimmed.startsWith("[")
      && !trimmed.startsWith('"')) return undefined;
    try {
      candidate = JSON.parse(trimmed);
    } catch {
      candidate = parseJsonPrefix(trimmed);
      if (candidate !== undefined) return candidate;
      return undefined;
    }
    if (candidate !== null && typeof candidate === "object") {
      return candidate;
    }
  }
  return undefined;
}

function ToolValue({ value, depth = 0 }) {
  const nested = value !== null && typeof value === "object";
  if (!nested) {
    const parsedString = parseNestedJson(value);
    if (parsedString !== undefined && depth < 8) {
      return <ToolValue value={parsedString} depth={depth + 1} />;
    }
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

function InlineMarkdown({ text }) {
  const tokens = text.split(/(\*\*[^*]+\*\*|``[^`]+``|`[^`]+`|\*[^*]+\*)/g);
  return tokens.map((token, index) => {
    if (token.startsWith("**") && token.endsWith("**")) {
      return (
        <strong key={index}>
          <InlineMarkdown text={token.slice(2, -2)} />
        </strong>
      );
    }
    if (token.startsWith("`") && token.endsWith("`")) {
      const delimiterSize = token.startsWith("``") ? 2 : 1;
      return (
        <code key={index} style={{ padding: "1px 4px", borderRadius: 3,
          color: "var(--amber)", background: "var(--bg2)",
          fontFamily: "var(--mono)", fontSize: "0.92em" }}>
          {token.slice(delimiterSize, -delimiterSize)}
        </code>
      );
    }
    if (token.startsWith("*") && token.endsWith("*")) {
      return (
        <em key={index}>
          <InlineMarkdown text={token.slice(1, -1)} />
        </em>
      );
    }
    return token;
  });
}

function isMarkdownBlockStart(line) {
  return /^(#{1,6})\s|^```|^\s*[-*+]\s|^\s*\d+\.\s|^>\s?/.test(line);
}

function MarkdownBody({ content }) {
  const lines = String(content ?? "").replace(/\r\n?/g, "\n").split("\n");
  const blocks = [];

  for (let index = 0; index < lines.length;) {
    const line = lines[index];
    if (!line.trim()) { index += 1; continue; }

    const fence = /^```([^`]*)$/.exec(line.trim());
    if (fence) {
      const code = [];
      index += 1;
      while (index < lines.length && !/^```\s*$/.test(lines[index].trim())) {
        code.push(lines[index]);
        index += 1;
      }
      if (index < lines.length) index += 1;
      blocks.push(
        <pre key={`code-${blocks.length}`} style={{ margin: "10px 0",
          padding: 12, overflowX: "auto", borderRadius: 4,
          color: "var(--text)", background: "var(--bg2)",
          fontFamily: "var(--mono)", fontSize: 11, lineHeight: 1.6 }}>
          <code>{code.join("\n")}</code>
        </pre>,
      );
      continue;
    }

    const heading = /^(#{1,6})\s+(.*)$/.exec(line);
    if (heading) {
      const level = heading[1].length;
      const style = { margin: level <= 2 ? "18px 0 8px" : "14px 0 6px",
        color: "var(--text)", fontSize: level <= 2 ? 17 : 14,
        lineHeight: 1.35, fontWeight: 650 };
      blocks.push(
        <div key={`heading-${blocks.length}`} style={style}>
          <InlineMarkdown text={heading[2]} />
        </div>,
      );
      index += 1;
      continue;
    }

    const listItem = /^\s*([-*+]|\d+\.)\s+(.*)$/.exec(line);
    if (listItem) {
      const ordered = listItem[1].endsWith(".");
      const items = [];
      while (index < lines.length) {
        const item = /^\s*([-*+]|\d+\.)\s+(.*)$/.exec(lines[index]);
        if (!item || item[1].endsWith(".") !== ordered) break;
        items.push(item[2]);
        index += 1;
      }
      const List = ordered ? "ol" : "ul";
      blocks.push(
        <List key={`list-${blocks.length}`} style={{ margin: "8px 0",
          paddingLeft: 24, color: "var(--text)", lineHeight: 1.65 }}>
          {items.map((item, itemIndex) => (
            <li key={itemIndex} style={{ paddingLeft: 3, margin: "3px 0" }}>
              <InlineMarkdown text={item} />
            </li>
          ))}
        </List>,
      );
      continue;
    }

    if (/^>\s?/.test(line)) {
      const quote = [];
      while (index < lines.length && /^>\s?/.test(lines[index])) {
        quote.push(lines[index].replace(/^>\s?/, ""));
        index += 1;
      }
      blocks.push(
        <blockquote key={`quote-${blocks.length}`} style={{ margin: "10px 0",
          padding: "3px 12px", borderLeft: "3px solid var(--amber)",
          color: "var(--muted)" }}>
          <InlineMarkdown text={quote.join(" ")} />
        </blockquote>,
      );
      continue;
    }

    const paragraph = [line];
    index += 1;
    while (index < lines.length && lines[index].trim()
      && !isMarkdownBlockStart(lines[index])) {
      paragraph.push(lines[index]);
      index += 1;
    }
    blocks.push(
      <p key={`paragraph-${blocks.length}`} style={{ margin: "8px 0",
        color: "var(--text)", lineHeight: 1.65 }}>
        <InlineMarkdown text={paragraph.join(" ")} />
      </p>,
    );
  }

  return (
    <div style={{ padding: 14, fontFamily: "var(--sans, sans-serif)",
      fontSize: 13, color: "var(--text)", overflowWrap: "anywhere" }}>
      {blocks}
    </div>
  );
}

function parseToolCallSummaries(content) {
  if (typeof content !== "string") return null;
  const blocks = content.trim().split(/\n{2,}/);
  if (!blocks.length) return null;
  const calls = blocks.map(block => {
    const match = block.match(/^tool call:\s*(.+?)\s*\narguments:\s*([\s\S]*)$/i);
    if (!match) return null;
    let args = match[2].trim();
    try { args = JSON.parse(args); } catch { /* Keep readable raw arguments. */ }
    return { name: match[1], args };
  });
  return calls.every(Boolean) ? calls : null;
}

function ContentBody({ content, isTool }) {
  const toolCalls = isTool ? null : parseToolCallSummaries(content);
  if (toolCalls) {
    return (
      <div style={{ padding: 14, color: "var(--text)", background: "var(--bg)" }}>
        <div style={{ marginBottom: 10, color: "var(--muted)", fontSize: 11 }}>
          The model requested these tools. Inputs and results appear in the tool content view.
        </div>
        {toolCalls.map((call, index) => (
          <section key={`${call.name}-${index}`} style={{ marginBottom: 10,
              padding: 10, border: "1px solid var(--border)",
              borderRadius: 4, background: "var(--bg2)" }}>
            <div style={{ marginBottom: 6, color: "var(--amber)",
                fontFamily: "var(--mono)", fontSize: 12 }}>
              {call.name}
            </div>
            <div style={{ color: "var(--muted)", fontSize: 10,
                marginBottom: 4 }}>Arguments</div>
            <ToolValue value={call.args} />
          </section>
        ))}
      </div>
    );
  }

  // Captured prompts and responses can also be JSON (for example, a list of
  // chat messages). Render that shape as fields instead of exposing escapes.
  const parsedValue = parseStructuredValue(content);
  const isParsedValue = parsedValue !== undefined;
  const embedded = isParsedValue ? null : extractStructuredBlock(content);
  const partialEmbedded = !embedded && !isParsedValue
    ? (() => {
      const start = typeof content === "string"
        ? content.search(/[\[{]/) : -1;
      if (start < 0) return null;
      const value = parseJsonPrefix(content.slice(start));
      return value ? { before: content.slice(0, start), value, after: "" } : null;
    })()
    : null;
  const structuredBlock = embedded ?? partialEmbedded;

  if (isParsedValue) {
    return (
      <div style={{ margin: 0, padding: 14, overflowX: "auto",
        fontFamily: "var(--mono)", fontSize: 12, lineHeight: 1.65,
        color: "var(--text)", background: "var(--bg)" }}>
        <ToolValue value={parsedValue} />
      </div>
    );
  }

  if (structuredBlock) {
    return (
      <div style={{ color: "var(--text)", background: "var(--bg)" }}>
        {structuredBlock.before.trim() && (isTool ? (
          <pre style={{ margin: 0, padding: 14, whiteSpace: "pre-wrap",
            overflowWrap: "anywhere", fontFamily: "var(--mono)",
            fontSize: 12, lineHeight: 1.65 }}>
            {structuredBlock.before.trimEnd()}
          </pre>
        ) : <MarkdownBody content={structuredBlock.before.trimEnd()} />)}
        <div style={{ padding: 14, overflowX: "auto", fontFamily: "var(--mono)",
          fontSize: 12, lineHeight: 1.65 }}>
          <ToolValue value={structuredBlock.value} />
        </div>
        {structuredBlock.after.trim() && (isTool ? (
          <pre style={{ margin: 0, padding: 14, whiteSpace: "pre-wrap",
            overflowWrap: "anywhere", fontFamily: "var(--mono)",
            fontSize: 12, lineHeight: 1.65 }}>
            {structuredBlock.after.trim()}
          </pre>
        ) : <MarkdownBody content={structuredBlock.after.trim()} />)}
      </div>
    );
  }

  if (!isTool) return <MarkdownBody content={content} />;

  return (
    <pre style={{ margin: 0, padding: 14, overflowX: "auto",
      whiteSpace: "pre-wrap", overflowWrap: "anywhere",
      fontFamily: "var(--mono)", fontSize: 12, lineHeight: 1.65,
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
              padding: "10px 14px", borderBottom: "1px solid var(--border)",
              fontFamily: "monospace", fontSize: 11, flexWrap: "wrap" }}>
              <span style={{ color: "var(--amber)" }}>{label}</span>
              <span style={{ color: "var(--text)", fontWeight: 600 }}>{row.name ?? "—"}</span>
              <span style={{ marginLeft: "auto", color: "var(--muted)" }}
                title={row.trace_id}>
                {shortTrace(row.trace_id)}{truncated}
              </span>
            </header>
            <ContentBody content={row.content} isTool={isTool} />
          </article>
        );
      })}
    </div>
  );
}
