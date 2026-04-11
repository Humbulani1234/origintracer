// src/components/GraphView.jsx
import { svcColor, short } from "./NodeTable";

const EDGE_DIM = "rgba(120,120,120,0.45)";

function NodeRow({ node }) {
  const type = node.type ?? node.node_type ?? node.service ?? "";
  const c = svcColor(type);
  return (
    <div style={{
      display: "grid", gridTemplateColumns: "100px 1fr auto",
      gap: 10, alignItems: "center", padding: "5px 14px",
      borderBottom: "1px solid rgba(42,42,42,0.4)",
      fontFamily: "monospace", fontSize: 10,
    }}>
      <span style={{
        background: c + "22", color: c,
        border: `1px solid ${c}55`,
        borderRadius: 4, padding: "2px 6px",
        textAlign: "center", fontSize: 9,
      }}>
        {short(type, 12)}
      </span>
      <span style={{ color: "var(--white, #e8e8e8)", fontWeight: 500 }}>
        {node.id}
      </span>
      <span style={{ color: "var(--cyan, #5ec4c4)" }}>
        ×{node.call_count ?? 0}
      </span>
    </div>
  );
}

function EdgeRow({ edge }) {
  const src   = edge.source ?? edge.from ?? "?";
  const tgt   = edge.target ?? edge.to   ?? "?";
  const etype = edge.type ?? "";
  const cnt   = edge.call_count ?? edge.weight ?? null;
  const suffix = [etype, cnt ? `×${cnt}` : ""].filter(Boolean).join(" ");

  return (
    <div style={{
      display: "grid", gridTemplateColumns: "1fr auto 1fr auto",
      gap: 8, alignItems: "center", padding: "5px 14px",
      borderBottom: "1px solid rgba(42,42,42,0.4)",
      fontFamily: "monospace", fontSize: 10,
    }}>
      <span style={{ color: "var(--amber, #d4a843)", fontWeight: 500 }}>
        {short(src, 28)}
      </span>
      <span style={{ color: EDGE_DIM }}>→</span>
      <span style={{ color: "var(--amber, #d4a843)", fontWeight: 500 }}>
        {short(tgt, 28)}
      </span>
      <span style={{ color: EDGE_DIM }}>
        {suffix ? `[${suffix}]` : ""}
      </span>
    </div>
  );
}

function SectionLabel({ text }) {
  return (
    <div style={{
      padding: "8px 14px 4px",
      fontFamily: "monospace", fontSize: 9,
      color: "var(--muted)", letterSpacing: "0.08em",
      borderBottom: "1px solid rgba(42,42,42,0.6)",
    }}>
      {text}
    </div>
  );
}

export default function GraphView({ nodes = [], edges = [] }) {
  if (!nodes.length && !edges.length) {
    return (
      <div style={{
        padding: "32px 14px", fontFamily: "monospace",
        fontSize: 11, color: "var(--muted)", textAlign: "center",
      }}>
        No graph data — nodes and edges appear here once the engine is running.
      </div>
    );
  }

  return (
    <div>
      {nodes.length > 0 && (
        <>
          <SectionLabel text={`NODES  ·  ${nodes.length}`} />
          {nodes.map(n => <NodeRow key={n.id} node={n} />)}
        </>
      )}
      {edges.length > 0 && (
        <>
          <SectionLabel text={`EDGES  ·  ${edges.length}`} />
          {edges.map((e, i) => <EdgeRow key={i} edge={e} />)}
        </>
      )}
    </div>
  );
}