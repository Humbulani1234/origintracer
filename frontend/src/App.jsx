import { useState, useEffect, useCallback } from "react";
import NodeTable from "./components/NodeTable";
import EdgeTable from "./components/EdgeTable";
import TraceTimeline from "./components/TraceTimeline";
import EventLog from "./components/EventLog";
import StatusBar from "./components/StatusBar";
import QueryBar from "./components/QueryBar";
import DiffView from "./components/DiffView";
import GraphView from "./components/GraphView";
import CausalView from "./components/CausalView";
import StatusView from "./components/StatusView";
import { api } from "./api/client";

const VIEWS = ["nodes", "edges", "trace", "events", "diff",
  "status", "graph", "causal"];

export default function App() {
  const [view, setView] = useState("nodes");
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [trace, setTrace] = useState(null);
  const [events, setEvents] = useState([]);
  const [diff, setDiff] = useState(null);
  const [status, setStatus] = useState({
    customer_id: null,
    snapshot: null,
    storage: null,
    timestamp: null,
  });
  const [causal, setCausal] = useState([]);
  const [loading, setLoading] = useState(false);
  const [backendError, setBackendError] = useState(null);
  const [workers, setWorkers] = useState([]);

  const refresh = useCallback(async () => {
    try {
      setBackendError(null);
      const [n, e, ev, s, d, g,] = await Promise.all([
        api.nodes(), api.edges(), api.events(), api.status(),
        api.diff(), api.graph()
      ]);
      const ca = await api.causal("origintracer-snapshot");
      const ws = await api.workers();
      if (n?.data?.data?.length) setNodes(n.data.data);
      if (e?.data?.data?.length) setEdges(e.data.data);
      if (ev?.data?.length) setEvents(ev.data);
      if (s) setStatus(s);
      if (d?.data) setDiff(d.data);
      if (g?.data?.data) setGraph(g.data.data);
      if (ca?.data?.length) setCausal(ca.data);
      if (ws?.data?.length) setWorkers(ws.data);
    } catch (err) {
      console.warn("Backend unavailable:", err?.message ?? err);
      setBackendError(err?.message ?? "Backend unavailable");
    }
  }, []);

  useEffect(() => { refresh(); }, [refresh]);
  useEffect(() => {
    const t = setInterval(refresh, 5000);
    return () => clearInterval(t);
  }, [refresh]);
  
  const runQuery = async (q) => {
    const lower = q.toLowerCase().trim();
    if (lower.startsWith("\\stitch") || lower.startsWith("stitch")) {
      const id = q.split(/\s+/)[1];
      if (!id) { setView("trace"); return; }
      setLoading(true);
      try {
        const res = await api.trace(id);
        const stages = res?.data?.data || res?.data || [];
        if (stages.length) setTrace({ id, stages });
        setView("trace");
      } catch { setView("trace"); }
      finally  { setLoading(false); }
    } else if (lower.includes("node")) setView("nodes");
    else if (lower.includes("edge")) setView("edges");
    else if (lower.includes("event")) setView("events");
    else if (lower.includes("trace")) setView("trace");
  };

  const badge = {
    nodes: `${nodes.length} nodes`,
    edges: `${edges.length} edges`,
    trace: trace ? `${trace.stages.length} stages` : "—",
    events: `${events.length} events`,
    diff: diff ? `${(diff.added_nodes?.length || 0) + (diff.added_edges?.length || 0)} changes` : "—",
    graph: `${nodes.length} nodes · ${edges.length} edges`,
    causal: `${causal.length} patterns`,
    status: "live",
  };

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="logo">STACK<span>TRACER</span></div>
        <nav className="nav">
          {VIEWS.map(v => (
            <div key={v}
              className={`nav-item ${view === v ? "active" : ""}`}
              onClick={() => setView(v)}>
              <span className="nav-dot" />
              {v}
            </div>
          ))}
        </nav>

        {workers.length > 1 && (
          <div style={{ padding: "8px 14px", borderTop: "1px solid var(--border)" }}>
            <div style={{ fontFamily: "monospace", fontSize: 9,
                color: "var(--muted)", marginBottom: 6, letterSpacing: "0.06em" }}>
              WORKERS
            </div>
            {workers.map(w => (
              <div key={w.pid}
                style={{
                  fontFamily: "monospace", fontSize: 10, padding: "3px 0",
                  cursor: "pointer",
                  color: "var(--muted)",
                }}>
                <span style={{
                  display: "inline-block", width: 6, height: 6,
                  borderRadius: "50%", marginRight: 6,
                  background: "#555",
                  verticalAlign: "middle",
                }} />
                pid {w.pid}
              </div>
            ))}
          </div>
        )}
      </aside>

      <div className="main">
        <div className="toolbar">
          <span className="toolbar-title">{view}</span>
          <span className="badge">{badge[view]}</span>
          {backendError && (
            <span style={{ fontFamily:"monospace", fontSize:10,
                color:"var(--red, #e05252)", marginLeft:"auto" }}>
              {backendError}
            </span>
          )}
        </div>
        <QueryBar onRun={runQuery} loading={loading} />
        <div className="content">
          {view === "nodes" && <NodeTable nodes={nodes} />}
          {view === "edges" && <EdgeTable edges={edges} />}
          {view === "trace" && <TraceTimeline trace={trace} />}
          {view === "events" && <EventLog events={events} />}
          {view === "diff" && <DiffView diff={diff} />}
          {view === "graph" && <GraphView nodes={nodes} edges={edges} />}
          {view === "causal" && <CausalView causal={causal} />}
          {view === "status" && (
            <StatusView
              nodes={nodes}
              edges={edges}
              events={events}
              status={status}
            />
          )}
        </div>
        <StatusBar nodes={nodes} edges={edges} events={events} status={status} />
      </div>
    </div>
  );
}