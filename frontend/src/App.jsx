// src/App.jsx
import { useState, useEffect, useCallback } from "react";
import NodeTable      from "./components/NodeTable";
import EdgeTable      from "./components/EdgeTable";
import TraceTimeline  from "./components/TraceTimeline";
import EventLog       from "./components/EventLog";
import StatusBar      from "./components/StatusBar";
import QueryBar       from "./components/QueryBar";
import { api }        from "./api/client";

const VIEWS = ["nodes", "edges", "trace", "events"];

export default function App() {
  const [view,    setView]    = useState("nodes");
  const [nodes,   setNodes]   = useState([]);
  const [edges,   setEdges]   = useState([]);
  const [events,  setEvents]  = useState([]);
  const [trace,   setTrace]   = useState(null);   // { id, stages: [] }
  const [status,  setStatus]  = useState(null);
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const [n, e, ev, s] = await Promise.all([
        api.nodes(), api.edges(), api.events(), api.status(),
      ]);
      setNodes(n.data?.data  || []);
      setEdges(e.data?.data  || []);
      setEvents(ev.data?.data || []);
      setStatus(s.data       || null);
    } catch { /* bridge not running — use mock data in dev */ }
  }, []);

  const runQuery = async (q) => {
    const lower = q.toLowerCase().trim();
    if (lower.startsWith("\\stitch") || lower.startsWith("stitch")) {
      const id = q.split(/\s+/)[1];
      if (!id) return;
      setLoading(true);
      try {
        const res = await api.trace(id);
        const stages = (res.data || []).flatMap(r => r.data?.data || []);
        setTrace({ id, stages });
        setView("trace");
      } finally { setLoading(false); }
    } else if (lower.includes("node"))  { setView("nodes");  }
    else if (lower.includes("edge"))    { setView("edges");  }
    else if (lower.includes("event"))   { setView("events"); }
  };

  useEffect(() => { refresh(); }, [refresh]);

  // Poll every 5 seconds
  useEffect(() => {
    const t = setInterval(refresh, 5000);
    return () => clearInterval(t);
  }, [refresh]);

  const badge = { nodes: `${nodes.length} nodes`, edges: `${edges.length} edges`, trace: trace ? `${trace.stages.length} stages` : "—", events: `${events.length} events` };

  return (
    <div className="shell">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="logo">STACK<span>TRACER</span></div>
        <nav className="nav">
          {VIEWS.map(v => (
            <div key={v} className={`nav-item ${view === v ? "active" : ""}`} onClick={() => setView(v)}>
              <span className="nav-dot" />
              {v}
            </div>
          ))}
        </nav>
        <div className="sockets">
          <span className="socket-dot" />
          {status?.sockets ?? "?"} socket{status?.sockets !== 1 ? "s" : ""} live
        </div>
      </aside>

      {/* Main */}
      <div className="main">
        <div className="toolbar">
          <span className="toolbar-title">{view}</span>
          <span className="badge">{badge[view]}</span>
        </div>

        <QueryBar onRun={runQuery} loading={loading} />

        <div className="content">
          {view === "nodes"  && <NodeTable  nodes={nodes} />}
          {view === "edges"  && <EdgeTable  edges={edges} />}
          {view === "trace"  && <TraceTimeline trace={trace} />}
          {view === "events" && <EventLog   events={events} />}
        </div>

        <StatusBar nodes={nodes} edges={edges} events={events} status={status} />
      </div>
    </div>
  );
}