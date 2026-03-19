// src/App.jsx
import { useState, useEffect, useCallback } from "react";
import NodeTable     from "./components/NodeTable";
import EdgeTable     from "./components/EdgeTable";
import TraceTimeline from "./components/TraceTimeline";
import EventLog      from "./components/EventLog";
import StatusBar     from "./components/StatusBar";
import QueryBar      from "./components/QueryBar";
import { api }       from "./api/client";

const VIEWS = ["nodes", "edges", "trace", "events"];

const MOCK_NODES = [
  { id:"gunicorn::master",                         service:"gunicorn", call_count:1,   avg_duration_ns:null },
  { id:"gunicorn::UvicornWorker-24861",            service:"gunicorn", call_count:1,   avg_duration_ns:null },
  { id:"uvicorn::/n1/",                            service:"uvicorn",  call_count:360, avg_duration_ns:9200000 },
  { id:"uvicorn::/async/",                         service:"uvicorn",  call_count:600, avg_duration_ns:9800000 },
  { id:"uvicorn::/db/",                            service:"uvicorn",  call_count:600, avg_duration_ns:8900000 },
  { id:"uvicorn::/slow/",                          service:"uvicorn",  call_count:120, avg_duration_ns:31000000 },
  { id:"asyncio::ASGIHandler.listen_for_disconnect", service:"asyncio", call_count:600, avg_duration_ns:240000 },
  { id:"asyncio::ASGIHandler.handle.<locals>.process_request", service:"asyncio", call_count:600, avg_duration_ns:50000 },
  { id:"django::/n1/",                             service:"django",   call_count:180, avg_duration_ns:10200000 },
  { id:"django::/async/",                          service:"django",   call_count:300, avg_duration_ns:9400000 },
  { id:"django::/db/",                             service:"django",   call_count:300, avg_duration_ns:8700000 },
  { id:"django::NPlusOneView",                     service:"django",   call_count:180, avg_duration_ns:9100000 },
  { id:"django::AsyncView",                        service:"django",   call_count:300, avg_duration_ns:9200000 },
  { id:"django::DbView",                           service:"django",   call_count:300, avg_duration_ns:8800000 },
  { id:'django::SELECT COUNT(*) FROM auth_user',   service:"django",   call_count:150, avg_duration_ns:2100000 },
  { id:'django::SELECT django_tracer_author.id...', service:"django",  call_count:90,  avg_duration_ns:1800000 },
  { id:"asyncio::AsyncView.get.<locals>.fetch_a", service:"asyncio",  call_count:150, avg_duration_ns:4100000 },
  { id:"asyncio::AsyncView.get.<locals>.fetch_b", service:"asyncio",  call_count:150, avg_duration_ns:3900000 },
  { id:"celery::MainProcess",                      service:"celery",   call_count:1,   avg_duration_ns:null },
  { id:"celery::ForkPoolWorker",                   service:"celery",   call_count:1,   avg_duration_ns:null },
  { id:"celery::myapp.tasks.process_report",       service:"celery",   call_count:4,   avg_duration_ns:71742000 },
  { id:"redis::GET",                               service:"redis",    call_count:240, avg_duration_ns:800000 },
  { id:"redis::SET",                               service:"redis",    call_count:80,  avg_duration_ns:900000 },
];

const MOCK_EDGES = [
  { 
    source:"gunicorn::master", target:"gunicorn::UvicornWorker-24861",
    type:"spawned", call_count:1 
  },
  { 
    source:"gunicorn::UvicornWorker-24861", target:"uvicorn::/n1/",
    type:"handled", call_count:360 
  },
  { source:"uvicorn::/n1/",                target:"django::/n1/",                          type:"calls",   call_count:180 },
  { source:"django::/n1/",                 target:"django::NPlusOneView",                  type:"calls",   call_count:180 },
  { source:"django::NPlusOneView",         target:"django::SELECT COUNT(*) FROM auth_user",type:"calls",   call_count:150 },
  { source:"django::NPlusOneView",         target:"django::SELECT django_tracer_author.id...", type:"calls", call_count:90 },
  { source:"django::/async/",              target:"django::AsyncView",                     type:"calls",   call_count:300 },
  { source:"django::AsyncView",            target:"asyncio::AsyncView.get.<locals>.fetch_a", type:"calls", call_count:150 },
  { source:"django::AsyncView",            target:"asyncio::AsyncView.get.<locals>.fetch_b", type:"calls", call_count:150 },
  { source:"celery::MainProcess",          target:"celery::ForkPoolWorker",                type:"spawned", call_count:1 },
  { source:"celery::ForkPoolWorker",       target:"celery::myapp.tasks.process_report",    type:"ran",     call_count:4 },
  { source:"django::/n1/",                target:"redis::GET",                             type:"calls",   call_count:240 },
  { source:"django::/n1/",                target:"redis::SET",                             type:"calls",   call_count:80 },
];

const MOCK_EVENTS = [
  { wall_time:"14:27:57.852", probe:"uvicorn.request.receive",  service:"uvicorn",  name:"/n1/",              duration_ms:null },
  { wall_time:"14:27:57.347", probe:"request.entry",            service:"django",   name:"/n1/",              duration_ms:115.56 },
  { wall_time:"14:27:57.522", probe:"django.view.enter",        service:"django",   name:"NPlusOneView",      duration_ms:162.33 },
  { wall_time:"14:27:57.678", probe:"request.exit",             service:"django",   name:"/n1/",              duration_ms:89.82 },
  { wall_time:"14:27:57.213", probe:"asyncio.task.create",      service:"asyncio",  name:"ASGIHandler.listen_for_disconnect", duration_ms:242.81 },
  { wall_time:"14:27:55.400", probe:"celery.task.start",        service:"celery",   name:"myapp.tasks.process_report", duration_ms:null },
  { wall_time:"14:27:55.553", probe:"celery.task.end",          service:"celery",   name:"myapp.tasks.process_report", duration_ms:71.74 },
  { wall_time:"14:27:54.110", probe:"redis.command.execute",    service:"redis",    name:"GET",               duration_ms:0.8 },
  { wall_time:"14:27:54.115", probe:"redis.command.execute",    service:"redis",    name:"SET",               duration_ms:0.9 },
];

const MOCK_TRACE = {
  id: "ab66749e-edc4-4d15-9fc8-d83c292da50d",
  stages: [
    { probe:"uvicorn.request.receive",  service:"uvicorn", name:"/n1/",         duration_ms:null },
    { probe:"asyncio.task.create",      service:"asyncio", name:"ASGIHandler.listen_for_disconnect", duration_ms:242.81 },
    { probe:"asyncio.task.create",      service:"asyncio", name:"ASGIHandler.handle.<locals>.process_request", duration_ms:46.97 },
    { probe:"request.entry",            service:"django",  name:"/n1/",         duration_ms:115.56 },
    { probe:"django.view.enter",        service:"django",  name:"NPlusOneView", duration_ms:162.33 },
    { probe:"django.view.exit",         service:"django",  name:"NPlusOneView", duration_ms:95.51 },
    { probe:"request.exit",             service:"django",  name:"/n1/",         duration_ms:89.82 },
    { probe:"uvicorn.response.start",   service:"uvicorn", name:"/n1/",         duration_ms:4.51 },
    { probe:"uvicorn.response.body",    service:"uvicorn", name:"/n1/",         duration_ms:0.23 },
    { probe:"uvicorn.request.complete", service:"uvicorn", name:"/n1/",         duration_ms:176.78 },
  ],
};

export default function App() {
  const [view,    setView]    = useState("nodes");
  const [nodes,   setNodes]   = useState(MOCK_NODES);
  const [edges,   setEdges]   = useState(MOCK_EDGES);
  const [events,  setEvents]  = useState(MOCK_EVENTS);
  const [trace,   setTrace]   = useState(MOCK_TRACE);
  const [status,  setStatus]  = useState({ sockets: 2 });
  const [loading, setLoading] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const [n, e, ev, s] = await Promise.all([
        api.nodes(), api.edges(), api.events(), api.status(),
      ]);
      if (n?.data?.data?.length)  setNodes(n.data.data);
      if (e?.data?.data?.length)  setEdges(e.data.data);
      if (ev?.data?.data?.length) setEvents(ev.data.data);
      if (s?.data)                setStatus(s.data);
    } catch {
      // backend not running — mock data stays
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
        const stages = (res.data || []).flatMap(r => r.data?.data || []);
        if (stages.length) setTrace({ id, stages });
        setView("trace");
      } catch { setView("trace"); }
      finally  { setLoading(false); }
    } else if (lower.includes("node"))  setView("nodes");
    else if (lower.includes("edge"))    setView("edges");
    else if (lower.includes("event"))   setView("events");
    else if (lower.includes("trace"))   setView("trace");
  };

  const badge = {
    nodes:  `${nodes.length} nodes`,
    edges:  `${edges.length} edges`,
    trace:  trace ? `${trace.stages.length} stages` : "—",
    events: `${events.length} events`,
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
        <div className="sockets">
          <span className="socket-dot" />
          {status?.sockets ?? "?"} socket{status?.sockets !== 1 ? "s" : ""} live
        </div>
      </aside>

      <div className="main">
        <div className="toolbar">
          <span className="toolbar-title">{view}</span>
          <span className="badge">{badge[view]}</span>
        </div>
        <QueryBar onRun={runQuery} loading={loading} />
        <div className="content">
          {view === "nodes"  && <NodeTable     nodes={nodes} />}
          {view === "edges"  && <EdgeTable     edges={edges} />}
          {view === "trace"  && <TraceTimeline trace={trace} />}
          {view === "events" && <EventLog      events={events} />}
        </div>
        <StatusBar nodes={nodes} edges={edges} events={events} status={status} />
      </div>
    </div>
  );
}