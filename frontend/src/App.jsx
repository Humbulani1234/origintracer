import { useCallback, useEffect, useState } from "react";
import NodeTable from "./components/NodeTable";
import EdgeTable from "./components/EdgeTable";
import TraceTimeline from "./components/TraceTimeline";
import EventLog from "./components/Eventlog";
import StatusBar from "./components/StatusBar";
import QueryBar from "./components/QueryBar";
import DiffView from "./components/DiffView";
import GraphView from "./components/GraphView";
import CausalView from "./components/CausalView";
import StatusView from "./components/StatusView";
import CausalHistory from "./components/CausalHistory";
import ContentView from "./components/ContentView";
import ErrorBoundary from "./components/ErrorBoundary";
import { api } from "./api/client";

const VIEWS = [
  "nodes",
  "edges",
  "trace",
  "events",
  "diff",
  "status",
  "graph",
  "causal",
  "history",
  "llm_content",
  "tool_content",
];

function workerResult(response) {
  if (!response?.ok) {
    throw new Error(response?.error || "Worker query failed");
  }
  if (response.data?.error) {
    throw new Error(response.data.error);
  }
  return response.data;
}

function normalizeWorkerNodes(nodes = []) {
  return nodes.map((node) => ({
    ...node,
    node_type: node.node_type ?? node.type,
    avg_duration_ns:
      node.avg_duration_ns ??
      (node.avg_ms == null ? null : node.avg_ms * 1_000_000),
  }));
}

function normalizeWorkerDiff(diff) {
  if (!diff) return null;
  return {
    ...diff,
    added_nodes: diff.added_nodes ?? [],
    removed_nodes: diff.removed_nodes ?? [],
    added_edges: diff.added_edges ?? diff.new_edges ?? [],
    removed_edges: diff.removed_edges ?? [],
  };
}

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
  const [workerInventoryLoaded, setWorkerInventoryLoaded] = useState(false);
  const [selectedWorkerPid, setSelectedWorkerPid] = useState(null);
  const [causalHistory, setCausalHistory] = useState([]);
  const [llmContent, setLlmContent] = useState([]);
  const [toolContent, setToolContent] = useState([]);

  const refresh = useCallback(async () => {
    try {
      const workerResponse = await api.workers();
      const availableWorkers = workerResponse?.data ?? [];
      setWorkers(availableWorkers);
      setWorkerInventoryLoaded(true);

      const selectedStillExists = availableWorkers.find(
        (worker) => String(worker.pid) === String(selectedWorkerPid),
      );
      const selectedWorker = selectedStillExists ?? availableWorkers[0] ?? null;
      const workerPid = selectedWorker ? String(selectedWorker.pid) : null;

      if (workerPid !== selectedWorkerPid) {
        setSelectedWorkerPid(workerPid);
        // Captures belong to one live process; do not show a prior worker's
        // prompts or tool values while the next worker is being selected.
        setLlmContent([]);
        setToolContent([]);
      }

      if (workerPid) {
        const [graphResponse, eventResponse, statusResponse, diffResponse,
          causalResponse, historyResponse] = await Promise.all([
          api.workerQuery(workerPid, "SHOW GRAPH"),
          api.workerQuery(workerPid, "SHOW EVENTS LIMIT 100"),
          api.workerQuery(workerPid, "SHOW STATUS"),
          api.workerQuery(workerPid, "DIFF"),
          api.workerQuery(workerPid, "CAUSAL"),
          api.causalHistory().catch(() => null),
        ]);

        const graph = workerResult(graphResponse)?.data ?? {};
        const liveEvents = workerResult(eventResponse)?.data ?? [];
        const liveStatus = workerResult(statusResponse)?.data ?? {};
        const liveDiff = workerResult(diffResponse)?.data ?? null;
        const liveCausal = workerResult(causalResponse)?.data ?? [];

        setNodes(normalizeWorkerNodes(graph.nodes));
        setEdges(graph.edges ?? []);
        setEvents(liveEvents);
        setStatus({ ...liveStatus, mode: "worker" });
        setDiff(normalizeWorkerDiff(liveDiff));
        setCausal(liveCausal);
        setCausalHistory(historyResponse?.data ?? []);
      } else {
        const historyResponse = await api.causalHistory().catch(() => null);

        setNodes([]);
        setEdges([]);
        setEvents([]);
        setTrace((current) => current?.source === "history" ? current : null);
        setStatus({ mode: "no-worker" });
        setDiff(null);
        setCausal([]);
        setCausalHistory(historyResponse?.data ?? []);
      }

      setBackendError(null);
    } catch (error) {
      console.warn("Backend unavailable:", error?.message ?? error);
      setBackendError(error?.message ?? "Backend unavailable");
    }
  }, [selectedWorkerPid]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  useEffect(() => {
    const timer = setInterval(refresh, 5000);
    return () => clearInterval(timer);
  }, [refresh]);

  const applyQueryResult = (result, query) => {
    const metric = result?.metric;
    const verb = result?.verb;

    if (metric === "nodes") {
      setNodes(normalizeWorkerNodes(result.data));
      setView("nodes");
    } else if (metric === "edges") {
      setEdges(result.data ?? []);
      setView("edges");
    } else if (metric === "events") {
      setEvents(result.data ?? []);
      setView("events");
    } else if (metric === "llm_content") {
      setLlmContent(result.data ?? []);
      setView("llm_content");
    } else if (metric === "tool_content") {
      setToolContent(result.data ?? []);
      setView("tool_content");
    } else if (metric === "graph") {
      setNodes(normalizeWorkerNodes(result.data?.nodes));
      setEdges(result.data?.edges ?? []);
      setView("graph");
    } else if (metric === "critical_path" || verb === "TRACE") {
      setTrace({
        id: result.trace_id ?? query.split(/\s+/)[1] ?? "trace",
        stages: result.data ?? [],
        source: "worker",
      });
      setView("trace");
    } else if (verb === "STATUS") {
      setStatus({ ...(result.data ?? {}), mode: "worker" });
      setView("status");
    } else if (verb === "DIFF") {
      setDiff(normalizeWorkerDiff(result.data));
      setView("diff");
    } else if (verb === "CAUSAL") {
      setCausal(result.data ?? []);
      setView("causal");
    }
  };

  const runQuery = async (query) => {
    const lower = query.toLowerCase().trim();
    if (lower.startsWith("\\stitch") || lower.startsWith("stitch")) {
      const id = query.split(/\s+/)[1];
      if (!id) {
        setView("trace");
        return;
      }
      setLoading(true);
      try {
        const response = await api.trace(id);
        const stages = response?.data?.data || response?.data || [];
        setTrace({ id, stages, source: "history" });
        setView("trace");
        setBackendError(null);
      } catch (error) {
        setBackendError(error?.message ?? "Trace lookup failed");
        setView("trace");
      } finally {
        setLoading(false);
      }
      return;
    }

    if (selectedWorkerPid) {
      setLoading(true);
      try {
        const response = await api.workerQuery(selectedWorkerPid, query);
        applyQueryResult(workerResult(response), query);
        setBackendError(null);
      } catch (error) {
        setBackendError(error?.message ?? "Worker query failed");
      } finally {
        setLoading(false);
      }
      return;
    }

    setBackendError("No live OriginTracer process is available for this query");
  };

  // Content capture is intentionally user-driven. Unlike graph/events/status,
  // it is not fetched by the five-second refresh loop: that keeps redacted
  // prompt, response, and tool previews out of routine dashboard traffic.
  const loadCapturedContent = async (metric) => {
    setView(metric);
    if (!selectedWorkerPid) {
      setBackendError("No live OriginTracer process is available for this query");
      return;
    }

    setLoading(true);
    try {
      const response = await api.workerQuery(
        selectedWorkerPid,
        `SHOW ${metric} LIMIT 100`,
      );
      applyQueryResult(workerResult(response), `SHOW ${metric} LIMIT 100`);
      setBackendError(null);
    } catch (error) {
      setBackendError(error?.message ?? "Worker query failed");
    } finally {
      setLoading(false);
    }
  };

  const selectedWorker = workers.find(
    (worker) => String(worker.pid) === String(selectedWorkerPid),
  );
  const noLiveWorker = workerInventoryLoaded && workers.length === 0;
  const showingHistoricalData = view === "history"
    || (view === "trace" && trace?.source === "history");
  const badge = {
    nodes: `${nodes.length} nodes`,
    edges: `${edges.length} edges`,
    trace: trace ? `${trace.stages.length} stages` : "—",
    events: `${events.length} events`,
    diff: diff
      ? `${(diff.added_nodes?.length || 0) + (diff.added_edges?.length || 0)} changes`
      : "—",
    graph: `${nodes.length} nodes · ${edges.length} edges`,
    llm_content: `${llmContent.length} entries`,
    tool_content: `${toolContent.length} entries`,
    causal: `${causal.length} patterns`,
    status: selectedWorker ? "live" : noLiveWorker ? "no worker" : "…",
    history: `${causalHistory.length} snapshots`,
  };

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="logo">ORIGIN<span>TRACER</span></div>
        <nav className="nav">
          {VIEWS.map((item) => (
            <div
              key={item}
              className={`nav-item ${view === item ? "active" : ""}`}
              onClick={() => (
                item === "llm_content" || item === "tool_content"
                  ? loadCapturedContent(item)
                  : setView(item)
              )}
            >
              <span className="nav-dot" />
              {item.replace("_", " ")}
            </div>
          ))}
        </nav>

        {workers.length > 0 && (
          <div className="sockets">
            <div className="socket-title">LIVE PROCESSES</div>
            {workers.map((worker) => {
              const active = String(worker.pid) === String(selectedWorkerPid);
              return (
                <button
                  type="button"
                  key={worker.pid}
                  className={`worker-option ${active ? "active" : ""}`}
                  onClick={() => setSelectedWorkerPid(String(worker.pid))}
                  title={worker.socket}
                >
                  <span className="socket-dot" />
                  pid {worker.pid}
                </button>
              );
            })}
          </div>
        )}
      </aside>

      <div className="main">
        <div className="toolbar">
          <span className="toolbar-title">{view.replace("_", " ")}</span>
          {selectedWorker && (
            <span className="worker-badge">pid {selectedWorker.pid}</span>
          )}
          <span className="badge">{badge[view]}</span>
          {backendError && (
            <span className="backend-error">{backendError}</span>
          )}
        </div>
        <QueryBar onRun={runQuery} loading={loading} />
        <div className="content">
          <ErrorBoundary key={view}>
            {noLiveWorker && !showingHistoricalData ? (
              <div className="no-worker-state">
                <div className="no-worker-title">
                  No live OriginTracer processes
                </div>
                <div>
                  Start an instrumented process to expose an OriginTracer
                  worker socket. This dashboard will select it automatically.
                </div>
                <div className="no-worker-history">
                  Historical causal history and explicit \stitch queries remain
                  available.
                </div>
              </div>
            ) : (
              <>
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
                    worker={selectedWorker}
                  />
                )}
                {view === "history" && <CausalHistory history={causalHistory} />}
                {view === "llm_content" && (
                  <ContentView kind="llm" rows={llmContent} />
                )}
                {view === "tool_content" && (
                  <ContentView kind="tool" rows={toolContent} />
                )}
              </>
            )}
          </ErrorBoundary>
        </div>
        <StatusBar
          nodes={nodes}
          edges={edges}
          events={events}
          status={status}
          worker={selectedWorker}
          workerInventoryLoaded={workerInventoryLoaded}
        />
      </div>
    </div>
  );
}
