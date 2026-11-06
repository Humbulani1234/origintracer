import { useCallback, useEffect, useRef, useState } from "react";
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
  "stitch",
  "events",
  "diff",
  "status",
  "graph",
  "causal",
  "history",
  "llm_content",
  "tool_content",
];

const WORKERS_PER_PAGE = 4;

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

function formatSnapshotTime(value) {
  if (value == null) return "time unknown";
  const date = new Date(typeof value === "number" ? value * 1000 : value);
  return Number.isNaN(date.getTime()) ? "time unknown" : date.toLocaleTimeString();
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
  const [snapshotInventoryLoaded, setSnapshotInventoryLoaded] = useState(false);
  const [selectedWorkerInstanceId, setSelectedWorkerInstanceId] = useState(() =>
    window.localStorage.getItem("origintracer.selected_worker_instance_id"),
  );
  const [snapshots, setSnapshots] = useState([]);
  const [workerPage, setWorkerPage] = useState(0);
  const [causalHistory, setCausalHistory] = useState([]);
  const [llmContent, setLlmContent] = useState([]);
  const [toolContent, setToolContent] = useState([]);
  const refreshInFlight = useRef(false);

  const refresh = useCallback(async () => {
    // Prevent a slow poll from overlapping the next interval and letting an
    // older inventory response overwrite a newer worker count or graph.
    if (refreshInFlight.current) return;
    refreshInFlight.current = true;
    try {
      const snapshotResponse = await api.snapshots();
      const reportingSnapshots = (snapshotResponse?.data ?? []).filter(
        (snapshot) => snapshot?.online === true,
      );
      setSnapshots(reportingSnapshots);
      setSnapshotInventoryLoaded(true);

      const savedInstanceId =
        selectedWorkerInstanceId ||
        window.localStorage.getItem("origintracer.selected_worker_instance_id");
      const selectedSnapshot = reportingSnapshots.find(
        (snapshot) => snapshot.worker_instance_id === savedInstanceId,
      ) ?? reportingSnapshots[0] ?? null;
      const instanceId = selectedSnapshot?.worker_instance_id ?? null;

      if (instanceId !== selectedWorkerInstanceId) {
        setSelectedWorkerInstanceId(instanceId);
        if (instanceId) {
          window.localStorage.setItem(
            "origintracer.selected_worker_instance_id",
            instanceId,
          );
        } else {
          window.localStorage.removeItem("origintracer.selected_worker_instance_id");
        }
        // Captures belong to one process instance; do not show a prior worker's
        // prompts or tool values while the next worker is being selected.
        setLlmContent([]);
        setToolContent([]);
      }

      if (instanceId) {
        const [graphResponse, eventResponse, statusResponse, diffResponse,
          causalResponse, historyResponse] = await Promise.all([
          api.graph(instanceId),
          api.events(instanceId, 100),
          api.status(instanceId),
          api.diff(instanceId).catch(() => null),
          api.causal(instanceId).catch(() => null),
          api.causalHistory(instanceId).catch(() => null),
        ]);

        // A worker can be selected while these requests are in flight. Ignore
        // the old response so its graph cannot replace the newly selected one.
        if (
          window.localStorage.getItem("origintracer.selected_worker_instance_id") !== instanceId
        ) {
          return;
        }

        const graph = graphResponse?.data ?? {};

        setNodes(normalizeWorkerNodes(graph.nodes));
        setEdges(graph.edges ?? []);
        setEvents(eventResponse?.data ?? []);
        setStatus(statusResponse ?? { mode: "snapshot" });
        setDiff(normalizeWorkerDiff(diffResponse?.data));
        setCausal(causalResponse?.data ?? []);
        setCausalHistory(historyResponse?.data ?? []);
      } else {
        setNodes([]);
        setEdges([]);
        setEvents([]);
        setTrace((current) => current?.source === "history" ? current : null);
        setStatus({ mode: "no-snapshot" });
        setDiff(null);
        setCausal([]);
        setCausalHistory([]);
      }

      setBackendError(null);
    } catch (error) {
      console.warn("Backend unavailable:", error?.message ?? error);
      setBackendError(error?.message ?? "Backend unavailable");
    } finally {
      refreshInFlight.current = false;
    }
  }, [selectedWorkerInstanceId]);

  useEffect(() => {
    refresh();
  }, [refresh]);

  useEffect(() => {
    const timer = setInterval(refresh, 5000);
    return () => clearInterval(timer);
  }, [refresh]);

  useEffect(() => {
    if (!selectedWorkerInstanceId || (view !== "llm_content" && view !== "tool_content")) {
      return undefined;
    }

    let cancelled = false;
    const loadCapturedContent = async () => {
      try {
        const kind = view === "llm_content" ? "llm" : "tool";
        const response = await api.capturedContent(
          selectedWorkerInstanceId,
          kind,
          100,
        );
        const captured = response?.data ?? [];
        if (!cancelled) {
          if (view === "llm_content") {
            setLlmContent(captured);
          } else {
            setToolContent(captured);
          }
          setBackendError(null);
        }
      } catch (error) {
        if (!cancelled) {
          setBackendError(error?.message ?? "Could not load stored content");
        }
      }
    };

    loadCapturedContent();
    const timer = setInterval(loadCapturedContent, 5000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [selectedWorkerInstanceId, view]);

  const runQuery = async (query) => {
    const normalized = query.trim().replace(/^\\/, "").replace(/\s+/g, " ");
    const upper = normalized.toUpperCase();
    const traceCommand = normalized.match(/^(?:TRACE\s+)([A-Za-z0-9._:/-]{1,128})$/i);
    const stitchCommand = normalized.match(/^(?:STITCH\s+)([A-Za-z0-9._:/-]{1,128})$/i);

    if (stitchCommand) {
      const id = stitchCommand[1];
      if (!id) {
        setView("stitch");
        return;
      }
      setLoading(true);
      try {
        const response = await api.stitchTrace(id);
        const stages = response?.stages ?? [];
        setTrace({
          id,
          stages,
          workersFound: response?.workers_found ?? [],
          workersSearched: response?.workers_searched ?? [],
          errors: response?.errors ?? [],
          source: "stitched",
        });
        setView("stitch");
        setBackendError(stages.length ? null : "No stored events found for this trace");
      } catch (error) {
        setBackendError(error?.message ?? "Trace lookup failed");
        setView("stitch");
      } finally {
        setLoading(false);
      }
      return;
    }

    if (traceCommand) {
      if (!selectedWorkerInstanceId) {
        setBackendError("Select a stored worker snapshot first");
        return;
      }
      setLoading(true);
      try {
        const response = await api.trace(traceCommand[1], selectedWorkerInstanceId);
        setTrace({
          id: traceCommand[1],
          stages: response?.data ?? [],
          source: "history",
        });
        setView("trace");
        setBackendError(null);
      } catch (error) {
        setBackendError(error?.message ?? "Stored trace lookup failed");
      } finally {
        setLoading(false);
      }
      return;
    }

    if (!selectedWorkerInstanceId) {
      setBackendError("Select a stored worker snapshot first");
      return;
    }

    setLoading(true);
    try {
      if (upper === "SHOW NODES" || upper === "NODES") {
        const response = await api.nodes(selectedWorkerInstanceId);
        setNodes(normalizeWorkerNodes(response?.data?.data ?? []));
        setView("nodes");
      } else if (upper === "SHOW EDGES" || upper === "EDGES") {
        const response = await api.edges(selectedWorkerInstanceId);
        setEdges(response?.data?.data ?? []);
        setView("edges");
      } else if (upper === "SHOW GRAPH" || upper === "GRAPH") {
        const response = await api.graph(selectedWorkerInstanceId);
        setNodes(normalizeWorkerNodes(response?.data?.nodes));
        setEdges(response?.data?.edges ?? []);
        setView("graph");
      } else if (upper === "SHOW EVENTS" || upper === "EVENTS") {
        const response = await api.events(selectedWorkerInstanceId, 100);
        setEvents(response?.data ?? []);
        setView("events");
      } else if (upper === "SHOW LLM_CONTENT" || upper === "SHOW LLM CONTENT") {
        const response = await api.capturedContent(selectedWorkerInstanceId, "llm");
        setLlmContent(response?.data ?? []);
        setView("llm_content");
      } else if (upper === "SHOW TOOL_CONTENT" || upper === "SHOW TOOL CONTENT") {
        const response = await api.capturedContent(selectedWorkerInstanceId, "tool");
        setToolContent(response?.data ?? []);
        setView("tool_content");
      } else if (upper === "SHOW STATUS" || upper === "STATUS") {
        setStatus(await api.status(selectedWorkerInstanceId));
        setView("status");
      } else if (upper === "CAUSAL" || upper === "SHOW CAUSAL") {
        const response = await api.causal(selectedWorkerInstanceId);
        setCausal(response?.data ?? []);
        setView("causal");
      } else if (upper.startsWith("DIFF")) {
        const since = normalized.match(/^DIFF(?:\s+SINCE\s+(.+))?$/i)?.[1] ?? "deployment";
        const response = await api.diff(selectedWorkerInstanceId, since);
        setDiff(normalizeWorkerDiff(response?.data));
        setView("diff");
      } else {
        throw new Error("This view supports stored API queries only; use the REPL for arbitrary live DSL queries.");
      }
      setBackendError(null);
    } catch (error) {
      setBackendError(error?.message ?? "Stored query failed");
    } finally {
      setLoading(false);
    }
  };

  // Captured content is polled only while its view is selected.
  const loadCapturedContent = (metric) => {
    setView(metric);
  };

  const selectedWorker = snapshots.find(
    (snapshot) => snapshot.worker_instance_id === selectedWorkerInstanceId,
  );
  const workerPageCount = Math.ceil(snapshots.length / WORKERS_PER_PAGE);
  const visibleWorkers = snapshots.slice(
    workerPage * WORKERS_PER_PAGE,
    (workerPage + 1) * WORKERS_PER_PAGE,
  );
  useEffect(() => {
    setWorkerPage((page) => Math.min(page, Math.max(0, workerPageCount - 1)));
  }, [workerPageCount]);
  const noReportingWorkers = snapshotInventoryLoaded && snapshots.length === 0;
  const badge = {
    nodes: `${nodes.length} nodes`,
    edges: `${edges.length} edges`,
    trace: trace ? `${trace.stages.length} stages` : "—",
    stitch: trace?.source === "stitched"
      ? `${trace.workersFound.length} workers · ${trace.stages.length} stages`
      : "—",
    events: `${events.length} events`,
    diff: diff
      ? `${(diff.added_nodes?.length || 0) + (diff.added_edges?.length || 0)} changes`
      : "—",
    graph: `${nodes.length} nodes · ${edges.length} edges`,
    llm_content: `${llmContent.length} entries`,
    tool_content: `${toolContent.length} entries`,
    causal: `${causal.length} patterns`,
    status: selectedWorker ? "reporting" : noReportingWorkers ? "no reporting workers" : "…",
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
              onClick={() => {
                if (
                  (item === "stitch" && trace?.source !== "stitched")
                  || (item === "trace" && trace?.source === "stitched")
                ) {
                  setTrace(null);
                }
                if (item === "llm_content" || item === "tool_content") {
                  loadCapturedContent(item);
                } else {
                  setView(item);
                }
              }}
            >
              <span className="nav-dot" />
              {item.replace("_", " ")}
            </div>
          ))}
        </nav>

        {snapshots.length > 0 && (
          <div className="snapshots">
            <div className="snapshot-title">CURRENTLY REPORTING ({snapshots.length})</div>
            {visibleWorkers.map((snapshot) => {
              const active = snapshot.worker_instance_id === selectedWorkerInstanceId;
              return (
                <button
                  type="button"
                  key={snapshot.worker_instance_id}
                  className={`worker-option ${active ? "active" : ""} is-reporting`}
                  onClick={() => {
                    setSelectedWorkerInstanceId(snapshot.worker_instance_id);
                    window.localStorage.setItem(
                      "origintracer.selected_worker_instance_id",
                      snapshot.worker_instance_id,
                    );
                  }}
                  title={`Currently reporting process instance ${snapshot.worker_instance_id}`}
                >
                  <span className="socket-dot" />
                  pid {snapshot.worker_pid ?? "unknown"} · {snapshot.nodes ?? 0} nodes · {formatSnapshotTime(snapshot.received_at)}
                </button>
              );
            })}
            {workerPageCount > 1 && (
              <div className="worker-pagination" aria-label="Worker pages">
                <button
                  type="button"
                  onClick={() => setWorkerPage((page) => Math.max(0, page - 1))}
                  disabled={workerPage === 0}
                  aria-label="Previous workers"
                >
                  ‹ Prev
                </button>
                <span>{workerPage + 1} / {workerPageCount}</span>
                <button
                  type="button"
                  onClick={() => setWorkerPage((page) => Math.min(workerPageCount - 1, page + 1))}
                  disabled={workerPage >= workerPageCount - 1}
                  aria-label="Next workers"
                >
                  Next ›
                </button>
              </div>
            )}
          </div>
        )}
      </aside>

      <div className="main">
        <div className="toolbar">
          <span className="toolbar-title">{view.replace("_", " ")}</span>
          {selectedWorker && (
            <span className={`worker-badge ${selectedWorker.online ? "is-reporting" : "is-stale"}`}>
              {selectedWorker.online ? "reporting" : "stale snapshot"} · pid {selectedWorker.worker_pid ?? "unknown"}
            </span>
          )}
          <span className="badge">{badge[view]}</span>
          {backendError && (
            <span className="backend-error">{backendError}</span>
          )}
        </div>
        <QueryBar onRun={runQuery} loading={loading} />
        <div className="content">
          <ErrorBoundary key={view}>
            {noReportingWorkers ? (
              <div className="no-worker-state">
                <div className="no-worker-title">
                  No currently reporting workers
                </div>
                <div>
                  Start an instrumented process configured to upload snapshots
                  to this FastAPI backend and wait for its heartbeat. The REPL
                  remains available for live inspection.
                </div>
              </div>
            ) : (
              <>
                {view === "nodes" && <NodeTable nodes={nodes} />}
                {view === "edges" && <EdgeTable edges={edges} />}
                {view === "trace" && <TraceTimeline trace={trace} />}
                {view === "stitch" && (
                  <TraceTimeline trace={trace} mode="stitch" />
                )}
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
                {view === "history" && (
                  <CausalHistory history={causalHistory} currentMatches={causal} />
                )}
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
          snapshotInventoryLoaded={snapshotInventoryLoaded}
        />
      </div>
    </div>
  );
}
