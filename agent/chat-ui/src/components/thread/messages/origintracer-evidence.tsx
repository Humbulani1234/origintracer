import type { ReactNode } from "react";
import { useWorker } from "@/providers/Worker";

type RecordValue = Record<string, unknown>;

type Observation = RecordValue & {
  tool?: string;
  evidence_id?: string;
  structured_data?: unknown;
};

function isRecord(value: unknown): value is RecordValue {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function records(value: unknown): RecordValue[] {
  return Array.isArray(value) ? value.filter(isRecord) : [];
}

function nestedRecords(value: unknown): RecordValue[] {
  let current = value;
  for (let depth = 0; depth < 4; depth += 1) {
    if (Array.isArray(current)) return records(current);
    if (!isRecord(current)) return [];
    current = current.data ?? current.matches ?? current.result;
  }
  return records(current);
}

function text(value: unknown, fallback = "—"): string {
  if (value === null || value === undefined || value === "") return fallback;
  if (
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return String(value);
  }
  return fallback;
}

function number(value: unknown): number | null {
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function payloadOf(value: unknown): unknown {
  if (!isRecord(value)) return value;
  if ("data" in value) return value.data;
  if ("result" in value) return value.result;
  if ("stages" in value) return value.stages;
  if ("events" in value) return value.events;
  return value;
}

function CompactTable({
  rows,
  columns,
  label,
}: {
  rows: RecordValue[];
  columns: {
    key: string;
    label: string;
    render?: (row: RecordValue) => ReactNode;
  }[];
  label: string;
}) {
  if (!rows.length) {
    return (
      <p className="text-muted-foreground text-sm">No {label} returned.</p>
    );
  }
  const shown = rows.slice(0, 6);
  const table = (items: RecordValue[]) => (
    <div className="w-full min-w-0 max-w-full overflow-x-auto rounded-lg border">
      <table className="w-full min-w-[34rem] text-left text-sm">
        <thead className="bg-muted/60 text-muted-foreground">
          <tr>
            {columns.map((column) => (
              <th
                key={column.key}
                className="px-3 py-2 font-medium"
              >
                {column.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {items.map((row, index) => (
            <tr
              key={`${text(row.id, text(row.name, String(index)))}-${index}`}
              className="border-t align-top"
            >
              {columns.map((column) => (
                <td
                  key={column.key}
                  className="max-w-80 px-3 py-2 break-words"
                >
                  {column.render ? column.render(row) : text(row[column.key])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
  return (
    <div className="space-y-2">
      {table(shown)}
      {rows.length > shown.length && (
        <details>
          <summary className="text-muted-foreground cursor-pointer text-xs">
            Show all {rows.length} {label}
          </summary>
          <div className="mt-2">{table(rows)}</div>
        </details>
      )}
    </div>
  );
}

function MetricCards({
  items,
}: {
  items: { label: string; value: unknown }[];
}) {
  return (
    <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
      {items.map(({ label, value }) => (
        <div
          key={label}
          className="rounded-lg border px-3 py-2"
        >
          <p className="text-muted-foreground text-xs">{label}</p>
          <p className="mt-1 text-sm font-medium break-words">{text(value)}</p>
        </div>
      ))}
    </div>
  );
}

function WorkerStatus({ value }: { value: unknown }) {
  const data = payloadOf(value);
  if (!isRecord(data)) return null;
  const probes = Array.isArray(data.probes_active)
    ? data.probes_active.map((item) => text(item)).filter(Boolean)
    : typeof data.probes_active === "string"
      ? [data.probes_active]
      : [];
  const items = [
    {
      label: "Worker",
      value: data.pid ? `PID ${text(data.pid)}` : data.worker_instance_id,
    },
    { label: "Uptime", value: data.uptime },
    { label: "Active requests", value: data.active_requests },
    { label: "Graph nodes", value: data.graph_nodes },
    { label: "Graph edges", value: data.graph_edges },
  ].filter((item) => item.value !== undefined);
  return (
    <div className="space-y-3">
      <MetricCards items={items} />
      {probes.length > 0 && (
        <section>
          <p className="text-muted-foreground mb-1.5 text-xs font-medium">
            Active probes
          </p>
          <div className="flex flex-wrap gap-1.5">
            {probes.map((probe) => (
              <span
                key={probe}
                className="bg-muted rounded-full px-2.5 py-1 text-xs font-medium"
              >
                {probe}
              </span>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}

function LiveWorkersOverview({ value }: { value: unknown }) {
  const { workerPid, setWorkerPid } = useWorker();
  const data = payloadOf(value);
  if (!isRecord(data)) return null;

  const workers = records(data.workers);
  const totalWorkers = number(data.total_live_workers) ?? workers.length;
  if (!workers.length) {
    return (
      <p className="text-muted-foreground text-sm">
        No live workers were returned in this overview.
      </p>
    );
  }

  return (
    <div className="space-y-3">
      <p className="text-muted-foreground text-sm">
        {totalWorkers} live worker{totalWorkers === 1 ? "" : "s"} found. Focus a worker
        to use it for your next question.
      </p>
      <div className="grid min-w-0 gap-3 sm:grid-cols-2">
        {workers.map((worker, index) => {
          const pid = text(worker.pid, "");
          if (!pid) return null;

          const statusValue = worker.status;
          const status =
            isRecord(statusValue) && isRecord(statusValue.data)
              ? statusValue.data
              : isRecord(statusValue)
                ? statusValue
                : {};
          const graph = isRecord(worker.graph) ? worker.graph : {};
          const probes = Array.isArray(status.probes_active)
            ? status.probes_active.map((item) => text(item)).filter(Boolean)
            : typeof status.probes_active === "string"
              ? [status.probes_active]
            : [];
          const activeRules = Array.isArray(status.rules_active)
            ? status.rules_active.map((item) => text(item)).filter(Boolean)
            : typeof status.rules_active === "string"
              ? [status.rules_active]
              : [];
          const normalizedMatches = nestedRecords(worker.matched_rules);
          const matches = normalizedMatches.length > 0
            ? normalizedMatches
            : nestedRecords(worker.causal_rule_matches);
          const nodes = records(graph.nodes);
          const edges = records(graph.edges);
          const registeredRules = payloadOf(worker.registered_causal_rules);
          const sectionErrors = Array.isArray(worker.section_errors)
            ? worker.section_errors.map((item) => text(item))
            : [];
          const focused = workerPid === pid;
          const isOnline = status.online === true;
          const heartbeatAge = number(status.heartbeat_age_s);

          return (
            <article
              key={pid + "-" + text(worker.worker_instance_id, String(index))}
              className="min-w-0 rounded-xl border p-3"
            >
              <div className="flex min-w-0 items-start justify-between gap-3">
                <div className="min-w-0">
                  <h4 className="truncate text-sm font-semibold">Worker PID {pid}</h4>
                  <p className="text-muted-foreground mt-1 text-xs">
                    Uptime {text(status.uptime)}
                  </p>
                </div>
                <div className="flex shrink-0 flex-col items-end gap-2">
                  <span className={"rounded-full border px-2 py-1 text-[11px] " + (isOnline
                    ? "border-emerald-500/30 text-emerald-700 dark:text-emerald-300"
                    : "border-amber-500/30 text-amber-700 dark:text-amber-300")}
                  >
                    {isOnline ? "Reporting" : "Stale"}
                    {heartbeatAge !== null ? ` · ${heartbeatAge}s ago` : ""}
                  </span>
                  <button
                    type="button"
                    aria-pressed={focused}
                    onClick={() => setWorkerPid(pid)}
                    className={"rounded-full border px-3 py-1.5 text-xs font-medium transition-colors " + (focused
                      ? "border-primary bg-primary/10 text-foreground"
                      : "border-border hover:bg-muted")}
                  >
                    {focused ? "Focused" : "Focus this worker"}
                  </button>
                </div>
              </div>

              <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-3">
                <div className="bg-muted/50 rounded-lg px-2 py-1.5">
                  <p className="text-muted-foreground text-[11px]">Active requests</p>
                  <p className="text-sm font-medium">{text(status.active_requests)}</p>
                </div>
                <div className="bg-muted/50 rounded-lg px-2 py-1.5">
                  <p className="text-muted-foreground text-[11px]">Graph nodes</p>
                  <p className="text-sm font-medium">{text(graph.node_count)}</p>
                </div>
                <div className="bg-muted/50 rounded-lg px-2 py-1.5">
                  <p className="text-muted-foreground text-[11px]">Graph edges</p>
                  <p className="text-sm font-medium">{text(graph.edge_count)}</p>
                </div>
              </div>

              {(graph.nodes_truncated === true || graph.edges_truncated === true) && (
                <p className="text-muted-foreground mt-2 text-xs">
                  Preview shows {text(graph.nodes_preview_count, "0")} of {text(graph.node_count, "0")} nodes and {text(graph.edges_preview_count, "0")} of {text(graph.edge_count, "0")} edges, ranked by call count.
                </p>
              )}

              {(nodes.length > 0 || edges.length > 0) && (
                <details className="mt-3 rounded-lg border px-3 py-2">
                  <summary className="cursor-pointer text-xs font-medium">
                    Show graph preview ({nodes.length} nodes, {edges.length} edges)
                  </summary>
                  <div className="mt-3 space-y-3">
                    {nodes.length > 0 && (
                      <section>
                        <p className="text-muted-foreground mb-1 text-xs font-medium">Busiest nodes</p>
                        <ul className="space-y-1 text-xs">
                          {nodes.map((node, nodeIndex) => (
                            <li key={text(node.id, String(nodeIndex))} className="flex flex-wrap justify-between gap-x-3">
                              <span className="break-all">{text(node.id)}</span>
                              <span className="text-muted-foreground shrink-0">
                                {text(node.call_count)} calls{node.avg_ms != null ? ` · ${text(node.avg_ms)} ms avg` : ""}
                              </span>
                            </li>
                          ))}
                        </ul>
                      </section>
                    )}
                    {edges.length > 0 && (
                      <section>
                        <p className="text-muted-foreground mb-1 text-xs font-medium">Busiest edges</p>
                        <ul className="space-y-1 text-xs">
                          {edges.map((edge, edgeIndex) => (
                            <li key={`${text(edge.source)}-${text(edge.target)}-${edgeIndex}`} className="flex flex-wrap justify-between gap-x-3">
                              <span className="break-all">{text(edge.source)} → {text(edge.target)}</span>
                              <span className="text-muted-foreground shrink-0">{text(edge.call_count)} calls</span>
                            </li>
                          ))}
                        </ul>
                      </section>
                    )}
                  </div>
                </details>
              )}

              {probes.length > 0 && (
                <div className="mt-3">
                  <p className="text-muted-foreground mb-1.5 text-xs">Active probes</p>
                  <div className="flex flex-wrap gap-1.5">
                    {probes.map((probe) => (
                      <span
                        key={probe}
                        className="bg-muted rounded-full px-2 py-1 text-xs"
                      >
                        {probe}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {activeRules.length > 0 && (
                <div className="mt-3">
                  <p className="text-muted-foreground mb-1.5 text-xs">Active rules</p>
                  <div className="flex flex-wrap gap-1.5">
                    {activeRules.map((rule) => (
                      <span key={rule} className="bg-muted rounded-full px-2 py-1 text-xs">{rule}</span>
                    ))}
                  </div>
                </div>
              )}

              <div className="mt-3">
                <p className="text-muted-foreground mb-1.5 text-xs">
                  Matched causal rules ({matches.length})
                </p>
                {matches.length > 0 ? (
                  <div className="space-y-2">
                    {matches.map((match, matchIndex) => (
                      <article key={text(match.rule, "rule") + "-" + matchIndex} className="rounded-lg border px-3 py-2">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <span className="text-xs font-medium">{text(match.rule)}</span>
                          {match.confidence !== undefined && (
                            <span className="bg-muted rounded-full px-2 py-0.5 text-[11px]">
                              Confidence {text(match.confidence)}
                            </span>
                          )}
                        </div>
                        {typeof match.explanation === "string" && (
                          <p className="text-muted-foreground mt-1 text-xs">{match.explanation}</p>
                        )}
                        {records(match.hotspot_queries).length > 0 && (
                          <ul className="text-muted-foreground mt-1 space-y-1 text-xs">
                            {records(match.hotspot_queries).map((hotspot, hotspotIndex) => (
                              <li key={`${text(hotspot.node)}-${hotspotIndex}`} className="break-words">
                                {text(hotspot.node)} · {text(hotspot.call_count)} calls
                                {hotspot.pct != null ? ` · ${text(hotspot.pct)}%` : ""}
                                {hotspot.avg_ms != null ? ` · ${text(hotspot.avg_ms)} ms avg` : ""}
                              </li>
                            ))}
                          </ul>
                        )}
                      </article>
                    ))}
                  </div>
                ) : (
                  <p className="text-muted-foreground text-xs">
                    {sectionErrors.includes("causal_rule_matches")
                      ? "Rule matches unavailable."
                      : "This overview check returned no matched patterns."}
                  </p>
                )}
              </div>

              {activeRules.length === 0 && worker.registered_causal_rules !== undefined && (
                <details className="mt-3">
                  <summary className="text-muted-foreground cursor-pointer text-xs">
                    Rule inventory
                  </summary>
                  <div className="mt-2 text-xs break-words">
                    {Array.isArray(registeredRules)
                      ? registeredRules.map((rule) => isRecord(rule)
                        ? text(rule.name, text(rule.rule, text(rule.id)))
                        : text(rule)).join(", ") || "None returned"
                      : text(registeredRules, "None returned")}
                  </div>
                </details>
              )}
            </article>
          );
        })}
      </div>
      {data.workers_truncated === true && (
        <p className="text-muted-foreground text-xs">
          The overview was truncated; not all live workers are shown.
        </p>
      )}
    </div>
  );
}

function rawOperation(value: unknown): string {
  const raw = text(value, "Unknown operation");
  const separator = raw.indexOf("::");
  return separator >= 0 ? raw.slice(separator + 2) : raw;
}

function OperationName({ value }: { value: unknown }) {
  const raw = text(value, "Unknown operation");
  const name = rawOperation(value);
  const sql = /^\s*(SELECT|INSERT|UPDATE|DELETE|WITH)\b/i.exec(name);
  const label = sql ? `SQL query · ${sql[1].toUpperCase()}` : name;
  return (
    <div className="min-w-0">
      <p className="line-clamp-2 break-all text-sm font-medium" title={raw}>
        {label}
      </p>
      {sql && (
        <details className="mt-1">
          <summary className="text-muted-foreground cursor-pointer text-xs">
            View SQL
          </summary>
          <code className="bg-muted mt-1 block max-h-32 overflow-auto rounded p-2 text-xs whitespace-pre-wrap break-all">
            {name}
          </code>
        </details>
      )}
    </div>
  );
}

function nodeRows(value: unknown): RecordValue[] {
  const data = payloadOf(value);
  const rows = Array.isArray(data)
    ? records(data)
    : isRecord(data)
      ? records(data.nodes)
      : [];
  return [...rows].sort(
    (a, b) => (number(b.call_count) ?? -1) - (number(a.call_count) ?? -1),
  );
}

function NodeCards({ rows }: { rows: RecordValue[] }) {
  return (
    <div className="space-y-2">
      {rows.map((row, index) => (
        <article
          key={`${text(row.id, text(row.node, String(index)))}-${index}`}
          className="flex min-w-0 flex-wrap items-start justify-between gap-3 rounded-lg border px-3 py-2.5"
        >
          <div className="min-w-0 flex-1">
            <OperationName value={row.id ?? row.node} />
            {typeof row.service === "string" && (
              <p className="text-muted-foreground mt-1 text-xs">{row.service}</p>
            )}
          </div>
          <div className="flex shrink-0 flex-wrap items-center gap-3 text-xs tabular-nums">
            {row.call_count !== undefined && (
              <span><strong>{text(row.call_count)}</strong> calls</span>
            )}
            {row.avg_ms != null && (
              <span className="text-muted-foreground">{text(row.avg_ms)} ms avg</span>
            )}
          </div>
        </article>
      ))}
    </div>
  );
}

function Nodes({ value }: { value: unknown }) {
  const rows = nodeRows(value);
  const shown = rows.slice(0, 6);
  return (
    <div className="space-y-2">
      <p className="text-muted-foreground text-xs">
        {rows.length} observed operation{rows.length === 1 ? "" : "s"}, ranked by calls
      </p>
      <NodeCards rows={shown} />
      {rows.length > shown.length && (
        <details>
          <summary className="text-muted-foreground cursor-pointer text-xs">
            Show all {rows.length} operations
          </summary>
          <div className="mt-2"><NodeCards rows={rows.slice(shown.length)} /></div>
        </details>
      )}
    </div>
  );
}

function edgeRows(value: unknown): RecordValue[] {
  const data = payloadOf(value);
  const rows = Array.isArray(data)
    ? records(data)
    : isRecord(data)
      ? records(data.edges)
      : [];
  return [...rows].sort(
    (a, b) =>
      (number(b.call_count ?? b.weight) ?? -1) -
      (number(a.call_count ?? a.weight) ?? -1),
  );
}

function RelationshipCards({ rows }: { rows: RecordValue[] }) {
  return (
    <div className="space-y-2">
      {rows.map((row, index) => {
        const source = row.source ?? row.from;
        const target = row.target ?? row.to;
        return (
          <article
            key={`${text(source)}-${text(target)}-${index}`}
            className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-2 rounded-lg border px-3 py-2.5"
          >
            <OperationName value={source} />
            <span className="text-muted-foreground text-sm" aria-label="to">→</span>
            <div className="min-w-0">
              <OperationName value={target} />
              <div className="text-muted-foreground mt-1 flex flex-wrap gap-x-2 text-xs">
                {row.type !== undefined && <span>{text(row.type)}</span>}
                {(row.call_count ?? row.weight) !== undefined && (
                  <span>{text(row.call_count ?? row.weight)} calls</span>
                )}
              </div>
            </div>
          </article>
        );
      })}
    </div>
  );
}

function Relationships({ value }: { value: unknown }) {
  const rows = edgeRows(value);
  const shown = rows.slice(0, 6);
  return (
    <div className="space-y-2">
      <p className="text-muted-foreground text-xs">
        {rows.length} observed relationship{rows.length === 1 ? "" : "s"}. Arrow direction follows the returned graph edge.
      </p>
      <RelationshipCards rows={shown} />
      {rows.length > shown.length && (
        <details>
          <summary className="text-muted-foreground cursor-pointer text-xs">
            Show all {rows.length} relationships
          </summary>
          <div className="mt-2"><RelationshipCards rows={rows.slice(shown.length)} /></div>
        </details>
      )}
    </div>
  );
}

function Edges({ value }: { value: unknown }) {
  return <Relationships value={value} />;
}

function Graph({ value }: { value: unknown }) {
  const data = payloadOf(value);
  if (!isRecord(data)) return null;
  const nodes = nodeRows(data.nodes);
  const edges = edgeRows(data.edges);
  if (!nodes.length && !edges.length) return null;
  const serviceCounts = new Map<string, number>();
  for (const node of nodes) {
    const service = text(node.service, "Other");
    serviceCounts.set(service, (serviceCounts.get(service) ?? 0) + 1);
  }
  return (
    <div className="space-y-3">
      <MetricCards
        items={[
          { label: "Nodes", value: nodes.length },
          { label: "Edges", value: edges.length },
          { label: "Services", value: serviceCounts.size },
        ]}
      />
      <div className="flex flex-wrap gap-2">
        {[...serviceCounts].map(([service, count]) => (
          <span key={service} className="bg-muted rounded-full px-2.5 py-1 text-xs">
            {service} · {count} nodes
          </span>
        ))}
      </div>
      {nodes.length > 0 && (
        <section className="space-y-2">
          <h4 className="text-sm font-medium">Busiest operations</h4>
          <NodeCards rows={nodes.slice(0, 3)} />
          {nodes.length > 3 && (
            <details>
              <summary className="text-muted-foreground cursor-pointer text-xs">
                Show all {nodes.length} operations
              </summary>
              <div className="mt-2"><NodeCards rows={nodes.slice(3)} /></div>
            </details>
          )}
        </section>
      )}
      {edges.length > 0 && (
        <section className="space-y-2">
          <h4 className="text-sm font-medium">Observed call relationships</h4>
          <p className="text-muted-foreground text-xs">
            Recorded links only; arrows do not establish execution order or cause.
          </p>
          <Relationships value={edges} />
        </section>
      )}
    </div>
  );
}

function Trace({ value }: { value: unknown }) {
  const outer = isRecord(value) ? value : {};
  const stages = records(payloadOf(value));
  if (!stages.length) return null;
  const durations = stages.map((stage) => number(stage.duration_ms));
  const max = Math.max(
    0,
    ...durations.filter((item): item is number => item !== null),
  );
  return (
    <div className="space-y-2">
      {typeof outer.trace_id === "string" && (
        <p className="text-muted-foreground text-xs">
          Trace <span className="font-mono">{outer.trace_id}</span>
        </p>
      )}
      {stages.map((stage, index) => {
        const duration = number(stage.duration_ms);
        const width =
          duration !== null && max > 0
            ? Math.max(2, (duration / max) * 100)
            : 0;
        return (
          <div
            key={`${text(stage.probe, String(index))}-${index}`}
            className="grid grid-cols-[minmax(0,1fr)_5rem] items-center gap-3 rounded-md border px-3 py-2"
          >
            <div className="min-w-0">
              <p className="truncate text-sm font-medium">
                {text(stage.name, text(stage.probe, `Stage ${index + 1}`))}
              </p>
              {typeof stage.probe === "string" && (
                <p className="text-muted-foreground truncate text-xs">
                  {stage.probe}
                </p>
              )}
              {duration !== null && max > 0 && (
                <div className="bg-muted mt-2 h-1.5 overflow-hidden rounded-full">
                  <div
                    className="bg-primary h-full rounded-full"
                    style={{ width: `${width}%` }}
                  />
                </div>
              )}
            </div>
            <span className="text-right text-sm tabular-nums">
              {duration === null ? "—" : `${duration.toFixed(2)} ms`}
            </span>
          </div>
        );
      })}
      {durations.every((item) => item === null) && (
        <p className="text-muted-foreground text-xs">
          The trace response did not include stage duration values.
        </p>
      )}
    </div>
  );
}

function EventList({ value }: { value: unknown }) {
  const outer = isRecord(value) ? value : {};
  const data = payloadOf(value);
  const rows = Array.isArray(data)
    ? records(data)
    : isRecord(data)
      ? records(data.events)
      : [];
  if (!rows.length) return null;
  const sorted = [...rows].sort(
    (a, b) => (number(a.ts) ?? 0) - (number(b.ts) ?? 0),
  );
  return (
    <div className="space-y-2">
      {typeof outer.trace_id === "string" && (
        <p className="text-muted-foreground text-xs">
          Trace <span className="font-mono">{outer.trace_id}</span>
        </p>
      )}
      <CompactTable
        label="events"
        rows={sorted}
        columns={[
          { key: "name", label: "Event" },
          { key: "probe", label: "Probe" },
          { key: "service", label: "Service" },
          {
            key: "trace_id",
            label: "Trace ID",
            render: (row) => text(row.trace_id, "—"),
          },
        ]}
      />
    </div>
  );
}

function formatMilliseconds(value: unknown): string {
  const parsed = number(value);
  if (parsed === null) return "—";
  const precision = Math.abs(parsed) < 0.01 && parsed !== 0 ? 6 : 3;
  return `${parsed.toFixed(precision).replace(/\.?0+$/, "")} ms`;
}

function LatencyDiagnostics({ value }: { value: unknown }) {
  const rootValue = payloadOf(value);
  const root = isRecord(rootValue) ? rootValue : {};
  const sections = [
    { key: "database_latency", label: "Database queries" },
    { key: "request_latency", label: "Request paths" },
    { key: "asyncio_latency", label: "Async operations" },
  ].map((section) => ({
    ...section,
    rows: isRecord(root[section.key])
      ? records((root[section.key] as RecordValue).data)
      : [],
  }));
  const ruleEnvelope = isRecord(root.causal_rule_matches)
    ? root.causal_rule_matches
    : {};
  const rules = records(ruleEnvelope.data);
  const hasMeasurements = sections.some((section) => section.rows.length > 0);

  return (
    <div className="space-y-4">
      <p className="text-muted-foreground text-xs">
        Averages are per call. Total durations are aggregates across calls.
      </p>
      {sections.map(({ key, label, rows }) =>
        rows.length > 0 ? (
          <section key={key} className="space-y-2">
            <h4 className="text-sm font-medium">{label}</h4>
            <CompactTable
              label={label.toLowerCase()}
              rows={rows}
              columns={[
                {
                  key: "node",
                  label: "Operation",
                  render: (row) => <OperationName value={row.node ?? row.name} />,
                },
                { key: "call_count", label: "Calls" },
                {
                  key: "avg_duration_ms",
                  label: "Avg per call",
                  render: (row) => formatMilliseconds(row.avg_duration_ms),
                },
                {
                  key: "total_duration_ms",
                  label: "Total",
                  render: (row) => formatMilliseconds(row.total_duration_ms),
                },
              ]}
            />
          </section>
        ) : null,
      )}
      {!hasMeasurements && (
        <p className="text-muted-foreground text-sm">
          No latency measurements were returned.
        </p>
      )}
      {rules.length > 0 && (
        <section className="space-y-2">
          <h4 className="text-sm font-medium">Detected rule patterns</h4>
          {rules.map((rule, index) => (
            <article
              key={`${text(rule.rule, String(index))}-${index}`}
              className="rounded-lg border px-3 py-2.5"
            >
              <div className="flex flex-wrap items-center justify-between gap-2">
                <p className="text-sm font-medium">{text(rule.rule, "Rule match")}</p>
                {rule.confidence !== undefined && (
                  <span className="bg-muted rounded-full px-2 py-0.5 text-xs">
                    Confidence {text(rule.confidence)}
                  </span>
                )}
              </div>
              {typeof rule.explanation === "string" && (
                <p className="text-muted-foreground mt-1 text-sm break-words">
                  {rule.explanation}
                </p>
              )}
            </article>
          ))}
          <p className="text-muted-foreground text-xs">
            Rule matches identify patterns; they do not prove root cause.
          </p>
        </section>
      )}
    </div>
  );
}

function CausalMatches({ value }: { value: unknown }) {
  const rootValue = payloadOf(value);
  const root = isRecord(rootValue) ? rootValue : {};
  const matchEnvelope = isRecord(root.matches) ? root.matches : {};
  const matches = Array.isArray(matchEnvelope.data)
    ? records(matchEnvelope.data)
    : Array.isArray(matchEnvelope.result)
      ? records(matchEnvelope.result)
      : records(root.matches);
  const registeredEnvelope = isRecord(root.registered_rules)
    ? root.registered_rules
    : {};
  const registeredValue = Array.isArray(registeredEnvelope.data)
    ? registeredEnvelope.data
    : Array.isArray(registeredEnvelope.result)
      ? registeredEnvelope.result
      : root.registered_rules;
  const registered = Array.isArray(registeredValue)
    ? registeredValue
        .map((rule) =>
          typeof rule === "string"
            ? rule
            : isRecord(rule)
              ? text(rule.rule, text(rule.name, ""))
              : "",
        )
        .filter(Boolean)
    : [];
  const matchByRule = new Map<string, RecordValue>();
  for (const match of matches) {
    const name = text(match.rule, "");
    if (name) matchByRule.set(name, match);
  }
  const rules = [...new Set([...registered, ...matchByRule.keys()])];

  if (rules.length === 0) {
    return (
      <p className="text-muted-foreground text-sm">
        No registered causal rules were returned.
      </p>
    );
  }

  return (
    <div className="space-y-1.5">
      <p className="text-muted-foreground text-xs">
        {registered.length} registered · {matches.length} matched in this evaluation
      </p>
      {rules.map((rule) => {
        const match = matchByRule.get(rule);
        return (
          <article
            key={rule}
            className="rounded-lg border px-3 py-2"
          >
            <div className="flex flex-wrap items-center justify-between gap-2">
              <p className="text-sm font-medium break-words">{rule}</p>
              <span
                className={`rounded-full px-2 py-0.5 text-xs ${
                  match
                    ? "bg-primary/10 text-primary"
                    : "bg-muted text-muted-foreground"
                }`}
              >
                {match
                  ? `Matched${match.confidence !== undefined ? ` · ${text(match.confidence)}` : ""}`
                  : "No match"}
              </span>
            </div>
            {match && typeof match.explanation === "string" && (
              <details className="mt-1.5">
                <summary className="text-muted-foreground cursor-pointer text-xs">
                  Match details
                </summary>
                <p className="text-muted-foreground mt-1 text-sm">
                  {match.explanation}
                </p>
              </details>
            )}
          </article>
        );
      })}
      <p className="text-muted-foreground pt-1 text-xs">
        A rule match identifies a pattern; it does not prove root cause.
      </p>
    </div>
  );
}

function CausalHistory({ value }: { value: unknown }) {
  const rows = records(payloadOf(value));
  if (!rows.length)
    return (
      <p className="text-muted-foreground text-sm">
        No saved causal-rule history entries were returned.
      </p>
    );
  return (
    <CompactTable
      label="saved matches"
      rows={rows}
      columns={[
        {
          key: "rule",
          label: "Rule",
          render: (row) => text(row.rule, text(row.rule_id)),
        },
        { key: "confidence", label: "Confidence" },
        {
          key: "timestamp",
          label: "Recorded",
          render: (row) =>
            text(row.timestamp, text(row.observed_at, text(row.ts))),
        },
        { key: "worker_pid", label: "Worker" },
      ]}
    />
  );
}

const VISUAL_TOOLS = new Set([
  "get_live_workers_overview",
  "get_worker_status",
  "get_worker_nodes",
  "get_worker_edges",
  "get_worker_graph",
  "get_worker_trace",
  "get_worker_stitched_trace",
  "get_worker_events",
  "get_worker_database_events",
  "get_worker_latency_diagnostics",
  "evaluate_worker_rules",
  "get_worker_causal_history",
]);

export function hasOriginTracerEvidenceView(tool: unknown): boolean {
  return typeof tool === "string" && VISUAL_TOOLS.has(tool);
}

export function OriginTracerEvidence({
  observation,
}: {
  observation: Observation;
}) {
  const tool = typeof observation.tool === "string" ? observation.tool : "";
  const data = observation.structured_data;
  let label = "";
  let content: ReactNode = null;

  if (tool === "get_live_workers_overview") {
    label = "Live workers";
    content = <LiveWorkersOverview value={data} />;
  } else if (tool === "get_worker_status") {
    label = "Worker status";
    content = <WorkerStatus value={data} />;
  } else if (tool === "get_worker_nodes") {
    label = "Observed operations";
    content = <Nodes value={data} />;
  } else if (tool === "get_worker_edges") {
    label = "Observed edges";
    content = <Edges value={data} />;
  } else if (tool === "get_worker_graph") {
    label = "Worker graph";
    content = <Graph value={data} />;
  } else if (
    tool === "get_worker_trace" ||
    tool === "get_worker_stitched_trace"
  ) {
    label = "Trace stages";
    content = <Trace value={data} />;
  } else if (
    tool === "get_worker_events" ||
    tool === "get_worker_database_events"
  ) {
    label = "Recorded events";
    content = <EventList value={data} />;
  } else if (tool === "get_worker_latency_diagnostics") {
    label = "Latency diagnostics";
    content = <LatencyDiagnostics value={data} />;
  } else if (tool === "evaluate_worker_rules") {
    label = "Live causal-rule matches";
    content = <CausalMatches value={data} />;
  } else if (tool === "get_worker_causal_history") {
    label = "Saved causal-rule history";
    content = <CausalHistory value={data} />;
  }

  if (!content) return null;
  return (
    <section className="space-y-2 rounded-xl border p-3 sm:p-4">
      <div className="flex items-center justify-between gap-2">
        <h3 className="text-sm font-medium">{label}</h3>
        {typeof observation.evidence_id === "string" && (
          <span className="text-muted-foreground text-xs">
            {observation.evidence_id}
          </span>
        )}
      </div>
      {content}
    </section>
  );
}
