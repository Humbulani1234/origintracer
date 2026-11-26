import { svcColor, short } from "./NodeTable";
import ObservationDetails from "./ObservationDetails";

const formatMs = (value) =>
  typeof value === "number" && Number.isFinite(value)
    ? `${value.toFixed(3)} ms`
    : "—";

const formatTimestamp = (value) =>
  typeof value === "number" && Number.isFinite(value)
    ? new Date(value * 1000).toLocaleString()
    : "—";

function Section({ title, children, subtitle }) {
  return (
    <section style={{
      marginTop: 14,
      border: "1px solid var(--border)",
      borderRadius: 4,
      background: "var(--bg2)",
      overflow: "hidden",
    }}>
      <div style={{ padding: "9px 11px", borderBottom: "1px solid var(--border)" }}>
        <div style={{ color: "var(--text)", fontSize: 11, fontWeight: 600 }}>{title}</div>
        {subtitle && (
          <div style={{ color: "var(--muted)", fontSize: 11, marginTop: 4 }}>{subtitle}</div>
        )}
      </div>
      <div style={{ padding: 10 }}>{children}</div>
    </section>
  );
}

function Metric({ label, value, detail }) {
  return (
    <div style={{ minWidth: 150, flex: "1 1 150px", padding: 9,
      border: "1px solid var(--border)", borderRadius: 4, background: "var(--bg)" }}>
      <div style={{ color: "var(--muted)", fontSize: 11, textTransform: "uppercase",
        letterSpacing: "0.05em" }}>{label}</div>
      <div style={{ color: "var(--amber)", fontFamily: "monospace", fontSize: 13,
        marginTop: 5 }}>{value}</div>
      {detail && <div style={{ color: "var(--muted)", fontSize: 11, marginTop: 4,
        overflowWrap: "anywhere" }}>{detail}</div>}
    </div>
  );
}

function DataRow({ label, value }) {
  return (
    <div style={{ display: "grid", gridTemplateColumns: "minmax(130px, 0.7fr) minmax(0, 2fr)",
      gap: 10, padding: "5px 0", borderBottom: "1px solid var(--border)", fontSize: 11 }}>
      <span style={{ color: "var(--muted)" }}>{label}</span>
      <span style={{ color: "var(--text)", overflowWrap: "anywhere" }}>{value ?? "—"}</span>
    </div>
  );
}

function TraceCompleteness({ completeness }) {
  if (!completeness) {
    return (
      <Section title="Trace completeness">
        <div style={{ color: "var(--muted)", fontSize: 11 }}>
          Lifecycle and capture diagnostics were not returned by the backend.
        </div>
      </Section>
    );
  }

  const capture = completeness.capture_loss ?? {};
  const sources = capture.sources ?? {};
  const workerTotals = capture.worker_wide_loss_totals ?? {};
  const lifecycle = completeness.lifecycle_record;
  const heartbeat = completeness.lifecycle_heartbeat;
  const perTraceCount = Number(capture.dropped_events ?? 0);
  let captureSummary = "Capture-loss diagnostics unavailable.";
  if (Object.keys(capture).length > 0) {
    if (perTraceCount > 0) {
      captureSummary = `${perTraceCount} trace-correlated dropped event(s) reported.`;
    } else if (capture.trace_counts_truncated) {
      captureSummary = "No matching trace-loss count was retained; counters were truncated, so loss cannot be ruled out.";
    } else {
      captureSummary = "No trace-correlated capture loss was reported.";
    }
  }

  return (
    <Section title="Trace completeness" subtitle="Observed lifecycle and capture evidence; missing boundaries alone do not establish why a trace ended.">
      <DataRow label="Status" value={completeness.status} />
      <DataRow label="Status basis" value={completeness.status_basis} />
      <DataRow label="Request entry observed" value={completeness.request_entry_observed == null ? "—" : String(completeness.request_entry_observed)} />
      <DataRow label="Request terminal observed" value={completeness.request_terminal_observed == null ? "—" : String(completeness.request_terminal_observed)} />
      {completeness.request_error_probes?.length > 0 && (
        <DataRow label="Request error probes" value={completeness.request_error_probes.join(", ")} />
      )}
      <DataRow label="Events returned" value={`${completeness.events_returned ?? "—"}${completeness.events_may_be_truncated ? " · sample may be truncated" : ""}`} />
      <DataRow label="Lifecycle heartbeat" value={heartbeat
        ? `${heartbeat.fresh ? "fresh" : "stale or unavailable"}${heartbeat.heartbeat_age_s == null ? "" : ` · ${Number(heartbeat.heartbeat_age_s).toFixed(1)}s old`}`
        : "unavailable"} />
      {lifecycle && (
        <>
          <DataRow label="Tracker state" value={[lifecycle.state, lifecycle.reason].filter(Boolean).join(" · ")} />
          {lifecycle.exception_type && <DataRow label="Observed exception" value={lifecycle.exception_type} />}
          {lifecycle.event_count != null && <DataRow label="Tracked events" value={lifecycle.event_count} />}
          {lifecycle.in_flight_ms != null && <DataRow label="In flight at sample" value={formatMs(lifecycle.in_flight_ms)} />}
          {lifecycle.last_event_at != null && <DataRow label="Last tracked event" value={formatTimestamp(lifecycle.last_event_at)} />}
        </>
      )}
      <div style={{ marginTop: 8, color: "var(--text)", fontSize: 11 }}>{captureSummary}</div>
      {Object.keys(sources).length > 0 && (
        <DataRow label="Trace-specific sources" value={Object.entries(sources)
          .map(([name, count]) => `${name}: ${count}`).join(" · ")} />
      )}
      {Object.keys(workerTotals).length > 0 && (
        <DataRow label="Worker-wide totals" value={`${Object.entries(workerTotals)
          .map(([name, count]) => `${name}: ${count}`).join(" · ")} (not trace-correlated)`} />
      )}
      {capture.trace_counts_truncated && (
        <div style={{ color: "var(--muted)", fontSize: 11, marginTop: 7 }}>
          Per-trace counters were truncated; an absent trace-specific count is inconclusive.
        </div>
      )}
    </Section>
  );
}

function TraceTiming({ timing }) {
  if (!timing) {
    return (
      <Section title="Request and trace timing">
        <div style={{ color: "var(--muted)", fontSize: 11 }}>
          Timing details were not returned by the backend.
        </div>
      </Section>
    );
  }
  const spans = Array.isArray(timing.spans) ? timing.spans : [];
  const overlaps = Array.isArray(timing.parallel_overlaps)
    ? timing.parallel_overlaps
    : [];

  return (
    <>
      <Section title="Request and trace timing" subtitle="Wall duration is elapsed time. Cumulative inclusive duration may count nested or overlapping work more than once.">
        <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
          <Metric label="Request wall duration" value={formatMs(timing.request_wall_duration_ms)} detail={timing.request_wall_duration_ms == null ? "Requires observed request boundaries" : timing.trace_wall_duration_source} />
          <Metric label="Trace wall duration" value={formatMs(timing.trace_wall_duration_ms)} detail={timing.trace_wall_duration_source ?? "No complete trace interval returned"} />
          <Metric label="Cumulative inclusive" value={formatMs(timing.cumulative_inclusive_duration_ms)} detail={timing.cumulative_scope} />
          <Metric label="Observed overlaps" value={timing.parallel_overlaps_count ?? overlaps.length}
            detail={`${timing.parallel_overlaps_truncated ? "Detail rows are truncated · " : ""}${timing.parallel_overlap_scope ?? "Returned interval overlaps"}`} />
        </div>
        <DataRow label="Interval basis" value={timing.interval_basis} />
        <DataRow label="Event sample truncated" value={timing.events_may_be_truncated == null ? "—" : String(timing.events_may_be_truncated)} />
      </Section>

      <Section title={`Measured spans (${spans.length}${timing.spans_truncated ? ` of ${timing.spans_count}` : ""})`}
        subtitle="Only rows with paired boundaries or an explicit recorded duration have a measured interval.">
        {spans.length === 0 ? (
          <div style={{ color: "var(--muted)", fontSize: 11 }}>No complete span intervals were returned.</div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <div style={{ minWidth: 980 }}>
              <div style={{ display: "grid", gridTemplateColumns: "minmax(180px, 2fr) minmax(145px, 1.4fr) minmax(150px, 1.3fr) minmax(150px, 1.3fr) 90px 90px minmax(170px, 2fr)",
                gap: 8, padding: "0 0 6px", color: "var(--muted)", fontSize: 11 }}>
                <span>operation</span><span>observed boundaries</span><span>start timestamp</span><span>end timestamp</span><span>inclusive</span><span>self-time</span><span>self-time scope / source</span>
              </div>
              {spans.map((span) => (
                <div key={span.span_id} style={{ display: "grid", gridTemplateColumns: "minmax(180px, 2fr) minmax(145px, 1.4fr) minmax(150px, 1.3fr) minmax(150px, 1.3fr) 90px 90px minmax(170px, 2fr)",
                  gap: 8, padding: "6px 0", borderTop: "1px solid var(--border)",
                  color: "var(--text)", fontSize: 11, alignItems: "start" }}>
                  <span title={span.name} style={{ overflowWrap: "anywhere" }}>
                    <span>{span.service ?? "unknown"}::{span.name ?? "unnamed"}</span>
                    <span style={{ display: "block", color: "var(--muted)", fontSize: 11, marginTop: 3 }}>
                      span {span.span_id}{span.parent_span_id ? ` · parent ${span.parent_span_id}` : " · root span"}
                    </span>
                  </span>
                  <span style={{ color: "var(--muted)", overflowWrap: "anywhere" }}>
                    {span.probe_start || span.probe_end
                      ? `${span.probe_start ?? "—"} → ${span.probe_end ?? "—"}`
                      : "Duration only · no paired probes"}
                  </span>
                  <span style={{ color: "var(--muted)", fontFamily: "monospace", overflowWrap: "anywhere" }}>
                    {formatTimestamp(span.start_timestamp)}
                  </span>
                  <span style={{ color: "var(--muted)", fontFamily: "monospace", overflowWrap: "anywhere" }}>
                    {formatTimestamp(span.end_timestamp)}
                  </span>
                  <span style={{ fontFamily: "monospace", color: "var(--amber)" }}>{formatMs(span.inclusive_duration_ms)}</span>
                  <span style={{ fontFamily: "monospace" }}>{formatMs(span.self_time_ms)}</span>
                  <span style={{ color: "var(--muted)", overflowWrap: "anywhere" }}>
                    <span style={{ display: "block" }}>{span.timing_source ?? "Timing source unavailable"}</span>
                    {span.self_time_scope && <span style={{ display: "block", marginTop: 3 }}>{span.self_time_scope}</span>}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </Section>

      {overlaps.length > 0 && (
        <Section title="Observed interval overlaps" subtitle={timing.parallel_overlap_scope}>
          {overlaps.map((overlap, index) => (
            <DataRow key={`${overlap.span_ids?.join("-")}-${index}`}
              label={`${overlap.relationship} · ${formatMs(overlap.overlap_ms)}`}
              value={`spans ${overlap.span_ids?.join(" and ") ?? "—"}${overlap.parent_span_id ? ` · parent ${overlap.parent_span_id}` : ""}`} />
          ))}
        </Section>
      )}
    </>
  );
}

export default function TraceTimeline({ trace, mode = "trace" }) {
  if (!trace) {
    return (
      <div style={{ padding: "32px 14px", fontFamily: "monospace", fontSize: 11,
        color: "var(--muted)", textAlign: "center" }}>
        enter <span style={{ color: "var(--amber)" }}>
          {mode === "stitch" ? "\\stitch &lt;trace_id&gt;" : "TRACE &lt;trace_id&gt;"}
        </span> in the query bar to {mode === "stitch" ? "search uploaded events across stored workers" : "inspect the selected snapshot's stored events"}
      </div>
    );
  }

  const { id, stages = [] } = trace;
  const stitched = trace.source === "stitched";
  const timelineValues = stages.map((stage) => {
    const value = stitched
      ? stage.duration_ms
      : (stage.inter_event_gap_ms ?? stage.duration_ms);
    return typeof value === "number" ? value : null;
  });
  const maxGap = Math.max(0, ...timelineValues.filter((value) => value > 0));
  const timing = trace.timing;
  const completeness = trace.traceCompleteness;

  return (
    <div style={{ padding: 14 }}>
      <div style={{ fontFamily: "monospace", fontSize: 11, color: "var(--muted)",
        marginBottom: 8, letterSpacing: "0.06em" }}>
        {stitched ? "stitched trace " : "trace "}
        <span title={id} style={{ color: "var(--amber)", overflowWrap: "anywhere" }}>{id}</span>
        {stitched && (
          <div style={{ marginTop: 5 }}>
            found on worker(s) {trace.workersFound.join(", ") || "none"}
            {trace.errors.length > 0 && ` · ${trace.errors.length} worker error(s)`}
          </div>
        )}
      </div>

      {!stitched && <>
        <TraceTiming timing={timing} />
        <TraceCompleteness completeness={completeness} />
      </>}

      {Array.isArray(trace.warnings) && trace.warnings.map((warning, index) => (
        <div key={`${warning.code ?? "warning"}-${index}`} style={{ marginTop: 8,
          padding: 8, borderLeft: "3px solid var(--amber)", background: "var(--bg2)",
          color: "var(--muted)", fontSize: 11, overflowWrap: "anywhere" }}>
          {warning.message ?? warning.code}
        </div>
      ))}

      <Section title={`Recorded event sequence (${stages.length})`}
        subtitle={stitched
          ? "Per-event recorded durations only; no cross-worker wall duration is calculated."
          : "Gap is elapsed time since the preceding event, not time spent in this operation. Recorded event duration is shown separately."}>
        {stages.length === 0 ? (
          <div style={{ color: "var(--muted)", fontSize: 11 }}>No stored event rows were returned for this trace.</div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <div style={{ minWidth: 620 }}>
              <div style={{ display: "grid", gridTemplateColumns: "90px minmax(50px, 1fr) 100px minmax(220px, 2fr)",
                gap: 9, padding: "0 0 6px", color: "var(--muted)", fontSize: 11 }}>
                <span>{stitched ? "recorded duration" : "gap from previous"}</span>
                <span></span>
                <span>{stitched ? "" : "event duration"}</span>
                <span>event</span>
              </div>
              {stages.map((stage, index) => {
                const gap = timelineValues[index];
                const recorded = stitched ? null : stage.recorded_duration_ms;
                const barWidth = gap > 0 && maxGap > 0 ? Math.max(2, (gap / maxGap) * 100) : 0;
                return (
                  <div key={`${stage.span_id ?? stage.timestamp ?? index}-${index}`}
                    style={{ display: "grid", gridTemplateColumns: "90px minmax(50px, 1fr) 100px minmax(220px, 2fr)",
                      gap: 9, alignItems: "center", padding: "5px 0",
                      borderTop: "1px solid var(--border)", fontSize: 11, minHeight: 25 }}>
                    <span style={{ textAlign: "right", fontFamily: "monospace",
                      color: gap > 0 ? "var(--muted)" : "var(--muted)" }}>
                      {gap != null && gap > 0 ? formatMs(gap) : "—"}
                    </span>
                    <div style={{ height: 6, background: "var(--bg3)", borderRadius: 2 }}>
                      <div style={{ height: "100%", width: `${barWidth}%`,
                        background: svcColor(stage.service), borderRadius: 2 }} />
                    </div>
                    <span style={{ textAlign: "right", fontFamily: "monospace",
                      color: recorded != null ? "var(--amber)" : "var(--muted)" }}>
                      {recorded == null ? "—" : formatMs(recorded)}
                    </span>
                    <span style={{ color: "var(--text)", overflowWrap: "anywhere" }}>
                      <span>{stage.probe ?? "unknown probe"}</span>
                      <span style={{ marginLeft: 5 }}>
                        <span style={{ color: "var(--muted)" }}>{stage.service ?? "unknown"}::</span>
                        {short(String(stage.name ?? "unnamed"), 36)}
                      </span>
                      {stitched && <span style={{ marginLeft: 6 }}>pid {stage.worker_pid}</span>}
                      {stage.timestamp != null && <span style={{ display: "block", fontSize: 11,
                        marginTop: 2 }}>at {formatTimestamp(stage.timestamp)}</span>}
                      <span style={{ display: "block", marginTop: 4 }}>
                        <ObservationDetails
                          metadata={stage.metadata}
                          spanId={stage.span_id}
                          parentSpanId={stage.parent_span_id}
                          pid={stage.pid}
                          tid={stage.tid}
                          workerPid={stage.worker_pid}
                          workerInstanceId={stage.worker_instance_id}
                        />
                      </span>
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        )}
        {stitched && (
          <div style={{ marginTop: 10, color: "var(--muted)", fontSize: 11 }}>
            {stages.length} events across {trace.workersFound.length} worker(s) · no combined end-to-end duration
          </div>
        )}
      </Section>
    </div>
  );
}
