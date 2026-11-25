function formatLabel(key) {
  return key.replaceAll("_", " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

export default function ObservationDetails({
  metadata,
  spanId,
  parentSpanId,
  pid,
  tid,
  workerPid,
  workerInstanceId,
}) {
  const details = { ...(metadata ?? {}) };
  if (spanId) details.span_id = spanId;
  if (parentSpanId) details.parent_span_id = parentSpanId;
  if (pid != null) details.pid = pid;
  if (tid != null) details.tid = tid;
  if (workerPid != null) details.worker_pid = workerPid;
  if (workerInstanceId) details.worker_instance_id = workerInstanceId;
  const entries = Object.entries(details);

  if (!entries.length) return <span className="observation-details-empty">—</span>;

  return (
    <details className="observation-details">
      <summary>inspect <span>{entries.length}</span></summary>
      <dl className="observation-details-panel">
        {entries.map(([key, value]) => (
          <div className="observation-detail" key={key}>
            <dt>{formatLabel(key)}</dt>
            <dd>{String(value)}</dd>
          </div>
        ))}
      </dl>
    </details>
  );
}
