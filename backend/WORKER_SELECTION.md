# Live worker selection

OriginTracer keeps one runtime graph inside each instrumented process. The React
dashboard therefore selects a process before requesting live graph data; it does
not merge or relocate those graphs.

## Current workflow

```text
Instrumented process
  -> /tmp/origintracer-{pid}.sock
  -> LocalQueryServer evaluates the existing DSL against that process's Engine

React dashboard
  -> GET /api/v1/workers
  -> selects a discovered PID
  -> POST /api/v1/workers/{pid}/query {"query": "SHOW GRAPH"}

FastAPI
  -> resolves the PID against the current socket inventory
  -> sends newline-delimited JSON through the Unix socket
  -> returns that worker's response to React
```

The browser never receives authority to choose an arbitrary filesystem path.
FastAPI constructs the path from the integer PID and accepts it only while that
path appears in `discover_sockets()`.

React automatically selects the first live process, preserves the selection
while that PID remains available, and falls back to another live process if it
exits. When no local socket is available, it clears process-owned data and
displays an explicit `No live OriginTracer processes` state. It does not show
a previously uploaded snapshot as though that snapshot were a live process.

Persistence-backed features remain separate: causal history can still be
browsed, and an explicit `\\stitch <trace_id>` lookup can still retrieve a
historical trace when no live worker exists.

This workflow requires FastAPI to share the instrumented processes' `/tmp` and
PID namespace. It is intended for the local/live inspection surface represented
by the existing REPL.

## Deliberately postponed

Live worker selection does not require changing the uploader or persistence
model. A separate future decision may add durable per-process attribution by:

1. stamping event batches, graph snapshots, and diffs with the uploader PID;
2. keying backend graphs by `(customer_id, pid)`;
3. storing the PID with snapshots and diffs; and
4. adding PID filters to historical events, traces, and causal history.

That future work should not change the Unix-socket bridge: the bridge answers
questions about a process's current in-memory engine, while persistence answers
historical and cross-process questions.
