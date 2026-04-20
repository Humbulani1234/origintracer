# REPL Overview

The REPL connects to the engine via a Unix socket (`/tmp/stacktracer-*.sock`) and queries it in real time while your app runs.

## Start

```bash
python -m stacktracer.scripts.repl
```

## Command reference

| Command | Description |
|---|---|
| `SHOW nodes` | All graph nodes with call count and avg duration |
| `SHOW edges` | All graph edges with call count |
| `SHOW events LIMIT 20` | Recent probe events |
| `SHOW latency` | Nodes sorted by avg duration |
| `SHOW active` | Currently in-flight requests |
| `SHOW status` | Engine health, buffer depth, dropped events |
| `SHOW probes` | Active probes |
| `CAUSAL` | Run all causal rules |
| `CAUSAL <tag>` | Run rules filtered by tag |
| `DIFF SINCE deployment` | Graph changes since last deployment marker |
| `MARK DEPLOYMENT <label>` | Mark a deployment boundary |
| `TRACE <trace_id>` | Events for one trace |
| `\stitch <trace_id>` | Merge multi-process graphs into one timeline |
| `HOTSPOT TOP 10` | Top 10 nodes by call count |
| `BLAME <node>` | Upstream callers of a node |

## \stitch command

`\stitch` is the centrepiece. It takes a `trace_id` visible in `SHOW events` and reconstructs the full causal timeline across process boundaries — gunicorn worker + celery worker + redis — into one ordered sequence of stages with durations.

```
› \stitch fbaa0af4-2305-42df-97db-caec54bd65a1

  nginx::accept          0.2ms
  uvicorn::receive       0.1ms
  django::/api/orders/   4.3ms
    django::OrderView    4.1ms
    django::SELECT...    3.8ms  ← 90 calls
  celery::send_email     12.4ms
  ─────────────────────────────
  total                  17.3ms
```

---