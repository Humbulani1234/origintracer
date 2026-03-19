# StackTracer UI

Minimal terminal-aesthetic dashboard for the StackTracer runtime graph.
Mirrors the REPL in a browser. Amber on black. Monospace data.

---

## What it shows

Four views — nodes, edges, trace timeline, events. Switches via sidebar nav
or by typing REPL-style commands in the query bar.

The trace timeline is the centrepiece — proportional duration bars per stage,
colored by service, end-to-end latency summary at the bottom. Same output as
`\stitch <trace_id>` in the REPL, rendered visually.

---

## Prerequisites

Node.js 18+ — https://nodejs.org (LTS version)

---

## Quick start

```bash
cd stacktracer-ui
npm install
npm run dev       # http://localhost:5173
```

Mock data is hardcoded as initial state — the UI is fully functional with no
backend running. All four views, the query bar, and the trace timeline work
immediately on first load.

---

## Build for deployment

```bash
npm run build
```

Produces `dist/` — static HTML, JS, CSS. Copy only this folder to your server.

```bash
# serve on EC2 with Python (no nginx needed for demo)
cd dist
python3 -m http.server 3000
```

Or with nginx — point `root` at the `dist/` directory and add
`try_files $uri $uri/ /index.html` for client-side routing.

---

## Wiring to real data

When the backend is running, edit `src/api/client.js` — set `VITE_API_URL`
or change the `BASE` default:

```js
const BASE = import.meta.env.VITE_API_URL || "http://localhost:7123";
```

The UI polls every 5 seconds. When the API returns data, mock state is replaced.
When the API is unavailable, mock data stays — no errors shown to the user.

The API bridge (`stacktracer/bridge.py`) is a 20-line FastAPI app that wraps
the Unix socket queries as HTTP endpoints. See `src/api/client.js` for the
full spec.

---

## Query bar

Type REPL-style commands and press Enter or click run:

```
SHOW nodes
SHOW edges
SHOW events
TRACE              ← switches to trace view
\stitch <id>       ← fetches real trace from backend if available
```

---

---

## Design

Terminal aesthetic — `#0d0d0d` background, amber `#e8a020` accent,
JetBrains Mono for all data. No UI framework. No component library.
Plain React with inline styles and a single CSS file.

Dark mode only. The graph is a developer tool — it does not need a light mode.