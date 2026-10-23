# React UI - Overview

Minimal terminal-aesthetic dashboard that mirrors the REPL in a browser.
Connects to the FastAPI backend and renders the live RuntimeGraph.

## Quick Start

```bash
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173`.

## Query Bar

Type REPL-style commands and press Enter or click Run:

| Command | Description |
|---|---|
| `SHOW nodes` | All nodes in the current graph |
| `SHOW edges` | All edges with call counts |
| `SHOW events` | Recent probe events |
| `TRACE` | Switch to trace view |
| `\stitch <id>` | Fetch and reconstruct a trace from the backend |

The query bar accepts the same DSL as the REPL. See the
[Command Reference](../repl/overview.md) for the full list.

## Views

The UI has two primary views:

**Graph view** - renders the RuntimeGraph as a node-edge diagram. Node size
reflects `call_count`. Edge labels show call frequency and average latency.

**Trace view** - activated by `TRACE` or `\stitch <id>`. Renders the
critical path for a single trace as a waterfall.

# React UI — Installation

## Prerequisites

Node.js 18 or higher - [nodejs.org](https://nodejs.org) (LTS version).

## Install

```bash
cd frontend
npm install
```

## Run in development

```bash
npm run dev
# http://localhost:5173
```

## Build for production

```bash
npm run build
# Output: frontend/dist/
```

Serve `dist/` from any static host or mount it behind the FastAPI app.

# React UI — Wiring to Backend

The UI expects the FastAPI backend running at `http://localhost:8001` by
default.

## Configuration

Set the backend URL in `frontend/.env`:

```bash
VITE_API_BASE=http://localhost:8001
```

For production point this at your deployed backend URL.

## Authentication

The UI sends `Authorization: Bearer <key>` on every request. Set the key in:

```bash
VITE_API_KEY=sk_dev_yyy # just for development
```

Keys are validated against `ORIGINTRACER_API_KEYS` on the backend. See
[Backend Overview](../backend/overview.md) for how to configure keys.

## Health Check

The UI polls `GET /health` on load to confirm the backend is reachable before
sending any queries. If it fails, a connection error banner is shown.