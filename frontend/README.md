# OriginTracer UI

Minimal terminal-aesthetic dashboard for the OriginTracer runtime graph.
Mirrors the REPL in a browser.

---
## Prerequisites

Node.js 18+ — https://nodejs.org (LTS version)

---
## Quick start

```bash
cd frontend
npm install
npm run dev # http://localhost:5173
```
---
## Query bar

Type REPL-style commands and press Enter or click run:
Currently supports `\stitch <id>`
```
SHOW nodes
SHOW edges
SHOW events
TRACE << switches to trace view
\stitch <id>  << fetches real trace from backend if available
```