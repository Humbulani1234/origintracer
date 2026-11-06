# LangGraph Rules

The LangGraph rules use metadata from the LangGraph probe to find repeated model calls and slow model or tool work. Enable the probe and rules in the
application's `origintracer.yaml`:

```yaml
probes:
  - langgraph

rules:
  - loop_runaway
  - agent_latency_hotspot
```

Run `CAUSAL` in the REPL to see matching rules, and to show only LangGraph findings, run `CAUSAL langgraph`.

## Loop runaway

**Confidence:** 0.85  
**Tags:** `langgraph`, `agent`, `loop`

This rule finds a model node whose aggregate call count is at least eight andnwhose call count is at least eight(adjustable) times the aggregate call count of its nearest owning graph node. It follows graph callers to find that outer graph, allowing for wrapper chains between the model and the graph.

The rule can point to a loop that does not converge, a conditional exit that does not fire, or a tool error path that keeps requesting more model work. It
does not prove which of these caused the repeated calls.

The evidence includes the model node, owning graph node, aggregate call counts, the call ratio, and average model duration.

## Agent latency hotspot

**Confidence:** 0.65  
**Tags:** `langgraph`, `agent`, `performance`

This rule finds an LLM or tool node that accounts for more than 40(adjustable) percent of the observed LLM and tool time. At least two model or tool nodes and a non-zero total duration are needed for a result.

Chain durations are not included because they usually include the time of their child operations. Including both would count the same work more than once. The evidence reports each matching node, its kind, call count, average duration, and share of total leaf-work time.

## Limits

The RuntimeGraph groups observations by service and node name over the life of the process. The loop ratio and latency share are aggregate measurements; they
do not describe each individual run. The rules also do not compare parallel branch overlap. Earlier runs can affect a later result. Restart the application before a rule demonstration when you want a fresh graph.

The `applications/langgraph/README.md` guide describes the separate invocation scripts for the two rule scenarios.
