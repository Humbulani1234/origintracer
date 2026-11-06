# LangGraph Probe

The LangGraph probe observes LangChain and LangGraph work through their callback interface by recording graph and chain operations, model calls, tool
calls, and errors. It uses callback run IDs to connect each operation to its direct parent, including when parallel branches overlap.

## Setup

Install LangChain or LangGraph in the application environment. The probe uses `langchain-core`, which those packages install. Add the probe to
`origintracer.yaml`:

```yaml
probes:
  - langgraph
```

The callback hook is installed when `origintracer.init()` starts the configured
probe. LangGraph callback configuration then carries the handler through graph, model, and tool calls, while application code does not need to add the handler to each invocation.

## Observed operations

| Event | Meaning |
|---|---|
| `langgraph.chain.enter` | A runnable, graph, or graph node started |
| `langgraph.chain.exit` | That runnable or graph node completed |
| `langgraph.llm.enter` | A language model call started |
| `langgraph.llm.exit` | The model call completed |
| `langgraph.tool.enter` | A tool call started |
| `langgraph.tool.exit` | The tool call completed |
| `langgraph.error` | A chain, model, or tool call failed |

The enter and exit observations for one callback use the same `span_id`, and the exit observation adds the duration and completion details to that operation.
The engine counts the logical operation once.

## Parent relationships

The probe maps LangChain's `run_id` to `span_id` and `parent_run_id` to `parent_span_id`, since a parent can have several children. This lets the engine represent concurrent graph branches by their callback relationships, not by whichever sibling event happened to arrive first.

When a callback has no parent, the probe uses the active OriginTracer trace if one exists. A standalone top-level callback starts its own trace, and for an
out-of-order child, the probe can inherit the trace ID remembered for its callback parent.

## Event metadata

Every event uses the service name `langgraph`, and the probe adds `langgraph_kind` with the value `chain`, `llm`, or `tool`. Chain events can also include the
graph node name, step, triggers, path, and checkpoint namespace when LangGraph provides them. Outer graph chains are marked with `is_langgraph_root` so rules
can compare model calls with their owning graph.

Inputs and outputs are not copied into events by default; the probe records input and output keys and bounded callback metadata instead.

### Optional content capture

Prompt and response text can be captured by setting
`ORIGINTRACER_CAPTURE_LLM_CONTENT=1`. Tool arguments and results can be captured separately with `ORIGINTRACER_CAPTURE_TOOL_CONTENT=1`. Both options are off by default.

Captured values are bounded and common credential patterns are redacted, but application content may still be sensitive, and for security reasons enable capture only for data you are allowed to inspect.

The default preview limit is 4,000 characters. Set
`ORIGINTRACER_LLM_CONTENT_MAX_CHARS` or
`ORIGINTRACER_TOOL_CONTENT_MAX_CHARS` to change the limit.

## See also

- [LangGraph rules](../rules/langgraph_rules.md)
- The application run guide is in `applications/langgraph/README.md`.
