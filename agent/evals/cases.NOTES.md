# Evaluation case notes

## `single_worker_trace_path`

Known model limitation: the current Ollama model may fail to issue the dependent `get_worker_trace` tool call after `get_worker_events`, even when the event result contains trace IDs and the trace tool is pending. The case checks this tool-use behavior; a failure does not by itself indicate a graph or evidence-selection bug. Re-evaluate with a model/provider that reliably calls tools before changing the case expectations.
