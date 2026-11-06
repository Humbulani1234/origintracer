# Agent Backend Client API

These functions call the FastAPI backend's stored-data endpoints. Worker reads
are scoped by stable process instance identity; event and trace data are read
from the backend's persisted event store. The approved action functions write
only the deployment marker and causal-history data described in their
docstrings.

## Worker inventory and reads

::: origintracer_agent.backend.list_workers

::: origintracer_agent.backend.get_worker_snapshot

::: origintracer_agent.backend.get_causal_history

::: origintracer_agent.backend.query_worker

::: origintracer_agent.backend.stitch_stored_trace

::: origintracer_agent.backend.validate_pid

## Approved writes

::: origintracer_agent.backend.record_deployment_marker

::: origintracer_agent.backend.evaluate_and_record_rules

## Query construction

::: origintracer_agent.backend.build_event_query

::: origintracer_agent.backend.build_causal_query

::: origintracer_agent.backend.build_trace_query

::: origintracer_agent.backend.build_diff_query

## Model and service configuration

::: origintracer_agent.config.backend_url

::: origintracer_agent.config.backend_api_key

::: origintracer_agent.config.model_name

::: origintracer_agent.config.ollama_model_tag

::: origintracer_agent.config.ollama_base_url

::: origintracer_agent.config.configured_chat_model

::: origintracer_agent.webapp.lifespan

::: origintracer_agent.docs.retrieve_documentation
