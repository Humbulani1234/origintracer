# Agent Backend Client API

These functions call the FastAPI backend's stored-data endpoints. Worker reads
are scoped by stable process instance identity; event and trace data are read
from the backend's persisted event store. The approved action functions write
only the deployment marker and causal-history data described in their
docstrings.

!!! info "Backend access"
    The client scopes worker reads to a resolved process instance. Write
    operations are listed separately from read and query-building functions.

### Worker inventory and reads

::: origintracer_agent.backend.list_workers
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.backend.get_worker_snapshot
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.backend.get_causal_history
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.backend.query_worker
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.backend.stitch_stored_trace
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.backend.validate_pid
    options:
      heading_level: 4
      show_root_full_path: false

### Approved writes

::: origintracer_agent.backend.record_deployment_marker
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.backend.evaluate_and_record_rules
    options:
      heading_level: 4
      show_root_full_path: false

### Query construction

::: origintracer_agent.backend.build_event_query
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.backend.build_causal_query
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.backend.build_trace_query
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.backend.build_diff_query
    options:
      heading_level: 4
      show_root_full_path: false

### Model and service configuration

::: origintracer_agent.config.backend_url
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.config.backend_api_key
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.config.model_name
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.config.ollama_model_tag
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.config.ollama_base_url
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.config.configured_chat_model
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.webapp.lifespan
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.docs.retrieve_documentation
    options:
      heading_level: 4
      show_root_full_path: false
