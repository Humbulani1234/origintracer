# origintracer

The top-level public API. Import and call `init()` once in `AppConfig.ready()`.

::: origintracer
    options:
      members:
        - init
        - shutdown
        - get_engine
        - mark_deployment
        - _register_post_init_callback

---

# core.engine

::: origintracer.core.engine.Engine
    options:
      members:
        - process
        - process_batch
        - stop

---

# core.runtime_graph

::: origintracer.core.runtime_graph.RuntimeGraph
    options:
      members:
        - upsert_node
        - upsert_edge
        - add_from_event
        - all_nodes
        - all_edges
        - get_node
        - neighbors
        - callers
        - reachable_from

::: origintracer.core.runtime_graph.GraphNode

::: origintracer.core.runtime_graph.GraphEdge

---

# core.event_schema

::: origintracer.core.event_schema.NormalizedEvent
    options:
      members:
        - now
        - to_dict
        - from_dict

---

# core.causal

::: origintracer.core.causal.PatternRegistry
    options:
      members:
        - register
        - evaluate
        - rule_names
        - register

::: origintracer.core.causal.CausalRule

::: origintracer.core.causal.CausalMatch

---

# core.temporal

::: origintracer.core.temporal.TemporalStore
    options:
      members:
        - capture
        - label_diff
        - new_edges_since
        - removed_edges_since
        - changes_since
        - latest_diff

---

# core.active_requests

::: origintracer.core.active_requests.ActiveRequestTracker
    options:
      members:
        - start
        - event
        - complete
        - active_count
        - slow_in_flight
        - recent_completions
        - percentile
        - all_patterns_summary

::: origintracer.core.active_requests.RequestSpan

---

# sdk.emitter

::: origintracer.sdk.emitter
    options:
      members:
        - emit
        - emit_direct
        - _restart_drain_thread

---

# sdk.base_probe

::: origintracer.sdk.base_probe.BaseProbe

---