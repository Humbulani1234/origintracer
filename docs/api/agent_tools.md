# Agent Tools API

Read-only tools inspect backend snapshots and persisted event data. Tool
metadata helps the planner select reads and gives the assessor guidance for
interpreting returned values. The two write actions are invoked only after
the workflow's approval interrupt has been resumed with approval.

!!! info "Capability boundary"
    Read tools inspect stored worker data. The final section contains the only
    tools here that can write, and both are approval-gated.

### Read-only worker tools

::: origintracer_agent.tools.list_live_workers
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_live_workers_overview
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_status
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_snapshot
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_nodes
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_edges
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_graph
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_hotspots
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_database_hotspots
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_database_events
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_component_diagnostics
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_events
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_trace
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_stitched_trace
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_causal_history
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.evaluate_worker_rules
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.get_worker_changes
    options:
      heading_level: 4
      show_root_full_path: false

### Tool metadata and planning support

::: origintracer_agent.tools.ToolProfile
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.read_only_tool_catalog
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.select_planned_read_only_tools
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.research_guidance_for_tools
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.assessment_guidance_for_tools
    options:
      heading_level: 4
      show_root_full_path: false

### Approval-gated actions

::: origintracer_agent.tools.record_deployment_marker_action
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.tools.evaluate_and_record_rules_action
    options:
      heading_level: 4
      show_root_full_path: false
