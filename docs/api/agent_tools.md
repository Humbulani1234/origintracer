# Agent Tools API

Read-only tools inspect backend snapshots and persisted event data. Tool
metadata helps the planner select reads and gives the assessor guidance for
interpreting returned values. The two write actions are invoked only after
the workflow's approval interrupt has been resumed with approval.

## Read-only worker tools

::: origintracer_agent.tools.list_live_workers

::: origintracer_agent.tools.get_live_workers_overview

::: origintracer_agent.tools.get_worker_status

::: origintracer_agent.tools.get_worker_snapshot

::: origintracer_agent.tools.get_worker_nodes

::: origintracer_agent.tools.get_worker_edges

::: origintracer_agent.tools.get_worker_graph

::: origintracer_agent.tools.get_worker_hotspots

::: origintracer_agent.tools.get_worker_database_hotspots

::: origintracer_agent.tools.get_worker_database_events

::: origintracer_agent.tools.get_worker_latency_diagnostics

::: origintracer_agent.tools.get_worker_events

::: origintracer_agent.tools.get_worker_trace

::: origintracer_agent.tools.get_worker_stitched_trace

::: origintracer_agent.tools.get_worker_causal_history

::: origintracer_agent.tools.evaluate_worker_rules

::: origintracer_agent.tools.get_worker_changes

## Tool metadata and planning support

::: origintracer_agent.tools.ToolProfile

::: origintracer_agent.tools.read_only_tool_catalog

::: origintracer_agent.tools.select_planned_read_only_tools

::: origintracer_agent.tools.research_guidance_for_tools

::: origintracer_agent.tools.assessment_guidance_for_tools

## Approval-gated actions

::: origintracer_agent.tools.record_deployment_marker_action

::: origintracer_agent.tools.evaluate_and_record_rules_action
