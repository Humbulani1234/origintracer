# Agent Workflow Graph API

The graph routes chat, investigation, and approved actions. Investigation
nodes plan bounded reads, record tool evidence, assess deliverables, and
synthesize a scoped report. The compiled `agent` object is the Agent Server
graph entry point referenced by `agent/langgraph.json`; it is described here
in prose because the compiled object itself has no Python docstring.

!!! info "Reading this reference"
    Each entry is generated from its Python docstring and signature. The graph
    is grouped by request route, investigation stage, and approval boundary.

### Request routing and general chat

::: origintracer_agent.graph.classify_request
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.route_after_classification
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.answer_general_chat
    options:
      heading_level: 4
      show_root_full_path: false

### Investigation lifecycle

::: origintracer_agent.graph.prepare_investigation
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.plan_investigation
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.gather_evidence
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.record_evidence
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.assess_evidence
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.synthesize_findings
    options:
      heading_level: 4
      show_root_full_path: false

### Workflow routing

::: origintracer_agent.graph.route_after_prepare
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.route_after_research
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.route_after_recorded_evidence
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.route_after_assessment
    options:
      heading_level: 4
      show_root_full_path: false

### Approval-gated actions

::: origintracer_agent.graph.request_action_approval
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.route_after_approval
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.graph.execute_approved_action
    options:
      heading_level: 4
      show_root_full_path: false
