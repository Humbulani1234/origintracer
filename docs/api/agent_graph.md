# Agent Workflow Graph API

The graph routes chat, investigation, and approved actions. Investigation
nodes plan bounded reads, record tool evidence, assess deliverables, and
synthesize a scoped report. The compiled `agent` object is the Agent Server
graph entry point referenced by `agent/langgraph.json`; it is described here
in prose because the compiled object itself has no Python docstring.

## Request routing and general chat

::: origintracer_agent.graph.classify_request

::: origintracer_agent.graph.route_after_classification

::: origintracer_agent.graph.answer_general_chat

## Investigation lifecycle

::: origintracer_agent.graph.prepare_investigation

::: origintracer_agent.graph.plan_investigation

::: origintracer_agent.graph.gather_evidence

::: origintracer_agent.graph.record_evidence

::: origintracer_agent.graph.assess_evidence

::: origintracer_agent.graph.synthesize_findings

## Workflow routing

::: origintracer_agent.graph.route_after_prepare

::: origintracer_agent.graph.route_after_research

::: origintracer_agent.graph.route_after_recorded_evidence

::: origintracer_agent.graph.route_after_assessment

## Approval-gated actions

::: origintracer_agent.graph.request_action_approval

::: origintracer_agent.graph.route_after_approval

::: origintracer_agent.graph.execute_approved_action
