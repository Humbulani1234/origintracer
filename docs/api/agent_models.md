# Agent Investigation Data Models

The state schema separates conversation history, selected worker identity,
investigation questions, plans, hypotheses, evidence, conclusions, and
approval state. Pydantic model docstrings and field descriptions are rendered
from source.

## Thread state

::: origintracer_agent.state.InvestigationState

## Plan and requested outputs

::: origintracer_agent.investigation.PlanStep

::: origintracer_agent.investigation.DeliverableDraft

::: origintracer_agent.investigation.DeliverableQuestion

::: origintracer_agent.investigation.DeliverableNextStep

::: origintracer_agent.investigation.HypothesisDraft

::: origintracer_agent.investigation.InvestigationPlan

## Intent and action schemas

::: origintracer_agent.investigation.ActionIntent

::: origintracer_agent.investigation.DeploymentMarkerIntent

::: origintracer_agent.investigation.CausalRuleActionIntent

## Assessment and report schemas

::: origintracer_agent.investigation.HypothesisReview

::: origintracer_agent.investigation.FindingDraft

::: origintracer_agent.investigation.DirectAnswerDraft

::: origintracer_agent.investigation.EvidenceAssessmentFields

::: origintracer_agent.investigation.EvidenceAssessment

::: origintracer_agent.investigation.EvidenceAssessmentWithoutHypotheses
