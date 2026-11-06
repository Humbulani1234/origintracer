# Agent Investigation Data Models

The state schema separates conversation history, selected worker identity,
investigation questions, plans, hypotheses, evidence, conclusions, and
approval state. Pydantic model docstrings and field descriptions are rendered
from source.

!!! info "Schema guide"
    These models describe the data passed between workflow nodes. The section
    headings group them by role; field types and descriptions come from source.

### Thread state

::: origintracer_agent.state.InvestigationState
    options:
      heading_level: 4
      show_root_full_path: false

### Plan and requested outputs

::: origintracer_agent.investigation.PlanStep
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.DeliverableDraft
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.DeliverableQuestion
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.DeliverableNextStep
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.HypothesisDraft
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.InvestigationPlan
    options:
      heading_level: 4
      show_root_full_path: false

### Intent and action schemas

::: origintracer_agent.investigation.ActionIntent
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.DeploymentMarkerIntent
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.CausalRuleActionIntent
    options:
      heading_level: 4
      show_root_full_path: false

### Assessment and report schemas

::: origintracer_agent.investigation.HypothesisReview
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.FindingDraft
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.DirectAnswerDraft
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.EvidenceAssessmentFields
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.EvidenceAssessment
    options:
      heading_level: 4
      show_root_full_path: false

::: origintracer_agent.investigation.EvidenceAssessmentWithoutHypotheses
    options:
      heading_level: 4
      show_root_full_path: false
