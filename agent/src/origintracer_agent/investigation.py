"""Structured model outputs used by the investigation workflow."""

from typing import Literal

from pydantic import BaseModel, Field


class PlanStep(BaseModel):
    """One question the evidence gathering part of the graph should answer."""

    description: str
    purpose: str
    suggested_tools: list[str] = Field(
        default_factory=list, max_length=8
    )


class HypothesisDraft(BaseModel):
    """A possible explanation to compare with gathered evidence."""

    claim: str
    reason_to_check: str


class InvestigationPlan(BaseModel):
    """Plan and possible explanations for one user request."""

    continue_existing: bool = False
    level: Literal["query", "multi_step", "verification"]
    scope_summary: str
    steps: list[PlanStep] = Field(
        default_factory=list, max_length=8
    )
    hypotheses: list[HypothesisDraft] = Field(
        default_factory=list, max_length=6
    )
    unresolved_questions: list[str] = Field(
        default_factory=list, max_length=10
    )


class ActionIntent(BaseModel):
    """Select investigation or one action, without action arguments."""

    action: Literal[
        "investigate",
        "record_deployment_marker",
        "evaluate_and_record_rules",
    ] = Field(
        default="investigate",
        description="Choose an action only for a clear request to perform it now.",
    )


class DeploymentMarkerIntent(BaseModel):
    """Extract only the deployment-marker label for a clear write request."""

    label: str = Field(
        default="deployment",
        min_length=1,
        max_length=200,
        description=(
            "The label explicitly requested for the deployment marker. "
            "Use deployment when the user did not provide a label."
        ),
    )


class CausalRuleActionIntent(BaseModel):
    """Extract optional tags for an explicitly requested causal evaluation."""

    tags: list[str] = Field(
        default_factory=list,
        max_length=20,
        description="Optional causal rule tags for a rule evaluation action.",
    )


class HypothesisReview(BaseModel):
    """Evidence review for one planned explanation."""

    hypothesis_id: str = Field(
        description=(
            "Copy the exact hypothesis id from valid_hypotheses. Do not use "
            "a claim label, ordinal, or newly invented identifier."
        )
    )
    status: Literal["supported", "weakened", "inconclusive"]
    explanation: str
    evidence_for: list[str] = Field(
        max_length=20,
        description=(
            "Exact evidence IDs from the assessment's valid_evidence_ids "
            "list, such as E001. Never use tool names or result section names."
        ),
    )
    evidence_against: list[str] = Field(
        max_length=20,
        description=(
            "Exact evidence IDs from the assessment's valid_evidence_ids "
            "list, such as E001. Never use tool names or result section names."
        ),
    )


class FindingDraft(BaseModel):
    """A finding that must cite at least one evidence record."""

    statement: str
    confidence: Literal["low", "moderate", "high"]
    evidence_ids: list[str] = Field(
        min_length=1,
        max_length=20,
        description=(
            "Exact evidence IDs from the assessment's valid_evidence_ids "
            "list, such as E001. A nested result section like database_events "
            "is not an evidence ID."
        ),
    )


class EvidenceAssessment(BaseModel):
    """Assessment of the evidence gathered so far."""

    hypothesis_reviews: list[HypothesisReview] = Field(
        description=(
            "One review for each relevant item in valid_hypotheses. "
            "Use an empty list only when there are no hypotheses."
        )
    )
    findings: list[FindingDraft] = Field(
        description=(
            "Evidence-cited user-level observations. Include at least one "
            "when the supplied evidence contains a relevant observation, "
            "even if no cause is established."
        )
    )
    completed_step_ids: list[str] = Field(
        max_length=8,
        description="Only exact IDs from valid_step_ids that the evidence addresses.",
    )
    unresolved_questions: list[str] = Field(
        max_length=10,
        description="Questions the supplied evidence cannot answer.",
    )
    limitations: list[str] = Field(
        max_length=10,
        description="Material limits of the supplied evidence.",
    )
    sufficient_to_answer: bool
    next_steps: list[str] = Field(
        max_length=6,
        description=(
            "Concrete evidence still needed. Use plain descriptions, never "
            "invent plan-step IDs; return an empty list when none is needed."
        ),
    )
