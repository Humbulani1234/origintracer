"""Structured model outputs used by the investigation workflow."""

from typing import Literal

from pydantic import BaseModel, Field


class PlanStep(BaseModel):
    """One question the evidence gathering part of the graph should answer."""

    description: str
    purpose: str
    suggested_tools: list[str] = Field(
        min_length=1,
        max_length=8,
        description=(
            "At least one available read-only tool needed for this step."
        ),
    )


class DeliverableDraft(BaseModel):
    """One user-requested output and the reads that can supply it."""

    description: str = Field(
        description="A concise, observable part of the user's requested answer."
    )
    suggested_tools: list[str] = Field(
        min_length=1,
        max_length=8,
        description="Available read-only tools that provide this output.",
    )


class DeliverableQuestion(BaseModel):
    """
    A material unanswered question attached to a requested output.
    """

    deliverable_id: str
    question: str


class DeliverableNextStep(BaseModel):
    """A necessary follow-up read attached to a requested output."""

    deliverable_id: str
    description: str


class HypothesisDraft(BaseModel):
    """A testable causal claim for a verification investigation."""

    claim: str = Field(
        description=(
            "A plausible explanation of a problem the user asked to diagnose."
        )
    )


class InvestigationPlan(BaseModel):
    """Plan and possible explanations for one user request."""

    continue_existing: bool = Field(
        description=(
            "Required when existing_investigation is true. Return true when "
            "the request depends on, refers to, or adds evidence to that "
            "investigation; return false only for an unrelated new question. "
            "Interpret conversational context semantically, without requiring "
            "particular follow-up wording."
        )
    )
    worker_scope: Literal[
        "not_requested", "all", "selected", "ambiguous"
    ] = Field(
        description=(
            "Always classify scope: interpret an operational overview from "
            "the user's meaning; use not_requested for other requests. "
            "Use all for all live workers, selected for the selected worker, "
            "ambiguous only when context cannot resolve the scope, and "
            "not_requested for requests that are not worker overviews."
        ),
    )
    target_worker_pid: str | None = Field(
        default=None,
        description=(
            "Set only when the current request targets a worker different "
            "from the caller-selected worker. Resolve the PID from the "
            "current request, prior conversation, or supplied worker "
            "inventory. Never invent or infer a PID from ordering unless "
            "the inventory makes that reference unambiguous. The graph "
            "validates the PID against the backend inventory before reads."
        ),
    )
    level: Literal["query", "multi_step", "verification"] = (
        Field(
            description=(
                "Choose verification whenever the user asks to diagnose a problem "
                "or compare possible causes, even when several reads are needed. "
                "Use multi_step for multiple related factual checks that do not "
                "compare causes."
            )
        )
    )
    scope_summary: str
    steps: list[PlanStep] = Field(
        min_length=1,
        max_length=8,
        description=(
            "At least one concrete read-only step that advances the request."
        ),
    )
    deliverables: list[DeliverableDraft] = Field(
        default_factory=list,
        max_length=8,
        description=(
            "Enumerate the distinct outputs explicitly requested by the user. "
            "Each deliverable must be supported by one or more planned reads."
        ),
    )
    hypotheses: list[HypothesisDraft] = Field(
        max_length=6,
        description=(
            "For verification, include testable explanations. If the user "
            "names causes and asks to compare them or gather evidence for and "
            "against each, include each named explanation even if the available "
            "evidence may only support an inconclusive review. Do not invent "
            "unrequested causes. Use an empty list when no causal comparison "
            "is requested and no distinguishable explanations are available."
        ),
    )
    unresolved_questions: list[str] = Field(
        default_factory=list,
        max_length=10,
        description=(
            "Only material ambiguities not resolved by the planned reads; "
            "do not restate the request as a question."
        ),
    )


class ActionIntent(BaseModel):
    """Route ordinary conversation, investigations, or explicit actions."""

    action: Literal[
        "chat",
        "out_of_scope",
        "investigate",
        "record_deployment_marker",
        "evaluate_and_record_rules",
    ] = Field(
        default="investigate",
        description=(
            "Choose chat for greetings, date/time, OriginTracer questions, "
            "observability, and general computer science, computing, and "
            "systems topics. Choose "
            "investigate for live worker inspection or explicit actions. "
            "Choose out_of_scope for unrelated topics."
        ),
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
    """Evidence review aligned by position with a planned explanation."""

    status: Literal["supported", "weakened", "inconclusive"]
    explanation: str
    evidence_for: list[str] = Field(
        default_factory=list,
        max_length=20,
        description=(
            "Exact evidence IDs from the assessment's valid_evidence_ids "
            "list, such as E001. Never use tool names or result section names. "
            "Return an empty list when no evidence supports the claim."
        ),
    )
    evidence_against: list[str] = Field(
        default_factory=list,
        max_length=20,
        description=(
            "Exact evidence IDs from the assessment's valid_evidence_ids "
            "list, such as E001. Never use tool names or result section names. "
            "Return an empty list when no evidence weakens the claim."
        ),
    )


class FindingDraft(BaseModel):
    """An evidence-grounded observation that cites a returned evidence record."""

    statement: str = Field(
        description=(
            "State an observed measurement, rule match, or relationship. "
            "For causal comparisons, do not state a hypothesis as an observed "
            "fact; assess it in hypothesis_reviews. Do not claim cause or "
            "contribution without request-correlated evidence."
        )
    )
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


class DirectAnswerDraft(BaseModel):
    """Concise answer to the user's latest request, grounded in evidence."""

    statement: str = Field(
        default="",
        description=(
            "Answer only the current request in clear, concise language. Use "
            "short labeled lines or bullets for multiple requested categories. "
            "Include every requested list item, but keep unrequested raw "
            "payload detail in findings or evidence. State uncertainty when "
            "the evidence does not settle the question."
        ),
    )
    evidence_ids: list[str] = Field(
        default_factory=list,
        max_length=20,
        description="Evidence IDs supporting the direct answer.",
    )


class EvidenceAssessmentFields(BaseModel):
    """Fields shared by assessments, whether or not hypotheses exist."""

    direct_answer: DirectAnswerDraft = Field(
        default_factory=DirectAnswerDraft,
        description=(
            "A concise, self-contained answer to current_request, grounded in "
            "the cited evidence and hypothesis reviews."
        ),
    )
    findings: list[FindingDraft] = Field(
        description=(
            "Evidence-cited observations of measurements, rule matches, or "
            "relationships. Include relevant observations even when no cause "
            "is established. In a causal comparison, keep explanations and "
            "their support in hypothesis_reviews rather than asserting them "
            "as findings."
        )
    )
    answered_deliverable_ids: list[str] = Field(
        default_factory=list,
        description="Requested output IDs fully answered by the supplied evidence.",
    )
    unanswered_deliverable_ids: list[str] = Field(
        default_factory=list,
        description="Requested output IDs still materially unanswered.",
    )
    completed_step_ids: list[str] = Field(
        max_length=8,
        description="Only exact IDs from valid_step_ids that the evidence addresses.",
    )
    unresolved_questions: list[str] = Field(
        max_length=10,
        description=(
            "Only unanswered questions whose answers are necessary to "
            "complete the user's request; exclude optional future inquiry."
        ),
    )
    limitations: list[str] = Field(
        max_length=10,
        description=(
            "Material evidence limits that affect the requested conclusion; "
            "do not restate general properties of a metric as a limitation."
        ),
    )
    next_steps: list[str] = Field(
        max_length=6,
        description=(
            "Only evidence gathering necessary to complete the request or "
            "explicitly requested by the user. Return an empty list when "
            "the request is answered; optional future investigation is not "
            "a next step. Never invent plan-step IDs."
        ),
    )


class EvidenceAssessment(EvidenceAssessmentFields):
    """Assessment that reviews each supplied hypothesis in order."""

    hypothesis_reviews: list[HypothesisReview] = Field(
        description=(
            "Return exactly one review for each supplied hypothesis, in the "
            "same order. Do not add identifiers or extra reviews."
        )
    )


class EvidenceAssessmentWithoutHypotheses(
    EvidenceAssessmentFields
):
    """Assessment schema for requests with no hypotheses to review."""
