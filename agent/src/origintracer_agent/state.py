"""State carried by one OriginTracer investigation thread."""

from typing import Any, NotRequired

from langchain.agents import AgentState


class InvestigationState(AgentState):
    """
    Conversation state plus the structured work for an investigation.

    Agent Server checkpoints this state with the conversation thread. The
    process ID is supplied by the caller and is never selected by the model.

    State fields
    ------------
    ``messages`` is inherited from ``AgentState`` and holds the conversation.
    The fields below are optional so a new thread can start with only a user
    message and a selected worker.
    * ``worker_pid`` is the worker selected for the current run. The caller
      supplies it; tools use it to query that worker.
    * ``worker_instance_id`` identifies the selected worker process instance.
      It distinguishes a restarted worker that happens to reuse the same PID.
    * ``investigation_pid`` is the worker attached to the saved investigation.
      A change starts a new investigation so evidence from different workers
      is not combined.
    * ``investigation_instance_id`` identifies the worker instance attached to
      the saved investigation and is checked alongside ``investigation_pid``
      before prior evidence is reused.
    * ``investigation_id`` is a stable grouping ID for one investigation. It
      persists across related turns and changes when a fresh investigation
      starts; LangGraph events carry it as metadata alongside their trace IDs.
    * ``prior_investigation_id`` temporarily holds the saved ID while the graph
      decides whether the current request continues that investigation. The
      planner clears it after making that decision.
    * ``planned_continuation`` records the planner's continuity decision until
        the graph has verified the current worker process instance.
    * ``new_investigation`` asks the graph to clear prior investigation work
        before handling the current request.
    * ``scope`` records the planner's operational scope decision, including
        whether an overview covers all live workers or the selected worker.
    * ``last_user_message_id`` records the last user message handled by the
        graph so the same message is not processed twice.
    * ``question`` is the original question for the active investigation.
      ``current_request`` is the newest request being handled, and
      ``questions`` keeps the requests added to this investigation.
    * ``scope`` records what the investigation covers, including its worker.
    * ``plan`` contains the evidence gathering steps and their status.
    * ``deliverables`` lists outputs requested in the current turn, and
      ``deliverable_status`` records whether each output is answered or unanswered.
      ``investigation_deliverables`` retains prior turn outputs.
      ``investigation_level`` records whether the plan is a direct query, a multi-step
        investigation, or a check of competing explanations.
    * ``hypotheses`` contains possible explanations and the evidence found for
      or against each one.
    * ``evidence`` contains tool observations with IDs and source details.
      ``tool_results`` keeps the corresponding tool calls and their outcomes.
      ``recorded_tool_call_ids`` prevents a tool result from being recorded
      more than once.
    * ``direct_answer`` contains the assessor's concise answer to the latest
      request and the evidence IDs that support it. ``findings`` contains
      current-turn observations linked to evidence IDs;
      ``investigation_findings`` retains conclusions from earlier turns.
      ``turn_evidence_baseline`` marks evidence already present when this turn
      began so the response can include new observations and relevant cited
      prior observations without repeating the entire investigation.
      ``unresolved_questions`` lists what the evidence has not answered;
      ``limitations`` records limits in the available data; and
      ``next_steps`` lists useful follow-up checks.
    * ``research_round`` counts evidence gathering and assessment rounds.
      ``assessment_sufficient`` says whether the evidence supports a report;
      ``last_research_had_tools`` tells the graph whether tool results need
      recording before assessment.
    * ``assessment_failure`` records a semantic-validation failure that stops
      report synthesis while preserving gathered evidence.
    * ``response_mode`` records whether the latest turn is general chat,
      an investigation, or an action proposal.
    * ``status`` records the current stage or outcome of the investigation.
      ``final_report`` contains the completed, evidence-linked response.
    * ``proposed_action`` holds an action and its arguments while it waits for
      approval. ``action_result`` records the outcome after approval, and
      ``action_status`` records whether it was awaiting approval, approved,
      cancelled, or failed.
    """

    worker_pid: NotRequired[str | int]
    worker_instance_id: NotRequired[str]
    investigation_pid: NotRequired[str]
    investigation_instance_id: NotRequired[str]
    investigation_id: NotRequired[str]
    prior_investigation_id: NotRequired[str]
    planned_continuation: NotRequired[bool]
    new_investigation: NotRequired[bool]
    last_user_message_id: NotRequired[str]
    question: NotRequired[str]
    current_request: NotRequired[str]
    questions: NotRequired[list[str]]
    scope: NotRequired[dict[str, Any]]
    plan: NotRequired[list[dict[str, Any]]]
    deliverables: NotRequired[list[dict[str, Any]]]
    investigation_deliverables: NotRequired[list[dict[str, Any]]]
    deliverable_status: NotRequired[list[dict[str, Any]]]
    investigation_level: NotRequired[str]
    hypotheses: NotRequired[list[dict[str, Any]]]
    evidence: NotRequired[list[dict[str, Any]]]
    tool_results: NotRequired[list[dict[str, Any]]]
    recorded_tool_call_ids: NotRequired[list[str]]
    direct_answer: NotRequired[dict[str, Any]]
    findings: NotRequired[list[dict[str, Any]]]
    investigation_findings: NotRequired[list[dict[str, Any]]]
    turn_evidence_baseline: NotRequired[list[str]]
    unresolved_questions: NotRequired[list[str]]
    limitations: NotRequired[list[str]]
    next_steps: NotRequired[list[str]]
    research_round: NotRequired[int]
    assessment_sufficient: NotRequired[bool]
    assessment_failure: NotRequired[dict[str, Any]]
    last_research_had_tools: NotRequired[bool]
    status: NotRequired[str]
    response_mode: NotRequired[str]
    final_report: NotRequired[dict[str, Any]]
    proposed_action: NotRequired[dict[str, Any]]
    action_result: NotRequired[dict[str, Any]]
    action_status: NotRequired[str]
