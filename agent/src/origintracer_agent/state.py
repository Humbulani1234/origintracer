"""State carried by one OriginTracer investigation thread."""

from typing import Any, NotRequired

from langchain.agents import AgentState


class InvestigationState(AgentState):
    """Conversation state and structured investigation work for one thread.

    LangGraph checkpoints this state with the conversation.
    A new thread can begin with inherited conversation messages and
    an optional worker selection. The caller supplies the worker PID;
    the model does not choose it.

    Attributes
    ----------
    messages : list[BaseMessage]
        Conversation history inherited from ``AgentState``.
    worker_pid : str or int, optional
        Worker selected for the current run. The caller supplies this PID,
        and worker tools use it to resolve the stored snapshot to inspect.
    worker_instance_id : str, optional
        Stable identity of the selected worker process. This distinguishes a
        restarted process that reuses a PID.
    planned_worker_pid : str, optional
        Worker target resolved by the planner from the user's request or known
        worker inventory. The prepare node verifies it against the backend
        inventory before any worker-scoped tool runs.
    caller_selected_worker_pid : str, optional
        Most recent worker PID supplied by the caller. It remains available as
        the UI-selection reference after an all-worker overview clears the
        active worker scope, and is distinct from a planner-resolved target.
    worker_inventory : list of dict, optional
        Recent worker identities returned by an all-worker overview. This
        supports later references such as another worker while each query
        still verifies the PID against current backend state.
    investigation_pid : str, optional
        PID attached to the saved investigation. A change starts a new
        investigation so evidence from different workers is not combined.
    investigation_instance_id : str, optional
        Worker instance attached to the saved investigation. The graph checks
        it alongside ``investigation_pid`` before reusing prior evidence.
    investigation_id : str, optional
        Stable grouping ID for one investigation. It persists across related
        turns and changes when a fresh investigation starts. LangGraph events
        include it as metadata alongside their trace IDs.
    prior_investigation_id : str, optional
        Saved investigation ID held temporarily while the graph decides
        whether the current request continues that investigation.
    planned_continuation : bool, optional
        Planner's continuity decision, retained until the graph verifies the
        selected worker process instance.
    new_investigation : bool, optional
        Requests that prior investigation work be cleared before the current
        request is handled.
    scope : dict, optional
        Operational scope selected by the planner, including whether a worker
        overview covers all live workers or only the selected worker.
    last_user_message_id : str, optional
        ID of the last user message handled by the graph, used to avoid
        processing the same message twice.
    question : str, optional
        First request that started the active investigation. Prompt context
        exposes this persisted value as ``investigation_origin_question``.
    current_request : str, optional
        Newest user request being handled.
    questions : list[str], optional
        Requests added to the current investigation across related turns.
        The latest item is projected into planner context as
        ``existing_question``; those context names are not separate state
        fields.
    plan : list[dict], optional
        Evidence-gathering steps and their status.
    deliverables : list[dict], optional
        Outputs requested in the current turn.
    deliverable_status : list[dict], optional
        Whether each current-turn output has been answered.
    investigation_deliverables : list[dict], optional
        Requested outputs retained from earlier turns in the investigation.
    investigation_level : str, optional
        Plan type: a direct query, a multi-step investigation, or a comparison
        of competing explanations.
    hypotheses : list[dict], optional
        Possible explanations and the evidence found for or against each one.
    evidence : list[dict], optional
        Tool observations with evidence IDs and source details.
    tool_results : list[dict], optional
        Tool calls and their success or failure outcomes.
    recorded_tool_call_ids : list[str], optional
        Tool call IDs already recorded, used to prevent duplicate results.
    direct_answer : dict, optional
        Concise answer to the latest request and the evidence IDs that
        support it.
    findings : list[dict], optional
        Current-turn observations linked to evidence IDs.
    investigation_findings : list[dict], optional
        Conclusions retained from earlier turns.
    turn_evidence_baseline : list[str], optional
        Evidence IDs already present when the current turn began. The report
        uses this baseline to include new observations and relevant cited prior
        observations without repeating the full investigation.
    unresolved_questions : list[str], optional
        Questions the available evidence has not answered.
    limitations : list[str], optional
        Limits in the available data that affect the conclusions.
    next_steps : list[str], optional
        Useful follow-up checks.
    research_round : int, optional
        Number of evidence-gathering and assessment rounds completed.
    assessment_sufficient : bool, optional
        Whether the gathered evidence supports a report.
    last_research_had_tools : bool, optional
        Whether the latest research step returned tool calls that must be
        recorded before assessment.
    assessment_failure : dict, optional
        Semantic-validation failure that stops report synthesis while
        preserving the gathered evidence.
    response_mode : str, optional
        Whether the latest turn is general chat, an investigation, or an action
        proposal.
    status : str, optional
        Current stage or outcome of the investigation.
    final_report : dict, optional
        Completed response with findings and evidence references.
    proposed_action : dict, optional
        Action and arguments awaiting caller approval.
    action_result : dict, optional
        Result returned after an approved action executes.
    action_status : str, optional
        Whether the proposed action is awaiting approval, approved, cancelled,
        or failed.
    """

    worker_pid: NotRequired[str | int]
    worker_instance_id: NotRequired[str]
    planned_worker_pid: NotRequired[str | int | None]
    caller_selected_worker_pid: NotRequired[str | int | None]
    worker_inventory: NotRequired[list[dict[str, Any]]]
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
