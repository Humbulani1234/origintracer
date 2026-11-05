"""Run the evaluation cases against the OriginTracer Agent Server graph."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langgraph_sdk import get_sync_client
from openevals.llm import create_llm_as_judge

from origintracer_agent.config import (
    configured_chat_model,
)

AGENT_DIR = Path(__file__).resolve().parents[1]
CASES_PATH = Path(__file__).with_name("cases.json")
GRAPH_ID = "origintracer_investigator"

_QUALITY_PROMPT = """
You are reviewing one OriginTracer agent evaluation. Score whether the agent
met the stated criterion using only the user request and the agent output.

Return a passing score only when the answer is supported by the state and
evidence shown in the output, responds to the request, and reports uncertainty
when the output shows that evidence is missing. For an action request, check
that the action is clearly presented as waiting for approval and not completed,
unless interactive output shows an interrupt followed by explicit approval; in
that case grade the reported action result and approval state.
For multi-turn requests, evaluate the latest turn as the task and use earlier
turns as context. The report summary must directly answer the latest request
in concise, understandable language before the evidence details; do not make
the user infer the answer from findings. Do not reward invented facts or claims
that an action ran when it is pending.
Verify every material finding against its cited evidence. A citation that
exists is insufficient if the evidence does not support the particular number,
comparison, or inference. Fail the answer for material unsupported claims even
when it also contains a correct answer to the request.
When duration metrics include both averages and totals, check that per-call
averages are distinguished from totals aggregated across calls. Do not accept
claims that equate an aggregate total with one request's latency or infer
causation from worker-wide patterns without request-level correlation. A null
or zero duration is not evidence that a component is healthy or had no effect.
An unmatched causal rule means the pattern was not detected in that result; it
does not prove the behavior was absent. Event timestamps are not durations.
Events returned by a bounded query establish only what appears in that sample;
absence from the sample does not establish general absence or an alternative
cause. Do not infer an external dependency, latency cause, or unreported timing
from missing events.
For multi-worker results, verify each statement against the matching PID's
own status, graph, and rule rows. Fail worker-to-worker field leakage, claims
of “similar” or “identical” patterns without a field-by-field basis, and using
a node name as proof of a semantic pattern. Never accept multiplying an average
duration by a call count as per-user or per-request impact.
Do not add plausible causes, thresholds, or measurements that are absent from
the request, criterion, and agent output. Missing evidence means the cause is
unknown; it does not imply that an unobserved alternative must be responsible.

<request>
{inputs}
</request>

<criterion>
{reference_outputs}
</criterion>

<agent_output>
{outputs}
</agent_output>
""".strip()


def _read_cases() -> list[dict[str, Any]]:
    """Load the shared cases used by local and LangSmith evaluation."""
    with CASES_PATH.open(encoding="utf-8") as cases_file:
        cases = json.load(cases_file)

    for case in cases:
        turns = case.get("inputs", {}).get("turns", [])
        case["inputs"]["turns"] = [
            _case_text(turn) for turn in turns
        ]
        criterion = case.get("outputs", {}).get(
            "quality_criteria"
        )
        if criterion is not None:
            case["outputs"]["quality_criteria"] = _case_text(
                criterion
            )
    return cases


def _case_text(value: Any) -> str:
    """Read a case string, including its line-wrapped JSON form."""
    if isinstance(value, dict) and isinstance(
        value.get("lines"), list
    ):
        return " ".join(
            str(line).strip() for line in value["lines"]
        ).strip()
    return str(value)


def _as_text(content: Any) -> str:
    """Read text from a serialized LangChain message content value."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            str(block.get("text", ""))
            for block in content
            if isinstance(block, dict)
            and block.get("type") == "text"
        )
    return ""


def _state_values(state: dict[str, Any]) -> dict[str, Any]:
    """Return the thread's graph values in a dictionary form."""
    values = state.get("values", {})
    if isinstance(values, list):
        return (
            values[-1]
            if values and isinstance(values[-1], dict)
            else {}
        )
    return values if isinstance(values, dict) else {}


def _evidence_preview(evidence: Any) -> list[dict[str, Any]]:
    """Keep useful evidence context while bounding evaluator input size."""
    if not isinstance(evidence, list):
        return []
    previews = []
    for item in evidence[-8:]:
        if not isinstance(item, dict):
            continue
        raw_result = item.get("result")
        result = json.dumps(
            raw_result, ensure_ascii=False, default=str
        )
        preview = {
            "evidence_id": item.get("evidence_id"),
            "tool": item.get("tool"),
            "source": item.get("source"),
            "query": item.get("query"),
            "result_excerpt": result[:3_000],
        }
        if isinstance(raw_result, dict):
            registered = raw_result.get(
                "registered_causal_rules",
                raw_result.get("registered_rules"),
            )
            if isinstance(registered, dict):
                preview["registered_rules"] = registered.get(
                    "data", []
                )
            matches = raw_result.get(
                "causal_rule_matches", raw_result.get("matches")
            )
            if isinstance(matches, dict):
                preview["rule_matches"] = matches.get("data", [])
        previews.append(preview)
    return previews


def _format_output(state: dict[str, Any]) -> dict[str, Any]:
    """Extract the answer, evidence, tool path, and action state for grading."""
    values = _state_values(state)
    messages = values.get("messages", [])
    tool_names: list[str] = []
    answer = ""

    for message in messages:
        if not isinstance(message, dict):
            continue
        for call in message.get("tool_calls", []):
            if isinstance(call, dict) and call.get("name"):
                tool_names.append(str(call["name"]))
        if message.get("type") == "tool" and message.get("name"):
            tool_names.append(str(message["name"]))
        if message.get("type") == "ai" and not message.get(
            "tool_calls"
        ):
            text = _as_text(message.get("content"))
            if text:
                answer = text

    tasks = state.get("tasks", [])
    interrupts = [
        interrupt
        for task in tasks
        if isinstance(task, dict)
        for interrupt in task.get("interrupts", [])
    ]
    evidence = values.get("evidence", [])
    findings = values.get("findings", [])
    final_report = values.get("final_report", {})
    observations = (
        final_report.get("observations", [])
        if isinstance(final_report, dict)
        else []
    )

    return {
        "answer": answer,
        "status": values.get("status", "unknown"),
        "response_mode": values.get("response_mode"),
        "research_round": values.get("research_round"),
        "plan_steps": [
            {
                "id": step.get("id"),
                "description": step.get("description"),
                "status": step.get("status"),
                "suggested_tools": step.get(
                    "suggested_tools", []
                ),
            }
            for step in values.get("plan", [])
            if isinstance(step, dict)
        ],
        "pending_graph_nodes": state.get("next", []),
        "graph_task_errors": [
            str(task.get("error"))
            for task in state.get("tasks", [])
            if isinstance(task, dict) and task.get("error")
        ],
        "action_status": values.get("action_status"),
        "proposed_action": values.get("proposed_action"),
        "action_result": values.get("action_result"),
        "interrupts": interrupts,
        "tool_names": sorted(set(tool_names)),
        "evidence_count": (
            len(evidence) if isinstance(evidence, list) else 0
        ),
        "evidence_ids": [
            str(item.get("evidence_id"))
            for item in evidence
            if isinstance(item, dict) and item.get("evidence_id")
        ],
        "evidence": _evidence_preview(evidence),
        "findings": (
            findings if isinstance(findings, list) else []
        ),
        "final_report": final_report,
        "observation_tools": sorted(
            {
                str(item.get("tool"))
                for item in observations
                if isinstance(item, dict) and item.get("tool")
            }
        ),
        "hypothesis_count": len(values.get("hypotheses", [])),
        "question_count": len(values.get("questions", [])),
        "investigation_level": values.get("investigation_level"),
        "assessment_sufficient": values.get(
            "assessment_sufficient"
        ),
        "assessment_failure": values.get("assessment_failure"),
    }


_LEAVE_INTERRUPTED = object()


def _pending_interrupts(
    state: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract human-input payloads from a LangGraph thread snapshot."""
    return [
        item
        for task in state.get("tasks", [])
        if isinstance(task, dict)
        for item in task.get("interrupts", [])
        if isinstance(item, dict)
    ]


def _terminal_resume_value(payload: Any) -> Any:
    """Prompt for one interrupt response; return sentinel to leave it paused."""
    print("\nGraph paused for input:")
    print(
        json.dumps(
            payload, indent=2, ensure_ascii=False, default=str
        )
    )
    kind = (
        payload.get("type")
        if isinstance(payload, dict)
        else None
    )
    if kind == "origintracer_action_approval":
        answer = (
            input(
                "Type APPROVE to run this action, REJECT to cancel, "
                "or press Enter to leave it pending: "
            )
            .strip()
            .casefold()
        )
        if not answer:
            return _LEAVE_INTERRUPTED
        if answer in {"approve", "approved", "yes", "y"}:
            return {"approved": True}
        if answer in {"reject", "rejected", "no", "n"}:
            return {"approved": False}
        print(
            "Unrecognized response; leaving the action pending."
        )
        return _LEAVE_INTERRUPTED

    raw = input(
        "Resume value (JSON or plain text; Enter leaves it paused): "
    ).strip()
    if not raw:
        return _LEAVE_INTERRUPTED
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def make_target(
    keep_threads: bool, *, interactive: bool = False
):
    """Run cases against Agent Server, optionally answering graph interrupts."""
    server_url = os.getenv(
        "ORIGINTRACER_AGENT_SERVER_URL", "http://localhost:2024"
    )
    worker_pid = os.getenv("ORIGINTRACER_EVAL_WORKER_PID")
    client = get_sync_client(url=server_url)

    def target(inputs: dict[str, Any]) -> dict[str, Any]:
        case_id = inputs.get("case_id", "evaluation")
        selected_pid = inputs.get("worker_pid")
        if selected_pid is None and inputs.get(
            "requires_worker", False
        ):
            selected_pid = worker_pid
        if (
            inputs.get("requires_worker")
            and not selected_pid
            and interactive
        ):
            selected_pid = input(
                f"{case_id}: enter a PID with a stored worker snapshot: "
            ).strip()
        if inputs.get("requires_worker") and not selected_pid:
            raise ValueError(
                "Set ORIGINTRACER_EVAL_WORKER_PID to a PID with a stored "
                "worker snapshot or use "
                "--interactive to enter one for case "
                f"{case_id!r}."
            )
        if selected_pid and not str(selected_pid).isdigit():
            raise ValueError(
                "Worker PID must contain digits only."
            )

        thread = client.threads.create(
            metadata={"origintracer_eval_case": str(case_id)}
        )
        thread_id = str(thread["thread_id"])
        observed_interrupts: list[dict[str, Any]] = []
        resumed = False
        interactive_resumed = False
        scripted_interrupts = list(
            inputs.get("interrupt_responses", [])
        )
        preserve_pending_thread = False
        try:
            run_result: dict[str, Any] = {}
            for turn in inputs["turns"]:
                graph_input: dict[str, Any] = {
                    "messages": [
                        {"role": "user", "content": turn}
                    ]
                }
                if selected_pid:
                    graph_input["worker_pid"] = str(selected_pid)
                completed_run = client.runs.wait(
                    thread_id,
                    GRAPH_ID,
                    input=graph_input,
                    multitask_strategy="reject",
                )
                if isinstance(completed_run, dict):
                    run_result = completed_run

                while True:
                    thread_state = client.threads.get_state(
                        thread_id
                    )
                    pending = _pending_interrupts(thread_state)
                    if not pending:
                        break
                    interrupt_value = pending[0].get(
                        "value", pending[0]
                    )
                    if pending[0] not in observed_interrupts:
                        observed_interrupts.extend(pending)
                    kind = (
                        interrupt_value.get("type")
                        if isinstance(interrupt_value, dict)
                        else None
                    )
                    if interactive:
                        resume_value = _terminal_resume_value(
                            interrupt_value
                        )
                    elif (
                        kind
                        == "origintracer_worker_overview_scope"
                        and scripted_interrupts
                    ):
                        resume_value = scripted_interrupts.pop(0)
                    else:
                        break
                    if resume_value is _LEAVE_INTERRUPTED:
                        break
                    completed_run = client.runs.wait(
                        thread_id,
                        GRAPH_ID,
                        input={
                            "command": {"resume": resume_value}
                        },
                        multitask_strategy="reject",
                    )
                    resumed = True
                    if interactive:
                        interactive_resumed = True
                    if isinstance(completed_run, dict):
                        run_result = completed_run

            state = client.threads.get_state(thread_id)
            output = _format_output(state)
            preserve_pending_thread = bool(
                interactive and _pending_interrupts(state)
            )
            if preserve_pending_thread:
                output["thread_id"] = thread_id
            if observed_interrupts:
                output["interrupts"] = observed_interrupts
            output["interactive"] = interactive
            output["interactive_resumed"] = interactive_resumed
            output["resumed"] = resumed
            output["agent_run_status"] = run_result.get("status")
            if run_result.get("error"):
                output["agent_run_error"] = run_result["error"]
            return output
        finally:
            if not keep_threads and not preserve_pending_thread:
                client.threads.delete(thread_id)

    return target


def pathway_evaluator(
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    reference_outputs: dict[str, Any],
) -> dict[str, Any]:
    """Check graph status, tool use, evidence, and action approval state."""
    checks: list[tuple[bool, str]] = []
    expected_status = reference_outputs.get("expected_status")
    interactive_statuses = reference_outputs.get(
        "interactive_expected_statuses", []
    )
    if (
        outputs.get("interactive")
        and reference_outputs.get("must_be_interrupted")
        and not interactive_statuses
    ):
        interactive_statuses = [
            expected_status,
            "action_cancelled",
            "completed",
        ]
    if outputs.get("interactive") and interactive_statuses:
        checks.append(
            (
                outputs.get("status") in interactive_statuses,
                "interactive status expected one of "
                f"{interactive_statuses!r}, got {outputs.get('status')!r}",
            )
        )
    elif expected_status:
        checks.append(
            (
                outputs.get("status") == expected_status,
                f"status expected {expected_status!r}, got {outputs.get('status')!r}",
            )
        )
    expected_response_mode = reference_outputs.get(
        "expected_response_mode"
    )
    if expected_response_mode:
        checks.append(
            (
                outputs.get("response_mode")
                == expected_response_mode,
                "response mode expected "
                f"{expected_response_mode!r}, got "
                f"{outputs.get('response_mode')!r}",
            )
        )
    if reference_outputs.get("must_not_use_tools"):
        checks.append(
            (
                not outputs.get("tool_names")
                and outputs.get("evidence_count", 0) == 0,
                "general chat must not call worker tools or record evidence",
            )
        )
    if outputs.get("assessment_failure"):
        checks.append(
            (
                outputs.get("status") == "assessment_failed"
                and not outputs.get("final_report"),
                "assessment failures must stop before final report synthesis",
            )
        )

    tool_names = set(outputs.get("tool_names", []))
    required_tools = set(
        reference_outputs.get("required_tools", [])
    )
    if required_tools:
        missing = sorted(required_tools - tool_names)
        checks.append(
            (not missing, f"required tools missing: {missing}")
        )
    any_tools = set(reference_outputs.get("any_tools", []))
    if any_tools:
        checks.append(
            (
                bool(any_tools & tool_names),
                f"expected one of {sorted(any_tools)}, got {sorted(tool_names)}",
            )
        )

    required_observations = set(
        reference_outputs.get("required_observation_tools", [])
    )
    if required_observations:
        missing_observations = sorted(
            required_observations
            - set(outputs.get("observation_tools", []))
        )
        checks.append(
            (
                not missing_observations,
                "required evidence summaries missing: "
                f"{missing_observations}",
            )
        )
    for requirement in reference_outputs.get(
        "evidence_query_requirements", []
    ):
        if not isinstance(requirement, dict):
            continue
        expected_tool = str(requirement.get("tool", ""))
        candidates = [
            item
            for item in outputs.get("evidence", [])
            if isinstance(item, dict)
            and item.get("tool") == expected_tool
        ]
        required_terms = [
            str(term).casefold()
            for term in requirement.get("contains", [])
        ]
        forbidden_terms = [
            str(term).casefold()
            for term in requirement.get("not_contains", [])
        ]
        matches = any(
            all(
                term in str(item.get("query", "")).casefold()
                for term in required_terms
            )
            and not any(
                term in str(item.get("query", "")).casefold()
                for term in forbidden_terms
            )
            for item in candidates
        )
        checks.append(
            (
                matches,
                f"no {expected_tool} evidence query matched the required "
                "filter semantics",
            )
        )
    any_observations = set(
        reference_outputs.get(
            "required_observation_any_tools", []
        )
    )
    if any_observations:
        available_observations = set(
            outputs.get("observation_tools", [])
        )
        checks.append(
            (
                bool(any_observations & available_observations),
                "expected a report observation from one of "
                f"{sorted(any_observations)}, got "
                f"{sorted(available_observations)}",
            )
        )
    forbidden_observations = set(
        reference_outputs.get("forbidden_observation_tools", [])
    )
    if forbidden_observations:
        unexpected = sorted(
            forbidden_observations
            & set(outputs.get("observation_tools", []))
        )
        checks.append(
            (
                not unexpected,
                "unexpected report observations from tools: "
                f"{unexpected}",
            )
        )

    if expected_status == "completed" and outputs.get(
        "response_mode"
    ) not in {"chat", "out_of_scope"}:
        report = outputs.get("final_report")
        report_fields = {
            "answer",
            "summary",
            "summary_evidence_ids",
            "findings",
            "observations",
            "hypotheses",
            "unresolved_questions",
            "limitations",
            "next_steps",
        }
        has_report = isinstance(
            report, dict
        ) and report_fields.issubset(report)
        checks.append(
            (
                has_report,
                "completed run must include the report structure",
            )
        )
        if has_report:
            known_ids = set(outputs.get("evidence_ids", []))
            answer = report.get("answer")
            answer_present = isinstance(answer, str) and bool(
                answer.strip()
            )
            checks.append(
                (
                    answer_present,
                    "completed report must include a direct answer",
                )
            )
            summary_evidence_ids = report.get(
                "summary_evidence_ids", []
            )
            summary_citations_valid = (
                isinstance(summary_evidence_ids, list)
                and set(summary_evidence_ids).issubset(known_ids)
                and (
                    outputs.get("evidence_count", 0) == 0
                    or bool(summary_evidence_ids)
                )
            )
            checks.append(
                (
                    summary_citations_valid,
                    "report summary must cite valid evidence when evidence exists",
                )
            )
            observations = report.get("observations", [])
            observation_rows_valid = (
                isinstance(observations, list)
                and (
                    outputs.get("evidence_count", 0) == 0
                    or bool(observations)
                )
                and all(
                    isinstance(item, dict)
                    and item.get("evidence_id") in known_ids
                    and item.get("tool")
                    and item.get("summary")
                    for item in observations
                )
            )
            checks.append(
                (
                    observation_rows_valid,
                    "every report observation must summarize a recorded evidence ID",
                )
            )
            report_findings = report.get("findings", [])
            report_findings_valid = isinstance(
                report_findings, list
            ) and all(
                isinstance(item, dict)
                and item.get("evidence_ids")
                and set(item["evidence_ids"]).issubset(known_ids)
                for item in report_findings
            )
            checks.append(
                (
                    report_findings_valid,
                    "report findings must cite recorded evidence IDs",
                )
            )
            for field in ("unresolved_questions", "next_steps"):
                maximum = reference_outputs.get(f"max_{field}")
                if maximum is not None:
                    actual = report.get(field, [])
                    checks.append(
                        (
                            isinstance(actual, list)
                            and len(actual) <= int(maximum),
                            f"{field} count expected at most {maximum}, "
                            f"got {len(actual) if isinstance(actual, list) else 'invalid'}",
                        )
                    )

    if reference_outputs.get("must_be_sufficient_to_answer"):
        checks.append(
            (
                outputs.get("assessment_sufficient") is True,
                "assessment must mark the request answerable from gathered evidence",
            )
        )

    min_evidence = int(reference_outputs.get("min_evidence", 0))
    checks.append(
        (
            outputs.get("evidence_count", 0) >= min_evidence,
            f"evidence count expected at least {min_evidence}, "
            f"got {outputs.get('evidence_count', 0)}",
        )
    )
    max_evidence = reference_outputs.get("max_evidence")
    if max_evidence is not None:
        checks.append(
            (
                outputs.get("evidence_count", 0)
                <= int(max_evidence),
                f"evidence count expected at most {max_evidence}, "
                f"got {outputs.get('evidence_count', 0)}",
            )
        )
    required_evidence_terms = [
        str(term).casefold()
        for term in reference_outputs.get(
            "evidence_must_contain", []
        )
    ]
    if required_evidence_terms:
        evidence_text = json.dumps(
            outputs.get("evidence", []), ensure_ascii=False
        ).casefold()
        missing_terms = [
            term
            for term in required_evidence_terms
            if term not in evidence_text
        ]
        checks.append(
            (
                not missing_terms,
                f"registered rule names missing from evidence: {missing_terms}",
            )
        )
    min_hypotheses = int(
        reference_outputs.get("min_hypotheses", 0)
    )
    checks.append(
        (
            outputs.get("hypothesis_count", 0) >= min_hypotheses,
            f"hypothesis count expected at least {min_hypotheses}, "
            f"got {outputs.get('hypothesis_count', 0)}",
        )
    )
    max_hypotheses = reference_outputs.get("max_hypotheses")
    if max_hypotheses is not None:
        checks.append(
            (
                outputs.get("hypothesis_count", 0)
                <= int(max_hypotheses),
                f"hypothesis count expected at most {max_hypotheses}, "
                f"got {outputs.get('hypothesis_count', 0)}",
            )
        )
    min_findings = int(reference_outputs.get("min_findings", 0))
    if min_findings:
        checks.append(
            (
                len(outputs.get("findings", [])) >= min_findings,
                f"finding count expected at least {min_findings}, "
                f"got {len(outputs.get('findings', []))}",
            )
        )
    min_questions = int(
        reference_outputs.get("min_questions", 0)
    )
    if min_questions:
        checks.append(
            (
                outputs.get("question_count", 0)
                >= min_questions,
                f"question count expected at least {min_questions}, "
                f"got {outputs.get('question_count', 0)}",
            )
        )
    max_questions = reference_outputs.get("max_questions")
    if max_questions is not None:
        checks.append(
            (
                outputs.get("question_count", 0)
                <= int(max_questions),
                f"question count expected at most {max_questions}, "
                f"got {outputs.get('question_count', 0)}",
            )
        )
    if reference_outputs.get("findings_need_evidence"):
        evidence_ids = set(outputs.get("evidence_ids", []))
        findings = outputs.get("findings", [])
        cited = all(
            bool(finding.get("evidence_ids"))
            and set(finding["evidence_ids"]).issubset(
                evidence_ids
            )
            for finding in findings
        )
        checks.append(
            (
                cited,
                "every finding must cite recorded evidence IDs",
            )
        )

    expected_action = reference_outputs.get("expected_action")
    if expected_action:
        proposal = outputs.get("proposed_action") or {}
        checks.append(
            (
                proposal.get("name") == expected_action,
                f"expected pending action {expected_action!r}, "
                f"got {proposal.get('name')!r}",
            )
        )
    if reference_outputs.get("must_not_propose_action"):
        checks.append(
            (
                outputs.get("proposed_action") is None
                and outputs.get("action_status") is None,
                "an investigation request must not create an action proposal",
            )
        )
    if reference_outputs.get("action_requires_worker_pid"):
        arguments = (outputs.get("proposed_action") or {}).get(
            "arguments", {}
        )
        worker_pid = arguments.get("worker_pid")
        checks.append(
            (
                isinstance(worker_pid, str)
                and worker_pid.isdigit()
                and int(worker_pid) > 0,
                "deployment marker proposal must include the selected worker PID",
            )
        )
    if reference_outputs.get("must_be_interrupted"):
        checks.append(
            (
                bool(outputs.get("interrupts")),
                "expected the graph to pause for approval",
            )
        )
    if reference_outputs.get("must_not_execute_action") and not (
        outputs.get("interactive")
        and outputs.get("interactive_resumed")
    ):
        checks.append(
            (
                outputs.get("action_status")
                == "awaiting_approval"
                and outputs.get("action_result") is None,
                "the action must remain pending and must not have run",
            )
        )
    elif outputs.get("interactive_resumed") and expected_action:
        checks.append(
            (
                outputs.get("action_status")
                in {"completed", "cancelled"},
                "resumed action must report completion or cancellation",
            )
        )

    failed = [
        message for passed, message in checks if not passed
    ]
    return {
        "key": "graph_pathway",
        "score": not failed,
        "comment": (
            "All graph checks passed."
            if not failed
            else "; ".join(failed)
        ),
    }


def make_quality_evaluator():
    """Build an OpenEvals judge using the configured model provider."""
    judge_model = os.getenv(
        "ORIGINTRACER_EVAL_JUDGE_MODEL",
        os.getenv("ORIGINTRACER_AGENT_MODEL", "qwen3:4b"),
    )
    judge = configured_chat_model(judge_model, temperature=0)
    evaluator = create_llm_as_judge(
        prompt=_QUALITY_PROMPT,
        judge=judge,
        feedback_key="evidence_grounded_quality",
    )

    def quality_evaluator(
        inputs: dict[str, Any],
        outputs: dict[str, Any],
        reference_outputs: dict[str, Any],
    ) -> dict[str, Any]:
        if outputs.get("assessment_failure"):
            return {
                "key": "evidence_grounded_quality",
                "score": False,
                "comment": (
                    "The graph stopped with an assessment failure; no "
                    "completed report is available to grade."
                ),
            }
        if (
            reference_outputs.get("expected_status")
            == "completed"
            and reference_outputs.get("expected_response_mode")
            not in {"chat", "out_of_scope"}
            and (
                outputs.get("status") != "completed"
                or not isinstance(
                    outputs.get("final_report"), dict
                )
                or not outputs.get("final_report")
            )
        ):
            return {
                "key": "evidence_grounded_quality",
                "score": False,
                "comment": (
                    "The graph did not produce a completed final report; "
                    "the quality judge will not grade intermediate state. "
                    f"Graph status: {outputs.get('status')!r}; "
                    f"Agent Server run status: "
                    f"{outputs.get('agent_run_status')!r}."
                ),
            }
        return evaluator(
            inputs=inputs,
            outputs=outputs,
            reference_outputs=reference_outputs.get(
                "quality_criteria", ""
            ),
        )

    return quality_evaluator


def _with_case_ids(
    cases: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Format the shared JSON cases for the LangSmith evaluator interface."""
    return [
        {
            "inputs": {**case["inputs"], "case_id": case["id"]},
            "outputs": case["outputs"],
        }
        for case in cases
    ]


def run_local(
    cases: list[dict[str, Any]],
    target: Any,
    quality_evaluator: Any,
) -> int:
    """Run the graph and both evaluators without uploading results."""
    rows = []
    for case in cases:
        inputs = {**case["inputs"], "case_id": case["id"]}
        try:
            outputs = target(inputs)
            pathway = pathway_evaluator(
                inputs, outputs, case["outputs"]
            )
            quality = quality_evaluator(
                inputs, outputs, case["outputs"]
            )
            passed = bool(pathway["score"]) and bool(
                quality.get("score")
            )
            rows.append(
                {
                    "case_id": case["id"],
                    "passed": passed,
                    "outputs": outputs,
                    "evaluations": [pathway, quality],
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "case_id": case["id"],
                    "passed": False,
                    "error": str(exc),
                }
            )

    print(json.dumps(rows, indent=2, default=str))
    passed_count = sum(bool(row["passed"]) for row in rows)
    print(
        f"\nPassed {passed_count} of {len(rows)} evaluation cases."
    )
    return 0 if passed_count == len(rows) else 1


def run_langsmith(
    cases: list[dict[str, Any]],
    target: Any,
    quality_evaluator: Any,
) -> int:
    """Upload one experiment to LangSmith using the same graph and graders."""
    if not os.getenv("LANGSMITH_API_KEY"):
        raise ValueError(
            "Set LANGSMITH_API_KEY to use LangSmith evaluation mode."
        )
    from langsmith import evaluate

    results = evaluate(
        target,
        data=_with_case_ids(cases),
        evaluators=[pathway_evaluator, quality_evaluator],
        experiment_prefix="origintracer-agent-graph-evals",
        description=(
            "Offline evaluation cases run against the OriginTracer Agent Server graph."
        ),
        max_concurrency=1,
    )
    print(results)
    return 0


def main() -> int:
    """Run local graders or record the same evaluation as a LangSmith experiment."""
    load_dotenv(AGENT_DIR / ".env.example", override=False)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("local", "langsmith"),
        default="local",
        help="Run locally without uploading results, or record them in LangSmith.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help=(
            "Prompt for a worker PID when needed and answer LangGraph interrupts "
            "in the terminal. Action approvals require an explicit APPROVE."
        ),
    )
    parser.add_argument(
        "--keep-threads",
        action="store_true",
        help="Keep the evaluation threads on the Agent Server for inspection.",
    )
    parser.add_argument(
        "--case",
        action="append",
        dest="case_ids",
        metavar="ID",
        help=(
            "Run only this case ID. Repeat the option to select multiple "
            "cases; omit it to run the full suite."
        ),
    )
    args = parser.parse_args()

    if args.mode == "local":
        os.environ["LANGSMITH_TRACING"] = "false"
    elif args.interactive:
        parser.error(
            "--interactive is available only with --mode local."
        )

    cases = _read_cases()
    if args.case_ids:
        cases_by_id = {case["id"]: case for case in cases}
        unknown_ids = sorted(
            set(args.case_ids) - set(cases_by_id)
        )
        if unknown_ids:
            valid_ids = ", ".join(sorted(cases_by_id))
            parser.error(
                f"unknown case ID(s): {', '.join(unknown_ids)}. "
                f"Available IDs: {valid_ids}"
            )
        selected_ids = set(args.case_ids)
        cases = [
            case for case in cases if case["id"] in selected_ids
        ]

    target = make_target(
        keep_threads=args.keep_threads,
        interactive=args.interactive,
    )
    quality_evaluator = make_quality_evaluator()
    if args.mode == "langsmith":
        return run_langsmith(cases, target, quality_evaluator)
    return run_local(cases, target, quality_evaluator)


if __name__ == "__main__":
    sys.exit(main())
