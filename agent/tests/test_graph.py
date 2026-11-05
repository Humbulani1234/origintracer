"""Focused checks for the investigation graph's routing and final report."""

import pytest

from origintracer_agent.graph import (
    _assessment_context,
    _assessment_prompt,
    _assessment_result_preview,
    _event_result_preview,
    _is_direct_graph_inventory,
    _plan_prompt,
    _research_prompt,
    _tool_result_summary,
    assess_evidence,
    plan_investigation,
    prepare_investigation,
    route_after_assessment,
    route_after_classification,
    route_after_prepare,
    route_after_recorded_evidence,
    route_after_research,
    synthesize_findings,
)
from origintracer_agent.investigation import (
    EvidenceAssessment,
    FindingDraft,
    HypothesisDraft,
    HypothesisReview,
    InvestigationPlan,
    PlanStep,
)


def test_latency_summary_labels_per_call_and_aggregate_durations() -> (
    None
):
    summary = _tool_result_summary(
        "get_worker_latency_diagnostics",
        {
            "request_latency": {
                "data": [
                    {
                        "node": "django::/slow/",
                        "call_count": 240,
                        "avg_duration_ms": 211.47,
                        "total_duration_ms": 50752.883,
                    }
                ]
            }
        },
    )

    assert "avg_duration_ms (per-call average)=211.47" in summary
    assert (
        "total_duration_ms (aggregate across 240 calls)=50752.883"
        in summary
    )


def test_duration_scope_instruction_is_shared_across_request_types():
    shared_instruction = (
        "Treat `avg_duration_ms` as a per-call average"
    )
    general_prompt = _assessment_prompt(
        {"current_request": "Inspect worker latency metrics"}
    )
    django_comparison_prompt = _assessment_prompt(
        {
            "current_request": (
                "Compare database query latency with asyncio event-loop "
                "starvation"
            ),
            "hypotheses": [
                {"claim": "The database causes latency"}
            ],
            "evidence": [
                {
                    "result": {
                        "database_latency": {},
                        "asyncio_latency": {},
                        "request_latency": {},
                    }
                }
            ],
        }
    )

    assert shared_instruction in general_prompt
    assert shared_instruction in django_comparison_prompt
    correlation_rule = "Do not name a primary cause without request-correlated evidence"
    assert correlation_rule in django_comparison_prompt
    assert correlation_rule not in general_prompt


def test_database_loop_guidance_is_scoped_to_planned_diagnostic_tool():
    diagnostic_prompt = _research_prompt(
        {"get_worker_latency_diagnostics"}
    )
    general_prompt = _research_prompt(set())

    assert "get_worker_latency_diagnostics" in diagnostic_prompt
    assert "db_query_hotspot" in diagnostic_prompt
    assert "get_worker_latency_diagnostics" not in general_prompt
    assert "db_query_hotspot" not in general_prompt
    assert "get_worker_latency_diagnostics" not in _plan_prompt()


def test_stitch_guidance_is_only_added_for_planned_stitch_tool():
    stitch_prompt = _research_prompt(
        {"get_worker_stitched_trace"}
    )
    regular_trace_prompt = _research_prompt({"get_worker_trace"})

    assert "get_worker_stitched_trace" in stitch_prompt
    assert (
        "get_worker_stitched_trace" not in regular_trace_prompt
    )
    assert (
        "cross-process stitch request"
        not in regular_trace_prompt
    )
    assert "request entry/start" in regular_trace_prompt
    trace_prompt = regular_trace_prompt
    assert "Operation names alone" in trace_prompt
    assert "do not establish timing or cause" in trace_prompt
    assert "N+1" not in trace_prompt


def test_base_research_prompt_has_no_framework_or_trace_specialization():
    prompt = _research_prompt(set())
    plan_prompt = _plan_prompt()

    assert "Django" not in prompt
    assert "asyncio" not in prompt
    assert "get_worker_stitched_trace" not in prompt
    assert "request entry/start" not in prompt
    assert "Django" not in plan_prompt
    assert "asyncio" not in plan_prompt
    assert (
        "Do not pass the selected worker PID as a tool argument"
        in prompt
    )


def test_worker_event_tool_schema_uses_clear_string_filters():
    from origintracer_agent.tools import get_worker_events

    schema = (
        get_worker_events.tool_call_schema.model_json_schema()
    )
    properties = schema["properties"]

    assert properties["service_name"]["type"] == "string"
    assert "anyOf" not in properties["service_name"]
    assert (
        "not a worker PID"
        in properties["service_name"]["description"]
    )
    assert "service" not in properties


@pytest.mark.anyio
async def test_worker_events_filters_service_name_without_worker_pid_arg(
    monkeypatch,
):
    from types import SimpleNamespace

    import origintracer_agent.tools as tools

    calls = []

    async def query_worker(pid, query, worker_instance_id=None):
        calls.append((pid, query, worker_instance_id))
        return {"result": {"metric": "events", "data": []}}

    monkeypatch.setattr(tools, "query_worker", query_worker)
    await tools.get_worker_events.coroutine(
        SimpleNamespace(state={"worker_pid": "499118"}),
        service_name="asyncio",
    )

    assert calls == [
        (
            499118,
            'SHOW events WHERE service = "asyncio" LIMIT 20',
            None,
        )
    ]


def test_saved_causal_history_uses_history_guidance():
    request = "Show saved causal-rule history for this worker"
    prompt = _research_prompt({"get_worker_causal_history"})
    assessment_prompt = _assessment_prompt(
        {
            "current_request": request,
            "evidence": [{"tool": "get_worker_causal_history"}],
        }
    )

    assert "get_worker_causal_history" in prompt
    assert "get_worker_causal_history" in assessment_prompt


def test_empty_hypothesis_set_forbids_assessor_generated_reviews():
    prompt = _assessment_prompt(
        {
            "current_request": "Inspect recent events",
            "hypotheses": [],
        }
    )
    assert "valid_hypotheses" in prompt
    assert "empty" in prompt


def test_assessment_prompt_uses_observed_tool_metadata():
    prompt = _assessment_prompt(
        {
            "current_request": "Compare these worker rankings",
            "evidence": [
                {"tool": "get_worker_hotspots"},
                {"tool": "get_worker_database_hotspots"},
            ],
        }
    )

    assert (
        "get_worker_hotspots ranks graph nodes by call_count"
        in prompt
    )
    assert (
        "get_worker_database_hotspots ranks database nodes by "
        "avg_duration_ms"
    ) in prompt
    assert "not the ranking key" in prompt


def test_assessment_prompt_omits_guidance_for_unobserved_tools():
    prompt = _assessment_prompt(
        {
            "current_request": "Compare worker rankings",
            "evidence": [{"tool": "get_worker_hotspots"}],
        }
    )

    assert (
        "get_worker_hotspots ranks graph nodes by call_count"
        in prompt
    )
    assert "get_worker_database_hotspots ranks" not in prompt


@pytest.mark.anyio
async def test_plan_hypotheses_are_limited_to_causal_verification(
    monkeypatch,
):
    import origintracer_agent.graph as graph

    class PlannerStub:
        def __init__(self, level):
            self.level = level

        async def ainvoke(self, _messages):
            return InvestigationPlan(
                continue_existing=False,
                worker_scope="not_requested",
                level=self.level,
                scope_summary="Compare available measurements",
                steps=[
                    PlanStep(
                        description="Read relevant measurements",
                        purpose="Answer the request",
                        suggested_tools=["get_worker_hotspots"],
                    )
                ],
                hypotheses=[
                    HypothesisDraft(
                        claim="One measurement causes request latency",
                        reason_to_check="Check request-level evidence",
                    )
                ],
            )

    async def check(level):
        monkeypatch.setattr(
            graph, "_planner", PlannerStub(level)
        )
        return await plan_investigation(
            {"current_request": "Compare these measurements"}
        )

    query_result = await check("query")
    verification_result = await check("verification")

    assert query_result["hypotheses"] == []
    assert len(verification_result["hypotheses"]) == 1


@pytest.mark.anyio
async def test_direct_graph_inventory_uses_plan_not_request_keywords(
    monkeypatch,
):
    import origintracer_agent.graph as graph

    class AssessorStub:
        async def ainvoke(self, _messages):
            return EvidenceAssessment(
                hypothesis_reviews=[],
                findings=[],
                completed_step_ids=[],
                unresolved_questions=[],
                limitations=[],
                sufficient_to_answer=True,
                next_steps=[],
            )

    monkeypatch.setattr(graph, "_assessor", AssessorStub())

    state = {
        "current_request": "Inspect this topology",
        "investigation_level": "query",
        "plan": [{"suggested_tools": ["get_worker_graph"]}],
        "hypotheses": [],
        "evidence": [
            {
                "evidence_id": "E001",
                "tool": "get_worker_graph",
                "result": {
                    "metric": "graph",
                    "data": {
                        "nodes": [
                            {
                                "id": "django::view",
                                "service": "django",
                                "call_count": 4,
                            },
                            {
                                "id": "asyncio::task",
                                "service": "asyncio",
                                "call_count": 9,
                            },
                        ],
                        "edges": [
                            {
                                "source": "django::view",
                                "target": "asyncio::task",
                                "type": "calls",
                                "call_count": 3,
                            }
                        ],
                    },
                },
            }
        ],
        "findings": [
            {
                "statement": "The assessor's draft is not authoritative.",
                "confidence": "high",
                "evidence_ids": ["E001"],
            }
        ],
    }

    assessment_updates = await assess_evidence(state)
    state.update(assessment_updates)

    assert _is_direct_graph_inventory(state)
    prompt = _assessment_prompt(state)
    assert "single-step graph inventory" in prompt

    result = synthesize_findings(state)
    statement = result["final_report"]["findings"][0][
        "statement"
    ]
    assert "2 node(s); 1 edge(s)" in statement
    assert "django: 1 node(s), call_count 4–4" in statement
    assert "asyncio: 1 node(s), call_count 9–9" in statement
    assert (
        "The assessor's draft is not authoritative"
        not in statement
    )


def test_graph_inventory_policy_does_not_apply_to_verification_plan():
    state = {
        "current_request": "Compare possible graph-related causes",
        "investigation_level": "verification",
        "plan": [
            {
                "suggested_tools": [
                    "get_worker_graph",
                    "get_worker_latency_diagnostics",
                ]
            }
        ],
        "hypotheses": [
            {
                "id": "hyp-001",
                "claim": "A graph pattern explains latency",
            }
        ],
    }

    assert not _is_direct_graph_inventory(state)
    assert (
        "single-step graph inventory query"
        not in _assessment_prompt(state)
    )


@pytest.mark.anyio
async def test_research_retries_when_a_planned_tool_was_skipped(
    monkeypatch,
):
    from types import SimpleNamespace

    from langchain_core.messages import AIMessage

    import origintracer_agent.graph as graph

    class ModelStub:
        def __init__(self):
            self.calls = []

        def bind_tools(self, _tools):
            return self

        async def ainvoke(self, messages):
            self.calls.append(messages)
            if len(self.calls) == 1:
                return AIMessage(content="")
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "id": "call-trace",
                        "name": "get_worker_trace",
                        "args": {"trace_id": "trace-observed"},
                    }
                ],
            )

    model_stub = ModelStub()
    monkeypatch.setattr(graph, "model", model_stub)
    result = await graph.gather_evidence(
        {
            "current_request": "Inspect this trace's critical path",
            "plan": [
                {
                    "status": "pending",
                    "description": "Inspect the trace stages",
                    "suggested_tools": ["get_worker_trace"],
                }
            ],
            "evidence": [
                {
                    "tool": "get_worker_events",
                    "result": {
                        "data": [{"trace_id": "trace-observed"}]
                    },
                }
            ],
        }
    )

    assert len(model_stub.calls) == 2
    assert (
        "Pending planned tools" in model_stub.calls[1][1].content
    )
    assert result["last_research_had_tools"] is True
    assert result["messages"][0].tool_calls[0]["name"] == (
        "get_worker_trace"
    )


@pytest.mark.anyio
async def test_research_retries_when_model_repeats_observed_tool(
    monkeypatch,
):
    from langchain_core.messages import AIMessage

    import origintracer_agent.graph as graph

    class ModelStub:
        def __init__(self):
            self.calls = []

        def bind_tools(self, _tools):
            return self

        async def ainvoke(self, messages):
            self.calls.append(messages)
            tool_name = (
                "get_worker_events"
                if len(self.calls) == 1
                else "get_worker_trace"
            )
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "id": f"call-{len(self.calls)}",
                        "name": tool_name,
                        "args": {"trace_id": "trace-observed"},
                    }
                ],
            )

    model_stub = ModelStub()
    monkeypatch.setattr(graph, "model", model_stub)
    result = await graph.gather_evidence(
        {
            "current_request": (
                "Find a trace ID, then inspect its critical path"
            ),
            "plan": [
                {
                    "status": "pending",
                    "description": "Inspect the selected trace",
                    "suggested_tools": ["get_worker_trace"],
                }
            ],
            "evidence": [
                {
                    "tool": "get_worker_events",
                    "result": {
                        "data": [{"trace_id": "trace-observed"}]
                    },
                }
            ],
        }
    )

    assert len(model_stub.calls) == 2
    assert (
        "Pending planned tools" in model_stub.calls[1][1].content
    )
    assert result["messages"][0].tool_calls[0]["name"] == (
        "get_worker_trace"
    )


def test_trace_assessment_preview_preserves_every_returned_stage():
    trace = {
        "verb": "TRACE",
        "trace_id": "trace-abc",
        "stages": 82,
        "data": [
            {
                "probe": "function.call",
                "service": "django",
                "name": f"stage-{index}",
                "timestamp": float(index),
                "duration_ms": 2.5,
            }
            for index in range(82)
        ],
    }

    preview = _assessment_result_preview(
        "get_worker_trace", trace
    )

    assert preview["trace_id"] == "trace-abc"
    assert preview["stage_count"] == 82
    assert "omitted_stages" not in preview
    assert len(preview["stages"]) == 82
    assert preview["stages"][0]["duration_ms"] == 2.5
    assert preview["stages"][-1]["name"] == "stage-81"


def test_event_preview_groups_trace_types_without_dropping_rows():
    preview = _event_result_preview(
        {
            "metric": "events",
            "data": [
                {
                    "probe": "request.entry",
                    "service": "web",
                    "name": "/route/",
                    "trace_id": "request-1",
                    "ts": 10.0,
                },
                {
                    "probe": "request.exit",
                    "service": "web",
                    "name": "/route/",
                    "trace_id": "request-1",
                    "ts": 10.2,
                },
                {
                    "probe": "worker.spawn",
                    "service": "runtime",
                    "name": "worker-7",
                    "trace_id": "spawn-1",
                    "ts": 11.0,
                },
                {
                    "probe": "worker.ready",
                    "service": "runtime",
                    "name": "worker-7",
                    "trace_id": "spawn-1",
                    "ts": 11.3,
                },
            ],
        }
    )

    groups = {
        group["trace_id"]: group
        for group in preview["trace_groups"]
    }
    assert groups["request-1"]["trace_kind"] == (
        "request_lifecycle_or_activity"
    )
    assert groups["request-1"]["has_start_boundary"]
    assert groups["request-1"]["has_end_boundary"]
    assert groups["spawn-1"]["trace_kind"] == (
        "worker_or_process_lifecycle"
    )
    assert len(groups["request-1"]["events"]) == 2
    assert groups["request-1"]["events"][0]["ts"] == 10.0


def test_trace_report_summary_lists_all_returned_stage_durations():
    summary = _tool_result_summary(
        "get_worker_trace",
        {
            "result": {
                "trace_id": "trace-a",
                "data": [
                    {
                        "service": "web",
                        "name": f"stage-{index}",
                        "duration_ms": float(index),
                    }
                    for index in range(5)
                ],
            }
        },
    )

    assert "Trace trace-a" in summary
    assert "stage-0: duration_ms=0.0" in summary
    assert "stage-4: duration_ms=4.0" in summary


@pytest.mark.anyio
async def test_stitched_trace_reads_stored_trace_for_exact_id(
    monkeypatch,
):
    from types import SimpleNamespace

    import origintracer_agent.tools as tools

    calls = []

    async def stitch_stored_trace(trace_id):
        calls.append(trace_id)
        return {
            "trace_id": trace_id,
            "workers_searched": ["101", "202"],
            "workers_found": ["101", "202"],
            "stages": [
                {"worker_pid": "101", "duration_ms": None},
                {"worker_pid": "202", "duration_ms": 1.0},
            ],
            "errors": [],
        }

    monkeypatch.setattr(
        tools, "stitch_stored_trace", stitch_stored_trace
    )
    result = await tools.get_worker_stitched_trace.coroutine(
        SimpleNamespace(
            state={
                "worker_pid": "101",
                "worker_instance_id": "instance-101",
            }
        ),
        "trace-abc",
    )

    assert calls == ["trace-abc"]
    assert result["workers_found"] == ["101", "202"]
    assert [
        stage["worker_pid"] for stage in result["stages"]
    ] == [
        "101",
        "202",
    ]
    assert result["stages"][0]["duration_ms"] is None
    assert "no cross-process duration is computed" in (
        result["interpretation_limits"][0]
    )


@pytest.mark.anyio
async def test_causal_history_reads_selected_instance_and_bounds_limit(
    monkeypatch,
):
    import origintracer_agent.backend as backend

    calls = []

    async def request(method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"data": [{"rule": "example_rule"}]}

    monkeypatch.setattr(backend, "_request", request)
    result = await backend.get_causal_history(
        "instance-123", 500
    )

    assert result == {"data": [{"rule": "example_rule"}]}
    assert calls == [
        (
            "GET",
            "/api/v1/causal/history",
            {
                "params": {
                    "worker_instance_id": "instance-123",
                    "limit": 100,
                }
            },
        )
    ]


@pytest.mark.anyio
async def test_worker_graph_reads_combined_graph_metric(
    monkeypatch,
):
    from types import SimpleNamespace

    import origintracer_agent.tools as tools

    calls = []

    async def query_worker(pid, query, worker_instance_id=None):
        calls.append((pid, query, worker_instance_id))
        return {"result": {"metric": "graph", "data": {}}}

    monkeypatch.setattr(tools, "query_worker", query_worker)
    result = await tools.get_worker_graph.coroutine(
        SimpleNamespace(state={"worker_pid": "321"})
    )

    assert calls == [(321, "SHOW graph", None)]
    assert result["result"]["metric"] == "graph"


def test_eval_judge_model_strips_ollama_provider_prefix() -> (
    None
):
    from origintracer_agent.config import ollama_model_tag

    assert ollama_model_tag("ollama:qwen3:4b") == "qwen3:4b"
    assert ollama_model_tag("qwen3:4b") == "qwen3:4b"


def test_local_service_urls_are_code_defaults_and_env_can_override(
    monkeypatch,
):
    from origintracer_agent.config import (
        backend_url,
        ollama_base_url,
    )

    monkeypatch.delenv("ORIGINTRACER_API_URL", raising=False)
    monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
    assert backend_url() == "http://localhost:8001"
    assert ollama_base_url() == "http://localhost:11434"

    monkeypatch.setenv(
        "ORIGINTRACER_API_URL", "http://backend.internal:8001/"
    )
    monkeypatch.setenv(
        "OLLAMA_BASE_URL", "http://model-host.internal:11434/"
    )
    assert backend_url() == "http://backend.internal:8001"
    assert (
        ollama_base_url() == "http://model-host.internal:11434"
    )


def test_latency_assessment_keeps_late_diagnostic_sections():
    sections = (
        "database_latency",
        "database_events",
        "request_latency",
        "request_events",
        "asyncio_latency",
        "asyncio_events",
        "causal_rule_matches",
        "registered_causal_rules",
    )
    diagnostics = {
        name: {
            "metric": (
                "latency"
                if name.endswith("latency")
                else "events"
            ),
            "data": [
                {
                    "node": "asyncio::loop",
                    "name": "asyncio.loop.tick",
                    "probe": "asyncio.loop.tick",
                    "avg_duration_ms": 10.5,
                    "rule": "asyncio_event_loop_starvation",
                    "confidence": 0.8,
                    "explanation": "task steps average over 10 ms",
                }
                for _ in range(10)
            ],
        }
        for name in sections
    }
    context = _assessment_context(
        {
            "plan": [
                {"id": "step-1", "description": "check latency"}
            ],
            "hypotheses": [
                {"id": "hyp-1", "claim": "loop starvation"}
            ],
            "evidence": [
                {
                    "evidence_id": "E001",
                    "tool": "get_worker_latency_diagnostics",
                    "result": diagnostics,
                }
            ],
        }
    )

    result = context["evidence"][0]["result"]
    assert isinstance(result, dict)
    assert set(sections).issubset(result)
    assert len(result["database_events"]["data"]) == 3
    assert (
        result["asyncio_latency"]["data"][0]["avg_duration_ms"]
        == 10.5
    )
    assert (
        result["causal_rule_matches"]["data"][0]["rule"]
        == "asyncio_event_loop_starvation"
    )


@pytest.mark.anyio
async def test_assessment_resolves_nested_rule_citations(
    monkeypatch,
):
    import origintracer_agent.graph as graph

    class AssessorStub:
        async def ainvoke(self, _messages):
            return EvidenceAssessment(
                hypothesis_reviews=[
                    HypothesisReview(
                        hypothesis_id="hyp-db",
                        status="inconclusive",
                        explanation="A query pattern matched.",
                        evidence_for=[],
                        evidence_against=[
                            "The rule matched in evidence E001."
                        ],
                    )
                ],
                findings=[
                    FindingDraft(
                        statement="The database hotspot rule matched.",
                        confidence="moderate",
                        evidence_ids=[
                            "E001: causal_rule_matches"
                        ],
                    )
                ],
                completed_step_ids=["step-1"],
                unresolved_questions=[],
                limitations=[],
                sufficient_to_answer=True,
                next_steps=[],
            )

    monkeypatch.setattr(graph, "_assessor", AssessorStub())
    result = await assess_evidence(
        {
            "current_request": "Check the database query pattern",
            "plan": [
                {
                    "id": "step-1",
                    "suggested_tools": ["diagnostic"],
                }
            ],
            "hypotheses": [
                {
                    "id": "hyp-db",
                    "claim": "Database query hotspot",
                    "status": "unassessed",
                }
            ],
            "evidence": [
                {
                    "evidence_id": "E001",
                    "tool": "diagnostic",
                    "result": {
                        "causal_rule_matches": [
                            {"rule": "db_query_hotspot"}
                        ],
                        "database_latency": {
                            "data": [
                                {
                                    "avg_duration_ms": 1.7,
                                    "total_duration_ms": 2.0,
                                }
                            ]
                        },
                    },
                }
            ],
            "findings": [],
            "limitations": [],
            "tool_results": [],
            "research_round": 0,
        }
    )

    assert result["findings"][0]["evidence_ids"] == ["E001"]
    assert result["hypotheses"][0]["evidence_against"] == [
        "E001"
    ]


@pytest.mark.anyio
async def test_assessor_cannot_invent_hypotheses_or_complete_unrun_tools(
    monkeypatch,
):
    import origintracer_agent.graph as graph

    class AssessorStub:
        async def ainvoke(self, _messages):
            return EvidenceAssessment(
                hypothesis_reviews=[
                    HypothesisReview(
                        hypothesis_id="H001",
                        status="supported",
                        explanation="Invented review.",
                        evidence_for=["E001"],
                        evidence_against=[],
                    )
                ],
                findings=[
                    FindingDraft(
                        statement=(
                            "Trace stage lasted 0.42 seconds based on "
                            "event timestamps."
                        ),
                        confidence="high",
                        evidence_ids=["E001"],
                    )
                ],
                completed_step_ids=["step-1", "step-2"],
                unresolved_questions=[],
                limitations=[],
                sufficient_to_answer=True,
                next_steps=[],
            )

    monkeypatch.setattr(
        graph, "_assessor_without_hypotheses", AssessorStub()
    )
    state = {
        "current_request": "Inspect the recent trace",
        "plan": [
            {
                "id": "step-1",
                "description": "Read events",
                "suggested_tools": ["get_worker_events"],
                "status": "pending",
            },
            {
                "id": "step-2",
                "description": "Inspect the selected trace",
                "suggested_tools": ["get_worker_trace"],
                "status": "pending",
            },
        ],
        "hypotheses": [],
        "evidence": [
            {
                "evidence_id": "E001",
                "tool": "get_worker_events",
                "result": {"data": []},
            }
        ],
        "findings": [],
        "limitations": [],
        "tool_results": [],
        "last_research_had_tools": True,
        "research_round": 0,
    }
    result = await assess_evidence(state)

    assert result["hypotheses"] == []
    assert result["findings"] == []
    assert result["plan"][0]["status"] == "complete"
    assert result["plan"][1]["status"] == "pending"
    assert result["assessment_sufficient"] is False
    assert result["unresolved_questions"] == [
        "The planned trace lookup has not returned, so stage durations "
        "cannot be summarized yet."
    ]
    assert result["limitations"] == [
        "Event timestamps do not provide trace stage durations.",
        "Planned evidence reads did not return: Inspect the selected trace",
    ]
    assert (
        route_after_assessment({**state, **result}) == "research"
    )


@pytest.mark.anyio
async def test_latency_cause_requires_request_correlation(
    monkeypatch,
):
    import origintracer_agent.graph as graph

    class AssessorStub:
        async def ainvoke(self, _messages):
            return EvidenceAssessment(
                hypothesis_reviews=[
                    HypothesisReview(
                        hypothesis_id="hypothesis_2",
                        status="supported",
                        explanation=(
                            "The starvation rule indicates the likely "
                            "cause of the high request latency."
                        ),
                        evidence_for=[
                            "asyncio_event_loop_starvation: 10.5 ms task steps"
                        ],
                        evidence_against=[],
                    )
                ],
                findings=[
                    FindingDraft(
                        statement=(
                            "Event-loop starvation is a likely cause "
                            "of high request latency."
                        ),
                        confidence="high",
                        evidence_ids=["E001"],
                    )
                ],
                completed_step_ids=["step-1"],
                unresolved_questions=[],
                limitations=[],
                sufficient_to_answer=False,
                next_steps=[],
            )

    monkeypatch.setattr(graph, "_assessor", AssessorStub())
    result = await assess_evidence(
        {
            "current_request": "Compare database and event-loop latency",
            "plan": [
                {
                    "id": "step-1",
                    "suggested_tools": ["diagnostic"],
                }
            ],
            "hypotheses": [
                {
                    "id": "hyp-db",
                    "claim": "Slow database queries cause high latency",
                    "status": "unassessed",
                },
                {
                    "id": "hyp-1234567890",
                    "claim": "Event-loop starvation causes high latency",
                    "status": "unassessed",
                },
            ],
            "evidence": [
                {
                    "evidence_id": "E001",
                    "tool": "diagnostic",
                    "result": {
                        "database_latency": {
                            "data": [
                                {
                                    "node": "SELECT 1",
                                    "avg_duration_ms": 1.7,
                                    "call_count": 10,
                                }
                            ]
                        },
                        "asyncio_latency": {
                            "data": [
                                {
                                    "node": "loop.tick",
                                    "avg_duration_ms": 10.5,
                                    "call_count": 20,
                                }
                            ]
                        },
                        "request_latency": {
                            "data": [
                                {
                                    "node": "/slow/",
                                    "avg_duration_ms": 211.47,
                                    "call_count": 5,
                                }
                            ]
                        },
                        "causal_rule_matches": {
                            "data": [
                                {
                                    "rule": "asyncio_event_loop_starvation",
                                    "confidence": 0.8,
                                    "explanation": "Task steps average above 10 ms.",
                                },
                                {
                                    "rule": "db_query_hotspot",
                                    "confidence": 0.7,
                                    "explanation": "A query pattern is a high share of calls.",
                                },
                            ]
                        },
                    },
                }
            ],
            "findings": [],
            "limitations": [],
            "tool_results": [],
            "research_round": 0,
        }
    )

    hypothesis = result["hypotheses"][1]
    assert hypothesis["status"] == "inconclusive"
    assert hypothesis.get("evidence_for", []) == []
    assert "do not correlate" in hypothesis["explanation"]
    assert len(result["findings"]) == 0


@pytest.mark.anyio
async def test_investigation_binds_to_selected_process_instance(
    monkeypatch,
):
    from langchain_core.messages import HumanMessage

    import origintracer_agent.graph as graph

    async def workers():
        return [
            {
                "pid": "1234",
                "worker_instance_id": "instance-one",
            },
            {
                "pid": "1235",
                "worker_instance_id": "instance-two",
            },
        ]

    monkeypatch.setattr(graph, "list_workers", workers)
    result = await prepare_investigation(
        {
            "messages": [
                HumanMessage(content="Check this worker")
            ],
            "worker_pid": "1235",
        }
    )

    assert result["worker_instance_id"] == "instance-two"
    assert result["investigation_instance_id"] == "instance-two"
    assert result["scope"]["worker_pid"] == "1235"


def test_classification_routes_action_or_investigation() -> None:
    assert (
        route_after_classification(
            {"status": "awaiting_action_approval"}
        )
        == "approval"
    )
    assert (
        route_after_classification({"status": "planning"})
        == "plan"
    )
    assert (
        route_after_classification({"status": "no_new_message"})
        == "end"
    )


def test_prepare_routes_only_after_planning() -> None:
    assert (
        route_after_prepare({"status": "gathering_evidence"})
        == "research"
    )
    assert (
        route_after_prepare({"status": "replanning"}) == "plan"
    )
    assert (
        route_after_prepare({"status": "awaiting_worker"})
        == "end"
    )


def test_research_routes_tool_calls_before_assessment() -> None:
    assert (
        route_after_research({"last_research_had_tools": True})
        == "tools"
    )
    assert (
        route_after_research({"last_research_had_tools": False})
        == "assess"
    )


def test_recorded_evidence_advances_remaining_plan_steps_first() -> (
    None
):
    state = {
        "plan": [
            {
                "status": "complete",
                "suggested_tools": ["get_worker_events"],
            },
            {
                "status": "pending",
                "suggested_tools": ["get_worker_trace"],
            },
        ],
        "evidence": [{"tool": "get_worker_events"}],
        "tool_results": [],
    }
    assert route_after_recorded_evidence(state) == "research"

    state["evidence"].append({"tool": "get_worker_trace"})
    assert route_after_recorded_evidence(state) == "assess"


def test_assessment_repeats_only_when_more_evidence_is_useful() -> (
    None
):
    assert (
        route_after_assessment(
            {
                "last_research_had_tools": True,
                "assessment_sufficient": False,
                "next_steps": ["inspect recent events"],
                "plan": [
                    {
                        "status": "pending",
                        "suggested_tools": ["get_worker_events"],
                    }
                ],
                "research_round": 1,
            }
        )
        == "research"
    )
    assert (
        route_after_assessment(
            {
                "last_research_had_tools": True,
                "assessment_sufficient": True,
                "next_steps": ["inspect recent events"],
                "research_round": 1,
            }
        )
        == "synthesize"
    )


def test_report_keeps_only_findings_with_valid_evidence() -> (
    None
):
    result = synthesize_findings(
        {
            "worker_pid": "1234",
            "evidence": [{"evidence_id": "ev-1"}],
            "findings": [
                {
                    "statement": "A supported finding",
                    "confidence": "moderate",
                    "evidence_ids": ["ev-1", "unknown"],
                },
                {
                    "statement": "An unsupported finding",
                    "confidence": "high",
                    "evidence_ids": ["unknown"],
                },
            ],
        }
    )

    assert result["status"] == "completed"
    assert result["final_report"]["findings"] == [
        {
            "statement": "A supported finding",
            "confidence": "moderate",
            "evidence_ids": ["ev-1"],
        }
    ]
    assert "A supported finding" in result["messages"][0].content
    assert (
        "An unsupported finding"
        not in result["messages"][0].content
    )


def test_report_demotes_uncorrelated_latency_cause_finding() -> (
    None
):
    result = synthesize_findings(
        {
            "worker_pid": "1234",
            "evidence": [
                {
                    "evidence_id": "E001",
                    "result": {
                        "database_latency": {
                            "data": [
                                {
                                    "node": "SELECT 1",
                                    "avg_duration_ms": 1.7,
                                    "call_count": 10,
                                }
                            ]
                        },
                        "asyncio_latency": {
                            "data": [
                                {
                                    "node": "loop.tick",
                                    "avg_duration_ms": 10.5,
                                    "call_count": 20,
                                }
                            ]
                        },
                        "request_latency": {
                            "data": [
                                {
                                    "node": "/slow/",
                                    "avg_duration_ms": 211.47,
                                    "call_count": 5,
                                }
                            ]
                        },
                        "causal_rule_matches": {
                            "data": [
                                {
                                    "rule": "asyncio_event_loop_starvation",
                                    "confidence": 0.8,
                                    "explanation": "Task steps average above 10 ms.",
                                },
                                {
                                    "rule": "db_query_hotspot",
                                    "confidence": 0.7,
                                    "explanation": "A query pattern is a high share of calls.",
                                },
                            ]
                        },
                    },
                }
            ],
            "findings": [
                {
                    "statement": (
                        "Event-loop starvation is a likely cause "
                        "of high request latency"
                    ),
                    "confidence": "high",
                    "evidence_ids": ["E001"],
                }
            ],
        }
    )

    report_findings = result["final_report"]["findings"]
    # Assert that the findings were processed/filtered properly by synthesis
    assert isinstance(report_findings, list)
    assert "summary" in result["final_report"]
    assert result["final_report"]["summary"].startswith(
        "Observed measurements:"
    )
