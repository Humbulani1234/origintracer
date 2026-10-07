"""Bounded Agent Server graphs that demonstrate LangGraph causal rules.

These graphs intentionally contain unhealthy behavior. They are kept separate
from the personal assistant so its normal workflow remains realistic. Each graph
is deterministic at the orchestration layer: it does not depend on an Ollama
model deciding whether to produce the condition a rule is intended to detect.
"""

from __future__ import annotations

import time
from typing import TypedDict

from langchain.tools import tool
from langgraph.graph import END, START, StateGraph

from personal_assistant.graph import model


class LoopState(TypedDict, total=False):
    """
    State retained while the bounded model loop advances.
    """

    iteration: int
    last_response: str


_LOOP_ITERATIONS = 9


def _call_model_again(state: LoopState) -> LoopState:
    """
    Make one short model call and advance the deliberate loop
    counter.
    """
    response = model.invoke(
        "Reply with the single word OK and no additional explanation."
    )
    return {
        "iteration": state.get("iteration", 0) + 1,
        "last_response": response.text,
    }


def _continue_loop(state: LoopState) -> str:
    """
    Stop after crossing the loop_runaway rule's eight-call threshold.
    """
    return (
        "again"
        if state["iteration"] < _LOOP_ITERATIONS
        else "done"
    )


_loop_builder = StateGraph(LoopState)
_loop_builder.add_node("repeated_model_call", _call_model_again)
_loop_builder.add_edge(START, "repeated_model_call")
_loop_builder.add_conditional_edges(
    "repeated_model_call",
    _continue_loop,
    {"again": "repeated_model_call", "done": END},
)
loop_runaway_scenario = _loop_builder.compile(
    name="loop_runaway_scenario"
)


class LatencyState(TypedDict, total=False):
    """
    Independent result fields written by the two parallel tool
    branches.
    """

    request: str
    fast_result: str
    slow_result: str


@tool
def fast_dependency(request: str) -> str:
    """
    Represent a healthy low-latency dependency.
    """
    time.sleep(0.01)
    return f"Fast result for: {request}"


@tool
def slow_dependency(request: str) -> str:
    """
    Represent a dependency whose latency dominates the agent's
    leaf work.
    """
    time.sleep(1.0)
    return f"Slow result for: {request}"


def _run_fast_dependency(state: LatencyState) -> LatencyState:
    """
    Execute the fast side of the parallel fan-out.
    """
    result = fast_dependency.invoke(
        {
            "request": state.get(
                "request", "latency demonstration"
            )
        }
    )
    return {"fast_result": result}


def _run_slow_dependency(state: LatencyState) -> LatencyState:
    """
    Execute the deliberately slow side of the parallel fan-out.
    """
    result = slow_dependency.invoke(
        {
            "request": state.get(
                "request", "latency demonstration"
            )
        }
    )
    return {"slow_result": result}


_latency_builder = StateGraph(LatencyState)
_latency_builder.add_node("fast_branch", _run_fast_dependency)
_latency_builder.add_node("slow_branch", _run_slow_dependency)
_latency_builder.add_edge(START, "fast_branch")
_latency_builder.add_edge(START, "slow_branch")
_latency_builder.add_edge("fast_branch", END)
_latency_builder.add_edge("slow_branch", END)
agent_latency_hotspot_scenario = _latency_builder.compile(
    name="agent_latency_hotspot_scenario"
)
