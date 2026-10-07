"""
Observe LangChain and LangGraph runs through their callback interface.

Architectural boundary
----------------------
This module translates framework callbacks into the framework-neutral lineage
contract understood by the engine. LangChain's ``run_id`` becomes ``span_id``
and ``parent_run_id`` becomes ``parent_span_id``. The engine sees only stable
operation identities and direct parentage; it never needs to know what a graph
node, superstep, conditional edge, or parallel dispatch means.

Every callback lifecycle keeps the same span ID. An ``*.enter`` event creates
one logical operation and its matching ``*.exit`` or ``langgraph.error`` event
enriches that operation with duration and result metadata. This avoids counting
the start and finish of one run as two invocations.

Concurrency and correlation
---------------------------
Callback parent IDs are authoritative even when sibling branches interleave.
If a child callback is observed before its parent event is drained, the engine
retains the relationship and connects the edge when the parent arrives.

When OriginTracer already has an active trace context, the entire callback tree
inherits it. Without an active context, a child inherits the trace ID recorded
for its callback parent; only a genuinely top-level run uses its own run ID as
the trace root. The parent lookup is essential for standalone asynchronous and
thread-pool execution, where callbacks do not necessarily run in the context
that started the outer graph.

Framework metadata
------------------
LangGraph-specific facts such as node name, step, triggers, path, and checkpoint
namespace remain event metadata. They are intentionally interpreted only by
LangGraph rules or research tooling. Inputs and outputs are represented by keys
or bounded summaries so this probe does not copy arbitrary application state.

Registration
------------
Call ``LangGraphProbe().start()`` once at process startup. The implementation
uses ``BaseCallbackHandler`` and ``register_configure_hook`` rather than
monkeypatching execution internals. ``langchain-core`` is therefore an optional
dependency required only when this probe module is imported.

LLM content capture
-------------------
Prompt and response text is not collected by default. For a focused debugging
session, set ``ORIGINTRACER_CAPTURE_LLM_CONTENT=1``. Captured text is redacted
for common credentials and truncated to a configurable bound before it enters
event metadata; other probes and ordinary event views remain unchanged.

Tool argument and result capture is independently controlled with
``ORIGINTRACER_CAPTURE_TOOL_CONTENT=1`` and follows the same redaction and
bounded-preview rules.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from contextvars import ContextVar
from typing import Any, Dict, List, Optional
from uuid import UUID

from langchain_core.callbacks.base import BaseCallbackHandler
from langchain_core.tracers.context import (
    register_configure_hook,
)

from ..context.vars import get_trace_id
from ..core.event_schema import NormalizedEvent, ProbeTypes
from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit

logger = logging.getLogger("origintracer.probes.langgraph")

ProbeTypes.register_many(
    {
        "langgraph.chain.enter": "LangChain runnable or graph node entered",
        "langgraph.chain.exit": "LangChain runnable or graph node completed",
        "langgraph.tool.enter": "LangChain tool invocation entered",
        "langgraph.tool.exit": "LangChain tool invocation completed",
        "langgraph.llm.enter": "LangChain model invocation entered",
        "langgraph.llm.exit": "LangChain model invocation completed",
        "langgraph.error": "LangChain callback run failed",
    }
)

# LangChain delivers start and terminal callbacks separately. This table retains
# the minimum state needed to correlate them and compute a duration. A shared
# handler may receive callbacks from multiple threads, so all access is locked.
_active_runs: Dict[UUID, Dict[str, Any]] = {}
_active_runs_lock = threading.RLock()

_CONTENT_ENV = "ORIGINTRACER_CAPTURE_LLM_CONTENT"
_CONTENT_LIMIT_ENV = "ORIGINTRACER_LLM_CONTENT_MAX_CHARS"
_TOOL_CONTENT_ENV = "ORIGINTRACER_CAPTURE_TOOL_CONTENT"
_TOOL_CONTENT_LIMIT_ENV = "ORIGINTRACER_TOOL_CONTENT_MAX_CHARS"
_DEFAULT_CONTENT_LIMIT = 4000


def _content_capture_enabled() -> bool:
    """
    Return whether opt-in LLM prompt/response capture is enabled.
    """
    return os.getenv(_CONTENT_ENV, "").strip().lower() not in {
        "",
        "0",
        "false",
        "no",
        "off",
    }


def _content_limit() -> int:
    """Read a safe upper bound for one captured prompt or response."""
    try:
        value = int(
            os.getenv(
                _CONTENT_LIMIT_ENV, str(_DEFAULT_CONTENT_LIMIT)
            )
        )
    except ValueError:
        value = _DEFAULT_CONTENT_LIMIT
    return max(128, min(value, 20_000))


def _tool_content_capture_enabled() -> bool:
    """Return whether opt-in tool argument/result capture is enabled."""
    return os.getenv(
        _TOOL_CONTENT_ENV, ""
    ).strip().lower() not in {
        "",
        "0",
        "false",
        "no",
        "off",
    }


def _tool_content_limit() -> int:
    """Read a safe upper bound for one captured tool value."""
    try:
        value = int(
            os.getenv(
                _TOOL_CONTENT_LIMIT_ENV,
                str(_DEFAULT_CONTENT_LIMIT),
            )
        )
    except ValueError:
        value = _DEFAULT_CONTENT_LIMIT
    return max(128, min(value, 20_000))


def _content_text(value: Any, depth: int = 0) -> str:
    """Convert common LangChain message/content shapes into bounded text."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    # LangChain results commonly nest generations -> generation -> message;
    # allow enough depth to reach message content while still bounding shape.
    if depth >= 6:
        return repr(value)
    if isinstance(value, dict):
        if "content" in value:
            role = value.get("role") or value.get("type")
            text = _content_text(value["content"], depth + 1)
            return f"{role}: {text}" if role else text
        try:
            return json.dumps(
                value, ensure_ascii=False, default=str
            )
        except (TypeError, ValueError):
            return repr(value)
    if isinstance(value, (list, tuple)):
        return "\n".join(
            item
            for item in (
                _content_text(part, depth + 1) for part in value
            )
            if item
        )
    if hasattr(value, "generations"):
        return _content_text(
            getattr(value, "generations"), depth + 1
        )
    if hasattr(value, "message"):
        # Chat generations can expose an empty message.content while keeping
        # textual output on generation.text.  Do not lose that response just
        # because the message also carries structured tool-call metadata.
        text = _content_text(
            getattr(value, "message"), depth + 1
        )
        if text:
            return text
        if hasattr(value, "text"):
            return _content_text(
                getattr(value, "text"), depth + 1
            )
        return text
    if hasattr(value, "text"):
        return _content_text(getattr(value, "text"), depth + 1)
    if hasattr(value, "content"):
        role = getattr(value, "type", None) or getattr(
            value, "role", None
        )
        text = _content_text(
            getattr(value, "content"), depth + 1
        )
        return f"{role}: {text}" if role else text
    return str(value)


def _llm_tool_calls_text(response: Any) -> str:
    """Describe tool-call-only model responses for the content view.

    Tool calls are often represented as an AI message with empty textual
    content.  A compact summary makes that orchestration visible in
    ``SHOW llm_content`` while the separate tool callbacks retain full,
    opt-in input/output capture.
    """
    calls = []
    for generation_list in (
        getattr(response, "generations", None) or []
    ):
        for generation in generation_list or []:
            message = getattr(generation, "message", None)
            for call in (
                getattr(message, "tool_calls", None) or []
            ):
                if isinstance(call, dict):
                    function = call.get("function") or {}
                    name = (
                        call.get("name")
                        or function.get("name")
                        or "tool"
                    )
                    args = call.get("args")
                    if args is None:
                        args = function.get("arguments")
                else:
                    name = getattr(call, "name", None) or "tool"
                    args = getattr(call, "args", None)
                try:
                    rendered_args = json.dumps(
                        args, ensure_ascii=False, default=str
                    )
                except (TypeError, ValueError):
                    rendered_args = str(args)
                calls.append(
                    f"tool call: {name}\narguments: {rendered_args}"
                )
    return "\n\n".join(calls)


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(\b(?:api[_ -]?key|access[_ -]?token|auth[_ -]?token|secret|password|passwd)\b\s*[:=]\s*)[^\s,;]+"
)
_BEARER_TOKEN = re.compile(
    r"(?i)(\bBearer\s+)[A-Za-z0-9._~+/=-]+"
)
_OPENAI_TOKEN = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")


def _redact_content(text: str) -> str:
    """Redact common credential forms before content enters event metadata."""
    text = _SECRET_ASSIGNMENT.sub(r"\1<redacted>", text)
    text = _BEARER_TOKEN.sub(r"\1<redacted>", text)
    return _OPENAI_TOKEN.sub("<redacted>", text)


def _llm_content_metadata(
    value: Any, field: str
) -> Dict[str, Any]:
    """Build opt-in, bounded metadata for one prompt or response value."""
    if not _content_capture_enabled():
        return {}
    text = _redact_content(_content_text(value))
    if not text and field == "response_preview":
        text = _redact_content(_llm_tool_calls_text(value))
    limit = _content_limit()
    truncated = len(text) > limit
    if truncated:
        text = text[:limit] + "…"
    return {
        field: text,
        f"{field}_truncated": truncated,
        "llm_content_captured": True,
    }


def _tool_content_metadata(
    value: Any, field: str
) -> Dict[str, Any]:
    """Build opt-in, bounded metadata for one tool argument or result value."""
    if not _tool_content_capture_enabled():
        return {}
    text = _redact_content(_content_text(value))
    limit = _tool_content_limit()
    truncated = len(text) > limit
    if truncated:
        text = text[:limit] + "…"
    return {
        field: text,
        f"{field}_truncated": truncated,
        "tool_content_captured": True,
    }


def _metadata_value(value: Any, depth: int = 0) -> Any:
    """Convert callback metadata to a bounded, serialization-safe value."""
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:500]
    if depth >= 2:
        return repr(value)[:500]
    if isinstance(value, dict):
        return {
            str(key)[:100]: _metadata_value(item, depth + 1)
            for key, item in list(value.items())[:50]
        }
    if isinstance(value, (list, tuple, set)):
        return [
            _metadata_value(item, depth + 1)
            for item in list(value)[:50]
        ]
    return repr(value)[:500]


def _framework_metadata(
    metadata: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Select useful scheduler facts without copying arbitrary callback state."""
    source = metadata or {}
    keys = (
        "langgraph_node",
        "langgraph_step",
        "langgraph_triggers",
        "langgraph_path",
        "langgraph_checkpoint_ns",
    )
    return {
        key: _metadata_value(source[key])
        for key in keys
        if key in source
    }


def _remember_run(
    *,
    run_id: UUID,
    parent_run_id: Optional[UUID],
    name: str,
    kind: str,
    metadata: Optional[Dict[str, Any]] = None,
    **state: Any,
) -> Dict[str, Any]:
    """
    Store a callback run and return its stable lifecycle description.

    Trace selection proceeds from the strongest available context to the
    weakest: an active OriginTracer trace, the recorded callback parent, then
    the run's own ID for a standalone root. Parent inheritance is performed
    while holding the active-run lock so sibling callbacks cannot corrupt it.
    """
    inherited_trace = get_trace_id()
    parent_span_id = (
        str(parent_run_id) if parent_run_id else None
    )
    with _active_runs_lock:
        parent = (
            _active_runs.get(parent_run_id)
            if parent_run_id
            else None
        )
        trace_id = (
            inherited_trace
            or (parent.get("trace_id") if parent else None)
            or str(run_id)
        )
        entry = {
            "t0": time.perf_counter(),
            "name": name,
            "kind": kind,
            "trace_id": trace_id,
            "parent_span_id": parent_span_id,
            "framework_metadata": _framework_metadata(metadata),
            **state,
        }
        _active_runs[run_id] = entry
        return entry


def _take_run(run_id: UUID) -> Optional[Dict[str, Any]]:
    """Atomically remove and return a run at its terminal callback."""
    with _active_runs_lock:
        return _active_runs.pop(run_id, None)


def _fallback_run(
    run_id: UUID,
    parent_run_id: Optional[UUID],
    *,
    name: str,
    kind: str,
) -> Dict[str, Any]:
    """Build minimal terminal state when a start callback was not observed."""
    inherited_trace = get_trace_id()
    with _active_runs_lock:
        parent = (
            _active_runs.get(parent_run_id)
            if parent_run_id
            else None
        )
        trace_id = (
            inherited_trace
            or (parent.get("trace_id") if parent else None)
            or str(run_id)
        )
    return {
        "name": name,
        "kind": kind,
        "trace_id": trace_id,
        "parent_span_id": (
            str(parent_run_id) if parent_run_id else None
        ),
        "framework_metadata": {},
    }


def _emit_run_event(
    *,
    run_id: UUID,
    entry: Dict[str, Any],
    probe: str,
    duration_ns: Optional[int] = None,
    **metadata: Any,
) -> None:
    """
    Emit one callback observation using the run's stable normalized lineage.

    The callback run ID is the operation's authoritative span identity. It is
    assigned to the normalized event before the event enters the emitter.
    """
    stable_metadata = {
        "langgraph_kind": entry["kind"],
        **entry.get("framework_metadata", {}),
    }
    if "is_langgraph_root" in entry:
        stable_metadata["is_langgraph_root"] = entry[
            "is_langgraph_root"
        ]

    event = NormalizedEvent.now(
        probe=probe,
        trace_id=entry["trace_id"],
        service="langgraph",
        name=entry["name"],
        parent_span_id=entry.get("parent_span_id"),
        duration_ns=duration_ns,
        **stable_metadata,
        **metadata,
    )
    event.span_id = str(run_id)
    emit(event)


def _duration_ns(entry: Dict[str, Any]) -> Optional[int]:
    """Return elapsed duration when the matching start callback was observed."""
    started_at = entry.get("t0")
    if started_at is None:
        return None
    return int((time.perf_counter() - started_at) * 1e9)


class OriginTracerCallbackHandler(BaseCallbackHandler):
    """
    Translate callback lifecycles into normalized causal observations.

    One handler is shared process-wide. Per-run state is keyed by callback run
    ID, which makes interleaved and parallel callbacks independent of arrival
    order. All LangGraph semantics remain metadata for framework-specific rules.
    """

    def on_chain_start(
        self,
        serialized: Dict[str, Any],
        inputs: Dict[str, Any],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:

        # import pdb
        # pdb.set_trace()

        framework_meta = metadata or {}
        node_name = (
            framework_meta.get("langgraph_node")
            or (serialized or {}).get("name")
            or "chain"
        )
        entry = _remember_run(
            run_id=run_id,
            parent_run_id=parent_run_id,
            name=node_name,
            kind="chain",
            metadata=metadata,
            is_langgraph_root="langgraph_node"
            not in framework_meta,
        )
        _emit_run_event(
            run_id=run_id,
            entry=entry,
            probe="langgraph.chain.enter",
            input_keys=(
                sorted(str(key) for key in inputs)
                if isinstance(inputs, dict)
                else None
            ),
            langgraph_tags=[
                str(tag)[:200] for tag in (tags or [])[:50]
            ],
        )

    def on_chain_end(
        self,
        outputs: Any,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        entry = _take_run(run_id)
        if entry is None:
            return
        _emit_run_event(
            run_id=run_id,
            entry=entry,
            probe="langgraph.chain.exit",
            duration_ns=_duration_ns(entry),
            output_keys=(
                sorted(str(key) for key in outputs)
                if isinstance(outputs, dict)
                else None
            ),
        )

    def on_chain_error(
        self,
        error: BaseException,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        entry = _take_run(run_id) or _fallback_run(
            run_id, parent_run_id, name="chain", kind="chain"
        )
        _emit_run_event(
            run_id=run_id,
            entry=entry,
            probe="langgraph.error",
            duration_ns=_duration_ns(entry),
            exception_type=type(error).__name__,
            exception_msg=str(error)[:200],
            source="on_chain_error",
        )

    def on_tool_start(
        self,
        serialized: Dict[str, Any],
        input_str: str,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:

        # import pdb
        # pdb.set_trace()

        tool_args = (input_str or "")[:200]
        entry = _remember_run(
            run_id=run_id,
            parent_run_id=parent_run_id,
            name=(serialized or {}).get("name", "tool"),
            kind="tool",
            metadata=kwargs.get("metadata"),
            tool_args=tool_args,
        )
        _emit_run_event(
            run_id=run_id,
            entry=entry,
            probe="langgraph.tool.enter",
            tool_args=tool_args,
            **_tool_content_metadata(
                input_str, "tool_input_preview"
            ),
        )

    def on_tool_end(
        self,
        output: Any,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        entry = _take_run(run_id)
        if entry is None:
            return
        _emit_run_event(
            run_id=run_id,
            entry=entry,
            probe="langgraph.tool.exit",
            duration_ns=_duration_ns(entry),
            tool_args=entry.get("tool_args", ""),
            success=True,
            **_tool_content_metadata(
                output, "tool_output_preview"
            ),
        )

    def on_tool_error(
        self,
        error: BaseException,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        entry = _take_run(run_id) or _fallback_run(
            run_id, parent_run_id, name="tool", kind="tool"
        )
        _emit_run_event(
            run_id=run_id,
            entry=entry,
            probe="langgraph.error",
            duration_ns=_duration_ns(entry),
            tool_args=entry.get("tool_args", ""),
            success=False,
            exception_type=type(error).__name__,
            exception_msg=str(error)[:200],
            source="on_tool_error",
            **_tool_content_metadata(
                error, "tool_error_preview"
            ),
        )

    def on_llm_start(
        self,
        serialized: Dict[str, Any],
        prompts: List[str],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:

        # import pdb
        # pdb.set_trace()

        entry = _remember_run(
            run_id=run_id,
            parent_run_id=parent_run_id,
            name=(serialized or {}).get("name", "llm"),
            kind="llm",
            metadata=kwargs.get("metadata"),
        )
        _emit_run_event(
            run_id=run_id,
            entry=entry,
            probe="langgraph.llm.enter",
            prompt_count=len(prompts) if prompts else 0,
            **_llm_content_metadata(prompts, "prompt_preview"),
        )

    # Chat models use the same callback shape and lineage semantics.
    on_chat_model_start = on_llm_start  # type: ignore[assignment]

    def on_llm_end(
        self,
        response: Any,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        entry = _take_run(run_id)
        if entry is None:
            return

        token_usage: Dict[str, Any] = {}
        llm_output = getattr(response, "llm_output", None) or {}
        if isinstance(llm_output, dict):
            token_usage = llm_output.get("token_usage", {}) or {}

        made_tool_call = False
        try:
            for generation_list in (
                getattr(response, "generations", None) or []
            ):
                for generation in generation_list:
                    message = getattr(
                        generation, "message", None
                    )
                    if message is not None and getattr(
                        message, "tool_calls", None
                    ):
                        made_tool_call = True
        except Exception:
            logger.debug(
                "langgraph probe: could not inspect model generations",
                exc_info=True,
            )

        _emit_run_event(
            run_id=run_id,
            entry=entry,
            probe="langgraph.llm.exit",
            duration_ns=_duration_ns(entry),
            prompt_tokens=token_usage.get("prompt_tokens"),
            completion_tokens=token_usage.get(
                "completion_tokens"
            ),
            made_tool_call=made_tool_call,
            **_llm_content_metadata(
                response, "response_preview"
            ),
        )

    def on_llm_error(
        self,
        error: BaseException,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        entry = _take_run(run_id) or _fallback_run(
            run_id, parent_run_id, name="llm", kind="llm"
        )
        _emit_run_event(
            run_id=run_id,
            entry=entry,
            probe="langgraph.error",
            duration_ns=_duration_ns(entry),
            exception_type=type(error).__name__,
            exception_msg=str(error)[:200],
            source="on_llm_error",
        )


# LangChain consults this hook whenever it assembles a callback manager.  In the
# manual Uvicorn adapter, the handler is already present in this ContextVar and
# ``inheritable=True`` carries it into the graph task and its child runs.
_handler_var: ContextVar[
    Optional[OriginTracerCallbackHandler]
] = ContextVar("origintracer_langgraph_handler", default=None)

# Agent Server's built-in run routes are created by the server after the custom
# application's lifespan context, so ``_handler_var`` is empty there.  Supplying
# ``handle_class`` and ``env_var`` lets LangChain create the same handler in that
# context when the probe is active.  This avoids modifying Agent Server routes,
# adding callbacks to every graph invocation, or changing REPL discovery.
_HANDLER_ENV = "ORIGINTRACER_LANGGRAPH_ENABLED"
register_configure_hook(
    _handler_var,
    inheritable=True,
    handle_class=OriginTracerCallbackHandler,
    env_var=_HANDLER_ENV,
)

_installed = False
_install_lock = threading.Lock()
_previous_handler_env: Optional[str] = None


class LangGraphProbe(BaseProbe):
    """Install and remove the process callback observer used by this module."""

    name = "langgraph"

    def start(self) -> None:
        """Enable callbacks for both inherited and server-created contexts."""
        global _installed, _previous_handler_env
        with _install_lock:
            if _installed:
                logger.warning(
                    "langgraph probe: already installed - skipping"
                )
                return
            # The ContextVar serves manual Uvicorn.  The process environment is
            # the fallback signal used by Agent Server's separate request tasks.
            _previous_handler_env = os.environ.get(_HANDLER_ENV)
            os.environ[_HANDLER_ENV] = "1"
            _handler_var.set(OriginTracerCallbackHandler())
            _installed = True
        logger.info(
            "langgraph probe: installed (global callback hook)"
        )

    def stop(self) -> None:
        """Disable both callback paths and discard unfinished run state."""
        global _installed, _previous_handler_env
        with _install_lock:
            if not _installed:
                return
            _handler_var.set(None)
            # Restore the environment exactly as it was before probe startup so
            # stopping OriginTracer cannot leave callbacks enabled process-wide.
            if _previous_handler_env is None:
                os.environ.pop(_HANDLER_ENV, None)
            else:
                os.environ[_HANDLER_ENV] = _previous_handler_env
            _previous_handler_env = None
            with _active_runs_lock:
                _active_runs.clear()
            _installed = False
        logger.info("langgraph probe: removed")
