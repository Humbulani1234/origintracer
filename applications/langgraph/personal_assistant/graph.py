"""A locally hosted personal-assistant graph built with LangChain and Ollama.

The supervisor delegates calendar and email work to two specialist agents. Each
specialist is exposed as a tool, which gives the supervisor freedom to request
independent work concurrently. The tools are deliberately harmless stubs: they
demonstrate an agent topology without creating real events or sending email.

``supervisor_agent`` is a compiled LangGraph graph. Agent Server imports it once
at process startup through the entry in ``langgraph.json`` and reuses it for
incoming runs. OriginTracer is initialized separately by the FastAPI lifespan in
``webapp.py`` before Agent Server accepts requests.
"""

from __future__ import annotations

import os
from datetime import date

from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain.tools import tool


@tool
def create_calendar_event(
    title: str,
    start_time: str,
    end_time: str,
    attendees: list[str],
    location: str = "",
) -> str:
    """Create a calendar event using ISO-formatted start and end datetimes."""
    return (
        f"Event created: {title} from {start_time} to {end_time} "
        f"with {len(attendees)} attendees"
    )


@tool
def send_email(
    to: list[str],
    subject: str,
    body: str,
    cc: list[str] | None = None,
) -> str:
    """Send an email to the supplied addresses using the generated content."""
    copied = f"; cc: {', '.join(cc)}" if cc else ""
    return f"Email sent to {', '.join(to)}; subject: {subject}{copied}"


@tool
def get_available_time_slots(
    attendees: list[str],
    date: str,
    duration_minutes: int,
) -> list[str]:
    """Return available times for attendees on an ISO-formatted date."""
    return ["09:00", "14:00", "16:00"]


# A different locally installed Ollama model can be selected without changing
# the graph source. The model must support tool calling for this example.
model_name = os.getenv("OLLAMA_MODEL", "qwen3:4b")
model_options: dict[str, object] = {"temperature": 0}
if ollama_base_url := os.getenv("OLLAMA_BASE_URL"):
    model_options["base_url"] = ollama_base_url
model = init_chat_model(f"ollama:{model_name}", **model_options)


calendar_agent = create_agent(
    model,
    tools=[create_calendar_event, get_available_time_slots],
    name="calendar_agent",
    system_prompt=(
        f"Today's date is {date.today().isoformat()}. "
        "You are a calendar scheduling assistant. Parse natural-language "
        "scheduling requests into ISO datetimes. Check availability when "
        "needed, create the event, and confirm what was scheduled. If required "
        "details are missing or no suitable slot exists, explain that clearly."
    ),
)


email_agent = create_agent(
    model,
    tools=[send_email],
    name="email_agent",
    system_prompt=(
        "You are an email assistant. Compose a professional subject and body "
        "from the request, send the message with send_email, and confirm what "
        "was sent. Do not invent a recipient when no address is supplied."
    ),
)


@tool
def schedule_event(request: str) -> str:
    """Schedule or check a calendar event from a natural-language request."""
    result = calendar_agent.invoke(
        {"messages": [{"role": "user", "content": request}]}
    )
    return result["messages"][-1].text


@tool
def manage_email(request: str) -> str:
    """Compose and send an email from a natural-language request."""
    result = email_agent.invoke(
        {"messages": [{"role": "user", "content": request}]}
    )
    return result["messages"][-1].text


supervisor_agent = create_agent(
    model,
    tools=[schedule_event, manage_email],
    name="personal_assistant",
    system_prompt=(
        "You are a helpful personal assistant that coordinates calendar and "
        "email work. Break a request into the required specialist tool calls. "
        "When actions are independent, request them together so they may run "
        "concurrently. Combine the specialists' results into a clear response."
    ),
)
