"""Send one request to the personal-assistant graph running in Agent Server.

This is the example's smallest client entry point. It uses only the Python
standard library, waits for the stateless run to finish, and prints the final
assistant response. Repeated executions create additional OriginTracer traces
inside the long-running Agent Server process.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_PROMPT = (
    "Schedule a one-hour design meeting next Tuesday at 2pm with "
    "design@example.com, and email design@example.com a reminder to review "
    "the new mockups."
)


def _arguments() -> argparse.Namespace:
    """Parse the small set of options useful when invoking a deployment."""
    parser = argparse.ArgumentParser(
        description="Invoke the OriginTracer personal-assistant example."
    )
    parser.add_argument(
        "prompt",
        nargs="?",
        default=DEFAULT_PROMPT,
        help="request for the personal assistant",
    )
    parser.add_argument(
        "--url",
        default=os.getenv(
            "LANGGRAPH_API_URL", "http://localhost:8123"
        ),
        help="Agent Server base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("LANGGRAPH_API_KEY"),
        help="optional Agent Server API key",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help="seconds to wait for the local model (default: %(default)s)",
    )
    return parser.parse_args()


def _final_content(result: Any) -> Any:
    """Return the final message content when the response has agent state."""
    if not isinstance(result, dict):
        return result
    messages = result.get("messages")
    if not isinstance(messages, list) or not messages:
        return result
    final_message = messages[-1]
    if not isinstance(final_message, dict):
        return final_message
    return final_message.get("content", final_message)


def invoke_and_print(
    *,
    assistant_id: str,
    graph_input: dict[str, Any],
    url: str | None = None,
    api_key: str | None = None,
    timeout: float = 300.0,
) -> int:
    """Submit one stateless Agent Server run and print its final output.

    The dedicated rule-scenario scripts reuse this transport while retaining
    their own explicit graph IDs and inputs. Environment variables provide the
    same deployment overrides as the command-line options in this module.
    """
    server_url = url or os.getenv(
        "LANGGRAPH_API_URL", "http://localhost:8123"
    )
    resolved_api_key = api_key or os.getenv("LANGGRAPH_API_KEY")
    endpoint = f"{server_url.rstrip('/')}/runs/wait"
    payload = {
        "assistant_id": assistant_id,
        "input": graph_input,
    }
    headers = {"Content-Type": "application/json"}
    if resolved_api_key:
        headers["X-Api-Key"] = resolved_api_key

    request = Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )

    print(f"Agent Server: {server_url}")
    print(f"Graph: {assistant_id}")
    print(f"Input: {json.dumps(graph_input)}\n")
    try:
        with urlopen(request, timeout=timeout) as response:
            result = json.load(response)
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(
            f"Agent Server returned HTTP {exc.code}: {detail}",
            file=sys.stderr,
        )
        return 1
    except URLError as exc:
        print(
            f"Could not reach Agent Server: {exc.reason}",
            file=sys.stderr,
        )
        return 1

    content = _final_content(result)
    if isinstance(content, str):
        print(content)
    else:
        print(json.dumps(content, indent=2))
    return 0


def main() -> int:
    """Submit one normal personal-assistant run and display its final output."""
    args = _arguments()
    return invoke_and_print(
        assistant_id="personal_assistant",
        graph_input={
            "messages": [
                {"role": "user", "content": args.prompt}
            ],
        },
        url=args.url,
        api_key=args.api_key,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    raise SystemExit(main())
