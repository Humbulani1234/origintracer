"""
Trigger the LangGraph ``agent_latency_hotspot`` causal rule.

The targeted graph starts two sibling tool branches concurrently. One completes
quickly and the other waits for one second, causing the slow tool to account for
well over 40 percent of aggregate leaf-work time. The scenario also exercises
OriginTracer's parent-aware handling of parallel LangGraph branches.
"""

from invoke import invoke_and_print


def main() -> int:
    """
    Invoke the parallel latency-hotspot scenario through Agent Server.
    """
    return invoke_and_print(
        assistant_id="agent_latency_hotspot_scenario",
        graph_input={"request": "compare two dependency calls"},
    )


if __name__ == "__main__":
    raise SystemExit(main())
