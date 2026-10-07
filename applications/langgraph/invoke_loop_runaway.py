"""Trigger the LangGraph ``loop_runaway`` causal rule.

The targeted graph performs nine bounded calls to the same Ollama model during
one outer invocation. The rule fires at eight calls per owning graph invocation,
so the scenario demonstrates a runaway signature while still terminating safely.
Run it against a freshly started Agent Server for the clearest aggregate counts.
"""

from invoke import invoke_and_print


def main() -> int:
    """
    Invoke the bounded model-loop scenario through Agent Server.
    """
    return invoke_and_print(
        assistant_id="loop_runaway_scenario",
        graph_input={"iteration": 0},
    )


if __name__ == "__main__":
    raise SystemExit(main())
