"""Settings for the agent's connection to OriginTracer and its model."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langchain.chat_models import init_chat_model

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / ".env.example"
load_dotenv(_ENV_FILE, override=False)


def backend_url() -> str:
    """Return the configured OriginTracer backend URL without a trailing slash."""
    return os.getenv(
        "ORIGINTRACER_API_URL", "http://localhost:8001"
    ).rstrip("/")


def backend_api_key() -> str:
    """Return the API key used by this service for backend requests."""
    return os.getenv("ORIGINTRACER_API_KEY", "")


def model_name() -> str:
    """Return the provider-prefixed model identifier for the agent."""
    return os.getenv("ORIGINTRACER_AGENT_MODEL", "qwen3:4b")


def ollama_model_tag(value: str) -> str:
    """Return the Ollama model tag accepted by ChatOllama."""
    return value.removeprefix("ollama:")


def ollama_base_url() -> str:
    """Return the Ollama server URL, which may point to another LAN host."""
    return os.getenv(
        "OLLAMA_BASE_URL", "http://localhost:11434"
    ).rstrip("/")


def configured_chat_model(
    model_spec: str, *, temperature: float | None = None
) -> Any:
    """Build a LangChain chat model from ``provider:model`` configuration.

    Unprefixed model names retain the existing Ollama behavior. Groq models
    use ``groq:<model-id>`` with ``GROQ_API_KEY``; Mistral models use
    ``mistral:<model-id>`` with ``MISTRAL_API_KEY``.
    """
    provider, separator, model = model_spec.partition(":")
    if not separator or provider not in {
        "ollama",
        "groq",
        "mistral",
    }:
        provider, model = "ollama", model_spec
    if not model:
        raise ValueError(
            "Model configuration must include a model after the provider."
        )
    model_options: dict[str, Any] = (
        {"temperature": temperature}
        if temperature is not None
        else {}
    )

    if provider == "ollama":
        return init_chat_model(
            model,
            model_provider="ollama",
            base_url=ollama_base_url(),
            **model_options,
        )

    key_name = (
        "GROQ_API_KEY"
        if provider == "groq"
        else "MISTRAL_API_KEY"
    )
    api_key = os.getenv(key_name, "").strip()
    if not api_key:
        raise ValueError(
            f"{key_name} is required when the configured model uses "
            f"the {provider}: provider."
        )
    if provider == "groq":
        return init_chat_model(
            model,
            model_provider="groq",
            api_key=api_key,
            **model_options,
        )
    return init_chat_model(
        model,
        model_provider="mistralai",
        api_key=api_key,
        **model_options,
    )
