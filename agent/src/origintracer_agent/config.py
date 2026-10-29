"""Settings for the agent's connection to OriginTracer and its model."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

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
    """Return the Ollama model identifier used by the investigation agent."""
    return os.getenv("ORIGINTRACER_AGENT_MODEL", "qwen3:4b")


def ollama_model_tag(value: str) -> str:
    """Return the Ollama model tag accepted by ChatOllama."""
    return value.removeprefix("ollama:")


def ollama_base_url() -> str:
    """Return the Ollama server URL, which may point to another LAN host."""
    return os.getenv(
        "OLLAMA_BASE_URL", "http://localhost:11434"
    ).rstrip("/")
