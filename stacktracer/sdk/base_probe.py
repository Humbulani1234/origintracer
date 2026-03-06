"""
sdk/base_probe.py + sdk/registry.py combined

BaseProbe defines the probe contract.
ProbeRegistry enables dynamic loading from YAML config.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, Type

logger = logging.getLogger("stacktracer.probes")


# ====================================================================== #
# Base Probe
# ====================================================================== #

class BaseProbe(ABC):
    """
    All probes inherit from this class.

    Subclass requirements
    ---------------------
    - Set class attribute `name: str`  (must be unique, matches YAML config key)
    - Implement `start()` — attach hooks, monkey-patch, or register middleware
    - Implement `stop()`  — detach and clean up

    Probes communicate ONLY via `sdk.emitter.emit(event)`.
    They NEVER import Engine, RuntimeGraph, or any core layer directly.
    """

    name: str = ""          # Override in each subclass

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if cls.name:
            ProbeRegistry.register(cls)

    @abstractmethod
    def start(self) -> None:
        """Attach the probe. Called once at startup."""

    @abstractmethod
    def stop(self) -> None:
        """Detach the probe and reverse any monkey-patching."""

    def __repr__(self) -> str:
        return f"<Probe:{self.name}>"


# ====================================================================== #
# Probe Registry
# ====================================================================== #

class ProbeRegistry:
    _registry: Dict[str, Type[BaseProbe]] = {}

    @classmethod
    def register(cls, probe_class: Type[BaseProbe]) -> Type[BaseProbe]:
        """Register a probe class. Called automatically via __init_subclass__."""
        cls._registry[probe_class.name] = probe_class
        return probe_class

    @classmethod
    def get(cls, name: str) -> Optional[Type[BaseProbe]]:
        return cls._registry.get(name)

    @classmethod
    def instantiate(cls, name: str) -> BaseProbe:
        probe_class = cls._registry.get(name)
        if probe_class is None:
            raise KeyError(
                f"Probe '{name}' not found. Registered: {list(cls._registry.keys())}"
            )
        return probe_class()

    @classmethod
    def available(cls) -> list:
        return list(cls._registry.keys())

    @classmethod
    def load_from_config(cls, config: dict) -> list:
        """
        Instantiate probes from a config dict:
            { "probes": ["django", "asyncio", "kernel"] }
        Returns list of instantiated (not yet started) probes.
        """
        probe_names = config.get("probes", [])
        probes = []
        for name in probe_names:
            try:
                probes.append(cls.instantiate(name))
                logger.info("Loaded probe: %s", name)
            except KeyError as exc:
                logger.warning("Unknown probe in config: %s", exc)
        return probes