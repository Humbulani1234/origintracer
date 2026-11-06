from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, Type

logger = logging.getLogger("origintracer.probes")


class BaseProbe(ABC):
    """Base contract for instrumenting a supported runtime or framework.

    Subclass requirements
    ---------------------
    * Set a unique ``name`` matching the key used in the probe configuration.
    * Implement ``start()`` to attach hooks, patches, or middleware.
    * Implement ``stop()`` to detach instrumentation and clean up.

    Attributes
    ----------
    name : str
        Unique probe name used by application configuration.

    Notes
    -----
    Probes emit observations through ``sdk.emitter.emit(event)`` and do not
    import ``Engine``, ``RuntimeGraph``, or other core implementation layers.
    """

    name: str = ""  # Override in each subclass

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if cls.name:
            ProbeRegistry.register(cls)

    @abstractmethod
    def start(self) -> None:
        """Attach instrumentation for this probe.

        Called once during OriginTracer startup.

        Returns
        -------
        None
            Installs this probe's hooks or callbacks.
        """

    @abstractmethod
    def stop(self) -> None:
        """Detach instrumentation and release probe resources.

        Called during OriginTracer shutdown.

        Returns
        -------
        None
            Removes this probe's hooks or callbacks.
        """

    def __repr__(self) -> str:
        return f"<Probe:{self.name}>"


class ProbeRegistry:
    """Register probe classes and create instances from configuration.

    Subclasses register automatically through ``BaseProbe.__init_subclass__``
    when they define a non-empty ``name`` attribute.
    """

    _registry: Dict[str, Type[BaseProbe]] = {}

    @classmethod
    def register(
        cls, probe_class: Type[BaseProbe]
    ) -> Type[BaseProbe]:
        """Register a probe class by its configured name.

        Parameters
        ----------
        probe_class : Type[BaseProbe]
            Probe implementation to register.

        Returns
        -------
        Type[BaseProbe]
            The registered class, allowing this method to be used as a
            decorator.
        """
        cls._registry[probe_class.name] = probe_class
        return probe_class

    @classmethod
    def get(cls, name: str) -> Optional[Type[BaseProbe]]:
        """Look up a registered probe class.

        Parameters
        ----------
        name : str
            Configured probe name.

        Returns
        -------
        Type[BaseProbe] or None
            Matching probe class, or ``None`` when the name is unknown.
        """
        return cls._registry.get(name)

    @classmethod
    def instantiate(cls, name: str) -> BaseProbe:
        """Create an instance of a registered probe.

        Parameters
        ----------
        name : str
            Configured probe name.

        Returns
        -------
        BaseProbe
            New, not-yet-started probe instance.

        Raises
        ------
        KeyError
            If no probe is registered under ``name``.
        """
        probe_class = cls._registry.get(name)
        if probe_class is None:
            raise KeyError(
                f"Probe '{name}' not found. Registered: {list(cls._registry.keys())}"
            )
        return probe_class()

    @classmethod
    def available(cls) -> list:
        """List names of registered probe classes.

        Returns
        -------
        list[str]
            Registered probe names.
        """
        return list(cls._registry.keys())

    @classmethod
    def load_from_config(cls, config: dict) -> list:
        """Instantiate configured probes that are registered.

        Unknown names are logged and skipped; returned instances have not yet
        been started.

        Parameters
        ----------
        config : dict
            Configuration mapping whose ``probes`` entry is a list of names.

        Returns
        -------
        list[BaseProbe]
            Instantiated probes in configuration order.
        """
        probe_names = config.get("probes", [])
        probes = []
        for name in probe_names:
            try:
                probes.append(cls.instantiate(name))
                logger.info("Loaded probe: %s", name)
            except KeyError as exc:
                logger.warning(
                    "Unknown probe in config: %s", exc
                )
        return probes
