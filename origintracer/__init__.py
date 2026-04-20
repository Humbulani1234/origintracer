"""
Minimal usage, most settings come from defaults:

    import origintracer
    origintracer.init(api_key="test-key-123")
    MIDDLEWARE = ["origintracer.probes.django_probe.TracerMiddleware", ...]

Config merge order:
    1. Package defaults - origintracer/config/defaults.yaml
    2. User yaml file - searched from cwd upward, or explicit config= path
    3. init() kwargs - highest priority, overrides everything
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os
import signal
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Type

import yaml

from origintracer.utils.logging import setup_file_logging

from .context.vars import get_trace_id
from .core.active_requests import ActiveRequestTracker
from .core.causal import PatternRegistry
from .core.engine import Engine
from .core.event_schema import NormalizedEvent
from .core.graph_compactor import GraphCompactor
from .core.graph_normalizer import GraphNormalizer
from .core.local_server import LocalQueryServer
from .core.runtime_graph import RuntimeGraph
from .core.semantic import SemanticLayer
from .sdk.base_probe import BaseProbe, ProbeRegistry
from .sdk.emitter import bind_engine, emit
from .sdk.uploader import Uploader

logger = logging.getLogger("origintracer.initialisation")

# Package-level variables
_config: Optional["ResolvedConfig"] = None
_engine: Optional[Engine] = None
_active_rules: Optional[Type[PatternRegistry]] = None
_active_probes: List[BaseProbe] = []
_uploader: Optional[Uploader] = None
_post_init_callbacks: List[Callable] = []
_local_server: Optional[LocalQueryServer] = None


# Used for probes that construct structural topology of the system
def _register_post_init_callback(fn: Callable) -> None:
    """
    Register a function to call once after init() completes.
    """
    if _engine is not None:
        fn()
    else:
        _post_init_callbacks.append(fn)


# ----------- Raw config loading and merging ---------------


def _load_package_defaults() -> Dict[str, Any]:
    """
    Load the package-shipped defaults.yaml.
    Lives at origintracer/config/defaults.yaml.
    If missing, returns empty dict and logs a warning.
    """
    defaults_path = os.path.join(
        os.path.dirname(__file__), "config", "defaults.yaml"
    )
    if not os.path.exists(defaults_path):
        logger.warning(
            "origintracer: defaults.yaml missing from package installation at %s. "
            "Reinstall: pip install --force-reinstall origintracer. "
            "Running with empty defaults - some features may be unavailable.",
            defaults_path,
        )
        return {}
    with open(defaults_path) as f:
        data = yaml.safe_load(f) or {}
    logger.debug(
        "Package defaults loaded from %s", defaults_path
    )
    return data


def _find_user_config(
    explicit_path: Optional[str],
) -> Optional[str]:
    """
    Locate the user's origintracer.yaml.

    Search order:
        1. explicit_path if provided
        2. ORIGINTRACER_CONFIG environment variable
        3. Walk up from cwd looking for origintracer.yaml
    """
    if explicit_path:
        if os.path.exists(explicit_path):
            return explicit_path
        logger.warning(
            "Explicit config path not found: %s", explicit_path
        )
        return None

    env_path = os.getenv("ORIGINTRACER_CONFIG")
    if env_path and os.path.exists(env_path):
        return env_path

    current = os.getcwd()
    for _ in range(5):
        candidate = os.path.join(current, "origintracer.yaml")
        if os.path.exists(candidate):
            return candidate
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent

    return None


def _load_user_config(path: Optional[str]) -> Dict[str, Any]:
    """
    Load user yaml if found, otherwise empty dict.
    """
    if path is None:
        return {}
    try:
        import yaml

        with open(path) as f:
            data = yaml.safe_load(f) or {}
        logger.info("User config loaded from %s", path)
        return data
    except Exception as exc:
        logger.warning(
            "Could not load user config %s: %s", path, exc
        )
        return {}


def _deep_merge(base: Dict, override: Dict) -> Dict:
    result = dict(base)
    for key, val in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(val, dict)
        ):
            result[key] = _deep_merge(result[key], val)
        elif (
            key == "semantic"
            and key in result
            and isinstance(val, list)
        ):
            # Pass an empty list for the init_kwarg since we are only
            # merging the file-based configurations here.
            result[key] = _merge_semantic(
                defaults=result[key],
                user_yaml=val,
                init_kwarg=[],
            )
        elif (
            key == "normalize"
            and key in result
            and isinstance(val, list)
        ):
            # For an empty list, clear the defaults
            # For provided rules, override the defaults
            if not val:
                result[key] = []
            else:
                result[key] = val
        else:
            result[key] = val
    return result


def _merge_semantic(
    defaults: List[Dict],
    user_yaml: List[Dict],
    init_kwarg: Optional[List[Dict]],
) -> List[Dict]:
    """
    Three-way label-keyed merge of semantic alias lists.
    Later sources win on the same label. Distinct labels from all sources kept.
    """
    merged: Dict[str, Dict] = {}
    for source in (defaults, user_yaml, init_kwarg or []):
        for entry in source or []:
            label = entry.get("label", "")
            if label:
                merged[label] = entry
    return list(merged.values())


def _extend_normalize(
    merged_yaml_rules: List[Dict],
    init_kwarg_rules: Optional[List[Dict]],
) -> List[Dict]:
    """
    Normalize rules are additive - kwarg rules extend the yaml list.
    User wanting a clean slate sets normalize: [] in their yaml first.
    """
    base = list(merged_yaml_rules)
    if init_kwarg_rules:
        base.extend(init_kwarg_rules)
    return base


# ----------------- ResolvedConfig ----------------------------


@dataclass
class ResolvedConfig:
    """
    Fully resolved configuration after merging defaults.yaml,
    user origintracer.yaml, and init() kwargs.
    Built once in init() and stored as _config.
    """

    api_key: str
    endpoint: str
    buffer_size: int
    flush_interval: int
    snapshot_interval: float
    probes: List[str]
    rules: List[str]
    builtin_probes: List[str]
    builtin_rules: List[str]
    semantic: List[Dict]
    normalize: List[Dict]
    compactor: Dict[str, Any]
    active_requests: Dict[str, Any]
    debug: bool
    enabled: bool
    config_path: Optional[str]

    def __post_init__(self) -> None:
        if (
            os.getenv("DJANGO_DEBUG", "false").lower() == "true"
            and not self.debug
        ):
            logger.info(
                "OriginTracer: DJANGO_DEBUG=True - disabling. "
                "Pass debug=True to origintracer.init() to enable in dev."
            )
            self.enabled = False


def _build_resolved_config(
    merged_yaml: Dict[str, Any],
    api_key: str,
    endpoint: str,
    probes: Optional[List[str]],
    rules: Optional[List[str]],
    semantic: Optional[List[Dict]],
    snapshot_interval: Optional[float],
    flush_interval: Optional[int],
    debug: bool,
    config_path: Optional[str],
    normalize: Optional[List[Dict]],
    compactor: Optional[Dict],
    active_requests: Optional[Dict],
) -> ResolvedConfig:
    """
    Apply init() kwargs as the final override layer on top of merged yaml.
    merged_yaml = _deep_merge(defaults.yaml, user origintracer.yaml).
    """
    resolved_semantic = _merge_semantic(
        defaults=merged_yaml.get("semantic", []),
        user_yaml=[],  # already in merged_yaml
        init_kwarg=semantic,
    )
    resolved_normalize = _extend_normalize(
        merged_yaml_rules=merged_yaml.get("normalize", []),
        init_kwarg_rules=normalize,
    )

    return ResolvedConfig(
        api_key=api_key,
        endpoint=endpoint,
        buffer_size=merged_yaml.get("buffer_size", 10_000),
        flush_interval=(
            flush_interval
            if flush_interval is not None
            else merged_yaml.get("flush_interval", 10)
        ),
        snapshot_interval=(
            snapshot_interval
            if snapshot_interval is not None
            else merged_yaml.get("snapshot_interval", 15.0)
        ),
        probes=(
            probes
            if probes is not None
            else merged_yaml.get("probes", [])
        ),
        rules=(
            rules
            if rules is not None
            else merged_yaml.get("rules", [])
        ),
        builtin_probes=merged_yaml.get("builtin_probes", []),
        builtin_rules=merged_yaml.get("builtin_rules", []),
        semantic=resolved_semantic,
        normalize=resolved_normalize,
        compactor=_deep_merge(
            merged_yaml.get("compactor", {}), compactor or {}
        ),
        active_requests=_deep_merge(
            merged_yaml.get("active_requests", {}),
            active_requests or {},
        ),
        debug=debug,
        enabled=True,
        config_path=config_path,
    )


# ---------------- Component initialisation ---------------


def _init_normalizer(cfg: ResolvedConfig) -> Any:
    from .core.graph_normalizer import GraphNormalizer

    normalizer = GraphNormalizer(
        enable_builtins=True,
        max_name_length=200,
        max_unique_names_per_service=500,
    )
    for rule in cfg.normalize:
        svc = rule.get("service", "*")
        pattern = rule.get("pattern")
        repl = rule.get("replacement")
        desc = rule.get("description", "")
        if pattern and repl:
            normalizer.add_pattern(
                service=svc,
                pattern=pattern,
                replacement=repl,
                description=desc,
            )
    return normalizer


def _init_compactor(cfg: ResolvedConfig) -> Any:
    from .core.graph_compactor import GraphCompactor

    c = cfg.compactor
    return GraphCompactor(
        max_nodes=c.get("max_nodes", 5_000),
        evict_to_ratio=c.get("evict_to_ratio", 0.80),
        node_ttl_s=c.get("node_ttl_s", 3600.0),
        min_call_count=c.get("min_call_count", 5),
    )


def _init_semantic(cfg: ResolvedConfig) -> Any:
    from .core.semantic import load_from_dict

    return load_from_dict(cfg.semantic)


def _init_tracker(cfg: ResolvedConfig) -> Any:
    from .core.active_requests import ActiveRequestTracker

    ar = cfg.active_requests
    return ActiveRequestTracker(
        ttl_s=ar.get("ttl_s", 30.0),
        max_size=ar.get("max_size", 10_000),
    )


def _init_engine(
    cfg: ResolvedConfig,
    normalizer: GraphNormalizer,
    compactor: GraphCompactor,
    semantic: SemanticLayer,
    tracker: ActiveRequestTracker,
    repository: Optional[Uploader],
) -> Engine:
    graph = RuntimeGraph()
    engine = Engine(
        semantic_layer=semantic,
        snapshot_interval_s=cfg.snapshot_interval,
    )
    engine.graph = graph
    engine.compactor = compactor
    engine.tracker = tracker
    engine.normalizer = normalizer

    if repository:
        engine.repository = repository

    bind_engine(engine)
    engine.start_background_tasks()
    return engine


def _init_probes(
    cfg: ResolvedConfig, engine: Any, app_root: str
) -> list:
    """
    1. Import builtin probe modules listed in defaults.yaml under builtin_probes.

    2. Discover user probes from <app_root>/origintracer/probes/*_probe.py.

    3. Start probes named in cfg.probes (user origintracer.yaml takes precedence
       over defaults.yaml probes list via _deep_merge).
    """
    # Builtin modules from defaults.yaml — no hardcoded list
    for module_path in cfg.builtin_probes:
        try:
            importlib.import_module(
                module_path, package=__name__
            )
        except ImportError as exc:
            logger.debug(
                "Builtin probe module not available: %s — %s",
                module_path,
                exc,
            )

    # User probes — auto-discovered from app directory
    _discover_user_probes(app_root)

    # Start probes named in cfg.probes
    probes = ProbeRegistry.load_from_config(
        {"probes": cfg.probes}
    )
    started = []
    for probe in probes:
        try:
            probe.start()
            started.append(probe)
            logger.info("Probe started: %s", probe.name)
        except Exception as exc:
            logger.warning(
                "Probe %s failed to start: %s", probe.name, exc
            )

    return started


def _app_root_from_config(config_path: Optional[str]) -> str:
    """
    App root = parent directory of origintracer.yaml, or cwd if no config found.
    User probes and rules are discovered relative to this directory.
    """
    if config_path and os.path.isfile(config_path):
        return os.path.dirname(os.path.abspath(config_path))
    return os.getcwd()


def _discover_user_probes(app_root: str) -> None:
    """
    Auto-discover *_probe.py files from <app_root>/origintracer/probes/.
    Importing registers the BaseProbe subclass with ProbeRegistry as a
    side-effect of class definition.
    """
    probes_dir = os.path.join(app_root, "probes")
    if not os.path.isdir(probes_dir):
        logger.debug(
            "User probe directory not found: %s", probes_dir
        )
        return

    for fname in sorted(os.listdir(probes_dir)):
        if not fname.endswith("_probe.py"):
            continue
        full_path = os.path.join(probes_dir, fname)
        module_name = f"_origintracer_user_probe_{fname[:-3]}"
        try:
            spec = importlib.util.spec_from_file_location(
                module_name, full_path
            )
            module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
            sys.modules[module_name] = module
            spec.loader.exec_module(module)  # type: ignore[union-attr]
            logger.info("User probe discovered: %s", fname)
        except Exception:
            logger.warning(
                "User probe %s failed to load:\n%s",
                fname,
                traceback.format_exc(),
            )
            sys.modules.pop(module_name, None)


def _init_rules(
    cfg: ResolvedConfig, engine: Engine, app_root: str
) -> Type[PatternRegistry]:
    """
    1. Import builtin rules modules listed in defaults.yaml under builtin_rules.

    2. Discover user probes from <app_root>/origintracer/rules/*_rule.py.

    3. Discover rules named in cfg.rules (user origintracer.yaml takes precedence
    """
    # 1. Builtins - import triggers PatternRegistry.register(...)
    for module_path in cfg.builtin_rules:
        try:
            importlib.import_module(
                module_path, package=__name__
            )
        except ImportError as exc:
            logger.debug(
                "Builtin rule not available: %s — %s",
                module_path,
                exc,
            )

    # 2. User rules - also registers into PatternRegistry.registry
    _discover_user_rules(app_root)

    # 3. Filter to intersection with cfg.rules
    PatternRegistry.apply_filter(cfg.rules)
    return PatternRegistry


def _discover_user_rules(app_root: str) -> None:
    """
    Auto-discover *_rules.py files from <app_root>/origintracer/rules/.
    Each file must expose register(registry) which adds CausalRule instances.
    """
    import importlib.util
    import traceback

    rules_dir = os.path.join(app_root, "rules")
    if not os.path.isdir(rules_dir):
        return

    for fname in sorted(os.listdir(rules_dir)):
        if not fname.endswith("_rules.py"):
            continue
        full_path = os.path.join(rules_dir, fname)
        module_name = f"_origintracer_user_rule_{fname[:-3]}"
        try:
            spec = importlib.util.spec_from_file_location(
                module_name, full_path
            )
            module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
            sys.modules[module_name] = module
            spec.loader.exec_module(module)  # type: ignore[union-attr]

            register_fn = getattr(module, "register", None)
            if register_fn is None:
                logger.warning(
                    "User rules file %s has no register(registry) - skipped",
                    fname,
                )
                sys.modules.pop(module_name, None)
                continue

            register_fn(PatternRegistry)
            logger.info(
                "User rules loaded from %s (registry now has %d rules)",
                fname,
                len(PatternRegistry.rule_names()),
            )
        except Exception:
            logger.warning(
                "User rules %s failed to load:\n%s",
                fname,
                traceback.format_exc(),
            )
            sys.modules.pop(module_name, None)


def _init_local_server(engine: Any) -> Any:
    try:
        from .core.local_server import LocalQueryServer

        server = LocalQueryServer(engine)
        server.start()
        return server
    except Exception as exc:
        logger.warning(
            "Local query server failed to start: %s", exc
        )
        return None


def _init_uploader(
    cfg: ResolvedConfig, engine: Any
) -> Optional[Any]:
    global _uploader
    if not cfg.api_key:
        logger.debug("Uploader: no api_key - skipping")
        return None
    try:
        from origintracer.sdk.uploader import Uploader

        uploader = Uploader(
            endpoint=cfg.endpoint,
            api_key=cfg.api_key,
            flush_interval=cfg.flush_interval,
            max_batch_size=500,
        )
        uploader.bind_engine(engine)  # give uploader the engine
        uploader.start()
        engine.repository = uploader
        _uploader = uploader
        logger.info(
            "Uploader active: %s (interval=%ds)",
            cfg.endpoint,
            cfg.flush_interval,
        )
        return uploader
    except Exception as exc:
        logger.warning("Uploader failed to start: %s", exc)
        return None


# ------------------ Public init() ----------------------


def init(
    api_key: str = "test-key-123",  # for dev
    endpoint: str = "http://localhost:8001",  # adjust
    config: Optional[str] = None,
    probes: Optional[List[str]] = None,
    rules: Optional[List[str]] = None,
    semantic: Optional[List[Dict]] = None,
    snapshot_interval: Optional[float] = None,
    flush_interval: Optional[int] = None,
    debug: bool = True,
    repository: Optional[Any] = None,
    normalize: Optional[List[Dict]] = None,
    compactor: Optional[Dict] = None,
    active_requests: Optional[Dict] = None,
    otel_mode: bool = False,
) -> None:
    """
    Initialise OriginTracer.

    Config merge order (last wins):

    1. ``origintracer/config/defaults.yaml`` - package defaults, never edited
    2. ``origintracer.yaml`` - user app config, takes precedence
    3. ``init()`` kwargs `` - highest priority

    Minimal usage::

        origintracer.init()

    Typical usage in ``apps.py``::

        origintracer.init(
            config = str(BASE_DIR / "origintracer.yaml"),
            debug  = True,
        )

    Parameters
    ----------
    api_key : str, optional
        API key for remote upload to OriginTracer backend.
    endpoint : str, optional
        Backend URL. Default: ``http://localhost:8001``
    config : str, optional
        Explicit path to user ``origintracer.yaml``.
        If omitted, searched automatically from cwd upward.
    probes : list[str], optional
        List of probe names to activate. Replaces the default list entirely.
        Default: ``["django", "asyncio", "uvicorn", "gunicorn", "nginx"]``
    semantic : dict, optional
        Extra semantic alias dicts to add or override.
        Merged with defaults by label - your label takes precedence on same key.
    snapshot_interval : float, optional
        Seconds between temporal graph snapshots. Default: ``15.0``
    flush_interval : int, optional
        Seconds between uploader event batch flushes. Default: ``10``
    debug : bool, optional
        If ``True``, enables OriginTracer even when ``DJANGO_DEBUG=True``.
        Default: ``False`` — auto-disables in Django debug environments.
    repository : object, optional
        Pre-built storage backend (``PGEventRepository``, ``ClickHouseRepository``).
        Overrides the remote uploader as the event sink.
    normalize : list[dict], optional
        Additional normalization rules beyond built-in patterns and yaml rules.
        Each rule: ``{"service": "django", "pattern": "...", "replacement": "..."}``
    compactor : dict, optional
        Override specific compactor settings. Merges key-by-key.
        Keys: ``max_nodes``, ``evict_to_ratio``, ``node_ttl_s``, ``min_call_count``
    active_requests : dict, optional
        Override ActiveRequestTracker settings. Merges key-by-key.
        Keys: ``ttl_s``, ``max_size``
    """
    global _config, _engine, _active_probes, _active_rules

    setup_file_logging(debug)

    # 1. Load and merge
    package_defaults = _load_package_defaults()
    user_config_path = _find_user_config(config)
    user_yaml = _load_user_config(user_config_path)
    merged_yaml = _deep_merge(package_defaults, user_yaml)
    app_root = _app_root_from_config(user_config_path)

    # 2. Build ResolvedConfig
    _config = _build_resolved_config(
        merged_yaml=merged_yaml,
        api_key=api_key,
        endpoint=endpoint,
        probes=probes,
        rules=rules,
        semantic=semantic,
        snapshot_interval=snapshot_interval,
        flush_interval=flush_interval,
        debug=debug,
        config_path=user_config_path,
        normalize=normalize,
        compactor=compactor,
        active_requests=active_requests,
    )

    if not _config.enabled:
        logger.info("OriginTracer disabled — not initialising")
        return

    # 3. Initialise components
    normalizer = _init_normalizer(_config)
    compactor_ = _init_compactor(_config)
    semantic_layer = _init_semantic(_config)
    tracker = _init_tracker(_config)

    _engine = _init_engine(
        cfg=_config,
        normalizer=normalizer,
        compactor=compactor_,
        semantic=semantic_layer,
        tracker=tracker,
        repository=repository,
    )

    _local_server = _init_local_server(_engine)

    _active_rules = _init_rules(_config, _engine, app_root)
    _engine.causal = _active_rules

    # Opentelemetry check
    if not otel_mode:
        _active_probes = _init_probes(_config, _engine, app_root)
        _engine.probes = _active_probes

    if repository is None:
        _init_uploader(_config, _engine)

    # post-init callbacks (e.g. gunicorn pre-fork event drain)
    for cb in _post_init_callbacks:
        try:
            cb()
        except Exception as exc:
            logger.warning("post-init callback failed: %s", exc)
    _post_init_callbacks.clear()
    logger.info(
        "OriginTracer ready | probes=%s config=%s app_root=%s",
        [p.name for p in _active_probes],
        user_config_path or "defaults only",
        app_root,
    )

    signal.signal(
        signal.SIGINT, _make_signal_handler(signal.SIGINT)
    )
    signal.signal(
        signal.SIGTERM, _make_signal_handler(signal.SIGTERM)
    )


def get_config() -> "ResolvedConfig":
    if _config is None:
        raise RuntimeError(
            "origintracer.init() has not been called"
        )
    return _config


def get_engine() -> Any:
    return _engine


def _make_signal_handler(signum):
    old_handler = signal.getsignal(signum)

    def handler(sig, frame):
        shutdown()
        if callable(old_handler):
            old_handler(sig, frame)

    return handler


def shutdown() -> None:
    global _active_probes, _engine, _uploader, _config, _local_server

    for probe in _active_probes:
        try:
            probe.stop()
        except Exception as exc:
            logger.debug(
                "Probe %s stop error: %s", probe.name, exc
            )
    _active_probes = []

    if _local_server:
        try:
            _local_server.stop()
        except Exception as exc:
            logger.debug("Local server stop error: %s", exc)
        _local_server = None

    if _uploader:
        try:
            _uploader.stop()
        except Exception as exc:
            logger.debug("Uploader stop error: %s", exc)
        _uploader = None

    if _engine:
        _engine.stop()
        _engine = None

    _config = None
    _post_init_callbacks: List[Callable] = []
    _active_rules: None
    logger.info("OriginTracer shut down")


def mark_deployment(label: str = "deployment") -> None:
    if _engine:
        _engine.mark_deployment(
            label
        )  # writes to local TemporalStore
    if _uploader:
        _uploader.send_deployment_marker(
            label
        )  # tells FastAPI backend
    else:
        logger.warning("mark_deployment called before init()")


__version__ = "0.1.0"  # use in official releases
__all__ = ["init"]
