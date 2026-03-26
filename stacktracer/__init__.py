"""
Minimal usage, most settings come from defaults:

    import stacktracer
    stacktracer.init(api_key="test-key-123")
    MIDDLEWARE = ["stacktracer.probes.django_probe.TracerMiddleware", ...]

Config merge order:
    1. Package defaults — stacktracer/config/defaults.yaml
    2. User yaml file — searched from cwd upward, or explicit config= path
    3. init() kwargs — highest priority, overrides everything
"""

from __future__ import annotations

import atexit
import importlib
import importlib.util
import logging
import os
import sys
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from .context.vars import get_trace_id  # noqa: F401
from .core.event_schema import NormalizedEvent  # noqa: F401
from .sdk.base_probe import ProbeRegistry
from .sdk.emitter import emit  # noqa: F401

logger = logging.getLogger("stacktracer")


# ====================================================================== #
# Package-level singletons — one per process
# ====================================================================== #

_config: Optional["ResolvedConfig"] = None
_engine: Optional[Any] = None
_active_probes: List[Any] = []
_uploader: Optional[Any] = None
_post_init_callbacks: List[Callable] = []


def _register_post_init_callback(fn: Callable) -> None:
    """Register a function to call once after init() completes."""
    if _engine is not None:
        fn()
    else:
        _post_init_callbacks.append(fn)


# ====================================================================== #
# Step 1 — Raw config loading and merging
# ====================================================================== #


def _load_package_defaults() -> Dict[str, Any]:
    """
    Load the package-shipped defaults.yaml.
    Lives at stacktracer/config/defaults.yaml — never edited by users.
    If missing (broken install) returns empty dict and logs a loud warning.
    All values the system needs must be in defaults.yaml — nothing is
    hardcoded in Python anymore.
    """
    import yaml

    defaults_path = os.path.join(
        os.path.dirname(__file__), "config", "defaults.yaml"
    )
    if not os.path.exists(defaults_path):
        logger.warning(
            "stacktracer: defaults.yaml missing from package installation at %s. "
            "Reinstall: pip install --force-reinstall stacktracer. "
            "Running with empty defaults — some features may be unavailable.",
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
    Locate the user's stacktracer.yaml.

    Search order:
        1. explicit_path if provided
        2. STACKTRACER_CONFIG environment variable
        3. Walk up from cwd looking for stacktracer.yaml (max 5 levels)
    """
    if explicit_path:
        if os.path.exists(explicit_path):
            return explicit_path
        logger.warning(
            "Explicit config path not found: %s", explicit_path
        )
        return None

    env_path = os.getenv("STACKTRACER_CONFIG")
    if env_path and os.path.exists(env_path):
        return env_path

    current = os.getcwd()
    for _ in range(5):
        candidate = os.path.join(current, "stacktracer.yaml")
        if os.path.exists(candidate):
            return candidate
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent

    return None


def _load_user_config(path: Optional[str]) -> Dict[str, Any]:
    """Load user yaml if found, otherwise empty dict."""
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

        # ADD THIS EXCEPTION:
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

        # Inside _deep_merge in stacktracer/__init__.py

        elif (
            key == "normalize"
            and key in result
            and isinstance(val, list)
        ):
            # If the user provides an empty list, it means "clear the defaults"
            # If they provide rules, they override the defaults
            if not val:
                result[key] = []
            else:
                # Otherwise, we keep the user's rules as the "New Base"
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
    Normalize rules are ADDITIVE — kwarg rules extend the yaml list.
    User wanting a clean slate sets normalize: [] in their yaml first.
    """
    base = list(merged_yaml_rules)
    if init_kwarg_rules:
        base.extend(init_kwarg_rules)
    return base


# ====================================================================== #
# Step 2 — ResolvedConfig
# ====================================================================== #


@dataclass
class ResolvedConfig:
    """
    Fully resolved configuration after merging defaults.yaml,
    user stacktracer.yaml, and init() kwargs.
    Built once in init() and stored as _config.
    """

    api_key: str
    endpoint: str
    sample_rate: float
    buffer_size: int
    flush_interval: int
    snapshot_interval: float
    redact_fields: List[str]
    probes: List[str]
    builtin_probes: List[
        str
    ]  # module paths — from defaults.yaml
    semantic: List[Dict]
    normalize: List[Dict]
    compactor: Dict[str, Any]
    nginx: Dict[str, Any]
    gunicorn: Dict[str, Any]
    active_requests: Dict[str, Any]
    observe: Dict[str, Any]
    debug: bool
    enabled: bool
    config_path: Optional[str]

    def __post_init__(self) -> None:
        if (
            os.getenv("DJANGO_DEBUG", "false").lower() == "true"
            and not self.debug
        ):
            logger.info(
                "StackTracer: DJANGO_DEBUG=True — disabling. "
                "Pass debug=True to stacktracer.init() to enable in dev."
            )
            self.enabled = False
        self.sample_rate = max(0.0, min(1.0, self.sample_rate))


def _build_resolved_config(
    merged_yaml: Dict[str, Any],
    api_key: str,
    endpoint: str,
    sample_rate: Optional[float],
    probes: Optional[List[str]],
    semantic: Optional[List[Dict]],
    snapshot_interval: Optional[float],
    flush_interval: Optional[int],
    debug: bool,
    config_path: Optional[str],
    normalize: Optional[List[Dict]],
    compactor: Optional[Dict],
    active_requests: Optional[Dict],
    observe: Optional[Dict],
) -> ResolvedConfig:
    """
    Apply init() kwargs as the final override layer on top of merged yaml.
    merged_yaml = _deep_merge(defaults.yaml, user stacktracer.yaml).
    """

    # import pdb
    # pdb.set_trace()

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
        sample_rate=(
            sample_rate
            if sample_rate is not None
            else merged_yaml.get("sample_rate", 0.01)
        ),
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
        redact_fields=merged_yaml.get("redact_fields", []),
        probes=(
            probes
            if probes is not None
            else merged_yaml.get("probes", [])
        ),
        builtin_probes=merged_yaml.get("builtin_probes", []),
        semantic=resolved_semantic,
        normalize=resolved_normalize,
        compactor=_deep_merge(
            merged_yaml.get("compactor", {}), compactor or {}
        ),
        nginx=merged_yaml.get("nginx", {}),
        gunicorn=merged_yaml.get("gunicorn", {}),
        active_requests=_deep_merge(
            merged_yaml.get("active_requests", {}),
            active_requests or {},
        ),
        observe=_deep_merge(
            merged_yaml.get("observe", {}), observe or {}
        ),
        debug=debug,
        enabled=True,
        config_path=config_path,
    )


# ====================================================================== #
# Step 3 — Component initialisation
# ====================================================================== #


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


def _init_pattern_registry(tracker: Any) -> Any:
    from .core.causal import build_default_registry

    return build_default_registry(tracker=tracker)


def _init_engine(
    cfg: ResolvedConfig,
    normalizer: Any,
    compactor: Any,
    semantic: Any,
    registry: Any,
    tracker: Any,
    repository: Optional[Any],
) -> Any:
    from .core.engine import Engine
    from .core.runtime_graph import RuntimeGraph
    from .sdk.emitter import bind_engine

    graph = RuntimeGraph()
    engine = Engine(
        causal_registry=registry,
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
) -> List[Any]:
    """
    1. Import builtin probe modules listed in defaults.yaml under builtin_probes.
       Side-effect: each module registers its BaseProbe subclass with ProbeRegistry.
       No hardcoded list here — defaults.yaml owns it.

    2. Discover user probes from <app_root>/stacktracer/probes/*_probe.py.

    3. Start probes named in cfg.probes (user stacktracer.yaml takes precedence
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
    App root = parent directory of stacktracer.yaml, or cwd if no config found.
    User probes and rules are discovered relative to this directory.
    """
    if config_path and os.path.isfile(config_path):
        return os.path.dirname(os.path.abspath(config_path))
    return os.getcwd()


def _discover_user_probes(app_root: str) -> None:
    """
    Auto-discover *_probe.py files from <app_root>/stacktracer/probes/.
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
        if not fname.endswith("_probe.py") or fname.startswith(
            "__"
        ):
            continue
        full_path = os.path.join(probes_dir, fname)
        module_name = f"_stacktracer_user_probe_{fname[:-3]}"
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


def _discover_user_rules(registry: Any, app_root: str) -> None:
    """
    Auto-discover *_rules.py files from <app_root>/stacktracer/rules/.
    Each file must expose register(registry) which adds CausalRule instances.
    """
    import importlib.util
    import traceback

    rules_dir = os.path.join(app_root, "stacktracer", "rules")
    if not os.path.isdir(rules_dir):
        return

    for fname in sorted(os.listdir(rules_dir)):
        if not fname.endswith("_rules.py") or fname.startswith(
            "__"
        ):
            continue
        full_path = os.path.join(rules_dir, fname)
        module_name = f"_stacktracer_user_rule_{fname[:-3]}"
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
                    "User rules file %s has no register(registry) — skipped",
                    fname,
                )
                sys.modules.pop(module_name, None)
                continue

            register_fn(registry)
            logger.info(
                "User rules loaded from %s (registry now has %d rules)",
                fname,
                len(registry.rule_names()),
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
        logger.debug("Uploader: no api_key — skipping")
        return None
    try:
        from stacktracer.sdk.uploader import Uploader

        uploader = Uploader(
            endpoint=cfg.endpoint,
            api_key=cfg.api_key,
            flush_interval=cfg.flush_interval,
            max_batch_size=500,
        )
        uploader.bind_engine(
            engine
        )  # ← give uploader the engine
        uploader.start()
        engine.repository = uploader
        _uploader = uploader
        logger.info(
            "Uploader active → %s (interval=%ds)",
            cfg.endpoint,
            cfg.flush_interval,
        )
        return uploader
    except Exception as exc:
        logger.warning("Uploader failed to start: %s", exc)
        return None


# ====================================================================== #
# Public init()
# ====================================================================== #


def init(
    api_key: str = "test-key-123",
    endpoint: str = "http://localhost:8000",
    config: Optional[str] = None,
    probes: Optional[List[str]] = None,
    semantic: Optional[List[Dict]] = None,
    sample_rate: Optional[float] = None,
    snapshot_interval: Optional[float] = None,
    flush_interval: Optional[int] = None,
    debug: bool = True,
    repository: Optional[Any] = None,
    normalize: Optional[List[Dict]] = None,
    compactor: Optional[Dict] = None,
    active_requests: Optional[Dict] = None,
    observe: Optional[Dict] = None,
    otel_mode: bool = False,
) -> None:
    """
    Initialise StackTracer.

    Config merge order (last wins):
        1. stacktracer/config/defaults.yaml   — package defaults, never edited
        2. stacktracer.yaml                   — user app config, takes precedence
        3. init() kwargs                      — highest priority

    Minimal usage:
        stacktracer.init()   # all defaults from defaults.yaml apply

    Typical usage in apps.py:
        stacktracer.init(
            config  = str(BASE_DIR / "stacktracer.yaml"),
            debug   = True,
        )

         Parameters
    ----------
    api_key
        API key for remote upload to StackTracer backend.
        Omit or pass "" to run in local-only mode (no upload).

    endpoint
        Backend URL. Default: https://api.stacktracer.io

    config
        Explicit path to user stacktracer.yaml.
        If omitted, searched automatically from cwd upward (max 5 levels).

    probes
        List of probe names to activate.
        REPLACES the default list entirely.
        Default: ["django", "asyncio", "uvicorn", "gunicorn", "nginx"]

    semantic
        Extra semantic alias dicts to add or override.
        MERGED with defaults by label — your label wins on same key.

    sample_rate
        Fraction of requests to trace (0.0–1.0). Default: 0.01 (1%)

    snapshot_interval
        Seconds between temporal graph snapshots. Default: 15.0

    flush_interval
        Seconds between uploader event batch flushes. Default: 10

    debug
        If True, enables StackTracer even when DJANGO_DEBUG=True.
        Default: False (auto-disables in Django debug environments)

    repository
        Pre-built storage backend (EventRepository, ClickHouseRepository).
        Overrides the remote uploader as the event sink.

    normalize
        Additional normalization rules beyond built-in patterns and yaml rules.
        EXTENDS — does not replace — the merged yaml normalize list.
        Each rule: {"service": "django", "pattern": "...", "replacement": "..."}

    compactor
        Override specific compactor settings.
        MERGES key-by-key — unspecified keys keep their yaml / default values.
        Keys: max_nodes, evict_to_ratio, node_ttl_s, min_call_count

    active_requests
        Override ActiveRequestTracker settings.
        MERGES key-by-key.  Keys: ttl_s, max_size
    """
    global _config, _engine, _active_probes

    # ── 1. Load and merge ─────────────────────────────────────────────
    package_defaults = _load_package_defaults()
    user_config_path = _find_user_config(config)
    user_yaml = _load_user_config(user_config_path)
    merged_yaml = _deep_merge(package_defaults, user_yaml)
    app_root = _app_root_from_config(user_config_path)

    # ── 2. Build ResolvedConfig ───────────────────────────────────────
    _config = _build_resolved_config(
        merged_yaml=merged_yaml,
        api_key=api_key,
        endpoint=endpoint,
        sample_rate=sample_rate,
        probes=probes,
        semantic=semantic,
        snapshot_interval=snapshot_interval,
        flush_interval=flush_interval,
        debug=debug,
        config_path=user_config_path,
        normalize=normalize,
        compactor=compactor,
        active_requests=active_requests,
        observe=observe,
    )

    if not _config.enabled:
        logger.info("StackTracer disabled — not initialising")
        return

    # ── 3. Initialise components ──────────────────────────────────────
    normalizer = _init_normalizer(_config)
    compactor_ = _init_compactor(_config)
    semantic_layer = _init_semantic(_config)
    tracker = _init_tracker(_config)
    registry = _init_pattern_registry(tracker)

    _discover_user_rules(registry, app_root)

    _engine = _init_engine(
        cfg=_config,
        normalizer=normalizer,
        compactor=compactor_,
        semantic=semantic_layer,
        registry=registry,
        tracker=tracker,
        repository=repository,
    )

    _init_local_server(_engine)

    if otel_mode:
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
        "StackTracer ready | probes=%s sample_rate=%.1f%% config=%s app_root=%s",
        [p.name for p in _active_probes],
        _config.sample_rate * 100,
        user_config_path or "defaults only",
        app_root,
    )
    atexit.register(shutdown)


# ====================================================================== #
# Public accessors
# ====================================================================== #


def get_config() -> "ResolvedConfig":
    if _config is None:
        raise RuntimeError(
            "stacktracer.init() has not been called"
        )
    return _config


def get_engine() -> Any:
    return _engine


# ====================================================================== #
# Shutdown
# ====================================================================== #


def shutdown() -> None:
    global _active_probes, _engine, _uploader, _config

    for probe in _active_probes:
        try:
            probe.stop()
        except Exception as exc:
            logger.debug(
                "Probe %s stop error: %s", probe.name, exc
            )
    _active_probes = []

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
    logger.info("StackTracer shut down")


# ====================================================================== #
# Decorators and helpers
# ====================================================================== #


def trace(name: Optional[str] = None):
    """
    Decorator for explicit function-level tracing.

        @stacktracer.trace("my_expensive_function")
        async def my_fn(): ...
    """
    import functools

    def decorator(fn: Any) -> Any:
        fn_name = name or fn.__qualname__
        is_async = _is_async_fn(fn)
        if is_async:

            @functools.wraps(fn)
            async def async_wrapper(
                *args: Any, **kwargs: Any
            ) -> Any:
                return await _traced_call(
                    fn, fn_name, args, kwargs, is_async=True
                )

            return async_wrapper
        else:

            @functools.wraps(fn)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                return _traced_call(
                    fn, fn_name, args, kwargs, is_async=False
                )

            return sync_wrapper

    return decorator


def _is_async_fn(fn: Any) -> bool:
    import asyncio
    import inspect

    return asyncio.iscoroutinefunction(
        fn
    ) or inspect.iscoroutinefunction(fn)


def _traced_call(fn, fn_name, args, kwargs, is_async):
    import time as _time

    from .context.vars import get_span_id, get_trace_id
    from .core.event_schema import NormalizedEvent
    from .sdk.emitter import emit

    trace_id = get_trace_id()
    if not trace_id:
        return fn(*args, **kwargs)

    emit(
        NormalizedEvent.now(
            probe="function.call",
            trace_id=trace_id,
            service="user",
            name=fn_name,
            parent_span_id=get_span_id(),
        )
    )
    start = _time.perf_counter()
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        emit(
            NormalizedEvent.now(
                probe="function.exception",
                trace_id=trace_id,
                service="user",
                name=fn_name,
                exception_type=type(exc).__name__,
            )
        )
        raise
    finally:
        emit(
            NormalizedEvent.now(
                probe="function.return",
                trace_id=trace_id,
                service="user",
                name=fn_name,
                duration_ns=int(
                    (_time.perf_counter() - start) * 1e9
                ),
            )
        )


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


# ====================================================================== #
# Public re-exports
# ====================================================================== #

__version__ = "0.1.0"
__all__ = [
    "init",
    "shutdown",
    "get_config",
    "get_engine",
    "trace",
    "mark_deployment",
    "emit",
    "NormalizedEvent",
    "get_trace_id",
    "ResolvedConfig",
]
