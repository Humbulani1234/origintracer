"""
stacktracer/__init__.py

Public API for the StackTracer agent library.

Minimal usage — everything else comes from defaults:

    import stacktracer
    stacktracer.init(api_key="sk_...")

    MIDDLEWARE = ["stacktracer.probes.django_probe.TracerMiddleware", ...]

Full usage — user overrides specific defaults:

    stacktracer.init(
        api_key          = os.getenv("STACKTRACER_API_KEY"),
        endpoint         = "https://api.stacktracer.io",
        config           = "stacktracer.yaml",   # path to user config file
        probes           = ["django", "asyncio"], # MERGED with defaults
        semantic         = [...],                 # MERGED with defaults
        sample_rate      = 0.05,
        debug            = False,
        repository       = None,
        normalize        = [...],  # extra normalization rules
        compactor        = {...},  # override compactor settings
    )

Config merge order (last wins):
    1. Package defaults  — stacktracer/config/defaults.yaml
    2. User yaml file    — searched from cwd upward, or explicit config= path
    3. init() kwargs     — highest priority, overrides everything

This means a user can run stacktracer.init(api_key="...") with no other
arguments and get the full nginx→gunicorn→django→postgres stack configured
sensibly. They only pass kwargs to override specific values.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger("stacktracer")


# ====================================================================== #
# Package-level singletons — one per process
# ====================================================================== #

_config: Optional["ResolvedConfig"] = None
_engine: Optional[Any] = None
_active_probes: List[Any] = []
_uploader: Optional[Any] = None


# ====================================================================== #
# Step 1 — Raw config loading and merging
# ====================================================================== #

def _load_package_defaults() -> Dict[str, Any]:
    """
    Load the package-shipped defaults.yaml.
    This is the full-stack default — nginx, gunicorn, uvicorn, django,
    asyncio, celery — configured correctly out of the box.
    Lives at stacktracer/config/defaults.yaml inside the package.
    Never edited by users.
    """
    import yaml
    defaults_path = os.path.join(
        os.path.dirname(__file__), "config", "defaults.yaml"
    )
    if not os.path.exists(defaults_path):
        logger.debug("Package defaults not found at %s — using hardcoded", defaults_path)
        return _hardcoded_defaults()
    with open(defaults_path) as f:
        return yaml.safe_load(f) or {}


def _hardcoded_defaults() -> Dict[str, Any]:
    """
    Fallback if defaults.yaml is missing (e.g. during development before
    the file is written). These are the same values that defaults.yaml
    would contain — duplicated here so init() always has a valid baseline.
    """
    return {
        "probes": ["django", "asyncio", "uvicorn", "gunicorn", "nginx"],
        "sample_rate": 0.01,
        "snapshot_interval": 15.0,
        "flush_interval": 10,
        "buffer_size": 10_000,
        "redact_fields": ["password", "token", "secret", "authorization"],
        "semantic": [
            {
                "label": "api",
                "description": "Public API surface",
                "node_patterns": ["django::/api/.*"],
                "services": [],
            },
            {
                "label": "db",
                "description": "All database interactions",
                "services": ["postgres", "mysql", "sqlite", "redis"],
                "node_patterns": [],
            },
        ],
        "normalize": [
            # Django REST Framework default router pattern
            {
                "service": "django",
                "pattern": r"/api/v\d+/",
                "replacement": "/api/{version}/",
            },
        ],
        "compactor": {
            "max_nodes":       5_000,
            "evict_to_ratio":  0.80,
            "node_ttl_s":      3600.0,
            "min_call_count":  5,
        },
        "nginx": {
            "mode":     "auto",
            "log_path": "/var/log/nginx/access.log",
            "lua_host": "127.0.0.1",
            "lua_port": 9119,
        },
        "gunicorn": {
            "worker_class": "uvicorn.workers.UvicornWorker",
        },
        "active_requests": {
            "ttl_s":    30.0,
            "max_size": 10_000,
        },
    }


def _find_user_config(explicit_path: Optional[str]) -> Optional[str]:
    """
    Locate the user's stacktracer.yaml.

    Search order:
        1. explicit_path if provided
        2. STACKTRACER_CONFIG environment variable
        3. Walk up from cwd looking for stacktracer.yaml (max 5 levels)

    Returns the path string if found, None if not found.
    """
    # Explicit path always wins
    if explicit_path:
        if os.path.exists(explicit_path):
            return explicit_path
        logger.warning("Explicit config path not found: %s", explicit_path)
        return None

    # Environment variable
    env_path = os.getenv("STACKTRACER_CONFIG")
    if env_path and os.path.exists(env_path):
        return env_path

    # Walk up from cwd
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
        logger.warning("Could not load user config %s: %s", path, exc)
        return {}


def _deep_merge(base: Dict, override: Dict) -> Dict:
    """
    Merge override into base recursively.
    Lists are REPLACED not extended — if user specifies probes: [django]
    they get exactly [django], not [django, asyncio, ...defaults].
    Dicts are merged key by key so user can override one compactor setting
    without having to repeat all of them.
    """
    result = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def _merge_semantic(
    defaults: List[Dict],
    user_yaml: List[Dict],
    init_kwarg: Optional[List[Dict]],
) -> List[Dict]:
    """
    Merge semantic aliases from three sources.
    Label is the dedup key — later sources win on the same label.
    All three sources contribute distinct labels.

    defaults:    package defaults.yaml semantic section
    user_yaml:   user's stacktracer.yaml semantic section
    init_kwarg:  semantic= passed directly to init()
    """
    merged: Dict[str, Dict] = {}
    for source in (defaults, user_yaml, init_kwarg or []):
        for entry in (source or []):
            label = entry.get("label", "")
            if label:
                merged[label] = entry
    return list(merged.values())


# ====================================================================== #
# Step 2 — ResolvedConfig: the single config object built from merged data
# ====================================================================== #

@dataclass
class ResolvedConfig:
    """
    The fully resolved configuration after merging package defaults,
    user yaml, and init() kwargs.

    This is built once inside init() and stored as _config.
    Nothing else in the system reads from raw yaml — everything
    reads from this object.
    """
    api_key:          str
    endpoint:         str
    sample_rate:      float
    buffer_size:      int
    flush_interval:   int
    snapshot_interval: float
    redact_fields:    List[str]
    probes:           List[str]
    semantic:         List[Dict]
    normalize:        List[Dict]
    compactor:        Dict[str, Any]
    nginx:            Dict[str, Any]
    gunicorn:         Dict[str, Any]
    active_requests:  Dict[str, Any]
    debug:            bool
    enabled:          bool
    config_path:      Optional[str]   # path of the user yaml that was loaded

    def __post_init__(self) -> None:
        # Auto-disable when Django DEBUG=True unless debug=True explicitly set
        if os.getenv("DJANGO_DEBUG", "false").lower() == "true" and not self.debug:
            logger.info(
                "StackTracer: DJANGO_DEBUG=True detected — disabling. "
                "Pass debug=True to stacktracer.init() to enable in dev."
            )
            self.enabled = False

        # Clamp sample_rate
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
) -> ResolvedConfig:
    """
    Build the final ResolvedConfig.
    init() kwargs are the last override — they win over everything.
    Only kwargs that are explicitly passed (not None) override the merged yaml.
    """
    # Semantic merging is special — three-way label-keyed merge
    resolved_semantic = _merge_semantic(
        defaults   = _hardcoded_defaults().get("semantic", []),
        user_yaml  = merged_yaml.get("semantic", []),
        init_kwarg = semantic,
    )

    return ResolvedConfig(
        api_key           = api_key,
        endpoint          = endpoint,
        sample_rate       = sample_rate      if sample_rate      is not None else merged_yaml.get("sample_rate",       0.01),
        buffer_size       = merged_yaml.get("buffer_size",       10_000),
        flush_interval    = flush_interval   if flush_interval   is not None else merged_yaml.get("flush_interval",    10),
        snapshot_interval = snapshot_interval if snapshot_interval is not None else merged_yaml.get("snapshot_interval", 15.0),
        redact_fields     = merged_yaml.get("redact_fields",     ["password", "token", "secret", "authorization"]),
        probes            = probes           if probes           is not None else merged_yaml.get("probes",            ["django", "asyncio"]),
        semantic          = resolved_semantic,
        normalize         = normalize        if normalize         is not None else merged_yaml.get("normalize",        []),
        compactor         = _deep_merge(merged_yaml.get("compactor", {}), compactor or {}),
        nginx             = merged_yaml.get("nginx",             {}),
        gunicorn          = merged_yaml.get("gunicorn",          {}),
        active_requests   = _deep_merge(merged_yaml.get("active_requests", {}), active_requests or {}),
        debug             = debug,
        enabled           = True,
        config_path       = config_path,
    )


# ====================================================================== #
# Step 3 — Component initialisation functions
# Each function is responsible for exactly one component.
# ====================================================================== #

def _init_normalizer(cfg: ResolvedConfig) -> Any:
    """
    Build GraphNormalizer with:
        - built-in patterns always on
        - user normalize rules from merged config
    """
    from .core.graph_normalizer import GraphNormalizer, NormalizationRule

    compactor_cfg = cfg.compactor
    normalizer = GraphNormalizer(
        enable_builtins             = True,
        max_name_length             = 200,
        max_unique_names_per_service= 500,
    )

    for rule in cfg.normalize:
        svc     = rule.get("service", "*")
        pattern = rule.get("pattern")
        repl    = rule.get("replacement")
        desc    = rule.get("description", "")
        if pattern and repl:
            normalizer.add_pattern(
                service     = svc,
                pattern     = pattern,
                replacement = repl,
                description = desc,
            )

    return normalizer


def _init_compactor(cfg: ResolvedConfig) -> Any:
    """Build GraphCompactor from merged compactor config section."""
    from .core.graph_compactor import GraphCompactor

    c = cfg.compactor
    return GraphCompactor(
        max_nodes      = c.get("max_nodes",       5_000),
        evict_to_ratio = c.get("evict_to_ratio",  0.80),
        node_ttl_s     = c.get("node_ttl_s",      3600.0),
        min_call_count = c.get("min_call_count",  5),
    )


def _init_semantic(cfg: ResolvedConfig) -> Any:
    """Build SemanticLayer from merged semantic config."""
    from .core.semantic import load_from_dict
    return load_from_dict(cfg.semantic)


def _init_tracker(cfg: ResolvedConfig) -> Any:
    """Build ActiveRequestTracker from merged active_requests config."""
    from .core.active_requests import ActiveRequestTracker

    ar = cfg.active_requests
    return ActiveRequestTracker(
        ttl_s    = ar.get("ttl_s",    30.0),
        max_size = ar.get("max_size", 10_000),
    )


def _init_pattern_registry(tracker: Any) -> Any:
    """
    Build PatternRegistry with all built-in rules.
    The anomaly rule closes over the tracker instance.
    """
    from .core.causal import build_default_registry
    return build_default_registry(tracker=tracker)


def _init_engine(
    cfg:        ResolvedConfig,
    normalizer: Any,
    compactor:  Any,
    semantic:   Any,
    registry:   Any,
    tracker:    Any,
    repository: Optional[Any],
) -> Any:
    """
    Build Engine with all components wired.
    RuntimeGraph needs the normalizer passed in — add it here.
    Compactor is attached so the snapshot loop can call it.
    """
    from .core.engine import Engine
    from .core.runtime_graph import RuntimeGraph
    from .sdk.emitter import bind_engine

    # RuntimeGraph currently takes no args — wire normalizer via attribute
    # until RuntimeGraph constructor is updated to accept it
    graph = RuntimeGraph()
    graph.normalizer = normalizer    # Engine.process() checks for this

    engine = Engine(
        causal_registry   = registry,
        semantic_layer    = semantic,
        snapshot_interval_s = cfg.snapshot_interval,
    )
    # Replace the default graph with our normalizer-equipped one
    engine.graph      = graph
    engine.compactor  = compactor
    engine.tracker    = tracker

    if repository:
        engine.repository = repository

    bind_engine(engine)              # starts DrainThread
    engine.start_background_tasks()  # starts snapshot thread

    return engine


def _init_probes(cfg: ResolvedConfig, engine: Any, registry: Any) -> List[Any]:
    """
    Import all built-in probe modules (side-effect: registers with ProbeRegistry).
    Then load and start probes named in cfg.probes.
    User probes discovered from stacktracer/probes/ directory convention.
    """
    from .sdk.base_probe import ProbeRegistry

    # Force-import all built-in probe modules so they register themselves.
    # Order matters — db_kprobe should come after asyncio so BPF programs
    # don't race during initialisation on very fast machines.
    _builtin_probe_modules = [
        ".probes.asyncio_probe",
        ".probes.django_probe",
        ".probes.gunicorn_probe",
        ".probes.uvicorn_probe",
        ".probes.nginx_probe",
        ".probes.db_kprobe",
        ".probes.celery_probe",
    ]
    for module_path in _builtin_probe_modules:
        try:
            import importlib
            importlib.import_module(module_path, package=__name__)
        except ImportError as exc:
            # Not every probe's dependencies will be installed — that is fine.
            # django_probe requires Django, celery_probe requires Celery, etc.
            logger.debug("Probe module not available: %s — %s", module_path, exc)

    # Auto-discover user probe files from convention directory
    _discover_user_probes()
    # Auto-discover user rule files from convention directory
    _discover_user_rules(registry)

    # Load and start each named probe
    probes = ProbeRegistry.load_from_config({"probes": cfg.probes})
    started = []
    for probe in probes:
        try:
            probe.start()
            started.append(probe)
            logger.info("Probe started: %s", probe.name)
        except Exception as exc:
            logger.warning("Probe %s failed to start: %s", probe.name, exc)

    return started

def _app_root_from_config() -> str:
    """
    Return the application root directory.

    This is the directory that contains the user's stacktracer.yaml.
    If no config file was found, falls back to cwd.

    Examples:
        /home/user/myproject/stacktracer.yaml  → /home/user/myproject/
        /srv/apps/django_app/stacktracer.yaml  → /srv/apps/django_app/
        (no config found)                      → os.getcwd()
    """
    if _config is not None and _config.config_path:
        return os.path.dirname(os.path.abspath(_config.config_path))
    return os.getcwd()

def _discover_user_probes() -> None:
    """
    Auto-discover user probe files following the convention:

        <app_root>/stacktracer/probes/*_probe.py

    Where <app_root> is the directory containing the user's stacktracer.yaml
    (the config_path the system found), or cwd if no config was found.

    Users never modify the package — their probes live in their application
    directory under stacktracer/probes/. The package's own probes/ directory
    is separate and already imported via _builtin_probe_modules above.

    Convention: file must be named *_probe.py to be discovered.
    The file self-registers by subclassing BaseProbe — no yaml entry needed.
    """
    import importlib.util

    # Derive the app root from the config path that was found,
    # or fall back to cwd if no config file was located.
    app_root  = _app_root_from_config()
    probes_dir = os.path.join(app_root, "stacktracer", "probes")

    if not os.path.isdir(probes_dir):
        logger.debug("No user probes directory at %s — skipping", probes_dir)
        return

    for fname in sorted(os.listdir(probes_dir)):
        if not fname.endswith("_probe.py") or fname.startswith("__"):
            continue
        full_path = os.path.join(probes_dir, fname)
        try:
            spec   = importlib.util.spec_from_file_location(fname[:-3], full_path)
            module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
            spec.loader.exec_module(module)                  # type: ignore[union-attr]
            logger.info("User probe loaded: %s", fname)
        except Exception as exc:
            logger.warning("User probe %s failed to load: %s", fname, exc)


def _discover_user_rules(registry: Any) -> None:
    """
    Auto-discover user rule files following the convention:

        <app_root>/stacktracer/rules/*_rules.py

    Each rules file must expose a top-level function:

        def register(registry: PatternRegistry) -> None:
            registry.register(CausalRule(...))
            registry.register(CausalRule(...))

    StackTracer calls register(registry) after the file is imported,
    passing the live PatternRegistry so rules are registered into the
    same registry as built-in rules and fire during CAUSAL queries.

    Convention: file must be named *_rules.py to be discovered.
    Users never modify the package — their rules live in their
    application directory under stacktracer/rules/.
    """
    import importlib.util

    app_root  = _app_root_from_config()
    rules_dir = os.path.join(app_root, "stacktracer", "rules")

    if not os.path.isdir(rules_dir):
        logger.debug("No user rules directory at %s — skipping", rules_dir)
        return

    for fname in sorted(os.listdir(rules_dir)):
        if not fname.endswith("_rules.py") or fname.startswith("__"):
            continue
        full_path = os.path.join(rules_dir, fname)
        try:
            spec   = importlib.util.spec_from_file_location(fname[:-3], full_path)
            module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
            spec.loader.exec_module(module)                  # type: ignore[union-attr]

            # Call register(registry) if present
            if hasattr(module, "register") and callable(module.register):
                module.register(registry)
                logger.info("User rules loaded: %s", fname)
            else:
                logger.warning(
                    "User rules file %s has no register(registry) function — skipped. "
                    "Add: def register(registry): registry.register(CausalRule(...))",
                    fname,
                )
        except Exception as exc:
            logger.warning("User rules %s failed to load: %s", fname, exc)

def _init_local_server(engine: Any) -> Any:
    """Start the Unix socket server for REPL → agent queries."""
    try:
        from .core.local_server import LocalQueryServer
        server = LocalQueryServer(engine)
        server.start()
        return server
    except Exception as exc:
        logger.warning("Local query server failed to start: %s", exc)
        return None


def _init_uploader(cfg: ResolvedConfig, engine: Any) -> Optional[Any]:
    """
    Start background uploader and wire as engine repository.
    Only started if api_key is set and endpoint is reachable config.
    """
    global _uploader

    if not cfg.api_key:
        logger.debug("Uploader: no api_key — skipping")
        return None

    try:
        from .buffer.uploader import Uploader
        uploader = Uploader(
            endpoint      = cfg.endpoint,
            api_key       = cfg.api_key,
            flush_interval= cfg.flush_interval,
            max_batch_size= 500,
        )
        uploader.start()

        # Wire uploader as engine repository (pull model — uploader reads event log)
        engine.repository = uploader
        _uploader = uploader

        logger.info("Uploader active → %s (interval=%ds)", cfg.endpoint, cfg.flush_interval)
        return uploader

    except Exception as exc:
        logger.warning("Uploader failed to start: %s", exc)
        return None


# ====================================================================== #
# Public init() — the single entry point
# ====================================================================== #

def init(
    api_key:           str                   = "",
    endpoint:          str                   = "https://api.stacktracer.io",
    config:            Optional[str]         = None,
    probes:            Optional[List[str]]   = None,
    semantic:          Optional[List[Dict]]  = None,
    sample_rate:       Optional[float]       = None,
    snapshot_interval: Optional[float]       = None,
    flush_interval:    Optional[int]         = None,
    debug:             bool                  = False,
    repository:        Optional[Any]         = None,
    normalize:         Optional[List[Dict]]  = None,
    compactor:         Optional[Dict]        = None,
    active_requests:   Optional[Dict]        = None,
) -> None:
    """
    Initialise StackTracer.

    All parameters are optional except api_key for remote upload.
    Call with no arguments to get full-stack defaults:

        stacktracer.init()

    Parameters
    ----------
    api_key
        API key for remote upload to StackTracer backend.
        Omit or set "" to run in local-only mode (no upload).

    endpoint
        Backend URL. Default: https://api.stacktracer.io

    config
        Explicit path to user stacktracer.yaml.
        If omitted, searches from cwd upward automatically.

    probes
        List of probe names to activate.
        REPLACES the default list (not merged).
        Default: ["django", "asyncio", "uvicorn", "gunicorn", "nginx"]

    semantic
        List of semantic alias dicts.
        MERGED with defaults by label — user aliases win on same label.

    sample_rate
        Fraction of requests to trace (0.0–1.0).
        Default: 0.01 (1%)

    snapshot_interval
        Seconds between temporal graph snapshots.
        Default: 15.0

    flush_interval
        Seconds between uploader batch flushes.
        Default: 10

    debug
        If True, enables StackTracer even when DJANGO_DEBUG=True.
        Default: False (auto-disables in Django debug environments)

    repository
        Pre-built storage repository (EventRepository, ClickHouseRepository).
        Overrides the uploader as the event sink.

    normalize
        Extra normalization rules beyond built-in patterns.
        Each: {"service": "django", "pattern": "...", "replacement": "..."}

    compactor
        Override compactor settings.
        Keys: max_nodes, evict_to_ratio, node_ttl_s, min_call_count.
        Only specified keys are overridden — others keep defaults.

    active_requests
        Override ActiveRequestTracker settings.
        Keys: ttl_s, max_size.
    """
    global _config, _engine, _active_probes

    # ── 1. Load and merge configs ─────────────────────────────────────
    package_defaults = _load_package_defaults()
    user_config_path = _find_user_config(config)
    user_yaml        = _load_user_config(user_config_path)
    merged_yaml      = _deep_merge(package_defaults, user_yaml)

    # ── 2. Build ResolvedConfig ───────────────────────────────────────
    _config = _build_resolved_config(
        merged_yaml       = merged_yaml,
        api_key           = api_key,
        endpoint          = endpoint,
        sample_rate       = sample_rate,
        probes            = probes,
        semantic          = semantic,
        snapshot_interval = snapshot_interval,
        flush_interval    = flush_interval,
        debug             = debug,
        config_path       = user_config_path,
        normalize         = normalize,
        compactor         = compactor,
        active_requests   = active_requests,
    )

    if not _config.enabled:
        logger.info("StackTracer disabled — not initialising")
        return

    # ── 3. Initialise all components in dependency order ─────────────

    # Normalizer and compactor first — engine needs them
    normalizer = _init_normalizer(_config)
    compactor_ = _init_compactor(_config)

    # Semantic layer — engine needs it for query resolution
    semantic_layer = _init_semantic(_config)

    # Tracker — registry closes over it, must come before registry
    tracker = _init_tracker(_config)

    # Pattern registry — closes over tracker
    registry = _init_pattern_registry(tracker)

    # Engine — wires everything together, starts drain thread + snapshot thread
    _engine = _init_engine(
        cfg        = _config,
        normalizer = normalizer,
        compactor  = compactor_,
        semantic   = semantic_layer,
        registry   = registry,
        tracker    = tracker,
        repository = repository,
    )

    # Local Unix socket server — REPL connects here
    _init_local_server(_engine)

    # Probes — started after engine so emit() has a bound engine
    _active_probes = _init_probes(_config, _engine, registry)

    # Uploader — started last, after probes are running
    # Only if no explicit repository passed (repository takes precedence)
    if repository is None:
        _init_uploader(_config, _engine)

    # ── 4. Log summary ───────────────────────────────────────────────
    logger.info(
        "StackTracer ready | probes=%s sample_rate=%.1f%% config=%s",
        [p.name for p in _active_probes],
        _config.sample_rate * 100,
        user_config_path or "defaults only",
    )


# ====================================================================== #
# Public accessors
# ====================================================================== #

def get_config() -> "ResolvedConfig":
    if _config is None:
        raise RuntimeError("stacktracer.init() has not been called")
    return _config


def get_engine() -> Any:
    """Returns the engine instance or None if not yet initialised."""
    return _engine


# ====================================================================== #
# Shutdown
# ====================================================================== #

def shutdown() -> None:
    """
    Gracefully stop all probes, flush the uploader, stop the engine.
    Call from Django's AppConfig.ready() shutdown signal or atexit.
    """
    global _active_probes, _engine, _uploader, _config

    for probe in _active_probes:
        try:
            probe.stop()
        except Exception as exc:
            logger.debug("Probe %s stop error: %s", probe.name, exc)
    _active_probes = []

    if _uploader:
        try:
            _uploader.stop()   # final flush before exit
        except Exception as exc:
            logger.debug("Uploader stop error: %s", exc)
        _uploader = None

    if _engine:
        _engine.stop()
        _engine = None

    _config = None
    logger.info("StackTracer shut down")


# ====================================================================== #
# Convenience decorators and helpers
# ====================================================================== #

def trace(name: Optional[str] = None):
    """
    Decorator for explicit function-level tracing.

        @stacktracer.trace("my_expensive_function")
        async def my_fn():
            ...

    Works with both sync and async functions.
    Does nothing if no active trace (no overhead outside traced requests).
    """
    import functools
    import time as _time

    def decorator(fn: Any) -> Any:
        fn_name   = name or fn.__qualname__
        is_async  = _is_async_fn(fn)

        if is_async:
            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await _traced_call(fn, fn_name, args, kwargs, is_async=True)
            return async_wrapper
        else:
            @functools.wraps(fn)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                return _traced_call(fn, fn_name, args, kwargs, is_async=False)
            return sync_wrapper

    return decorator


def _is_async_fn(fn: Any) -> bool:
    import asyncio, inspect
    return asyncio.iscoroutinefunction(fn) or inspect.iscoroutinefunction(fn)


def _traced_call(fn, fn_name, args, kwargs, is_async):
    import time as _time
    from .context.vars import get_trace_id, get_span_id
    from .sdk.emitter import emit
    from .core.event_schema import NormalizedEvent

    trace_id = get_trace_id()
    if not trace_id:
        if is_async:
            import asyncio
            return fn(*args, **kwargs)  # returns coroutine, caller awaits it
        return fn(*args, **kwargs)

    emit(NormalizedEvent.now(
        probe="function.call", trace_id=trace_id,
        service="user", name=fn_name,
        parent_span_id=get_span_id(),
    ))
    start = _time.perf_counter()
    try:
        if is_async:
            import asyncio
            return fn(*args, **kwargs)  # caller awaits the coroutine
        result = fn(*args, **kwargs)
        return result
    except Exception as exc:
        emit(NormalizedEvent.now(
            probe="function.exception", trace_id=trace_id,
            service="user", name=fn_name,
            exception_type=type(exc).__name__,
        ))
        raise
    finally:
        emit(NormalizedEvent.now(
            probe="function.return", trace_id=trace_id,
            service="user", name=fn_name,
            duration_ns=int((_time.perf_counter() - start) * 1e9),
        ))


def mark_deployment(label: str = "deployment") -> None:
    """
    Signal a deployment boundary to the temporal engine.
    Call from your CD pipeline after deploying:

        stacktracer.mark_deployment("v1.2.3")

    Or from the CLI:
        python -m stacktracer deploy --label "v1.2.3"
    """
    if _engine:
        _engine.mark_deployment(label)
    else:
        logger.warning("mark_deployment called before init()")


# ====================================================================== #
# Public re-exports — stable API surface
# ====================================================================== #

from .core.event_schema import NormalizedEvent  # noqa: F401
from .sdk.emitter import emit                    # noqa: F401
from .context.vars import get_trace_id           # noqa: F401

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