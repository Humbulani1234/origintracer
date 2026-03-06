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
    observe:          Dict[str, Any]   # observe.modules — app module prefixes for sys.monitoring
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


def _extend_normalize(
    merged_yaml_rules: List[Dict],
    init_kwarg_rules: Optional[List[Dict]],
) -> List[Dict]:
    """
    Normalize rules are ADDITIVE — init() kwarg rules extend the merged yaml
    list rather than replacing it.  There is no dedup key (unlike semantic
    aliases), so duplicates are the caller's responsibility.

    Why additive and not replacement like probes?
    Because a user adding one app-specific rule almost never wants to lose
    the built-in DRF version-collapse or the UUID rules already in defaults.
    If they genuinely want a clean slate they set normalize: [] in their yaml
    which wipes the merged list before their init() kwarg is appended.
    """
    base = list(merged_yaml_rules)          # copy — never mutate merged_yaml
    if init_kwarg_rules:
        base.extend(init_kwarg_rules)
    return base


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
    Build the final ResolvedConfig by applying init() kwargs as the last
    override layer on top of the already-merged yaml.

    Merge semantics per field type:
        scalar   (sample_rate, flush_interval …) — kwarg wins if not None,
                 else merged_yaml value, which always exists because
                 _deep_merge(package_defaults, user_yaml) ran first.
        list     (probes) — kwarg REPLACES merged_yaml list entirely.
                 User saying probes=["django"] means exactly ["django"].
        semantic (list of dicts keyed by label) — three-way label-keyed merge
                 so adding a new label is additive, overriding an existing
                 label replaces just that entry.
        normalize (list of dicts, no dedup key) — kwarg EXTENDS merged_yaml
                 list.  Replacement would silently drop built-in rules.
        dict     (compactor, active_requests) — _deep_merge so user can
                 override one key without repeating all others.
    """
    # merged_yaml already contains package_defaults merged with user yaml,
    # so .get() always finds a value — no hardcoded fallback needed here.
    resolved_semantic = _merge_semantic(
        defaults   = _hardcoded_defaults().get("semantic", []),
        user_yaml  = merged_yaml.get("semantic", []),
        init_kwarg = semantic,
    )

    resolved_normalize = _extend_normalize(
        merged_yaml_rules = merged_yaml.get("normalize", []),
        init_kwarg_rules  = normalize,
    )

    return ResolvedConfig(
        api_key           = api_key,
        endpoint          = endpoint,
        sample_rate       = sample_rate       if sample_rate       is not None else merged_yaml["sample_rate"],
        buffer_size       = merged_yaml["buffer_size"],
        flush_interval    = flush_interval    if flush_interval    is not None else merged_yaml["flush_interval"],
        snapshot_interval = snapshot_interval if snapshot_interval is not None else merged_yaml["snapshot_interval"],
        redact_fields     = merged_yaml["redact_fields"],
        probes            = probes            if probes            is not None else merged_yaml["probes"],
        semantic          = resolved_semantic,
        normalize         = resolved_normalize,
        compactor         = _deep_merge(merged_yaml.get("compactor", {}), compactor or {}),
        nginx             = merged_yaml.get("nginx",    {}),
        gunicorn          = merged_yaml.get("gunicorn", {}),
        active_requests   = _deep_merge(merged_yaml.get("active_requests", {}), active_requests or {}),
        observe           = _deep_merge(merged_yaml.get("observe", {}), observe or {}),
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
        - built-in patterns always on (UUID, numeric ID, SQL literals, etc.)
        - user normalize rules from merged config (defaults.yaml + stacktracer.yaml + init kwarg)
    """
    from .core.graph_normalizer import GraphNormalizer

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


def _init_probes(cfg: ResolvedConfig, engine: Any, app_root: str) -> List[Any]:
    """
    Import all built-in probe modules (side-effect: registers with ProbeRegistry).
    Discover user probe files from <app_root>/stacktracer/probes/.
    Then load and start probes named in cfg.probes.

    Each probe's start() is called with probe-specific kwargs where needed.
    Currently only DjangoProbe.start() takes a kwarg (observe_modules).
    """
    from .sdk.base_probe import ProbeRegistry

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
            logger.debug("Probe module not available: %s — %s", module_path, exc)

    _discover_user_probes(app_root)

    # Per-probe kwargs — keyed by probe name.
    # DjangoProbe needs observe_modules so sys.monitoring knows which
    # module prefixes to watch for view function entry/return events.
    observe_modules: List[str] = cfg.observe.get("modules", [])
    _probe_kwargs = {
        "django": {"observe_modules": observe_modules},
    }

    if observe_modules:
        logger.info("django probe: will observe modules via sys.monitoring: %s", observe_modules)
    else:
        logger.info(
            "django probe: observe.modules not configured — "
            "django.view.enter / django.view.exit events disabled. "
            "Add to stacktracer.yaml:\n"
            "  observe:\n    modules:\n      - myapp\n      - myapp.views"
        )

    probes = ProbeRegistry.load_from_config({"probes": cfg.probes})
    started = []
    for probe in probes:
        kwargs = _probe_kwargs.get(probe.name, {})
        try:
            probe.start(**kwargs)
            started.append(probe)
            logger.info("Probe started: %s", probe.name)
        except Exception as exc:
            logger.warning("Probe %s failed to start: %s", probe.name, exc)

    return started


def _app_root_from_config(config_path: Optional[str]) -> str:
    """
    Return the application root directory for user probe/rule discovery.

    If a stacktracer.yaml was found, the app root is its parent directory —
    that is where the user's stacktracer/probes/ and stacktracer/rules/
    convention directories live.

    If no config was found we fall back to cwd, which is the right answer
    when the user hasn't created a config file yet but is running from their
    project root.

    Example layout:
        myapp/                   ← app root (parent of stacktracer.yaml)
          stacktracer.yaml
          stacktracer/
            probes/
              payment_probe.py
            rules/
              payment_rules.py
    """
    if config_path and os.path.isfile(config_path):
        return os.path.dirname(os.path.abspath(config_path))
    return os.getcwd()


def _discover_user_probes(app_root: str) -> None:
    """
    Auto-discover probe files from <app_root>/stacktracer/probes/*_probe.py.
    Importing them is sufficient — each file registers its BaseProbe subclass
    with ProbeRegistry as a side-effect of the class definition.

    Convention:
        Any file named *_probe.py that contains a class inheriting BaseProbe
        is automatically available as a named probe in cfg.probes.
    """
    import importlib.util

    probes_dir = os.path.join(app_root, "stacktracer", "probes")
    if not os.path.isdir(probes_dir):
        return

    for fname in sorted(os.listdir(probes_dir)):   # sorted for deterministic load order
        if not fname.endswith("_probe.py") or fname.startswith("__"):
            continue
        full_path = os.path.join(probes_dir, fname)
        try:
            spec   = importlib.util.spec_from_file_location(fname[:-3], full_path)
            module = importlib.util.module_from_spec(spec)   # type: ignore[arg-type]
            spec.loader.exec_module(module)                  # type: ignore[union-attr]
            logger.info("User probe discovered: %s", fname)
        except Exception as exc:
            logger.warning("User probe %s failed to load: %s", fname, exc)


def _discover_user_rules(registry: Any, app_root: str) -> None:
    """
    Auto-discover rule files from <app_root>/stacktracer/rules/*_rules.py
    and register them into the already-built PatternRegistry.

    Convention:
        Each *_rules.py file must expose a top-level function:

            def register(registry: PatternRegistry) -> None:
                registry.register(CausalRule(...))
                registry.register(CausalRule(...))

        stacktracer.init() calls register(registry) on every discovered file.
        The function receives the live registry — the same object the engine
        will call .evaluate() on — so rules are immediately active.

    Must be called AFTER _init_pattern_registry() so the registry exists,
    and BEFORE _init_engine() starts the snapshot/evaluation loop —
    though in practice it is safe either way since the engine holds a
    reference to the registry, not a copy.

    Example:
        # myapp/stacktracer/rules/payment_rules.py
        from stacktracer.core.causal import CausalRule

        def register(registry):
            registry.register(CausalRule(
                name        = "payment_retry_storm",
                description = "Payment service retrying rapidly",
                predicate   = lambda g, t: _check(g, t),
                tags        = ["payment", "latency"],
                confidence  = 0.85,
            ))
    """
    import importlib.util

    rules_dir = os.path.join(app_root, "stacktracer", "rules")
    if not os.path.isdir(rules_dir):
        return

    for fname in sorted(os.listdir(rules_dir)):
        if not fname.endswith("_rules.py") or fname.startswith("__"):
            continue
        full_path = os.path.join(rules_dir, fname)
        try:
            spec   = importlib.util.spec_from_file_location(fname[:-3], full_path)
            module = importlib.util.module_from_spec(spec)   # type: ignore[arg-type]
            spec.loader.exec_module(module)                  # type: ignore[union-attr]

            register_fn = getattr(module, "register", None)
            if register_fn is None:
                logger.warning(
                    "User rules file %s has no register(registry) function — skipped",
                    fname,
                )
                continue

            register_fn(registry)
            rule_count = len(registry.rule_names())
            logger.info("User rules loaded from %s (registry now has %d rules)", fname, rule_count)

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
    observe:           Optional[Dict]        = None,
) -> None:
    """
    Initialise StackTracer.

    Minimal usage — all defaults apply:

        # settings.py
        MIDDLEWARE = [
            "stacktracer.probes.django_probe.TracerMiddleware",  # REQUIRED — must be first
            "django.middleware.security.SecurityMiddleware",
            ...
        ]

        # apps.py  (AppConfig.ready() — runs once after Django is fully loaded)
        import stacktracer
        stacktracer.init(api_key=os.getenv("STACKTRACER_API_KEY"))

    TracerMiddleware is NOT optional.
    It is the only place where a trace_id is generated and written into the
    ContextVar.  Without it every downstream probe (URL resolver, view dispatch,
    DB kprobe, asyncio probe) sees get_trace_id() == None and silently drops
    its events.  The middleware must be the FIRST entry in MIDDLEWARE so it
    wraps the entire request before any other middleware can call get_response.

    Django probe internals (NOT sys.monitoring):
        The django probe uses three hooks — all installed via stacktracer.init():
          1. TracerMiddleware      — HTTP request.entry / request.exit
          2. Patched URLResolver   — django.url.resolve event per request
          3. Patched BaseHandler   — django.view.enter event per request
        sys.monitoring (Python 3.12 event system) is used ONLY by the asyncio
        probe for coroutine scheduling events.  It is never used in the django
        probe.  No defaults.yaml "views:" section exists or is needed — every
        view dispatch is already observed through BaseHandler._get_response.

    Config merge order (last wins):
        1. defaults.yaml  (package-shipped — full production stack)
        2. stacktracer.yaml (searched from cwd upward, or explicit config= path)
        3. init() kwargs  (highest priority)

    Merge semantics per field type:
        scalar     sample_rate, flush_interval, snapshot_interval
                   → kwarg wins if provided, else yaml value
        list       probes
                   → kwarg REPLACES yaml list entirely
        semantic   list of label-keyed dicts
                   → three-way label merge — later source wins per label,
                     distinct labels from all sources are kept
        normalize  list of rule dicts (no dedup key)
                   → kwarg EXTENDS yaml list — rules accumulate
        dict       compactor, active_requests
                   → key-by-key deep merge — kwarg keys win, rest from yaml

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

    # ── 1. Load and merge configs ─────────────────────────────────────
    package_defaults = _load_package_defaults()
    user_config_path = _find_user_config(config)
    user_yaml        = _load_user_config(user_config_path)
    merged_yaml      = _deep_merge(package_defaults, user_yaml)

    # Derive app root from where the config was found (or cwd).
    # Both probe and rule discovery use this as their base directory.
    app_root = _app_root_from_config(user_config_path)

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
        observe           = observe,
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

    # User rules — discovered and registered into the registry before the
    # engine starts its evaluation loop.  The engine holds a reference to
    # registry (not a copy) so rules added here are immediately live.
    _discover_user_rules(registry, app_root)

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

    # Probes — started after engine so emit() has a bound engine.
    # User probes are discovered inside _init_probes via _discover_user_probes.
    _active_probes = _init_probes(_config, _engine, app_root)

    # Uploader — started last, after probes are running
    # Only if no explicit repository passed (repository takes precedence)
    if repository is None:
        _init_uploader(_config, _engine)

    # ── 4. Log summary ───────────────────────────────────────────────
    logger.info(
        "StackTracer ready | probes=%s sample_rate=%.1f%% config=%s app_root=%s",
        [p.name for p in _active_probes],
        _config.sample_rate * 100,
        user_config_path or "defaults only",
        app_root,
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