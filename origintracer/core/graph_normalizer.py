from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("stacktracer.normalizer")


# These cover the most common high-cardinality patterns encountered
# in Django, FastAPI, Flask, and Celery applications.
_BUILTIN_PATTERNS: List[Tuple[str, str]] = [
    # UUIDs  e.g. /api/orders/550e8400-e29b-41d4-a716-446655440000/
    (
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        "{uuid}",
    ),
    # Numeric IDs in URL segments  e.g. /api/users/1234/profile
    (r"/(\d+)/", "/{id}/"),
    (r"/(\d+)$", "/{id}"),
    # Numeric ID at path end without trailing slash
    (r"=(\d+)", "={id}"),
    # Hex object IDs (MongoDB style)  e.g. /api/docs/507f1f77bcf86cd799439011
    (r"/([0-9a-f]{24})/", "/{oid}/"),
    (r"/([0-9a-f]{24})$", "/{oid}"),
    # Memory addresses in coroutine repr  e.g. "coro at 0x7f3a2b4c1d0"
    (r"\s+at\s+0x[0-9a-f]+", ""),
    # Python object repr addresses  e.g. "<Task pending coro=<...> cb=[...] at 0x...>"
    (r"0x[0-9a-f]{8,}", "{addr}"),
    # SQL query normalization — collapse literal values to ?
    # SELECT * FROM users WHERE id = 1234  →  SELECT * FROM users WHERE id = ?
    (r"(?<==\s)(\d+)", "?"),
    (r"(?<==\s)'[^']*'", "?"),
    (r"(?<==\s)\"[^\"]*\"", "?"),
    # Celery task instance IDs embedded in names  e.g. "process_report[abc-123]"
    (r"\[[0-9a-f-]{8,}\]", ""),
    # Timestamp-like segments  e.g. /logs/2024-01-15/
    (r"/\d{4}-\d{2}-\d{2}/", "/{date}/"),
    (r"/\d{4}-\d{2}-\d{2}$", "/{date}"),
]

# Compile once at import time
_BUILTIN_COMPILED: List[Tuple[re.Pattern, str]] = [
    (re.compile(pattern), replacement)
    for pattern, replacement in _BUILTIN_PATTERNS
]


@dataclass
class NormalizationRule:
    """
    A named normalization rule for a specific service.

    Either `pattern` + `replacement` (regex substitution)
    or `fn` (arbitrary function) must be provided.
    """

    service: str  # "django", "celery", "*" for all
    description: str = ""

    # Regex approach
    pattern: Optional[str] = None
    replacement: Optional[str] = None

    # Internal compiled pattern
    _compiled: Optional[re.Pattern] = field(
        default=None, repr=False, compare=False
    )

    def __post_init__(self):
        if self.pattern:
            self._compiled = re.compile(self.pattern)

    def apply(self, name: str) -> str:
        if self._compiled and self.replacement is not None:
            return self._compiled.sub(self.replacement, name)
        return name


class GraphNormalizer:
    """
    Normalizes ``(service, name)`` pairs before graph insertion.

    Applied in order:

    1. Built-in patterns (UUIDs, numeric IDs, memory addresses, SQL literals)
    2. Per-service rules registered by user
    3. Global rules (``service="*"``) registered by user
    4. Max-length truncation

    The Problem
    -----------
    ``RuntimeGraph`` keys nodes by ``"service::name"``. If ``name`` is
    high-cardinality (URL with user IDs, query text, coroutine repr with memory
    addresses), the graph grows without bound::

        /api/users/1234/profile  →  one node per user
        /api/users/5678/profile  →  another node
        ...10,000 users = 10,000 nodes for what is structurally one endpoint

    This module normalizes names to their structural form before graph insertion::

        /api/users/1234/profile  →  /api/users/{id}/profile
        /api/users/5678/profile  →  /api/users/{id}/profile  (same node)

    The graph then has one node per endpoint pattern, with ``call_count``
    accumulating across all individual user requests. This is almost always what
    you want for causal reasoning — we care that *the user profile endpoint is
    slow*, not that user 1234 specifically was slow.

    Usage
    -----
    Pass to ``RuntimeGraph`` at construction time::

        from origintracer.core.graph_normalizer import GraphNormalizer

        normalizer = GraphNormalizer()
        graph = RuntimeGraph(normalizer=normalizer)

    ``RuntimeGraph`` calls ``normalizer.normalize(service, name)`` inside
    ``add_from_event()`` before constructing the node ID.

    Extending
    ---------
    In ``origintracer.yaml``:

    .. code-block:: yaml

        normalize:
          - service: django
            pattern: "/api/items/(\\d+)/"
            replacement: "/api/items/{id}/"
    """

    def __init__(
        self,
        enable_builtins: bool = True,
        max_name_length: int = 200,
        max_unique_names_per_service: int = 500,
    ) -> None:
        self._enable_builtins = enable_builtins
        self._max_name_length = max_name_length

        # Per-service rules: service → list of NormalizationRule
        self._rules: Dict[str, List[NormalizationRule]] = {}

        # Cardinality guard: service >> set of seen normalized names
        # When a service exceeds max_unique_names_per_service, new names
        # are bucketed into "{service}::high_cardinality_node"
        self._max_unique = max_unique_names_per_service
        self._seen_names: Dict[str, set] = {}

        # Cache normalized results to avoid re-computing on every event
        self._cache: Dict[Tuple[str, str], str] = {}
        self._cache_max = 50_000

    def add_pattern(
        self,
        service: str,
        pattern: str,
        replacement: str,
        description: str = "",
    ) -> None:
        r"""
        Add a regex normalization rule for a specific service.
        service = "*" applies to all services.

        Example:
            normalizer.add_pattern(
                service="django",
                pattern=r"/api/items/(\d+)/reviews/(\d+)/",
                replacement="/api/items/{id}/reviews/{review_id}/",
            )
        """
        rule = NormalizationRule(
            service=service,
            description=description,
            pattern=pattern,
            replacement=replacement,
        )
        self._rules.setdefault(service, []).append(rule)
        self._cache.clear()  # invalidate cache on new rule

    def normalize(self, service: str, name: str) -> str:
        """
        Return the normalized form of this (service, name) pair.
        """
        cache_key = (service, name)
        if cache_key in self._cache:
            return self._cache[cache_key]

        result = self._normalize_uncached(service, name)

        # Cardinality guard - if this service has too many unique names,
        # collapse overflow into a sentinel node rather than exploding the graph
        seen = self._seen_names.setdefault(service, set())
        if result not in seen:
            if len(seen) >= self._max_unique:
                logger.warning(
                    "Service '%s' exceeded %d unique node names. "
                    "New name '%s' bucketed to 'high_cardinality_overflow'. "
                    "Add a normalization rule to collapse these names.",
                    service,
                    self._max_unique,
                    result,
                )
                result = "high_cardinality_overflow"
            else:
                seen.add(result)

        # Evict cache if it grows too large (simple FIFO eviction)
        if len(self._cache) >= self._cache_max:
            # Drop oldest 10% of cache entries
            drop_count = self._cache_max // 10
            for key in list(self._cache.keys())[:drop_count]:
                del self._cache[key]

        self._cache[cache_key] = result
        return result

    def _normalize_uncached(
        self, service: str, name: str
    ) -> str:
        result = name

        # Step 1 - built-in patterns (applied to all services)
        if self._enable_builtins:
            for pattern, replacement in _BUILTIN_COMPILED:
                result = pattern.sub(replacement, result)

        # Step 2 - per-service rules
        for rule in self._rules.get(service, []):
            result = rule.apply(result)

        # Step 3 - global rules (service="*")
        for rule in self._rules.get("*", []):
            result = rule.apply(result)

        # Step 4 - truncate to max length
        if len(result) > self._max_name_length:
            result = result[: self._max_name_length] + "…"

        return result.strip()

    @classmethod
    def from_yaml(cls, config: dict) -> "GraphNormalizer":
        """
        Build a GraphNormalizer from the normalize: section of origintracer.yaml.

        YAML format:
            normalize:
              max_unique_names_per_service: 500
              rules:
                - service: django
                  pattern: "/api/items/(\\d+)/"
                  replacement: "/api/items/{id}/"
                  description: "Collapse item IDs"
                - service: celery
                  pattern: "\\[.*?\\]$"
                  replacement: ""
                  description: "Strip task instance brackets"
        """
        if not config:
            return cls()

        max_unique = config.get(
            "max_unique_names_per_service", 500
        )
        normalizer = cls(max_unique_names_per_service=max_unique)

        for rule_cfg in config.get("rules", []):
            normalizer.add_pattern(
                service=rule_cfg["service"],
                pattern=rule_cfg["pattern"],
                replacement=rule_cfg["replacement"],
                description=rule_cfg.get("description", ""),
            )

        return normalizer

    def stats(self) -> dict:
        r"""
        Return cardinality stats per service - useful in REPL \status.
        """
        return {
            service: len(names)
            for service, names in self._seen_names.items()
        }
