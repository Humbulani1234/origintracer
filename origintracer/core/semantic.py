from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
)


@dataclass
class SemanticAlias:
    label: str
    description: str
    node_patterns: List[
        str
    ]  # Exact node IDs  OR  regex patterns
    services: List[str]  # Service names to include wholesale
    tags: List[str] = field(default_factory=list)

    def matches_node(self, node_id: str) -> bool:
        for pattern in self.node_patterns:
            if pattern == node_id:
                return True
            try:
                # Change re.search to re.fullmatch
                if re.fullmatch(pattern, node_id):
                    return True
            except re.error:
                pass
        return False

    def matches_service(self, service: str) -> bool:
        return service in self.services


class SemanticLayer:
    """
    Maps human business concepts ("export system", "auth pipeline") to
    graph node IDs and service names.

    This layer is what makes queries human-legible.
    Instead of: SHOW latency WHERE node = "django::flags_client.call_remote"
    You write: SHOW latency WHERE system = "export"

    The mapping is built from:
    1. Static registration (config files, code)
    2. Interaction learning (future: watch what engineers search for)

    Usage
    -----
        layer = SemanticLayer()
        layer.register(SemanticAlias(
            label="export",
            description="The export pipeline",
            node_patterns=["django::handle_export", "django::flags_client.*"],
            services=["exporter"],
        ))

        node_ids = layer.resolve_nodes("export", graph)
    """

    def __init__(self) -> None:
        self._aliases: Dict[str, SemanticAlias] = {}

    def register(self, alias: SemanticAlias) -> None:
        self._aliases[alias.label.lower()] = alias

    def resolve_nodes(self, label: str, graph: Any) -> Set[str]:
        """
        Return all graph node IDs that belong to the named system.
        `graph` is a RuntimeGraph (typed loosely to avoid circular imports).
        """
        alias = self._aliases.get(label.lower())
        if not alias:
            return set()

        matched: Set[str] = set()
        for node in graph.all_nodes():
            if alias.matches_node(
                node.id
            ) or alias.matches_service(node.service):
                matched.add(node.id)
        return matched

    def resolve_services(self, label: str) -> List[str]:
        alias = self._aliases.get(label.lower())
        return alias.services if alias else []

    def describe(self, label: str) -> Optional[str]:
        alias = self._aliases.get(label.lower())
        return alias.description if alias else None

    def all_labels(self) -> List[str]:
        return list(self._aliases.keys())

    def __contains__(self, label: str) -> bool:
        return label.lower() in self._aliases

    def __iter__(self) -> Iterator[SemanticAlias]:
        yield from self._aliases.values()


def load_from_dict(data: List[Dict[str, Any]]) -> SemanticLayer:
    """
    Load semantic aliases from a parsed YAML/JSON structure.

    Expected format:
      - label: export
        description: The full export pipeline
        node_patterns:
          - "django::handle_export"
          - "django::flags_client.*"
        services:
          - exporter
        tags:
          - revenue-critical
    """
    layer = SemanticLayer()
    for entry in data:
        layer.register(
            SemanticAlias(
                label=entry["label"],
                description=entry.get("description", ""),
                node_patterns=entry.get("node_patterns", []),
                services=entry.get("services", []),
                tags=entry.get("tags", []),
            )
        )
    return layer


def load_from_yaml(path: str) -> SemanticLayer:
    """Load a single YAML file."""
    import yaml

    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return load_from_dict(data.get("semantic", []))


def merge_yaml_configs(*paths: str) -> dict:
    """
    Load and merge multiple YAML config files.
    Later paths win on conflicts.
    Missing files are silently skipped.
    """
    import yaml

    merged = {"probes": [], "semantic": []}

    for path in paths:
        if not path or not os.path.exists(path):
            continue
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        # Probes: extend the list (union, deduplicated later)
        merged["probes"].extend(data.get("probes", []))
        # Semantic: extend - user aliases simply add to built-ins
        merged["semantic"].extend(data.get("semantic", []))

    # Deduplicate probes by name (last wins)
    seen = {}
    for p in merged["probes"]:
        key = p if isinstance(p, str) else p.get("name", p)
        seen[key] = p
    merged["probes"] = list(seen.values())

    return merged
