from .retry_rules import RETRY_AMPLIFICATION
from ..core.causal import PatternRegistry

def build_default_registry() -> PatternRegistry:
    """Return a PatternRegistry pre-loaded with all built-in rules."""
    registry = PatternRegistry()
    registry.register(RETRY_AMPLIFICATION)
    registry.register(NEW_SYNC_CALL)
    registry.register(LOOP_STARVATION)
    registry.register(DB_HOTSPOT)
    return registry