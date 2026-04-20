# Causal Rules Overview

Rules are plain Python predicates evaluated against the `RuntimeGraph`. Each rule returns `(matched: bool, evidence: dict)`.

## Built-in starter rules

### Loop starvation

Fires when asyncio loop ticks average over 10ms. A blocking operation — CPU work, synchronous I/O, missing await — is starving other coroutines.

```python
# fires when:
n.avg_duration_ns > 10_000_000  # >10ms per tick
```

### N+1 query

Fires when a database query node is called more than 2× per view call. Classic ORM N+1 pattern.

```python
# fires when:
node.call_count >= caller.call_count * 2
```

### Retry amplification

Fires when downstream edges have high retry counts. A slow dependency is being amplified by retry loops.

```python
# fires when:
e.metadata.get("retries", 0) > 3
```

### New sync call after deployment

Fires when new call edges appeared after the most recent deployment marker. A newly introduced synchronous dependency is the probable root cause of latency.

```python
# fires when:
new_edges_since_deployment and ":calls" in edge_key
```

## Running rules

```
› CAUSAL
```

Or filter by tag:

```
› CAUSAL latency
› CAUSAL db
› CAUSAL asyncio
```

---

# Writing Custom Rules

Drop a `*_rules.py` file anywhere in your project. StackTracer discovers it automatically at startup.

## Minimal example

```python
# myapp/stacktracer/rules/payment_rules.py
from stacktracer.core.causal import CausalRule


def _slow_payment(graph, temporal):
    """Fire if payment.initialize averages over 2 seconds."""
    nodes = [
        n for n in graph.all_nodes()
        if n.service == "payment"
        and n.avg_duration_ns
        and n.avg_duration_ns > 2_000_000_000
    ]
    if not nodes:
        return False, {}
    return True, {
        "slow_nodes": [
            {"node": n.id, "avg_ms": round(n.avg_duration_ns / 1e6, 1)}
            for n in nodes
        ]
    }


def register(registry):
    registry.register(CausalRule(
        name        = "slow_payment_gateway",
        description = "Payment gateway averaging over 2s — check Paystack status.",
        predicate   = _slow_payment,
        confidence  = 0.85,
        tags        = ["payment", "latency"],
    ))
```

## Rule anatomy

```python
CausalRule(
    name        = "rule_name",          # unique identifier
    description = "Human explanation",  # shown in REPL output
    predicate   = my_fn,                # (graph, temporal) → (bool, dict)
    confidence  = 0.85,                 # 0.0–1.0
    tags        = ["latency"],          # for CAUSAL <tag> filtering
)
```

The predicate receives the live `RuntimeGraph` and `TemporalStore`. It must never raise — exceptions are caught and logged with `confidence=0.0`.

---