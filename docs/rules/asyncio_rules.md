# Built-in Rules

# New Sync Call After Deployment

**Confidence:** 0.85  
**Tags:** `deployment` `latency` `async`

## What it detects

A new synchronous call edge appeared in the graph after the most recent
deployment marker, and the nodes involved in those new edges are experiencing
elevated latency (>200ms average duration).

The new dependency itself is the probable root cause — not unrelated parts
of the system.

## Why this matters

Deployments that introduce a new synchronous call to a slow downstream service
are a common source of latency regressions. The pattern is:

1. Deploy goes out
2. A code path that was previously fire-and-forget (async, cached, removed)
   now calls a downstream service synchronously
3. That downstream service is slow
4. Latency spikes, but the call stack looks normal to APM tools because
   the new edge has always existed *since the deploy*

This rule makes that invisible dependency visible.

## Firing conditions

All of the following must be true:

| Condition | Detail |
|---|---|
| Deployment marker exists | `temporal.label_diff("deployment")` returns a result |
| 120s have elapsed | Establishes a post-deployment baseline |
| Post-deployment snapshots exist | At least one graph snapshot captured after deploy |
| New `:calls` edges exist | Edges not present in the pre-deployment baseline |
| Involved nodes are slow | `avg_duration_ns > 200ms` on nodes in new edges |

## Return payload

```python
{
    "deployment_timestamp": float, # unix timestamp of the deployment marker
    "new_sync_edges": list, # up to 10 new edge keys e.g. "django::A→django::B:calls"
    "slow_nodes": list, # node IDs of slow nodes in new edges
}
```

## Tuning

The two thresholds are currently hardcoded:

- **120s baseline window** - increase if your deployments take longer to
  stabilise before traffic is representative
- **200ms latency threshold** - lower this for latency-sensitive services,
  raise it to reduce noise in slower backends

## Example output

A firing rule on a Django app that added a synchronous Postgres call after
deploying a new middleware:

```python
{
    "deployment_timestamp": 1718023400.0,
    "new_sync_edges": [
        "django::middleware.AuthMiddleware→postgres::users:calls"
    ],
    "slow_nodes": [
        "postgres::users"
    ]
}
```

# asyncio Event Loop Starvation

**Confidence:** 0.80  
**Tags:** `asyncio` `latency` `blocking`

## What it detects

Event loop ticks averaging above 10ms, indicating something is blocking the
loop — CPU-bound work, a synchronous I/O call, or a missing `await` on the
hot path. While one coroutine blocks, all others starve.

## Why this matters

asyncio is cooperative. If one coroutine holds the loop for >10ms, every
other coroutine waiting to run is delayed by at least that much. Under load
this compounds - a single blocking call on a hot endpoint can make the entire
application feel slow even though only one code path is at fault.

Common causes:

- A `requests.get()` or `psycopg2` query called without `await`
- CPU-heavy work (serialization, regex, encryption) on the hot path
- A third-party library that is not async-native

## Firing conditions

| Condition | Detail |
|---|---|
| Node exists | `asyncio::loop.tick` present in the graph |
| Tick duration elevated | `avg_duration_ns > 3ms` on the tick node |

!!! note
    The predicate threshold is **3ms** at the node level. The rule description
    states 10ms because that is the user-facing threshold - ticks must average
    above 10ms across the observation window to be meaningful. The 3ms node
    threshold filters out momentary spikes.

## Return payload

```python
{
    "stalled_ticks": [
        {
            "node": str, # always "asyncio::loop.tick"
            "avg_ms": float, # average tick duration in milliseconds
            "count": int, # number of ticks observed
        }
        # up to 5 entries
    ]
}
```

## Tuning

The 3ms threshold in `_asyncio_loop_starvation` is conservative by design.
Lower it to catch subtler starvation, raise it to reduce noise in
CPU-constrained environments:

```python
and n.avg_duration_ns > 3_000_000   # 3ms — change to taste
```

## What to do when this fires

1. Check `stalled_ticks[0].avg_ms` - anything above 50ms is severe
2. Profile the hot path with `py-spy` or `austin` to find the blocking call
3. Wrap CPU-heavy work in `asyncio.run_in_executor()`
4. Replace synchronous clients (`requests`, `psycopg2`) with async equivalents
   (`httpx`, `asyncpg`)

 