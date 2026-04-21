# asyncio Probe - Architecture

The asyncio probe observes the event loop at three layers, from most to least
stable.

## Layer 1 - `sys_epoll_wait` kprobe

The event loop calls `epoll_wait()` as a syscall. The kprobe captures:

- How long the loop blocked waiting for I/O
- How many fds became ready
- Which fd numbers and event types (`EPOLLIN` / `EPOLLOUT`)

This is more accurate than patching `_run_once()` because it measures the
actual kernel call, not a Python wrapper around it.

The `epoll` kprobe also tells us *which fd became ready*, and we correlate:

```
fd --> socket --> connection_type
```

This lets us understand what the task was actually waiting for at the I/O level.

**Correlation with Python trace context** is handled via `KprobeBridge` - 
the bridge writes `(tid, trace_id)` to a BPF map when a trace starts, and the
kprobe reads the map to attribute kernel events to the right trace. See
[KprobeBridge](../../api/kprobe_bridge.md) for details.

## Layer 2 - `Task.__step` patch

Patches `Task._Task__step` at the class level to observe `_fut_waiter` state —
what future a task is currently blocked on.

!!! warning
    `Task` moved to a C extension in Python 3.12+, making this approach
    unstable. It accesses private methods and attributes and will be replaced
    with a `sys.monitoring`-based implementation in a future release.

## Layer 3 - `asyncio.create_task()` wrap

Wraps the public `create_task()` function - not a method on `Task`, not a
private attribute. This is the approved way to observe task creation and is
stable across all Python versions.

## Emitted ProbeTypes

| ProbeType | Fired when |
|---|---|
| `asyncio.loop.epoll_wait` | `epoll_wait` returned with N ready fds |
| `asyncio.loop.tick` | Coroutine entered via `Task.__step` |
| `asyncio.task.create` | `create_task()` called |