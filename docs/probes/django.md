# Django Probe

The Django probe observes request lifecycle, database queries, class-based
views, and unhandled exceptions using stable, version-safe extension points.
No private APIs are touched except `View.dispatch()`, which is a single patch
on the public CBV base class.

## Setup

`TracerMiddleware` is required and must be **first** in `MIDDLEWARE`:

```python
MIDDLEWARE = [
    "origintracer.probes.django_probe.TracerMiddleware",
    "django.middleware.security.SecurityMiddleware",
    ...
]
```

Everything else is installed automatically when `origintracer.init()` is called.

## Extension Points

| Hook | What it observes |
|---|---|
| `TracerMiddleware` | Full request lifecycle — entry, exit, duration, trace ID |
| `connection.execute_wrapper()` | Every ORM and raw SQL query with duration |
| `View.dispatch()` patch | CBV entry/exit with class name and duration |
| `got_request_exception` signal | Unhandled exceptions |

## Emitted ProbeTypes

| ProbeType | Fired when |
|---|---|
| `request.entry` | Request enters middleware |
| `request.exit` | Response leaves middleware |
| `django.view.enter` | CBV `dispatch()` entered |
| `django.view.exit` | CBV `dispatch()` returned |
| `django.db.query` | ORM or raw SQL query executed |
| `django.exception` | Unhandled exception raised |

## Trace ID Resolution

`TracerMiddleware` resolves `trace_id` in this order (first wins):

1. `X-Request-ID` HTTP header - for distributed tracing from an upstream proxy
2. Existing `trace_id` in context - for nested or chained calls
3. Fresh `uuid4()` - generated if none of the above are present

## KprobeBridge Integration

When `KprobeBridge` is available, `TracerMiddleware` registers the trace
into the BPF map so kernel-level probes (epoll, syscalls) can attribute
events to the correct Django request:

```python
bridge.register_trace(
    tid=loop_tid,
    trace_id=trace_id,
    service="django",
    start_ns=int(time.time_ns()),
    pid=os.getpid(),
)
```

Both the event loop TID and the handler thread TID are registered. On
request completion both are unregistered. If `KprobeBridge` is unavailable
(no `CAP_BPF`), this step is silently skipped - all Python-side observations
still function normally.

## Database Queries

The DB wrapper is installed via the `connection_created` signal rather than
iterating `connections.all()` at startup. This avoids the empty connections
problem - Django has not opened any DB connections yet when
`AppConfig.ready()` fires. The wrapper attaches automatically the first time
each connection is opened.

Each `django.db.query` event includes:

```python
{
    "name": str, # SQL text, truncated to 200 chars
    "duration_ns": int,
    "db_alias": str, # e.g. "default"
    "success": bool,
    "row_count": int, # on success
    "error": str, # on failure, truncated to 200 chars
}
```

## Class-Based Views

`View.dispatch()` is the single entry point for all CBVs. One patch captures
every CBV automatically - `AsyncView`, `SlowView`, `DetailView` etc. — without
needing to instrument each one individually.

!!! note
    `dispatch()` itself is synchronous even for async views. Django routes
    the request to `get()` / `post()` which may be async, but `dispatch()`
    always returns synchronously (or returns a coroutine that Django awaits).
    The `finally` block fires correctly in both cases at the dispatch level.

## Disabling the Probe

The probe is removed cleanly on `origintracer.stop()`:

- `View.dispatch()` is restored to the original
- The DB execute wrapper is removed from all open connections
- The `got_request_exception` signal handler is disconnected

Calling `stop()` and `start()` again is safe - the probe guards against
double-installation with a module-level `_patched` flag.