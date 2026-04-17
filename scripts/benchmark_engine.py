#!/usr/bin/env python3
"""
Benchmarks Engine.process() throughput at different graph sizes.
Run from the repo root:
    python -m origintracer.scripts.benchmark_engine

What it measures:
    - Events per second at 50/200/500/1000 simulated graph nodes
    - Time per event broken down by phase
    - Buffer drain throughput
    - _find_node cost at each graph size
"""

from __future__ import annotations

import logging
import statistics
import time
import uuid

import origintracer

logging.disable(
    logging.CRITICAL
)  # silence all OriginTracer logs during benchmark


def make_event(
    probe: str, service: str, name: str, trace_id: str = None
):
    from origintracer.core.event_schema import NormalizedEvent

    return NormalizedEvent.now(
        probe=probe,
        trace_id=trace_id or uuid.uuid4().hex[:16],
        service=service,
        name=name,
    )


def warm_up_graph(engine, n_nodes: int) -> None:
    """
    Pre-populate the graph with n_nodes so _find_node has realistic work.
    """
    for i in range(n_nodes):
        make_event(
            probe="django.view.exit",
            service="django",
            name=f"/api/endpoint/{i}/",
        )
        engine.graph.upsert_node(
            node_id=f"django::/api/endpoint/{i}/",
            node_type="django",
            service="django",
        )


def benchmark_process(n_events: int = 10_000, n_nodes: int = 50):
    """
    Benchmark Engine.process() with a pre-warmed graph.
    """
    origintracer.init(debug=False)
    engine = origintracer.get_engine()
    if engine is None:
        print("Engine not started - check stacktracer.init()")
        return

    warm_up_graph(engine, n_nodes)

    events = [
        make_event(
            probe="django.view.exit",
            service="django",
            name=f"/api/users/{i % 10}/",
            trace_id=uuid.uuid4().hex[:16],
        )
        for i in range(n_events)
    ]

    # warm up JIT/caches
    for evt in events[:100]:
        engine.process(evt)

    # measure
    times = []
    t_total = time.perf_counter()

    for evt in events:
        t0 = time.perf_counter()
        engine.process(evt)
        times.append(time.perf_counter() - t0)

    elapsed = time.perf_counter() - t_total
    eps = n_events / elapsed

    p50 = statistics.median(times) * 1e6
    p95 = sorted(times)[int(len(times) * 0.95)] * 1e6
    p99 = sorted(times)[int(len(times) * 0.99)] * 1e6
    mean = statistics.mean(times) * 1e6

    print(
        f"  graph_nodes={n_nodes:>5}  "
        f"events={n_events:>6}  "
        f"throughput={eps:>10,.0f} ev/s  "
        f"mean={mean:>6.1f}µs  "
        f"p50={p50:>6.1f}µs  "
        f"p95={p95:>6.1f}µs  "
        f"p99={p99:>6.1f}µs"
    )

    origintracer.shutdown()
    return eps


def benchmark_find_node(n_nodes: int = 50):
    """
    Benchmark _find_node() isolation.
    """
    origintracer.init(debug=False)
    engine = origintracer.get_engine()
    warm_up_graph(engine, n_nodes)

    # add a known node to find
    engine.graph.upsert_node(
        node_id="gunicorn::UvicornWorker-99999",
        node_type="gunicorn",
        service="gunicorn",
        metadata={"worker_pid": 99999},
    )

    N = 10_000
    t0 = time.perf_counter()
    for _ in range(N):
        engine.graph._find_node(
            node_type="gunicorn",
            metadata_key="worker_pid",
            metadata_value=99999,
        )
    elapsed = time.perf_counter() - t0
    per_call_us = (elapsed / N) * 1e6

    print(
        f"  _find_node  graph_nodes={n_nodes:>5}  "
        f"per_call={per_call_us:>6.2f}µs  "
        f"throughput={N/elapsed:>10,.0f} calls/s"
    )

    origintracer.shutdown()


def benchmark_buffer(n_events: int = 100_000):
    """
    Benchmark EventBuffer push/drain throughput.
    """
    from origintracer.sdk.emitter import _DrainEventBuffer

    buf = _DrainEventBuffer(max_size=50_000)

    events = [
        make_event("django.view.exit", "django", f"/api/{i}/")
        for i in range(n_events)
    ]

    # push benchmark
    t0 = time.perf_counter()
    for evt in events:
        buf.push(evt)
    push_elapsed = time.perf_counter() - t0
    push_eps = n_events / push_elapsed

    # drain benchmark
    drained = 0
    t0 = time.perf_counter()
    while True:
        batch = buf.drain(max_batch=500)
        if not batch:
            break
        drained += len(batch)
    drain_elapsed = time.perf_counter() - t0
    drain_eps = (
        drained / drain_elapsed if drain_elapsed > 0 else 0
    )

    print(
        f"  buffer  push={push_eps:>12,.0f} ev/s  "
        f"drain={drain_eps:>12,.0f} ev/s  "
        f"dropped={buf.dropped}"
    )


def main():
    print("\n=== OriginTracer Engine Benchmark ===\n")

    print("EventBuffer push/drain throughput:")
    benchmark_buffer(100_000)

    print("\n_find_node() cost at increasing graph sizes:")
    for n in [50, 200, 500, 1000, 2000]:
        benchmark_find_node(n)

    print(
        "\nEngine.process() throughput at increasing graph sizes:"
    )
    results = {}
    for n_nodes in [50, 200, 500, 1000]:
        eps = benchmark_process(n_events=5_000, n_nodes=n_nodes)
        results[n_nodes] = eps

    print("\nBreak-even request rates (20 events/request):")
    for n_nodes, eps in results.items():
        rps = eps / 20
        print(
            f"  graph_nodes={n_nodes:>5}  "
            f"max_rps={rps:>8,.0f}  "
            f"({'OK fine' if rps > 500 else 'WARN watch this' if rps > 100 else 'ERR needs fix'})"
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
