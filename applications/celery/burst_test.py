"""
celery_burst.py

Burst traffic against the Celery tasks app.
Each wave fires concurrent requests then goes quiet so you can
observe the graph in the REPL between bursts — including how
celery tasks land after the HTTP request has already returned.

URLs hit (all under /tasks/):
    /tasks/report/<id>/       full django → redis → celery path
    /tasks/cache/<id>/        redis cache hit/miss then celery
    /tasks/bulk-notify/       fans out many celery tasks at once
    /tasks/export/<id>/       heavier task
    /tasks/failing/           error path (rare)
    /tasks/status/            lightweight poll

Usage:
    python celery_burst.py
    python celery_burst.py --waves 5 --burst 50 --quiet 5
    python celery_burst.py --waves 3 --burst 100 --workers 30 --quiet 8
"""

import argparse
import re
import random
import time
import threading
from urllib.request import urlopen, Request
from urllib.error import HTTPError
from collections import Counter

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_WAVES = 4
DEFAULT_BURST_SIZE = 30
DEFAULT_WORKERS = 15
DEFAULT_QUIET_S = 4.0


def build_pool(base: str) -> list:
    report_ids = list(range(1, 11))
    export_ids = list(range(1, 6))
    pool = []
    for rid in report_ids:
        pool.append((f"{base}/tasks/report/{rid}/", "GET", None))
        pool.append((f"{base}/tasks/cache/{rid}/", "GET", None))
        pool.append(
            (f"{base}/tasks/cache/{rid}/", "GET", None)
        )  # double weight — cache hits
    for eid in export_ids:
        pool.append((f"{base}/tasks/export/{eid}/", "GET", None))
    pool.extend(
        [
            (f"{base}/tasks/bulk-notify/", "GET", None),
            (f"{base}/tasks/bulk-notify/", "GET", None),
            (f"{base}/tasks/status/", "GET", None),
            (f"{base}/tasks/status/", "GET", None),
            (f"{base}/tasks/status/", "GET", None),
            (f"{base}/tasks/failing/", "GET", None),  # rare
        ]
    )
    return pool


def fire_burst(base_url: str, count: int, workers: int) -> dict:
    pool = build_pool(base_url)
    queue = (pool * ((count // len(pool)) + 1))[:count]
    random.shuffle(queue)

    results = []
    lock = threading.Lock()
    by_path = {}

    def _worker():
        while True:
            with lock:
                if not queue:
                    return
                url, method, _ = queue.pop(0)
            t0 = time.perf_counter()
            try:
                with urlopen(
                    Request(url, method=method), timeout=15
                ) as r:
                    status = r.status
                    r.read()
            except HTTPError as exc:
                status = exc.code
            except Exception:
                status = 0
            ms = (time.perf_counter() - t0) * 1000
            path = re.sub(
                r"/\d+/",
                "/{id}/",
                "/" + url.split("//", 1)[-1].split("/", 1)[-1],
            )
            with lock:
                results.append({"status": status, "ms": ms})
                by_path.setdefault(path, []).append(ms)

    threads = [
        threading.Thread(target=_worker, daemon=True)
        for _ in range(min(workers, count))
    ]
    t0 = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0

    ok = sum(1 for r in results if r["status"] == 200)
    err = len(results) - ok
    ms = [r["ms"] for r in results]
    statuses = Counter(r["status"] for r in results)

    return {
        "count": len(results),
        "ok": ok,
        "err": err,
        "elapsed": elapsed,
        "rps": len(results) / elapsed,
        "mean_ms": sum(ms) / len(ms) if ms else 0,
        "p95_ms": sorted(ms)[int(len(ms) * 0.95)] if ms else 0,
        "statuses": statuses,
        "by_path": by_path,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_BASE_URL)
    parser.add_argument(
        "--waves", type=int, default=DEFAULT_WAVES
    )
    parser.add_argument(
        "--burst", type=int, default=DEFAULT_BURST_SIZE
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS
    )
    parser.add_argument(
        "--quiet", type=float, default=DEFAULT_QUIET_S
    )
    args = parser.parse_args()

    print()
    print(f"  celery tasks burst test")
    print(
        f"  {args.waves} waves  ·  {args.burst} req/burst  "
        f"·  {args.workers} workers  ·  {args.quiet}s quiet"
    )
    print(f"  Target: {args.url}/tasks/")
    print()

    all_results = []

    for wave in range(1, args.waves + 1):
        print(
            f"  ── Wave {wave}/{args.waves} "
            f"── firing {args.burst} requests ...",
            end="",
            flush=True,
        )
        r = fire_burst(args.url, args.burst, args.workers)
        all_results.append(r)

        codes = "  ".join(
            f"{k}:{v}" for k, v in sorted(r["statuses"].items())
        )
        print(
            f"  {r['elapsed']:.2f}s  {r['rps']:.0f} req/s  "
            f"[{codes}]  "
            f"mean={r['mean_ms']:.0f}ms  p95={r['p95_ms']:.0f}ms"
        )

        if wave < args.waves:
            print(
                f"         quiet {args.quiet}s "
                f"— celery tasks still landing, run SHOW nodes in REPL ...",
                end="",
                flush=True,
            )
            time.sleep(args.quiet)
            print()

    # aggregate summary across all waves
    print()
    print("─" * 66)
    print("  All waves complete — aggregate summary")
    total_reqs = sum(r["count"] for r in all_results)
    total_ok = sum(r["ok"] for r in all_results)
    total_err = sum(r["err"] for r in all_results)
    total_s = sum(r["elapsed"] for r in all_results)
    all_ms = []
    agg_paths = {}
    for r in all_results:
        for path, times in r["by_path"].items():
            agg_paths.setdefault(path, []).extend(times)
            all_ms.extend(times)
    all_ms.sort()
    mean = sum(all_ms) / len(all_ms) if all_ms else 0
    p95 = all_ms[int(len(all_ms) * 0.95)] if all_ms else 0

    print(
        f"  Total requests : {total_reqs}  ok={total_ok}  err={total_err}"
    )
    print(f"  Mean latency   : {mean:.0f}ms   p95={p95:.0f}ms")
    print()
    print("  Per-URL mean latency (ms):")
    for path, times in sorted(agg_paths.items()):
        avg = sum(times) / len(times)
        print(
            f"    {path:<35}  {avg:>8.1f}  ({len(times)} requests)"
        )
    print("─" * 66)
    print()
    print("  Explore the graph in the REPL:")
    print("    SHOW nodes")
    print("    SHOW edges")
    print("    SHOW events LIMIT 20")
    print(
        "    \\stitch <trace_id>   # stitch gunicorn + celery graphs"
    )
    print()


if __name__ == "__main__":
    main()
