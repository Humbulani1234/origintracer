"""
celery_load.py

Steady concurrent load against the Celery tasks app.

URLs hit (all under /tasks/):
    /tasks/report/<id>/       ReportView      — dispatches celery task
    /tasks/cache/<id>/        RedisCacheView  — redis GET/SET then task
    /tasks/bulk-notify/       BulkNotifyView  — dispatches many tasks
    /tasks/export/<id>/       ExportView      — heavy export task
    /tasks/failing/           FailingJobView  — intentionally fails (tests error path)
    /tasks/status/            StatusView      — lightweight poll

Usage:
    python celery_load.py
    python celery_load.py --requests 200 --workers 20
    python celery_load.py --url http://127.0.0.1:8000 --requests 500 --workers 50 --delay 0
"""

import argparse
import random
import threading
import time
from collections import Counter
from urllib.error import HTTPError
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_REQUESTS = 100
DEFAULT_WORKERS = 10
DEFAULT_DELAY = 0.05


def build_queue(base: str, total: int) -> list:
    report_ids = list(range(1, 11))
    export_ids = list(range(1, 6))

    # Weight: report and cache most frequent — they exercise the full
    # django → redis → celery path. failing kept rare but present so
    # the error path appears in the graph.
    pool = []
    for rid in report_ids:
        pool.extend(
            [
                (f"{base}/tasks/report/{rid}/", "GET", None),
            ]
            * 3
        )
    for rid in report_ids:
        pool.extend(
            [
                (f"{base}/tasks/cache/{rid}/", "GET", None),
            ]
            * 3
        )
    for eid in export_ids:
        pool.extend(
            [
                (f"{base}/tasks/export/{eid}/", "GET", None),
            ]
            * 2
        )
    pool.extend(
        [
            (f"{base}/tasks/bulk-notify/", "GET", None),
        ]
        * 5
    )
    pool.extend(
        [
            (f"{base}/tasks/status/", "GET", None),
        ]
        * 5
    )
    pool.extend(
        [
            (f"{base}/tasks/failing/", "GET", None),
        ]
        * 2
    )  # rare — just enough to see error nodes in the graph

    queue = []
    while len(queue) < total:
        queue.extend(pool)
    random.shuffle(queue)
    return queue[:total]


def worker(
    wid: int,
    queue: list,
    results: list,
    delay: float,
    lock: threading.Lock,
):
    while True:
        with lock:
            if not queue:
                return
            url, method, body = queue.pop(0)
        t0 = time.perf_counter()
        try:
            req = Request(url, method=method)
            with urlopen(req, timeout=15) as resp:
                status = resp.status
                resp.read()
        except HTTPError as exc:
            status = exc.code
        except Exception:
            status = 0
        ms = (time.perf_counter() - t0) * 1000
        with lock:
            results.append({"url": url, "status": status, "ms": ms})
        if delay:
            time.sleep(delay)


def print_progress(results, total, lock, stop):
    while not stop.is_set():
        time.sleep(0.5)
        with lock:
            done = len(results)
        pct = done / total * 100
        bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        print(
            f"\r  [{bar}] {done}/{total} ({pct:.0f}%)",
            end="",
            flush=True,
        )
    print()


def summarise(results, elapsed):
    total = len(results)
    statuses = Counter(r["status"] for r in results)
    by_path = {}
    for r in results:
        # extract /tasks/xxx/ from full URL
        path = "/" + r["url"].split("//", 1)[-1].split("/", 1)[-1]
        # collapse numeric IDs for display
        import re

        path = re.sub(r"/\d+/", "/{id}/", path)
        by_path.setdefault(path, []).append(r["ms"])

    durations = sorted(r["ms"] for r in results)
    p50 = durations[int(len(durations) * 0.50)]
    p95 = durations[int(len(durations) * 0.95)]
    p99 = durations[int(len(durations) * 0.99)]
    mean = sum(durations) / len(durations)

    print()
    print("─" * 66)
    print("  celery tasks load summary")
    print(f"  Requests   : {total}")
    print(f"  Elapsed    : {elapsed:.2f}s")
    print(f"  Throughput : {total / elapsed:.1f} req/s")
    print()
    print("  Status codes:")
    for code, count in sorted(statuses.items()):
        note = ""
        if code == 200:
            note = "OK"
        elif code == 0:
            note = "connection error"
        print(f"    {code}  {count:>5}  {note}")
    print()
    print("  Latency (ms)  overall:")
    print(f"    mean {mean:>8.1f}  p50 {p50:>8.1f}  p95 {p95:>8.1f}  p99 {p99:>8.1f}")
    print()
    print("  Per-URL mean latency (ms):")
    for path, times in sorted(by_path.items()):
        avg = sum(times) / len(times)
        print(f"    {path:<35}  {avg:>8.1f}  ({len(times)} requests)")
    print("─" * 66)
    print()
    print("  Explore the graph in the REPL (both gunicorn and celery sockets):")
    print("    SHOW nodes")
    print("    SHOW edges")
    print("    SHOW events LIMIT 20")
    print("    \\stitch <trace_id>   # cross-process trace assembly")
    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_BASE_URL)
    parser.add_argument("--requests", type=int, default=DEFAULT_REQUESTS)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    args = parser.parse_args()

    queue = build_queue(args.url, args.requests)
    results = []
    lock = threading.Lock()
    stop = threading.Event()

    print()
    print("  celery tasks load test")
    print(f"  {args.requests} requests  ·  {args.workers} workers  ·  delay={args.delay}s")
    print(f"  Target: {args.url}")
    print()

    threads = [
        threading.Thread(
            target=worker,
            args=(i, queue, results, args.delay, lock),
            daemon=True,
        )
        for i in range(args.workers)
    ]
    prog = threading.Thread(
        target=print_progress,
        args=(results, args.requests, lock, stop),
        daemon=True,
    )

    t0 = time.perf_counter()
    prog.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    prog.join()

    summarise(results, time.perf_counter() - t0)


if __name__ == "__main__":
    main()
