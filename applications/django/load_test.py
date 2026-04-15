"""
Steady concurrent load against the django_tracer app.

URLs hit:
    / IndexView
    /async/ - AsyncView
    /slow/ - SlowView
    /db/ - DbView
    /n1/ - NPlusOneView

Usage:
    python django_load.py
    python django_load.py --requests 200 --workers 20
    python django_load.py --url http://127.0.0.1:8000 --requests 500 --workers 50 --delay 0
"""

import argparse
import random
import threading
import time
from collections import Counter
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_REQUESTS = 100
DEFAULT_WORKERS = 10
DEFAULT_DELAY = 0.05


def build_queue(base: str, total: int) -> list:
    # Weight the URLs - slow and n+1 less frequent so they don't time out everything
    weighted = [
        (f"{base}/", "GET", None, 30),  # index - most traffic
        (f"{base}/async/", "GET", None, 25),  # async view
        (f"{base}/db/", "GET", None, 25),  # db view
        (f"{base}/n1/", "GET", None, 15),  # n+1 view
        (f"{base}/slow/", "GET", None, 5),  # slow - keep rare
    ]
    pool = []
    for url, method, body, weight in weighted:
        pool.extend([(url, method, body)] * weight)
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
            results.append(
                {"url": url, "status": status, "ms": ms}
            )
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
    by_url = {}
    for r in results:
        path = r["url"].split("//", 1)[-1].split("/", 1)[-1]
        path = "/" + path if not path.startswith("/") else path
        by_url.setdefault(path, []).append(r["ms"])

    durations = sorted(r["ms"] for r in results)
    p50 = durations[int(len(durations) * 0.50)]
    p95 = durations[int(len(durations) * 0.95)]
    p99 = durations[int(len(durations) * 0.99)]
    mean = sum(durations) / len(durations)

    print()
    print("─" * 62)
    print("  django_tracer load summary")
    print(f"  Requests   : {total}")
    print(f"  Elapsed    : {elapsed:.2f}s")
    print(f"  Throughput : {total / elapsed:.1f} req/s")
    print()
    print("  Status codes:")
    for code, count in sorted(statuses.items()):
        print(f"    {code}  {count:>5}")
    print()
    print("  Latency (ms)  overall:")
    print(
        f"    mean {mean:>8.1f}  p50 {p50:>8.1f}  p95 {p95:>8.1f}  p99 {p99:>8.1f}"
    )
    print()
    print("  Per-URL mean latency (ms):")
    for path, times in sorted(by_url.items()):
        avg = sum(times) / len(times)
        print(
            f"    {path:<20}  {avg:>8.1f}  ({len(times)} requests)"
        )
    print("─" * 62)
    print()
    print("  Explore the graph in the REPL:")
    print("    SHOW nodes")
    print("    SHOW edges")
    print("    SHOW events LIMIT 20")
    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_BASE_URL)
    parser.add_argument(
        "--requests", type=int, default=DEFAULT_REQUESTS
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS
    )
    parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY
    )
    args = parser.parse_args()

    queue = build_queue(args.url, args.requests)
    results = []
    lock = threading.Lock()
    stop = threading.Event()

    print()
    print("  django_tracer load test")
    print(
        f"  {args.requests} requests  ·  {args.workers} workers  ·  delay={args.delay}s"
    )
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
