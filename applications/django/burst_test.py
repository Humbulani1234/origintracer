"""
django_burst.py

Burst traffic against the django_tracer app.
Sends waves with quiet gaps — watch the graph build and stabilise
between bursts in the REPL.

URLs hit:
    /           IndexView
    /async/     AsyncView
    /slow/      SlowView  (kept rare)
    /db/        DbView
    /n1/        NPlusOneView

Usage:
    python django_burst.py
    python django_burst.py --waves 6 --burst 40 --quiet 4
"""

import argparse
import random
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_WAVES = 4
DEFAULT_BURST_SIZE = 30
DEFAULT_WORKERS = 15
DEFAULT_QUIET_S = 3.0


def build_pool(base: str) -> list:
    return (
        [(f"{base}/", "GET", None)] * 30
        + [(f"{base}/async/", "GET", None)] * 25
        + [(f"{base}/db/", "GET", None)] * 25
        + [(f"{base}/n1/", "GET", None)] * 15
        + [(f"{base}/slow/", "GET", None)] * 5
    )


def fire_burst(base_url: str, count: int, workers: int) -> dict:
    pool = build_pool(base_url)
    queue = (pool * ((count // len(pool)) + 1))[:count]
    random.shuffle(queue)

    results = []
    lock = threading.Lock()

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
            with lock:
                results.append(
                    {
                        "status": status,
                        "ms": (time.perf_counter() - t0) * 1000,
                    }
                )

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
    return {
        "count": len(results),
        "ok": ok,
        "err": err,
        "elapsed": elapsed,
        "rps": len(results) / elapsed,
        "mean_ms": sum(ms) / len(ms) if ms else 0,
        "p95_ms": sorted(ms)[int(len(ms) * 0.95)] if ms else 0,
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
    print("  django_tracer burst test")
    print(
        f"  {args.waves} waves  ·  {args.burst} req/burst  "
        f"·  {args.workers} workers  ·  {args.quiet}s quiet"
    )
    print(f"  Target: {args.url}")
    print()

    for wave in range(1, args.waves + 1):
        print(
            f"  ── Wave {wave}/{args.waves} "
            f"── firing {args.burst} requests ...",
            end="",
            flush=True,
        )
        r = fire_burst(args.url, args.burst, args.workers)
        print(
            f"  {r['elapsed']:.2f}s  {r['rps']:.0f} req/s  "
            f"ok={r['ok']} err={r['err']}  "
            f"mean={r['mean_ms']:.0f}ms  p95={r['p95_ms']:.0f}ms"
        )

        if wave < args.waves:
            print(
                f"         quiet {args.quiet}s "
                f"— run SHOW nodes in REPL now ...",
                end="",
                flush=True,
            )
            time.sleep(args.quiet)
            print()

    print()
    print("  All waves complete.")
    print("  Explore the graph:")
    print("    SHOW nodes")
    print("    SHOW edges")
    print("    SHOW events LIMIT 20")
    print()


if __name__ == "__main__":
    main()
