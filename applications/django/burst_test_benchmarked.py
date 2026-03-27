"""
Complete benchmark suite for StackTracer.
Requires the /__tracer__/stats/ view to be active in Django.
"""

from __future__ import annotations

import argparse
import json
import random
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_WAVES = 6
DEFAULT_BURST_SIZE = 100
DEFAULT_WORKERS = 15
DEFAULT_QUIET_S = 5.0

# Data Retrieval


def _get_live_engine_stats(base_url: str) -> dict:
    """
    Fetches actual metrics from the running Django process via HTTP.
    This replaces the 'ghost import' that caused 0-stats previously.
    """
    try:
        url = f"{base_url.rstrip('/')}/__tracer__/stats/"
        with urlopen(url, timeout=3) as r:
            data = json.loads(r.read().decode())
            return data
    except Exception as exc:
        return {"error": str(exc)}


def _print_engine_health(
    label: str, stats: dict, prev_dropped: int = 0
) -> int:
    """Prints engine health with your original formatting."""
    if not stats or "error" in stats:
        print(
            f"    [{label}] engine stats unavailable (check Django route)"
        )
        return prev_dropped

    dropped_total = stats.get("buf_dropped", 0)
    dropped_delta = dropped_total - prev_dropped
    health = "OK" if dropped_delta == 0 else "DROPPED"

    print(
        f"    [{label}] "
        f"nodes={stats.get('node_count', 0)}  "
        f"edges={stats.get('edge_count', 0)}  "
        f"buf_depth={stats.get('buf_depth', 0)}  "
        f"in_flight={stats.get('in_flight', 0)}  "
        f"{health}"
        + (f"={dropped_delta}" if dropped_delta > 0 else "")
    )

    if dropped_delta > 0:
        print(
            f"    [{label}] total dropped={dropped_total} — drain is lagging"
        )

    # Patterns Summary
    patterns = stats.get("patterns_summary", {})
    if patterns:
        top = sorted(
            patterns.items(),
            key=lambda x: x[1]["count"],
            reverse=True,
        )[:4]
        for pattern, s in top:
            print(
                f"             {pattern[:48]:<48}  "
                f"n={s['count']:>4}  "
                f"avg={s['avg_ms']:>6.1f}ms  "
                f"p99={s['p99_ms']:>6.1f}ms"
            )

    return dropped_total


# HTTP burst logic


def build_pool(base: str) -> list:
    return (
        [(f"{base}/", "GET")] * 30
        + [(f"{base}/async/", "GET")] * 25
        + [(f"{base}/db/", "GET")] * 25
        + [(f"{base}/n1/", "GET")] * 15
        + [(f"{base}/slow/", "GET")] * 5
    )


def fire_burst(base_url: str, count: int, workers: int) -> dict:
    pool = build_pool(base_url.rstrip("/"))
    queue = (pool * ((count // len(pool)) + 1))[:count]
    random.shuffle(queue)

    results = []
    lock = threading.Lock()

    def _worker():
        while True:
            with lock:
                if not queue:
                    return
                url, method = queue.pop(0)
            t0 = time.perf_counter()
            try:
                # Increased timeout to 20s to prevent the 15s deadlock errors
                with urlopen(
                    Request(url, method=method), timeout=20
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
    ms = [r["ms"] for r in results]

    return {
        "count": len(results),
        "ok": ok,
        "err": len(results) - ok,
        "elapsed": elapsed,
        "rps": len(results) / elapsed if elapsed else 0,
        "mean_ms": sum(ms) / len(ms) if ms else 0,
        "p95_ms": sorted(ms)[int(len(ms) * 0.95)] if ms else 0,
        "p99_ms": sorted(ms)[int(len(ms) * 0.99)] if ms else 0,
    }


# -------------------- Main ----------------------------------------


def main():
    import pdb

    pdb.set_trace()
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

    print("\n🚀 StackTracer Burst Test Suite v2.0")
    print(
        f"   {args.waves} waves · {args.burst} req/burst · {args.workers} workers"
    )
    print(f"   Target: {args.url}\n")

    total_requests = 0
    # total_dropped = 0
    all_wave_rps = []
    prev_dropped = 0

    # Baseline
    pre_stats = _get_live_engine_stats(args.url)
    if pre_stats and "error" not in pre_stats:
        print("   Baseline engine state:")
        prev_dropped = _print_engine_health("pre ", pre_stats, 0)
        print()

    for wave in range(1, args.waves + 1):
        print(
            f"   ── Wave {wave}/{args.waves} firing...",
            end="",
            flush=True,
        )

        r = fire_burst(args.url, args.burst, args.workers)
        total_requests += r["count"]
        all_wave_rps.append(r["rps"])

        print(
            f"\n   ── Wave {wave} HTTP:  {r['elapsed']:.2f}s  {r['rps']:.0f} req/s  ok={r['ok']} err={r['err']}"
        )
        print(
            f"      Mean={r['mean_ms']:.0f}ms  p95={r['p95_ms']:.0f}ms  p99={r['p99_ms']:.0f}ms"
        )

        # Engine health immediately after wave
        post_wave = _get_live_engine_stats(args.url)
        if post_wave:
            est_events = r["count"] * 12  # Normalized estimate
            print(
                f"   ── Wave {wave} probes: ~{est_events:,} events emitted"
            )
            prev_dropped = _print_engine_health(
                "post", post_wave, prev_dropped
            )

        if wave < args.waves:
            print(
                f"\n      Quiet {args.quiet}s — draining...",
                end="",
                flush=True,
            )
            time.sleep(args.quiet)
            post_quiet = _get_live_engine_stats(args.url)
            depth = (
                post_quiet.get("buf_depth", 0)
                if post_quiet
                else 0
            )
            print(
                f" {'✓ drained' if depth == 0 else f'⚠ depth: {depth}'}"
            )

    # Final Summary
    final_stats = _get_live_engine_stats(args.url)
    print("\n   ________________________________")
    print("   Run complete")
    print("   _________________________________")
    if final_stats and "error" not in final_stats:
        print(f"   Total Requests: {total_requests:,}")
        print(
            f"   Total Dropped:  {final_stats.get('buf_dropped', 0):,}"
        )
        print(f"   Peak RPS:       {max(all_wave_rps):.0f}")

        patterns = final_stats.get("patterns_summary", {})
        if patterns:
            print("\n   Latency Breakdown:")
            for p, s in sorted(
                patterns.items(),
                key=lambda x: x[1]["p99_ms"],
                reverse=True,
            ):
                bar = "█" * min(int(s["p99_ms"] / 20), 20)
                print(
                    f"   {p[:40]:<40} | {s['count']:>4} hits | {s['p99_ms']:>6.1f}ms p99 {bar}"
                )


if __name__ == "__main__":
    main()
