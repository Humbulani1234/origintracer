"""
Run a bounded end-to-end OriginTracer workload through Nginx.

The runner sends real HTTP requests through Nginx to the existing demo routes;
it does not start, stop, or reconfigure any services. Use the Nginx listener
(normally port 80), not the Gunicorn/Uvicorn port, so each request traverses
Nginx -> Gunicorn -> Uvicorn -> Django.

WHAT THIS RUN EXERCISES

- asyncio_event_loop_starvation: GET /slow/ calls time.sleep() in an async
  view.
- db_query_hotspot: GET /db/ and GET /n1/ repeat ORM reads. /n1/ is most
  useful when the demo database has multiple Author rows.
- n_plus_one_queries: GET /n1/ loops over authors and queries their books.
- duplicate_transaction: sequential POSTs to /write-twice/ execute two UPDATEs
  per view call and roll the transaction back. Enable this demo endpoint in the
  Django/Gunicorn process before startup with
  ORIGINTRACER_ENABLE_DEMO_SCENARIOS=1. It is disabled by default.
- request_duration_anomaly: a match requires at least 50 historical calls, at
  least 10 recent samples, and recent P99 above 3x the historical average.
  The waves provide samples but do not guarantee that ratio.
- traffic_spike: can match when at least 10 nodes are newly added in the graph
  diff window. A warm graph may not have new nodes.
- post_deployment_regression: can match when a new edge involves a node with
  average duration of at least 200 ms. The runner does not create a deployment
  marker, and a warm graph may not have new edges.
- external_dependency_timeout: optional --external-requests calls hit a local
  delayed HTTP server. Enable ORIGINTRACER_ENABLE_DEMO_SCENARIOS=1 and the
  core external_http probe first. Run the script on the same host as Django.
- worker_imbalance: the backend compares request-counter deltas across fresh
  snapshots from workers under one Gunicorn master. It needs at least two
  measured workers and 20 observed requests (30 seconds for a zero-request
  worker). Use at least two Gunicorn workers; the workload includes blocking
  /slow/ requests, but scheduler distribution means a match is not guaranteed.
- retry_amplification: real Nginx upstream attempts are counted. It requires
  more than 3 retries on an individual request, so this workload only matches
  if the site's Nginx upstream configuration actually retries that many times.

WHAT THIS RUN CANNOT GUARANTEE OR GENERATE

- cascade_failure: this workload does not intentionally crash a worker. The
  rule now requires explicit worker/process crash telemetry; graph compaction
  and node removal alone cannot trigger it.

Rule matches are conditional on actual recorded evidence and each rule's
thresholds; the runner does not inject synthetic telemetry. Defaults are
3 waves of 40 requests with at most 4 concurrent clients to keep load modest.
"""

from __future__ import annotations

import argparse
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import (
    BaseHTTPRequestHandler,
    ThreadingHTTPServer,
)
from threading import Thread
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "http://127.0.0.1:80"
ROUTES = ("/", "/async/", "/slow/", "/db/", "/n1/")
WRITE_SCENARIO = "/write-twice/"
EXTERNAL_SCENARIO = "/external/"
DEMO_DEPENDENCY_PORT = 18765
MAX_CONCURRENCY = 8


class _DelayedDependencyHandler(BaseHTTPRequestHandler):
    """
    Local-only dependency that responds just above the 2-second threshold.
    """

    def do_GET(self):
        time.sleep(2.2)
        body = b"delayed dependency response"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


def _request(
    base_url: str,
    path: str,
    timeout: float,
    method: str = "GET",
) -> tuple[int, float, str | None]:
    """
    Issue one request and return status, elapsed milliseconds, and an error.
    """
    started = time.perf_counter()
    try:
        request = Request(
            f"{base_url.rstrip('/')}{path}",
            data=b"" if method == "POST" else None,
            headers={
                "User-Agent": "OriginTracer-rule-scenario/1.0"
            },
            method=method,
        )
        with urlopen(request, timeout=timeout) as response:
            response.read()
            return (
                response.status,
                (time.perf_counter() - started) * 1000,
                None,
            )
    except HTTPError as exc:
        return (
            exc.code,
            (time.perf_counter() - started) * 1000,
            str(exc),
        )
    except (TimeoutError, URLError, OSError) as exc:
        return (
            0,
            (time.perf_counter() - started) * 1000,
            str(exc),
        )


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(
        len(ordered) - 1, int((len(ordered) - 1) * percentile)
    )
    return ordered[index]


def _run_wave(
    base_url: str,
    request_count: int,
    concurrency: int,
    timeout: float,
) -> dict[str, object]:
    paths = [
        ROUTES[index % len(ROUTES)]
        for index in range(request_count)
    ]
    results = []
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [
            pool.submit(_request, base_url, path, timeout)
            for path in paths
        ]
        for future in as_completed(futures):
            results.append(future.result())
    elapsed = time.perf_counter() - started
    durations = [result[1] for result in results]
    return {
        "count": len(results),
        "ok": sum(
            1 for status, _, _ in results if status == 200
        ),
        "errors": [error for _, _, error in results if error],
        "mean_ms": (
            statistics.fmean(durations) if durations else 0.0
        ),
        "p95_ms": _percentile(durations, 0.95),
        "p99_ms": _percentile(durations, 0.99),
        "elapsed_s": elapsed,
        "rps": len(results) / elapsed if elapsed else 0.0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_BASE_URL,
        help="Nginx base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--waves",
        type=int,
        default=3,
        help="Number of bounded request waves",
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=40,
        help="Requests per wave, evenly split across demo routes",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help=f"Concurrent clients (1-{MAX_CONCURRENCY})",
    )
    parser.add_argument(
        "--write-requests",
        type=int,
        default=5,
        help="Sequential rollback-only POST requests for duplicate-write detection",
    )
    parser.add_argument(
        "--external-requests",
        type=int,
        default=0,
        help="Optional calls to the local delayed dependency (requires demo route enabled)",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=3.0,
        help="Seconds between waves for upload/flush work",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Per-request timeout in seconds",
    )
    args = parser.parse_args()

    if (
        args.waves < 1
        or args.requests < 1
        or args.write_requests < 0
        or args.external_requests < 0
    ):
        parser.error(
            "--waves and --requests must be positive; optional request counts cannot be negative"
        )
    if not 1 <= args.concurrency <= MAX_CONCURRENCY:
        parser.error(
            f"--concurrency must be between 1 and {MAX_CONCURRENCY}"
        )
    if args.pause < 0 or args.timeout <= 0:
        parser.error(
            "--pause must be non-negative and --timeout must be positive"
        )

    base_url = args.url.rstrip("/")
    if ":8000" in base_url or ":8001" in base_url:
        print(
            "Warning: this URL looks like a direct app/backend port; use the Nginx listener to exercise every layer."
        )

    print("OriginTracer bounded rule-scenario workload")
    print(f"Target: {base_url}")
    print(
        f"Plan: {args.waves} waves x {args.requests} requests; concurrency={args.concurrency}"
    )
    print("GET routes: " + ", ".join(ROUTES))
    print(
        f"Rollback-only write scenario: {args.write_requests} sequential POSTs to {WRITE_SCENARIO}"
    )

    status, elapsed_ms, error = _request(
        base_url, "/", args.timeout
    )
    if status != 200:
        print(
            f"Preflight failed: GET / returned HTTP {status} in {elapsed_ms:.1f} ms"
        )
        if error:
            print(f"  {error}")
        return 1
    print(f"Preflight: GET / returned 200 ({elapsed_ms:.1f} ms)")

    for wave in range(1, args.waves + 1):
        result = _run_wave(
            base_url,
            args.requests,
            args.concurrency,
            args.timeout,
        )
        print(
            f"Wave {wave}/{args.waves}: {result['ok']}/{result['count']} HTTP 200; "
            f"{result['rps']:.1f} req/s; mean={result['mean_ms']:.1f} ms; "
            f"p95={result['p95_ms']:.1f} ms; p99={result['p99_ms']:.1f} ms"
        )
        errors = result["errors"]
        if errors:
            print(
                f"  Failed requests: {len(errors)}; first error: {errors[0]}"
            )
        if wave < args.waves and args.pause:
            time.sleep(args.pause)

    if args.write_requests:
        write_results = [
            _request(
                base_url,
                WRITE_SCENARIO,
                args.timeout,
                method="POST",
            )
            for _ in range(args.write_requests)
        ]
        write_ok = sum(
            1 for status, _, _ in write_results if status == 200
        )
        failed_writes = [
            result
            for result in write_results
            if result[0] != 200
        ]
        print(
            f"Duplicate-write scenario: {write_ok}/{len(write_results)} "
            "rollback-only POST requests returned HTTP 200"
        )
        if failed_writes:
            status, _, error = failed_writes[0]
            print(
                f"  Failed requests: {len(failed_writes)}; first HTTP status: {status}"
            )
            if error:
                print(f"  {error}")
            if status == 404:
                print(
                    "  Enable the demo route in Django with "
                    "ORIGINTRACER_ENABLE_DEMO_SCENARIOS=1, then restart it."
                )

    if args.external_requests:
        try:
            dependency = ThreadingHTTPServer(
                ("127.0.0.1", DEMO_DEPENDENCY_PORT),
                _DelayedDependencyHandler,
            )
        except OSError as exc:
            print(
                f"Could not bind local demo dependency on port "
                f"{DEMO_DEPENDENCY_PORT}: {exc}"
            )
            return 1
        dependency_thread = Thread(
            target=dependency.serve_forever, daemon=True
        )
        dependency_thread.start()
        try:
            results = [
                _request(
                    base_url,
                    EXTERNAL_SCENARIO,
                    args.timeout,
                )
                for _ in range(args.external_requests)
            ]
            succeeded = sum(
                1 for status, _, _ in results if status == 200
            )
            print(
                f"External dependency scenario: {succeeded}/{len(results)} "
                "requests completed through the local delayed HTTP service"
            )
            failures = [
                result for result in results if result[0] != 200
            ]
            if failures:
                print(
                    f"  First HTTP status: {failures[0][0]}; {failures[0][2]}"
                )
        finally:
            dependency.shutdown()
            dependency.server_close()
            dependency_thread.join(timeout=2)

    print(
        "Completed. Allow at least one uploader flush interval, then inspect worker rule matches."
    )
    print(
        "This script generates real HTTP traffic; it does not guarantee every rule threshold is met."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
