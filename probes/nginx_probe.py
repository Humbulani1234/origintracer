"""
probes/nginx_probe.py

Nginx probe via eBPF uprobe or nginx log parsing (two modes).

Mode A (eBPF): attach to nginx worker process symbols — requires bcc + root.
Mode B (log tail): tail /var/log/nginx/access.log and parse structured JSON logs.

Mode B is suitable for most production environments without kernel privileges.
Enable JSON logging in nginx:
    log_format json_combined escape=json '{'
        '"time":"$time_iso8601",'
        '"method":"$request_method",'
        '"uri":"$uri",'
        '"status":$status,'
        '"request_time":$request_time,'
        '"upstream_response_time":"$upstream_response_time"'
    '}';
    access_log /var/log/nginx/access.log json_combined;
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Optional

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent

logger = logging.getLogger("stacktracer.probes.nginx")

_SYNTHETIC_TRACE_PREFIX = "nginx-"   # nginx events don't carry trace IDs by default


class NginxProbe(BaseProbe):
    name = "nginx"

    def __init__(
        self,
        log_path: str = "/var/log/nginx/access.log",
        mode: str = "log",       # "log" or "ebpf"
    ) -> None:
        self._log_path = log_path
        self._mode = mode
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        if self._mode == "ebpf":
            logger.warning("nginx eBPF mode not yet implemented — falling back to log mode")

        if not os.path.exists(self._log_path):
            logger.warning("nginx log not found at %s — nginx probe inactive", self._log_path)
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._tail_loop,
            daemon=True,
            name="stacktracer-nginx-probe",
        )
        self._thread.start()
        logger.info("nginx probe tailing %s", self._log_path)

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def _tail_loop(self) -> None:
        try:
            with open(self._log_path, "r") as f:
                f.seek(0, 2)  # Seek to end
                while self._running:
                    line = f.readline()
                    if line:
                        self._handle_line(line.strip())
                    else:
                        time.sleep(0.05)
        except Exception as exc:
            logger.error("nginx probe tail error: %s", exc)

    def _handle_line(self, line: str) -> None:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            # Not JSON — try common log format (partial)
            record = self._parse_common_log(line)
            if not record:
                return

        uri = record.get("uri", record.get("request", "unknown"))
        status = int(record.get("status", 0))
        request_time_s = float(record.get("request_time", 0))
        upstream_time_s = record.get("upstream_response_time", "")

        # Nginx doesn't carry trace IDs — generate a synthetic one per request
        trace_id = f"{_SYNTHETIC_TRACE_PREFIX}{time.time_ns()}"

        emit(NormalizedEvent.now(
            probe="request.entry",
            trace_id=trace_id,
            service="nginx",
            name=uri,
            status_code=status,
            duration_ns=int(request_time_s * 1e9),
        ))

        if upstream_time_s and upstream_time_s != "-":
            try:
                emit(NormalizedEvent.now(
                    probe="request.exit",
                    trace_id=trace_id,
                    service="nginx",
                    name=uri,
                    upstream_duration_ns=int(float(upstream_time_s) * 1e9),
                ))
            except ValueError:
                pass

    @staticmethod
    def _parse_common_log(line: str) -> Optional[dict]:
        """Minimal parser for combined log format."""
        try:
            parts = line.split('"')
            if len(parts) < 5:
                return None
            request = parts[1]   # e.g. "GET /path HTTP/1.1"
            status_part = parts[2].strip().split()
            method, uri = request.split()[:2] if " " in request else ("", request)
            return {
                "uri": uri,
                "method": method,
                "status": status_part[0] if status_part else "0",
            }
        except Exception:
            return None


"""
probes/flask_probe.py

Flask probe — uses Flask's application hooks (no monkey-patching needed).
Add to your Flask app:

    from stacktracer.probes.flask_probe import init_flask
    init_flask(app)

Or use the FlaskProbe class with the probe SDK.
"""

import uuid as _uuid
import random as _random

from ..sdk.base_probe import BaseProbe
from ..sdk.emitter import emit
from ..core.event_schema import NormalizedEvent
from ..context.vars import set_trace, reset_trace


def init_flask(app: object, sample_rate: float = 0.05) -> None:
    """
    Instrument a Flask application.
    Call after creating your Flask app but before running it.
    """
    try:
        import flask
    except ImportError:
        logger.warning("Flask not installed — flask probe inactive")
        return

    # Store token per request in flask.g
    @app.before_request
    def _before():
        if _random.random() > sample_rate:
            flask.g._trace_token = None
            return

        trace_id = str(_uuid.uuid4())
        flask.g._trace_token = set_trace(trace_id)

        emit(NormalizedEvent.now(
            probe="request.entry",
            trace_id=trace_id,
            service="flask",
            name=flask.request.path,
            method=flask.request.method,
            path=flask.request.path,
        ))

    @app.after_request
    def _after(response):
        token = getattr(flask.g, "_trace_token", None)
        if token is None:
            return response

        trace_id = get_trace_id() or "unknown"
        emit(NormalizedEvent.now(
            probe="request.exit",
            trace_id=trace_id,
            service="flask",
            name=flask.request.path,
            status_code=response.status_code,
        ))
        reset_trace(token)
        return response


class FlaskProbe(BaseProbe):
    name = "flask"

    def __init__(self, sample_rate: float = 0.05) -> None:
        self._sample_rate = sample_rate

    def start(self) -> None:
        logger.info(
            "FlaskProbe requires explicit init_flask(app) call — "
            "add 'from stacktracer.probes.flask_probe import init_flask; init_flask(app)' "
            "to your application setup."
        )

    def stop(self) -> None:
        pass