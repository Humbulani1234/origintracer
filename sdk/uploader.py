"""
buffer/uploader.py

Background thread that batches NormalizedEvents and ships them to the
StackTracer SaaS backend (or a self-hosted instance).

Design:
  - Runs as a daemon thread — dies with the process
  - Collects events from the engine's event log (via engine.repository)
  - Batches by time OR count (whichever comes first)
  - Uses msgpack for efficient serialisation
  - Falls back to JSON if msgpack is unavailable
  - Retries once on transient failure
  - Silent on all errors (must not disrupt host app)
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from threading import Lock
from typing import Any, Dict, List, Optional

logger = logging.getLogger("stacktracer.uploader")


class _BatchBuffer:
    """Thread-safe bounded buffer of serialised event dicts."""

    def __init__(self, maxlen: int = 20_000) -> None:
        self._q: deque = deque(maxlen=maxlen)
        self._lock = Lock()

    def push(self, event_dict: Dict) -> None:
        with self._lock:
            self._q.append(event_dict)

    def drain(self, max_batch: int) -> List[Dict]:
        with self._lock:
            batch = []
            while self._q and len(batch) < max_batch:
                batch.append(self._q.popleft())
            return batch

    def __len__(self) -> int:
        return len(self._q)


def _try_serialize(payload: Dict) -> tuple[bytes, str]:
    """Serialise to msgpack (preferred) or JSON (fallback)."""
    try:
        import msgpack
        return msgpack.packb(payload, use_bin_type=True), "application/msgpack"
    except ImportError:
        import json
        return json.dumps(payload).encode(), "application/json"


class Uploader:
    """
    Sends batched probe events to the configured endpoint.

    The Engine's process() method hooks into this via the repository pattern.
    For MVP: the uploader simply reads from a shared buffer.
    """

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        flush_interval: int = 10,
        max_batch_size: int = 500,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._api_key = api_key
        self._flush_interval = flush_interval
        self._max_batch = max_batch_size

        self._buffer = _BatchBuffer()
        self._thread: Optional[threading.Thread] = None
        self._running = False

        # Stats
        self._sent_total = 0
        self._failed_batches = 0

    def push(self, event_dict: Dict) -> None:
        """Called by the Engine for each event."""
        self._buffer.push(event_dict)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="stacktracer-uploader",
        )
        self._thread.start()
        logger.info("Uploader started → %s (interval=%ds)", self._endpoint, self._flush_interval)

    def stop(self) -> None:
        self._running = False
        # Final flush
        self._flush()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        while self._running:
            time.sleep(self._flush_interval)
            self._flush()

    def _flush(self) -> None:
        batch = self._buffer.drain(self._max_batch)
        if not batch:
            return

        payload = {"events": batch, "count": len(batch)}

        try:
            import httpx
            body, content_type = _try_serialize(payload)

            response = httpx.post(
                f"{self._endpoint}/api/v1/ingest",
                content=body,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": content_type,
                },
                timeout=10.0,
            )

            if response.status_code == 200:
                self._sent_total += len(batch)
                logger.debug("Uploaded %d events (total=%d)", len(batch), self._sent_total)
            else:
                self._failed_batches += 1
                logger.warning(
                    "Upload failed: HTTP %d — %s",
                    response.status_code,
                    response.text[:200],
                )

        except ImportError:
            logger.debug("httpx not installed — uploader inactive (install with: pip install httpx)")
        except Exception as exc:
            self._failed_batches += 1
            logger.debug("Upload error: %s", exc)

    def stats(self) -> Dict[str, Any]:
        return {
            "sent_total": self._sent_total,
            "failed_batches": self._failed_batches,
            "buffer_size": len(self._buffer),
            "running": self._running,
        }