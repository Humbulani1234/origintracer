from __future__ import annotations

import logging
import threading
import time
from collections import deque
from threading import Lock
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger("origintracer.uploader")


class _UploaderEventBuffer:
    """
    A bounded FIFO buffer of event dicts.
    Bounded at maxlen - oldest events are silently dropped when full.
    This is intentional: dropping old events under sustained overload
    is safer than growing memory without bound.
    """

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


# Serialisation helpers


def _serialize_events(payload: Dict) -> tuple:
    """Serialise event batch to msgpack (preferred) or JSON (fallback)."""
    try:
        import msgpack

        return (
            msgpack.packb(payload, use_bin_type=True),
            "application/msgpack",
        )
    except ImportError:
        import json

        return json.dumps(payload).encode(), "application/json"


def _serialize_graph(graph: Any) -> tuple:
    """Serialise RuntimeGraph — prefers protobuf, falls back to msgpack."""
    try:
        from origintracer.core.graph_serializer import (
            MsgpackSerializer,
        )

        return (
            MsgpackSerializer().serialize(graph),
            "application/msgpack",
        )

    except ImportError:
        from origintracer.core.graph_serializer import (
            ProtobufSerializer,
        )

        return (
            ProtobufSerializer().serialize(graph),
            "application/x-protobuf",
        )


class Uploader:
    """
    Ships probe events and graph snapshots to the StackTracer backend.

    Implements the BaseRepository insert_event() interface so Engine.process()
    can call it as a repository without knowing about HTTP transport.

    Usage in stacktracer/__init__.py:
        uploader = Uploader(endpoint=..., api_key=...)
        uploader.start()
        uploader.bind_engine(engine)      # must be called after start()
        engine.repository = uploader      # Engine calls insert_event() per event


    """

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        flush_interval: int = 10,  # seconds between event batch flushes
        snapshot_interval: int = 60,  # seconds between graph snapshot flushes
        max_batch_size: int = 500,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._api_key = api_key
        self._flush_interval = flush_interval
        self._snapshot_interval = snapshot_interval
        self._max_batch = max_batch_size

        self._event_buffer = _UploaderEventBuffer()

        self._engine: Optional[Any] = (
            None  # set by bind_engine()
        )

        self._thread: Optional[threading.Thread] = None
        self._running: bool = False

        # Timing
        self._last_event_flush_s: float = 0.0
        self._last_snapshot_flush_s: float = 0.0

        # Stats
        self._events_sent_total: int = 0
        self._snapshots_sent: int = 0
        self._diffs_sent: int = 0
        self._failed_event_sends: int = 0
        self._failed_snap_sends: int = 0
        self._failed_diffs_sends: int = 0

    def insert_event(self, event: Any) -> None:
        """
        Repository interface. Called by Engine.process() for every event.
        Converts NormalizedEvent to dict and queues for batch upload.
        Never raises — must not disrupt the drain thread.
        """
        try:
            self._event_buffer.push(event.to_dict())
        except Exception:
            pass

    def bind_engine(self, engine: Any) -> None:
        """
        Wire the engine so the uploader can serialise its graph for snapshots.
        Call this after start() from stacktracer/__init__.py _init_uploader().
        """
        self._engine = engine

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
        logger.info(
            "Uploader started → %s  events=%ds  snapshots=%ds",
            self._endpoint,
            self._flush_interval,
            self._snapshot_interval,
        )

    def stop(self) -> None:
        """
        Stop the background thread and perform one final flush of both
        events and snapshot before returning. Called by stacktracer.shutdown().
        """
        self._running = False
        # Final flushes on the calling thread (not the daemon thread)
        self._flush_events()
        self._flush_snapshot()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info(
            "Uploader stopped  events_sent=%d  snapshots_sent=%d",
            self._events_sent_total,
            self._snapshots_sent,
        )

    # ---------------------- Background loop ------------------

    def _run(self) -> None:
        """
        Main loop. Sleeps in short increments so stop() is responsive.
        Checks whether each flush interval has elapsed on every wake.
        """
        while self._running:
            time.sleep(1)  # wake every second to check intervals

            now = time.time()

            if (
                now - self._last_event_flush_s
                >= self._flush_interval
            ):
                self._flush_events()
                self._last_event_flush_s = now

            if (
                now - self._last_snapshot_flush_s
                >= self._snapshot_interval
            ):
                self._flush_snapshot()
                self._last_snapshot_flush_s = now

    def _flush_events(self) -> None:
        """
        Drain the event buffer and POST to /api/v1/events.
        Silently discards on any error — probe events are best-effort.
        """
        batch = self._event_buffer.drain(self._max_batch)
        if not batch:
            return
        payload = {"events": batch, "count": len(batch)}

        try:
            body, content_type = _serialize_events(payload)
            response = httpx.post(
                f"{self._endpoint}/api/v1/events",
                content=body,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": content_type,
                },
                timeout=10.0,
            )
            if response.status_code == 200:
                self._events_sent_total += len(batch)
                logger.debug(
                    "Events uploaded: %d  total=%d",
                    len(batch),
                    self._events_sent_total,
                )
            else:
                self._failed_event_sends += 1
                logger.warning(
                    "Event upload failed: HTTP %d — %s",
                    response.status_code,
                    response.text[:200],
                )

        except ImportError:
            logger.debug(
                "httpx not installed — uploader inactive  "
                "(pip install httpx)"
            )
        except httpx.TimeoutException as e:
            logger.debug(
                "Timeout — backend not responding: %s", e
            )
        except httpx.ConnectError as e:
            logger.debug("Connection error: %s", e)
        except Exception as exc:
            self._failed_event_sends += 1
            logger.debug("Event upload error: %s", exc)

    def _flush_snapshot(self) -> None:
        """
        Serialize the current RuntimeGraph and upload it to the backend.

        This method runs on the uploader thread and is responsible for:
        1. Serializing the in-memory graph into a compact binary format
        2. Sending a full snapshot to the backend
        3. Optionally sending the latest incremental diff

        Failure handling:
        - Serialization failures abort the entire operation.
        - Snapshot and diff uploads are tracked independently.
        - All errors are logged at debug/warning level; no exceptions escape.

        Backend contract:
        - POST /api/v1/graph/snapshot → full graph replacement
        - POST /api/v1/graph/diff     → incremental update
        """
        if self._engine is None:
            logger.debug(
                "Snapshot skipped: engine not initialized"
            )
            return

        # --- Serialize graph ---
        try:
            data, content_type = _serialize_graph(
                self._engine.graph
            )
        except Exception as exc:
            logger.debug("Graph serialization failed: %s", exc)
            return

        import httpx

        # --- Send snapshot ---
        try:
            snapshot_resp = httpx.post(
                f"{self._endpoint}/api/v1/graph/snapshot",
                content=data,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": content_type,
                },
                timeout=30.0,
            )

            if snapshot_resp.status_code == 200:
                self._snapshots_sent += 1
                info = snapshot_resp.json()
                logger.debug(
                    "Snapshot uploaded: %d bytes nodes=%s edges=%s",
                    len(data),
                    info.get("nodes", "?"),
                    info.get("edges", "?"),
                )
            else:
                self._failed_snap_sends += 1
                logger.warning(
                    "Snapshot upload failed: HTTP %d — %s",
                    snapshot_resp.status_code,
                    snapshot_resp.text[:200],
                )
                return  # don’t send diff if snapshot failed

        except httpx.TimeoutException as exc:
            self._failed_snap_sends += 1
            logger.debug("Snapshot timeout: %s", exc)
            return
        except httpx.ConnectError as exc:
            self._failed_snap_sends += 1
            logger.debug("Snapshot connection error: %s", exc)
            return
        except Exception as exc:
            self._failed_snap_sends += 1
            logger.debug("Snapshot upload error: %s", exc)
            return

        # --- Send latest diff (optional) ---
        latest_diff = self._engine.temporal.latest_diff()
        if not latest_diff:
            return

        try:
            diff_resp = httpx.post(
                f"{self._endpoint}/api/v1/graph/diff",
                json=latest_diff,
                headers={
                    "Authorization": f"Bearer {self._api_key}"
                },
                timeout=5.0,
            )

            if diff_resp.status_code == 200:
                self._diffs_sent += 1
                info = diff_resp.json()
                logger.debug(
                    "Diff uploaded: nodes=%s edges=%s",
                    info.get("nodes", "?"),
                    info.get("edges", "?"),
                )
            else:
                self._failed_diffs_sends += 1
                logger.warning(
                    "Diff upload failed: HTTP %d — %s",
                    diff_resp.status_code,
                    diff_resp.text[:200],
                )

        except httpx.TimeoutException as exc:
            self._failed_diffs_sends += 1
            logger.debug("Diff timeout: %s", exc)
        except httpx.ConnectError as exc:
            self._failed_diffs_sends += 1
            logger.debug("Diff connection error: %s", exc)
        except Exception as exc:
            self._failed_diffs_sends += 1
            logger.debug("Diff upload error: %s", exc)

    def send_deployment_marker(self, label: str) -> None:
        try:
            httpx.post(
                f"{self._endpoint}/api/v1/deployment",
                json={"label": label},
                headers={
                    "Authorization": f"Bearer {self._api_key}"
                },
                timeout=5.0,
            )
        except httpx.TimeoutException as e:
            logger.debug(
                "Timeout - backend not responding: %s", e
            )
        except httpx.ConnectError as e:
            logger.debug("Connection error: %s", e)
        except Exception as exc:
            logger.warning(
                "Failed to send deployment marker: %s", exc
            )

    def stats(self) -> Dict[str, Any]:
        return {
            "events_sent_total": self._events_sent_total,
            "snapshots_sent": self._snapshots_sent,
            "failed_event_sends": self._failed_event_sends,
            "failed_snap_sends": self._failed_snap_sends,
            "event_buffer_size": len(self._event_buffer),
            "engine_bound": self._engine is not None,
            "running": self._running,
        }
