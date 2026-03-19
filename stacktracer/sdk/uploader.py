"""
buffer/uploader.py

Background thread that ships probe events and graph snapshots to the
StackTracer backend (SaaS or self-hosted).

Two independent flush loops run on a single daemon thread:

    _flush_events()    — every flush_interval seconds (default 10s)
                         drains EventBuffer → POST /api/v1/events
                         carries raw NormalizedEvent dicts for persistence

    _flush_snapshot()  — every snapshot_interval seconds (default 60s)
                         serialises engine.graph → POST /api/v1/graph/snapshot
                         carries the full RuntimeGraph as msgpack bytes
                         FastAPI deserialises this and serves all graph queries


"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from threading import Lock
from typing import Any, Dict, List, Optional

logger = logging.getLogger("stacktracer.uploader")


# ====================================================================== #
# Internal buffers
# ====================================================================== #


class _UploaderEventBuffer:
    """
    Thread-safe bounded FIFO buffer of event dicts.
    Bounded at maxlen — oldest events are silently dropped when full.
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


class _SnapshotSlot:
    """
    Single-slot buffer for the latest graph snapshot bytes.
    Only the most recent snapshot matters — superseded ones are discarded.
    Thread-safe. Used to decouple snapshot serialisation (done in the
    drain thread when engine.graph is read) from the HTTP send.
    """

    def __init__(self) -> None:
        self._data: Optional[bytes] = None
        self._content_type: str = "application/msgpack"
        self._pending: bool = False
        self._lock = Lock()

    def put(
        self,
        data: bytes,
        content_type: str = "application/msgpack",
    ) -> None:
        with self._lock:
            self._data = data
            self._content_type = content_type
            self._pending = True

    def take(self) -> Optional[tuple]:
        """Return (data, content_type) and clear pending flag. None if nothing waiting."""
        with self._lock:
            if not self._pending or self._data is None:
                return None
            result = (self._data, self._content_type)
            self._pending = False
            return result

    @property
    def pending(self) -> bool:
        with self._lock:
            return self._pending


# ====================================================================== #
# Serialisation helpers
# ====================================================================== #


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
    """Serialise RuntimeGraph to msgpack bytes using GraphSerializer."""
    from stacktracer.core.graph_serializer import (
        MsgpackSerializer,
    )

    data = MsgpackSerializer().serialize(graph)
    return data, "application/msgpack"


# ====================================================================== #
# Uploader
# ====================================================================== #


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

    Thread model:
        One daemon thread runs _run() which calls both flush methods
        on independent time schedules. The application thread only ever
        calls insert_event() → _event_buffer.push() which is ~0.5µs.
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
        self._snapshot_slot = _SnapshotSlot()

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
        self._failed_event_sends: int = 0
        self._failed_snap_sends: int = 0

    # ------------------------------------------------------------------ #
    # BaseRepository interface — Engine calls this per event
    # ------------------------------------------------------------------ #

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

    # ------------------------------------------------------------------ #
    # Engine binding — must be called after start()
    # ------------------------------------------------------------------ #

    def bind_engine(self, engine: Any) -> None:
        """
        Wire the engine so the uploader can serialise its graph for snapshots.
        Call this after start() from stacktracer/__init__.py _init_uploader().
        """
        self._engine = engine

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

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
        print(">>>>>uploader")
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

    # ------------------------------------------------------------------ #
    # Background loop
    # ------------------------------------------------------------------ #

    def _run(self) -> None:
        """
        Main loop. Sleeps in short increments so stop() is responsive.
        Checks whether each flush interval has elapsed on every wake.
        """
        while self._running:
            print(">>>WE DO REACH HERE")
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

    # ------------------------------------------------------------------ #
    # Event flush
    # ------------------------------------------------------------------ #

    def _flush_events(self) -> None:
        """
        Drain the event buffer and POST to /api/v1/events.
        Silently discards on any error — probe events are best-effort.
        """
        batch = self._event_buffer.drain(self._max_batch)
        if not batch:
            return
        print(">>>>I RUN TOO")
        payload = {"events": batch, "count": len(batch)}

        try:
            import httpx

            body, content_type = _serialize_events(payload)
            print(">>>>I RUN TOO alsooo", body)
            print(f"MY END POINT{self._endpoint}/api/v1/events")
            response = httpx.post(
                f"{self._endpoint}/api/v1/events",
                content=body,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": content_type,
                },
                timeout=10.0,
            )
            print(">>>>MY REPONSE", response)
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
        except httpx.TimeoutException:
            print(">>>>TIMEOUT — backend not responding")
        except httpx.ConnectError as e:
            print(">>>>CONNECT ERROR", e)
        except Exception as e:
            print(">>>>ERROR", type(e).__name__, e)
        except Exception as exc:
            self._failed_event_sends += 1
            logger.debug("Event upload error: %s", exc)

    # ------------------------------------------------------------------ #
    # Snapshot flush
    # ------------------------------------------------------------------ #

    def _flush_snapshot(self) -> None:
        """
        Serialise the current RuntimeGraph and POST to /api/v1/graph/snapshot.

        Serialisation happens here on the uploader thread — not on the
        application thread and not on the drain thread. The graph's RLock
        is held only for the duration of the serialisation read, which is
        proportional to graph size (~5ms for 5000 nodes).

        FastAPI deserialises the bytes, stores them in memory and in the
        database, and serves all graph queries from the deserialised graph.
        """
        if self._engine is None:
            print(">>>GRAPH SERIALISATION and we return home")
            return

        try:
            data, content_type = _serialize_graph(
                self._engine.graph
            )
            print(">>>GRAPH SERIALISATION", data)
        except Exception as exc:
            print(">>>GRAPH SERIALISATION")
            logger.debug("Graph serialisation failed: %s", exc)
            return

        try:
            import httpx

            response = httpx.post(
                f"{self._endpoint}/api/v1/graph/snapshot",
                content=data,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": content_type,
                },
                timeout=30.0,  # snapshot can be ~1MB — allow more time
            )
            print(">>>GRAPH RESPONSE", response)
            if response.status_code == 200:
                self._snapshots_sent += 1
                info = response.json()
                logger.debug(
                    "Snapshot uploaded: %d bytes  nodes=%s  edges=%s",
                    len(data),
                    info.get("nodes", "?"),
                    info.get("edges", "?"),
                )
            else:
                self._failed_snap_sends += 1
                logger.warning(
                    "Snapshot upload failed: HTTP %d — %s",
                    response.status_code,
                    response.text[:200],
                )

        except ImportError:
            pass  # already warned by _flush_events
        except httpx.TimeoutException:
            print(">>>>TIMEOUT — backend not responding")
        except httpx.ConnectError as e:
            print(">>>>CONNECT ERROR", e)
        except Exception as e:
            print(">>>>ERROR", type(e).__name__, e)
        except Exception as exc:
            self._failed_event_sends += 1
            logger.debug("Event upload error: %s", exc)
        except Exception as exc:
            self._failed_snap_sends += 1
            logger.debug("Snapshot upload error: %s", exc)

    # ------------------------------------------------------------------ #
    # Stats
    # ------------------------------------------------------------------ #

    def stats(self) -> Dict[str, Any]:
        return {
            "events_sent_total": self._events_sent_total,
            "snapshots_sent": self._snapshots_sent,
            "failed_event_sends": self._failed_event_sends,
            "failed_snap_sends": self._failed_snap_sends,
            "event_buffer_size": len(self._event_buffer),
            "snapshot_pending": self._snapshot_slot.pending,
            "engine_bound": self._engine is not None,
            "running": self._running,
        }
