from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from threading import Lock
from typing import Any, Dict, List, Optional

import httpx

from origintracer.core.event_schema import NormalizedEvent
from origintracer.core.worker_identity import (
    current_worker_identity,
)

logger = logging.getLogger("origintracer.uploader")


class _UploaderEventBuffer:
    """
    A bounded FIFO buffer of event dicts.
    Bounded at maxlen - oldest events are silently dropped when full.
    """

    def __init__(self, maxlen: int = 20_000) -> None:
        self._q: deque = deque(maxlen=maxlen)
        self._lock = Lock()

    def push(self, event_dict: Dict) -> None:
        with self._lock:
            self._q.append(event_dict)

    def drain(self, max_batch: int) -> List[Dict]:
        with self._lock:
            batch: list = []
            while self._q and len(batch) < max_batch:
                batch.append(self._q.popleft())
            return batch

    def __len__(self) -> int:
        return len(self._q)


def _serialize_events(payload: Dict) -> tuple:
    """
    Serialise event batch to msgpack (preferred) or JSON (fallback).
    """
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
    """
    Serialise RuntimeGraph - prefers protobuf, falls back to msgpack.
    """
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
    """Upload probe events and worker state to an OriginTracer backend.

    Implements the ``BaseRepository.insert_event()`` interface so
    ``Engine.process()`` can use it without knowing about HTTP transport.

    Parameters
    ----------
    endpoint : str
        Base URL of the OriginTracer backend.
    api_key : str
        Bearer token used to authenticate uploads.
    flush_interval : int, optional
        Seconds between event batch uploads.
    snapshot_interval : float, optional
        Seconds between graph snapshot uploads. Runtime heartbeats use a
        shorter independent interval.
    max_batch_size : int, optional
        Maximum number of events sent in one batch.

    Examples
    --------
    >>> uploader = Uploader(endpoint="http://localhost:8001", api_key="token")
    >>> uploader.start()
    >>> uploader.bind_engine(engine)
    >>> engine.repository = uploader

    Notes
    -----
    Call ``stop()`` during shutdown to flush pending events and a final graph
    snapshot, then join the background threads.
    """

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        flush_interval: int = 10,  # seconds between event batch flushes
        snapshot_interval: float = 60,  # seconds between graph snapshot flushes
        max_batch_size: int = 500,
    ) -> None:
        """Configure backend uploads and background flush intervals.

        Parameters
        ----------
        endpoint : str
            Base URL of the OriginTracer backend.
        api_key : str
            Bearer token used to authenticate requests.
        flush_interval : int, optional
            Seconds between event batch flushes.
        snapshot_interval : float, optional
            Seconds between graph snapshot flushes and the upper bound used to
            derive the runtime heartbeat cadence.
        max_batch_size : int, optional
            Maximum number of events included in one upload batch.
        """
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
        self._runtime_status_thread: Optional[
            threading.Thread
        ] = None
        self._running: bool = False
        # Runtime liveness must not depend on graph serialization or snapshot
        # uploads succeeding. Keep its cadence short even when snapshots are
        # configured less frequently.
        self._runtime_status_interval = max(
            1.0, min(15.0, float(snapshot_interval))
        )

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
        """Queue a probe event for batch upload.

        Called by ``Engine.process()`` for each event. The event is converted
        to a mapping and added to the bounded upload queue.

        Parameters
        ----------
        event : NormalizedEvent
            Probe event to serialize and upload.

        Returns
        -------
        None

        Notes
        -----
        Serialization errors are suppressed so an upload problem does not
        interrupt event processing.
        """
        try:
            self._event_buffer.push(event.to_dict())
        except Exception:
            pass

    def bind_engine(self, engine: Any) -> None:
        """Attach the engine whose graph and runtime state will be uploaded.

        Parameters
        ----------
        engine : Engine
            Engine instance supplying graph snapshots and runtime status.

        Returns
        -------
        None
        """
        self._engine = engine

    @staticmethod
    def _identity_headers() -> Dict[str, str]:
        worker_instance_id, worker_pid = (
            current_worker_identity()
        )
        return {
            "X-OriginTracer-Worker-Instance-ID": worker_instance_id,
            "X-OriginTracer-Worker-PID": str(worker_pid),
        }

    def start(self) -> None:
        """Start background event, snapshot, and heartbeat uploads.

        Returns
        -------
        None

        Notes
        -----
        The event and snapshot thread periodically sends event batches to
        ``POST /api/v1/events`` and graph snapshots to
        ``POST /api/v1/graph/snapshot``. It also sends incremental graph diffs
        when available. Event uploads are best effort; failed batches are
        discarded.

        A second thread posts runtime heartbeats independently of graph
        snapshots, so a worker can report liveness if its graph cannot be
        serialized. Upload errors are logged and do not escape the background
        threads. Call ``stop()`` during shutdown.
        """
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="origintracer-uploader",
        )
        self._thread.start()
        self._runtime_status_thread = threading.Thread(
            target=self._run_runtime_status,
            daemon=True,
            name="origintracer-runtime-heartbeat",
        )
        self._runtime_status_thread.start()
        logger.info(
            "Uploader started: %s  events=%ds  snapshots=%ds",
            self._endpoint,
            self._flush_interval,
            self._snapshot_interval,
        )

    def stop(self) -> None:
        """Stop background uploads and make a final event and graph flush.

        Returns
        -------
        None

        Notes
        -----
        This method joins the upload and heartbeat threads, waiting up to five
        seconds for each. It is called by ``origintracer.shutdown()``.
        """
        self._running = False
        # Final flushes on the calling thread
        self._flush_events()
        self._flush_snapshot()
        if self._thread:
            self._thread.join(timeout=5)
        if self._runtime_status_thread:
            self._runtime_status_thread.join(timeout=5)
        logger.info(
            "Uploader stopped  events_sent=%d  snapshots_sent=%d",
            self._events_sent_total,
            self._snapshots_sent,
        )

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

    def _run_runtime_status(self) -> None:
        """Refresh the worker heartbeat independently of graph snapshots.

        The backend requires one stored snapshot before it accepts runtime
        status. Once the first snapshot succeeds, this loop keeps the worker
        presence fresh even if a later graph serialization or snapshot upload
        stalls. Heartbeats are deliberately more frequent than snapshots.
        """
        last_status = 0.0
        while self._running:
            now = time.monotonic()
            if (
                self._engine is not None
                and self._snapshots_sent > 0
                and now - last_status
                >= self._runtime_status_interval
            ):
                self._flush_runtime_status()
                last_status = now
            time.sleep(1)

    def _flush_events(self) -> None:
        """
        Drain the event buffer and POST to /api/v1/events.
        Silently discards on any error - probe events are best-effort.
        """
        batch = self._event_buffer.drain(self._max_batch)
        if not batch:
            return
        payload = {
            "events": batch,
            "count": len(batch),
            "worker_pid": str(os.getpid()),
        }

        try:
            body, content_type = _serialize_events(payload)
            response = httpx.post(
                f"{self._endpoint}/api/v1/events",
                content=body,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": content_type,
                    **self._identity_headers(),
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
                "httpx not installed - uploader inactive  "
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
        - POST /api/v1/graph/snapshot --> full graph replacement
        - POST /api/v1/graph/diff --> incremental update
        """
        if self._engine is None:
            logger.debug(
                "Snapshot skipped: engine not initialized"
            )
            return

        # Serialize graph
        try:
            self._attach_semantic_membership()
            data, content_type = _serialize_graph(
                self._engine.graph
            )
        except Exception as exc:
            logger.debug("Graph serialization failed: %s", exc)
            return

        # Send snapshot
        try:
            snapshot_resp = httpx.post(
                f"{self._endpoint}/api/v1/graph/snapshot",
                content=data,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": content_type,
                    **self._identity_headers(),
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
                self._flush_causal_matches()
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

        # Send latest diff
        latest_diff = self._engine.temporal.latest_diff()
        if not latest_diff:
            return

        try:
            diff_resp = httpx.post(
                f"{self._endpoint}/api/v1/graph/diff",
                json=latest_diff,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    **self._identity_headers(),
                },
                timeout=5.0,
            )

            if diff_resp.status_code == 200:
                self._diffs_sent += 1
                diff_resp.json()
                logger.debug(
                    "Diff uploaded: added_nodes=%d removed_nodes=%d "
                    "added_edges=%d removed_edges=%d",
                    len(latest_diff.get("added_nodes") or []),
                    len(latest_diff.get("removed_nodes") or []),
                    len(latest_diff.get("added_edges") or []),
                    len(latest_diff.get("removed_edges") or []),
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

    def _flush_causal_matches(self) -> None:
        """Upload matches from this process's configured rule registry."""
        if self._engine is None:
            return
        try:
            worker_instance_id, worker_pid = (
                current_worker_identity()
            )
            matches = [
                match.to_dict()
                for match in self._engine.evaluate()
            ]
            response = httpx.post(
                f"{self._endpoint}/api/v1/causal/report",
                json={
                    "worker_instance_id": worker_instance_id,
                    "worker_pid": str(worker_pid),
                    "matches": matches,
                },
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    **self._identity_headers(),
                },
                timeout=10.0,
            )
            if response.status_code != 200:
                logger.warning(
                    "Causal match upload failed: HTTP %d — %s",
                    response.status_code,
                    response.text[:200],
                )
        except httpx.TimeoutException as exc:
            logger.debug("Causal match upload timeout: %s", exc)
        except httpx.ConnectError as exc:
            logger.debug(
                "Causal match upload connection error: %s", exc
            )
        except Exception as exc:
            logger.debug("Causal match upload error: %s", exc)

    def _flush_runtime_status(self) -> None:
        """Upload runtime-only status so remote readers can judge freshness.

        The heartbeat is sent independently of graph snapshots and is keyed
        by the uploader's worker-instance identity. It reports uptime,
        active requests, probes, configured rules, graph counts, semantic
        label count, and event-log size because those values are not reliably
        recoverable from a serialized graph. A failed heartbeat is logged at
        debug level; it does not fail the graph upload loop.
        """
        if self._engine is None:
            return
        engine = self._engine
        graph = engine.graph
        tracker = getattr(engine, "tracker", None)
        probes = getattr(engine, "probes", None) or []
        with graph._lock:
            graph_nodes = len(graph._nodes)
            graph_edges = len(graph._edge_index)
        event_log = getattr(engine, "_event_log", [])
        event_log_lock = getattr(engine, "_event_log_lock", None)
        if event_log_lock is None:
            event_log_size = len(event_log)
        else:
            with event_log_lock:
                event_log_size = len(event_log)
        semantic = getattr(engine, "semantic", None)
        rule_registry = getattr(engine, "causal", None)
        status = {
            "uptime_s": round(
                time.monotonic() - engine._started_at, 1
            ),
            "active_requests": (
                tracker.active_count() if tracker else 0
            ),
            "probes_active": [
                str(getattr(probe, "name", probe))
                for probe in probes
            ],
            "probes_total": len(probes),
            "rules_active": list(
                getattr(rule_registry, "_rules", {}).keys()
            ),
            "graph_nodes": graph_nodes,
            "graph_edges": graph_edges,
            "semantic_labels": (
                len(semantic.all_labels()) if semantic else 0
            ),
            "event_log_size": event_log_size,
            "snapshot_interval_s": float(
                self._snapshot_interval
            ),
        }
        try:
            response = httpx.post(
                f"{self._endpoint}/api/v1/worker/runtime",
                json={
                    "status": status,
                    "observed_at": time.time(),
                },
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    **self._identity_headers(),
                },
                timeout=5.0,
            )
            if response.status_code != 200:
                logger.debug(
                    "Runtime status upload failed: HTTP %d — %s",
                    response.status_code,
                    response.text[:200],
                )
        except Exception as exc:
            logger.debug("Runtime status upload error: %s", exc)

    def _attach_semantic_membership(self) -> None:
        """Embed each configured semantic alias on matching graph nodes.

        The backend serves stored snapshots without access to the originating
        process configuration. Persisting these labels with node metadata lets
        remote query clients apply the same semantic filters as the REPL.
        """
        engine = self._engine
        if engine is None:
            return
        graph = engine.graph
        semantic = getattr(engine, "semantic", None)
        if semantic is None:
            return
        with graph._lock:
            for node in graph._nodes.values():
                labels = [
                    alias.label
                    for alias in semantic
                    if alias.matches_node(node.id)
                    or alias.matches_service(node.service)
                ]
                node.metadata["semantic_labels"] = labels

    def send_deployment_marker(self, label: str) -> None:
        """Send a deployment marker to the configured backend.

        Parameters
        ----------
        label : str
            Name identifying the deployment event.

        Returns
        -------
        None

        Notes
        -----
        The request uses the current worker identity and logs upload failures
        without raising them to the application.
        """
        try:
            response = httpx.post(
                f"{self._endpoint}/api/v1/deployment",
                json={
                    "label": label,
                    "customer_id": "local_dev",
                    "worker_pid": str(os.getpid()),
                },
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    **self._identity_headers(),
                },
                timeout=5.0,
            )
            if response.status_code != 200:
                logger.warning(
                    "Deployment marker upload failed: HTTP %d — %s",
                    response.status_code,
                    response.text[:200],
                )
            else:
                logger.info(
                    "Deployment marker uploaded: label=%s",
                    label,
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
        """Return counters describing uploader activity.

        Returns
        -------
        Dict[str, Any]
            Mapping containing event and snapshot totals, failed upload
            counts, queue size, engine binding state, and running state.
        """
        return {
            "events_sent_total": self._events_sent_total,
            "snapshots_sent": self._snapshots_sent,
            "failed_event_sends": self._failed_event_sends,
            "failed_snap_sends": self._failed_snap_sends,
            "event_buffer_size": len(self._event_buffer),
            "engine_bound": self._engine is not None,
            "running": self._running,
        }
