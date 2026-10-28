"""Process-scoped identity used to associate uploads with one worker."""

from __future__ import annotations

import os
import threading
import uuid

_lock = threading.Lock()
_identity_pid: int | None = None
_worker_instance_id: str | None = None


def _reset_lock_after_fork() -> None:
    """Replace the inherited mutex so a child cannot deadlock on a parent lock."""
    global _lock
    _lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_lock_after_fork)


def current_worker_identity() -> tuple[str, int]:
    """Return the current process instance ID and OS PID.

    The PID check makes the identity fork-safe: a child receives a fresh
    instance ID on its first call, even if this module was imported by its
    parent process.
    """
    global _identity_pid, _worker_instance_id

    pid = os.getpid()
    if _identity_pid != pid or _worker_instance_id is None:
        with _lock:
            if (
                _identity_pid != pid
                or _worker_instance_id is None
            ):
                _identity_pid = pid
                _worker_instance_id = str(uuid.uuid4())
    return _worker_instance_id, pid
