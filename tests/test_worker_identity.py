from origintracer.core import worker_identity


def test_worker_identity_is_stable_for_current_process():
    instance_id, pid = worker_identity.current_worker_identity()
    again, same_pid = worker_identity.current_worker_identity()
    assert instance_id == again
    assert pid == same_pid


def test_worker_identity_changes_when_pid_changes(monkeypatch):
    first_id, first_pid = (
        worker_identity.current_worker_identity()
    )
    monkeypatch.setattr(
        worker_identity.os, "getpid", lambda: first_pid + 1
    )
    second_id, second_pid = (
        worker_identity.current_worker_identity()
    )
    assert second_pid == first_pid + 1
    assert second_id != first_id
