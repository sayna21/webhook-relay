import threading

from relay.config import Settings
from relay.worker import run_loop


def test_worker_already_stopped_never_opens_database():
    stop = threading.Event()
    stop.set()
    run_loop(Settings(database_url="postgresql://invalid.invalid/nope"), stop_event=stop)


def test_worker_bounds_claims_to_free_slots_and_commits_results(monkeypatch):
    from contextlib import contextmanager
    from datetime import UTC, datetime

    import relay.worker as worker
    from relay.deliver import AttemptResult

    stop = threading.Event()
    lock = threading.Lock()
    state = {"issued": 0, "finished": 0, "active": 0, "maximum": 0}
    connections = []
    http_clients = []

    class Pool:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        @contextmanager
        def connection(self):
            conn = object()
            connections.append(conn)
            yield conn

    def claim(conn, identity, batch, lease, max_attempts):
        with lock:
            assert batch <= 2 - (state["issued"] - state["finished"])
            count = min(batch, 5 - state["issued"])
            jobs = [{"id": state["issued"] + i} for i in range(count)]
            state["issued"] += count
            return jobs

    def send(job, settings, client):
        import time

        with lock:
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
            http_clients.append(client)
        time.sleep(0.01)
        with lock:
            state["active"] -= 1
        now = datetime.now(UTC)
        return AttemptResult("success", 204, None, now, now, 0)

    def record(conn, job, **kwargs):
        assert kwargs["worker_id"] == "test-worker"
        assert kwargs["outcome"] == "success"
        assert kwargs["max_attempts"] == 15
        with lock:
            state["finished"] += 1
            if state["finished"] == 5:
                stop.set()

    monkeypatch.setattr(worker, "make_pool", lambda *a, **kw: Pool())
    monkeypatch.setattr(worker.store, "claim_due", claim)
    monkeypatch.setattr(worker.store, "record_result", record)
    monkeypatch.setattr(worker, "send_delivery", send)
    thread = threading.Thread(
        target=worker.run_loop,
        kwargs={
            "settings": Settings(concurrency=2, batch=100, poll_ms=1),
            "stop_event": stop,
            "worker_id": "test-worker",
        },
    )
    thread.start()
    thread.join(timeout=5)
    stop.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert state["finished"] == 5
    assert state["maximum"] == 2
    assert len(set(map(id, connections))) == len(connections)
    assert len(set(map(id, http_clients))) == 1
    assert http_clients[0].is_closed


def test_worker_stop_during_claim_drains_committed_batch(monkeypatch):
    from contextlib import contextmanager
    from datetime import UTC, datetime

    import relay.worker as worker
    from relay.deliver import AttemptResult

    stop = threading.Event()
    claimed = []
    recorded = []

    class Pool:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        @contextmanager
        def connection(self):
            yield object()

    def claim(*args):
        claimed.append(True)
        stop.set()
        return [{"id": 1}, {"id": 2}]

    def send(*args, **kwargs):
        now = datetime.now(UTC)
        return AttemptResult("success", 200, None, now, now, 0)

    monkeypatch.setattr(worker, "make_pool", lambda *a, **kw: Pool())
    monkeypatch.setattr(worker.store, "claim_due", claim)
    monkeypatch.setattr(
        worker.store, "record_result", lambda conn, job, **kwargs: recorded.append(job["id"])
    )
    monkeypatch.setattr(worker, "send_delivery", send)
    previous = worker.signal.getsignal(worker.signal.SIGTERM)
    worker.run_loop(Settings(concurrency=2), stop_event=stop)
    assert claimed == [True]
    assert sorted(recorded) == [1, 2]
    assert worker.signal.getsignal(worker.signal.SIGTERM) == previous


def test_worker_existing_stop_file_never_opens_database(tmp_path):
    path = tmp_path / "stop"
    path.touch()
    run_loop(Settings(database_url="postgresql://invalid.invalid/nope"), stop_file=path)


def test_worker_idle_limit_exits_without_busy_polling(monkeypatch):
    from contextlib import contextmanager

    import relay.worker as worker

    calls = []

    class Pool:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        @contextmanager
        def connection(self):
            yield object()

    def claim(*args):
        calls.append(True)
        return []

    monkeypatch.setattr(worker, "make_pool", lambda *a, **kw: Pool())
    monkeypatch.setattr(worker.store, "claim_due", claim)
    worker.run_loop(Settings(poll_ms=1), max_idle_polls=3)
    assert len(calls) == 3
