import json
import random
from datetime import UTC, datetime
from uuid import UUID

import httpx
import pytest

from relay.config import Settings


def test_worker_already_stopped_never_opens_database():
    import threading

    from relay.worker import run_loop

    stop = threading.Event()
    stop.set()
    run_loop(Settings(database_url="postgresql://invalid.invalid/nope"), stop_event=stop)


def test_worker_cli_supports_required_flags_without_database():
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-m", "relay.worker", "--help"], capture_output=True, text=True, timeout=5
    )
    assert result.returncode == 0, result.stderr
    for flag in ["--concurrency", "--batch", "--lease-s", "--poll-ms"]:
        assert flag in result.stdout


def test_delivery_measures_duration_with_monotonic_clock(monkeypatch):
    import relay.deliver as delivery

    class Clock(datetime):
        @classmethod
        def now(cls, tz):
            return next(wall)

    wall = iter([datetime(2026, 10, 11, tzinfo=UTC), datetime(2026, 10, 10, tzinfo=UTC)])
    monotonic = iter([100.0, 100.125])
    monkeypatch.setattr(delivery, "datetime", Clock)
    monkeypatch.setattr(delivery.time, "monotonic", lambda: next(monotonic))
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200))) as client:
        result = delivery.send_delivery(claim(), Settings(), client=client)
    assert (result.finished_at - result.started_at).total_seconds() == 0.125


def test_delivery_creates_and_closes_own_client_for_real_http():
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    from relay.deliver import send_delivery

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        job = claim()
        job["url"] = f"http://127.0.0.1:{server.server_port}/webhook"
        assert send_delivery(job, Settings(http_timeout_s=0.5)).outcome == "success"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    assert not thread.is_alive()


@pytest.mark.parametrize(
    "exception,error",
    [
        (httpx.ReadTimeout, "timeout"),
        (httpx.ConnectTimeout, "timeout"),
        (httpx.WriteTimeout, "timeout"),
        (httpx.PoolTimeout, "timeout"),
        (httpx.ConnectError, "network"),
        (httpx.ReadError, "network"),
        (httpx.WriteError, "network"),
        (httpx.RemoteProtocolError, "network"),
    ],
)
def test_delivery_network_errors_are_bounded_retry_results(exception, error):
    from relay.deliver import send_delivery

    def receive(request):
        raise exception("private-url-and-secret-must-not-leak", request=request)

    with httpx.Client(transport=httpx.MockTransport(receive)) as client:
        result = send_delivery(claim(), Settings(), client=client, rng=random.Random(0))
    assert result.outcome == "retry"
    assert result.http_status is None
    assert result.error == error
    assert 0 <= result.retry_delay_s <= 10
    assert result.finished_at >= result.started_at


def test_delivery_does_not_follow_redirects_on_shared_client():
    from relay.deliver import send_delivery

    seen = []

    def receive(request):
        seen.append(str(request.url))
        return (
            httpx.Response(302, headers={"Location": "http://receiver/other"})
            if len(seen) == 1
            else httpx.Response(200)
        )

    with httpx.Client(transport=httpx.MockTransport(receive), follow_redirects=True) as client:
        result = send_delivery(claim(), Settings(), client=client)
    assert result.outcome == "dead"
    assert result.http_status == 302
    assert seen == [claim()["url"]]


def test_delivery_timeout_is_explicit_for_all_phases():
    from relay.deliver import send_delivery

    seen = []

    def receive(request):
        seen.append(request.extensions["timeout"])
        return httpx.Response(200)

    with httpx.Client(transport=httpx.MockTransport(receive), timeout=None) as client:
        send_delivery(claim(), Settings(http_timeout_s=0.25), client=client)
    assert seen == [{"connect": 0.25, "read": 0.25, "write": 0.25, "pool": 0.25}]


@pytest.mark.parametrize(
    "status,expected",
    [
        (200, "success"),
        (299, "success"),
        (302, "dead"),
        (307, "dead"),
        (400, "dead"),
        (410, "dead"),
        (408, "retry"),
        (429, "retry"),
        (500, "retry"),
        (503, "retry"),
    ],
)
def test_delivery_classifies_response_and_schedules_jitter(status, expected):
    from relay.deliver import send_delivery

    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(status))) as client:
        result = send_delivery(
            claim(), Settings(retry_base_s=10, retry_cap_s=20), client=client, rng=random.Random(17)
        )
    assert result.outcome == expected
    assert result.http_status == status
    assert result.retry_delay_s == (random.Random(17).uniform(0, 10) if expected == "retry" else 0)


def claim():
    return {
        "id": 7,
        "event_id": UUID("d89db720-f809-41a6-81fa-fd912f33418b"),
        "subscription_id": "subscriber",
        "attempt_count": 1,
        "lease_token": "token",
        "url": "http://receiver/webhook/subscriber",
        "secret": "secret",
        "event_type": "order.created",
        "payload": {"z": 2, "a": "é"},
        "event_created_at": datetime(2026, 10, 11, tzinfo=UTC),
    }


def test_delivery_signed_immutable_event_body_and_headers():
    from relay.deliver import send_delivery
    from relay.signing import verify

    received = []

    def receive(request):
        received.append(request)
        return httpx.Response(204)

    job = claim()
    with httpx.Client(transport=httpx.MockTransport(receive)) as client:
        first = send_delivery(job, Settings(), client=client)
        job["attempt_count"] = 2
        job["payload"] = {"a": "é", "z": 2}
        second = send_delivery(job, Settings(), client=client)
        assert not client.is_closed
    assert first.outcome == second.outcome == "success"
    assert first.http_status == 204
    assert first.error is None
    assert first.retry_delay_s == 0
    assert first.started_at.tzinfo == UTC
    assert first.finished_at >= first.started_at
    assert received[0].content == received[1].content
    assert json.loads(received[0].content) == {
        "id": str(job["event_id"]),
        "type": "order.created",
        "payload": job["payload"],
        "created_at": "2026-10-11T00:00:00+00:00",
    }
    for attempt, request in enumerate(received, 1):
        assert request.method == "POST"
        assert request.headers["Content-Type"] == "application/json"
        assert request.headers["Relay-Event-Id"] == str(job["event_id"])
        assert request.headers["Relay-Delivery-Id"] == "7"
        assert request.headers["Relay-Attempt"] == str(attempt)
        assert verify(
            "secret",
            request.headers["Relay-Signature"],
            request.content,
            datetime.now(UTC).timestamp(),
        )
