"""Real subprocess tests for delivery under transient failure and hard crashes."""

import os
import signal
import time

import pytest

from bench.harness import Processes
from bench.run import run_scenario
from relay import store
from relay.db import connect

pytestmark = pytest.mark.chaos


def test_e2e_failing_receiver(db_url):
    result = run_scenario(
        db_url,
        events=200,
        subscriptions=3,
        workers=2,
        failure_rate=0.25,
        timeout_rate=0.05,
        deadline_s=120,
    )
    assert result["lost"] == 0
    assert result["delivered_unique"] + result["dead"] >= 600
    assert result["signed_sample"]["verified"]


def test_sigkill_worker_loses_nothing(db_url):
    result = run_scenario(
        db_url,
        events=100,
        subscriptions=3,
        workers=2,
        kill=True,
        failure_rate=0.25,
        timeout_rate=0.05,
        deadline_s=120,
        http_timeout_s=0.5,
        timeout_s=0.75,
        lease_s=2,
    )
    assert result["lost"] == 0
    assert result["kill_progress"] >= 0.3
    print(f"Abrupt crash method={result['kill_method']}; duplicates={result['duplicates']}")


def test_sigterm_graceful(db_url, tmp_path):
    # Windows has no POSIX SIGTERM: exercise the same stop-event drain using a stop file.
    stop_file = tmp_path / "stop"
    with Processes(db_url) as processes:
        receiver = processes.server("receiver.mock_receiver:app", "/receipts")
        import httpx

        with connect(db_url) as conn:
            sub = store.create_subscription(conn, receiver + "/webhook/pending", ["*"])
            conn.execute(
                "UPDATE subscriptions SET url=%s WHERE id=%s",
                (receiver + "/webhook/" + str(sub["id"]), sub["id"]),
            )
            for i in range(20):
                store.fan_out(conn, "graceful", {"n": i})
        response = httpx.post(
            receiver + "/configure",
            json={
                "secrets": {str(sub["id"]): sub["secret"]},
                "failure_rate": 0,
                "timeout_rate": 0,
                "latency_min_ms": 300,
                "latency_max_ms": 300,
            },
            trust_env=False,
        )
        response.raise_for_status()
        worker = processes.start(
            "-m",
            "relay.worker",
            "--concurrency",
            "2",
            "--batch",
            "2",
            "--stop-file",
            str(stop_file),
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            with connect(db_url) as conn:
                active = conn.execute(
                    "SELECT count(*) AS n FROM deliveries WHERE status='in_flight'"
                ).fetchone()["n"]
            if active:
                break
            if worker.poll() is not None:
                raise AssertionError(processes.error(worker))
            time.sleep(0.02)
        assert active > 0
        if os.name == "nt":
            stop_file.touch()
        else:
            worker.send_signal(signal.SIGTERM)
        worker.wait(timeout=15)
        assert worker.returncode == 0
        with connect(db_url) as conn:
            assert (
                conn.execute(
                    "SELECT count(*) AS n FROM deliveries WHERE status='in_flight'"
                ).fetchone()["n"]
                == 0
            )
