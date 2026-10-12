import hashlib
import hmac
import json
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient


@pytest.mark.parametrize(
    "config",
    [
        {"failure_rate": -0.1},
        {"timeout_rate": 1.1},
        {"failure_rate": 0.8, "timeout_rate": 0.8},
        {"latency_min_ms": 20, "latency_max_ms": 10},
        {"timeout_s": 0},
        {"secrets": {"bad-uuid": "secret"}},
        {"secrets": {"11111111-1111-4111-8111-111111111111": ""}},
        {"unknown_knob": 1},
    ],
)
def test_configuration_validation(config):
    from receiver.mock_receiver import create_app

    with TestClient(create_app()) as client:
        assert client.post("/configure", json=config).status_code == 422


def test_register_secret_after_subscription_creation():
    from receiver.mock_receiver import create_app

    with TestClient(create_app()) as client:
        response = client.post(
            "/configure", json={"secrets": {SUBSCRIPTION: "creation-secret"}, "rng_seed": 7}
        )
        assert response.status_code == 200
        assert response.json()["known_subscriptions"] == [SUBSCRIPTION]
        assert "creation-secret" not in response.text


SUBSCRIPTION = "11111111-1111-4111-8111-111111111111"
EVENT = "22222222-2222-4222-8222-222222222222"
SECRET = "test-local-secret"


def test_configured_seed_restarts_receiver_random_sequence():
    from receiver.mock_receiver import create_app

    with TestClient(
        create_app(
            secrets={SUBSCRIPTION: SECRET},
            failure_rate=0.5,
            timeout_rate=0,
            latency_min_ms=0,
            latency_max_ms=0,
        )
    ) as client:
        sequences = []
        for _ in range(2):
            client.post("/configure", json={"rng_seed": 417}).raise_for_status()
            sequences.append([signed_request(client).status_code for _ in range(20)])
        assert sequences[0] == sequences[1]
        assert set(sequences[0]) == {200, 500}


def signed_request(client, *, subscription=SUBSCRIPTION, event=EVENT, attempt=1, body=None):
    body = body or json.dumps({"id": event, "type": "test", "payload": {}}).encode()
    timestamp = int(time.time())
    digest = hmac.new(SECRET.encode(), str(timestamp).encode() + b"." + body, hashlib.sha256)
    headers = {
        "Relay-Signature": f"t={timestamp},v1={digest.hexdigest()}",
        "Relay-Event-Id": event,
        "Relay-Delivery-Id": "1",
        "Relay-Attempt": str(attempt),
        "Content-Type": "application/json",
    }
    return client.post(f"/webhook/{subscription}", content=body, headers=headers)


def test_verified_delivery_acknowledges_every_attempt_but_dedupes_effects_per_pair():
    from receiver.mock_receiver import create_app

    other = "33333333-3333-4333-8333-333333333333"
    application = create_app(
        failure_rate=0,
        timeout_rate=0,
        secrets={SUBSCRIPTION: SECRET, other: SECRET},
    )
    with TestClient(application) as client:
        assert signed_request(client).status_code == 200
        assert signed_request(client, attempt=2).status_code == 200
        assert signed_request(client, subscription=other).status_code == 200
        receipts = client.get("/receipts").json()
        assert [r["duplicate"] for r in receipts] == [False, True, False]
        assert [r["side_effect_applied"] for r in receipts] == [True, False, True]
        assert all(r["acknowledged"] and r["signature_valid"] for r in receipts)
        assert receipts[0]["event_id"] == EVENT
        assert receipts[1]["attempt"] == 2
        assert receipts[0]["delivery_id"] == 1
        assert receipts[0]["raw_body"]
        assert receipts[0]["signature"]
        assert SECRET not in json.dumps(receipts)


def test_failure_and_timeout_categories_are_disjoint_and_seeded():
    from receiver.mock_receiver import create_app

    def run():
        app = create_app(
            failure_rate=0.5,
            timeout_rate=0.5,
            timeout_s=0.001,
            latency_min_ms=0,
            latency_max_ms=0,
            rng_seed=8,
            secrets={SUBSCRIPTION: SECRET},
        )
        with TestClient(app) as client:
            for attempt in range(1, 21):
                signed_request(client, attempt=attempt)
            return client.get("/receipts").json()

    first, second = run(), run()
    assert [r["category"] for r in first] == [r["category"] for r in second]
    assert {r["category"] for r in first} == {"failure", "timeout"}
    for receipt in first:
        if receipt["category"] == "failure":
            assert receipt["http_status"] == 500
            assert not receipt["acknowledged"]
            assert not receipt["side_effect_applied"]
        else:
            assert receipt["http_status"] == 200
            assert receipt["acknowledged"]


@pytest.mark.parametrize(
    "problem",
    [
        "bad_subscription",
        "bad_event",
        "bad_delivery",
        "bad_attempt",
        "missing_headers",
        "bad_json",
        "bad_payload",
        "mismatched_event",
    ],
)
def test_malformed_webhooks_are_clean_errors_and_recorded(problem):
    from receiver.mock_receiver import create_app

    app = create_app(failure_rate=0, timeout_rate=0, secrets={SUBSCRIPTION: SECRET})
    body = {"id": EVENT, "type": "test", "payload": {}}
    headers = {"Relay-Event-Id": EVENT, "Relay-Delivery-Id": "1", "Relay-Attempt": "1"}
    subscription = SUBSCRIPTION
    if problem == "bad_subscription":
        subscription = "not-a-uuid"
    elif problem == "bad_event":
        headers["Relay-Event-Id"] = "bad"
    elif problem == "bad_delivery":
        headers["Relay-Delivery-Id"] = "wat"
    elif problem == "bad_attempt":
        headers["Relay-Attempt"] = "-1"
    elif problem == "missing_headers":
        headers.clear()
    elif problem == "bad_payload":
        body["payload"] = []
    elif problem == "mismatched_event":
        body["id"] = SUBSCRIPTION
    with TestClient(app) as client:
        raw = b"{" if problem == "bad_json" else json.dumps(body).encode()
        response = client.post(f"/webhook/{subscription}", content=raw, headers=headers)
        assert response.status_code == 422
        receipts = client.get("/receipts").json()
        assert len(receipts) == 1
        assert receipts[0]["http_status"] == 422
        assert receipts[0]["acknowledged"] is False


def test_reset_clears_receipts_and_dedupe_but_preserves_secret_registration():
    from receiver.mock_receiver import create_app

    with TestClient(create_app(failure_rate=0, timeout_rate=0)) as client:
        client.post("/configure", json={"secrets": {SUBSCRIPTION: SECRET}})
        signed_request(client)
        assert client.post("/reset").status_code == 200
        assert client.get("/receipts").json() == []
        assert signed_request(client).status_code == 200
        assert client.get("/receipts").json()[0]["side_effect_applied"] is True


def test_timeout_waits_three_seconds_without_blocking_receiver_loop():
    from receiver.mock_receiver import create_app

    app = create_app(
        failure_rate=0,
        timeout_rate=1,
        latency_min_ms=0,
        latency_max_ms=0,
        secrets={SUBSCRIPTION: SECRET},
    )
    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as pool:
        started = time.monotonic()
        future = pool.submit(signed_request, client)
        receipts = []
        deadline = started + 2
        while not receipts and time.monotonic() < deadline:
            receipts = client.get("/receipts").json()
            if not receipts:
                time.sleep(0.01)
        assert receipts, "The receiver event loop must stay responsive during timeout"
        assert not future.done()
        assert receipts[0]["acknowledged"] is True
        assert receipts[0]["side_effect_applied"] is True
        assert receipts[0]["response_completed"] is False
        assert future.result(timeout=5).status_code == 200
        assert time.monotonic() - started >= 3
        assert client.get("/receipts").json()[0]["response_completed"] is True


def test_unknown_subscription_rejected_and_recorded():
    from receiver.mock_receiver import create_app

    with TestClient(create_app()) as client:
        response = client.post(f"/webhook/{SUBSCRIPTION}", json={"payload": {}})
        assert response.status_code == 403
        receipts = client.get("/receipts").json()
        assert len(receipts) == 1
        assert receipts[0]["http_status"] == 403
        assert receipts[0]["acknowledged"] is False
        assert receipts[0]["signature_valid"] is False
        assert "secret" not in receipts[0]
