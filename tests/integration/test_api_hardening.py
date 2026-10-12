"""Regression tests for API values PostgreSQL cannot store."""

import json
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def hardening_client(monkeypatch):
    from relay.api import create_app

    def unexpected_connection(*args, **kwargs):
        raise AssertionError("Invalid request reached PostgreSQL")

    monkeypatch.setattr("relay.api.connect", unexpected_connection)
    with TestClient(create_app(), raise_server_exceptions=False) as client:
        yield client


@pytest.mark.parametrize(
    "payload",
    [
        {"nested": [{"value": "nul\x00value"}]},
        {"nested": [{"nul\x00key": True}]},
        {"nested": [{"value": "\ud800"}]},
        {"nested": [{"value": "\udfff"}]},
        {"nested": [{"\ud800": None}]},
        {"nested": [{"\udfff": None}]},
    ],
)
def test_payload_unstorable_unicode_is_422_before_storage(hardening_client, payload):
    response = hardening_client.post(
        "/events",
        content=json.dumps({"type": "valid", "payload": payload}),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422


@pytest.mark.parametrize("number", ["1e400", "-1e400"])
def test_nested_overflowing_json_number_is_422_before_storage(hardening_client, number):
    # Exponent overflow is legal JSON syntax; the JSON parser produces infinity.
    response = hardening_client.post(
        "/events",
        content='{"type":"valid","payload":{"nested":[{"number":' + number + "}]}}",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422


@pytest.mark.parametrize("event_type", ["nul\x00type", "bad\ud800", "bad\udfff"])
@pytest.mark.parametrize("path", ["/events", "/subscriptions"])
def test_event_type_unstorable_unicode_is_422_before_storage(hardening_client, event_type, path):
    body = (
        {"type": event_type, "payload": {}}
        if path == "/events"
        else {"url": "http://localhost:8102", "event_types": [event_type]}
    )
    response = hardening_client.post(
        path,
        content=json.dumps(body),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422


@pytest.mark.parametrize("offset", [9223372036854775808, 10**100])
def test_delivery_offset_overflow_is_422_before_storage(hardening_client, offset):
    assert hardening_client.get("/deliveries", params={"offset": offset}).status_code == 422


@pytest.mark.integration
def test_storage_validation_preserves_valid_unicode_and_json(db_url):
    from relay.api import create_app
    from relay.config import Settings

    event_type = "订单.完成🚀"
    payload = {
        "日本語🔑": [None, True, False, 0, -42, 1.25, 1e100, {"text": "café привет 🚀"}],
        "escaped": "line\n\tend",
    }
    with TestClient(create_app(Settings(database_url=db_url))) as client:
        subscription = client.post(
            "/subscriptions",
            json={"url": "http://localhost:8102", "event_types": [event_type]},
        )
        assert subscription.status_code == 201
        event = client.post("/events", json={"type": event_type, "payload": payload})
        assert event.status_code == 202
        assert event.json()["deliveries"] == 1
        saved = client.get("/events/" + event.json()["event_id"])
        assert saved.status_code == 200
        assert saved.json()["type"] == event_type
        # JSONB normalizes exponent notation; compare decimal JSON values, not binary floats.
        assert json.loads(saved.text, parse_float=Decimal)["payload"] == json.loads(
            json.dumps(payload), parse_float=Decimal
        )
        assert client.get("/deliveries", params={"offset": 9223372036854775807}).status_code == 200


@pytest.mark.parametrize(
    "path,body",
    [
        ("/events", '{"type":1e400,"payload":{}}'),
        ("/events", '{"type":-1e400,"payload":{}}'),
        ("/subscriptions", '{"url":"http://localhost:8102","event_types":[1e400]}'),
        ("/subscriptions", '{"url":"http://localhost:8102","event_types":[-1e400]}'),
        ("/subscriptions", '{"url":1e400,"event_types":["*"]}'),
        ("/subscriptions", '{"url":"http://example.com/\\ud800","event_types":["*"]}'),
        ("/subscriptions", '{"url":"http://example.com/\\udfff","event_types":["*"]}'),
        ("/events", '{"type":1e400,"payload":{},"\\ud800":true}'),
    ],
)
def test_validation_errors_serialize_safely(hardening_client, path, body):
    response = hardening_client.post(
        path, content=body, headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422
    errors = response.json()["detail"]
    assert errors
    assert all(set(error) == {"type", "loc", "msg"} for error in errors)
