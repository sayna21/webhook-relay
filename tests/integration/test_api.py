import pytest
from fastapi.testclient import TestClient


@pytest.mark.parametrize(
    "body",
    [
        {"url": "ftp://example.com", "event_types": ["*"]},
        {"url": "http://user:password@example.com", "event_types": ["*"]},
        {"url": "http://localhost:8102", "event_types": []},
        {"url": "invalid", "event_types": ["*"]},
        {"url": "http://localhost:8102", "event_types": [""]},
        {"url": "http://localhost:8102", "event_types": ["   "]},
        {"url": "http://localhost:8102", "event_types": "*"},
    ],
)
def test_api_validation_errors(body):
    from relay.api import create_app

    with TestClient(create_app()) as client:
        assert client.post("/subscriptions", json=body).status_code == 422


@pytest.fixture
def client(db_url):
    from relay.api import create_app
    from relay.config import Settings

    with TestClient(create_app(Settings(database_url=db_url))) as result:
        yield result


@pytest.mark.integration
def test_subscription_creation_get_list_disable_and_secret_only_once(client):
    response = client.post(
        "/subscriptions", json={"url": "http://127.0.0.1:8102/webhook", "event_types": ["*"]}
    )
    assert response.status_code == 201
    subscription = response.json()
    assert subscription["id"]
    assert subscription["secret"]
    assert client.get("/subscriptions").json()[0]["id"] == subscription["id"]
    detail = client.get(f"/subscriptions/{subscription['id']}")
    assert detail.status_code == 200
    assert "secret" not in detail.json()
    assert subscription["secret"] not in client.get("/subscriptions").text
    changed = client.patch(f"/subscriptions/{subscription['id']}", json={"active": False})
    assert changed.status_code == 200
    assert changed.json()["active"] is False
    assert "secret" not in changed.json()


@pytest.mark.parametrize(
    "authorization", [None, "Basic private", "Bearer wrong", "Bearer", "Bearer private extra"]
)
def test_optional_admin_token_rejects_unauthorized_requests(authorization):
    from relay.api import create_app
    from relay.config import Settings

    with TestClient(
        create_app(Settings(admin_token="private")), raise_server_exceptions=False
    ) as client:
        headers = {} if authorization is None else {"Authorization": authorization}
        response = client.get("/subscriptions", headers=headers)
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "Bearer"
        assert "private" not in response.text
        assert client.get("/healthz").status_code == 200


def test_health_is_public_and_factory_and_global_app_work():
    from relay.api import app, create_app
    from relay.config import Settings

    with TestClient(app) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
    with TestClient(create_app(Settings(admin_token="private"))) as client:
        assert client.get("/healthz").status_code == 200


@pytest.mark.integration
def test_events_idempotency_fanout_and_status(client):
    wildcard = client.post(
        "/subscriptions", json={"url": "http://localhost:8102", "event_types": ["*"]}
    ).json()
    matching = client.post(
        "/subscriptions", json={"url": "http://localhost:8102", "event_types": ["order.created"]}
    ).json()
    disabled = client.post(
        "/subscriptions", json={"url": "http://localhost:8102", "event_types": ["*"]}
    ).json()
    client.patch(f"/subscriptions/{disabled['id']}", json={"active": False})
    client.post("/subscriptions", json={"url": "http://localhost:8102", "event_types": ["other"]})
    body = {"type": "order.created", "payload": {"order": 42}}
    first = client.post("/events", json=body, headers={"Idempotency-Key": "order-42"})
    assert first.status_code == 202
    assert first.json()["deliveries"] == 2
    repeat = client.post("/events", json=body, headers={"Idempotency-Key": "order-42"})
    assert repeat.status_code == 200
    assert repeat.json() == first.json()
    conflict = client.post(
        "/events", json={**body, "payload": {"order": 43}}, headers={"Idempotency-Key": "order-42"}
    )
    assert conflict.status_code == 422
    event = client.get(f"/events/{first.json()['event_id']}").json()
    assert {d["subscription_id"] for d in event["deliveries"]} == {wildcard["id"], matching["id"]}
    assert event["payload"] == body["payload"]
    assert wildcard["secret"] not in str(event)
    assert client.get("/stats").json()["counts"]["pending"] == 2
    page = client.get(
        "/deliveries", params={"status": "pending", "subscription_id": wildcard["id"], "limit": 1}
    ).json()
    assert len(page["items"]) == 1
    assert page["limit"] == 1
    assert client.get("/deliveries", params={"offset": 2}).json()["items"] == []


@pytest.mark.integration
def test_api_replay_preserves_attempt_history(client, db_url):
    from relay import store
    from relay.db import connect

    client.post("/subscriptions", json={"url": "http://localhost:8102", "event_types": ["*"]})
    client.post("/events", json={"type": "sample", "payload": {}})
    with connect(db_url) as conn:
        claim = store.claim_due(conn, "api-test", 1, 30)[0]
        assert store.record_result(conn, claim, "dead", worker_id="api-test", http_status=400)
    path = f"/deliveries/{claim['id']}"
    assert client.get(path + "/attempts").json()[0]["outcome"] == "dead"
    replayed = client.post(path + "/replay")
    assert replayed.status_code == 200
    assert replayed.json()["status"] == "pending"
    assert replayed.json()["attempt_count"] == 0
    assert len(client.get(path + "/attempts").json()) == 1
    assert client.post(path + "/replay").status_code == 409
    with connect(db_url) as conn:
        claim = store.claim_due(conn, "api-test", 1, 30)[0]
        assert store.record_result(conn, claim, "success", worker_id="api-test", http_status=204)
    assert client.post(path + "/replay").status_code == 200
    assert len(client.get(path + "/attempts").json()) == 2


@pytest.mark.integration
@pytest.mark.parametrize(
    "path",
    [
        "/subscriptions/00000000-0000-0000-0000-000000000000",
        "/events/00000000-0000-0000-0000-000000000000",
        "/deliveries/9223372036854775807/attempts",
    ],
)
def test_api_missing_resources_are_404(client, path):
    assert client.get(path).status_code == 404


@pytest.mark.integration
def test_missing_replay_and_subscription_patch(client):
    assert client.post("/deliveries/9223372036854775807/replay").status_code == 404
    assert (
        client.patch(
            "/subscriptions/00000000-0000-0000-0000-000000000000", json={"active": False}
        ).status_code
        == 404
    )


@pytest.mark.parametrize(
    "method,path,kwargs",
    [
        ("post", "/events", {"json": {"type": "", "payload": {}}}),
        ("post", "/events", {"json": {"type": 12, "payload": {}}}),
        ("post", "/events", {"json": {"type": "ok", "payload": []}}),
        ("post", "/events", {"json": {"type": "ok", "payload": {}, "extra": 1}}),
        (
            "post",
            "/events",
            {"json": {"type": "ok", "payload": {}}, "headers": {"Idempotency-Key": " "}},
        ),
        (
            "patch",
            "/subscriptions/00000000-0000-0000-0000-000000000000",
            {"json": {"active": "false"}},
        ),
        ("get", "/events/not-a-uuid", {}),
        ("get", "/deliveries?status=invalid", {}),
        ("get", "/deliveries?limit=0", {}),
        ("get", "/deliveries?limit=1001", {}),
        ("get", "/deliveries?offset=-1", {}),
        ("get", "/deliveries?subscription_id=bad", {}),
        ("get", "/deliveries/0/attempts", {}),
        ("post", "/deliveries/-1/replay", {}),
    ],
)
def test_api_invalid_requests_do_not_touch_database(method, path, kwargs):
    from relay.api import create_app

    with TestClient(create_app()) as client:
        assert getattr(client, method)(path, **kwargs).status_code == 422


@pytest.mark.integration
def test_valid_admin_token_allows_management(db_url):
    from relay.api import create_app
    from relay.config import Settings

    with TestClient(create_app(Settings(database_url=db_url, admin_token="private"))) as client:
        assert client.get("/stats", headers={"Authorization": "Bearer private"}).status_code == 200
