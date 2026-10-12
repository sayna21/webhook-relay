"""Real PostgreSQL storage invariants; shared fixtures create a fresh test database."""

import pytest

pytestmark = pytest.mark.integration


def test_migrations_idempotent(db_url, db_conn):
    from relay.db import apply_migrations, connect, make_pool

    apply_migrations(db_url)
    apply_migrations(db_url)
    with connect(db_url) as conn:
        assert isinstance(conn.execute("SELECT 1 AS value").fetchone(), dict)
        assert conn.execute("SELECT count(*) AS n FROM schema_migrations").fetchone()["n"] == 1
        columns = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'deliveries' ORDER BY ordinal_position"
        ).fetchall()
        assert [row["column_name"] for row in columns] == [
            "id",
            "event_id",
            "subscription_id",
            "status",
            "attempt_count",
            "next_attempt_at",
            "lease_token",
            "lease_expires_at",
            "last_error",
            "succeeded_at",
            "created_at",
            "updated_at",
        ]
    with make_pool(db_url, max_size=2) as pool:
        with pool.connection() as conn:
            assert conn.execute("SELECT 1 AS value").fetchone() == {"value": 1}


def test_subscription_secret_creation_only(db_conn):
    from relay import store

    created = store.create_subscription(db_conn, "http://127.0.0.1:8102/webhook", ["x"])
    assert created["active"] is True
    assert len(created["secret"]) >= 32
    assert created["event_types"] == ["x"]
    assert "secret" not in store.get_subscription(db_conn, created["id"])
    assert all("secret" not in s for s in store.list_subscriptions(db_conn))
    disabled = store.set_subscription_active(db_conn, created["id"], False)
    assert disabled["active"] is False
    assert "secret" not in disabled
    assert store.get_subscription(db_conn, "00000000-0000-0000-0000-000000000000") is None
    assert (
        store.set_subscription_active(db_conn, "00000000-0000-0000-0000-000000000000", True) is None
    )


def test_fanout_matching_active_only(db_conn):
    from relay import store

    matching = store.create_subscription(db_conn, "http://example.com/matching", ["x"])
    wildcard = store.create_subscription(db_conn, "http://example.com/all", ["*"])
    inactive = store.create_subscription(db_conn, "http://example.com/inactive", ["x"])
    store.set_subscription_active(db_conn, inactive["id"], False)
    store.create_subscription(db_conn, "http://example.com/other", ["y"])
    result = store.fan_out(db_conn, "x", {"value": 1})
    assert result["deliveries"] == 2
    assert result["replayed"] is False
    rows = db_conn.execute("SELECT subscription_id FROM deliveries").fetchall()
    assert {r["subscription_id"] for r in rows} == {matching["id"], wildcard["id"]}


def test_fanout_atomic_rollback(db_conn):
    from psycopg.errors import RaiseException

    from relay import store

    store.create_subscription(db_conn, "http://example.com/all", ["*"])
    db_conn.execute("""
        CREATE FUNCTION reject_delivery() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'fanout failure'; END; $$;
        CREATE TRIGGER reject_delivery BEFORE INSERT ON deliveries
        FOR EACH ROW EXECUTE FUNCTION reject_delivery();
    """)
    try:
        with pytest.raises(RaiseException, match="fanout failure"):
            store.fan_out(db_conn, "x", {})
        assert db_conn.execute("SELECT count(*) AS n FROM events").fetchone()["n"] == 0
        assert db_conn.execute("SELECT count(*) AS n FROM deliveries").fetchone()["n"] == 0
    finally:
        db_conn.execute("DROP TRIGGER reject_delivery ON deliveries")
        db_conn.execute("DROP FUNCTION reject_delivery()")


def test_event_idempotency_same_key(db_conn):
    from relay import store

    store.create_subscription(db_conn, "http://example.com/all", ["*"])
    first = store.fan_out(db_conn, "x", {"a": 1, "b": 2}, "same-key")
    again = store.fan_out(db_conn, "x", {"b": 2, "a": 1}, "same-key")
    assert first["event_id"] == again["event_id"]
    assert first["deliveries"] == again["deliveries"] == 1
    assert again["replayed"] is True
    assert db_conn.execute("SELECT count(*) AS n FROM events").fetchone()["n"] == 1
    assert db_conn.execute("SELECT count(*) AS n FROM deliveries").fetchone()["n"] == 1


def test_idempotency_key_reuse_different_body_rejected(db_conn):
    from relay import store

    store.fan_out(db_conn, "x", {"a": True}, "conflict")
    for event_type, payload in [("x", {"a": 1}), ("y", {"a": True}), ("x", {"a": False})]:
        with pytest.raises(store.IdempotencyConflict):
            store.fan_out(db_conn, event_type, payload, "conflict")
    assert db_conn.execute("SELECT count(*) AS n FROM events").fetchone()["n"] == 1


def test_concurrent_event_idempotency_same_key(db_url):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from relay import store
    from relay.db import connect

    with connect(db_url) as conn:
        store.create_subscription(conn, "http://example.com/all", ["*"])
    barrier = Barrier(8)

    def produce(_):
        with connect(db_url) as conn:
            barrier.wait(timeout=10)
            return store.fan_out(conn, "x", {"a": 1}, "race-key")

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(produce, range(8)))
    assert len({r["event_id"] for r in results}) == 1
    assert sum(not r["replayed"] for r in results) == 1
    assert all(r["deliveries"] == 1 for r in results)
    with connect(db_url) as conn:
        assert conn.execute("SELECT count(*) AS n FROM deliveries").fetchone()["n"] == 1


def _seed(conn, n=1):
    from relay import store

    sub = store.create_subscription(conn, "http://example.com/all", ["*"])
    for i in range(n):
        store.fan_out(conn, "x", {"i": i})
    conn.commit()
    return sub


def test_concurrent_claims_disjoint(db_url):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from relay import store
    from relay.db import connect

    with connect(db_url) as conn:
        _seed(conn, 20)
    barrier = Barrier(4)

    def claim(i):
        with connect(db_url) as conn:
            barrier.wait(timeout=10)
            return store.claim_due(conn, str(i), 5, 30)

    with ThreadPoolExecutor(max_workers=4) as executor:
        claims = sum(executor.map(claim, range(4)), [])
    assert len(claims) == len({c["id"] for c in claims}) == 20
    assert all(c["attempt_count"] == 1 for c in claims)
    assert all(
        {
            "event_id",
            "subscription_id",
            "lease_token",
            "url",
            "secret",
            "event_type",
            "payload",
            "event_created_at",
            "claimed_at",
        }
        <= c.keys()
        for c in claims
    )


def _expire(conn, delivery_id):
    conn.execute(
        "UPDATE deliveries SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
        (delivery_id,),
    )
    conn.commit()


def test_expired_lease_reclaimed(db_conn):
    from relay import store

    sub = _seed(db_conn)
    first = store.claim_due(db_conn, "a", 1, 30)[0]
    db_conn.commit()
    assert store.claim_due(db_conn, "b", 1, 30) == []
    db_conn.commit()
    # Disabling after fanout must not prevent the already queued delivery.
    store.set_subscription_active(db_conn, sub["id"], False)
    _expire(db_conn, first["id"])
    second = store.claim_due(db_conn, "b", 1, 30)[0]
    assert second["id"] == first["id"]
    assert second["lease_token"] != first["lease_token"]
    assert second["attempt_count"] == 2
    history = store.list_attempts(db_conn, first["id"])
    assert len(history) == 1
    assert history[0]["attempt_no"] == 1
    assert history[0]["error"] == "lease expired; HTTP outcome unknown"
    assert history[0]["worker_id"] == "b"  # Reclaimer records the observation.


def test_stale_completion_rejected(db_conn):
    from relay import store

    _seed(db_conn)
    first = store.claim_due(db_conn, "a", 1, 30)[0]
    _expire(db_conn, first["id"])
    second = store.claim_due(db_conn, "b", 1, 30)[0]
    assert store.record_result(db_conn, first, "success", worker_id="a", http_status=200) is False
    assert store.record_result(db_conn, second, "success", worker_id="b", http_status=200) is True
    row = db_conn.execute("SELECT * FROM deliveries WHERE id = %s", (first["id"],)).fetchone()
    assert row["status"] == "succeeded"
    assert row["lease_token"] is row["lease_expires_at"] is None
    assert row["succeeded_at"] is not None
    attempts = db_conn.execute("SELECT * FROM delivery_attempts ORDER BY id").fetchall()
    assert [(a["attempt_no"], a["outcome"]) for a in attempts] == [
        (1, "stale"),
        (1, "stale"),
        (2, "success"),
    ]


def test_retry_schedules_future_attempt(db_conn):
    from relay import store

    _seed(db_conn)
    claim = store.claim_due(db_conn, "w", 1, 30)[0]
    before = db_conn.execute("SELECT now() AS now").fetchone()["now"]
    assert store.record_result(
        db_conn,
        claim,
        "retry",
        worker_id="w",
        http_status=500,
        error="unavailable",
        retry_delay_s=60,
    )
    row = db_conn.execute("SELECT * FROM deliveries WHERE id = %s", (claim["id"],)).fetchone()
    assert row["status"] == "pending"
    assert row["next_attempt_at"] >= before + __import__("datetime").timedelta(seconds=60)
    assert row["last_error"] == "unavailable"
    assert row["lease_token"] is row["lease_expires_at"] is None
    assert store.claim_due(db_conn, "w", 1, 30) == []


def test_max_attempts_goes_dead(db_conn):
    from relay import store

    _seed(db_conn)
    for attempt in (1, 2):
        claim = store.claim_due(db_conn, "w", 1, 30, max_attempts=2)[0]
        assert claim["attempt_count"] == attempt
        assert store.record_result(
            db_conn, claim, "retry", worker_id="w", http_status=500, retry_delay_s=0, max_attempts=2
        )
    row = db_conn.execute("SELECT * FROM deliveries WHERE id = %s", (claim["id"],)).fetchone()
    assert row["status"] == "dead"
    assert row["attempt_count"] == 2
    assert store.claim_due(db_conn, "w", 1, 30) == []
    attempts = db_conn.execute("SELECT outcome FROM delivery_attempts ORDER BY id").fetchall()
    assert [a["outcome"] for a in attempts] == ["retry", "dead"]


def test_non_retryable_4xx_dead_immediately(db_conn):
    from relay import store

    _seed(db_conn)
    claim = store.claim_due(db_conn, "w", 1, 30)[0]
    assert store.record_result(
        db_conn, claim, "dead", worker_id="w", http_status=400, error="bad request"
    )
    row = db_conn.execute("SELECT * FROM deliveries WHERE id = %s", (claim["id"],)).fetchone()
    assert row["status"] == "dead"
    assert row["attempt_count"] == 1
    assert row["succeeded_at"] is None


def test_410_disables_subscription(db_conn):
    from relay import store

    sub = _seed(db_conn)
    claim = store.claim_due(db_conn, "w", 1, 30)[0]
    assert store.record_result(db_conn, claim, "dead", worker_id="w", http_status=410)
    assert store.get_subscription(db_conn, sub["id"])["active"] is False
    assert store.fan_out(db_conn, "x", {})["deliveries"] == 0


def test_stale_410_no_subscription_or_delivery_side_effects(db_conn):
    from relay import store

    sub = _seed(db_conn)
    first = store.claim_due(db_conn, "a", 1, 30)[0]
    _expire(db_conn, first["id"])
    second = store.claim_due(db_conn, "b", 1, 30)[0]
    before = db_conn.execute("SELECT * FROM deliveries WHERE id = %s", (first["id"],)).fetchone()
    assert store.record_result(db_conn, first, "dead", worker_id="a", http_status=410) is False
    assert store.get_subscription(db_conn, sub["id"])["active"] is True
    after = db_conn.execute("SELECT * FROM deliveries WHERE id = %s", (first["id"],)).fetchone()
    assert before == after
    assert store.record_result(db_conn, second, "success", worker_id="b", http_status=200)


def test_exhausted_crashed_claim_terminalized(db_conn):
    from relay import store

    _seed(db_conn)
    claim = store.claim_due(db_conn, "crashed", 1, 30, max_attempts=1)[0]
    _expire(db_conn, claim["id"])
    assert store.claim_due(db_conn, "reaper", 1, 30, max_attempts=1) == []
    row = db_conn.execute("SELECT * FROM deliveries WHERE id = %s", (claim["id"],)).fetchone()
    assert row["status"] == "dead"
    assert row["attempt_count"] == 1
    assert row["lease_token"] is row["lease_expires_at"] is None
    attempts = db_conn.execute(
        "SELECT * FROM delivery_attempts WHERE delivery_id = %s", (claim["id"],)
    ).fetchall()
    assert len(attempts) == 1
    assert attempts[0]["attempt_no"] == 1
    assert attempts[0]["outcome"] == "dead"
    assert "lease" in attempts[0]["error"]
    assert (
        store.record_result(db_conn, claim, "dead", worker_id="crashed", http_status=410) is False
    )


def test_replay_dead_to_pending(db_conn):
    from relay import store

    _seed(db_conn)
    claim = store.claim_due(db_conn, "w", 1, 30)[0]
    store.record_result(db_conn, claim, "dead", worker_id="w", http_status=400)
    replayed = store.replay(db_conn, claim["id"])
    assert replayed["status"] == "pending"
    assert replayed["attempt_count"] == 0
    assert replayed["lease_token"] is replayed["lease_expires_at"] is None
    assert len(db_conn.execute("SELECT * FROM delivery_attempts").fetchall()) == 1
    assert store.replay(db_conn, -1) is None
    with pytest.raises(store.InvalidTransition):
        store.replay(db_conn, claim["id"])


def test_replay_fences_old_completion_and_preserves_history(db_conn):
    from relay import store

    sub = _seed(db_conn)
    first = store.claim_due(db_conn, "old", 1, 30)[0]
    assert store.record_result(db_conn, first, "success", worker_id="old", http_status=200)
    replayed = store.replay(db_conn, first["id"])
    assert replayed["succeeded_at"] is None
    assert replayed["attempt_count"] == 0
    new = store.claim_due(db_conn, "new", 1, 30)[0]
    before = db_conn.execute("SELECT * FROM deliveries WHERE id = %s", (first["id"],)).fetchone()
    assert store.record_result(db_conn, first, "dead", worker_id="old", http_status=410) is False
    assert (
        before
        == db_conn.execute("SELECT * FROM deliveries WHERE id = %s", (first["id"],)).fetchone()
    )
    assert store.get_subscription(db_conn, sub["id"])["active"] is True
    assert new["lease_token"] != first["lease_token"]
    assert store.record_result(db_conn, new, "success", worker_id="new", http_status=200)
    attempts = db_conn.execute("SELECT outcome FROM delivery_attempts ORDER BY id").fetchall()
    assert [a["outcome"] for a in attempts] == ["success", "stale", "success"]


def test_read_apis_stats_filters_pagination(db_conn):
    from relay import store

    sub = _seed(db_conn, 3)
    claims = store.claim_due(db_conn, "w", 2, 30)
    store.record_result(db_conn, claims[0], "success", worker_id="w", http_status=200)
    store.record_result(db_conn, claims[1], "dead", worker_id="w", http_status=400)
    dead = store.list_deliveries(
        db_conn, status="dead", subscription_id=sub["id"], limit=1, offset=0
    )
    assert (dead["limit"], dead["offset"]) == (1, 0)
    assert [d["id"] for d in dead["items"]] == [claims[1]["id"]]
    assert len(store.list_deliveries(db_conn, limit=1, offset=1)["items"]) == 1
    event = store.get_event(db_conn, claims[0]["event_id"])
    assert event["type"] == "x"
    assert event["payload"] == {"i": 0}
    assert event["deliveries"][0]["status"] == "succeeded"
    assert "secret" not in str(event)
    assert store.get_event(db_conn, "00000000-0000-0000-0000-000000000000") is None
    assert store.list_attempts(db_conn, claims[1]["id"])[0]["outcome"] == "dead"
    stats = store.stats(db_conn)
    assert stats["counts"] == {"pending": 1, "in_flight": 0, "succeeded": 1, "dead": 1}
    assert stats["oldest_pending_age_s"] >= 0


def test_result_atomic_rollback_includes_subscription_disable(db_conn):
    from psycopg.errors import RaiseException

    from relay import store

    sub = _seed(db_conn)
    claim = store.claim_due(db_conn, "w", 1, 30)[0]
    db_conn.execute("""
        CREATE FUNCTION reject_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'history failure'; END; $$;
        CREATE TRIGGER reject_attempt BEFORE INSERT ON delivery_attempts
        FOR EACH ROW EXECUTE FUNCTION reject_attempt();
    """)
    try:
        with pytest.raises(RaiseException, match="history failure"):
            store.record_result(db_conn, claim, "dead", worker_id="w", http_status=410)
        row = db_conn.execute("SELECT * FROM deliveries WHERE id = %s", (claim["id"],)).fetchone()
        assert row["status"] == "in_flight"
        assert row["lease_token"] == claim["lease_token"]
        assert store.get_subscription(db_conn, sub["id"])["active"] is True
        assert db_conn.execute("SELECT count(*) AS n FROM delivery_attempts").fetchone()["n"] == 0
    finally:
        db_conn.execute("DROP TRIGGER reject_attempt ON delivery_attempts")
        db_conn.execute("DROP FUNCTION reject_attempt()")
