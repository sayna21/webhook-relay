import pytest
from pydantic import ValidationError


def test_subscription_create_validation():
    from relay.models import SubscriptionCreate

    value = SubscriptionCreate(url="http://127.0.0.1:8102/webhook", event_types=["*"])
    assert str(value.url) == "http://127.0.0.1:8102/webhook"
    assert value.event_types == ["*"]
    for url, types in [
        ("ftp://example.com", ["x"]),
        ("http://example.com", []),
        ("http://user:pass@example.com", ["x"]),
        ("http://", ["x"]),
    ]:
        with pytest.raises(ValidationError):
            SubscriptionCreate(url=url, event_types=types)


def test_event_create_and_active_update():
    from relay.models import EventCreate, SubscriptionUpdate

    assert EventCreate(type="vehicle.updated", payload={"id": 1}).model_dump() == {
        "type": "vehicle.updated",
        "payload": {"id": 1},
    }
    assert SubscriptionUpdate(active=False).active is False
    with pytest.raises(ValidationError):
        EventCreate(type="", payload={})
    with pytest.raises(ValidationError):
        EventCreate(type="x", payload=[1])


def test_db_connection_and_pool_dict_rows(monkeypatch):
    import psycopg
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool

    from relay import db

    observed = []
    sentinel = object()

    def connect(dsn, **kwargs):
        observed.append((dsn, kwargs))
        return sentinel

    monkeypatch.setenv("RELAY_DATABASE_URL", "postgresql://example/relay")
    monkeypatch.setattr(psycopg, "connect", connect)
    assert db.connect() is sentinel
    assert observed == [("postgresql://example/relay", {"row_factory": dict_row})]

    def unopened_pool(*args, **kwargs):
        return ConnectionPool(*args, **kwargs, open=False)

    monkeypatch.setattr(db, "ConnectionPool", unopened_pool)
    pool = db.make_pool("postgresql://example/relay", max_size=3)
    assert pool.min_size == 1
    assert pool.max_size == 3
    assert pool.kwargs["row_factory"] is dict_row
    pool.close()


def test_migration_schema_matches_plan():
    from pathlib import Path

    sql = (Path(__file__).resolve().parents[2] / "migrations" / "001_init.sql").read_text()
    for table in ("subscriptions", "events", "deliveries", "delivery_attempts"):
        assert "CREATE TABLE " + table in sql
    assert "CREATE TYPE delivery_status AS ENUM ('pending','in_flight','succeeded','dead')" in sql
    assert "UNIQUE (event_id, subscription_id)" in sql
    assert "idempotency_key text UNIQUE" in sql
    assert "CREATE INDEX deliveries_due" in sql
    assert "CREATE INDEX deliveries_leased" in sql
    assert "lease_token uuid" in sql
    assert "succeeded_at timestamptz" in sql


def test_migration_orchestration_skips_applied_versions(monkeypatch):
    from contextlib import contextmanager

    from relay import db

    executed = []
    applied = set()

    class Connection:
        @contextmanager
        def transaction(self):
            executed.append("BEGIN")
            yield
            executed.append("COMMIT")

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            pass

        def execute(self, sql, params=None):
            executed.append(sql)
            if sql.startswith("INSERT INTO schema_migrations"):
                applied.add(params[0])
            self.rows = (
                [{"version": v} for v in applied] if sql.startswith("SELECT version") else []
            )
            return self

        def fetchall(self):
            return self.rows

    monkeypatch.setattr(db, "connect", lambda dsn: Connection())
    db.apply_migrations("postgresql://example/relay")
    db.apply_migrations("postgresql://example/relay")
    assert applied == {"001_init.sql"}
    assert sum("CREATE TABLE subscriptions" in sql for sql in executed) == 1
    assert executed.count("BEGIN") == executed.count("COMMIT") == 2
    assert sum("pg_advisory_xact_lock" in sql for sql in executed) == 2


class ScriptedConnection:
    """Unit-only boundary spy; does NOT substitute for real integration tests."""

    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = []
        self.transactions = 0

    def transaction(self):
        from contextlib import contextmanager

        @contextmanager
        def scope():
            self.transactions += 1
            yield

        return scope()

    def execute(self, query, params=None):
        self.calls.append((query, params))
        result = self.rows.pop(0)

        class Cursor:
            def fetchone(self):
                return result

            def fetchall(self):
                return result

        return Cursor()


def test_subscription_boundary_parameterization_and_secret_generation():
    from relay import store

    conn = ScriptedConnection([{"id": "id1"}, [], None, {"id": "id1", "active": False}])
    url = "http://example.com/'; DROP TABLE events; --"
    assert store.create_subscription(conn, url, ["x"])["id"] == "id1"
    assert conn.transactions == 1
    query, args = conn.calls[0]
    assert url not in query
    assert args[:2] == (url, ["x"])
    assert len(args[2]) >= 32
    assert store.list_subscriptions(conn) == []
    assert store.get_subscription(conn, "id1") is None
    assert store.set_subscription_active(conn, "id1", False) == {"id": "id1", "active": False}
    for query, _args in conn.calls[1:]:
        assert "secret" not in query
    assert conn.transactions == 2


def test_fanout_transaction_boundary():
    from psycopg.types.json import Jsonb

    from relay import store

    conn = ScriptedConnection([{"id": "e1"}, [{"id": 1}, {"id": 2}]])
    result = store.fan_out(conn, "x'--", {"value": 1})
    assert result == {"event_id": "e1", "deliveries": 2, "replayed": False}
    assert conn.transactions == 1
    assert "x'--" not in conn.calls[0][0]
    assert isinstance(conn.calls[0][1][1], Jsonb)
    query = conn.calls[1][0]
    assert "active" in query and "event_types" in query


def test_idempotency_conflict_path_reloads_committed_event():
    from relay import store

    conn = ScriptedConnection([None, {"id": "e1", "same_body": True}, {"n": 3}])
    assert store.fan_out(conn, "x", {"a": 1}, "same-key") == {
        "event_id": "e1",
        "deliveries": 3,
        "replayed": True,
    }
    assert conn.transactions == 1
    assert "ON CONFLICT (idempotency_key) DO NOTHING" in conn.calls[0][0]
    assert "WHERE idempotency_key = %s" in conn.calls[1][0]


def test_idempotency_conflicting_body_rejected_before_fanout():
    from relay import store

    conn = ScriptedConnection([None, {"id": "e1", "same_body": False}])
    with pytest.raises(ValueError, match="Idempotency"):
        store.fan_out(conn, "x", {"a": 2}, "same-key")
    assert len(conn.calls) == 2


def test_claim_atomic_cte_and_join_boundary():
    from relay import store

    claim = {"id": 1, "lease_token": "token1"}
    conn = ScriptedConnection([[claim]])
    assert store.claim_due(conn, "worker1", 4, 30) == [claim]
    assert conn.transactions == 1
    query, params = conn.calls[0]
    assert "FOR UPDATE SKIP LOCKED" in query
    assert "WITH due AS" in query
    assert "attempt_count = d.attempt_count + 1" in query
    assert "gen_random_uuid()" in query
    assert "s.secret" in query and "e.payload" in query
    assert params["batch"] == 4 and params["lease_s"] == 30


def test_result_locks_and_fences_before_writing():
    from relay import store

    claim = {"id": 1, "lease_token": "t1", "attempt_count": 1, "subscription_id": "s1"}
    conn = ScriptedConnection([{"id": 1, "lease_token": "t1", "status": "in_flight"}, None, None])
    assert store.record_result(conn, claim, "success", worker_id="w1", http_status=200) is True
    assert conn.transactions == 1
    assert "FOR UPDATE" in conn.calls[0][0]
    assert "succeeded_at" in conn.calls[1][0]
    assert "INSERT INTO delivery_attempts" in conn.calls[2][0]

    stale = ScriptedConnection([{"id": 1, "lease_token": "t2", "status": "in_flight"}, None])
    assert store.record_result(stale, claim, "dead", worker_id="w1", http_status=410) is False
    assert len(stale.calls) == 2
    assert "INSERT INTO delivery_attempts" in stale.calls[1][0]
    assert "stale" in stale.calls[1][1]


def test_retry_result_scheduling_boundary():
    from relay import store

    claim = {"id": 1, "lease_token": "t1", "attempt_count": 1}
    conn = ScriptedConnection(
        [{"id": 1, "lease_token": "t1", "status": "in_flight", "attempt_count": 1}, None, None]
    )
    assert store.record_result(conn, claim, "retry", worker_id="w", retry_delay_s=60)
    query, args = conn.calls[1]
    assert "next_attempt_at = now() + make_interval" in query
    assert "pending" in args
    assert 60 in args


@pytest.mark.parametrize("outcome,count,max_attempts", [("retry", 2, 2), ("dead", 1, 15)])
def test_dead_result_boundary(outcome, count, max_attempts):
    from relay import store

    claim = {"id": 1, "lease_token": "t1", "attempt_count": count}
    conn = ScriptedConnection(
        [{"id": 1, "lease_token": "t1", "status": "in_flight", "attempt_count": count}, None, None]
    )
    assert store.record_result(conn, claim, outcome, worker_id="w", max_attempts=max_attempts)
    query, args = conn.calls[1]
    assert "dead" in args
    assert "dead" in conn.calls[2][1]


def test_current_410_disable_is_same_transaction():
    from relay import store

    claim = {"id": 1, "lease_token": "t1", "attempt_count": 1, "subscription_id": "s1"}
    conn = ScriptedConnection(
        [
            {"id": 1, "lease_token": "t1", "status": "in_flight", "subscription_id": "s1"},
            None,
            None,
            None,
        ]
    )
    assert store.record_result(conn, claim, "dead", worker_id="w", http_status=410)
    assert conn.transactions == 1
    assert any("UPDATE subscriptions SET active = false" in q for q, _ in conn.calls)


def test_claim_exhaustion_evidence_is_atomic():
    from relay import store

    conn = ScriptedConnection([[]])
    assert store.claim_due(conn, "reaper", 1, 30, max_attempts=1) == []
    query, params = conn.calls[0]
    assert "exhausted AS" in query
    assert "INSERT INTO delivery_attempts" in query
    assert "attempt_count < %(max_attempts)s" in query
    assert params["max_attempts"] == 1
    assert params["worker_id"] == "reaper"


def test_replay_locked_transition_and_lease_invalidation():
    from relay import store

    conn = ScriptedConnection([{"status": "dead"}, {"id": 1, "status": "pending"}])
    assert store.replay(conn, 1) == {"id": 1, "status": "pending"}
    assert conn.transactions == 1
    assert "FOR UPDATE" in conn.calls[0][0]
    query = conn.calls[1][0]
    assert "attempt_count = 0" in query and "lease_token = NULL" in query
    assert "succeeded_at = NULL" in query
    with pytest.raises(ValueError, match="replay"):
        store.replay(ScriptedConnection([{"status": "in_flight"}]), 1)
    assert store.replay(ScriptedConnection([None]), -1) is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"batch": 0},
        {"batch": -1},
        {"batch": 1.5},
        {"lease_s": 0},
        {"lease_s": float("inf")},
        {"max_attempts": 0},
    ],
)
def test_claim_invalid_parameters_do_not_touch_connection(kwargs):
    from relay import store

    values = {"batch": 1, "lease_s": 30, "max_attempts": 15, **kwargs}
    conn = ScriptedConnection([])
    with pytest.raises(ValueError):
        store.claim_due(conn, "w", **values)
    assert conn.calls == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {"outcome": "invalid"},
        {"retry_delay_s": -1},
        {"retry_delay_s": float("nan")},
        {"max_attempts": 0},
    ],
)
def test_result_invalid_parameters_do_not_touch_connection(kwargs):
    from relay import store

    values = {"outcome": "retry", **kwargs}
    conn = ScriptedConnection(
        [{"id": 1, "status": "in_flight", "lease_token": "t1", "attempt_count": 1}, None, None]
    )
    with pytest.raises(ValueError):
        store.record_result(
            conn, {"id": 1, "lease_token": "t1", "attempt_count": 1}, worker_id="w", **values
        )
    assert conn.calls == []


def test_subscription_model_rejects_invalid_port_and_blank_event_type():
    from relay.models import SubscriptionCreate

    for url, types in [
        ("http://example.com:bad", ["x"]),
        ("https://example.com:99999", ["x"]),
        ("http://example.com", [""]),
    ]:
        with pytest.raises(ValidationError):
            SubscriptionCreate(url=url, event_types=types)


def test_get_event_boundary_contains_delivery_statuses_not_secrets():
    from relay import store

    conn = ScriptedConnection(
        [{"id": "e1", "type": "x", "payload": {}}, [{"id": 1, "status": "dead"}], None]
    )
    assert store.get_event(conn, "e1")["deliveries"] == [{"id": 1, "status": "dead"}]
    assert store.get_event(conn, "missing") is None
    assert all("secret" not in query for query, _ in conn.calls)


def test_list_deliveries_boundary_filters_pagination_not_secrets():
    from relay import store

    conn = ScriptedConnection([[{"id": 1}]])
    assert store.list_deliveries(conn, status="dead", subscription_id="s1", limit=1, offset=2) == {
        "items": [{"id": 1}],
        "limit": 1,
        "offset": 2,
    }
    query, params = conn.calls[0]
    assert "secret" not in query
    assert "status = %s" in query and "subscription_id = %s" in query
    assert params == ("dead", "s1", 1, 2)


def test_list_attempts_boundary_keeps_history_order_not_secrets():
    from relay import store

    conn = ScriptedConnection([[{"outcome": "dead"}]])
    assert store.list_attempts(conn, 1) == [{"outcome": "dead"}]
    query, params = conn.calls[0]
    assert "ORDER BY id" in query and "WHERE delivery_id = %s" in query
    assert "secret" not in query
    assert params == (1,)


def test_stats_boundary_includes_zero_counts_empty_pending_age():
    from relay import store

    conn = ScriptedConnection([[{"status": "dead", "n": 1}], {"age": None}])
    assert store.stats(conn) == {
        "counts": {"pending": 0, "in_flight": 0, "succeeded": 0, "dead": 1},
        "oldest_pending_age_s": None,
    }
    assert all("secret" not in query for query, _ in conn.calls)
