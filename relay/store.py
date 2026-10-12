"""Short PostgreSQL transactions for subscriptions, fan-out, and leased deliveries.

Producer idempotency keys are global, not per-subscription or per-event type.
"""

import math
import secrets

from psycopg.types.json import Jsonb


class IdempotencyConflict(ValueError):
    """A global producer key already exists with a different JSON body."""


class InvalidTransition(ValueError):
    """The requested delivery state change is not permitted."""


def _positive_int(name, value):
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _finite_seconds(name, value, *, allow_zero=False):
    if (
        type(value) not in (int, float)
        or not math.isfinite(value)
        or value < 0
        or (not allow_zero and value == 0)
    ):
        raise ValueError(f"{name} must be finite and positive/nonnegative")


_SUBSCRIPTION_PUBLIC = "id, url, event_types, active, created_at"


def create_subscription(conn, url, event_types) -> dict:
    with conn.transaction():
        return conn.execute(
            "INSERT INTO subscriptions (url, event_types, secret) VALUES (%s, %s, %s) "
            "RETURNING id, url, event_types, secret, active, created_at",
            (url, event_types, secrets.token_hex(32)),
        ).fetchone()


def list_subscriptions(conn) -> list[dict]:
    return conn.execute(
        f"SELECT {_SUBSCRIPTION_PUBLIC} FROM subscriptions ORDER BY created_at, id"
    ).fetchall()


def get_subscription(conn, subscription_id) -> dict | None:
    return conn.execute(
        f"SELECT {_SUBSCRIPTION_PUBLIC} FROM subscriptions WHERE id = %s", (subscription_id,)
    ).fetchone()


def set_subscription_active(conn, subscription_id, active) -> dict | None:
    with conn.transaction():
        return conn.execute(
            f"UPDATE subscriptions SET active = %s WHERE id = %s RETURNING {_SUBSCRIPTION_PUBLIC}",
            (active, subscription_id),
        ).fetchone()


def fan_out(conn, event_type: str, payload: dict, idempotency_key: str | None = None) -> dict:
    with conn.transaction():
        event = conn.execute(
            "INSERT INTO events (type, payload, idempotency_key) VALUES (%s, %s, %s) "
            "ON CONFLICT (idempotency_key) DO NOTHING RETURNING id",
            (event_type, Jsonb(payload), idempotency_key),
        ).fetchone()
        if event is None:
            event = conn.execute(
                "SELECT id, (type = %s AND payload = %s) AS same_body "
                "FROM events WHERE idempotency_key = %s",
                (event_type, Jsonb(payload), idempotency_key),
            ).fetchone()
            if not event["same_body"]:
                raise IdempotencyConflict("Idempotency-Key already used with a different body")
            count = conn.execute(
                "SELECT count(*) AS n FROM deliveries WHERE event_id = %s", (event["id"],)
            ).fetchone()["n"]
            return {"event_id": str(event["id"]), "deliveries": count, "replayed": True}
        deliveries = conn.execute(
            "INSERT INTO deliveries (event_id, subscription_id) "
            "SELECT %s, id FROM subscriptions WHERE active "
            "AND (%s = ANY(event_types) OR '*' = ANY(event_types)) RETURNING id",
            (event["id"], event_type),
        ).fetchall()
        return {"event_id": str(event["id"]), "deliveries": len(deliveries), "replayed": False}


def claim_due(
    conn, worker_id: str, batch: int, lease_s: float, max_attempts: int = 15
) -> list[dict]:
    _positive_int("batch", batch)
    _positive_int("max_attempts", max_attempts)
    _finite_seconds("lease_s", lease_s)
    with conn.transaction():
        return conn.execute(
            """
            WITH due AS (
                SELECT id, status, attempt_count, updated_at FROM deliveries
                WHERE (status = 'pending' AND next_attempt_at <= now())
                   OR (status = 'in_flight' AND lease_expires_at <= now())
                ORDER BY next_attempt_at, id
                LIMIT %(batch)s FOR UPDATE SKIP LOCKED
            ), exhausted AS (
                UPDATE deliveries d
                SET status = 'dead', lease_token = NULL, lease_expires_at = NULL,
                    last_error = CASE WHEN due.status = 'in_flight'
                                      THEN 'lease expired at maximum attempts'
                                      ELSE 'maximum attempts exhausted' END,
                    updated_at = now()
                FROM due WHERE d.id = due.id AND due.attempt_count >= %(max_attempts)s
                RETURNING d.id, d.attempt_count, d.last_error,
                          due.status AS prior_status, due.updated_at AS claimed_at
            ), evidence AS (
                INSERT INTO delivery_attempts
                    (delivery_id, attempt_no, started_at, finished_at, error, outcome, worker_id)
                SELECT id, attempt_count, claimed_at, now(), last_error, 'dead', %(worker_id)s
                FROM exhausted WHERE prior_status = 'in_flight'
            ), abandoned AS (
                INSERT INTO delivery_attempts
                    (delivery_id, attempt_no, started_at, finished_at, error, outcome, worker_id)
                SELECT id, attempt_count, updated_at, now(),
                       'lease expired; HTTP outcome unknown', 'stale', %(worker_id)s
                FROM due WHERE status = 'in_flight' AND attempt_count < %(max_attempts)s
            ), claimed AS (
                UPDATE deliveries d
                SET status = 'in_flight', lease_token = gen_random_uuid(),
                    lease_expires_at = now() + make_interval(secs => %(lease_s)s),
                    attempt_count = d.attempt_count + 1, updated_at = now()
                FROM due WHERE d.id = due.id AND due.attempt_count < %(max_attempts)s
                RETURNING d.*
            )
            SELECT c.*, s.url, s.secret, e.type AS event_type, e.payload,
                   e.created_at AS event_created_at, c.updated_at AS claimed_at
            FROM claimed c JOIN subscriptions s ON s.id = c.subscription_id
            JOIN events e ON e.id = c.event_id ORDER BY c.next_attempt_at, c.id
            """,
            {
                "batch": batch,
                "lease_s": lease_s,
                "max_attempts": max_attempts,
                "worker_id": worker_id,
            },
        ).fetchall()


def record_result(
    conn,
    claim: dict,
    outcome: str,
    *,
    worker_id: str,
    http_status: int | None = None,
    error: str | None = None,
    started_at=None,
    finished_at=None,
    retry_delay_s: float = 0,
    max_attempts: int = 15,
) -> bool:
    _positive_int("max_attempts", max_attempts)
    _finite_seconds("retry_delay_s", retry_delay_s, allow_zero=True)
    if outcome not in ("success", "retry", "dead"):
        raise ValueError("outcome must be success, retry, or dead")
    with conn.transaction():
        current = conn.execute(
            "SELECT * FROM deliveries WHERE id = %s FOR UPDATE", (claim["id"],)
        ).fetchone()
        if current is None:
            return False
        accepted = current["status"] == "in_flight" and str(current["lease_token"]) == str(
            claim["lease_token"]
        )
        if accepted:
            if outcome == "retry" and current["attempt_count"] >= max_attempts:
                outcome = "dead"
            status = {"success": "succeeded", "retry": "pending", "dead": "dead"}[outcome]
            conn.execute(
                "UPDATE deliveries SET status = %s, "
                "next_attempt_at = now() + make_interval(secs => %s), "
                "succeeded_at = CASE WHEN %s = 'success' THEN now() ELSE NULL END, "
                "lease_token = NULL, lease_expires_at = NULL, last_error = %s, updated_at = now() "
                "WHERE id = %s AND lease_token = %s AND status = 'in_flight'",
                (
                    status,
                    retry_delay_s if outcome == "retry" else 0,
                    outcome,
                    None if outcome == "success" else error,
                    claim["id"],
                    claim["lease_token"],
                ),
            )
        conn.execute(
            "INSERT INTO delivery_attempts "
            "(delivery_id, attempt_no, started_at, finished_at, "
            "http_status, error, outcome, worker_id) "
            "VALUES (%s, %s, COALESCE(%s, %s, now()), COALESCE(%s, now()), %s, %s, %s, %s)",
            (
                claim["id"],
                claim["attempt_count"],
                started_at,
                claim.get("claimed_at"),
                finished_at,
                http_status,
                error,
                outcome if accepted else "stale",
                worker_id,
            ),
        )
        if accepted and http_status == 410:
            conn.execute(
                "UPDATE subscriptions SET active = false WHERE id = %s",
                (current["subscription_id"],),
            )
        return accepted


def replay(conn, delivery_id) -> dict | None:
    with conn.transaction():
        current = conn.execute(
            "SELECT status FROM deliveries WHERE id = %s FOR UPDATE", (delivery_id,)
        ).fetchone()
        if current is None:
            return None
        if current["status"] not in ("dead", "succeeded"):
            raise InvalidTransition("replay requires a dead or succeeded delivery")
        return conn.execute(
            "UPDATE deliveries SET status = 'pending', attempt_count = 0, next_attempt_at = now(), "
            "lease_token = NULL, lease_expires_at = NULL, succeeded_at = NULL, last_error = NULL, "
            "updated_at = now() WHERE id = %s RETURNING *",
            (delivery_id,),
        ).fetchone()


def get_event(conn, event_id) -> dict | None:
    event = conn.execute("SELECT * FROM events WHERE id = %s", (event_id,)).fetchone()
    if event is not None:
        event["deliveries"] = conn.execute(
            "SELECT * FROM deliveries WHERE event_id = %s ORDER BY id", (event_id,)
        ).fetchall()
    return event


def list_deliveries(conn, status=None, subscription_id=None, limit=100, offset=0) -> dict:
    clauses = []
    params = []
    if status is not None:
        clauses.append("status = %s")
        params.append(status)
    if subscription_id is not None:
        clauses.append("subscription_id = %s")
        params.append(subscription_id)
    query = "SELECT * FROM deliveries"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    items = conn.execute(
        query + " ORDER BY id LIMIT %s OFFSET %s", (*params, limit, offset)
    ).fetchall()
    return {"items": items, "limit": limit, "offset": offset}


def list_attempts(conn, delivery_id) -> list[dict]:
    return conn.execute(
        "SELECT * FROM delivery_attempts WHERE delivery_id = %s ORDER BY id", (delivery_id,)
    ).fetchall()


def stats(conn) -> dict:
    counts = {"pending": 0, "in_flight": 0, "succeeded": 0, "dead": 0}
    for row in conn.execute(
        "SELECT status, count(*) AS n FROM deliveries GROUP BY status"
    ).fetchall():
        counts[row["status"]] = row["n"]
    age = conn.execute(
        "SELECT EXTRACT(EPOCH FROM now() - min(created_at)) AS age "
        "FROM deliveries WHERE status = 'pending'"
    ).fetchone()["age"]
    return {"counts": counts, "oldest_pending_age_s": None if age is None else max(0.0, float(age))}
