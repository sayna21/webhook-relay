"""One synchronous, signed delivery attempt; no database transaction here."""

import json
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

from relay.config import Settings
from relay.retry import backoff_delay, classify
from relay.signing import sign


@dataclass(frozen=True)
class AttemptResult:
    outcome: str
    http_status: int | None
    error: str | None
    started_at: datetime
    finished_at: datetime
    retry_delay_s: float


def send_delivery(claim: dict, settings: Settings, client=None, rng=None) -> AttemptResult:
    if client is None:
        with httpx.Client(
            timeout=settings.http_timeout_s, follow_redirects=False, trust_env=False
        ) as owned_client:
            return send_delivery(claim, settings, owned_client, rng)
    created_at = claim["event_created_at"]
    body = json.dumps(
        {
            "id": str(claim["event_id"]),
            "type": claim["event_type"],
            "payload": claim["payload"],
            "created_at": created_at.astimezone(UTC).isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    started_at = datetime.now(UTC)
    start_tick = time.monotonic()
    headers = {
        "Content-Type": "application/json",
        "Relay-Event-Id": str(claim["event_id"]),
        "Relay-Delivery-Id": str(claim["id"]),
        "Relay-Attempt": str(claim["attempt_count"]),
        "Relay-Signature": sign(claim["secret"], int(time.time()), body),
    }
    status = None
    error = None
    try:
        response = client.post(
            claim["url"],
            content=body,
            headers=headers,
            follow_redirects=False,
            timeout=settings.http_timeout_s,
        )
        status = response.status_code
    except httpx.TimeoutException:
        error = "timeout"
    except httpx.TransportError:
        error = "network"
    outcome = classify(status, error)
    delay = (
        backoff_delay(
            claim["attempt_count"],
            settings.retry_base_s,
            settings.retry_cap_s,
            rng if rng is not None else random,
        )
        if outcome == "retry"
        else 0.0
    )
    finished_at = started_at + timedelta(seconds=time.monotonic() - start_tick)
    return AttemptResult(outcome, status, error, started_at, finished_at, delay)
