"""Local-only receiver; management routes must never be exposed publicly."""

import asyncio
import random
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError, model_validator


class ReceiverConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    failure_rate: float = Field(default=0.25, ge=0, le=1)
    timeout_rate: float = Field(default=0.05, ge=0, le=1)
    timeout_s: float = Field(default=3, gt=0)
    latency_min_ms: float = Field(default=5, ge=0)
    latency_max_ms: float = Field(default=30, ge=0)
    rng_seed: int | None = None
    secrets: dict[UUID, Annotated[str, Field(min_length=1)]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def check_ranges(self):
        if self.failure_rate + self.timeout_rate > 1:
            raise ValueError("failure_rate + timeout_rate must be <= 1")
        if self.latency_min_ms > self.latency_max_ms:
            raise ValueError("latency_min_ms must be <= latency_max_ms")
        return self


class EventBody(BaseModel):
    id: UUID
    type: str = Field(min_length=1, validation_alias=AliasChoices("type", "event_type"))
    payload: dict


def create_app(**options) -> FastAPI:
    config = ReceiverConfig(**options)
    rng = random.Random(config.rng_seed)
    application = FastAPI()
    receipts = []
    processed = set()

    @application.post("/configure")
    async def configure(values: dict):
        nonlocal config
        merged = config.model_dump()
        merged.update(values)
        if "secrets" in values and isinstance(values["secrets"], dict):
            merged["secrets"] = {**config.secrets, **values["secrets"]}
        try:
            config = ReceiverConfig.model_validate(merged)
        except ValidationError as exc:
            raise HTTPException(422, "Invalid receiver configuration") from exc
        if "rng_seed" in values:
            rng.seed(config.rng_seed)
        return {
            **config.model_dump(exclude={"secrets"}),
            "known_subscriptions": sorted(str(key) for key in config.secrets),
        }

    @application.post("/reset")
    async def reset():
        receipts.clear()
        processed.clear()
        return {"ok": True}

    @application.get("/receipts")
    def get_receipts():
        return receipts

    @application.post("/webhook/{subscription_id}")
    async def webhook(subscription_id: str, request: Request):
        raw = await request.body()
        signature = request.headers.get("Relay-Signature", "")
        valid = False
        status = 403
        event_id = request.headers.get("Relay-Event-Id")
        delivery_id = None
        attempt = None
        try:
            subscription_id = str(UUID(subscription_id))
            secret = config.secrets.get(UUID(subscription_id))
            if secret is not None:
                event_id = str(UUID(event_id or ""))
                delivery_header = request.headers.get("Relay-Delivery-Id", "")
                attempt_header = request.headers.get("Relay-Attempt", "")
                if not delivery_header.isdecimal() or not attempt_header.isdecimal():
                    raise ValueError("Delivery and attempt must be positive integers")
                delivery_id, attempt = int(delivery_header), int(attempt_header)
                if delivery_id < 1 or attempt < 1:
                    raise ValueError("Delivery and attempt must be positive integers")
                body = EventBody.model_validate_json(raw)
                if str(body.id) != event_id:
                    raise ValueError("Body and header event IDs differ")
                from relay.signing import verify

                valid = verify(secret, signature, raw, now=int(datetime.now(UTC).timestamp()))
                status = 200 if valid else 403
        except (ValueError, ValidationError):
            status = 422
        category = "rejected"
        delay = 0
        if valid:
            draw = rng.random()
            category = (
                "failure"
                if draw < config.failure_rate
                else "timeout"
                if draw < config.failure_rate + config.timeout_rate
                else "success"
            )
            status = 500 if category == "failure" else 200
            delay = rng.uniform(config.latency_min_ms, config.latency_max_ms) / 1000
            if category == "timeout":
                delay += config.timeout_s
        acknowledged = status == 200
        pair = (event_id, subscription_id)
        duplicate = acknowledged and pair in processed
        if acknowledged:
            processed.add(pair)
        receipt = {
            "event_id": event_id,
            "subscription_id": subscription_id,
            "delivery_id": delivery_id,
            "attempt": attempt,
            "http_status": status,
            "acknowledged": acknowledged,
            "signature_valid": valid,
            "category": category,
            "duplicate": duplicate,
            "side_effect_applied": acknowledged and not duplicate,
            "received_at": datetime.now(UTC).isoformat(),
            "raw_body": raw.decode("utf-8", errors="replace"),
            "signature": signature,
            "response_completed": False,
        }
        receipts.append(receipt)
        await asyncio.sleep(delay)
        receipt["response_completed"] = True
        return JSONResponse({"acknowledged": acknowledged}, status_code=status)

    return application


app = create_app()
