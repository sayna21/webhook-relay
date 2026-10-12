"""Management API. URL validation alone does not provide SSRF protection."""

import hmac
import json
import math
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.exceptions import RequestValidationError
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    StrictBool,
    StringConstraints,
    field_validator,
)

from relay import store
from relay.config import Settings
from relay.db import connect

EventType = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=255)]
DeliveryStatus = Literal["pending", "in_flight", "succeeded", "dead"]
DeliveryId = Annotated[int, Field(gt=0, le=9223372036854775807)]


def _validate_storage_json(value):
    """Reject values PostgreSQL text/JSONB and the signed JSON body cannot represent."""
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            if "\x00" in item or any(0xD800 <= ord(char) <= 0xDFFF for char in item):
                raise HTTPException(422, "Strings must contain valid Unicode without NUL")
        elif isinstance(item, float) and not math.isfinite(item):
            raise HTTPException(422, "JSON numbers must be finite")
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
    return value


class SubscriptionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    url: HttpUrl
    event_types: list[EventType] = Field(min_length=1)

    @field_validator("event_types")
    @classmethod
    def storage_event_types(cls, value):
        return _validate_storage_json(value)

    @field_validator("url")
    @classmethod
    def no_credentials(cls, url):
        if url.username is not None or url.password is not None:
            raise ValueError("URL credentials are not allowed")
        return url


class SubscriptionPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    active: StrictBool


class EventCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    type: EventType
    payload: dict[str, Any]

    @field_validator("type", "payload")
    @classmethod
    def storage_values(cls, value):
        return _validate_storage_json(value)


def _found(value):
    if value is None:
        raise HTTPException(404, "Not found")
    return value


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    application = FastAPI(title="Relay")
    application.state.settings = settings

    @application.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        # Invalid inputs may include infinity or unpaired surrogates; never echo them.
        detail = [{key: error[key] for key in ("type", "loc", "msg")} for error in exc.errors()]
        return Response(
            content=json.dumps({"detail": detail}, ensure_ascii=True, allow_nan=False),
            status_code=422,
            media_type="application/json",
        )

    def authenticate(authorization: Annotated[str | None, Header()] = None):
        if not settings.admin_token:
            return
        expected = f"Bearer {settings.admin_token}".encode()
        supplied = (authorization or "").encode()
        if not hmac.compare_digest(supplied, expected):
            raise HTTPException(401, "Unauthorized", headers={"WWW-Authenticate": "Bearer"})

    routes = APIRouter(dependencies=[Depends(authenticate)])

    @application.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @routes.post("/subscriptions", status_code=201)
    def create_subscription(body: SubscriptionCreate):
        with connect(settings.database_url) as conn:
            result = store.create_subscription(conn, str(body.url), body.event_types)
        return {"id": result["id"], "secret": result["secret"]}

    @routes.get("/subscriptions")
    def list_subscriptions():
        with connect(settings.database_url) as conn:
            return store.list_subscriptions(conn)

    @routes.get("/subscriptions/{subscription_id}")
    def get_subscription(subscription_id: UUID):
        with connect(settings.database_url) as conn:
            return _found(store.get_subscription(conn, subscription_id))

    @routes.patch("/subscriptions/{subscription_id}")
    def set_subscription_active(subscription_id: UUID, body: SubscriptionPatch):
        with connect(settings.database_url) as conn:
            return _found(store.set_subscription_active(conn, subscription_id, body.active))

    @routes.post("/events", status_code=202)
    def create_event(
        body: EventCreate,
        response: Response,
        idempotency_key: Annotated[str | None, Header(min_length=1, max_length=255)] = None,
    ):
        if idempotency_key is not None and not idempotency_key.strip():
            raise HTTPException(422, "Idempotency-Key must not be blank")
        _validate_storage_json(idempotency_key)
        try:
            with connect(settings.database_url) as conn:
                result = store.fan_out(conn, body.type, body.payload, idempotency_key)
        except store.IdempotencyConflict as exc:
            raise HTTPException(422, "Idempotency-Key already used with a different body") from exc
        response.status_code = 200 if result["replayed"] else 202
        return {"event_id": result["event_id"], "deliveries": result["deliveries"]}

    @routes.get("/events/{event_id}")
    def get_event(event_id: UUID):
        with connect(settings.database_url) as conn:
            return _found(store.get_event(conn, event_id))

    @routes.get("/deliveries")
    def list_deliveries(
        status: DeliveryStatus | None = None,
        subscription_id: UUID | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        offset: Annotated[int, Query(ge=0, le=9223372036854775807)] = 0,
    ):
        with connect(settings.database_url) as conn:
            return store.list_deliveries(conn, status, subscription_id, limit, offset)

    @routes.get("/deliveries/{delivery_id}/attempts")
    def list_attempts(delivery_id: DeliveryId):
        with connect(settings.database_url) as conn:
            _found(
                conn.execute("SELECT id FROM deliveries WHERE id = %s", (delivery_id,)).fetchone()
            )
            return store.list_attempts(conn, delivery_id)

    @routes.post("/deliveries/{delivery_id}/replay")
    def replay(delivery_id: DeliveryId):
        try:
            with connect(settings.database_url) as conn:
                return _found(store.replay(conn, delivery_id))
        except store.InvalidTransition as exc:
            raise HTTPException(409, "Replay requires a dead or succeeded delivery") from exc

    @routes.get("/stats")
    def stats():
        with connect(settings.database_url) as conn:
            return store.stats(conn)

    application.include_router(routes)
    return application


app = create_app()
