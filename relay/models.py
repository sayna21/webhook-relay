"""Validated management API request models."""

from urllib.parse import urlsplit

from pydantic import BaseModel, Field, field_validator


class SubscriptionCreate(BaseModel):
    url: str
    event_types: list[str] = Field(min_length=1)

    @field_validator("url")
    @classmethod
    def http_endpoint(cls, value):
        parsed = urlsplit(value)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("url must be an http(s) endpoint")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("url credentials are forbidden")
        _ = parsed.port  # urlsplit validates malformed/out-of-range ports on access.
        return value

    @field_validator("event_types")
    @classmethod
    def nonempty_event_types(cls, value):
        if any(not event_type.strip() for event_type in value):
            raise ValueError("event_types entries must be nonempty")
        return value


class EventCreate(BaseModel):
    type: str = Field(min_length=1)
    payload: dict


class SubscriptionUpdate(BaseModel):
    active: bool
