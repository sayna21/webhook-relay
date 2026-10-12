"""Validated immutable process configuration."""

import math
import os
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql://relay@127.0.0.1:55432/relay"
    http_timeout_s: float = 2.0
    lease_s: float = 30.0
    concurrency: int = 8
    batch: int = 16
    poll_ms: int = 50
    retry_base_s: float = 10.0
    retry_cap_s: float = 3600.0
    max_attempts: int = 15
    admin_token: str = ""

    def __post_init__(self):
        for name in ("http_timeout_s", "lease_s", "retry_base_s", "retry_cap_s"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError(f"RELAY_{name.upper()}: must be a finite number")
            invalid_sign = value < 0 if name.startswith("retry_") else value <= 0
            if invalid_sign:
                raise ValueError(f"RELAY_{name.upper()}: must be finite and positive/nonnegative")
        for name in ("concurrency", "batch", "max_attempts", "poll_ms"):
            value = getattr(self, name)
            minimum = 0 if name == "poll_ms" else 1
            if type(value) is not int or value < minimum:
                raise ValueError(f"RELAY_{name.upper()}: must be an integer >= {minimum}")
        if self.retry_base_s > self.retry_cap_s:
            raise ValueError("RELAY_RETRY_BASE_S must be <= RELAY_RETRY_CAP_S")
        if not isinstance(self.database_url, str) or not self.database_url.strip():
            raise ValueError("RELAY_DATABASE_URL must be nonempty")

    @classmethod
    def from_env(cls):
        values = {}
        defaults = cls()
        for field in fields(cls):
            name = "RELAY_" + field.name.upper()
            if name in os.environ:
                cast = type(getattr(defaults, field.name))
                try:
                    values[field.name] = cast(os.environ[name])
                except ValueError as exc:
                    raise ValueError(f"{name}: invalid value") from exc
        return cls(**values)
