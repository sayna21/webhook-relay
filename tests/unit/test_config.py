import pytest


def test_settings_defaults():
    from relay.config import Settings

    s = Settings()
    assert s.database_url == "postgresql://relay@127.0.0.1:55432/relay"
    assert (s.http_timeout_s, s.lease_s, s.concurrency, s.batch, s.poll_ms) == (2, 30, 8, 16, 50)
    assert (s.retry_base_s, s.retry_cap_s, s.max_attempts, s.admin_token) == (10, 3600, 15, "")


def test_settings_environment(monkeypatch):
    from dataclasses import FrozenInstanceError

    import pytest

    from relay.config import Settings

    values = {
        "DATABASE_URL": "postgresql://example/relay",
        "HTTP_TIMEOUT_S": "0.5",
        "LEASE_S": "5",
        "CONCURRENCY": "3",
        "BATCH": "7",
        "POLL_MS": "0",
        "RETRY_BASE_S": "0",
        "RETRY_CAP_S": "12",
        "MAX_ATTEMPTS": "2",
        "ADMIN_TOKEN": "local-token",
    }
    for name, value in values.items():
        monkeypatch.setenv("RELAY_" + name, value)
    s = Settings.from_env()
    assert (s.database_url, s.http_timeout_s, s.lease_s) == ("postgresql://example/relay", 0.5, 5)
    assert (s.concurrency, s.batch, s.poll_ms, s.max_attempts) == (3, 7, 0, 2)
    assert (s.retry_base_s, s.retry_cap_s, s.admin_token) == (0, 12, "local-token")
    with pytest.raises(FrozenInstanceError):
        s.batch = 10


@pytest.mark.parametrize(
    "kwargs",
    [
        {"http_timeout_s": 0},
        {"lease_s": -1},
        {"concurrency": 0},
        {"batch": 0},
        {"max_attempts": 0},
        {"poll_ms": -1},
        {"retry_base_s": -1},
        {"retry_cap_s": -1},
        {"retry_base_s": 11, "retry_cap_s": 10},
        {"lease_s": float("inf")},
        {"http_timeout_s": float("nan")},
        {"retry_cap_s": float("nan")},
        {"concurrency": 1.5},
        {"batch": True},
        {"database_url": ""},
    ],
)
def test_settings_reject_invalid_constructor(kwargs):
    from relay.config import Settings

    with pytest.raises(ValueError):
        Settings(**kwargs)


@pytest.mark.parametrize(
    "name,value",
    [
        ("RELAY_BATCH", "oops"),
        ("RELAY_LEASE_S", "0"),
        ("RELAY_MAX_ATTEMPTS", "1.5"),
        ("RELAY_POLL_MS", "-1"),
    ],
)
def test_settings_invalid_env_names_value(monkeypatch, name, value):
    from relay.config import Settings

    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        Settings.from_env()


def test_positive_subnanosecond_timeout_is_valid():
    from relay.config import Settings

    assert Settings(http_timeout_s=1e-20).http_timeout_s == 1e-20


@pytest.mark.parametrize("kwargs", [{"lease_s": "30"}, {"retry_base_s": True}])
def test_settings_wrong_numeric_type_is_clear_value_error(kwargs):
    from relay.config import Settings

    with pytest.raises(ValueError, match="RELAY_"):
        Settings(**kwargs)
