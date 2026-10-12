"""A session owns its database; each storage test starts with empty tables."""

import pytest

from bench.harness import fresh_database
from relay.db import connect


@pytest.fixture(scope="session")
def session_db_url():
    with fresh_database() as dsn:
        yield dsn


@pytest.fixture
def db_url(session_db_url):
    with connect(session_db_url) as conn:
        conn.execute(
            "TRUNCATE delivery_attempts, deliveries, events, subscriptions RESTART IDENTITY CASCADE"
        )
    return session_db_url


@pytest.fixture
def db_conn(db_url):
    with connect(db_url) as conn:
        yield conn
