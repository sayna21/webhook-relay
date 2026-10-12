import random

import pytest


def test_backoff_seeded_rng_deterministic():
    from relay.retry import backoff_delay

    first, second = random.Random(17), random.Random(17)
    assert [backoff_delay(a, 10, 3600, first) for a in range(1, 21)] == [
        backoff_delay(a, 10, 3600, second) for a in range(1, 21)
    ]


def test_backoff_full_jitter_exact_bound():
    from relay.retry import backoff_delay

    class EndpointRng:
        def uniform(self, low, high):
            assert low == 0
            return high

    for attempt in range(1, 21):
        assert backoff_delay(attempt, 10, 3600, EndpointRng()) == min(3600, 10 * 2 ** (attempt - 1))
    assert backoff_delay(10_000, 5e-324, 1e308, EndpointRng()) == 1e308


@pytest.mark.parametrize(
    "status,error,expected",
    [
        (200, None, "success"),
        (204, None, "success"),
        (299, None, "success"),
        (408, None, "retry"),
        (429, None, "retry"),
        (500, None, "retry"),
        (503, None, "retry"),
        (599, None, "retry"),
        (None, "timeout", "retry"),
        (None, "connection_refused", "retry"),
        (None, "connection_reset", "retry"),
        (None, "dns", "retry"),
        (410, None, "dead"),
        (400, None, "dead"),
        (401, None, "dead"),
        (404, None, "dead"),
        (499, None, "dead"),
        (300, None, "dead"),
        (301, None, "dead"),
        (307, None, "dead"),
        (199, None, "dead"),
        (600, None, "dead"),
        (None, None, "dead"),
        (None, "unknown", "dead"),
    ],
)
def test_classify_table(status, error, expected):
    from relay.retry import classify

    assert classify(status, error) == expected


@pytest.mark.parametrize(
    "attempt,base,cap",
    [
        (0, 1, 10),
        (-1, 1, 10),
        (1.5, 1, 10),
        (True, 1, 10),
        (1, -1, 10),
        (1, 11, 10),
        (1, 1, -1),
        (1, float("nan"), 10),
        (1, 1, float("inf")),
    ],
)
def test_backoff_rejects_invalid_input(attempt, base, cap):
    from relay.retry import backoff_delay

    with pytest.raises(ValueError):
        backoff_delay(attempt, base, cap, random.Random(0))


def test_backoff_huge_attempt_no_overflow():
    from relay.retry import backoff_delay

    assert 0 <= backoff_delay(10_000, 10.0, 3600.0, random.Random(5)) <= 3600
    assert backoff_delay(10**100, 0.0, 3600.0, random.Random(5)) == 0


def test_backoff_bounds():
    from relay.retry import backoff_delay

    rng = random.Random(42)
    for attempt in range(1, 21):
        delay = backoff_delay(attempt, 10, 3600, rng)
        assert 0 <= delay <= min(3600, 10 * 2 ** (attempt - 1))
