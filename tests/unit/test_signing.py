import hashlib
import hmac

import pytest


def test_verify_tampered_body():
    from relay.signing import sign, verify

    assert not verify("secret", sign("secret", 1, b'{"a":1}'), b'{"a": 1}', 1)


def test_verify_wrong_secret():
    from relay.signing import sign, verify

    assert not verify("wrong", sign("secret", 1, b"body"), b"body", 1)


def test_verify_uses_compare_digest(monkeypatch):
    from relay.signing import sign, verify

    calls = []
    real_compare = hmac.compare_digest

    def compare(left, right):
        calls.append((left, right))
        return real_compare(left, right)

    monkeypatch.setattr(hmac, "compare_digest", compare)
    header = sign("secret", 1, b"body")
    assert verify("secret", header, b"body", 1)
    assert len(calls) == 1
    assert not verify("secret", header, b"body", 301.001)
    assert verify("secret", header, b"body", 301)
    assert verify("secret", header, b"body", 1, tolerance_s=0)


@pytest.mark.parametrize(
    "now,tolerance", [(float("nan"), 300), (1, float("nan")), (1, float("inf")), (1, -1)]
)
def test_verify_invalid_clock_or_tolerance_fails_closed(now, tolerance):
    from relay.signing import sign, verify

    assert verify("secret", sign("secret", 1, b"body"), b"body", now, tolerance) is False


@pytest.mark.parametrize(
    "header",
    [
        "",
        None,
        "t=1",
        "v1=" + "a" * 64,
        "t=no,v1=" + "a" * 64,
        "t=1, v1=" + "a" * 64,
        " t=1,v1=" + "a" * 64,
        "t=1,v1=" + "a" * 64 + " ",
        "t=1,v1=" + "g" * 64,
        "t=1,v1=a",
        "t=1,v1=" + "a" * 64 + ",v1=" + "a" * 64,
        "t=+1,v1=" + "a" * 64,
        "t=01,v1=" + "a" * 64,
        "t=-1,v1=" + "a" * 64,
        "t=1,v1=" + "A" * 64,
        "t=1,v1=" + "a" * 64 + "\n",
        "t=" + "9" * 5000 + ",v1=" + "a" * 64,
    ],
)
def test_verify_malformed_headers(header):
    from relay.signing import verify

    assert verify("secret", header, b"body", 1) is False


@pytest.mark.parametrize("delta,expected", [(-301, False), (-300, True), (300, True), (301, False)])
def test_verify_expired_and_future_timestamp(delta, expected):
    from relay.signing import sign, verify

    now = 1_700_000_000
    assert verify("secret", sign("secret", now + delta, b"body"), b"body", now) is expected


def test_sign_verify_roundtrip():
    from relay.signing import sign, verify

    body = b'{"value": "raw  spacing", "x": 1}'
    timestamp = 1_700_000_000
    digest = hmac.new(b"secret", str(timestamp).encode() + b"." + body, hashlib.sha256).hexdigest()
    header = sign("secret", timestamp, body)
    assert header == f"t={timestamp},v1={digest}"
    assert verify("secret", header, body, now=timestamp)
