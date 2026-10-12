import math


def backoff_delay(attempt: int, base_s: float, cap_s: float, rng) -> float:
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ValueError("attempt must be a positive integer")
    if not (math.isfinite(base_s) and math.isfinite(cap_s) and 0 <= base_s <= cap_s):
        raise ValueError("require finite 0 <= base_s <= cap_s")
    if base_s == 0:
        return 0.0
    exponent = attempt - 1
    threshold = math.log2(cap_s) - math.log2(base_s)
    bound = cap_s if exponent >= threshold else min(cap_s, math.ldexp(base_s, exponent))
    return rng.uniform(0, bound)


def classify(status_code: int | None, error_kind: str | None = None) -> str:
    if status_code is None:
        return (
            "retry"
            if error_kind in {"timeout", "network", "connection_refused", "connection_reset", "dns"}
            else "dead"
        )
    if 200 <= status_code < 300:
        return "success"
    if status_code in {408, 429} or 500 <= status_code < 600:
        return "retry"
    return "dead"
