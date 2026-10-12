import hashlib
import hmac
import math
import re


def sign(secret: str, timestamp: int, body: bytes) -> str:
    message = str(timestamp).encode("ascii") + b"." + body
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={digest}"


def verify(secret, header, body, now, tolerance_s=300) -> bool:
    if not (math.isfinite(now) and math.isfinite(tolerance_s) and tolerance_s >= 0):
        return False
    if not isinstance(header, str):
        return False
    match = re.fullmatch(r"t=(0|[1-9][0-9]{0,19}),v1=([0-9a-f]{64})", header)
    if match is None:
        return False
    timestamp = int(match[1])
    if abs(now - timestamp) > tolerance_s:
        return False
    return hmac.compare_digest(sign(secret, timestamp, body), header)
