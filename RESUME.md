# Relay — Reliable Webhook Delivery Service

- Implemented transactional event fan-out in PostgreSQL and concurrent webhook workers using SKIP LOCKED leases and fencing tokens to reject stale completions.
- Added HMAC-SHA256 signatures, bounded retries with full jitter, attempt history, and dead-letter replay; passed 258 automated tests with 97% statement coverage in a clean checkout.
- Measured 170.3 successful deliveries/s with 4 workers against a receiver with 25% failures and 5% timeouts; a forced worker-crash run lost 0 of 10,000 deliveries.

Evidence: [benchmark record](results/benchmark-20261011-161927.json), commit `1b2d7f417e7b3d75904fbd8f74625e49cbce36ff`. Matrix figures are medians across 3 repetitions. Windows crash injection used TerminateProcess. Latency includes burst queueing and producer seeding; see README for measured percentiles, hardware and limits. These are local workload measurements, not production capacity claims.

Final correctness checks: [verification record](results/verification-20261011-182243.json), source commit `9d59454`. Verified with native PostgreSQL 18.4; the Docker PostgreSQL 16 runtime remains unverified on this device.
