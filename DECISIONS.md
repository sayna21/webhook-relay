# Design decisions

## PostgreSQL as the durable queue
Use a single transactional database for events and deliveries. A broker would add an independently operated service and a dual-write problem. Event insertion and subscriber fan-out commit together. The trade-off is database write load and a single availability boundary.

## SKIP LOCKED claims
Claim rows in a short transaction using `FOR UPDATE SKIP LOCKED`, then release locks before HTTP. SELECT followed by UPDATE races; holding locks while sending ties database connections to receiver latency. Skipping locked rows permits concurrent claimers but does not preserve delivery order.

## Leases and fencing
Every claim replaces a UUID token and sets an expiry. Completion locks the row and accepts only its current token and in-flight state. Expiry makes abandoned work eligible again; the token prevents a late sender from overwriting a newer claim. A slow request can still cause duplicate HTTP side effects. Workers claim only enough rows for free executor slots. The reclaimer records a stale observation for an abandoned attempt with unknown HTTP outcome; its worker ID identifies the observer. A late original result can add another stale observation for the same attempt. Benchmark attempt totals therefore sum delivery claim counters rather than audit rows.

## At-least-once semantics
A receiver may acknowledge just before the sender crashes. Retrying after lease expiry is necessary to avoid loss, so receivers deduplicate by event ID within each subscription. An exhausted attempt budget can end in the dead-letter queue; at-least-once is an attempt guarantee, not a promise that a permanently broken receiver succeeds.

## Retry classification
Retry timeouts, transport errors, 408, 429 and server errors. Other client errors and redirects terminate immediately because resending unchanged input does not repair them. A current fenced 410 result also disables future fan-out to the subscription. Already-created delivery rows remain eligible; disabling affects future events. Redirect following is disabled.

## Full jitter and bounded attempts
Delay is uniform from zero to the capped exponential bound. Logarithmic comparison avoids overflowing on huge attempt counts. Production uses base ten seconds, cap one hour and fifteen attempts. There are fourteen waits before the final attempt: the maximum scheduled waiting time is 23,110 seconds (6 h 25 min 10 s), excluding request time, polling and outages. Mean scheduled waiting time is half that. Crash reclamation consumes an attempt; exhausted abandoned claims become dead with audit evidence.

## Synchronous thread workers
A thread pool with a shared HTTP client is easy to trace and fits network-bound work. Each result owns its database connection and transaction. Worker concurrency bounds sockets and claim capacity. SIGTERM/SIGINT stop new claims and drain active tasks; Windows also supports a stop file because its process termination API is abrupt. Unhandled task errors fail the worker, leaving leases recoverable.

## Producer idempotency
Keys are global. A unique database constraint arbitrates concurrent submissions; the losing insert reads the committed original and compares type plus JSONB payload. Equal bodies return the original event without new fan-out. This is simpler than tenant-scoped keys but unsuitable for a multi-tenant public API without additional scoping.

## Signed raw bytes
Sign the exact serialized body with a timestamp using HMAC-SHA256. Strict header parsing and constant-time comparison reject malformed and tampered input. The time window limits old captures but permits replays inside that window; durable receiver deduplication is still needed. Subscription secrets are exposed only on creation and remain plaintext in the local database; production needs encryption and rotation.

## Local operator boundary
An optional static Bearer token guards management routes; health remains public. Bind services to loopback by default. URL validation rejects invalid schemes and credentials but does not block private networks, DNS rebinding or SSRF. Public hosting requires outbound network policy, proper identity/authorization and request-size limits.

## Measurement boundaries
Benchmarks use isolated databases and real HTTP subprocesses. Database timestamps measure event creation to successful completion, including time spent seeding before worker startup. Throughput excludes producer seeding and includes worker startup. Timeout receipts record receiver side effects even when the sender cannot observe the response. Dead and acknowledged pair sets form the loss accounting boundary; reported acknowledged pairs may overlap dead pairs.
