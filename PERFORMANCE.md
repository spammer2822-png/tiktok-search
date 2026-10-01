# DTK-only performance notes — 1 October 2026

The scanner no longer sends TikTok requests through the old Worker/native-Direct/Hybrid transport stack. DTK is the only platform backend.

## Concurrency model

The scanner may still run many profile jobs concurrently, but local DTK submissions are separately bounded by `dtk_max_connections` (default 64).

This is deliberate. DTK itself schedules TikTok traffic through its identity pool and endpoint token buckets. More scanner workers do not create more upstream capacity once every healthy identity is occupied.

The default identity settings are:

```text
minimum usable TikTok identities = 3
target TikTok identities         = 8
```

The scanner aligns DTK's TikTok pool settings at startup when its API key has sufficient scope and requests guest identity mints when the usable pool is below the minimum. DTK remains responsible for health, cooldowns, circuit breaking and replacement.

## Local HTTP

`dtk_backend.py` keeps one persistent HTTPX client for DTK with keep-alive connections. It does not launch Docker commands per request and it does not reload the API key per request.

Health/startup work happens once when the backend context opens.

## Docker startup

If `/readyz` is already healthy, scanner startup does no Docker work.

Otherwise the scanner:

1. starts Docker Desktop when needed;
2. locates or clones DTK;
3. prepares DTK's local environment;
4. starts the existing Compose stack;
5. builds images only when normal startup cannot use existing images;
6. waits for readiness before scanning.

This avoids rebuilding the browser image on ordinary runs.

## Pagination

Followers and following remain sequential per account because each next cursor is opaque and only known after the previous page. Different profiles can progress concurrently.

Default DTK relationship page size is 35 and remains configurable. The upstream Playground/API caps the count at 50.

## Persistence

The existing performance work outside the removed transport remains:

- SQLite WAL state
- page + cursor atomic commits
- bounded independent disk writers
- compact raw JSON
- incremental reports
- throttled terminal output with full detail in logs
- exact resume without replaying completed pages
- deduplication by stable numeric identity when available

## API-side throttling

DTK can return stable conditions such as:

- `RATE_LIMITED`
- `QUEUE_FULL`
- `IDENTITY_POOL_EXHAUSTED`
- `ENDPOINT_CIRCUIT_OPEN`
- `UPSTREAM_RISK_CONTROL`

The scanner respects retry timing rather than immediately resubmitting thousands of requests. If throughput is low because the identity pool is exhausted, raising Python worker count is not the cure. Humanity has tried shouting at queues before; the queue remains unimpressed.

## Historical measurements

Files under `hybrid_measurements/`, `verification_runs/`, and older performance documents describe earlier Worker/Direct/Hybrid builds. They are preserved for provenance and comparison, not as measurements of the DTK-only runtime.

No live requests-per-second claim is made for this DTK-only build until measured with the integrated scanner on the target machine.
