# Hybrid scanner configuration

The scanner retains the existing startup menu, durable queues, phase expansion,
target matching, reports and search folders. At Start/Resume confirmation choose
**3 — Backend configuration**, then Worker, Direct or Hybrid. Existing searches
default to Worker. Configuration is saved in that search's `scan_config.json`.
Edit saved JSON only while that search is stopped. Resume retains its identity,
completed jobs, committed pages and target observations.

## Backend settings

| Setting | Default | Meaning |
|---|---:|---|
| `backend_mode` | `worker` | `worker`, `direct`, or dynamic `hybrid` |
| `workers` | existing selected value | Positive integer; no fixed application ceiling |
| `max_connections` | `0` | Aggregate request budget; zero derives from workers |
| `worker_backend_max_connections` | `0` | Worker budget, bounded by aggregate; zero derives from aggregate |
| `direct_backend_max_connections` | `0` | Direct budget, bounded by aggregate; zero derives from aggregate |
| `worker_backend_keepalive_connections` | `0` | Retained Worker connections, distributed across proxy pools; zero derives from budget |
| `direct_backend_keepalive_connections` | `0` | Retained Direct connections, distributed across proxy pools; zero derives from budget |
| `connections_per_proxy` | `0` | Aggregate proxy admission across backends; zero derives from available request capacity |
| `direct_initial_concurrency` | `0` | Initial Direct admission; zero uses its configured ceiling |
| `worker_page_size` | `0` | Server controlled. Nonzero values rejected because no Worker count parameter is confirmed |
| `direct_page_size` | `35` | Independent Direct count parameter; 1–1000 accepted for controlled validation. Largest reliable live value remains unverified |
| `race_failed_requests` | `false` | Race a failed profile lookup through both available backends; first valid response wins; cancel loser |
| `worker_retry_attempts` | `4` | Worker retries in hybrid dispatcher; 1–20 |
| `direct_retry_attempts` | `4` | Direct retries; 1–20 |
| `direct_429_cooldown` | `60.0` | Seconds when HTTP Retry-After is absent; finite, nonnegative |
| `direct_session_file` | empty | Path to a private session JSON; empty uses guest session |
| `avatar_workers` | `0` | Optional image download concurrency; zero derives from request capacity |
| `io_workers` | `0` | Profile persistence threads; zero adapts from one up to CPU count using measured CPU/queued work; explicit positive value overrides |

Worker-only mode preserves the original `retry_attempts` setting and independent
one-extra-404 rule. Hybrid Worker requests use `worker_retry_attempts`. Global
configured request delays still apply, including across proxies and bootstrap.
Zero delay adds no fixed request-per-second throttle.

Direct does not launch FastAPI, Redis, PostgreSQL, an identity scheduler or task
polling. The native signer is vendored unchanged from Evil0ctal commit
`737bf3dfe9de1dbff57990c0ec4c9e02c75c3d0f`; license and attribution are in `vendor`.
Direct profile/list requests use the existing asynchronous transport and stores.

## Private session and proxies

Example session structure (replace values locally with your own session):

```json
{
  "cookies": {"sessionid": "YOUR_SESSION", "msToken": "YOUR_TOKEN"},
  "user_agent": "YOUR_BROWSER_USER_AGENT",
  "device_id": "YOUR_NUMERIC_DEVICE_ID"
}
```

`device_id` and `user_agent` are optional. One stable identity and persistent
cookie jar is retained per Direct proxy pool during the execution. Cookies are
scoped to TikTok and redacted from diagnostics. Store the file outside the
repository; `direct_session.json` is ignored by git. Keep proxy credentials local.
The existing `IP:PORT:USERNAME:PASSWORD` Webshare format and validation menu work
for Direct and Worker. Proxy authentication is confined to CONNECT, never sent
to the HTTPS origin. Each backend/proxy combination reuses a persistent client.

## Routing and pagination

Hybrid assignment uses recent valid records per occupied connection second,
success ratio, risk responses and active load. It is not a permanent 50/50 split.
Direct concurrency reacts to latency spikes, errors, timeouts, risk responses
and declining record yield despite rising attempts. Configuration supplies the
maximum; healthy recovery grows toward it.

Follower and following chains each keep their assigned backend. No cross-backend
cursor compatibility has been established, so failover always restarts that chain
at cursor `0`, retaining and deduplicating committed members and matches. Saved
opaque cursors stay exact within their original backend. Switching a saved Direct
search to Worker also restarts incompatible chains safely. Private, missing or
unobserved fields remain unknown; an HTTP 200 alone is never sufficient evidence.

## HTTP 429 lifecycle

In Hybrid mode the first confirmed Worker 429 closes Worker admission immediately,
cancels its transmitted network tasks, wakes queued continuations and migrates
unfinished work to Direct. Worker receives no retry/probe for this execution.
Direct-only mode never contacts Worker. Worker-only mode preserves the existing
global stop because that configuration does not permit Direct fallback.

A first Direct 429 pauses new Direct traffic and creates one central cooldown
timer. Retry-After takes precedence over `direct_429_cooldown`. State is checkpointed;
waiting continuations hold no network permits. When it expires, exactly one
request probes Direct, then successful requests gradually increase admission.
A further confirmed 429 after that opportunity stops the scan and flushes state,
statistics, logs and reports. This one-opportunity policy also applies when Direct
is selected without a prior Worker failure. Independent non-429 Direct risk/error
responses do not stop a healthy Worker backend.

The lifecycle spans bootstrap and main clients. A later new execution, including
resume, resets the backend lifecycle and may try Worker again. It does not erase
saved work. Required `findtik_*`, `direct_*` and global-stop counters appear in
hybrid/direct statistics, together with admission, signing, parsing, pool, proxy,
failure and record-yield measurements. Worker-only statistics retain their legacy
global 429 fields and backend request counters.

## Explicit resource and correctness limits

| Limit | Enforcement and reason | Configuration / effect of raising |
|---|---|---|
| POSIX open files | `runtime_ceiling`: `(RLIMIT_NOFILE - 128) // 10`, allowing SQLite/WAL, sockets and output files per active profile | Raise the operating-system descriptor limit or lower active budget; actual ceiling is displayed. Windows uses the explicit user budget |
| Aggregate/backend/proxy permits | Request gates and proxy admission; prevent configuration budgets being multiplied by pools | Settings above; higher values increase concurrent network demand |
| Shared SQLite writer | One ordered writer for the shared connection and transactions | Required for ordering/correctness; unrelated profile stores use separate configurable threads |
| Writer queue | Scales from requested workers; bounded pending executor submissions provide backpressure | No rate/timer cap; each caller waits for its critical commit |
| Cursor sequencing | One in-flight page per direction/chain | Required because the next cursor comes from the prior response; other accounts/directions continue |
| Retry budget / 404 / 429 | Bounded error attempts, one extra Worker 404, explicit 429 lifecycle | Retry settings above; no endless cooldown or failure loop |
| Keep-alive expiry | 90 seconds | Idle connection lifetime, not a request rate cap; opening a new connection remains possible |
| Backend metrics | 60 one-second buckets and latest 4096 latency samples | Bounded statistics overhead, not a traffic limit; counters retain execution totals |
| Profile cache | Bounded cache/in-flight coalescing in `HybridClient` | Eviction permits refetch; no cursor pages cached across chains |
| Avatar limits | 2 MiB downloaded, 4 million decoded pixels, 96-pixel thumbnail, allowed CDN URLs | Image safety and offline report size; never truncates account data |
| Report rendering | 20–500 visible rows, 200 reason suggestions, full searchable reason field | DOM bound; all saved rows remain searchable/filterable and exportable |
| Periodic noncritical output | Buffered log writer, 15-second stats snapshots, 300-second console/history updates | Does not hold request permits; final and 429 snapshots flush immediately |

These settings do not establish an upstream allowance or a measured live RPS.
Offline fixture and loopback benchmarks are explicitly labelled in their results.
Live compatibility, the largest reliable page size and live throughput require
the account/environment verification described in `LIVE_VERIFICATION.md`.
