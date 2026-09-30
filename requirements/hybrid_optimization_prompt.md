# Hybrid TikTok Scanner Backend + Full Performance Optimization

Modify my existing TikTok scanner so it supports a **hybrid backend architecture** using BOTH:

1. My current Worker API backend:

```text
https://tiktokinfov10.itfreeforever.workers.dev
```

2. A direct TikTok backend based on:

```text
https://github.com/Evil0ctal/Douyin_TikTok_Download_API
```

The goal is to use both backends together to maximize:

```text
successful requests/sec
successful accounts/sec
followers/sec
following/sec
successful TikTok records/sec
```

Do NOT replace my existing scanner architecture.

Keep all existing functionality such as:

```text
async workers
resume
SQLite/state persistence
deduplication
retry failed profiles
target matching
statistics
HTML reports
logging
global 429 handling
proxy support
graceful shutdown
```

The hybrid backend should be added underneath the existing scanner.

---

# Architecture

Implement:

```text
TikTok Scanner
      │
      ▼
Global Async Scheduler
      │
      ▼
Hybrid Backend Dispatcher
      │
      ├───────────────► Existing Worker Backend
      │
      └───────────────► Direct TikTok Backend
                           using selected Evil0ctal code
      │
      ▼
Unified Normalized Response
      │
      ▼
Existing scanner/database/report logic
```

---

# Existing Worker Backend

Keep support for:

```text
GET /?username=<username>

POST /api/followers?Uid=<uid>&minCursor=<cursor>

POST /api/following?Uid=<uid>&minCursor=<cursor>
```

Do not break its current behaviour.

---

# Direct TikTok Backend

Integrate only the useful TikTok components from Evil0ctal rather than forcing requests through the entire Evil0ctal REST/task infrastructure.

Use its functionality for:

```text
TikTok signing
/api/user/detail/
/api/user/list/
profile parsing
followers
following
secUid handling
numeric uid handling
risk-control detection
session/cookie support
proxy support
```

Avoid unnecessary:

```text
FastAPI task creation
task polling
Redis queue overhead
PostgreSQL task storage
conservative Evil scheduler limits
0.12 req/sec throttles
1-request-per-identity restrictions
```

The direct backend should connect as closely as possible to my scanner's existing async request engine.

---

# Unified Profile Format

Both backends must normalize profile data to:

```json
{
  "status": "ok",
  "data": {
    "profile": "https://avatar...",
    "username": "example",
    "fullname": "Example User",
    "userid": "123456789",
    "verified": "Yes✅",
    "private": "Public Account",
    "followers": "10000",
    "following": "500",
    "likes": "100000",
    "video": "100",
    "secUid": "...",
    "region": "GB",
    "created_time": "...",
    "last_change_name": "...",
    "language": "...",
    "open_favorite": "...",
    "see_following": "...",
    "heart_count": "100000"
  }
}
```

If a field is not available, return a safe `null` / unknown value rather than inventing data.

---

# Unified Followers / Following Format

Both backends must normalize follower/following pages to:

```json
{
  "users": [
    {
      "uniqueId": "username",
      "nickname": "Display Name",
      "avatarThumb": "https://...",
      "user_id": "123456789",
      "signature": "bio",
      "privateAccount": false,
      "verified": "No❌"
    }
  ],
  "hasMore": true,
  "minCursor": "..."
}
```

---

# Add Missing Evil Fields

Modify the Evil-derived TikTok parser so it includes:

```text
privateAccount
private/public profile state
```

for profile and follower/following entries where TikTok supplies it.

Also preserve useful fields such as:

```text
uid
secUid
uniqueId
nickname
signature
avatar
verified
followers
following
videoCount
heartCount
```

---

# Backend Modes

Add a configuration option:

```text
backend_mode
```

Possible values:

```text
worker
direct
hybrid
```

Meaning:

```text
worker
= use only the current Worker API

direct
= use only the direct Evil/TikTok backend

hybrid
= dynamically use both
```

---

# Hybrid Dispatcher

In hybrid mode, do NOT permanently split traffic 50/50.

Build a dynamic dispatcher.

Continuously track for each backend:

```text
attempted requests/sec
successful requests/sec
successful records/sec
average latency
median latency
p95 latency
p99 latency
timeout rate
network error rate
HTTP error rate
429 count
TikTok risk-control rate
invalid response rate
recent success %
```

Use these metrics to determine how much work each backend receives.

The main optimization target must be:

```text
successful TikTok records per second
```

NOT simply attempted requests/sec.

---

# Dynamic Routing

If the direct backend is performing well:

```text
Direct = 150 successful req/s
Worker = 50 successful req/s
```

route substantially more traffic to Direct.

If Direct starts hitting:

```text
risk-control
timeouts
empty responses
proxy failures
high latency
```

reduce Direct allocation and shift work toward Worker.

Likewise, if Worker becomes slower or starts failing, route more work toward Direct.

Do this automatically.

---

# Backend Scoring

Create a backend performance score based on factors such as:

```text
successful RPS
records returned per request
latency
error %
risk-control %
recent availability
```

For follower pages, account for page size.

A direct response returning:

```text
35 users
```

should be considered more valuable than a Worker response returning:

```text
20 users
```

at the same request rate.

The scheduler should primarily optimize:

```text
followers/sec
following/sec
accounts/sec
successful records/sec
```

rather than raw RPS alone.

---

# Cursor-Chain Ownership

Follower/following pagination is cursor dependent.

Do NOT randomly switch backend in the middle of a pagination chain unless the cursor formats have been verified as fully compatible.

Default behaviour:

```text
Account A followers
→ assigned to Direct
→ Direct owns entire cursor chain

Account B followers
→ assigned to Worker
→ Worker owns entire cursor chain
```

Keep that backend assignment until:

```text
hasMore = false
```

or the backend fails badly enough that failover is required.

---

# Cursor Failover

Implement controlled backend failover.

If a backend becomes unusable during a cursor chain:

1. Check whether its current cursor can safely be used by the other backend.
2. If compatible, continue from the same cursor.
3. If not compatible:
   - restart that account's pagination safely,
   - deduplicate already collected users,
   - never duplicate final results.

Do not blindly feed incompatible cursors into another backend.

---

# Page Size

Allow independent page sizes:

```text
worker_page_size
direct_page_size
```

Use the largest reliable Direct TikTok page size supported by the endpoint.

Current Evil research indicates approximately:

```text
35 users/page
```

for TikTok list requests.

Do not force the Direct backend down to Worker page sizes.

---

# Parallel Account Pagination

For one account:

```text
page 2 waits for page 1 cursor
```

but this must NEVER block other accounts.

Example:

```text
Account A page 2 waiting
Account B page 8 running
Account C profile running
Account D following page 3 running
Account E profile running
```

Run many independent account pagination state machines concurrently.

---

# Worker Counts

Keep my scanner's large worker system.

Support:

```text
100
500
1000
2500
5000
```

workers.

Do not arbitrarily limit workers because of CPU or RAM assumptions.

Workers are logical jobs and should be separated from actual connection limits.

Add separate settings:

```text
workers
worker_backend_max_connections
direct_backend_max_connections
worker_backend_keepalive_connections
direct_backend_keepalive_connections
connections_per_proxy
```

---

# Connection Pooling

Both backends must use:

```text
persistent HTTP clients
keep-alive
connection pooling
DNS reuse/cache where possible
TLS session reuse where possible
```

Do NOT create a new client/session per request.

---

# Direct Proxy Support

The Direct backend should support my Webshare proxy file:

```text
C:\Users\vailo\Downloads\webshare_proxy.txt
```

Format:

```text
IP:PORT:USERNAME:PASSWORD
```

Convert internally to:

```text
http://USERNAME:PASSWORD@IP:PORT
```

Track per proxy:

```text
successes
failures
latency
timeouts
risk-control responses
requests/sec
active connections
```

Do not constantly destroy/recreate proxy sessions.

---

# Adaptive Direct Concurrency

For the Direct backend, implement adaptive concurrency.

Start at a configurable level.

Increase concurrency while:

```text
successful RPS rises
followers/sec rises
latency remains acceptable
risk-control remains low
```

Reduce concurrency when:

```text
attempted RPS rises
but successful RPS falls
```

or:

```text
risk-control increases
timeouts increase
latency spikes
```

The goal is:

```text
maximum successful throughput
```

not maximum request spam.

---

# Retry Behaviour

Keep retries separate for each backend.

Retry:

```text
connection errors
timeouts
TLS failures
proxy failures
HTTP 5xx
temporary empty TikTok responses
temporary risk-control responses where appropriate
```

Do not block unrelated workers during retry delays.

Use async delayed retry queues.

---

# Backend-Specific Failure States

Record whether failures came from:

```text
worker_network_error
worker_http_error
worker_invalid_response
worker_429

direct_network_error
direct_proxy_error
direct_timeout
direct_http_error
direct_risk_control
direct_empty_response
direct_invalid_response
```

This is important for benchmarking.

---

# Global 429 Behaviour

Preserve my scanner's existing global 429 safety behaviour for the Worker backend.

For Direct TikTok, do not automatically assume every TikTok restriction is HTTP 429.

Also detect:

```text
empty body
captcha
empty userInfo
empty userList unexpectedly
TikTok risk status codes
risk-control responses
```

Direct backend restrictions should affect Direct routing independently unless a true global reason exists to stop everything.

Do not unnecessarily stop the Worker backend because Direct TikTok hit risk control.

---

# Optional Racing

Add an OPTIONAL mode for request racing.

For important/retry requests only:

```text
Worker request ──┐
                 ├── first valid result wins
Direct request ──┘
```

Cancel or ignore the slower result.

Do NOT enable racing for every request by default because that wastes traffic.

Possible setting:

```text
race_failed_requests = true
```

---

# Caching

Keep backend caches isolated when necessary.

Cache profiles safely.

Do not incorrectly cache cursor-dependent pages across unrelated requests.

If the exact same request is already in flight, optionally coalesce it so duplicate scanner jobs can share one result.

---

# Database and Logging

Do not make network requests wait for unnecessary database writes.

Batch/buffer:

```text
logs
metrics
statistics
noncritical persistence
```

Preserve crash-safe progress where required.

Do not synchronously print every request.

---

# Statistics

Extend `scan_stats.json` with:

```text
worker_requests_total
worker_requests_successful
worker_requests_failed
worker_requests_per_second
worker_successful_rps
worker_average_latency
worker_p95_latency

direct_requests_total
direct_requests_successful
direct_requests_failed
direct_requests_per_second
direct_successful_rps
direct_average_latency
direct_p95_latency
direct_risk_control_count

worker_followers_received
direct_followers_received

worker_following_received
direct_following_received

worker_records_per_second
direct_records_per_second

hybrid_total_successful_rps
hybrid_records_per_second

backend_switches
backend_failovers
cursor_chain_restarts
```

---

# Console

Show a compact live backend comparison such as:

```text
WORKER | 53.2 success/s | 1,064 users/s | 212ms | 1.2% errors
DIRECT | 91.7 success/s | 3,209 users/s | 178ms | 3.8% risk
TOTAL  | 144.9 success/s | 4,273 users/s
```

Do not spam the console for every request.

---

# HTML Report

Do not break or redesign my existing HTML report.

Only add backend statistics if useful.

Existing sections and functionality must remain intact.

---

# Benchmark Before and After

Before making changes, benchmark my existing scanner.

Then benchmark:

```text
Worker only
Direct only
Hybrid
```

under identical conditions.

Test:

```text
100 workers
500 workers
1000 workers
2500 workers
5000 workers
```

Measure:

```text
successful requests/sec
records/sec
accounts/sec
followers/sec
following/sec
average latency
p95 latency
error rate
risk-control rate
CPU
RAM
network use
```

---

# 10,000 Follower Test

Perform a dedicated test equivalent to:

```text
10,000 followers
```

Compare:

```text
Worker only
Direct only
Hybrid
```

Record:

```text
total completion time
number of requests
users/page
successful RPS
followers/sec
retries
failures
risk-control responses
```

---

# Large Scan Testing

Also stress test:

```text
100 accounts
1,000 accounts
10,000 accounts
```

Use mixed profile sizes.

Ensure one slow/private/failing profile never blocks unrelated accounts.

---

# Preserve Existing Scanner Behaviour

Do not remove or break:

```text
resume
completed-profile tracking
retry failed profiles
partial profiles
deduplication
target matching
reports
avatar handling
statistics
Ctrl+C graceful shutdown
global state persistence
```

---

# FINAL MANDATORY FULL-CODE REVIEW AND SPEED PASS

After ALL hybrid functionality has been implemented and all tests are passing, perform a **second complete review of the entire codebase from beginning to end**.

Do NOT stop after finishing the requested hybrid features.

Review **every single file, class, function, coroutine, loop, queue, lock, request path, parser, database operation, report generator, logger, retry path, startup path, shutdown path and helper function**.

The purpose of this final pass is to make the ENTIRE scanner substantially faster and more asynchronous, not only the new hybrid backend.

Actively search for every remaining performance bottleneck.

Review and optimize all of the following:

```text
network requests
HTTP connection creation
DNS resolution
TLS handshakes
connection reuse
keep-alive
proxy handling
proxy selection
session reuse
request scheduling
worker scheduling
async queues
semaphores
locks
global locks
SQLite usage
database commits
database reads
database writes
transactions
JSON parsing
JSON serialization
file reads
file writes
state persistence
logging
console printing
statistics calculation
HTML generation
avatar handling
deduplication
sets/maps/lookups
target matching
retry scheduling
timeout handling
pagination
cursor management
startup
resume
shutdown
cleanup
memory allocations
object creation
copies of large dictionaries/lists
unnecessary conversions
duplicate work
synchronous calls inside async paths
thread pools
executor usage
sleep calls
polling loops
contention
hot loops
CPU-heavy processing
```

Convert synchronous operations to proper asynchronous implementations wherever that produces a genuine performance improvement.

Do not convert things to async merely for appearance. Measure whether it helps.

The request hot path should contain the minimum possible work.

Move noncritical operations away from latency-sensitive request processing where safe.

Examples:

```text
network request
→ parse essential result
→ immediately release worker/connection
→ queue logging/statistics/noncritical persistence separately
```

Avoid:

```text
network request
→ synchronous logging
→ synchronous database commit
→ synchronous report update
→ multiple locks
→ then finally release worker
```

Review all shared-state locking.

Replace unnecessary global locks with:

```text
per-resource locks
lock-free structures where appropriate
atomic operations
batched updates
single-writer async queues
```

Reduce lock duration as much as possible.

Make sure thousands of workers are not contending on one shared lock.

Review database persistence.

Use techniques such as:

```text
batched writes
transaction batching
prepared/reused statements where appropriate
WAL where appropriate
single-writer queues where appropriate
reduced commit frequency where crash safety allows
efficient indexes
avoid repeated SELECTs
avoid N+1 database operations
```

Preserve resume/crash safety.

Review logging.

Logging must NEVER become a major throughput bottleneck.

Use:

```text
buffering
batching
async/background writers
rate-limited console updates
```

while preserving enough information to debug failures.

Review statistics.

Do not repeatedly scan huge datasets to calculate live statistics.

Use incremental counters and efficient rolling windows.

Review HTML report generation.

Large reports must remain fast with:

```text
thousands
tens of thousands
hundreds of thousands
```

of accounts.

Optimize:

```text
JSON serialization
DOM size
pagination/virtualization
search
filtering
sorting
avatar loading
lazy rendering
event handlers
memory usage
```

Do not regenerate the entire report unnecessarily while scanning.

Review avatar handling and ensure profile pictures work correctly in:

```text
Overview
Errors
All Accounts
Skipped Accounts
Target Matches
```

without slowing down report generation.

Review JSON parsing.

Avoid repeatedly parsing/serializing the same payload.

Avoid unnecessary deep copies.

Use the fastest safe parsing strategy available for the target Python environment.

Review all retry paths.

Retries must be asynchronous and must not occupy an active worker while sleeping.

Use delayed retry scheduling instead of:

```python
await asyncio.sleep(...)
```

inside a worker when that unnecessarily ties up worker capacity.

Review pagination.

One account's sequential cursor chain must never prevent unrelated accounts from progressing.

Review task creation.

Do not create millions of unnecessary asyncio Tasks at once.

Use bounded queues and efficient producer/consumer scheduling.

Review cancellation and Ctrl+C handling so high concurrency still shuts down quickly and persists state safely.

Review memory usage, but DO NOT arbitrarily reduce concurrency simply because memory usage is high.

The goal is to use the computer's available resources efficiently and aggressively.

Do not assume the computer cannot handle a high workload.

Identify the actual measured bottleneck.

---

# Performance Profiling

Profile the completed program rather than guessing.

Measure where time is actually spent.

Profile:

```text
CPU time per function
event-loop delays
request latency
connection-pool waits
semaphore waits
queue waits
lock waits
database latency
JSON parsing time
serialization time
logging time
report-generation time
proxy selection time
signing time
```

Locate the slowest hot paths and optimize them individually.

Repeat profiling after changes.

Do not stop after one optimization pass.

Use an iterative process:

```text
benchmark
→ profile
→ identify bottleneck
→ optimize
→ benchmark again
→ profile again
→ repeat
```

Continue until additional changes provide little meaningful improvement or would harm correctness/reliability.

---

# Verify Async Correctness

After optimization, confirm that the main scanning pipeline is truly asynchronous.

There should be no hidden blocking operations that freeze the event loop under load.

Inspect all third-party/library calls used inside async functions.

If an unavoidable blocking operation exists:

- move it out of the event loop,
- batch it,
- or isolate it appropriately.

Measure event-loop responsiveness under:

```text
100 workers
500 workers
1000 workers
2500 workers
5000 workers
```

---

# Extreme Testing

After the final optimization pass, stress-test the completed scanner again.

Test:

```text
high worker counts
large input datasets
slow proxies
dead proxies
mixed proxy speeds
timeouts
TLS failures
HTTP 5xx
HTTP 429
TikTok risk-control responses
private accounts
deleted accounts
invalid users
huge follower lists
huge following lists
Ctrl+C during heavy load
resume after interrupted scans
retry failed profiles
```

Verify there are:

```text
no deadlocks
no connection leaks
no task leaks
no file-handle leaks
no corrupted state
no duplicate accounts
no skipped accounts caused by concurrency bugs
no broken pagination
no lost results
```

---

# Final Before vs After Benchmark

At the very end, after ALL functionality and optimization work is complete, perform another full benchmark.

Compare:

```text
ORIGINAL SCANNER
vs
FIRST HYBRID IMPLEMENTATION
vs
FINAL FULLY OPTIMIZED HYBRID IMPLEMENTATION
```

Measure everything independently:

```text
successful requests/sec
attempted requests/sec
profiles/sec
accounts/sec
followers/sec
following/sec
successful records/sec
average latency
median latency
p95 latency
p99 latency
connection-pool wait time
queue wait time
database write time
JSON parsing time
CPU usage
RAM usage
network throughput
error rate
timeout rate
risk-control rate
10,000 follower completion time
large scan completion time
```

Calculate the measured speed improvement for each major area.

For example:

```text
Profile lookup: X.X× faster
Follower collection: X.X× faster
Following collection: X.X× faster
Database writes: X.X× faster
Resume loading: X.X× faster
JSON parsing: X.X× faster
HTML generation: X.X× faster
Overall scan: X.X× faster
```

Do not invent numbers.

Only report speed improvements that were actually measured.

If something became slower, explicitly identify it and fix it where possible.

---

# Final Requirement

The completed project should target:

```text
MAXIMUM SUCCESSFUL TIKTOK DATA THROUGHPUT
MAXIMUM ASYNC CONCURRENCY
MINIMUM REQUEST LATENCY
MINIMUM INTERNAL OVERHEAD
MINIMUM BLOCKING
MAXIMUM CONNECTION REUSE
FAST DATABASE PERSISTENCE
FAST RESUME
FAST REPORT GENERATION
```

while preserving correctness, stability, resume support, pagination correctness, proxy support, retries, reports and all existing features.

Do not consider the task complete after the hybrid backend merely works.

The task is complete only after:

1. The hybrid Worker + Direct backend works correctly.
2. Existing functionality remains working.
3. Every part of the codebase has been reviewed again.
4. All reasonable synchronous bottlenecks have been removed or optimized.
5. The entire pipeline has been profiled.
6. Every major performance bottleneck found has been addressed.
7. Worker-only, Direct-only and Hybrid modes have been benchmarked.
8. Extreme scenarios have been tested.
9. The final optimized version has been benchmarked against the original.
10. The final code has been reviewed once more for correctness, performance, concurrency bugs and regressions.

Take as much time as needed to review the whole codebase carefully.

Do not rush the implementation.

Do not stop at surface-level optimizations.

Review each major subsystem independently, benchmark it, optimize it, test it, then review the complete system again as one integrated scanner.
