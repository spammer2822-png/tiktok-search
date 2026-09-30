# Complete Previous Work First, Then Apply 429 Update

First, fully complete **everything from my previous instructions and the previously uploaded `.MD` file**.

Do NOT skip, replace, restart, simplify, partially ignore, or reinterpret those previous requirements.

The previous prompt and `.MD` remain the main specification.

Complete ALL previous work first, including:

- hybrid Worker + Direct/Evil backend
- backend modes
- dynamic routing
- async worker system
- performance optimization
- connection pooling
- proxy handling
- TikTok signing
- profile lookup
- followers
- following
- cursor handling
- pagination
- retries
- persistence
- resume
- deduplication
- target matching
- statistics
- console output
- HTML report requirements
- avatar handling
- database optimization
- logging optimization
- full-code profiling
- full async review
- stress testing
- before/after benchmarking
- final whole-code performance optimization pass

Do not treat this new message as a replacement for the earlier `.MD`.

The required order is:

```text
1. Complete everything from the previous prompt.
2. Complete everything from the previously uploaded .MD file.
3. BEFORE modifying the Evil0ctal code, run and document the mandatory baseline validation below.
4. Test all previous requirements.
5. Review every part of the implementation.
6. Perform the full-code performance and async optimization pass requested previously.
7. Fix every regression or bottleneck discovered.
8. Only AFTER all previous work is complete, apply the additional 429 behaviour below.
9. Re-test the full scanner again.
10. Review the complete codebase one final time.
```


# Mandatory Baseline Validation Before Modifying the Evil0ctal Code

Before changing, optimizing, integrating, or removing restrictions from the Evil0ctal repository, first validate the **original unmodified repository**.

Do NOT assume that the original repository currently works correctly just because the code exists or the README says a feature is supported.

The baseline validation must happen **before any Evil0ctal code is modified**.

## Required Baseline Validation

1. Use the exact current Evil0ctal repository/version being integrated.
2. Record the exact commit/hash or version being tested.
3. Install all required dependencies using the repository's documented setup.
4. Build/start all required components needed for its TikTok functionality.
5. Run the repository's existing:
   - unit tests,
   - integration tests,
   - parser tests,
   - scheduler tests,
   - TikTok-specific tests,
   - and any other relevant automated test suites.
6. Record every test result before making changes.
7. Clearly separate:
   - tests that already failed in the original repository,
   - environment/setup failures,
   - network-dependent failures,
   - and failures introduced by later modifications.
8. Do not silently ignore pre-existing failures.

## Verify the TikTok Features We Actually Need

Before modifying the repo, specifically verify the original implementation for:

```text
TikTok profile lookup
/api/user/detail/

TikTok followers
/api/user/list/
followers scene

TikTok following
/api/user/list/
following scene

secUid handling
numeric uid handling
cursor/minCursor handling
hasMore handling
profile parsing
follower/following parsing
avatar parsing
verified status
private/public information where available
TikTok signing
proxy/session handling
risk-control detection
```

Where live/network testing is possible, perform real baseline requests against test accounts that the tester is authorized to query.

Verify that the returned structures contain the fields and pagination data that the hybrid scanner needs.

Do not treat an HTTP 200 response alone as proof that the feature works.

Validate:

```text
response body
parsed output
user records
cursor progression
hasMore behaviour
pagination completion
duplicate handling
error classification
```

## Baseline Performance Measurements

Before optimization, measure the original Evil0ctal implementation under the same environment that will later be used for the optimized version.

Record at least:

```text
successful requests/sec
attempted requests/sec
average latency
median latency
p95 latency
p99 latency
profile lookups/sec
follower pages/sec
following pages/sec
followers/sec
following/sec
CPU usage
RAM usage
network usage
error rate
timeout rate
risk-control rate
```

Also record the original configuration, including:

```text
identity count
worker count
connection count
proxy count
page size
token-bucket settings
rate-limit settings
queue limits
timeouts
retry settings
```

This baseline must be saved so the final before/after benchmark is meaningful.

## Test a Known Follower Pagination Case

Before changing the follower/following implementation, test at least one account with enough followers/following to require multiple pages.

Verify:

```text
page 1 works
cursor from page 1 is valid
page 2 works
pagination continues correctly
hasMore is interpreted correctly
the final page terminates correctly
no users are duplicated
no users are skipped
```

Where practical, also test a larger account to confirm that pagination works across many pages.

## Environment Limitations

If live TikTok/network testing cannot be performed in the current execution environment:

1. State that limitation clearly.
2. Still run all local/unit/parser/scheduler tests that can run.
3. Do not falsely claim that live TikTok functionality was verified.
4. Create deterministic fixtures/mocks for the TikTok response shapes.
5. Run the real live verification later in an environment with outbound network access before claiming production readiness.

## Preserve Baseline Evidence

Save baseline test results and benchmark output to files inside the project, for example:

```text
baseline_evil_tests.json
baseline_evil_benchmark.json
baseline_evil_failures.log
```

The exact filenames may differ, but the evidence must be preserved.

The final report must clearly distinguish:

```text
PRE-EXISTING FAILURES
MODIFICATION-INTRODUCED FAILURES
FIXED PRE-EXISTING FAILURES
NEW PERFORMANCE REGRESSIONS
NEW PERFORMANCE IMPROVEMENTS
```

## Do Not Begin Optimization Until Baseline Is Understood

Do not start removing throttles, changing async behaviour, modifying the scheduler, changing the parser, or integrating the Direct backend until the baseline validation has been completed and documented.

If the original Evil0ctal implementation is already broken in an area required by this scanner, fix that problem first and document it separately before continuing with the hybrid optimization work.

After baseline validation is complete, continue with all previously requested hybrid, async, performance, 429, benchmarking, and full-code-review requirements.


# Additional 429 Behaviour

After the previous work is fully complete, implement this exact behaviour.

## FindTik / External Worker 429

If the external FindTik/Worker backend returns its first confirmed HTTP `429`:

```text
FindTik gets 429
        ↓
Immediately stop ALL new FindTik requests
        ↓
Disable FindTik for the remainder of the CURRENT scan
        ↓
Do not retry or probe FindTik again during this scan
        ↓
Move all compatible remaining work to Direct/Evil
        ↓
Continue the scan through Direct only
```

Set FindTik to a state such as:

```text
RATE_LIMITED_DISABLED
```

After this happens:

```text
FindTik traffic = 0%
Direct traffic = active
Scanner = continues
```

Do NOT let queued FindTik requests continue firing after the backend is disabled.

Safely transfer or preserve unfinished work.

For existing pagination chains, preserve cursors and already collected users.

If a chain must restart through Direct, deduplicate all previously collected records.

## Direct 429

If Direct/Evil later returns HTTP `429`:

```text
Direct gets 429
        ↓
Immediately pause ALL new Direct requests
        ↓
Preserve complete scanner state
        ↓
Enter Direct cooldown
```

Use `Retry-After` if available.

If there is no `Retry-After`, use a configurable cooldown such as:

```text
60 seconds
```

Do NOT have thousands of workers individually sleeping.

Use one central asynchronous cooldown/state mechanism.

During cooldown:

```text
FindTik = disabled
Direct = cooldown
Scanner = waiting
```

Preserve everything:

```text
completed accounts
unfinished accounts
followers already collected
following already collected
profile data
follower cursor
following cursor
pending jobs
retry state
deduplication state
target matches
statistics
database/state files
```

## Direct Recovery Attempt

After cooldown, make a small controlled Direct recovery attempt.

Do NOT instantly release the entire worker pool.

Use:

```text
small probe
→ low concurrency
→ gradual ramp-up
```

If Direct succeeds:

```text
Direct → healthy
Scanner → active
```

Continue the scan using Direct only.

## If Direct Gets 429 Again

If Direct still returns HTTP `429` during the recovery attempt:

```text
FindTik = already disabled
Direct = still rate limited
```

then stop the entire scanner gracefully.

Required behaviour:

```text
stop all new requests
        ↓
cancel/drain in-flight work safely
        ↓
save every unfinished account
        ↓
save every cursor
        ↓
flush database/state
        ↓
save statistics and logs
        ↓
update safe report files
        ↓
cleanly exit
```

Do NOT enter an endless:

```text
429
→ cooldown
→ 429
→ cooldown
→ 429
```

loop.

Direct gets one controlled cooldown/recovery opportunity.

If the recovery attempt still produces `429`, stop the scan.

## Exact Final Flow

```text
HYBRID ACTIVE
      │
      │ FindTik 429
      ▼
FINDTIK DISABLED
DIRECT ONLY
      │
      │ Direct healthy
      ├──────────────→ Continue scan
      │
      │ Direct 429
      ▼
DIRECT COOLDOWN
      │
      ▼
DIRECT RECOVERY TEST
      │
      ├── SUCCESS
      │      ↓
      │   DIRECT ONLY
      │   CONTINUE SCAN
      │
      └── 429 AGAIN
             ↓
        SAVE EVERYTHING
             ↓
        STOP SCANNER
```

## Important

The first FindTik `429` disables FindTik **only for the current scan**.

When the user starts or resumes a new scan later, FindTik may be tried again normally.

## Required Statistics

Track:

```text
findtik_429_count
findtik_disabled_after_429
findtik_jobs_moved_to_direct
findtik_requests_cancelled_after_429

direct_429_count
direct_cooldown_count
direct_recovery_attempts
direct_recovery_successes
direct_recovery_429s

scanner_stopped_due_to_all_backends_rate_limited
```

## Required Testing

Explicitly test:

```text
FindTik 429 while 100+ requests are queued
FindTik 429 while 2500 workers are active
FindTik 429 during follower pagination
FindTik 429 during following pagination
FindTik 429 while Direct remains healthy
Direct 429 after FindTik is disabled
Direct successful recovery
Direct repeated 429 after cooldown
graceful shutdown after both paths are unavailable
resume after that shutdown
```

Verify that:

```text
no FindTik requests leak through after it is disabled
Direct continues after FindTik 429
no scan state is lost
no duplicate users are created
no cursor state is corrupted
no queued jobs disappear
shutdown is clean
resume works correctly afterward
```

# Final Instruction

Again, complete the **entire previous prompt and previously uploaded `.MD` file first**.

Do not use this message as an excuse to skip any previous requirement.

After implementing this update, perform another complete review of the entire codebase for:

```text
correctness
performance
async behaviour
race conditions
deadlocks
request leaks
task leaks
connection leaks
pagination correctness
state persistence
resume correctness
database performance
network performance
logging performance
HTML report performance
```

Benchmark the final implementation again and compare it with the original scanner and the earlier hybrid implementation.

Do not consider the work complete until both the previous `.MD` requirements and this new `429` behaviour are fully implemented, tested, optimized, and reviewed.




# MAXIMUM PC-BOUND THROUGHPUT — NO ARTIFICIAL PERFORMANCE CAPS

The entire scanner must be engineered so that **every stage runs as fast as the computer, network, proxy pool, backend, and upstream service can actually sustain**.

Do NOT impose arbitrary conservative caps, sleeps, rate limits, queue ceilings, serialization points, artificial delays, or low concurrency defaults merely because high throughput might use a lot of CPU, RAM, bandwidth, sockets, or file I/O.

The objective is to use the available hardware aggressively and efficiently.

The scanner should attempt to use as much of the computer's real available performance as is useful for the workload.

This requirement applies to ALL of the following:

```text
request creation
request dispatch
HTTP transmission
response receiving
TikTok data scraping
response parsing
JSON parsing
data normalization
data transformation
deduplication
target matching
pagination handling
cursor handling
retry scheduling
proxy selection
backend selection
data aggregation
database writes
database reads
state persistence
statistics updates
log writing
console output
HTML/report data preparation
avatar processing
final result writing
shutdown flushing
resume loading
```

Every one of these paths must be reviewed for unnecessary waiting, serialization, blocking, locking, copying, conversion, flushing, or repeated work.

## No Arbitrary Internal Throughput Caps

Do NOT add hidden limits such as:

```text
fixed requests-per-second limits
fixed accounts-per-second limits
fixed profiles-per-second limits
fixed followers-per-second limits
fixed following-per-second limits
hard-coded async task rates
hard-coded proxy request rates
hard-coded parsing rates
hard-coded database write rates
hard-coded report-generation rates
unnecessary sleep() calls
artificial batching delays
artificial console delays that block processing
```

unless a limit is technically required for correctness, an external service explicitly requires it, or the user has configured it.

If a technical limit is genuinely required, document:

```text
what the limit is
why it exists
where it is enforced
whether it is configurable
what happens if it is raised
```

Do not silently cap throughput.

## Workers Are the Main User-Controlled Concurrency Setting

The main user-controlled performance setting should remain:

```text
workers
```

Examples:

```text
100
500
1000
2500
5000
```

The scanner should scale its internal concurrency architecture from the selected worker count.

Do NOT impose a second unrelated hidden worker ceiling underneath the user-selected value.

For example:

```text
workers = 2500
```

must NOT secretly become:

```text
actual usable jobs = 64
```

unless a real measured bottleneck or explicit connection configuration requires it.

If the runtime cannot sustain the requested worker count, expose the actual limiting factor clearly rather than silently reducing it.

## Connection Limits Must Scale From Worker Count

Connection-pool sizing should be derived from or explicitly linked to the selected worker count.

Do not use a tiny fixed connection pool that makes a high worker count meaningless.

For example, the implementation should support logic such as:

```text
workers
        ↓
backend allocation
        ↓
connection pool sizing
        ↓
per-proxy connection allocation
```

The exact formula should be benchmarked rather than guessed.

Connection configuration may include:

```text
max_connections
max_keepalive_connections
connections_per_proxy
worker_backend_connections
direct_backend_connections
```

but these should either:

1. automatically scale intelligently from `workers`, or
2. be explicitly user-configurable.

They must not act as hidden conservative caps.

## Worker Count and Connection Count

Workers and connections are not necessarily 1:1.

The implementation should determine the most efficient relationship.

For example:

```text
2500 workers
does NOT necessarily mean
2500 brand-new TCP connections
```

Workers should reuse persistent pooled connections whenever possible.

However, the connection pool must be large enough that workers are not spending most of their time waiting for a connection.

Measure:

```text
connection pool wait time
percentage of workers waiting for connections
active connections
idle keep-alive connections
connection reuse rate
new connection rate
TLS handshake rate
```

If connection-pool wait becomes a meaningful bottleneck, automatically or explicitly increase the pool where the environment supports it.

## Maximum Request Throughput

Request dispatch must be extremely fast.

Optimize:

```text
queue dequeue cost
backend selection
proxy selection
request construction
header construction
URL construction
JSON/body construction
HTTP client reuse
socket reuse
DNS reuse
TLS session reuse
response reading
error classification
result handoff
```

Avoid unnecessary object creation and deep copies in the request hot path.

The request pipeline should contain only work that must happen before the request can proceed.

## Maximum Scraping and Parsing Throughput

TikTok response processing must also be optimized aggressively.

Profile:

```text
JSON decoding
field extraction
normalization
validation
Pydantic/model construction
dictionary creation
list copying
string conversion
ID conversion
avatar URL extraction
follower/following normalization
```

Avoid expensive validation/model layers in the hot path when equivalent safe lightweight parsing can provide the same correctness.

If a heavier parser is needed for validation/debugging, consider a fast production path and a strict validation/testing path.

Do not parse the same payload multiple times.

Do not serialize data to JSON and immediately parse it back unless genuinely required.

## Maximum Data Handling Throughput

Internal data movement should avoid unnecessary copies.

Prefer:

```text
references
efficient immutable/shared structures where safe
sets/dicts for O(1) lookup
preallocated/batched containers where useful
incremental aggregation
streaming processing
```

Avoid repeatedly rebuilding giant lists or dictionaries.

Avoid repeatedly scanning all previous results to answer incremental questions.

Maintain indexes/counters as data arrives.

## Maximum Database / State Writing Throughput

Persistence must be extremely fast without sacrificing resume correctness.

Review:

```text
SQLite journal mode
WAL usage
synchronous setting where safe
transaction size
commit frequency
prepared statement reuse
executemany/batching
indexes
write contention
single-writer queue architecture
database connection reuse
checkpoint behaviour
```

Do NOT commit every tiny noncritical update individually if batching can preserve the required safety semantics.

Separate:

```text
CRITICAL state that must be persisted immediately
```

from:

```text
NONCRITICAL metrics/logging that can be buffered/batched
```

The network request hot path should not wait unnecessarily for disk I/O.

## Maximum File-Writing Throughput

For:

```text
JSON
JSONL
logs
stats
reports
state files
exports
```

use buffered/batched writes where safe.

Avoid repeatedly rewriting massive files when an append or incremental update is sufficient.

Avoid frequent expensive filesystem flushes for data that does not require immediate durability.

Critical resume data must still remain safe.

## Extremely Fast Logging and Printing

Console output and logging must never become a major bottleneck.

Do NOT call expensive synchronous `print()` or logger operations for every single request when operating at high throughput.

Use:

```text
in-memory counters
buffered logging
batched file writes
async/single-writer logging queue
periodic console refresh
rate-limited progress rendering
```

For example, instead of printing thousands of lines per second, update a compact live status display periodically.

The scanner must still preserve detailed errors and useful diagnostics.

High-speed logging must not mean losing important failure information.

## Fast Statistics

All statistics should be updated incrementally.

Do NOT repeatedly loop over the entire completed dataset to calculate:

```text
requests/sec
successful RPS
accounts/sec
followers/sec
following/sec
error rate
latency
backend performance
```

Use:

```text
atomic/in-memory counters
rolling windows
ring buffers/deques
incremental sums
histogram/quantile structures where appropriate
```

Statistics calculation must remain cheap even after millions of requests.

## Async Everywhere It Actually Helps

All I/O-heavy operations should be asynchronous where practical:

```text
HTTP requests
proxy traffic
network retries
queue waits
database writer handoff
file writer handoff
logging
cooldowns
timers
backend health checks
pagination scheduling
```

Do NOT block the event loop with synchronous disk/network work.

If a third-party library is blocking:

```text
replace it with async equivalent
or
isolate it appropriately
```

Do not use thread pools as a lazy substitute when a proper async implementation exists.

At the same time, do not turn CPU-only trivial functions into unnecessary coroutines.

Optimize based on measured performance.

## Avoid Worker Starvation

Workers must not remain occupied while waiting for things that can be scheduled independently.

Examples:

```text
retry backoff
429 cooldown
delayed retry
periodic logging
periodic stats writing
report refresh timers
```

These should use central scheduling/timers/queues rather than consuming active worker capacity.

## Queue Design

Queues must be optimized for large worker counts.

Review:

```text
queue contention
queue size
producer speed
consumer speed
priority handling
retry queue design
backend-specific queues
pagination queues
```

Do not create one giant lock around all scheduling.

Avoid queue architectures where thousands of workers fight over one slow critical section.

## Backend Throughput

The Worker and Direct backends should each be allowed to run up to the level that produces the highest successful throughput.

Do not artificially cap either backend below what the machine and backend can handle.

The hybrid dispatcher should continuously measure real performance and use available capacity.

For example:

```text
if Worker can sustain more valid traffic:
    use more Worker capacity

if Direct can sustain more valid traffic:
    use more Direct capacity

if both are healthy:
    use both aggressively
```

The exception is explicit 429/risk-control behaviour defined elsewhere in this specification.

## Performance Should Be Limited by Reality, Not Arbitrary Constants

The eventual limiting factor should be something real, such as:

```text
selected worker count
available connection capacity
proxy capacity
network bandwidth
CPU
RAM
disk throughput
backend response time
upstream response time
external rate limiting
TikTok risk-control
```

It should NOT be an unexplained internal constant left over from a conservative implementation.

Whenever throughput plateaus, identify and report the actual reason.

## Profiling Requirement

Under high load, explicitly determine what percentage of time is spent in:

```text
network waiting
connection-pool waiting
queue waiting
proxy selection
backend routing
TikTok signing
JSON parsing
normalization
database writing
database locking
file writing
logging
statistics
HTML/report generation
Python CPU execution
```

Optimize the largest contributors first.

## High-Worker Benchmark Requirement

For:

```text
100 workers
500 workers
1000 workers
2500 workers
5000 workers
```

record:

```text
actual active workers
peak active connections
average active connections
connection-pool wait time
queue wait time
successful RPS
attempted RPS
followers/sec
following/sec
profiles/sec
CPU utilization
RAM utilization
network throughput
disk throughput
event-loop lag
```

Determine whether increasing workers actually improves successful throughput.

Do NOT reduce workers automatically merely because resource usage rises.

Only treat a higher worker count as ineffective when benchmarks show that another resource has become the true bottleneck.

## Final Performance Goal

The implementation should be engineered toward:

```text
as fast as the PC can genuinely sustain
as fast as the network can genuinely sustain
as fast as the proxy/backend paths can genuinely sustain
as fast as TikTok can return valid data
```

with correctness preserved.

The scanner should not intentionally leave major amounts of CPU, network, connection, or I/O capacity unused because of arbitrary internal limits.

At the final review, specifically confirm:

```text
[ ] No hidden requests/sec cap remains
[ ] No hidden accounts/sec cap remains
[ ] No hidden follower/following throughput cap remains
[ ] No hidden fixed worker ceiling remains
[ ] Connection limits scale with or are configurable from worker count
[ ] Connection-pool wait is measured
[ ] Network hot path is fully optimized
[ ] Parsing hot path is fully optimized
[ ] Data handling hot path is fully optimized
[ ] Database/state writing is fully optimized
[ ] Logging/printing is non-blocking or efficiently buffered
[ ] Statistics remain fast under very large scans
[ ] Async workers are not wasted on timers/cooldowns
[ ] Actual bottlenecks are measured and documented
[ ] Final throughput is limited by real system/backend constraints rather than arbitrary code limits
```


# FINAL MANDATORY COMPLETION CHECKLIST

Do not consider the project complete until **every applicable item below has been explicitly verified**.

Mark each item as completed only after it has actually been implemented, tested, reviewed, or measured.

```text
[ ] Previous prompt fully completed
[ ] Previous uploaded .MD fully completed
[ ] Original Evil0ctal repository baseline tested before modification
[ ] Exact Evil0ctal commit/version recorded
[ ] Original Evil0ctal unit tests run
[ ] Original Evil0ctal integration tests run where available
[ ] Original Evil0ctal TikTok/parser/scheduler tests run
[ ] Pre-existing Evil0ctal failures documented separately
[ ] Environment/network limitations documented honestly
[ ] Baseline Evil0ctal benchmark saved

[ ] TikTok profile lookup verified
[ ] TikTok followers verified
[ ] TikTok following verified
[ ] secUid handling verified
[ ] numeric uid handling verified
[ ] cursor/minCursor handling verified
[ ] hasMore handling verified
[ ] multi-page pagination verified
[ ] no duplicate users across pagination
[ ] no skipped users caused by pagination
[ ] avatar parsing verified
[ ] verified status handling verified
[ ] private/public status handling verified where available

[ ] Worker-only backend works
[ ] Direct-only backend works
[ ] Hybrid backend works
[ ] Dynamic backend routing works
[ ] Backend scoring works
[ ] Backend assignment is concurrency-safe
[ ] Cursor-chain ownership works
[ ] Cursor failover works safely
[ ] Incompatible cursor fallback/restart works
[ ] Restarted chains deduplicate correctly

[ ] FindTik first HTTP 429 immediately disables FindTik for current scan
[ ] No new FindTik requests start after FindTik is disabled
[ ] Queued FindTik requests are cancelled/migrated safely
[ ] Direct backend continues after FindTik 429
[ ] FindTik jobs are migrated to Direct where compatible
[ ] FindTik stays disabled for remainder of current scan

[ ] Direct HTTP 429 immediately pauses Direct traffic
[ ] Direct cooldown works
[ ] Retry-After is respected when available
[ ] Direct cooldown does not occupy thousands of workers
[ ] Direct recovery probe works
[ ] Direct successful recovery resumes scan gradually
[ ] Direct second 429 after cooldown triggers graceful scanner shutdown
[ ] All state is saved before shutdown
[ ] Resume after rate-limit shutdown works correctly

[ ] 100-worker test completed
[ ] 500-worker test completed
[ ] 1000-worker test completed
[ ] 2500-worker test completed
[ ] 5000-worker test completed

[ ] 10,000-follower benchmark completed where environment permits
[ ] Worker-only 10k test measured
[ ] Direct-only 10k test measured
[ ] Hybrid 10k test measured
[ ] Requests/page measured
[ ] Followers/sec measured
[ ] Successful RPS measured
[ ] Completion time measured

[ ] 100-account stress test completed
[ ] 1,000-account stress test completed where practical
[ ] 10,000-account stress test completed where practical
[ ] Slow proxy test completed
[ ] Dead proxy test completed
[ ] TLS failure test completed
[ ] Timeout test completed
[ ] HTTP 5xx test completed
[ ] HTTP 429 test completed
[ ] TikTok risk-control test completed
[ ] Private account test completed
[ ] Deleted/invalid account test completed
[ ] Ctrl+C under heavy load tested
[ ] Resume after interrupted scan tested

[ ] No deadlocks detected
[ ] No task leaks detected
[ ] No connection leaks detected
[ ] No file-handle leaks detected
[ ] No corrupted state detected
[ ] No duplicate final accounts/users detected
[ ] No lost queued jobs detected
[ ] No broken pagination detected
[ ] No request leakage after backend disable/pause

[ ] Persistent HTTP clients reused
[ ] Connection pooling verified
[ ] Keep-alive verified
[ ] DNS/TLS reuse reviewed
[ ] Proxy-session reuse reviewed
[ ] Unnecessary client/session creation removed
[ ] Blocking network calls removed from async hot paths
[ ] Unnecessary sleeps removed/replaced
[ ] Retry waiting moved off active worker capacity where appropriate

[ ] SQLite/database hot path reviewed
[ ] Batched writes implemented where safe
[ ] Transaction/commit frequency reviewed
[ ] N+1 database operations reviewed
[ ] Database indexes reviewed
[ ] Crash-safe resume preserved

[ ] JSON parsing profiled
[ ] JSON serialization profiled
[ ] Unnecessary deep copies removed
[ ] Large object allocations reviewed
[ ] Deduplication structures reviewed
[ ] Target matching reviewed for hot-path efficiency

[ ] Logging hot path reviewed
[ ] Console output rate-limited/batched
[ ] Noncritical logging moved off request hot path where safe
[ ] Statistics use incremental counters where possible
[ ] Rolling metrics do not repeatedly rescan huge datasets

[ ] HTML report generation profiled
[ ] Large report performance tested
[ ] Overview avatars verified
[ ] Errors avatars verified
[ ] All Accounts avatars verified
[ ] Skipped Accounts avatars verified
[ ] Target Matches avatars verified
[ ] Search/filter/sort performance tested
[ ] DOM size/lazy rendering reviewed

[ ] CPU profiling completed
[ ] Event-loop latency profiled
[ ] Queue wait time profiled
[ ] Semaphore wait time profiled
[ ] Lock wait time profiled
[ ] Connection-pool wait time profiled
[ ] Database latency profiled
[ ] JSON parse/serialize time profiled
[ ] Logging time profiled
[ ] Report-generation time profiled
[ ] TikTok signing time profiled
[ ] Proxy selection time profiled

[ ] Entire codebase reviewed after first implementation
[ ] Entire codebase reviewed again after optimization
[ ] Every major synchronous bottleneck reviewed
[ ] Async correctness reviewed
[ ] Race conditions reviewed
[ ] Shared-state locking reviewed
[ ] Final regression pass completed

[ ] ORIGINAL SCANNER benchmark recorded
[ ] FIRST HYBRID IMPLEMENTATION benchmark recorded
[ ] FINAL OPTIMIZED HYBRID benchmark recorded
[ ] Worker-only benchmark recorded
[ ] Direct-only benchmark recorded
[ ] Hybrid benchmark recorded
[ ] Successful RPS compared
[ ] Accounts/sec compared
[ ] Followers/sec compared
[ ] Following/sec compared
[ ] Successful records/sec compared
[ ] Average latency compared
[ ] Median latency compared
[ ] p95 latency compared
[ ] p99 latency compared
[ ] CPU usage compared
[ ] RAM usage compared
[ ] Network usage compared
[ ] Error/risk-control rates compared

[ ] Measured speed improvement calculated for each major subsystem
[ ] Any regressions explicitly identified
[ ] Any remaining bottlenecks explicitly documented
[ ] No unmeasured performance claims made

[ ] Final code runs successfully
[ ] Final configuration is documented
[ ] Final dependencies are documented
[ ] Final scanner can resume safely
[ ] Final reports generate successfully
[ ] Final logs/statistics persist successfully
[ ] Final graceful shutdown works
[ ] Final implementation satisfies every previous requirement
```

## Completion Rule

If any required checkbox cannot be completed because of an environmental limitation, missing dependency, inaccessible network, unavailable test account, or other external restriction:

1. Do NOT mark it complete.
2. State exactly why it could not be verified.
3. Complete every test that can still be performed locally.
4. Provide the exact command/test procedure needed to verify the remaining item in the proper environment.
5. Do NOT claim production readiness or a measured speed improvement for anything that was not actually tested.

The final delivery must include a concise checklist/status report showing:

```text
COMPLETED
FAILED
BLOCKED BY ENVIRONMENT
NOT APPLICABLE
```

for every major group above.

Do not declare the task complete until this checklist has been reviewed one final time against the actual finished code.
