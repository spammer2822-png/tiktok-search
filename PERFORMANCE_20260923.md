# Performance, recovery and statistics

This update retains the existing Worker schemas, scan modes, cursor rules,
Webshare support, output folders, exports and offline report layout. It changes
scheduling/persistence, adds explicit failed-profile retries on resume, and
makes the first Worker HTTP 429 stop the current execution.

## Bottlenecks and changes

| Area reviewed | Change |
| --- | --- |
| Thousands of profile tasks and connection pools | A durable queue feeds bounded profile tasks. A shared semaphore/adaptive limit governs requests; retained per-route clients are bounded too. |
| SQLite commits, file writes and avatar-index reads blocking the loop | One disk worker and a bounded 256-command queue perform persistence. Producers await results for backpressure and ordered atomic operations. |
| Repeated queue counts and full JSON mirrors | SQLite triggers maintain counts by phase/status. Atomic JSON mirrors are batched at roughly 15 seconds and flushed at shutdown. |
| Re-reading completed exports and parsing members twice | Verified exports receive size/mtime stamps; unchanged completions are reused. Discovery consumes already-normalized page members. |
| Repeated trust-store loading and idle proxy pools | Origin TLS trust is initialized once off-loop. Per-route clients reuse connections and unused pools can be evicted within the resource budget. |
| Independent retry timers and dense capacity polling | One heap/timer schedules delayed continuations. Retries release HTTP permits; capacity waiters wake on release. |
| Excessive console output | Routine progress is throttled to about once per second; detail stays in buffered logs and critical messages print immediately. |
| Large report object/JSON/HTML copies | Shared field names and compact row arrays are streamed; saved profile metadata avoids unnecessary per-profile database reads. |
| Browser parsing/filtering/username sorting | A Web Worker owns data, search indexing and cached row indices; only the selected page reaches the UI. |

## Concurrency and HTTP connections

`workers` accepts 1–10,000 as the desired maximum, not requests/second. Runtime
uses the smaller of that setting and `max_connections` (default 64, range
1–256). POSIX file-descriptor limits can lower the ceiling, reserving resources
for sockets and per-profile SQLite/WAL files. Windows uses the explicit budget.
This does not infer a Webshare plan entitlement or Worker quota.

Profile tasks are bounded to twice the ceiling, never exceeding configured
workers: at most 128 with default settings. Remaining jobs stay in SQLite;
there is no task per account. Effective concurrency starts at at most eight,
can grow by two after sufficient successes at intervals of at least five
seconds, and can decrease on repeated failures or sustained latency increases.
It never intentionally pushes to 429 to learn a quota.

API requests, retries and optional avatars share aggregate pacing. Delays are
applied after client preparation, directly before dispatch, preventing setup
delays from bunching starts. Idle time does not accumulate burst tokens. Each
list still follows its exact returned cursor sequentially.

HTTPX clients are reused per proxy route. Credentials use `httpx.Proxy` auth,
never Worker headers. Lease counts prevent closing a client in use; unused
clients can be evicted, with retained client count bounded by the ceiling.
Per-client connections/keep-alive are bounded, idle expiry is 30 seconds, and
the shared gate limits aggregate requests across clients. Connect/read/write/
pool/total timeouts remain configurable. Total API timeouts start after pacing:
waiting for the configured delay is not a network failure. HTTPX reuses DNS/
connections through keep-alive; no speculative DNS cache was added.

Temporary network failures and supported 5xx responses use bounded backoff and
jitter. One retry timer queues continuations without an HTTP permit, allowing
other profile tasks to proceed. The special one-extra-attempt 404 rule remains.
Genuine proxy connection/authentication failures may use a healthy route;
Worker restrictions never trigger bypass rotation. Proxy CONNECT concurrency
errors remain separate from Worker HTTP 429.

## Immediate global 429 stop

The response-header hook recognizes a Worker 429 before reading its body.
Without awaiting, it sets shared rate-limit/stop events, closes admission and
cancels other unfinished API-request tasks. The receiving request records 429
and raises its rate-limit result.

Stop checks occur before claiming work, after request-slot admission, after
borrowing a client, during pacing, before retries and in HTTPX's send hook.
The retry timer wakes waiters, which stop rather than dispatch. No 429 retry
occurs in the same execution. Requests already transmitted cannot be recalled;
completed responses may still commit. Profile tasks get a short saving grace
period before cancellation. Admitted atomic disk operations finish before
their database connections close.

Unstarted/interrupted jobs return to pending instead of becoming failures.
Final statistics record `stopped_rate_limited`, stop reason/time and 429 count.
Checkpoints, history, matches and the report are saved. Later resume selects
the same folder and continues pending work.

## Retry/resume and checkpoints

Resume offers `Retry failed profiles? [Y/N]`, default N. Y requeues
`network_timeout`, `network_error`, `partial` and legacy `response_timeout`.
Previous status/result enter SQLite `retry_history` in the same transaction
as the pending transition. Successful profiles remain complete. N retains
terminal failures and continues pending/interrupted work.

Saved identity, completed lists, members, opaque cursors, matches and exports
survive retries. A success updates the same row; another failure stores the
latest result. Retrying Phase 1 restores the correct phase, and new Phase 2
discoveries are deduplicated. Missing cursors/restrictions cannot be invented
away or treated as proof of completion.

SQLite uses FULL synchronous WAL transactions. Members and their continuation
cursor commit together; essential target matches remain immediate. Replaying
committed observations repairs crashes between page commits and result writes.
`in_progress` claims become pending on restart. A finished export can recover
a job whose final queue update was interrupted.

SQLite is authoritative; scan_state.json, session.json and configuration
progress are periodic atomic mirrors. The bounded disk queue provides
backpressure; cancellation waits for an admitted atomic operation. Finalization
flushes mirrors/statistics/logs. A force kill cannot generate a final report or
save current in-memory counters, but committed work remains recoverable. Keep
all SQLite files with a resumable search.

Verified completed exports carry a size/mtime stamp, so unchanged files need
not be repeatedly loaded. Changed/legacy exports are checked fully. Missing or
invalid completed exports become `missing_export` for review, rather than
being silently rescanned. Stamps optimize local reads; they are not a
cryptographic tamper check.

## Statistics, throughput and ETA

scan_stats.json is written before API work, about every 15 seconds, at the
five-minute checkpoint and during graceful shutdown. One reporting task sends
snapshots to the disk worker; request updates only touch cheap loop-owned
memory counters.

`lifetime` covers the current execution, including username bootstrap requests.
`overall_*`, total_accounts_processed and queue totals include prior saved
progress. Each execution has a session_id. Rolling metrics use bounded
one-second buckets over approximately 300 seconds, with up to one second of
boundary error. Before five minutes, elapsed time is the rate denominator.

| Metric | Meaning |
| --- | --- |
| Requests total / second | Dispatched Worker API attempts, including retries. |
| Successful requests | HTTP success with valid JSON and no declared Worker error. Subsequent profile/schema validation can still fail. |
| Failed versus cancelled requests | Actual failures are separate from shutdown cancellations. Calls cancelled before dispatch are not attempts. |
| Accounts processed / second | Unique profiles reaching a terminal outcome this execution; retry attempts do not add accounts. |
| Successful accounts | Required lists are complete; skips and restrictions are separate. |
| Recovered after retry | A request succeeds after an earlier attempt failed, independently of final profile success. |
| Active requests / peak in flight | Dispatched Worker requests. active_request_slots separately includes admitted tasks preparing/waiting to send. |
| Effective concurrency | Current request-slot limit, distinct from desired workers and observed in-flight requests. |

Avatars and proxy validation are excluded from Worker request metrics, although
their time/shared pacing affects throughput. One account can need many requests,
so requests/sec is not interchangeable with accounts/sec.

Every 300 seconds the terminal shows rolling profile/request success/failure
rates, latency, retries/recoveries, timeouts, network errors, 5xx/429 counts,
concurrency, remaining work and ETA. Explicit JSON fields include
accounts_last_5_minutes, successful_accounts_last_5_minutes and
failed_accounts_last_5_minutes. Lightweight progress appears between snapshots.

ETA = remaining queued/in-progress accounts / recent successful accounts/sec.
It requires at least 30 seconds and five recent successful profiles. Stops,
429s, zero rate and insufficient samples show unavailable/null. Failures/skips
cannot inflate this success rate. Different list sizes and new Phase 2 work
can change the estimate; it is not a promised completion time.

scan_stats_history.jsonl appends full snapshots every five minutes and at
graceful shutdown. Session IDs/timestamps distinguish resume executions without
rewriting history. A torn final line from a force kill is removed on the next
append; earlier complete lines remain. In-memory metrics since the last
snapshot can be lost independently of committed scan data.

## Offline report

The output remains HTML plus relative report_assets for avatars. No new data
file must be fetched. The generator embeds a shared field list and compact
NDJSON array records once, streams the temporary HTML, then atomically replaces
the report.

An embedded Web Worker parses/indexes data, filters/sorts and caches filtered
indices between page changes. A reused Intl.Collator avoids reconstructing
locale-sort options. Only 20–500 rows, according to the existing page-size
selector, enter the DOM. Search is debounced by 170 ms; details reuse the
selected page's record. Existing tabs, filters, relationships, avatars and
responsive layout remain.

If Blob Workers are blocked, cooperative fallback parsing/filtering yields
periodically and sorting uses a yielding merge sort. This path is slower for
large reports. Browsers still read a large data block and retain an index: the
implementation does not claim constant memory or zero browser pauses.

## Measurements

Single local runs against the uploaded ZIP used synthetic data in the same
Linux environment. They are not live TikTok/Fintok/Webshare measurements.
CPU/storage/browser differences affect results. Small raw results are included
in benchmarks/.

### Scanner: 500 accounts, 1,500 mock requests

Both versions used Python 3.12.14, 10,000 desired workers, zero configured
delay, fixed 20 ms mock latency and real SQLite/export writes.

| Metric | Uploaded | Updated |
| --- | ---: | ---: |
| Scan time | 7.571 s | 5.002 s |
| Accounts/sec | 66.04 | 99.95 |
| Mock requests/sec | 198.12 | 299.86 |
| Peak simultaneous requests | 500 | 8 |
| Tasks observed at dispatch | 1,024 | 141 |
| Event-loop delay, 95th percentile | 108.40 ms | 1.14 ms |
| Maximum sampled event-loop delay | 1,722.57 ms | 21.59 ms |
| Peak process RSS | 180.33 MiB | 64.59 MiB |

Tasks are sampled at dispatch, not exhaustively counted over creation.
Earlier Python 3.11 stability runs completed all 500 accounts with 4 and 5,000
desired workers, peaking at 4 and 8 requests respectively. These are not the
identical-interpreter comparison above. Mock speed must not be used as a real
API-rate target; real paging, latency, proxies, errors and delays dominate it.

### Report: 180,000 synthetic accounts

Python generation used 3.12.14; browser checks used offline headless Chromium
153.0.8010.0. Peak Python RSS includes fixture construction and generation.
Minor later template edits can change the exact HTML byte count.

| Metric | Uploaded | Updated |
| --- | ---: | ---: |
| Python generation | 32.978 s | 18.292 s |
| HTML bytes | 186,209,109 | 118,536,433 |
| Peak Python process RSS | 1,961.14 MiB | 656.53 MiB |
| Browser load to usable page | 3,919 ms | 2,844 ms |
| Username sort after numeric reorder | 9,319 ms | 499 ms |
| Numeric ascending sort | 82 ms | 131 ms |
| Next page | 51 ms | 60 ms |
| Search including debounce | 196 ms | 227 ms |
| Status filter | 101 ms | 73 ms |
| Largest sampled interaction UI lag | 9,285 ms | 302 ms |
| DOM elements on final 20-row page | 797 | 799 |
| Report HTTP requests | 0 | 0 |

Worker messaging adds overhead to some small actions. The main benefit is
moving expensive dataset work off the UI thread and speeding username sorts.
The HTML remains substantial; not every action is faster or entirely free of
pauses. A 1,000-row Worker-blocked test also passed, as did moved-file offline
avatar/missing-image tests. See TESTING.md for reproduction and limitations.

## Compatibility and references

Actual Python 3.11/3.12 execute the complete suite. No new runtime dependency
is added beyond existing HTTPX/SOCKS and Pillow. Old configurations default the
optional connection ceiling; databases receive additive tables/indexes/counters.
Output filenames, including sucess_find.json, remain. Resume updates the same
report with old and new data. Keep report_worker.js beside the generator; its
code is embedded in generated reports. Windows runtime and real service/proxy
account availability were not tested here.

Official references used:

- [Python 3.11 asyncio](https://docs.python.org/3.11/library/asyncio-task.html)
- [HTTPX async clients](https://www.python-httpx.org/async/)
- [HTTPX resource limits](https://www.python-httpx.org/advanced/resource-limits/)
- [HTTPX event hooks](https://www.python-httpx.org/advanced/event-hooks/)
- [HTTPX proxies](https://www.python-httpx.org/advanced/proxies/)
- [Webshare connections](https://apidocs.webshare.io/proxy-connection)
- [Webshare concurrency](https://help.webshare.io/en/articles/8375281-what-is-concurrency)
- [MDN Web Workers](https://developer.mozilla.org/en-US/docs/Web/API/Web_Workers_API/Using_web_workers)
