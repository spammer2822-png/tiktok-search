# Scanner performance update — 26 September 2026

See [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md) for this release's measured original/current/new comparisons, scaling results, limitations and regressions. The earlier measurements are preserved in [PERFORMANCE_20260923.md](PERFORMANCE_20260923.md); they are historical, not measurements of this release.

## Request concurrency

`workers` still accepts 1–10,000 and keeps the value you selected. It is a concurrency ceiling, not a requests-per-second target. Missing `max_connections`, or `max_connections: 0`, now uses that worker setting. An explicit `max_connections` from 1 to 10,000 remains an optional additional limit. An existing saved value of 64 is respected; set it to 0 in `scan_config.json` while stopped to use automatic mode. Your supplied scan configuration has no explicit limit and uses automatic mode immediately.

The scanner starts at up to 64 admitted requests, doubles the effective limit at intervals of at least one second after sufficient successful responses, and can reach the configured ceiling. Observed repeated failures, severe latency growth, pacing and cooldowns still reduce useful concurrency. A positive request delay remains global across all routes. There is no assumed RAM limit or inferred Webshare plan entitlement. On POSIX, the actual file-descriptor soft limit can lower the ceiling to reserve descriptors for sockets and per-profile SQLite/WAL files; Windows uses the configured ceiling.

The console shows configured workers, the connection ceiling and effective admission. A dispatcher grows profile tasks with admission and available jobs instead of constructing one task per account. The durable SQLite queue retains all other accounts. Capacity notifications wake eligible waiters rather than broadcasting to every waiting task on each response. Followers and following pagination still uses exact returned cursors; pages within one list cannot be fetched speculatively.

## Persistence and CPU/disk utilization

Shared state and immediate target-match files retain one ordered writer. Independent per-profile databases, raw responses and exports use a separate executor. A profile awaits each operation before touching its database again. Cancellation waits for an admitted commit before closing that database, including repeated cancellation requests.

Optional `io_workers: 0` is automatic (also the default when absent). It starts one profile writer, can grow toward the detected CPU count when queued I/O leaves CPU idle, and reduces competing writers when the Python process is CPU-saturated. Explicit values 1–256 override this automatic selection, bounded by the configured profile workers. This tuning affects disk work only. More writers proved slower for the CPU-heavy local fixture; it does not constrain API concurrency. The process keeps using available network concurrency independently.

FULL synchronous SQLite WAL commits, page+cursor transactions, fsync, atomic replacement and immediate target observations remain. Raw API JSON is compact instead of indented, with the same parsed data and credential cleaning performed once. Normal user-facing configuration/checkpoint JSON remains formatted. Cleaners avoid regular expressions on plain strings; final profile exports use SQLite's JSON operation to add list positions without decoding and re-encoding every saved member in Python.

Pending resume checks enumerate existing export names once instead of resolving/stat-ing a nonexistent file for every untouched account. Completed export stamps and all legacy/crash repairs remain. Discovery exports use one SQL query with ordered source aggregation instead of a Python query for each account. Interrupt finalization no longer exports the entire queue twice.

## Console and statistics

Errors, retries, target matches, checkpoint notices and stop reasons remain visible immediately. Ordinary detail is batched once per second: up to 20 recent lines plus an explicit count of additional lines in `run.log`. No full-detail log records are discarded. Live rows include phase, totals, successes, failures, partial/restricted outcomes, pending work, requests, retries, request and account rates, active requests, configured/effective workers, elapsed time and ETA.

`scan_stats.json` is created before work, refreshed about every 15 seconds and finalized on graceful stop. Five-minute detail and append-only `scan_stats_history.jsonl` remain. Request counters include Worker API attempts; avatars and proxy validation are excluded. Saved overall progress includes earlier executions; session metrics cover the current execution. ETA still requires adequate recent successful-profile evidence and is null on stops.

## Stop, retry and resume

The first Worker HTTP 429 still closes admission synchronously at the response-header hook, cancels unnecessary in-flight HTTP tasks, prevents new sends and stops without retrying or rotating proxies. Already transmitted requests cannot be recalled. Completed responses may still commit. Pending/interrupted accounts remain recoverable in the same search folder.

The first Ctrl+C or SIGTERM requests graceful cancellation. Additional presses during saving/report generation no longer interrupt cleanup. A forced process kill cannot run cleanup, but committed SQLite data remains resumable. Keep the whole search folder, including SQLite and WAL files.

The existing retry-failed option still requeues `network_timeout`, `network_error`, `partial` and legacy `response_timeout`, preserves prior results in retry history, keeps completed profiles complete and reuses saved list cursors. All existing special 404, transient 5xx, proxy classification, credential handling and aggregate pacing rules remain.

## Offline report

The report still includes every saved row, old and new, with the same controls, fields, sources, relationship certainty and local avatars. Database readers now close connections deterministically. Bulk discovery lookup and a page-file inventory avoid repeated missing-file checks and Python SQL calls.

The embedded row representation stores shared defaults once and indexed differences for each row. The included Worker and cooperative fallback reconstruct the same objects; this is lossless storage, not omitted records. Existing report HTML files remain self-contained and unchanged until the next atomic regeneration. New reports require no additional JSON files or network access. Only the selected 20–500 rows enter the DOM.

## References checked

- [HTTPX async clients](https://www.python-httpx.org/async/)
- [HTTPX resource limits](https://www.python-httpx.org/advanced/resource-limits/)
- [HTTPX proxy authentication](https://www.python-httpx.org/advanced/proxies/)
- [HTTPX timeouts](https://www.python-httpx.org/advanced/timeouts/)
- [Webshare proxy connections](https://apidocs.webshare.io/proxy-connection)
- [Webshare concurrency](https://help.webshare.io/en/articles/8375281-what-is-concurrency)
- [Python 3.11 cancellation](https://docs.python.org/3.11/library/asyncio-task.html)

No new runtime dependency was added. Results measured on the local test machine are not a promise of a particular live Worker or Webshare request rate.
