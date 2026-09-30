# Historical speed-release instruction review — 27 September 2026

This review covers the preceding speed-release specification. It does not certify
the subsequent hybrid or combined maximum-throughput specification. See
`HYBRID_WORK_STATUS.md` for the current status, baseline results and missing-source
blocker. The new avatar fix is verified by the 170-test suite and offline browser
checks described in `TESTING.md`.

The entire supplied performance-optimization Markdown was read before changes and reviewed again against the finished release. Its required clarifying questions were answered: compare the original ZIP, use the screenshot as the desired console-information reference, optimize the exact currently running ZIP, and analyze the attached scan configuration/statistics/log. Earlier features and API/privacy/pagination requirements remain in force. No new image edits were requested or made.

| Requirement | Implementation and evidence |
|---|---|
| Continue the existing project | Updated the supplied current ZIP; retained its modules, entry point, schemas, settings and report controls. No replacement framework or new runtime dependency. |
| Investigate before editing | Read both versions and provided run artifacts; original/current synthetic baselines preceded performance edits. Detailed findings and limits in BENCHMARK_REPORT.md. |
| Fair three-version comparisons | Same Python/data/workers/latency/routing/raw settings, sequential samples, three repetitions; method and environment restart disclosure included. |
| Exact multipliers and regressions | 67 comparison metrics, means/min/max/medians, deltas, percentages and explicit slower results; raw JSON retained. |
| Networking and clients | Reused HTTPX clients per route, bounded pools, verified TLS, direct/one/four-proxy loopback measurements; credential isolation asserted. |
| Worker/task/queue architecture | Visible configured/effective ceilings, dynamic bounded tasks, durable indexed job queue, targeted capacity wakeups; all six requested worker levels measured. |
| Maximum useful resources | Removed implicit 64/256 connection caps; no guessed RAM or plan restriction. POSIX uses actual descriptor allowance. Automatic I/O responds to measured CPU/I/O; explicit override remains. |
| Sequential cursor dependencies | Captured protocol unchanged: users array, exact returned cursor, start 0, explicit termination rules, stalled/malformed cursor handling. No invented cursors or speculative pages. |
| Profile and member schemas | Profile status checked before data; userid vs member user_id strings; username vs uniqueId; private string vs privateAccount boolean; formatted counts/metadata preserved. Existing schema tests retained. |
| Matching and deduplication | Case-insensitive unique username and stable numeric UID identity; never nickname identity. Duplicate-safe SQLite page commits and immediate match recording. |
| Global pacing | All Worker attempts share admission/dispatch spacing across proxies, including delayed client setup and retries. |
| First HTTP 429 stops globally | Header hook sets stop before reading slow body; claim/send/retry paths honor it. High-worker test preserves one previous completion and 999 pending jobs. No 429 retry or restriction-bypass rotation. |
| Transient retries | Shared delayed retry mechanism releases permits while waiting; 404 allowance, 500/502/503, proxy and timeout behavior retained. Explicit synthetic DNS/TLS/disconnect recovery and exhaustion tested. |
| Retry failed profiles on resume | Y/N choice requeues timeout/network_error/partial/legacy response_timeout transactionally. Completed lists, cursor checkpoints, previous matches and success identity survive. |
| Resume and crash recovery | Pending filename inventory optimization retains export repair/stamps/legacy migration; process-kill windows and committed cursor/result recovery tests. Large pending and small completed resume measured separately. |
| Ctrl+C once/repeatedly | First signal cancels session once; admitted writes finish; guard persists through report generation. Real subprocess tests verify final state, one report and same-folder resume. Forced kill cannot run cleanup but committed state remains recoverable. |
| Durable file writes | SQLite FULL/WAL page+cursor transactions, fsync, atomic JSON replacement and target saves retained. Independent profile I/O cannot race that profile's close. Shared state stays ordered. |
| JSON performance | Single serialization per atomic write, compact raw whitespace only, fast safe redaction, encoded member export, ordered discovery aggregation. Lossless output checks and independent export/commit/JSON timings. |
| Checkpoint/statistics costs | Large-state checkpoint and summary measurements; independent statistics counter/snapshot timing; scan event-loop lag. Unmeasured hardware/lock costs explicitly identified. |
| Rich console information | Phase, all outcome counts, queue, requests/retries, request/account rates, workers, elapsed/ETA, important errors/checkpoints/429 visible. Routine detail batched; complete run.log retained. Screenshot not edited. |
| Periodic statistics/history | About 15-second current JSON plus five-minute detail/history and final writes; recent/session/overall scopes, active requests, phase, I/O, queue and guarded ETA. Clock tests cover 300/600-second history and torn tails. |
| Console speed tests | Normal/minimal/log-disabled benchmark-only modes measured three times each; same persistence/raw behavior. Windows terminal rendering not claimed. |
| HTML generation | Closed database readers, inventory/bulk metadata lookup, lossless default/delta encoding, identical fields/data/controls; 180k generation and size comparisons. |
| Browser performance/features | Three sparse 180k browser runs per version; additional rich 180k Worker/fallback and 1k local-avatar checks. Paging, sorting, search/filter, tabs, details, matches/mutuals, mobile, escaping and offline behavior pass. |
| Memory and GC pressure | Bounded tasks/cache/queues, fewer copies/encoding cycles and report metadata reuse; measured process peak RSS. Whole-browser/Worker heap and long-duration leaks not certified. |
| Webshare support | Four-field/URL/gateway authentication retained; static/rotating routes preserved, proxy-only/direct fallback, separate proxy/Worker failures, authentic CONNECT headers and redaction tested. Official Webshare/HTTPX references retained. |
| Existing scan modes | JSON and username input, bootstrap dataset generation, normal/double phase, one-level discovery, all original skip/restricted/private/raw behaviors preserved. 398-account and 220-page regression fixtures retained. |
| Per-run isolation | Atomic Windows-safe target/_2/_3 folder allocation; all runtime outputs/checkpoints/raw/temps stay in that folder; same-folder locked resume. Concurrent folder tests retained. |
| Reports after resume | Regenerated report reads saved old and new results from the selected run; atomic replacement of that run's report, no cross-run mixing or deletion of other searches. |
| Error/failure scenarios | Invalid/malformed/empty/slow responses, dead/auth/timeout proxies, connectivity failure, 5xx/429, repeated signals, crashes, duplicates and recoverable checkpoints covered by the regression suite. |
| Full review | Reviewed changed runtime/report/statistics/persistence paths and documentation against the Markdown; removed stale 64-default documentation and corrected benchmark-only fixture assumptions. |
| Compatibility and deliverable | 165 tests pass on Python 3.11.16 and 3.12.14/Linux. Windows-specific path behavior is tested, actual Windows execution is not. Runnable single-folder ZIP includes source, templates, tests, benchmark data and documentation. |

## Explicit limits

No live API/Webshare stress benchmark or user-machine execution was performed. Large synthetic datasets exercise state/report paths, not 180,000 completed API profiles. Socket/DNS/TLS failure behavior is injected in tests, with separate real local successful TLS and authenticated proxy tests; no internet DNS or live TLS-failure timing is claimed. Benchmark ratios apply to their stated workload and do not promise 400 or 10,000 requests per second. Cooperative browser fallback remains slower than the normal Worker engine. The local direct/single-proxy transport regression versus the original is reported, not hidden.
