# Verification

Run from the extracted project folder:

```cmd
py -3.11 -m pip install -r requirements-test.txt
py -3.11 run_tests.py
```

The current suite contains **170 tests**, with additional parameterized cases.
It was run on actual Python **3.11.16** and **3.12.14**, on Linux, with HTTPX
0.28.1 and Pillow 12.3.0. Tests use synthetic Worker responses, mock transports
and local TLS/CONNECT proxy servers. No real account search or Webshare secrets
are required. The runner selects tests explicitly to avoid rerunning inherited
test classes as accidental duplicates.

## September 27 avatar verification

The five additional tests cover saved avatars for pending/error/partial accounts,
input-file avatars, background backfill, global-stop admission and cancellation
before cache closure. Logs are in `verification/20260927_avatars/`.
The browser harness checks Overview, All Accounts, Errors, Skipped Accounts,
Target Matches and Mutuals in both browser-worker and cooperative rendering modes.
It also checks the details dialog, search, filtering, sorting, paging, mobile
layout, placeholders, zero remote requests and zero JavaScript errors.

The repair CLI was run twice against an offline fixture: the second run reused
the cache, scan configuration/state bytes did not change, and the report template
and browser worker remain byte-for-byte equal to the preceding speed release.

These are local/mock/loopback checks, not live TikTok or Webshare benchmarks.
The historical results below describe the previous release.

## September 26–27 release results

The final Python 3.11.16 and 3.12.14 suites each pass **165 tests**, including parameterized subcases. Exact logs are in `benchmarks/20260926/tests_release_311.log` and `tests_release_312.log`. Eight speed-update tests cover default/delta report reconstruction, closed report database handles, Unicode/duplicate member export, pending-export recovery, fast credential cleaning, configured connection ceilings, independent disk ownership under repeated cancellation, explicit DNS/TLS/disconnect recovery/exhaustion, and repeated SIGINT during report generation (some tests cover multiple items).

Production source remained unchanged during the final benchmark and verification runs. All comparisons and caveats are in **BENCHMARK_REPORT.md**. The 67 measured comparison rows include slowdowns as well as gains; all primary comparisons have three samples. Additional local scans cover 1,000 completed profiles and the 64/400/1,000/2,500/5,000/10,000 worker settings. Large resume/report/finalization uses 180,000 synthetic jobs. These large fixtures do not pretend to be 180,000 completed live API scans.

## September 26–27 release results

The final Python 3.11.16 and 3.12.14 suites each pass **165 tests**, including parameterized subcases. Exact logs are in `benchmarks/20260926/tests_release_311.log` and `tests_release_312.log`. Eight speed-update tests cover default/delta report reconstruction, closed report database handles, Unicode/duplicate member export, pending-export recovery, fast credential cleaning, configured connection ceilings, independent disk ownership under repeated cancellation, explicit DNS/TLS/disconnect recovery/exhaustion, and repeated SIGINT during report generation (some tests cover multiple items).

Production source remained unchanged during the final benchmark and verification runs. All comparisons and caveats are in **BENCHMARK_REPORT.md**. The 67 measured comparison rows include slowdowns as well as gains; all primary comparisons have three samples. Additional local scans cover 1,000 completed profiles and the 64/400/1,000/2,500/5,000/10,000 worker settings. Large resume/report/finalization uses 180,000 synthetic jobs. These large fixtures do not pretend to be 180,000 completed live API scans.

## Recovery, integrity and current changes

- Resume Y requeues network-timeout, network-error and partial statuses; N
  preserves them. Completed accounts are reused without requests. Tests cover
  finished searches with eligible failures, the CLI prompt, same-folder resume
  and the legacy response-timeout status.
- Partial retries keep committed pages, exact opaque cursors, completed lists
  and target matches. Requeue history and pending state are transactional.
  Forced termination after requeue/claim recovers safely.
- Existing process-kill tests cover page/result persistence windows, saved
  cursor continuation, committed export recovery, Ctrl+C and SIGTERM.
- A slow-body 429 stops from response headers without reading the body. A
  10,000-worker/1,000-job test checks bounded tasks, retains one previous
  completion, keeps 999 accounts pending, writes rate-limit statistics and
  prevents further dispatch. Bootstrap and delayed retries obey the same stop.
- Client pools remain bounded with many routes and share an origin TLS context
  with hostname verification. A synchronized delayed-client-setup regression
  proves preparation cannot bunch paced requests.
- Disk-writer cancellation preserves an admitted commit with backpressure and
  a responsive event loop.
- Injected clocks verify rolling/lifetime statistics, unique profiles,
  requests/retries/recoveries, latency, ETA thresholds, zero-rate/unavailable
  ETA and rate-limit status precedence. Simulated ticks verify 15-second writes
  and 300/600-second history without waiting ten wall-clock minutes. History
  append, resume sessions and torn-tail repair are tested.
- Configuration covers ordinary, 400, 5,000 and 10,000 desired workers, bounded
  connections and old configuration compatibility.

## Existing coverage retained

- Exact profile/member fields, privacy strings versus booleans, formatted
  counts, numeric UID strings, case-insensitive matching and deduplication.
- Normal/double-phase scans, the 398-account bootstrap fixture, natural-end
  count mismatches, empty/restricted lists, malformed members, missing/stalled
  cursor handling, raw-data controls and no invented cursors.
- Immediate matches, tri-state relationships, mutuals, observation timestamps,
  UID aliases, duplicate recovery and Phase 2 discovery.
- Restricted-result reuse/migration, private/not-found/error counts, checked
  progress, resumed totals and size limits.
- One additional HTTP 404 attempt with identical route/UID/cursor/page, normal
  5xx/timeout recovery afterward, bounded exhaustion, and 220 committed pages
  preserved before a later failure. Worker 429 always stops.
- Webshare four-field/URL/gateway parsing, special credential characters,
  HTTP/SOCKS settings, single/multiple proxies, real local CONNECT auth
  isolation, bad credentials, dead routes, timeouts, fallback and redaction.
- Optional proxy validation, failure exclusion, retesting, skip behavior,
  explicit direct choice and pre-start order; no restriction bypass rotation.
- Concurrent folder allocation, Windows filename sanitization, isolated
  temporary files, same-folder resume and redacted append-only logs.
- Shutdown reports, atomic replacement failure, preserved success/report
  metadata and actual `main.py` integration.
- Valid/invalid/oversized/expired/timeout avatars, byte/pixel limits, global
  pacing, duplicate/restart/rename cache reuse, atomic replacement, metadata
  stripping, missing/corrupt caches, interrupted downloads, host timeout
  suppression and old-record backfill.
- Legitimate credential substrings in profile/member data remain intact;
  authentication fields, credential-bearing URLs and diagnostics are sanitized.
  Saved Worker origins remain compatible and invalid origins are rejected.
- Report script escaping and literal template markers inside metadata.

## Performance and browser checks

Reproduction instructions and all timing scopes are in `BENCHMARK_REPORT.md`.
New harnesses include `benchmark_suite.py`, `benchmark_components.py`,
`benchmark_export.py`, `benchmark_saved.py`, `benchmark_shutdown.py` and
`benchmark_sparse_report.cjs`. Result paths must have an existing parent folder.
All use synthetic data; network benchmarks use loopback TLS/CONNECT servers.

The original rich-fixture generator remains available:

```cmd
py -3.11 tests/benchmark_optimization.py report --output C:\Temp\report_benchmark_new --accounts 180000
```

The optional browser harness requires Node.js, Playwright and Chromium, which
are development tools rather than application dependencies:

```text
node tests/benchmark_report.cjs /absolute/path/report.html /absolute/path/chromium
node tests/benchmark_report.cjs /absolute/path/report.html /absolute/path/chromium fallback
```

Set `PLAYWRIGHT_MODULE` if needed. Chromium 153.0.8010.0 tested all three sparse
180,000-row reports in three separate launches per version, entirely offline.
The updated sparse report averaged 553 ms to its first visible results. An
additional rich 180,000-row fixture passed numeric/username sorts, 20/500-row
pages, search, filters, tabs, dialogs, relationships, keyboard dismissal,
script escaping, bounded DOM and mobile layout. It became ready in 1,012 ms
with its Worker and 9,423 ms with Workers deliberately disabled. The fallback's
first numeric sort took 6,708 ms; it yields to the UI but remains slower.
These rich times are single functional verification runs, not three-run claims.
All checks produced zero page errors and zero external HTTP requests.

`tests/check_report_avatars.cjs` also passed after regenerating an existing
synthetic 1,000-row avatar fixture with this release. It copied only HTML/assets
to a new directory and verified offline table/dialog avatars, mobile dimensions,
missing-image placeholders and zero HTTP requests. No user image was modified.
Browser metrics and final verification logs are included with the measurements.

## Verification limits

Windows was not available for execution. Its path/name rules have automated
coverage, but socket/disk behavior can differ. Live Worker/CDN availability,
real Webshare credentials/plan limits and real API speed are not certified.
The optional application proxy check tests selected routes when run by the
user. Primary browser timings have three local measurements; rich/fallback checks are single
verification runs. Even the optimized 180,000-row HTML is substantial. No new coverage-percentage claim is made.
