# Verification

Run from this folder on Windows:

```cmd
py -3.12 -m pip install -r requirements-test.txt
py -3.12 run_tests.py
```

Python 3.11 is also supported; use py -3.11 in the same commands. On Linux run python3.12 or python3.11. The explicit runner avoids inherited unittest classes being collected twice.

## Current and historical gates

- Runtime f2991c2: **224 tests passed locally on Python 3.12**, including the new terminal-checkpoint and Worker retry-budget regressions. Exact output: verification_runs/20261001_local/tests_312.log.
- Final Windows/Linux 3.11/3.12 CI: run **36822278401**, pending reconciliation. Windows excludes six POSIX process-signal tests and skips the repeated POSIX SIGINT harness. Native Windows handle ownership, disk cleanup, scheduling and socket/proxy checks are included.
- Previous complete green gate: verification_runs/36761511342/regressions, **222 tests on Linux** and **216 on Windows (one skipped)**. Do not conflate that count with the two subsequently added tests.
- Unmodified upstream repeat: **2,529 unit/replay and 528 integration passes**, with PostgreSQL 17/TimescaleDB and Redis 8. See hybrid_baseline/ci_verified_20260930.json. Historical setup/DNS failures remain in the original logs.

## Behavioral coverage

| Area | Test modules / evidence |
|---|---|
| Worker contract, profile/list fields, pacing, proxies, retries and failure classification | test_async_scanner, test_configured_scanner, test_proxy_wire |
| Direct signing, normalization, risk responses, mode selection, routing, isolation, cursor ownership, racing/cache | test_hybrid_backend, test_hybrid_review, test_hybrid_audit |
| Worker 429 with 100+ queued and 2,500 active requests; both list directions; central Direct cooldown/probe/ramp/stop; durable resume | test_rate_limit_control, test_hybrid_audit |
| Atomic pages/exports, forced process interruption, failed-profile requeue, resumed aliases/targets, count mismatches, two-phase discovery | test_persistent_scanner, test_prior_features, test_latest_requirements, test_optimization, test_release_checks |
| Bounded scheduling, no fixed 64-worker ceiling, repeated cancellation and signals, file-handle cleanup | test_speed_update, test_hybrid_audit, test_async_scanner |
| Run-folder allocation, credential redaction, previous saved-config compatibility | test_run_folders, test_configured_scanner, test_release_checks |
| Avatar cache/repair, pending/error/partial profiles, placeholders, byte-preserved scan state | test_avatar_update, test_report_avatar_fix |
| Metrics/history, resumed session accounting, 15-second snapshots and five-minute history/display | test_optimization, test_hybrid_audit |

Tests use synthetic API responses and real local TLS/CONNECT servers. No live account search or Webshare credentials are required. An HTTP 200 alone is not considered valid profile/list data.

## Matched performance and browser checks

Final CI run 36822278359 measures original, first-hybrid and final source variants on one runner per comparison group. Cases run sequentially inside that group and source snapshots remain immutable while results are committed. It covers:

- 56 full pipeline cases: three modes where supported; 100/500/1,000/2,500/5,000 configured workers; both 10,000-member directions; 1,000 and 10,000 mixed accounts; raw captures, SQLite, exports, report and resume.
- 35 real verified loopback TLS cases through each production dispatcher, including all 5,000 logical jobs. Connections, CPU/RAM, wire bytes, latencies and event-loop lag are measured. The fixture origin shares the process, so its CPU cost is included.
- Original/first/final components and CPU profiles; separate logging/statistics comparisons; aggregate queue, lock, pool, signing, parse, normalization and persistence timings. Overlapping task-duration percentages are not an additive CPU partition.
- 180,000-account reports in browser-worker and cooperative fallback modes: search, filtering, sorting, 20/500-row pages, details and mobile layout. Six-section avatar checks include five cached images, a missing-image placeholder, idempotent repair and unchanged scan state. All run offline.
- Single/repeated SIGINT in real subprocesses with one report, 1,000 preserved pending jobs and same-folder recovery, for all three source variants.

The September matrices are preserved separately. Their analysis is verification_analysis/20260930/MEASUREMENTS.md. Earlier hybrid_measurements/current results used a harness that did not select the production Worker-only factory; they are historical and do not supply current ratios. The initial TLS fixture username error was fixed in the fixture, not by weakening validation.

To reproduce an individual scan measurement:

```cmd
mkdir verification_output
py -3.12 tests/benchmark_hybrid.py --source . --backend hybrid --workers 2500 --accounts 1000 --latency .001 --result verification_output/hybrid.json
```

Use --profile for a separate CPU-profile run. --io-workers permits a controlled persistence-thread comparison. Benchmark timings must not be compared across different machines.

Browser harnesses require Node, Playwright and Chromium only for development. The pinned setup is in the browser/final verification workflows. Set PLAYWRIGHT_MODULE when Playwright is installed outside the project, then use tests/benchmark_sparse_report.cjs and tests/check_avatar_sections.cjs with an absolute report and Chromium path.

## Remaining live scope

No production TikTok compatibility, live request rate, maximum reliable page size, proxy-plan capacity or real Windows console key injection is certified by offline fixtures. Follow LIVE_VERIFICATION.md for authorized accounts/session setup and manual checks. The application and both specifications are audited in REQUIREMENTS_AUDIT.md, SPECIFICATION_AUDIT.json and HYBRID_COMPLETION_CHECKLIST.csv.
