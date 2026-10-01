# Measured performance and verification — 26–27 September 2026

This release preserves the scanner's features and improves the measured local pipeline. The controlled baseline did **not** reproduce a general slowdown of the current ZIP versus the original ZIP: the current code was already faster on that workload. The supplied live run did reveal a hidden 64-connection default, slow concurrency growth, and lengthy shutdown/report work. No claim about the exact cause of remote API latency is made without a comparable live trace.

## Current-runtime acceptance pass — 1 October 2026

The final acceptance pass measures runtime commit `1e069ffb57fdf09bf6575d02576dbc5828cc2d21` with scanner source SHA-256 `c83b9de10f1b9556fa6c368d0d582674202cf734c4631eef16fa36481fa010dd`. The matched scan matrix is run **36852148338**; current network/CPU/browser evidence is run **36852148287**; fresh shutdown/resume is **36854871728**; fresh four-platform regression is **36854871750**. Offline/local evidence is not a live TikTok throughput claim.

### Current 10,000 mixed-account fixture

| Version / mode | Completion s | Successful RPS | Accounts/s | CPU s | Peak RSS MiB |
|---|---:|---:|---:|---:|---:|
| Original / Worker | 207.601 | 144.51 | 48.17 | 197.728 | 570.5 |
| First / Worker | 216.262 | 138.72 | 46.24 | 210.831 | 581.7 |
| First / Direct | 283.547 | 105.80 | 35.27 | 276.749 | 592.1 |
| First / Hybrid | 249.522 | 120.23 | 40.08 | 241.091 | 586.5 |
| Final / Worker | 209.863 | 142.95 | 47.65 | 201.518 | 538.2 |
| Final / Direct | 270.866 | 110.76 | 36.92 | 271.152 | 560.6 |
| Final / Hybrid | 243.587 | 123.16 | 41.05 | 244.070 | 625.0 |

This current sample makes the tradeoff explicit: Final Worker is about **1.1% slower** than Original Worker on completion time, while Final Direct is about **4.5% faster** than First Direct and Final Hybrid is about **2.4% faster** than First Hybrid. These are single matched samples on one runner, not universal speed guarantees.

### Current dedicated 10,000-follower + 10,000-following fixture

| Version / mode | Completion s | Requests | Users/follower page | Followers/s |
|---|---:|---:|---:|---:|
| Original / Worker | 5.952 | 1,001 | 20.00 | 1680.06 |
| First / Worker | 6.231 | 1,001 | 20.00 | 1604.94 |
| First / Direct | 6.460 | 573 | 34.97 | 1547.93 |
| First / Hybrid | 6.327 | 1,001 | 20.00 | 1580.46 |
| Final / Worker | 6.629 | 1,001 | 20.00 | 1508.52 |
| Final / Direct | 6.454 | 573 | 34.97 | 1549.50 |
| Final / Hybrid | 6.588 | 1,001 | 20.00 | 1517.88 |

Final Worker is **11.4% slower** than Original Worker in this dedicated list fixture. Profiling shows the final path retains additional durable state/discovery and observability work, including 1,001 atomic JSON writes, 1,000 page commits and 1,000 durable discovery updates. Those operations support resume/crash guarantees; they were not removed merely to improve a synthetic benchmark. The report therefore does **not** claim universal speedup.

### Current acceptance coverage

- Matched scan comparison **36852148338** completed all 56 production-dispatcher cases plus original/first/final components and profiles.
- Real loopback TLS evidence **36852148287** completed all 35 Worker/Direct/Hybrid cases at 100, 500, 1,000, 2,500 and 5,000 configured workers.
- CPU evidence **36852148287** completed per-thread profiling and observability measurements. Cumulative coroutine/thread timing is not added as if it were exclusive CPU share.
- Browser evidence **36852148287** and fresh rerun **36854983351** completed 180,000-account Worker/fallback, search/filter/sort/page/mobile and avatar checks with zero external requests or JavaScript errors.
- Shutdown run **36854871728** completed all six original/first/final single/repeated interruption and same-folder recovery cases.
- Regression run **36854871750** passed all four Linux/Windows Python 3.11/3.12 jobs.

Measured slowdowns remain part of the evidence. Live compatibility, live upstream RPS, maximum reliable Direct page size and proxy-plan capacity remain blocked by environment and are not inferred from these fixtures.

## Versions and fairness

- Original: `TikTokScanner.zip`, SHA-256 `f8aeaa0281422f1982292e0ec86fc25657529fdf010acaccdd5aa1c9a6aa665d`.
- Current before optimization: `TikTokScanner_optimized(2).zip`, SHA-256 `59ca1309383d8af0b14bcc15493b303396e62f2ddb96a103b9caaeca129fc011`. Its source files matched the earlier September 23 delivered version.
- Updated: the source files in this release. Historical September 23 measurements remain separately labeled in `PERFORMANCE_20260923.md`.

The original and current baselines were measured before the performance edits. Final comparisons use three fresh processes per version, sequentially on the same workspace, Python 3.11.16, Linux 6.18.44, HTTPX 0.28.1 and Pillow 12.3.0. Nine CPUs were visible with an eight-CPU cgroup quota; the file descriptor limit was 16,384. Additional shutdown/export/browser verification followed the environment restart on September 27 with the same software and quota. The exact underlying host allocation is not attested across that restart; each comparative subsystem tested all three versions together. No performance tests ran concurrently with each other. Warm filesystem caches are possible for all versions; there was no explicit cache purge.

The normal full workload uses 96 profiles in the Phase 2 queue, 12 pages in each direction, 20 members per page, 2,400 requests, 2,500 configured workers, zero aggregate delay, direct routing, raw retention, normal logs, real SQLite commits, JSON exports, report generation and completed-profile resume. HTTPX MockTransport supplies a fixed 20 ms delay and exact opaque continuation cursors. It tests Python scheduling/persistence with deterministic API data, not internet throughput. Import time is separate; total workload includes dataset creation/import, scan, final exports, HTML and recovery, but excludes imports and removal of the synthetic fixture. Scan request rates use the scan duration. No optimized-only feature disabling was used.

The sparse large fixture contains 180,000 jobs, 3,600 partial statuses, and three discovery sources per account. It reflects the mostly-pending supplied scan, not 180,000 completed live accounts. The finalization benchmark reuses each version's saved fixture across three fresh processes; existing output mirrors are atomically replaced. The independent export benchmark writes 10,000 normalized saved members five times per process, verifying count/order/positions. No real account data, proxy secrets, or input scan files are included in this ZIP.

Each table uses arithmetic means unless marked otherwise. Time multiplier = old time / updated time; throughput multiplier = updated rate / old rate. A value below 1 means slower performance. Size reductions are labeled separately. Raw samples and all min/max/mean/median calculations are in `benchmarks/20260926/`; no measured sample was dropped as an outlier. Earlier tuning experiments are excluded because they ran different code, not because their timings were inconvenient.

## Overall result

Full workload: **21.711 s original → 14.647 s current → 9.271 s updated**. The updated release is **2.34× faster than the original** and **1.58× faster than the current ZIP** on this workload. The corresponding scan-only successful throughput is 113.40 → 164.54 → **260.87 requests/s**. All versions completed all 96 accounts and 2,400 requests, with zero request failures, retries and timeouts. These are local synthetic results, not a promised live request rate.

## What the supplied live run establishes

The provided configuration used 2,500 workers, zero delay, direct access, raw retention, and no `max_connections` field. The current ZIP consequently defaulted to 64 connections and began at eight, growing by two at five-second intervals. Even under ideal growth it needed roughly 140 seconds to reach 64. This could not express the requested concurrency ceiling.

The snapshot covered 617.515 seconds: 23,161 attempts, 23,012 successes, 125 failures, 24 in flight, 68 retries and 12 recoveries. Its whole-session rate was 37.51 attempts/s. The latest 300-second window reported 16,652 successful requests (55.51/s) and 114 processed accounts (0.38/s). A large account can require many sequential cursor pages; workers do not make those pages independently addressable. The last log sequence spent about 65 seconds saving after interruption, then 277.54 seconds between the final summary and report completion. Those are observed wall-clock intervals, not CPU or disk profiles of the user's PC. The supplied files do not establish the original version's live speed under identical server conditions.

## Networking, admission and worker scheduling

`runtime_ceiling`, `AsyncRequestGate.slot/observe` and `run_scan` now respect a missing/zero connection setting as automatic, initially admit up to 64, and grow after successful observations. Explicit saved limits remain respected. Global request spacing and the first-429 emergency stop still apply across every route. The FIFO admission waiters wake only eligible tasks. The dispatcher grows profile tasks with admission and remaining work instead of creating a task per discovered account. No RAM capacity or Webshare plan limit is guessed. POSIX uses the actual descriptor budget; Windows uses the configured ceiling.

The scan metrics below measure the combined scheduling, parsing and persistence pipeline. Initial queue wait means time from scan start until a job is claimed; it is not an isolated semaphore benchmark. Peak in-flight counts and sampled task counts are actual observations, not the configured worker number. Worker CPU utilization and connector occupancy were not traced continuously, so an exact percentage is not claimed. Adding useful request slots helps latency-bound workloads; this local fixture spends much of its time in Python/SQLite work.

The independent network test uses 640 requests, 32 request slots, a local verified TLS server with 5 ms response delay, and direct, one authenticated CONNECT proxy, or four authenticated CONNECT proxies. All versions use the same harness. It asserts that proxy credentials do not reach origin headers. Current and updated clients reused connections; the original created roughly one TCP/TLS connection per request. **The original still performed better in the direct and single-proxy local fixture.** Pooling/scheduling contention under this local concurrent workload remains a regression versus that original. It is retained for reliable connection reuse; current-to-updated networking changes are small and noisy. Four separate proxy client pools performed much better. These loopback proxy results do not predict a Webshare plan or remote connection speed, and are not a reason to rotate proxies after a Worker restriction. DNS timing and individual TLS handshake duration were not isolated; the measured accepted connection/tunnel counts establish reuse, not handshake speed.

## Profile processing, JSON and disk work

The hot paths were repeated cleaning/encoding, per-page state operations, and final member export. `CredentialRedactor` now avoids regex work on ordinary strings and reuses key sets; credential-bearing URLs and diagnostic fields still receive cleaning. `atomic_write_json` serializes once, while raw Worker files use compact whitespace with identical parsed content. `UserStore.iter_encoded_members` lets SQLite add the position to already normalized/redacted payloads, avoiding one Python decode/encode cycle per exported member. The serial 10,000-member export benchmark independently verifies this improvement; inclusive wall times summed over concurrent calls must not be treated as independent elapsed costs.

Per-profile databases/raw files/exports use an independent executor; shared state, target observations and statistics retain ordered writes. Each profile awaits its operations sequentially. Admitted disk work completes before cancellation can close its database. FULL synchronous WAL, atomic page+cursor transactions, fsync and atomic replacement remain. Automatic profile I/O starts one writer and grows when queued I/O leaves CPU idle; explicit `io_workers` remains available. A fixed four-writer tuning experiment was slower on this CPU-heavy fixture because competing Python work increased contention, so automatic growth uses observed CPU/I/O behavior. This is separate from network admission and assumes nothing about the user's RAM. Further improvement would require reducing CPU work or changing the durable storage model; those changes need additional evidence rather than weakening writes.

Checkpoint/summary microbenchmarks run on 180,000 rows, with 20 checkpoint samples and 100 summary samples per process. Statistics request-pair measurements run 100,000 updates, and snapshot measurements run 10,000 snapshots. The original ZIP has no equivalent `ScanStatistics` reporter, so those cells are N/A, not zero-cost speedups. The existing reporter stays on the event loop without cross-thread locks; no separate lock-wait duration is meaningful. Event-loop lag is measured during full scans, not falsely attributed entirely to checkpointing. Reported CPU is process CPU time/wall time, with 100% meaning one core; sampled peaks use short 20 ms windows and may exceed 100%. Higher CPU use with greater throughput can be useful utilization.

## Resume and finalization

`DurableScanState.recover` inventories existing exports once and skips nonexistent paths for untouched pending jobs. It retains checks for real exports, completed-file stamps, crash recovery and legacy result migration. The large sparse resume gains do not imply equal gains for 180,000 completed profiles with large exports. The small completed-profile measurement is reported separately. Failed-profile candidate lookup remains indexed SQL; the timed read does not requeue or mutate statuses. The normal resume option still requeues retryable failures and resumes saved opaque cursors while preserving completed lists and matches.

Discovery finalization uses one SQL query with ordered source aggregation instead of a Python query per account; the 180,000-row output benchmark verifies all queue entries and all three sources. It writes the same-sized JSON data for each version. Interrupt handling avoids a duplicate whole-state finalization. `ShutdownGuard` cancels the owning session once and stays active during final report generation; repeated Ctrl+C cannot cancel an admitted save. The shutdown comparison interrupts 1,000 pending jobs after a mocked HTTP request starts, then checks exit code 130, all 1,000 pending, one report and same-folder recovery. The separate repeated-signal test intentionally delays the report by 250 ms to ensure signals arrive during cleanup. Its time is not used as an optimization comparison. An initial harness attempt sent signals after the process had already saved and was exiting; the synchronized test corrected that test setup, not scanner data.

## Console and statistics output

The attached screenshot was used only to understand the requested terminal information. No image was edited. Immediate messages include errors, retries, target matches, checkpoints and stop reasons. Routine profile/page detail is batched once per second with up to 20 recent lines and an explicit omitted-line count; the full detail remains in `run.log`. The two live rows include phase, processed/total, successful/failed/partial/restricted counts, pending, request successes/failures/retries, request and account rates, configured/effective concurrency, I/O writers, elapsed time and guarded ETA.

The three console modes were measured on the identical full-scan fixture: normal; minimal terminal with full log; and console/log disabled **only in the benchmark**. Terminal output was redirected to a file, so Windows console rendering cost is not measured. Filesystem checkpoints/raw results remained enabled in all modes. Small negative overhead estimates reflect run variation rather than proof that logging makes the scanner faster. No noisy negative estimate is converted to a misleading speed multiplier.

| Version | Normal s | Minimal s | Disabled s | Normal minus disabled | Estimated normal overhead | Minimal overhead |
|---|---:|---:|---:|---:|---:|---:|
| Original ZIP | 21.165 | 20.673 | 20.149 | +1.016 s | +5.04% | +2.60% |
| Current ZIP | 14.587 | 14.351 | 13.968 | +0.619 s | +4.43% | +2.75% |
| Updated ZIP | 9.215 | 9.632 | 9.304 | -0.089 s | -0.96% | +3.53% |

Snapshots remain about every 15 seconds, with five-minute detailed output/history and final writes. Recent and lifetime request statistics cover the current execution; overall profile totals include previous executions. Rates, failures, recoveries, timeouts, proxy errors, 429/5xx, phase and queue remain present. Automatic I/O activity is also exposed. Timer-driven tests verify 300/600-second history without waiting ten real minutes.

## HTML generation and offline browser

`report_generator` now closes read connections deterministically, inventories page database filenames, and joins saved discovery payloads in bulk. Default/delta row encoding stores repeated values once and reconstructs every original field. It does not delete accounts or hide data. Existing pagination, filters, sort, search, details, source provenance, relationships, local avatars, escaping and the cooperative fallback remain. Python report generation is still substantially CPU-bound and still holds row structures in memory. Streaming those structures further could reduce memory, but was not required to achieve the measured gains and would require a separate change to the report builder.

Each version's identical 180,000-row sparse report was opened through `file://` in three independent offline Chromium 153 launches. Ready time includes navigation, HTML parse, indexing and the first 20 visible rows; it is not isolated JavaScript initialization. Page/sort/search/filter times include Playwright action overhead and search debounce. The main-thread heap readings exclude Worker heaps and browser process memory, and `performance.memory` is coarsely rounded; no whole-browser RAM reduction claim is made. External HTTP requests and page errors were zero. A separate rich 180,000-row fixture verifies numeric sorts, matches, mutuals, skipped/errors/phases tabs, details, keyboard dismissal, 20/500-row DOM bounds, script escaping and mobile layout in both Worker and cooperative fallback modes. Local avatar relocation/missing-image behavior is checked separately.

## Scaling: maximum useful concurrency

The scaling fixture runs 256 accounts, two pages per direction, 20 members/page, 1,280 requests and fixed 200 ms MockTransport latency with raw files and full durability. Three samples per setting. Request and successful request rates are equal; failures/retries/timeouts are zero throughout. Initial queue = 256 and final pending = 0. Actual TCP connections are N/A for MockTransport; observed in-flight is shown instead. This test does not simulate 10,000 simultaneously ready network requests or establish an ideal live setting.

| Workers | Ceiling | req/s = success/s | accounts/s | mean / p95 ms | CPU avg / peak % | RSS MiB | In-flight peak | Tasks peak | Claim wait ms | vs 64 throughput |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 64 | 171.41 | 34.28 | 200.70 / 201.52 | 100.0 / 164.8 | 52.58 | 64.0 | 135.0 | 2578.5 | 1.000× |
| 400 | 400 | 158.15 | 31.63 | 200.78 / 201.51 | 101.3 / 165.5 | 91.65 | 63.7 | 322.0 | 591.9 | 0.923× |
| 1,000 | 1,000 | 148.47 | 29.69 | 200.96 / 201.91 | 100.2 / 181.0 | 91.11 | 62.3 | 319.3 | 632.2 | 0.866× |
| 2,500 | 1,625 | 163.07 | 32.61 | 200.83 / 201.62 | 100.0 / 168.1 | 90.49 | 66.0 | 321.3 | 584.2 | 0.951× |
| 5,000 | 1,625 | 147.10 | 29.42 | 200.84 / 201.54 | 101.8 / 180.2 | 90.98 | 59.3 | 317.0 | 621.1 | 0.858× |
| 10,000 | 1,625 | 155.93 | 31.19 | 200.78 / 201.59 | 100.6 / 181.4 | 92.43 | 63.0 | 322.7 | 623.9 | 0.910× |

There was **no useful improvement beyond 64 configured workers in this particular short, storage/CPU-heavy fixture**. Higher settings admit more profile work and can add contention, while observed in-flight demand remained much lower than their ceilings. The non-monotonic results must not be generalized into a universal 64-worker cap. That old cap is removed; 400–10,000 remain selectable, with visible actual limits. The high settings reached a 1,625 ceiling on this POSIX machine solely because of its real descriptor budget. The Windows path does not use that POSIX limit. Sustained real network latency, pagination length, proxy plan and API capacity can change the useful setting. A separate 1,000-account, 3,000-request scan at 10,000 configured workers completed with no failures; see the raw medium result. No live server was stress tested.

Additional rich-report checks (single runs): Worker ready **1,012 ms**; cooperative fallback ready **9,423 ms**, first numeric sort **6,708 ms**, with zero page errors or external requests. The fallback yields between batches and preserves functionality, but cannot match normal Worker responsiveness at this size. Local avatars also passed relocation and missing-file checks.

## Regressions and limits

The statistics snapshot added phase and I/O activity fields and measured about 4.7% slower than the current snapshot (roughly 0.6 microseconds extra); this small cost preserves useful visibility. The comparison tables intentionally include unchanged/noisy operations. Standalone JSON parsing/pretty encoding and proxy parsing were not replaced; small differences can be benchmark noise. Member normalization was not given an invented alternate schema. The updated scan's mean claim wait and peak RSS can be higher than the current version because more profiles execute concurrently. Those are explicit trade-offs for throughput, not evidence of a leak. The independent direct/single-proxy TLS fixture remains slower than the original; local pooling contention is a remaining performance issue, not a hidden gain. Browser search may remain slightly slower because its intentional debounce and action overhead dominate. Check the exact values and percentage changes below rather than assuming every component improved.

No live Worker requests, real Webshare credentials/plan throughput, Windows execution, isolated DNS timing, disk device utilization, disk hardware bandwidth, whole-browser/Worker RAM, long-duration memory-leak proof, or isolated per-lock scheduling CPU was measured. Functional failure tests inject network/TLS/DNS-style connection errors, timeout classes, 500/502/503, proxy auth/dead/slow routes, restrictions and first-429 stops. These prove behavior in the tested cases, not service availability. Latency percentiles in MockTransport measure the delayed transport handler, not complete queue-to-durable-save time; local TLS measurements include request queue/client handling. CPU/RSS tests ran on the test host, not the user's 32 GB PC.

## Reproduction

Install `requirements-test.txt`, then use a new output directory and run each version sequentially with the same Python executable. Replace `OLDER_PROJECT` with an extracted comparison version. The harness creates synthetic files only and never contacts the production Worker.

```cmd
mkdir C:\Temp\scanner_measurements
py -3.11 tests/benchmark_suite.py scan --source . --result C:\Temp\scanner_measurements\scan_1.json
py -3.11 tests/benchmark_suite.py report --source . --accounts 180000 --keep --result C:\Temp\scanner_measurements\report_1.json
py -3.11 tests/benchmark_components.py --source . --result C:\Temp\scanner_measurements\components_1.json
py -3.11 tests/benchmark_export.py --source . --result C:\Temp\scanner_measurements\export_1.json
```

Repeat with result names `_2` and `_3`. Scaling adds `--workers 64` (or another tested level), `--accounts 256 --pages 2 --latency .2`. Console tests add `--console minimal` or `--console disabled`. `benchmark_saved.py` accepts `--fixture` pointing to the kept synthetic 180,000-row report folder. `benchmark_shutdown.py` requires POSIX signal injection and is not a Windows signal-certification tool. Browser benchmarks require development-only Node, Playwright and Chromium; `benchmark_sparse_report.cjs` takes HTML, Chromium path and output JSON. The rich fixture and browser/fallback commands are documented in TESTING.md.

## All measured comparisons

These are arithmetic means of three fresh-process samples. “Slower” describes a multiplier below 1; size ratios describe reduction, not execution speed. Descriptive CPU/task/connection counts have no favorable-direction multiplier. N/A indicates no equivalent baseline facility.

| Component | Original | Current before | Updated | vs original | vs current |
|---|---:|---:|---:|---:|---:|
| Full workload including HTML/resume (s) | 21.711 | 14.647 | 9.271 | 2.342× faster | 1.580× faster |
| Scan and durable finalization (s) | 21.165 | 14.587 | 9.215 | 2.297× faster | 1.583× faster |
| Requests/s | 113.400 | 164.543 | 260.868 | 2.300× faster | 1.585× faster |
| Successful requests/s | 113.400 | 164.543 | 260.868 | 2.300× faster | 1.585× faster |
| Accounts/s | 4.536 | 6.582 | 10.435 | 2.300× faster | 1.585× faster |
| Accounts/min | 272.159 | 394.903 | 626.083 | 2.300× faster | 1.585× faster |
| Mock transport mean latency (ms) | 170.740 | 21.001 | 20.697 | 8.250× faster | 1.015× faster |
| Mock transport median latency (ms) | 162.058 | 20.787 | 20.567 | 7.880× faster | 1.011× faster |
| Mock transport p95 latency (ms) | 274.451 | 22.191 | 21.307 | 12.881× faster | 1.041× faster |
| Mock transport p99 latency (ms) | 569.713 | 24.356 | 23.017 | 24.752× faster | 1.058× faster |
| Module import startup (ms) | 70.800 | 84.296 | 108.799 | 0.651× (slower) | 0.775× (slower) |
| 96-job dataset import (ms) | 9.087 | 11.363 | 12.169 | 0.747× (slower) | 0.934× (slower) |
| Completed-96 resume (ms) | 420.249 | 10.926 | 13.483 | 31.169× faster | 0.810× (slower) |
| Initial job claim wait (ms) | 301.514 | 38.917 | 53.078 | 5.681× faster | 0.733× (slower) |
| Event-loop p95 delay (ms) | 258.292 | 1.776 | 1.156 | 223.356× faster | 1.536× faster |
| Event-loop maximum delay (ms) | 2481.602 | 25.507 | 11.999 | 206.810× faster | 2.126× faster |
| Scan CPU time (s) | 23.828 | 14.556 | 9.408 | 2.533× faster | 1.547× faster |
| Scan CPU average (% of one core) | 109.751 | 99.381 | 101.500 | — | — |
| Scan CPU sampled peak (% of one core) | 163.219 | 115.489 | 162.218 | — | — |
| Scan process peak RSS (MiB) | 93.741 | 69.969 | 74.780 | 1.254× reduction | 0.936× reduction (larger) |
| Observed in-flight peak | 95.667 | 12.000 | 21.667 | — | — |
| Sampled task count peak | 208.000 | 166.000 | 124.667 | — | — |
| 180k pending resume (s) | 12.336 | 12.483 | 0.4208 | 29.312× faster | 29.662× faster |
| 180k checkpoint mean (ms) | 83.445 | 9.036 | 7.968 | 10.473× faster | 1.134× faster |
| 180k state summary (ms) | 84.135 | 6.830 | 6.840 | 12.300× faster | 0.999× (slower) |
| 180k failed candidate lookup (ms) | 0.8696 | 0.3414 | 0.2691 | 3.231× faster | 1.269× faster |
| JSON parse: 20 members (us) | 44.549 | 44.288 | 44.438 | 1.002× faster | 0.997× (slower) |
| Pretty JSON serialize: 20 members (us) | 179.268 | 181.071 | 194.948 | 0.920× (slower) | 0.929× (slower) |
| Credential clean: 20 members (us) | 276.589 | 275.150 | 74.980 | 3.689× faster | 3.670× faster |
| Member normalization: 20 members (us) | 55.407 | 59.129 | 56.552 | 0.980× (slower) | 1.046× faster |
| Atomic pretty JSON save (ms) | 0.8246 | 0.8337 | 0.5617 | 1.468× faster | 1.484× faster |
| Webshare entry parse (us) | 10.604 | 11.207 | 10.535 | 1.006× faster | 1.064× faster |
| 20-member durable commit (ms) | 0.9625 | 0.7463 | 0.5024 | 1.916× faster | 1.486× faster |
| 20-member duplicate page (ms) | 0.6668 | 0.6408 | 0.4362 | 1.528× faster | 1.469× faster |
| 10k-member final export (ms) | 72.300 | 71.163 | 22.517 | 3.211× faster | 3.160× faster |
| Request statistics start+finish (us) | N/A | 4.137 | 4.133 | — | 1.001× faster |
| Statistics snapshot (us) | N/A | 13.100 | 13.712 | — | 0.955× (slower) |
| Normal terminal workload (s) | 21.165 | 14.587 | 9.215 | 2.297× faster | 1.583× faster |
| Minimal terminal workload (s) | 20.673 | 14.351 | 9.632 | 2.146× faster | 1.490× faster |
| Console/log-disabled workload (s) | 20.149 | 13.968 | 9.304 | 2.166× faster | 1.501× faster |
| 180k HTML generation (s) | 39.377 | 32.184 | 19.681 | 2.001× faster | 1.635× faster |
| 180k HTML size (MiB) | 134.505 | 69.967 | 18.429 | 7.299× reduction | 3.797× reduction |
| 180k report process peak RSS (MiB) | 1482.766 | 485.667 | 484.680 | 3.059× reduction | 1.002× reduction |
| 180k browser ready (ms) | 1999.919 | 1274.445 | 552.547 | 3.619× faster | 2.306× faster |
| 180k initial UI maximum lag (ms) | 1157.567 | 129.300 | 79.800 | 14.506× faster | 1.620× faster |
| Browser next-page action (ms) | 73.753 | 56.643 | 55.908 | 1.319× faster | 1.013× faster |
| Browser username sort action (ms) | 585.336 | 94.331 | 78.887 | 7.420× faster | 1.196× faster |
| Browser search action (ms) | 191.910 | 205.324 | 208.185 | 0.922× (slower) | 0.986× (slower) |
| Browser partial filter action (ms) | 50.615 | 46.007 | 38.582 | 1.312× faster | 1.192× faster |
| 180k final queue/discovery JSON (s) | 4.523 | 4.413 | 2.733 | 1.655× faster | 1.614× faster |
| Single Ctrl+C shutdown+report (s) | 1.556 | 0.1775 | 0.0984 | 15.820× faster | 1.805× faster |
| Resume after Ctrl+C (ms) | 34.278 | 30.271 | 6.384 | 5.369× faster | 4.741× faster |
| Local TLS / 0 proxies / req/s | 194.284 | 150.306 | 156.621 | 0.806× (slower) | 1.042× faster |
| Local TLS / 0 proxies / mean latency ms | 160.484 | 210.375 | 201.862 | 0.795× (slower) | 1.042× faster |
| Local TLS / 0 proxies / p95 latency ms | 201.713 | 487.438 | 457.430 | 0.441× (slower) | 1.066× faster |
| Local TLS / 0 proxies / TCP/TLS connections | 640.000 | 24.000 | 24.000 | — | — |
| Local TLS / 0 proxies / requests/connection | 1.000 | 26.667 | 26.667 | 26.667× reuse | 1.000× reuse |
| Local TLS / 1 proxies / req/s | 143.945 | 122.175 | 116.705 | 0.811× (slower) | 0.955× (slower) |
| Local TLS / 1 proxies / mean latency ms | 220.683 | 259.178 | 272.228 | 0.811× (slower) | 0.952× (slower) |
| Local TLS / 1 proxies / p95 latency ms | 257.702 | 575.781 | 608.570 | 0.423× (slower) | 0.946× (slower) |
| Local TLS / 1 proxies / TCP/TLS connections | 636.000 | 32.000 | 32.000 | — | — |
| Local TLS / 1 proxies / requests/connection | 1.006 | 20.000 | 20.000 | 19.875× reuse | 1.000× reuse |
| Local TLS / 4 proxies / req/s | 165.007 | 362.673 | 363.085 | 2.200× faster | 1.001× faster |
| Local TLS / 4 proxies / mean latency ms | 191.113 | 87.378 | 87.041 | 2.196× faster | 1.004× faster |
| Local TLS / 4 proxies / p95 latency ms | 243.253 | 216.388 | 216.821 | 1.122× faster | 0.998× (slower) |
| Local TLS / 4 proxies / TCP/TLS connections | 624.000 | 32.000 | 32.000 | — | — |
| Local TLS / 4 proxies / requests/connection | 1.026 | 20.000 | 20.000 | 19.500× reuse | 1.000× reuse |

## Exact differences and percentages

Delta = updated minus comparison mean, in the unit stated for each metric. Percent = (updated / old − 1) × 100. Negative time/size changes are improvements; positive throughput changes are improvements. CPU/activity changes are descriptive. Ratios above use unrounded means.

| Metric | Delta vs original | % vs original | Delta vs current | % vs current |
|---|---:|---:|---:|---:|
| Full workload including HTML/resume (s) | -12.440 | -57.30% | -5.375 | -36.70% |
| Scan and durable finalization (s) | -11.951 | -56.46% | -5.372 | -36.83% |
| Requests/s | 147.468 | +130.04% | 96.325 | +58.54% |
| Successful requests/s | 147.468 | +130.04% | 96.325 | +58.54% |
| Accounts/s | 5.899 | +130.04% | 3.853 | +58.54% |
| Accounts/min | 353.924 | +130.04% | 231.180 | +58.54% |
| Mock transport mean latency (ms) | -150.043 | -87.88% | -0.3046 | -1.45% |
| Mock transport median latency (ms) | -141.491 | -87.31% | -0.2196 | -1.06% |
| Mock transport p95 latency (ms) | -253.144 | -92.24% | -0.8836 | -3.98% |
| Mock transport p99 latency (ms) | -546.697 | -95.96% | -1.339 | -5.50% |
| Module import startup (ms) | 37.999 | +53.67% | 24.503 | +29.07% |
| 96-job dataset import (ms) | 3.082 | +33.92% | 0.8056 | +7.09% |
| Completed-96 resume (ms) | -406.765 | -96.79% | 2.557 | +23.40% |
| Initial job claim wait (ms) | -248.436 | -82.40% | 14.161 | +36.39% |
| Event-loop p95 delay (ms) | -257.136 | -99.55% | -0.6195 | -34.88% |
| Event-loop maximum delay (ms) | -2469.602 | -99.52% | -13.508 | -52.96% |
| Scan CPU time (s) | -14.419 | -60.52% | -5.148 | -35.36% |
| Scan CPU average (% of one core) | -8.252 | -7.52% | 2.119 | +2.13% |
| Scan CPU sampled peak (% of one core) | -1.001 | -0.61% | 46.729 | +40.46% |
| Scan process peak RSS (MiB) | -18.961 | -20.23% | 4.811 | +6.88% |
| Observed in-flight peak | -74.000 | -77.35% | 9.667 | +80.56% |
| Sampled task count peak | -83.333 | -40.06% | -41.333 | -24.90% |
| 180k pending resume (s) | -11.915 | -96.59% | -12.062 | -96.63% |
| 180k checkpoint mean (ms) | -75.478 | -90.45% | -1.069 | -11.83% |
| 180k state summary (ms) | -77.295 | -91.87% | 0.0101 | +0.15% |
| 180k failed candidate lookup (ms) | -0.6005 | -69.05% | -0.0723 | -21.17% |
| JSON parse: 20 members (us) | -0.1107 | -0.25% | 0.1507 | +0.34% |
| Pretty JSON serialize: 20 members (us) | 15.680 | +8.75% | 13.876 | +7.66% |
| Credential clean: 20 members (us) | -201.610 | -72.89% | -200.171 | -72.75% |
| Member normalization: 20 members (us) | 1.145 | +2.07% | -2.577 | -4.36% |
| Atomic pretty JSON save (ms) | -0.2629 | -31.88% | -0.2720 | -32.62% |
| Webshare entry parse (us) | -0.0685 | -0.65% | -0.6713 | -5.99% |
| 20-member durable commit (ms) | -0.4602 | -47.81% | -0.2439 | -32.69% |
| 20-member duplicate page (ms) | -0.2305 | -34.58% | -0.2046 | -31.92% |
| 10k-member final export (ms) | -49.783 | -68.86% | -48.646 | -68.36% |
| Request statistics start+finish (us) | N/A | N/A | -0.0043 | -0.10% |
| Statistics snapshot (us) | N/A | N/A | 0.6123 | +4.67% |
| Normal terminal workload (s) | -11.951 | -56.46% | -5.372 | -36.83% |
| Minimal terminal workload (s) | -11.041 | -53.41% | -4.719 | -32.88% |
| Console/log-disabled workload (s) | -10.845 | -53.82% | -4.664 | -33.39% |
| 180k HTML generation (s) | -19.696 | -50.02% | -12.503 | -38.85% |
| 180k HTML size (MiB) | -116.076 | -86.30% | -51.539 | -73.66% |
| 180k report process peak RSS (MiB) | -998.086 | -67.31% | -0.9870 | -0.20% |
| 180k browser ready (ms) | -1447.372 | -72.37% | -721.898 | -56.64% |
| 180k initial UI maximum lag (ms) | -1077.767 | -93.11% | -49.500 | -38.28% |
| Browser next-page action (ms) | -17.845 | -24.20% | -0.7346 | -1.30% |
| Browser username sort action (ms) | -506.449 | -86.52% | -15.445 | -16.37% |
| Browser search action (ms) | 16.275 | +8.48% | 2.861 | +1.39% |
| Browser partial filter action (ms) | -12.033 | -23.77% | -7.424 | -16.14% |
| 180k final queue/discovery JSON (s) | -1.789 | -39.56% | -1.679 | -38.06% |
| Single Ctrl+C shutdown+report (s) | -1.458 | -93.68% | -0.0791 | -44.59% |
| Resume after Ctrl+C (ms) | -27.894 | -81.37% | -23.887 | -78.91% |
| Local TLS / 0 proxies / req/s | -37.663 | -19.39% | 6.316 | +4.20% |
| Local TLS / 0 proxies / mean latency ms | 41.378 | +25.78% | -8.513 | -4.05% |
| Local TLS / 0 proxies / p95 latency ms | 255.717 | +126.77% | -30.008 | -6.16% |
| Local TLS / 0 proxies / TCP/TLS connections | -616.000 | -96.25% | 0.000 | +0.00% |
| Local TLS / 0 proxies / requests/connection | 25.667 | +2566.67% | 0.000 | +0.00% |
| Local TLS / 1 proxies / req/s | -27.240 | -18.92% | -5.470 | -4.48% |
| Local TLS / 1 proxies / mean latency ms | 51.545 | +23.36% | 13.050 | +5.04% |
| Local TLS / 1 proxies / p95 latency ms | 350.869 | +136.15% | 32.789 | +5.69% |
| Local TLS / 1 proxies / TCP/TLS connections | -604.000 | -94.97% | 0.000 | +0.00% |
| Local TLS / 1 proxies / requests/connection | 18.994 | +1887.50% | 0.000 | +0.00% |
| Local TLS / 4 proxies / req/s | 198.078 | +120.04% | 0.4115 | +0.11% |
| Local TLS / 4 proxies / mean latency ms | -104.072 | -54.46% | -0.3368 | -0.39% |
| Local TLS / 4 proxies / p95 latency ms | -26.432 | -10.87% | 0.4324 | +0.20% |
| Local TLS / 4 proxies / TCP/TLS connections | -592.000 | -94.87% | 0.000 | +0.00% |
| Local TLS / 4 proxies / requests/connection | 18.974 | +1850.00% | 0.000 | +0.00% |

## Repetition ranges

Each cell is **minimum / maximum / mean / median**; units match the metric. Three fresh-process samples per version. Component means contain repeated operations inside each process; ranges below are between those process means, not individual-request percentiles. Complete machine-readable records accompany this report.

| Metric | Original | Current before | Updated |
|---|---:|---:|---:|
| Full workload including HTML/resume (s) | 21.618 / 21.889 / 21.711 / 21.626 | 14.569 / 14.786 / 14.647 / 14.584 | 8.863 / 9.777 / 9.271 / 9.174 |
| Scan and durable finalization (s) | 21.014 / 21.360 / 21.165 / 21.122 | 14.499 / 14.734 / 14.587 / 14.528 | 8.821 / 9.706 / 9.215 / 9.117 |
| Requests/s | 112.360 / 114.212 / 113.400 / 113.627 | 162.893 / 165.532 / 164.543 / 165.204 | 247.263 / 272.093 / 260.868 / 263.247 |
| Successful requests/s | 112.360 / 114.212 / 113.400 / 113.627 | 162.893 / 165.532 / 164.543 / 165.204 | 247.263 / 272.093 / 260.868 / 263.247 |
| Accounts/s | 4.494 / 4.568 / 4.536 / 4.545 | 6.516 / 6.621 / 6.582 / 6.608 | 9.891 / 10.884 / 10.435 / 10.530 |
| Accounts/min | 269.663 / 274.109 / 272.159 / 272.706 | 390.944 / 397.276 / 394.903 / 396.489 | 593.432 / 653.024 / 626.083 / 631.792 |
| Mock transport mean latency (ms) | 162.040 / 178.875 / 170.740 / 171.306 | 20.957 / 21.034 / 21.001 / 21.012 | 20.670 / 20.712 / 20.697 / 20.709 |
| Mock transport median latency (ms) | 153.837 / 166.207 / 162.058 / 166.130 | 20.742 / 20.819 / 20.787 / 20.799 | 20.542 / 20.584 / 20.567 / 20.574 |
| Mock transport p95 latency (ms) | 259.084 / 301.606 / 274.451 / 262.663 | 21.977 / 22.479 / 22.191 / 22.115 | 21.271 / 21.331 / 21.307 / 21.318 |
| Mock transport p99 latency (ms) | 320.746 / 826.178 / 569.713 / 562.217 | 23.446 / 25.021 / 24.356 / 24.600 | 22.874 / 23.094 / 23.017 / 23.082 |
| Module import startup (ms) | 66.905 / 73.246 / 70.800 / 72.247 | 70.546 / 99.612 / 84.296 / 82.730 | 63.352 / 197.992 / 108.799 / 65.052 |
| 96-job dataset import (ms) | 8.333 / 9.709 / 9.087 / 9.218 | 9.873 / 13.840 / 11.363 / 10.377 | 9.028 / 16.351 / 12.169 / 11.127 |
| Completed-96 resume (ms) | 385.015 / 478.172 / 420.249 / 397.559 | 9.351 / 11.925 / 10.926 / 11.502 | 9.311 / 18.829 / 13.483 / 12.309 |
| Initial job claim wait (ms) | 234.912 / 410.740 / 301.514 / 258.890 | 32.273 / 50.077 / 38.917 / 34.399 | 52.010 / 55.026 / 53.078 / 52.198 |
| Event-loop p95 delay (ms) | 241.326 / 284.796 / 258.292 / 248.754 | 1.672 / 1.830 / 1.776 / 1.825 | 1.123 / 1.205 / 1.156 / 1.140 |
| Event-loop maximum delay (ms) | 1812.418 / 3057.790 / 2481.602 / 2574.597 | 18.124 / 39.182 / 25.507 / 19.216 | 7.473 / 14.849 / 11.999 / 13.676 |
| Scan CPU time (s) | 23.535 / 24.027 / 23.828 / 23.922 | 14.494 / 14.671 / 14.556 / 14.503 | 9.059 / 9.850 / 9.408 / 9.315 |
| Scan CPU average (% of one core) | 108.867 / 111.099 / 109.751 / 109.288 | 99.217 / 99.545 / 99.381 / 99.380 | 100.752 / 102.206 / 101.500 / 101.542 |
| Scan CPU sampled peak (% of one core) | 145.392 / 198.337 / 163.219 / 145.928 | 113.971 / 116.886 / 115.489 / 115.611 | 157.755 / 167.273 / 162.218 / 161.626 |
| Scan process peak RSS (MiB) | 93.531 / 93.867 / 93.741 / 93.824 | 69.352 / 71.059 / 69.969 / 69.496 | 72.090 / 79.617 / 74.780 / 72.633 |
| Observed in-flight peak | 95.000 / 96.000 / 95.667 / 96.000 | 12.000 / 12.000 / 12.000 / 12.000 | 18.000 / 25.000 / 21.667 / 22.000 |
| Sampled task count peak | 192.000 / 217.000 / 208.000 / 215.000 | 166.000 / 166.000 / 166.000 / 166.000 | 121.000 / 128.000 / 124.667 / 125.000 |
| 180k pending resume (s) | 11.543 / 13.723 / 12.336 / 11.741 | 12.117 / 12.765 / 12.483 / 12.565 | 0.3756 / 0.4801 / 0.4208 / 0.4068 |
| 180k checkpoint mean (ms) | 79.274 / 88.274 / 83.445 / 82.787 | 8.304 / 9.818 / 9.036 / 8.988 | 7.759 / 8.167 / 7.968 / 7.976 |
| 180k state summary (ms) | 81.757 / 88.674 / 84.135 / 81.975 | 6.258 / 7.588 / 6.830 / 6.644 | 6.410 / 7.544 / 6.840 / 6.567 |
| 180k failed candidate lookup (ms) | 0.3717 / 1.597 / 0.8696 / 0.6401 | 0.3067 / 0.4097 / 0.3414 / 0.3078 | 0.2587 / 0.2756 / 0.2691 / 0.2731 |
| JSON parse: 20 members (us) | 42.167 / 48.157 / 44.549 / 43.323 | 42.337 / 46.592 / 44.288 / 43.934 | 42.521 / 46.932 / 44.438 / 43.862 |
| Pretty JSON serialize: 20 members (us) | 170.225 / 186.051 / 179.268 / 181.528 | 170.961 / 188.716 / 181.071 / 183.537 | 187.359 / 203.915 / 194.948 / 193.569 |
| Credential clean: 20 members (us) | 270.802 / 286.354 / 276.589 / 272.612 | 268.226 / 279.113 / 275.150 / 278.112 | 72.338 / 77.479 / 74.980 / 75.123 |
| Member normalization: 20 members (us) | 53.086 / 59.023 / 55.407 / 54.113 | 53.609 / 66.303 / 59.129 / 57.477 | 55.444 / 57.561 / 56.552 / 56.652 |
| Atomic pretty JSON save (ms) | 0.7507 / 0.9364 / 0.8246 / 0.7866 | 0.7983 / 0.8942 / 0.8337 / 0.8085 | 0.5477 / 0.5736 / 0.5617 / 0.5637 |
| Webshare entry parse (us) | 10.384 / 10.847 / 10.604 / 10.581 | 11.028 / 11.380 / 11.207 / 11.212 | 10.279 / 10.865 / 10.535 / 10.463 |
| 20-member durable commit (ms) | 0.9385 / 0.9900 / 0.9625 / 0.9591 | 0.7278 / 0.7625 / 0.7463 / 0.7486 | 0.4547 / 0.5435 / 0.5024 / 0.5089 |
| 20-member duplicate page (ms) | 0.6330 / 0.7278 / 0.6668 / 0.6396 | 0.6334 / 0.6501 / 0.6408 / 0.6390 | 0.3812 / 0.4676 / 0.4362 / 0.4599 |
| 10k-member final export (ms) | 71.349 / 74.145 / 72.300 / 71.407 | 70.055 / 72.438 / 71.163 / 70.997 | 22.296 / 22.720 / 22.517 / 22.535 |
| Request statistics start+finish (us) | N/A | 4.046 / 4.288 / 4.137 / 4.078 | 4.013 / 4.316 / 4.133 / 4.069 |
| Statistics snapshot (us) | N/A | 12.254 / 14.290 / 13.100 / 12.756 | 13.174 / 14.214 / 13.712 / 13.748 |
| Normal terminal workload (s) | 21.014 / 21.360 / 21.165 / 21.122 | 14.499 / 14.734 / 14.587 / 14.528 | 8.821 / 9.706 / 9.215 / 9.117 |
| Minimal terminal workload (s) | 19.699 / 21.360 / 20.673 / 20.959 | 13.543 / 15.229 / 14.351 / 14.282 | 8.819 / 10.229 / 9.632 / 9.849 |
| Console/log-disabled workload (s) | 20.025 / 20.298 / 20.149 / 20.124 | 13.204 / 14.713 / 13.968 / 13.987 | 9.233 / 9.392 / 9.304 / 9.286 |
| 180k HTML generation (s) | 38.244 / 40.670 / 39.377 / 39.217 | 31.312 / 33.924 / 32.184 / 31.317 | 19.437 / 19.953 / 19.681 / 19.654 |
| 180k HTML size (MiB) | 134.505 / 134.505 / 134.505 / 134.505 | 69.967 / 69.967 / 69.967 / 69.967 | 18.429 / 18.429 / 18.429 / 18.429 |
| 180k report process peak RSS (MiB) | 1482.711 / 1482.844 / 1482.766 / 1482.742 | 485.535 / 485.883 / 485.667 / 485.582 | 484.410 / 484.832 / 484.680 / 484.797 |
| 180k browser ready (ms) | 1950.759 / 2058.142 / 1999.919 / 1990.856 | 1261.285 / 1299.400 / 1274.445 / 1262.649 | 536.478 / 561.051 / 552.547 / 560.111 |
| 180k initial UI maximum lag (ms) | 979.000 / 1279.100 / 1157.567 / 1214.600 | 111.000 / 144.800 / 129.300 / 132.100 | 75.500 / 88.000 / 79.800 / 75.900 |
| Browser next-page action (ms) | 61.133 / 94.201 / 73.753 / 65.926 | 55.328 / 59.006 / 56.643 / 55.593 | 50.589 / 61.377 / 55.908 / 55.758 |
| Browser username sort action (ms) | 567.053 / 613.827 / 585.336 / 575.127 | 93.009 / 95.835 / 94.331 / 94.150 | 75.817 / 80.475 / 78.887 / 80.369 |
| Browser search action (ms) | 187.917 / 197.220 / 191.910 / 190.592 | 203.526 / 207.614 / 205.324 / 204.832 | 203.069 / 217.801 / 208.185 / 203.684 |
| Browser partial filter action (ms) | 50.028 / 51.373 / 50.615 / 50.444 | 39.107 / 58.845 / 46.007 / 40.069 | 38.342 / 38.959 / 38.582 / 38.446 |
| 180k final queue/discovery JSON (s) | 4.413 / 4.703 / 4.523 / 4.453 | 4.398 / 4.438 / 4.413 / 4.402 | 2.678 / 2.833 / 2.733 / 2.690 |
| Single Ctrl+C shutdown+report (s) | 1.470 / 1.695 / 1.556 / 1.503 | 0.1752 / 0.1790 / 0.1775 / 0.1784 | 0.0973 / 0.0998 / 0.0984 / 0.0980 |
| Resume after Ctrl+C (ms) | 33.848 / 35.003 / 34.278 / 33.983 | 28.540 / 33.122 / 30.271 / 29.151 | 6.278 / 6.559 / 6.384 / 6.316 |
| Local TLS / 0 proxies / req/s | 184.341 / 201.525 / 194.284 / 196.987 | 146.933 / 156.777 / 150.306 / 147.207 | 153.515 / 159.405 / 156.621 / 156.944 |
| Local TLS / 0 proxies / mean latency ms | 154.260 / 169.199 / 160.484 / 157.993 | 201.894 / 214.763 / 210.375 / 214.467 | 197.918 / 206.016 / 201.862 / 201.650 |
| Local TLS / 0 proxies / p95 latency ms | 188.521 / 211.291 / 201.713 / 205.327 | 456.541 / 513.698 / 487.438 / 492.075 | 422.261 / 479.632 / 457.430 / 470.397 |
| Local TLS / 0 proxies / TCP/TLS connections | 640.000 / 640.000 / 640.000 / 640.000 | 24.000 / 24.000 / 24.000 / 24.000 | 24.000 / 24.000 / 24.000 / 24.000 |
| Local TLS / 0 proxies / requests/connection | 1.000 / 1.000 / 1.000 / 1.000 | 26.667 / 26.667 / 26.667 / 26.667 | 26.667 / 26.667 / 26.667 / 26.667 |
| Local TLS / 1 proxies / req/s | 134.534 / 154.231 / 143.945 / 143.071 | 119.347 / 127.799 / 122.175 / 119.377 | 106.395 / 122.756 / 116.705 / 120.964 |
| Local TLS / 1 proxies / mean latency ms | 205.167 / 235.513 / 220.683 / 221.370 | 247.233 / 265.219 / 259.178 / 265.083 | 257.943 / 297.320 / 272.228 / 261.421 |
| Local TLS / 1 proxies / p95 latency ms | 225.967 / 285.659 / 257.702 / 261.479 | 561.324 / 593.099 / 575.781 / 572.920 | 561.073 / 690.265 / 608.570 / 574.374 |
| Local TLS / 1 proxies / TCP/TLS connections | 636.000 / 636.000 / 636.000 / 636.000 | 32.000 / 32.000 / 32.000 / 32.000 | 32.000 / 32.000 / 32.000 / 32.000 |
| Local TLS / 1 proxies / requests/connection | 1.006 / 1.006 / 1.006 / 1.006 | 20.000 / 20.000 / 20.000 / 20.000 | 20.000 / 20.000 / 20.000 / 20.000 |
| Local TLS / 4 proxies / req/s | 163.277 / 166.402 / 165.007 / 165.343 | 338.365 / 389.105 / 362.673 / 360.549 | 359.757 / 366.167 / 363.085 / 363.331 |
| Local TLS / 4 proxies / mean latency ms | 189.472 / 193.154 / 191.113 / 190.714 | 81.192 / 93.292 / 87.378 / 87.650 | 86.268 / 87.852 / 87.041 / 87.004 |
| Local TLS / 4 proxies / p95 latency ms | 236.311 / 252.190 / 243.253 / 241.257 | 189.493 / 268.242 / 216.388 / 191.430 | 191.357 / 240.568 / 216.821 / 218.537 |
| Local TLS / 4 proxies / TCP/TLS connections | 624.000 / 624.000 / 624.000 / 624.000 | 32.000 / 32.000 / 32.000 / 32.000 | 32.000 / 32.000 / 32.000 / 32.000 |
| Local TLS / 4 proxies / requests/connection | 1.026 / 1.026 / 1.026 / 1.026 | 20.000 / 20.000 / 20.000 / 20.000 | 20.000 / 20.000 / 20.000 / 20.000 |

## Completion and remaining bottlenecks

The release regression suite and offline browser checks are recorded in TESTING.md and REQUIREMENTS_REVIEW.md. Correct schemas/cursors, immediate matches, failed-profile retry, same-folder resume, per-run isolation, raw data, proxy credential isolation, aggregate pacing and header-time global 429 stop remain. The report includes existing and newly saved data when regenerated after resume. No important feature was removed to improve these numbers. Remote Worker response time, sequential page dependencies, per-plan proxy capacity, Python CPU work and durable storage costs remain practical limits; configured workers are never a promise of requests per second.
