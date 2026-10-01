# Matched hybrid verification measurements

Generated from preserved measurements. Each comparison uses variants from the same runner/job. Scan, network, browser and shutdown jobs use separate hosts; no ratios cross those jobs. These are single samples per matrix cell. Small differences are observations, not statistical speed guarantees.

## Ten thousand mixed accounts

| Version / mode | Seconds | Successful RPS | Accounts/s | CPU seconds | Peak RSS MiB |
|---|---:|---:|---:|---:|---:|
| original / worker | 199.019 | 150.74 | 50.25 | 194.421 | 567.8 |
| first / worker | 205.905 | 145.70 | 48.57 | 202.904 | 579.8 |
| first / direct | 287.084 | 104.50 | 34.83 | 279.935 | 592.8 |
| first / hybrid | 240.937 | 124.51 | 41.50 | 236.209 | 588.4 |
| final / worker | 215.670 | 139.10 | 46.37 | 204.699 | 586.3 |
| final / direct | 273.153 | 109.83 | 36.61 | 275.126 | 654.3 |
| final / hybrid | 244.387 | 122.76 | 40.92 | 244.517 | 637.9 |

Both follower and following counts cycle through 0, 1, 4 and 10 per profile. Raw captures, SQLite, exports and report/resume code are real. Transport latency is synthetic. CPU seconds include the scan and post-scan report/resume; completion_seconds is scan time.

## Ten thousand followers and ten thousand following

| Version / mode | Seconds | Requests | Users/follower page | Followers/s |
|---|---:|---:|---:|---:|
| original / worker | 5.947 | 1001 | 20.00 | 1681.50 |
| first / worker | 6.164 | 1001 | 20.00 | 1622.19 |
| first / direct | 6.429 | 573 | 34.97 | 1555.52 |
| first / hybrid | 6.268 | 1001 | 20.00 | 1595.44 |
| final / worker | 6.380 | 1001 | 20.00 | 1567.38 |
| final / direct | 6.554 | 573 | 34.97 | 1525.75 |
| final / hybrid | 6.471 | 1001 | 20.00 | 1545.44 |

## Real loopback TLS successful RPS

| Version / mode | 100 | 500 | 1,000 | 2,500 | 5,000 workers |
|---|---:|---:|---:|---:|---:|
| original / worker | 166.10 | 156.92 | 154.89 | 159.16 | 167.82 |
| first_hybrid / worker | 162.00 | 157.72 | 158.47 | 158.69 | 176.39 |
| first_hybrid / direct | 126.38 | 124.39 | 119.92 | 120.90 | 120.73 |
| first_hybrid / hybrid | 157.65 | 145.27 | 148.48 | 148.19 | 149.34 |
| final / worker | 156.71 | 155.68 | 154.08 | 152.95 | 153.27 |
| final / direct | 128.80 | 119.58 | 121.95 | 120.05 | 118.41 |
| final / hybrid | 166.58 | 150.20 | 147.99 | 145.30 | 148.88 |

Every configured logical worker sends two sequential requests. HTTPX, TLS verification, signing, pooling and production backend selection are real. The local origin shares the process/CPU, so this measures the combined client/server fixture, not TikTok or proxy-plan capacity. Kernel loopback byte counters include both ends. Full active/idle connections, latencies, network bytes, CPU/RAM and waits are retained in network_comparisons.csv and source JSON.

## Offline 180,000-account browser

| Version / engine | Load ms | Sort ms | Search ms | Maximum initial UI lag ms |
|---|---:|---:|---:|---:|
| original / worker | 806.86 | 108.78 | 204.08 | 77.30 |
| original / fallback | 9341.53 | 7078.01 | 353.74 | 69.40 |
| first / worker | 809.64 | 109.83 | 200.23 | 62.20 |
| first / fallback | 9312.97 | 7087.06 | 362.63 | 64.10 |
| final / worker | 720.95 | 110.41 | 202.40 | 40.40 |
| final / fallback | 1320.38 | 635.47 | 213.55 | 77.10 |

All cases validate 180,000 searchable accounts, page sizes 20/500, sorting, filters, details and mobile layout, with zero external requests and JavaScript errors. Six-section avatar checks additionally validate saved pictures, placeholders and idempotent repair.

## Interpreting the complete comparisons

- CSVs include every available numeric metric, including slowdowns. Empty cells mean the older source did not expose that metric; they do not mean zero.
- stage_timings.csv reports aggregate task/operation elapsed durations as percentages of wall time. Concurrent waits overlap, so values can exceed 100%. Signing includes executor wait. Disk stages include the called operation and any internal JSON/SQLite work. They are not exclusive CPU shares.
- CPU profiles preserve per-function exclusive/cumulative observations in the source run. Cumulative coroutine/thread times overlap and must not be added together.
- Full-pipeline 100-job worker-scaling cases cannot activate more than 100 profile owners. The separate TLS matrix activates all 100–5,000 configured jobs and records actual connection use.
- The original scanner already contains its prior report/persistence speed release. The first hybrid and final variants add signing/routing/metrics; universal speedup is not assumed.
- Live compatibility, largest reliable page size, real service risk rates and live upstream baseline throughput remain unverified; see LIVE_VERIFICATION.md.

Exact runs: scan=36759496281, network=36761511421, browser=36761289359, shutdown=36760498050.
