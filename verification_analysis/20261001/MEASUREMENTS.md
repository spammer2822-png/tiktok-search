# Matched hybrid verification measurements

Generated from preserved measurements. Each comparison uses variants from the same runner/job. Scan, network, browser and shutdown jobs use separate hosts; no ratios cross those jobs. These are single samples per matrix cell. Small differences are observations, not statistical speed guarantees.

## Ten thousand mixed accounts

| Version / mode | Seconds | Successful RPS | Accounts/s | CPU seconds | Peak RSS MiB |
|---|---:|---:|---:|---:|---:|
| original / worker | 224.265 | 133.77 | 44.59 | 215.366 | 570.0 |
| first / worker | 223.954 | 133.96 | 44.65 | 215.819 | 582.1 |
| first / direct | 305.068 | 98.34 | 32.78 | 299.622 | 591.9 |
| first / hybrid | 272.925 | 109.92 | 36.64 | 264.092 | 590.3 |
| final / worker | 218.089 | 137.56 | 45.85 | 207.033 | 530.2 |
| final / direct | 279.408 | 107.37 | 35.79 | 279.947 | 619.9 |
| final / hybrid | 245.637 | 122.13 | 40.71 | 245.272 | 602.5 |

Both follower and following counts cycle through 0, 1, 4 and 10 per profile. Raw captures, SQLite, exports and report/resume code are real. Transport latency is synthetic. CPU seconds include the scan and post-scan report/resume; completion_seconds is scan time.

## Ten thousand followers and ten thousand following

| Version / mode | Seconds | Requests | Users/follower page | Followers/s |
|---|---:|---:|---:|---:|
| original / worker | 6.167 | 1001 | 20.00 | 1621.59 |
| first / worker | 6.578 | 1001 | 20.00 | 1520.16 |
| first / direct | 6.563 | 573 | 34.97 | 1523.76 |
| first / hybrid | 6.721 | 1001 | 20.00 | 1487.86 |
| final / worker | 6.564 | 1001 | 20.00 | 1523.53 |
| final / direct | 6.633 | 573 | 34.97 | 1507.67 |
| final / hybrid | 6.621 | 573 | 34.97 | 1510.39 |

## Real loopback TLS successful RPS

| Version / mode | 100 | 500 | 1,000 | 2,500 | 5,000 workers |
|---|---:|---:|---:|---:|---:|
| original / worker | 161.04 | 161.98 | 157.63 | 160.24 | 173.61 |
| first_hybrid / worker | 167.98 | 159.06 | 155.49 | 160.08 | 154.82 |
| first_hybrid / direct | 125.90 | 124.08 | 125.39 | 123.53 | 124.39 |
| first_hybrid / hybrid | 172.98 | 157.27 | 155.41 | 153.85 | 150.70 |
| final / worker | 158.82 | 149.78 | 152.15 | 153.34 | 154.20 |
| final / direct | 126.29 | 120.88 | 120.47 | 119.05 | 120.98 |
| final / hybrid | 166.51 | 151.62 | 151.19 | 146.49 | 152.33 |

Every configured logical worker sends two sequential requests. HTTPX, TLS verification, signing, pooling and production backend selection are real. The local origin shares the process/CPU, so this measures the combined client/server fixture, not TikTok or proxy-plan capacity. Kernel loopback byte counters include both ends. Full active/idle connections, latencies, network bytes, CPU/RAM and waits are retained in network_comparisons.csv and source JSON.

## Offline 180,000-account browser

| Version / engine | Load ms | Sort ms | Search ms | Maximum initial UI lag ms |
|---|---:|---:|---:|---:|
| original / worker | 781.55 | 106.06 | 204.46 | 73.90 |
| original / fallback | 9322.87 | 7104.16 | 360.08 | 66.90 |
| first / worker | 775.68 | 107.55 | 203.78 | 73.10 |
| first / fallback | 9319.50 | 7078.87 | 370.66 | 66.40 |
| final / worker | 765.00 | 107.20 | 204.34 | 68.90 |
| final / fallback | 1313.58 | 596.13 | 201.46 | 64.00 |

All cases validate 180,000 searchable accounts, page sizes 20/500, sorting, filters, details and mobile layout, with zero external requests and JavaScript errors. Six-section avatar checks additionally validate saved pictures, placeholders and idempotent repair.

## Interpreting the complete comparisons

- CSVs include every available numeric metric, including slowdowns. Empty cells mean the older source did not expose that metric; they do not mean zero.
- stage_timings.csv reports aggregate task/operation elapsed durations as percentages of wall time. Concurrent waits overlap, so values can exceed 100%. Signing includes executor wait. Disk stages include the called operation and any internal JSON/SQLite work. They are not exclusive CPU shares.
- CPU profiles preserve per-function exclusive/cumulative observations in the source run. Cumulative coroutine/thread times overlap and must not be added together.
- Full-pipeline 100-job worker-scaling cases cannot activate more than 100 profile owners. The separate TLS matrix activates all 100–5,000 configured jobs and records actual connection use.
- The original scanner already contains its prior report/persistence speed release. The first hybrid and final variants add signing/routing/metrics; universal speedup is not assumed.
- Live compatibility, largest reliable page size, real service risk rates and live upstream baseline throughput remain unverified; see LIVE_VERIFICATION.md.

Exact runs: scan=36822278359, network=36822278359, browser=36822278359, shutdown=36822278359.
