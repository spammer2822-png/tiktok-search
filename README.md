# TikTok Scanner — DTK-only build

This branch uses **one TikTok network backend only: DTK / Evil0ctal Douyin_TikTok_Download_API**.

The old Worker, native Direct, and Hybrid runtime implementations have been removed. The scanner still owns its existing durable scan pipeline: discovery, profile queues, phase 1 / phase 2, exact resume, SQLite checkpoints, duplicate prevention, target matching, JSON exports, statistics, avatar caching, HTML reports, retries, and graceful shutdown.

## What happens when you start it

Run:

```cmd
py -3.11 -m pip install -r requirements.txt
py -3.11 main.py
```

When DTK is enabled, the scanner automatically:

1. Checks `http://127.0.0.1:8000/readyz`.
2. If DTK is already healthy, reuses it.
3. If Docker is not running, attempts to start Docker Desktop on Windows.
4. Locates the DTK repository. The first location checked is:
   `%USERPROFILE%\Downloads\Douyin_TikTok_Download_API`
5. If the DTK repository is missing and auto-clone is enabled, clones the pinned upstream revision.
6. Creates DTK's local `.env` credentials when needed.
7. Starts the DTK Docker Compose stack with the browser profile.
8. Waits for the API to become healthy.
9. Loads the DTK API key.
10. Configures the TikTok identity pool and disables automatic Douyin identity minting for this TikTok-only scanner.
11. Automatically mints TikTok guest identities when the usable pool is below the configured minimum.
12. Uses only the DTK REST API for TikTok profile, followers, and following requests.

The scanner does not need the DTK web console to remain open. Docker/DTK services do need to run while a scan is using them; the scanner handles starting them automatically.

## DTK API key

The key is loaded in this order:

1. `DTK_API_KEY` environment variable.
2. `dtk_api_key.txt` beside the scanner.
3. Interactive hidden prompt.

`dtk_api_key.txt` is deliberately ignored by Git and must never be committed. The scanner redacts the loaded key from logs and generated diagnostic data.

Required read scope:

```text
tiktok:read
```

Automatic identity-pool management also needs sufficient DTK administrative/identity-management scope. A read-only key can still use an already-managed DTK pool, but cannot change pool settings or request mints.

## DTK endpoints used

The scanner calls the local DTK API:

```text
GET /api/v1/tiktok/user
GET /api/v1/tiktok/user/followers
GET /api/v1/tiktok/user/following
```

DTK owns the browser identity, cookies, fingerprint, signing, identity health, token buckets, circuit breakers, and upstream TikTok transport. The scanner does not maintain a separate TikTok cookie/session JSON anymore.

## Identity automation

Default scanner settings:

```text
DTK minimum usable TikTok identities: 3
DTK TikTok target identities:        8
DTK list page size:                  35
DTK local HTTP connection ceiling:   64
Docker/DTK auto-start:               enabled
Automatic identity management:       enabled
```

The values can be changed from **DTK configuration** before starting/resuming a scan.

DTK itself remains the authority for identity health and replacement. The scanner only aligns the desired TikTok pool marks and gives startup minting a kick when the pool is short.

## Docker and DTK requirements

Recommended Windows setup:

- Windows 10/11
- WSL2
- Docker Desktop
- Python 3.11 or newer

The scanner can **start** Docker Desktop but does not silently install Docker Desktop or WSL for you.

DTK is pinned to the upstream revision recorded in `dtk_backend.py`, and the browser image uses the recorded CloakBrowser commit. This avoids silently changing the TikTok protocol stack underneath a scan.

## Resume and durability

Existing scanner durability is retained:

- every fetched relationship page and its continuation cursor are committed together;
- completed profiles are not rescanned on resume;
- pending/in-progress jobs recover after interruption;
- failed/partial profiles can be explicitly requeued;
- target matches are written immediately;
- duplicate identities are deduplicated;
- phase transitions are stored transactionally;
- Ctrl+C performs graceful persistence and report generation where possible.

All scan files stay inside the selected search folder under the configured log root.

## Reports and statistics

The existing offline HTML report, JSON exports, avatar cache, `scan_stats.json`, and `scan_stats_history.jsonl` remain.

Backend metrics are now DTK-only and include DTK request success/failure, latency, returned records, rate-limit/queue/identity-pool errors, and effective scanner concurrency.

Historical Worker/Hybrid benchmark and verification files remain in the repository as historical evidence. They are not active runtime code.

## Security

Never commit:

- `dtk_api_key.txt`
- DTK `.env` secrets
- cookies or identity exports
- scan output containing private local data

The scanner sends the DTK API key only to the configured DTK API origin. It does not send the key to TikTok or any former Worker endpoint.

## Important throughput note

Increasing scanner workers does not magically make TikTok accept the same number of simultaneous requests. DTK deliberately schedules through usable browser identities and endpoint-specific token buckets. If DTK reports `IDENTITY_POOL_EXHAUSTED`, `QUEUE_FULL`, `RATE_LIMITED`, or risk control, the scanner respects DTK's retry information rather than hammering localhost harder, because apparently computers also benefit from not being shouted at.

## Main project files

- `main.py` — startup entry point.
- `tiktok_worker_scanner.py` — durable scanner pipeline. The filename is retained for compatibility; it no longer contains the legacy Worker transport.
- `dtk_backend.py` — DTK API adapter, Docker/DTK lifecycle automation, API-key loading, and identity-pool setup.
- `backend_metrics.py` — DTK backend metrics.
- `scan_statistics.py` — execution statistics and history.
- `scan_runtime.py` — runtime/disk scheduling.
- `avatar_cache.py` — bounded offline avatar cache.
- `report_generator.py`, `report_template.html`, `report_worker.js` — report system.
- `run_tests.py`, `tests/test_dtk_backend.py` — current DTK regression suite.

## Verification status

The upstream DTK stack has already been live-tested on the target Windows machine: its API, worker, Redis, PostgreSQL, browser-rpc and a minted TikTok guest identity came up successfully, and an end-to-end TikTok smoke test succeeded.

The repository CI covers the scanner integration without requiring live TikTok credentials. Live end-to-end scanner traffic should still be treated separately from deterministic CI evidence.
