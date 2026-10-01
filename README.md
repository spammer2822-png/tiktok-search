# Hybrid update — 1 October 2026

Worker, Direct and Hybrid modes are available at **Start/Resume → Backend configuration**. See [HYBRID_CONFIGURATION.md](HYBRID_CONFIGURATION.md) for routing, session/proxy settings, the new 429 lifecycle and explicit resource limits. Both authoritative specifications are mapped in [REQUIREMENTS_AUDIT.md](REQUIREMENTS_AUDIT.md) and [HYBRID_COMPLETION_CHECKLIST.csv](HYBRID_COMPLETION_CHECKLIST.csv). Live validation is not yet certified.

# September 26 speed update

Read **BENCHMARK_REPORT.md** for measured speed comparisons and **PERFORMANCE.md** for concurrency details. Run `py -3.11 -m pip install -r requirements.txt` and `py -3.11 main.py` from this folder. Resume your existing search through the normal menu; keep its entire log folder.

The fixed 64-connection default is removed. Your configured workers remain the ceiling when `max_connections` is absent or 0. An explicitly saved nonzero connection limit remains in force. Repeated Ctrl+C presses now wait for saving/report generation. No scan data migration or restart is needed.

# TikTok Scanner

## Report picture repair — 27 September 2026

Profile pictures now use saved discovery data and profile checkpoints consistently,
including pending, partial and failed accounts. Missing saved pictures are cached
in the background during scanning. The report layout and controls are unchanged.

To repair an existing report without rescanning its accounts, run:

```cmd
py -3.11 repair_report.py "C:\Users\vailo\Downloads\TiktokSearch_Logs\amlie7951"
```

Use the folder for that specific scan. This downloads missing pictures from saved
avatar URLs, reuses cached images, and rebuilds the report in the same folder.
It preserves scan results and cursors. Accounts without an available picture keep
their placeholder. Close the scanner before repairing the same run.

Worker, Direct and Hybrid implementations are present. See `HYBRID_WORK_STATUS.md`
for current acceptance evidence and remaining live-verification requirements.

Extract this entire folder. Keep the Python modules, HTML template and
`report_worker.js` together. The report generator embeds the JavaScript; users
opening a generated report do not need that source file beside the HTML.
Run only `main.py`; it imports the other modules automatically.

## Start on Windows

Open a terminal in this folder:

```cmd
py -3.11 -m pip install -r requirements.txt
py -3.11 main.py
```

Python 3.11 or newer is required. Python 3.12 also works. The original
`tiktok_worker_scanner.py` entry remains available for existing startup shortcuts.

Default locations:

- Input: `C:\Users\vailo\Downloads\kt_expose_public_profiles_cleaned.json`
- Webshare proxies: `C:\Users\vailo\Downloads\webshare_proxy.txt`
- Search logs: `C:\Users\vailo\Downloads\TiktokSearch_Logs`

Optional path overrides: `TIKTOK_INPUT_JSON`, `TIKTOK_PROXY_FILE`, and
`TIKTOK_EXPORT_DIR`. Your datasets and proxy credentials are not included in
this project.

The default Worker is unchanged. To use a compatible replacement Worker, set
`TIKTOK_WORKER_ORIGIN` to its HTTPS `*.workers.dev` origin before creating a new
search. New searches save it as `worker_origin` in `scan_config.json`; resume
uses that saved value. Older configurations without this field retain the
original endpoint. To change an existing search's endpoint, explicitly edit
that field while the scanner is stopped. No path, credentials, query parameters,
direct TikTok endpoint, or automatic service failover is accepted. A replacement
must implement the same confirmed API schemas; this does not bypass restrictions.

## Search and resume

Startup lists unfinished searches and finished searches with retryable failures. Select one to resume its saved configuration
and queues, or choose a new search. New searches accept a JSON dataset or a
starting TikTok username. Configure the target, size limits, normal or double
phase mode, workers, delays, and proxy use. Zero size limits mean unlimited.

Workers run independent profiles concurrently. The saved worker setting accepts
any positive integer and is a desired maximum. Admission starts at the configured
capacity, with Direct optionally starting at `direct_initial_concurrency`. The
profile task pool grows with admission and available jobs. Missing
`max_connections`, or 0, uses the configured workers; an explicit positive value
is respected.
The POSIX file-descriptor budget can lower it. Startup and statistics display both
configured workers and actual concurrency. These values are not requests/second
or an API/proxy plan entitlement. Global request delays are still respected,
including across proxies. All Worker requests share aggregate pacing and rate-limit controls.
Each list follows its exact returned cursor sequentially. Double phase mode
adds exactly one level of newly discovered public profiles.

When proxies are enabled, the optional validation choice is the last setup
step before final Start/Resume confirmation. A validation pass checks each
loaded proxy once through the same HTTPX proxy stack, using Webshare's official
HTTPS IP diagnostic. It does not fetch TikTok accounts. Failed checks exclude
that route from the current run. If none pass, choose direct mode explicitly,
retest, or return to configuration. Skipping validation leaves normal lazy
connection checks enabled. Validation does not rewrite the proxy file.

Webshare `IP:PORT:USERNAME:PASSWORD`, authenticated proxy URLs, IP-authorized
endpoints, and explicitly configured SOCKS5 URLs are supported. The
`p.webshare.io` gateway is recognized; configured usernames, session parameters,
and endpoint schemes are retained. Credentials authenticate with the proxy,
never with the Worker. Direct fallback is available unless proxy-only mode is
selected. Worker 403/429 responses do not mark a proxy broken or trigger rotation
to bypass a restriction.

Each new search atomically reserves `target`, `target_2`, `target_3`, etc.
inside the logs directory. All scan outputs, temporary files, checkpoints,
logs and reports stay in that search folder. Resume uses that same folder and
acquires a process lock. Keep the entire search folder, including SQLite files.

Ctrl+C and SIGTERM save available progress and generate an incomplete report.
A forced process kill cannot generate a report at the moment it happens;
restart and select the same folder to recover committed pages and exact cursors.

## Completion, restrictions and relationships

- A natural endpoint is usable even when returned counts differ from advertised
  counts. The difference remains a `complete_count_mismatch` warning.
- An explicitly hidden list stays `list_restricted`, with `complete: false`.
  If other accessible lists finish, the profile is terminal `restricted`.
- A username bootstrap can use completed lists alongside explicitly restricted
  lists. Genuine request, cursor, parsing, or interruption failures still stop
  bootstrap. Empty and entirely unavailable sources finish cleanly with an
  explanation and saved metadata.
- Saved restricted results are reused. Unambiguous older partial/failed results
  with only visibility restrictions are migrated without re-fetching hidden lists.
- Relationship values are `true`, `false`, or `null` (unknown). A hidden direction
  does not prove absence or disprove an earlier confirmed observation. A mutual
  requires confirmed observations in both directions.
- Each Worker HTTP 404 gets exactly one additional attempt for the same request,
  route, UID, cursor and logical page. A second 404 remains an HTTP error;
  committed pages and the continuation cursor survive. HTTP 404 alone does not
  mean the TikTok account is missing. In Worker-only mode the first Worker HTTP 429 stops the
  entire scan immediately. In Hybrid mode it disables Worker and migrates work
  to Direct. Worker is never retried in the same execution.
  That one-time 404 allowance is independent of ordinary transient-error retries:
  `404 -> 500 -> success` can recover when the normal budget permits it.

Progress counts every committed terminal outcome, including private, skipped,
not-found, restricted and failed profiles. The final summary separates checked,
completed, failed, skipped, restricted, pending and private totals, and shows
previously checked profiles versus this execution.

On resume, answer **Retry failed profiles? [Y/N]** (default N):

- **Y:** requeue `network_timeout`, `network_error`, `partial`, and the legacy
  `response_timeout` status. Completed profiles remain completed. Saved profile
  information, completed lists, collected pages, exact continuation cursors,
  target observations and exports are retained. Latest outcomes update the same
  queue rows; the previous failures stay in SQLite `retry_history`.
- **N:** continue pending/interrupted work and retain prior failures.

Recovery also repairs `in_progress` claims after a crash. A retry stopped before
completion remains pending on the next resume. Retrying a partial result cannot
invent a missing cursor or unlock a restricted list: these outcomes stay partial
or restricted until usable evidence is available. Other permanent statuses such
as `http_error`, `private`, `not_found`, and size skips are not automatically
requeued. A finished queue and a fully complete relationship dataset remain
separate status fields. Resume updates the same HTML with all old and new data.

## Rate limits, statistics and performance

In Worker-only mode, the first Worker HTTP 429 closes the global request gate as
soon as its response headers arrive. Hybrid mode instead disables only Worker
and migrates unfinished work to Direct; Direct has one cooldown/recovery opportunity
before a repeated 429 triggers shutdown. See HYBRID_CONFIGURATION.md. In all modes, Waiting semaphore users, queued profiles, delayed retries and
HTTPX request hooks all check the stop state. No automatic retry or proxy
rotation follows a Worker 429. Already transmitted calls cannot be recalled;
unfinished HTTP tasks are cancelled immediately and completed responses may
still be committed. Profile tasks get a short shutdown grace period to save
results, then are cancelled safely. Pending accounts remain pending.

Each execution creates `scan_stats.json` in its search folder before API work,
updates it about every 15 seconds, and writes a final atomic snapshot on graceful
shutdown. Detailed statistics print every 300 seconds; ordinary progress is
throttled to roughly once per second, with critical messages shown immediately.
Full detail remains in `run.log` and `errors.log`; the terminal now batches recent
page detail and shows errors/retries immediately.

Statistics include session totals, a rolling five-minute window, request success
and failure counts, timeouts, retries, recovered requests, latency, account
outcomes, configured/effective concurrency, remaining work and ETA. Request
counts include Worker API attempts (bootstrap included), excluding optional
avatars and proxy validation. A successful request means HTTP success with valid
JSON and no Worker API error; profile completion is counted separately.

One account can need multiple requests. Request retries increase request counts,
not the number of unique profiles processed. `lifetime` covers the current
execution; `overall_*` and queue totals cover saved progress across executions.
The rolling window uses bounded one-second buckets, accurate to about one second.
ETA uses remaining accounts divided by recent successfully completed accounts
per second, after at least 30 seconds and five successful profiles. Skips,
restrictions and failures do not inflate that rate. ETA is unavailable on stops,
429s, insufficient samples or zero recent throughput; Phase 2 discovery can
increase remaining work.

`scan_stats_history.jsonl` appends a record every five minutes and on graceful
shutdown, retaining earlier resume sessions. A forced kill may lose in-memory
metrics since the last 15-second snapshot, but committed SQLite pages/jobs
survive. The next run repairs a torn final history line if needed.

Ordered state writing and an adaptive pool for independent profile files move
database work, exports and atomic snapshots off the event loop. SQLite still commits pages and cursors together using FULL
synchronous WAL transactions; essential target matches remain immediate.
Queue totals use transactional counters. JSON checkpoint mirrors are batched;
SQLite is authoritative after an unexpected stop. No schema reset or deletion
of existing scan data is required. See `PERFORMANCE.md` for architecture,
measurements and trade-offs.

## Offline HTML report

Open `report_<search-folder-name>.html` inside the search folder. It is generated
automatically from saved files after completion or graceful stopping, and
updated atomically on resume. The scanner prints its full path; it does not open
a browser automatically.

The report includes overview/configuration, target matches, confirmed mutuals,
all accounts, skipped accounts, errors and phase statistics. Search, combined
filters, numeric sorting, page sizes 20/50/100/250/500 and account detail dialogs
work offline. Only the current page is rendered. Unknown relationships remain
explicit. There are no JSON download controls.

The report makes no network requests. While scanning, the confirmed profile
`data.profile` URL (or a saved member's normalized `avatarThumb`) is used to
cache an avatar in `report_assets/avatars/` inside the search folder. Table rows
and account dialogs display circular, consistently sized avatars. Missing,
expired, failed or invalid images use an initial/neutral placeholder.

Downloads use the same HTTPX proxy connections, aggregate spacing and request
limits as the scanner. Only HTTPS URLs on the CDN domains listed in
`avatar_cache.py` are eligible; no TikTok website/API calls, redirects or cookie
authentication are added. A CDN HTTP error is separate from a Worker error;
restrictions never cause proxy rotation. Genuine proxy-connect failures can
use another healthy route, with the usual proxy-only/direct-fallback policy.

Each image is limited to 2 MiB downloaded and 4 million decoded pixels,
validated by Pillow, stripped of metadata, and reduced to a JPEG of at most
96 × 96 pixels and 32 KiB. Animated images use the first frame. Hash-based
filenames avoid username/path collisions and expose no URLs or credentials.
Files are replaced atomically; unfinished downloads are never used as images.
Concurrent duplicate URLs and numeric UIDs share cached images. Failed sources
are tried at most once per execution and have a one-hour cooldown across runs.
Interrupted downloads can retry when resumed. Valid cached images are reused
even when their old signed URL expires or the account has a new username.

Resume can cache avatars from older saved profiles without rescanning their
lists. Reports generated after interruption use whatever images are already
cached. Old records with no image fields still work. Unknown CDN hosts and
unrecoverable expired URLs remain placeholders; the scanner does not try to
bypass a CDN restriction or refresh them through the TikTok website.
Three consecutive transport failures stop optional requests to that CDN host
for the current execution, so an outage cannot delay every remaining profile.

**For offline sharing/moving, copy the HTML together with `report_assets`, or
copy the entire search folder.** Relative paths work after moving the folder.
Copying only the HTML retains all report data and controls but loses cached
avatars. The report remains lightweight because image bytes are separate;
only visible rows load images. Already embedded legacy raster images are
validated and bounded before they can be displayed.

Only clicking an account's explicit profile link opens TikTok in a new tab.
Existing JSON output remains available separately, including the intentionally
preserved filename `sucess_find.json`.

## Files and verification

- `main.py`: startup entry point.
- `tiktok_worker_scanner.py`: existing scanner, Worker transport, configuration,
  durable queue, pagination, exports and shutdown handling.
- `proxy_validation.py`: optional Webshare validation.
- `avatar_cache.py`: bounded image validation, local cache and offline lookup.
- `report_generator.py` / `report_template.html`: saved-data report assembly and
  self-contained dashboard template.
- `report_worker.js`: background report parsing, search, filters and cached sort;
  embedded in the generated HTML. A cooperative fallback works if Workers are blocked.
- `scan_runtime.py`: bounded disk writer, local resource budget and retry timer.
- `scan_statistics.py`: session counters, rolling buckets, ETA and history.
- `run_tests.py`, `tests/`: repeatable local tests with synthetic fixtures.
- `TESTING.md`: verification scope and limitations.
- `CHANGELOG.md`: this review's fixes and changed-file summary.
- `PERFORMANCE.md`: measured comparisons and detailed implementation notes.
- `REQUIREMENTS_REVIEW.md`: review against all 36 instruction sections.
- `benchmarks/`: small local measurement results; no real scan data.

Official references used for Webshare compatibility:

- [Connection modes and authentication](https://apidocs.webshare.io/proxy-connection)
- [Downloading proxy lists](https://help.webshare.io/en/articles/8596696-how-can-i-download-my-proxy-list)
- [Rotating endpoints](https://help.webshare.io/en/articles/8375645-how-to-connect-through-a-rotating-proxy-endpoint)
- [Concurrency](https://help.webshare.io/en/articles/8375281-what-is-concurrency)
- [Configuration errors](https://help.webshare.io/en/articles/8570214-what-are-the-configuration-errors)
- [HTTPX proxy configuration](https://www.python-httpx.org/advanced/proxies/)
- [HTTPX async clients](https://www.python-httpx.org/async/)
- [Pillow image validation and thumbnails](https://pillow.readthedocs.io/en/stable/reference/Image.html)
- [Pillow Python version support](https://pillow.readthedocs.io/en/stable/installation/python-support.html)
