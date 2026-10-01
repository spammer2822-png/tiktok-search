# 1 October 2026 — DTK-only runtime

- Removed the active Worker, native Direct and Hybrid transports.
- Added `dtk_backend.py` as the only TikTok network backend.
- Added automatic Docker Desktop detection/startup on Windows.
- Added automatic pinned DTK clone/setup/startup when the local API is unavailable.
- Added automatic DTK browser profile startup and TikTok identity-pool management.
- Added local API-key loading/redaction. `dtk_api_key.txt` is Git-ignored and never committed.
- Added DTK profile, followers and following normalization into the existing scanner pipeline.
- Preserved durable SQLite resume, phase scanning, deduplication, target matching, reports, avatars and statistics.
- Replaced Hybrid/Direct/Worker runtime metrics with DTK backend metrics.
- Removed obsolete Hybrid/Direct/rate-limit backend modules and their active regression tests.
- Added DTK-specific deterministic regression tests.
- Historical Hybrid benchmark/specification evidence remains as history only and is not executable runtime code.

# 2026-09-27 — consistent report pictures

- Resolve missing avatars from saved discovery records, input data and profile checkpoints.
- Backfill saved avatars during scanning so pending and failed accounts can display pictures.
- Add `repair_report.py` to update an existing run's pictures/report without rescanning accounts.
- Preserve report templates, controls, account results, cursor state and the existing cache.
- Add regression coverage for pending, error and partial accounts, cached repair and cancellation.
- This checkpoint remains Worker-only. Hybrid implementation/recovery is unfinished;
  see `HYBRID_WORK_STATUS.md`. Upstream baseline evidence is included separately.

# 2026-09-26

- Remove the fixed 64/256 connection ceiling; grow toward configured workers with observed feedback and global pacing.
- Schedule useful profile tasks dynamically and avoid broadcast wakeups.
- Separate ordered shared persistence from adaptive independent profile I/O.
- Avoid duplicate raw-response cleaning and repeated member serialization; preserve JSON contents and durability.
- Speed large pending-queue resume using a directory inventory.
- Close report database readers, batch metadata lookup and embed lossless row differences.
- Restore visible errors, retries, checkpoint notices and full progress metrics with batched detail.
- Protect graceful shutdown from repeated signals and avoid duplicate final exports.
- Include three-version local benchmarks, real loopback TLS/CONNECT checks and regression tests.

# Performance, resume and statistics update — 23 September 2026

- Added the Y/N retry choice on resume for network errors, timeouts and partial
  profiles, including finished runs with eligible failures. Requeue and failure
  history commit atomically. Completed work, pages, cursors and matches survive.
- Worker HTTP 429 stops new API calls immediately, before reading its body.
  Semaphore admission, retry timers and HTTPX send hooks share the stop event.
  Bootstrap follows the same rule. Waiting accounts remain pending.
- Separated configured workers (up to 10,000) from effective concurrency, local
  connection limits and bounded profile tasks. Added gradual adaptation, a
  shared retry timer and bounded reusable per-proxy client pools.
- Added atomic live statistics, rolling five-minute metrics, request/account
  throughput, guarded ETA and append-only statistics history.
- Moved persistence to one bounded disk-writer queue, added transactional queue
  counters, batched JSON mirrors, cached member counts and bounded metadata.
  Avatar index/file checks also run off the event loop. Essential page commits
  and immediate target recording retain their durability.
- Avoided re-reading unchanged completed exports on every restart. Changed,
  missing and legacy exports still take the validation/recovery path.
- Embedded compact report rows once and streamed output. Report parsing,
  filtering and cached sorting use a Web Worker with a cooperative fallback.
  Only the selected page is rendered. Skip reasons have an editable filter with
  bounded suggestions instead of an arbitrarily large select menu.
- Preserved scan modes, schemas, proxy authentication, global pacing, exact
  cursors, raw-data controls, size limits, offline avatars and search folders.
- Enforced spacing immediately before dispatch, after proxy/client preparation,
  so delayed setup cannot bunch request starts. Trust-store loading is reused
  and runs off the event loop. Unfinished HTTP tasks cancel immediately on 429.

See `PERFORMANCE.md` and `TESTING.md` for measurements and verification.
Historical notes below describe the prior update. Its old Worker 429 retry
behavior is superseded by the immediate global stop above.

## Previous avatar and reliability update

The attached reference image was used only to identify the existing avatar
placement beside usernames. The report's design, filters, navigation, scan
behavior, output layout and existing JSON formats are retained.

## Changes

- Added offline avatars beside account names and in detail dialogs. Images are
  downloaded while scanning, validated, reduced and cached inside each run.
  Duplicate accounts/URLs and resumed runs reuse that cache. Missing/failed
  images use placeholders. The HTML never contacts the CDN.
- Fixed the special 404 retry incorrectly disabling ordinary retries for later
  server errors, timeouts or rate limits.
- Recovered committed target observations and discoveries before duplicate-UID
  and size-limit early returns. Startup also repairs earlier terminal duplicate
  results from their saved pages. Recovery remains idempotent.
- Hardened proxy error classification: structured statuses take priority;
  fallback parsing uses sanitized CONNECT status lines and known error tokens,
  not arbitrary 403/429 substrings in authenticated URLs.
- Separated diagnostic redaction from API-data cleaning. Profile names, IDs,
  counts and bios are parsed unchanged even when they contain a proxy credential
  substring. Explicit authentication fields, authenticated URLs and diagnostics
  remain sanitized at persistence/output boundaries.
- Corrected Phase 2's discovery count. Phase 1 discoveries are no longer reported
  as new Phase 2 discoveries; Phase 2 still does not start a third phase.
- Removed unused `delay_range`, `worker_settings`, `resume_enabled`,
  `select_session` and their now-unreferenced environment parsers. Their old
  environment-only worker test now checks the actual saved-configuration path.
  Active CLI worker/delay selection and durable resume behavior are unchanged.
- Added a saved `worker_origin` configuration field, defaulting to the existing
  Worker. Old configurations still work. No automatic API fallback is added.

## Files

Runtime changes: `tiktok_worker_scanner.py`, `report_generator.py`,
`report_template.html`, `requirements.txt`; new `avatar_cache.py`.

Verification/documentation: new `tests/test_avatar_update.py`, updated
`run_tests.py`, `tests/test_async_scanner.py`, `tests/test_configured_scanner.py`,
`tests/test_release_checks.py`, `README.md`, `TESTING.md`, and this changelog.
The startup entry, Webshare validation module and existing directory structure
remain intact. Only generated runs gain the `report_assets/avatars` directory.

See `TESTING.md` for executed checks and platform/live-service limitations.
See the README for cache limits, CDN eligibility, resume behavior and moving
reports with their assets. No real user data or proxy credentials are bundled.
