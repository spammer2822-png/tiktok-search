# Current work status — 1 October 2026

The recovered implementation remains preserved in GitHub. No restart from scratch was performed. This continuation is isolated on `work/final-verification-20261001`; the pre-continuation state is anchored by `rollback/pre-final-verification-20261001` at `de31c84e45fcd6e74c48833e5c3107d81ab04bdc`. See `CONTINUATION_ROLLBACK.md` for exact rollback instructions and the two evidence-only intermediate commits that briefly landed on `main`.

## Implementation

Implemented and retained: existing Worker contract, Worker/Direct/Hybrid modes, native signing and normalization, dynamic valid-record routing, per-chain ownership and safe deduplicating restarts, persistent isolated proxy sessions, scalable configured workers/connections, adaptive Direct concurrency, shared per-execution 429 lifecycle, durable stop/resume, incremental metrics, background persistence/logging, large offline reports and avatar repair.

The continuation found one verification-infrastructure defect rather than a scanner-runtime defect: parallel evidence jobs all pushed to hard-coded `main` and could race each other. Evidence publishing is now branch-aware, retries with bounded backoff, and all evidence-producing workflows use one serialized publisher group. Scanner runtime source was not changed by this continuation.

## Current acceptance evidence

- Unmodified upstream: 2,529 unit/replay and 528 integration passes; see `hybrid_baseline/ci_verified_20260930.json`.
- Current runtime under acceptance: commit `1e069ffb57fdf09bf6575d02576dbc5828cc2d21`, scanner source SHA-256 `c83b9de10f1b9556fa6c368d0d582674202cf734c4631eef16fa36481fa010dd`.
- Matched full scan comparison: run **36852148338**, comparison job **COMPLETED/SUCCESS**. All 56 production-dispatcher cases completed, plus original/first/final component and CPU-profile cases.
- Real loopback TLS: run **36852148287**, network evidence **COMPLETED**, all 35 original/first/final Worker/Direct/Hybrid cases through 5,000 configured workers returned successfully.
- CPU/profiling: run **36852148287**, CPU evidence **COMPLETED** with per-thread profiling and explicit observability measurements.
- Large offline report/browser: run **36852148287** completed; fresh isolated rerun **36854983351** also completed 180,000-account Worker/fallback and six-section avatar checks.
- Shutdown/resume: fresh run **36854871728** passed all six original/first/final single/repeated SIGINT cases.
- Final regression gate: fresh run **36854871750** passed Ubuntu Python 3.11/3.12 (225 tests each) and Windows Python 3.11/3.12 (219 tests each, one POSIX-only skip).

## Specification status

The reconciled acceptance audit contains **184 exact checklist requirements**:

- **181 COMPLETED**
- **0 FAILED**
- **3 BLOCKED BY ENVIRONMENT**

The three blocked items are `CHECK-016`, `CHECK-017`, and `CHECK-184`. They ultimately depend on live verification that cannot be honestly inferred from local fixtures.

All locally/offline-verifiable requirements are complete. `REQUIREMENTS_AUDIT.md`, `SPECIFICATION_AUDIT.json`, and `HYBRID_COMPLETION_CHECKLIST.csv` are the current acceptance authority.

## Measured performance caveats

The final implementation is not claimed to be universally faster in every mode. Current matched measurements show improvements in several final Direct/Hybrid and reporting paths, but the dedicated Worker-only 10,000-follower fixture is slower than the original. That regression is preserved and documented rather than hidden. Its measured work is dominated by durable page/state/discovery writes and observability that preserve resume/crash guarantees; removing those guarantees merely to improve the benchmark was rejected.

No live TikTok request rate, production risk-control rate, maximum reliable Direct page size, Webshare plan capacity, or live upstream throughput is certified.

## Remaining external work

Live TikTok compatibility, largest reliable Direct page size and production throughput remain **BLOCKED BY ENVIRONMENT** because authorized accounts, Direct identity/session, proxy pool and maintainer-configured upstream deployment were not supplied. `LIVE_VERIFICATION.md` contains the exact commands/procedure needed to close those three final checklist blockers.
