# Current work status — 30 September 2026

Recovered work and original/first-hybrid/upstream archives are preserved in GitHub. No restart from scratch was performed. The local execution workspace became unavailable; work continues from the latest remote code using GitHub Actions. Do not treat stale transient paths as a recoverable current checkout.

Implemented: existing Worker contract, Worker/Direct/Hybrid modes, native signing and normalization, dynamic valid-record routing, per-chain ownership and safe deduplicating restarts, persistent isolated proxy sessions, scalable configured workers/connections, adaptive Direct concurrency, shared per-execution 429 lifecycle, durable stop/resume, incremental metrics, background persistence/logging, large offline reports and avatar repair.

Verification:
- Unmodified upstream: 2529 unit/replay and 528 integration passes; see hybrid_baseline/ci_verified_20260930.json.
- Scanner: prior complete green run 36759162542 (Linux 221 tests; Windows 215 with one POSIX-only skip). A later Windows repeat exposed early pacing admission; runtime fix e6e64a7 now awaits its fresh four-platform gate.
- Corrected real loopback TLS matrix: run 36759670077 COMPLETED; all 15 cases through 5000 workers.
- Original/first/final single and repeated SIGINT/resume: run 36760498050 COMPLETED, all six cases.
- Matched production-dispatcher full scan matrix, profiles/components and 180000-row browser/avatar verification: run 36759496281 still running. Its original network-fixture failure is preserved and superseded by 36759670077.

Remaining feasible work: finish and review matched measurements/browser results; resolve regressions; update both-spec audit and benchmark report; confirm latest regression gate and final source/spec review; preserve final integrity manifest and deliverable.

Live TikTok compatibility, largest reliable page size and production throughput remain BLOCKED BY ENVIRONMENT: authorized accounts/Direct identity/session/proxy/deployment configuration were not supplied. See LIVE_VERIFICATION.md. Local fixtures and loopback TLS are not live certification. The old missing-source and Redis/DNS baseline blockers are resolved and must not be carried forward as current blockers.
