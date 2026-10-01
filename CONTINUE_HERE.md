# Current recovery checkpoint — 1 October 2026

Runtime checkpoint f2991c23800fa0d645cb512d2a20eb3ffba75d76 is pushed to main. Always fetch current main first because verification jobs push evidence after every completed case. Never force-push over their results.

The previous local workspace survived. Its nine uncommitted local benchmark files were copied to ../recovery_preserved_20261001 with a manifest and Git bundle before updating. They were then preserved in GitHub commit 14cd55e, explicitly labelled historical harness-v1 results. The current checkout is resume_20260930/TikTokScanner. An immutable pre-optimization worktree is ../perf_before_20261001. Original/first-hybrid archives and exact upstream archive remain in the repository.

Runtime fixes: cancellation-safe SQLite construction/close; Windows precise pacing; cooperative report MessageChannel yielding; avoid a second terminal-page commit; preserve Worker-only legacy retry settings. Latest local full suite: 224 tests pass on Python 3.12.

Completed September evidence: 36759496281/comparison (56 cases + components/profiles), 36761511421/network (35 production-dispatcher TLS cases), 36761289359/browser (all report/avatar checks), 36760498050/shutdown (6 cases), and 36761511342/regressions (222 Linux / 216 Windows tests, one POSIX skip on Windows). Pristine upstream: 2529 unit/replay + 528 integration passes.

Current final CI: scanner 36822278401; comparison/network/browser/shutdown 36822278359. These use immutable f2991c2 source snapshots. Wait for completion, inspect failures, and fetch evidence. Generate final comparison files using tests/summarize_verification.py with all four run arguments set to 36822278359. September analysis is under verification_analysis/20260930 and must remain historical.

Remaining: finish benchmark/performance assessment, update all 184 checklist rows and 79 specification-section mappings, final source/spec review, final test evidence and release hash/ZIP. Live TikTok accounts/identity configuration are still absent; do not claim production readiness or largest reliable live page size. LIVE_VERIFICATION.md contains the procedure.

Git fetch works. CLI push has no credentials in this workspace; use the connected GitHub create_tree/create_commit/update_ref tools, preserving latest parent and force=false. After publishing equivalent local commits, fetch; compare trees; align the local branch with git reset --soft origin/main only after that comparison succeeds. Never upload session/proxy credentials.
