# Work checkpoint — 27 September 2026

The complete new `hybrid_tiktok_scanner_combined_final_max_throughput(1).md`
and the preceding `hybrid_tiktok_scanner_optimization_prompt(1).md` were read.
Their order, completion criteria and measured-performance requirements are retained.

**This is not the finished hybrid release.** It contains the verified avatar fix
on the existing Worker-only scanner plus newly preserved upstream baseline evidence.
The unfinished `scanner_hybrid_20260927` implementation recorded in prior work notes
is absent from this restored workspace. The latest recoverable uploaded archive,
`TikTokScanner(2).zip` (SHA-256
`13349f41931ae330e31fa96f6260baa94c74c62407f12cdb886c4f19daff0e31`), has no hybrid
backend modules or `backend_mode` implementation. It is the preceding Worker-only
version. No hybrid implementation was recreated or silently substituted for it.
The missing hybrid source/ZIP is needed to resume that work exactly.

## Verified progress

- Avatar regression suite: 170 tests passed independently on Python 3.11.16 and
  Python 3.12.14; both exact logs are included.
- Both offline browser rendering modes pass all six account sections, modal
  pictures, search/filter/sort/paging and mobile checks without external requests.
- Report repair preserves existing scan state and reuses previously cached images.
- Existing `report_template.html` and `report_worker.js` are unchanged.
- Evil0ctal commit `737bf3dfe9de1dbff57990c0ec4c9e02c75c3d0f` was recovered and tested
  without tracked source changes. Its source archive and baseline evidence are in
  `hybrid_baseline/`.

The chronology of any earlier, now-missing integration cannot be audited from
the available files. This recovered checkout was tested unmodified before any
new integration changes; this does not retroactively establish that the lost
work followed the new baseline-before-integration requirement.

## Mandatory baseline results

The documented `make install` / `uv sync --all-extras` completed on Python 3.12.14.
An initial test run is preserved exactly: 2,504 passed, 23 failed, 2 setup errors.
Its failures separate into inherited environment proxy/SOCKS setup, a missing
`DTK_SECRET_KEY`, and DNS-dependent webhook assertions.

With a generated throwaway key and proxy environment variables removed **for the
offline unit/replay run only**, the unmodified suite records **2,526 passed and
3 failed**. All three remaining failures are webhook DNS-resolution failures.
This is not a clean all-tests-passed result and is not presented as one.
Relevant passing groups include 95 TikTok replay tests, 276 signing tests,
22 scheduler-health tests, 131 transport tests and 6 paging tests.

Integration: **31 passed, 480 skipped, 17 setup errors**. `make fixtures-up` failed
because Docker is not installed. The 17 errors connect to the unavailable test
Redis instance; service-dependent skips are also retained. Live contract tests:
**3 skipped**, because there is no configured deployment/API key or test subjects.
No live TikTok request or live service throughput is claimed.

The separate offline component harness tests real unmodified builders, parsers
and native signing, including 65- and 10,000-record follower and following fixtures.
It checks exact cursor forwarding, page termination and complete fixture coverage.
Its timings are **component microbenchmarks**, not HTTP RPS, scheduler throughput,
live account performance, or a replacement for the required full service baseline.
Unavailable live metrics are `null`, not zero or invented estimates.

Baseline gaps relevant to later integration: profile `privateAccount` survives in
raw data but is not a normalized Author field; list members do not retain raw data
by default. Upstream treats a missing `userList` as empty when `hasMore` is false.
The scanner adapter must handle these deliberately. The upstream builder defaults
to 30 records; the 35-record ceiling mentioned elsewhere is not established here
as a live follower/following maximum.

Failure classification:

- **PRE-EXISTING FAILURES:** no source defect established by these runs; the recorded
  baseline failures are environment/setup/network dependent. Functional integration
  gaps are documented above without relabelling them as passing requirements.
- **MODIFICATION-INTRODUCED FAILURES:** none detected in the restored avatar branch;
  no upstream source was modified. Missing hybrid code cannot be assessed.
- **FIXED PRE-EXISTING FAILURES:** test environment setup corrected; no upstream
  product fix claimed. The avatar regression is fixed in the available scanner.
- **NEW PERFORMANCE REGRESSIONS / IMPROVEMENTS:** not measured for the missing
  hybrid implementation. No new speed multiplier is claimed.

## Required continuation order

1. Recover the unfinished hybrid source and its first-implementation measurements.
   Inspect and verify it against both saved specifications; do not recreate it silently.
2. Finish the preceding hybrid requirements, remaining baseline work, stress tests,
   full-code review and iterative measured optimization.
3. Then implement the new rate-limit state machine: first confirmed Worker HTTP 429
   disables Worker for this execution, admits no queued/new Worker requests and
   moves compatible work to Direct. Preserve collected users and restart incompatible
   cursor chains at zero with stable-ID deduplication.
4. Direct HTTP 429 centrally pauses Direct. Honor Retry-After, otherwise use the
   configured cooldown. Admit a controlled probe and gradual recovery. A second
   Direct 429 after that opportunity must save all state and stop gracefully.
   Starting/resuming a new execution resets Worker disablement.
5. Scale concurrency/pools from the selected workers or explicit settings, measure
   real bottlenecks and connection waits, preserve pacing configured by the user,
   and document technical limits. Do not claim 2,500 workers means 2,500 RPS.
6. Run the full final regression and three-version benchmark matrix before delivery.

## Completion status by mandatory group

| Group | Status | Evidence or remaining blocker |
|---|---|---|
| Read both specifications in full | COMPLETED | Exact copies retained under requirements/ |
| Previous complete hybrid task | BLOCKED BY ENVIRONMENT | Recorded working directory is absent; source recovery required |
| Unmodified upstream pin/dependencies/local baseline | COMPLETED | Commit, archive, install log, unit/replay JUnit evidence |
| Entire upstream test suite passes | FAILED | Three DNS-dependent assertions still fail; service tests blocked separately |
| Original integration/live service baseline | BLOCKED BY ENVIRONMENT | Docker/services, deployment and authorized live subjects unavailable |
| Offline profile/list/signing/schema checks | COMPLETED | Existing upstream suites and baseline_components.py |
| Live profile/followers/following pagination | BLOCKED BY ENVIRONMENT | No configured deployment or live test run |
| Worker-only existing regression behavior | COMPLETED | 170-test regression suite; not a live service certification |
| Direct/hybrid/routing/scoring/cursor failover | BLOCKED BY ENVIRONMENT | Missing unfinished hybrid source |
| New Worker-disable and Direct-recovery 429 flow | BLOCKED BY ENVIRONMENT | Missing hybrid source; preceding work must finish first |
| New 100/500/1,000/2,500/5,000-worker hybrid matrix | BLOCKED BY ENVIRONMENT | Missing hybrid implementation; historical Worker evidence is not substituted |
| 10k follower/following component fixtures | COMPLETED | Four deterministic multi-page checks, no duplicates or skipped fixture users |
| Three-mode 10k follower and 100/1k/10k account benchmarks | BLOCKED BY ENVIRONMENT | Full hybrid implementation unavailable |
| Hybrid failure/load/shutdown/resume/leak tests | BLOCKED BY ENVIRONMENT | Existing Worker tests pass; requested new hybrid paths unavailable |
| New hybrid pooling/proxy/session instrumentation | BLOCKED BY ENVIRONMENT | Missing source; upstream transport tests alone are insufficient |
| Final hybrid database/JSON/logging/statistics review | BLOCKED BY ENVIRONMENT | Full implementation required before final review |
| Avatar consistency/report design and controls | COMPLETED | Browser modes, repair CLI and state-preservation checks |
| Final hybrid full-code profiling/async review | BLOCKED BY ENVIRONMENT | Missing code and full benchmark environment |
| Original/first/final hybrid performance comparison | BLOCKED BY ENVIRONMENT | First hybrid source/measurements unavailable |
| Final hybrid configuration/dependencies/release review | BLOCKED BY ENVIRONMENT | Project is unfinished; not production-ready certification |
| Additional image edits/redesign | NOT APPLICABLE | None requested for this work; no image edits made |

The full specifications remain authoritative; this checkpoint does not mark their
unchecked items complete. The included completion matrix preserves each checklist
item separately for the remaining review.

## Reproducing and finishing upstream validation

Use a machine with Docker Compose, working outbound DNS and Python 3.12 or 3.13.
Extract `hybrid_baseline/upstream_737bf3d_unmodified.zip`, or check out the exact pin:

```bash
git clone https://github.com/Evil0ctal/Douyin_TikTok_Download_API.git upstream
cd upstream
git checkout --detach 737bf3dfe9de1dbff57990c0ec4c9e02c75c3d0f
make install
export DTK_SECRET_KEY="$(openssl rand -hex 32)"
uv run --frozen pytest tests/unit tests/replay -q --junitxml=unit_replay.xml
make fixtures-up
DTK_DATABASE_URL=postgresql+asyncpg://dtk:dtk_test_password@127.0.0.1:55432/dtk_test uv run --frozen alembic upgrade head
uv run --frozen pytest tests/integration -q -m integration --junitxml=integration.xml
make fixtures-down
```

The database in those commands is a disposable test database. Do not point the
integration tests at a database containing user data; upstream clears test tables.
If an inherited SOCKS proxy causes HTTPX import errors, configure the documented
test environment rather than recording the run as a scanner regression.

For live tests, deploy using upstream `documents/en/02-installation.md`, configure
authorized test subjects as described in `tests/contract/README.md`, populate the
identity pool, and set `DTK_CONTRACT_BASE_URL` and `DTK_CONTRACT_API_KEY` locally.
Then run `uv run --frozen pytest tests/contract -m live -v`. The shipped contract
suite is not itself the scanner's required follower/following benchmark: add the
authorized multi-page case and verify parsed users, exact returned cursors and
termination before claiming live support. Preserve credentials outside evidence.

Run the component benchmark from the upstream checkout with
`uv run --frozen python ../baseline_components.py` after placing that harness next
to `upstream/`. It performs no network requests. Final hybrid test commands cannot
be stated accurately until the actual missing implementation is recovered.
