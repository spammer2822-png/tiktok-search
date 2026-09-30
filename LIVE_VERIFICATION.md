# Remaining live verification

Status: **BLOCKED BY ENVIRONMENT**. No authorized live test-account list,
maintainer-configured upstream deployment/API key or Direct session/proxy pool
was supplied for this run. The repository does not contain those credentials.
Offline response fixtures and real loopback TLS tests do not establish live
TikTok compatibility, a largest reliable Direct page size, or production RPS.
No account has been invented or queried to fill this gap.

The original, unmodified Evil0ctal archive and pre-integration test results are
preserved under `hybrid_baseline/`. Its full service test repeat now passes on
GitHub with PostgreSQL/Redis; see `ci_verified_20260930.json`. This removes the
earlier local service-setup blocker, while the live checks below remain open.

## Reproduce the original service test baseline

Run the **Pristine upstream baseline** GitHub Actions workflow using Run workflow.
It restores the exact pinned archive and original file modes, checks every
extracted file's bytes, installs the locked dependencies, builds the web console,
runs migrations, then runs these commands against the original source:

```bash
uv run pytest tests/unit tests/replay -q --junitxml=baseline-unit.xml
uv run pytest tests/integration -q -m integration --junitxml=baseline-integration.xml
```

The workflow saves original logs, JUnit and source hashes. It does not patch
upstream code or mark failed tests as expected. The historical local failures
(inherited proxy dependency, missing test secret, DNS and unavailable Redis)
remain documented separately from the successful repeat.

## Original live service checks

In a separate pristine checkout of the pinned upstream source, follow its
deployment instructions to run its services and configure a test identity.
Set `DTK_CONTRACT_BASE_URL` and `DTK_CONTRACT_API_KEY` locally. Populate that
checkout's explicitly empty `tests/contract/subjects.py` with maintainer-approved
test subjects, retaining the original archive unchanged. Run:

```bash
uv run pytest tests/contract -m live -v --junitxml=baseline-live.xml
```

Those upstream contract tests cover video parsing and pool health. They do not
prove follower/following pagination. Separately use the original deployment's
author-detail, author-followers and author-following operations, as exposed in
that deployment's OpenAPI, for the same authorized account. Save the request
configuration, response bodies, parsed fields and exact returned cursor chain.
Require at least two pages and natural termination in both directions; use an
authorized 10,000-follower/following fixture account when available. Record the
original identity count, worker count, pool count, proxies, page size, token
bucket, retry and timeout settings. Do not remove upstream throttles in this
baseline. Record timestamps and performance separately from the scanner.

## Scanner live checks, same inputs and environment

Create a local `authorized_accounts.json` containing only the supplied test
usernames. Keep session and proxy files outside the repository. From this folder:

```bash
python -m pip install -r requirements-test.txt
TIKTOK_INPUT_JSON=/absolute/path/authorized_accounts.json \
TIKTOK_EXPORT_DIR=/absolute/path/live-worker python main.py
```

On Windows PowerShell set `$env:TIKTOK_INPUT_JSON` and `$env:TIKTOK_EXPORT_DIR`
to those paths, then run `py -3.12 main.py`.

1. Choose a new JSON search and the authorized target. Use normal mode, zero
   skip limits, and the agreed worker/delay/proxy settings. At confirmation select
   **Backend configuration → Worker**. Save configuration and Start.
2. Repeat with separate output folders and identical input/settings for **Direct**
   and **Hybrid**. Supply the private Direct session path when required. Do not
   silently reuse an already completed queue as a fresh benchmark.
3. Verify profile body and normalized output: numeric UID remains a string;
   secUid, avatar and verified/privacy values agree where supplied; missing
   fields remain unknown. Validate both list scenes, actual records, exact cursor
   progression and terminal `hasMore=false`. Check final UID sets for duplicates,
   overlap recovery, missing fixture members and target relationships.
4. Compare Direct page-size candidates on the same stable account, starting with
   35. Record actual records/page, pages, invalid/empty responses and completion
   time. Change only `direct_page_size` between runs. Keep `worker_page_size=0`
   unless the Worker API gains a confirmed, documented count parameter.
5. Where the authorized environment permits, repeat at 100, 500, 1000, 2500 and
   5000 workers and at 100/1000/10000 authorized input accounts. Record active
   workers/connections, pool/admission waits, per-backend attempts/successes,
   records/sec, CPU/RAM, interface bytes, disk I/O and event-loop lag. Compare
   original, first-hybrid and final source snapshots with the same fixtures,
   credentials and host. Changing live account data limits exact comparisons;
   retain timestamps and note the uncertainty.
6. Interrupt a partially collected test scan, resume the same folder, and confirm
   completed accounts are reused and the saved opaque cursor continues correctly.
   Review `scan_state.json`, profile exports, `sucess_find.json`, `scan_stats.json`,
   history, logs and offline HTML. Do not intentionally provoke live rate limits;
   the deterministic 429 suite verifies the required recovery flow locally.

Preserve sanitized logs/configuration, source commit, UTC timing, machine and
dependency versions, outcomes and measured metrics. Exclude cookies, proxy
passwords and private session files. An HTTP 200, empty body or skipped test is
not a completed verification. Report the smallest failing response shape without
credentials if the upstream protocol has changed; add it as a regression fixture
before changing the adapter.
