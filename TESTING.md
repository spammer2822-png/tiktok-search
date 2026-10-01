# DTK-only verification

Run from the project folder:

```cmd
py -3.11 -m pip install -r requirements-test.txt
py -3.11 run_tests.py
```

Python 3.11 and 3.12 are exercised in GitHub Actions on Windows and Ubuntu.

## Active regression scope

The active suite is intentionally DTK-specific after removal of the old Worker, native Direct and Hybrid transports.

- `tests/test_dtk_backend.py`
  - DTK-only configuration migration/validation
  - author/member normalization
  - private profile handling
  - API-key scope parsing
  - identity-pool tuning
  - DTK profile/followers/following route selection
  - environment credential loading

- `tests/test_dtk_pipeline.py`
  - durable page commits
  - exact opaque cursor persistence
  - duplicate suppression
  - target detection
  - completed-chain reuse without another request
  - resume after a temporary DTK failure

The CI suite uses synthetic local responses. It never needs the real API key and never sends TikTok traffic.

## Live evidence

The upstream DTK stack was separately run on the target Windows machine with:

- API healthy
- worker healthy
- PostgreSQL healthy
- Redis healthy
- browser-rpc healthy
- successful TikTok guest identity mint
- successful real TikTok smoke test
- successful TikTok author-profile request in the Playground

This proves the upstream DTK installation works on the machine. It does not by itself prove every scanner code path, so deterministic integration tests remain separate from live evidence.

## Historical evidence

The repository still contains earlier Worker/Direct/Hybrid benchmarks, audits and verification logs. They are retained as historical project evidence only. They do not describe the active backend and are not loaded by `run_tests.py`.

## Release gate

Before packaging a DTK-only release:

1. The current branch must pass the Windows/Ubuntu Python 3.11/3.12 matrix.
2. `dtk_api_key.txt` must remain ignored and absent from Git history.
3. The release snapshot must be generated from the exact final branch head.
4. The ZIP must be hash-checked after any local-only credential file is added.
5. The GitHub branch and protected rollback branch must remain separate.
