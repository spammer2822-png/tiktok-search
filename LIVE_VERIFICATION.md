# DTK-only live verification status

Status: **scanner integration verified in deterministic CI; latest integrated live TikTok scan still requires the target Windows machine**.

## Already established

Before this scanner refactor, the local Evil0ctal/Douyin_TikTok_Download_API stack was successfully brought up on the target Windows PC with:

- DTK API healthy
- DTK worker healthy
- Redis healthy
- PostgreSQL healthy
- browser-rpc healthy
- a TikTok guest identity minted successfully
- an upstream TikTok smoke test completed successfully
- an upstream TikTok author/profile request completed successfully

Those results establish that the selected DTK revision, Docker stack, browser-rpc path and guest-identity minting can work on the target machine. They do **not** by themselves prove every scanner integration path.

## Repository verification

The DTK-only branch regression suite runs on:

- Windows / Python 3.11
- Windows / Python 3.12
- Ubuntu / Python 3.11
- Ubuntu / Python 3.12

The active suite verifies the DTK adapter and durable scanner pipeline without requiring a live TikTok account or secret in CI. It covers DTK-only configuration, profile/list normalization, API-key scope handling, identity-pool settings, DTK endpoint selection, opaque cursor persistence, duplicate removal, target matching, terminal-page reuse and interruption/resume semantics.

The CI environment intentionally contains no real DTK API key, browser identity or TikTok credentials.

## Final local live check

On the target Windows PC:

```cmd
cd /d "<TikTokScanner folder>"
py -3.11 -m pip install -r requirements.txt
py -3.11 main.py
```

The scanner should then:

1. check `http://127.0.0.1:8000/readyz`;
2. start Docker Desktop automatically if the Docker engine is not available;
3. locate or install the pinned DTK source according to the saved DTK configuration;
4. start the DTK Docker Compose browser stack if it is not already healthy;
5. load the API key from `DTK_API_KEY`, the ignored local `dtk_api_key.txt`, or the hidden prompt;
6. validate the key has `tiktok:read`;
7. inspect the TikTok identity pool and, when permitted by the key, align the configured pool marks and mint missing TikTok guest identities;
8. request a public TikTok profile through `/api/v1/tiktok/user`;
9. request followers and following through the DTK list endpoints;
10. persist returned opaque cursors and resume from the exact committed cursor after interruption.

For a complete live acceptance run, use an authorized public test account whose followers/following are visible and large enough to require multiple pages. Verify:

- profile UID and secUid normalization;
- multiple follower pages;
- multiple following pages;
- natural `has_more=false` termination;
- no duplicate saved identities;
- target matching;
- report and statistics generation;
- Ctrl+C followed by same-folder resume;
- no API key, cookies or identity secrets in logs, JSON reports or HTML.

Do not intentionally force upstream rate limits. Queue-full, rate-limit, identity-pool and risk-control behavior is covered deterministically by adapter logic/tests and should be observed naturally if it occurs.

## Security

Never commit `dtk_api_key.txt`, DTK `.env`, cookies, browser identity exports or other local credentials to GitHub. The tracked repository snapshot contains no real API key. A user-specific local delivery ZIP may contain `dtk_api_key.txt` only when the owner explicitly requested that convenience; that local credential copy is not part of Git history or CI artifacts.
