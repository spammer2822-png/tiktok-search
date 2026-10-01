"""DTK-only TikTok backend with local Docker/identity lifecycle management."""
from __future__ import annotations

import asyncio
import getpass
import itertools
import json
import os
import secrets
import shutil
import subprocess
import sys
import time
from collections import OrderedDict, deque
from contextvars import ContextVar
from pathlib import Path
from urllib.parse import urlparse, urlsplit

import tiktok_worker_scanner as s
from backend_metrics import BackendMetrics

DTK_UPSTREAM = "https://github.com/Evil0ctal/Douyin_TikTok_Download_API.git"
DTK_PIN = "d8f874cd5b647b0ca087a57b15a458c3864439fa"
CLOAKBROWSER_COMMIT = "9bc5e374d7fc3a4099360bbd93e570b1e7ec8618"

DEFAULTS = {
    "backend_mode": "dtk",
    "dtk_base_url": "http://127.0.0.1:8000",
    "dtk_repo_path": "",
    "dtk_api_key_file": "dtk_api_key.txt",
    "dtk_auto_start": True,
    "dtk_auto_clone": True,
    "dtk_auto_mint": True,
    "dtk_min_usable_identities": 3,
    "dtk_target_identities": 8,
    "dtk_identity_wait_seconds": 600,
    "dtk_proxy_mint_cooldown_seconds": 900,
    "dtk_identity_control_poll_seconds": 5,
    "dtk_page_size": 35,
    "dtk_wait_seconds": 30,
    "dtk_request_attempts": 5,
    "dtk_max_connections": 64,
    "dtk_startup_timeout_seconds": 300,
    "dtk_stop_stack_on_exit": False,
    "avatar_workers": 0,
}

RETRYABLE_CODES = {
    "RATE_LIMITED",
    "QUEUE_FULL",
    "IDENTITY_POOL_EXHAUSTED",
    "ENDPOINT_CIRCUIT_OPEN",
    "UPSTREAM_RISK_CONTROL",
    "SIGNING_FAILED",
    "INTERNAL",
    "DOWNLOADER_UNAVAILABLE",
}
NON_RETRYABLE_CODES = {
    "INVALID_URL",
    "UNSUPPORTED_CONTENT",
    "INVALID_PARAM",
    "UNAUTHENTICATED",
    "FORBIDDEN_SCOPE",
    "NOT_FOUND",
    "CONTENT_PRIVATE",
    "UPSTREAM_CHANGED",
    "CANCELLED",
    "METHOD_NOT_ALLOWED",
    "UNSUPPORTED_MEDIA_TYPE",
    "NOT_CONFIGURED",
}

# Errors that indicate DTK's current TikTok identity/session pool needs attention.
# Recovery is serialized and rate-limited so a burst of failed requests cannot
# accidentally launch a browser-mint stampede.
IDENTITY_RECOVERY_CODES = {
    "IDENTITY_POOL_EXHAUSTED",
    "UPSTREAM_RISK_CONTROL",
    "SIGNING_FAILED",
}
IDENTITY_REPAIR_COOLDOWN_SECONDS = 15.0
IDENTITY_MINT_TASK_CAP = 10
IDENTITY_DEBUG_SECONDS = 5.0
IDENTITY_RATE_LIMIT_FLOOR_SECONDS = 1.0
IDENTITY_RETRY_MAX_SECONDS = 30.0
IDENTITY_FATAL_REASONS = {
    "browser_rpc_unconfigured",
    "no_free_proxy",
    "proxy_not_found",
    "proxy_undecryptable",
}


def _bool(value, name):
    if type(value) is not bool:
        raise s.ExporterError(f"{name} must be a boolean.")


def validate(config):
    mode = config.get("backend_mode", "dtk")
    if mode != "dtk":
        raise s.ExporterError("This build is DTK-only; backend_mode must be dtk.")
    base = str(config.get("dtk_base_url", DEFAULTS["dtk_base_url"])).rstrip("/")
    parsed = urlparse(base)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise s.ExporterError("dtk_base_url must be an http(s) URL.")
    for name in ("dtk_auto_start", "dtk_auto_clone", "dtk_auto_mint", "dtk_stop_stack_on_exit"):
        _bool(config.get(name, DEFAULTS[name]), name)
    for name, low, high in (
        ("dtk_min_usable_identities", 1, 200),
        ("dtk_target_identities", 1, 200),
        ("dtk_identity_wait_seconds", 1, 3600),
        ("dtk_proxy_mint_cooldown_seconds", 30, 86400),
        ("dtk_identity_control_poll_seconds", 1, 60),
        ("dtk_page_size", 1, 50),
        ("dtk_wait_seconds", 1, 30),
        ("dtk_request_attempts", 1, 20),
        ("dtk_max_connections", 1, 512),
        ("dtk_startup_timeout_seconds", 15, 1800),
    ):
        value = config.get(name, DEFAULTS[name])
        if type(value) is not int or not low <= value <= high:
            raise s.ExporterError(f"{name} must be an integer from {low} to {high}.")
    if config.get("dtk_target_identities", 8) < config.get("dtk_min_usable_identities", 3):
        raise s.ExporterError("dtk_target_identities cannot be below dtk_min_usable_identities.")


def migrate_config(config):
    migrated = dict(config)
    for key, value in DEFAULTS.items():
        migrated.setdefault(key, value)
    # The earlier DTK-only build generated 120 seconds here, which is shorter
    # than DTK's own manual-mint queue can legitimately need. Upgrade only that
    # old generated default; explicit custom values are otherwise preserved.
    if migrated.get("dtk_identity_wait_seconds") == 120:
        migrated["dtk_identity_wait_seconds"] = 600
    migrated["backend_mode"] = "dtk"
    migrated.pop("direct_session_file", None)
    migrated.pop("worker_backend_max_connections", None)
    migrated.pop("direct_backend_max_connections", None)
    migrated.pop("worker_backend_keepalive_connections", None)
    migrated.pop("direct_backend_keepalive_connections", None)
    migrated.pop("direct_initial_concurrency", None)
    migrated.pop("direct_retry_attempts", None)
    migrated.pop("worker_retry_attempts", None)
    migrated.pop("direct_429_cooldown", None)
    migrated.pop("race_failed_requests", None)
    return migrated


class DtkApiError(s.ScannerApiError):
    def __init__(self, code, message="", *, status=None, retry_after=None, request_id=None, details=None):
        kind_map = {
            "RATE_LIMITED": "rate_limited",
            "QUEUE_FULL": "queue_full",
            "IDENTITY_POOL_EXHAUSTED": "identity_pool_exhausted",
            "ENDPOINT_CIRCUIT_OPEN": "circuit_open",
            "UPSTREAM_RISK_CONTROL": "risk_control",
            "SIGNING_FAILED": "signing_failed",
            "INTERNAL": "temporary_server_failure",
            "DOWNLOADER_UNAVAILABLE": "temporary_server_failure",
            "UNAUTHENTICATED": "access_denied",
            "FORBIDDEN_SCOPE": "access_denied",
            "NOT_FOUND": "not_found",
            "CONTENT_PRIVATE": "content_private",
            "UPSTREAM_CHANGED": "invalid_response",
            "INVALID_URL": "invalid_response",
            "INVALID_PARAM": "invalid_response",
            "NOT_CONFIGURED": "configuration_error",
        }
        kind = kind_map.get(code, "dtk_error")
        safe = f"DTK {code}"
        if request_id:
            safe += f" (request {request_id})"
        if message:
            safe += f": {message}"
        super().__init__(safe, kind=kind, code=status, retryable=code in RETRYABLE_CODES)
        self.dtk_code = code
        self.retry_after = retry_after
        self.request_id = request_id
        self.details = details


def _run(command, *, cwd=None, env=None, timeout=120):
    flags = 0
    if os.name == "nt":
        flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=env,
        timeout=timeout,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        creationflags=flags,
        check=False,
    )


def _docker_ready():
    try:
        result = _run(["docker", "info", "--format", "{{.ServerVersion}}"], timeout=15)
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0 and bool(result.stdout.strip())


def _start_docker_desktop():
    if _docker_ready():
        return
    if shutil.which("docker"):
        try:
            _run(["docker", "desktop", "start"], timeout=30)
        except Exception:
            pass
    if os.name == "nt" and not _docker_ready():
        candidates = [
            Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Docker" / "Docker" / "Docker Desktop.exe",
            Path(os.environ.get("LOCALAPPDATA", "")) / "Programs" / "Docker" / "Docker" / "Docker Desktop.exe",
        ]
        for executable in candidates:
            if executable.is_file():
                try:
                    subprocess.Popen(
                        [str(executable)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        creationflags=getattr(subprocess, "DETACHED_PROCESS", 0)
                        | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
                    )
                    break
                except OSError:
                    continue
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if _docker_ready():
            return
        time.sleep(2)
    raise s.ExporterError("Docker Desktop did not become ready within 3 minutes.")


def _default_repo_path():
    override = os.getenv("DTK_REPO_PATH", "").strip()
    if override:
        return Path(override).expanduser()
    candidates = [
        Path.home() / "Downloads" / "Douyin_TikTok_Download_API",
        Path(__file__).resolve().parent.parent / "Douyin_TikTok_Download_API",
        Path.cwd() / "Douyin_TikTok_Download_API",
    ]
    for path in candidates:
        if (path / "docker" / "compose.yml").is_file():
            return path
    return candidates[0]


def _ensure_repo(settings):
    configured = str(settings.get("dtk_repo_path", "")).strip()
    path = Path(configured).expanduser() if configured else _default_repo_path()
    if (path / "docker" / "compose.yml").is_file():
        return path
    if not settings.get("dtk_auto_clone", True):
        raise s.ExporterError(f"DTK repository not found: {path}")
    if not shutil.which("git"):
        raise s.ExporterError("DTK is missing and Git is not installed, so it cannot be cloned automatically.")
    path.parent.mkdir(parents=True, exist_ok=True)
    s.console(f"[DTK] Repository missing; cloning pinned upstream into {path} ...")
    result = _run(["git", "clone", DTK_UPSTREAM, str(path)], timeout=300)
    if result.returncode != 0:
        raise s.ExporterError("Automatic DTK clone failed. Install/clone Evil0ctal DTK manually and set DTK_REPO_PATH.")
    checkout = _run(["git", "checkout", DTK_PIN], cwd=path, timeout=60)
    if checkout.returncode != 0:
        raise s.ExporterError("DTK cloned, but the verified upstream revision could not be selected.")
    return path


def _ensure_dtk_env(repo):
    env_path = repo / ".env"
    lines = []
    if env_path.exists():
        lines = env_path.read_text(encoding="utf-8").splitlines()
    keys = {line.split("=", 1)[0].strip() for line in lines if "=" in line and not line.lstrip().startswith("#")}
    additions = []
    if "DTK_SECRET_KEY" not in keys:
        additions.append("DTK_SECRET_KEY=" + secrets.token_urlsafe(48))
    pg = None
    if "POSTGRES_PASSWORD" not in keys:
        pg = secrets.token_hex(24)
        additions.append("POSTGRES_PASSWORD=" + pg)
    if "REDIS_PASSWORD" not in keys:
        redis = secrets.token_hex(24)
        additions.append("REDIS_PASSWORD=" + redis)
    else:
        redis = None
    combined = lines + additions
    values = {}
    for line in combined:
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            values[k.strip()] = v.strip()
    if "DTK_DATABASE_URL" not in values:
        password = values.get("POSTGRES_PASSWORD") or pg
        additions.append(f"DTK_DATABASE_URL=postgresql+asyncpg://dtk:{password}@postgres:5432/dtk")
    if "DTK_REDIS_URL" not in values:
        password = values.get("REDIS_PASSWORD") or redis
        additions.append(f"DTK_REDIS_URL=redis://:{password}@redis:6379/0")
    if "DTK_BROWSER_RPC_URL" not in values:
        additions.append("DTK_BROWSER_RPC_URL=http://browser-rpc:9000")
    if additions:
        text = "\n".join(lines + additions).rstrip() + "\n"
        env_path.write_text(text, encoding="utf-8")


def _compose_up(repo, *, force_browser_rebuild=False):
    environment = os.environ.copy()
    environment["CLOAKBROWSER_COMMIT"] = CLOAKBROWSER_COMMIT
    compose = ["docker", "compose", "-p", "dtk", "-f", "docker/compose.yml", "--profile", "browser"]

    if force_browser_rebuild:
        s.console(
            "[DTK] Browser runtime pin changed; rebuilding browser-rpc with the verified "
            "CloakBrowser revision before identity minting."
        )
        built = _run(compose + ["build", "browser-rpc"], cwd=repo, env=environment, timeout=1200)
        if built.returncode != 0:
            tail = "\n".join(built.stdout.splitlines()[-20:])
            raise s.ExporterError("DTK browser-rpc rebuild failed.\n" + tail)

    base = compose + ["up", "-d"]
    result = _run(base, cwd=repo, env=environment, timeout=240)
    if result.returncode == 0:
        return
    s.console("[DTK] Existing images were not enough; building the DTK/browser images once ...")
    result = _run(base + ["--build"], cwd=repo, env=environment, timeout=1200)
    if result.returncode != 0:
        tail = "\n".join(result.stdout.splitlines()[-20:])
        raise s.ExporterError("DTK Docker stack failed to start.\n" + tail)


def _running_browser_pin():
    """Return the browser-rpc backend pin stamped into the running container."""
    if not _docker_ready():
        return None
    try:
        result = _run(
            [
                "docker", "inspect", "--format",
                "{{range .Config.Env}}{{println .}}{{end}}",
                "dtk-browser-rpc-1",
            ],
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    prefix = "DTK_BROWSER_BACKEND_PIN="
    for line in result.stdout.splitlines():
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    return None


def _ensure_browser_revision(settings):
    """Rebuild browser-rpc only when the running image is on an older pin."""
    current = _running_browser_pin()
    desired_suffix = "@" + CLOAKBROWSER_COMMIT
    if current and current.endswith(desired_suffix):
        return
    if not _docker_ready():
        return
    repo = _ensure_repo(settings)
    _ensure_dtk_env(repo)
    if current:
        s.console(
            f"[DTK] Existing browser-rpc pin {current.rsplit('@', 1)[-1][:12]} is older/different; "
            f"upgrading to {CLOAKBROWSER_COMMIT[:12]}."
        )
    _compose_up(repo, force_browser_rebuild=True)


async def _health(base_url, timeout=3.0):
    try:
        import httpx
        async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
            response = await client.get(base_url.rstrip("/") + "/readyz")
            if response.status_code != 200:
                return False
            body = response.json()
            return body.get("status") == "ok"
    except Exception:
        return False


async def ensure_local_dtk(settings):
    base = str(settings.get("dtk_base_url", DEFAULTS["dtk_base_url"])).rstrip("/")
    host = (urlparse(base).hostname or "").casefold()
    healthy = await _health(base)
    if healthy:
        if (
            settings.get("dtk_auto_start", True)
            and host in {"127.0.0.1", "localhost", "::1"}
        ):
            await asyncio.to_thread(_ensure_browser_revision, settings)
            # Re-check after a possible browser image recreation. The API itself
            # normally remains up, but this makes the startup contract explicit.
            if not await _health(base):
                raise s.ExporterError("DTK API stopped responding after browser-rpc upgrade.")
        return None
    if not settings.get("dtk_auto_start", True):
        raise s.ExporterError(f"DTK is not reachable at {base}; auto-start is disabled.")
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise s.ExporterError("Auto-start is only supported for a local DTK URL.")
    s.console("[DTK] Local API is offline; starting Docker Desktop and the DTK stack automatically ...")
    await asyncio.to_thread(_start_docker_desktop)
    repo = await asyncio.to_thread(_ensure_repo, settings)
    await asyncio.to_thread(_ensure_dtk_env, repo)
    await asyncio.to_thread(_compose_up, repo)
    await asyncio.to_thread(_ensure_browser_revision, settings)
    deadline = time.monotonic() + settings.get("dtk_startup_timeout_seconds", 300)
    while time.monotonic() < deadline:
        if await _health(base):
            s.console("[DTK] API, PostgreSQL and Redis are ready.")
            return repo
        await asyncio.sleep(2)
    raise s.ExporterError("DTK containers started, but /readyz never became healthy.")


def _key_path(settings):
    value = str(settings.get("dtk_api_key_file", "dtk_api_key.txt")).strip() or "dtk_api_key.txt"
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path(__file__).resolve().parent / path
    return path


async def load_api_key(settings):
    value = os.getenv("DTK_API_KEY", "").strip()
    path = _key_path(settings)
    if not value and path.is_file():
        value = path.read_text(encoding="utf-8").strip()
    if not value and sys.stdin is not None and sys.stdin.isatty():
        s.console("[DTK] API key not found. Paste it once; it will be stored only in the local ignored credential file.")
        value = (await asyncio.to_thread(getpass.getpass, "DTK API key: ")).strip()
        if value:
            path.write_text(value + "\n", encoding="utf-8")
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass
    if not value:
        raise s.ExporterError(
            "DTK API key missing. Set DTK_API_KEY or place the key in dtk_api_key.txt beside main.py."
        )
    if not value.startswith("dtk_"):
        raise s.ExporterError("The DTK API key format is invalid.")
    return value


def _avatar_url(author):
    avatar = author.get("avatar")
    if isinstance(avatar, dict):
        value = avatar.get("url")
        if isinstance(value, str):
            return value
        urls = avatar.get("urls")
        if isinstance(urls, list):
            return next((x for x in urls if isinstance(x, str)), "")
    return ""


def _profile_from_author(author, username):
    if not isinstance(author, dict):
        raise s.InvalidBackendResponse("DTK profile data must be an object.")
    unique = author.get("unique_id")
    if not isinstance(unique, str) or not unique:
        unique = username
    if unique.casefold() != username.casefold():
        raise s.InvalidBackendResponse("DTK returned a different username than requested.")
    uid = author.get("uid")
    if isinstance(uid, bool) or not isinstance(uid, (str, int)):
        raise s.InvalidBackendResponse("DTK profile uid is missing.")
    uid = str(uid)
    sec_uid = author.get("sec_uid")
    if not isinstance(sec_uid, str) or not sec_uid:
        raise s.InvalidBackendResponse("DTK profile sec_uid is missing; relationship lists cannot be addressed safely.")
    stats = author.get("stats") if isinstance(author.get("stats"), dict) else {}
    raw = author.get("raw") if isinstance(author.get("raw"), dict) else {}
    privacy = raw.get("privateAccount")
    if privacy is True:
        raise s.PublicProfileRequired(f"@{unique} is private; list scanning skipped.")
    return s.ProfileSnapshot(
        username=unique,
        uid=uid,
        display_name=author.get("nickname") if isinstance(author.get("nickname"), str) else unique,
        sec_uid=sec_uid,
        profile_url=author.get("web_url") if isinstance(author.get("web_url"), str) else f"{s.TIKTOK_ORIGIN}/@{unique}",
        private_account=False,
        verified=author.get("verified") if isinstance(author.get("verified"), bool) else None,
        avatar_url=_avatar_url(author),
        follower_count=stats.get("follower_count") if isinstance(stats.get("follower_count"), int) else None,
        following_count=stats.get("following_count") if isinstance(stats.get("following_count"), int) else None,
        likes_count=stats.get("total_digg") if isinstance(stats.get("total_digg"), int) else None,
        video_count=stats.get("content_count") if isinstance(stats.get("content_count"), int) else None,
        advertised_counts={
            "followers": stats.get("follower_count"),
            "following": stats.get("following_count"),
            "likes": stats.get("total_digg"),
            "videos": stats.get("content_count"),
        },
        following_visible=None,
        metadata={"backend": "dtk", "signature": author.get("signature")},
        raw_user=author if s.KEEP_RAW_MEMBER_DATA else {},
    )


def _member_from_author(author):
    if not isinstance(author, dict):
        return author
    stats = author.get("stats") if isinstance(author.get("stats"), dict) else {}
    raw = author.get("raw") if isinstance(author.get("raw"), dict) else {}
    return {
        "uniqueId": author.get("unique_id") or "",
        "nickname": author.get("nickname") or "",
        "avatarThumb": _avatar_url(author),
        "user_id": author.get("uid") or "",
        "secUid": author.get("sec_uid") or "",
        "signature": author.get("signature") or "",
        "privateAccount": raw.get("privateAccount") if isinstance(raw.get("privateAccount"), bool) else None,
        "verified": "Yes✅" if author.get("verified") is True else "No❌" if author.get("verified") is False else None,
        "followers": stats.get("follower_count"),
        "following": stats.get("following_count"),
        "videoCount": stats.get("content_count"),
        "heartCount": stats.get("total_digg"),
        **({"raw_dtk": author} if s.KEEP_RAW_MEMBER_DATA else {}),
    }


def _append_jsonl(path, payload):
    """Append one durable, redacted diagnostic record without truncating older errors."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(s.REDACTOR.clean(payload), ensure_ascii=True, separators=(",", ":")) + "\n"
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(line)
        stream.flush()
        os.fsync(stream.fileno())


class DtkClient:
    bootstrap_mode = False

    def __init__(self, gate, pool, *, settings=None, raw_directory=None, avatar_directory=None, client_factory=None):
        try:
            import httpx
        except ImportError:
            raise s.MissingDependency("Install: py -3.11 -m pip install -r requirements.txt") from None
        self.httpx = httpx
        self.gate = gate
        self.pool = pool
        self.settings = migrate_config(settings or {})
        validate(self.settings)
        self.base_url = self.settings["dtk_base_url"].rstrip("/")
        self.raw_directory = raw_directory
        self.output_directory = (
            Path(avatar_directory)
            if avatar_directory is not None
            else (Path(raw_directory).parent if raw_directory is not None else None)
        )
        self.client = None
        self.api_key = ""
        self.request_retries = ContextVar("dtk_request_retries", default=0)
        self.profile_cache = OrderedDict()
        self.metrics = getattr(gate.stats, "backends", None) or BackendMetrics()
        if gate.stats:
            gate.stats.backends = self.metrics
        self.avatars = None
        self.avatar_client = None
        self.avatar_backfill_task = None
        self.avatar_backfill_requested = False
        self.identity_repair_lock = asyncio.Lock()
        self.identity_repair_last_mint = 0.0
        self.dtk_error_lock = asyncio.Lock()
        self.identity_task_states = {}
        self.identity_task_proxies = {}
        self.identity_proxy_cooldown_until = {}
        self.identity_proxy_info = {}
        self.identity_proxy_cursor = 0
        self.identity_control_blocked_until = 0.0
        self.identity_control_last_notice_until = 0.0
        self.identity_activity_seen = set()
        self.identity_activity_current = None
        self.identity_activity_backoff = None
        if avatar_directory is not None:
            from avatar_cache import AvatarCache
            self.avatars = AvatarCache(
                avatar_directory,
                self.download_avatar,
                disk_io=s.disk_call,
                concurrency=self.settings.get("avatar_workers", 0) or gate.ceiling,
            )

    async def __aenter__(self):
        await ensure_local_dtk(self.settings)
        self.api_key = await load_api_key(self.settings)
        for redactor in (s.REDACTOR,):
            redactor.secrets = sorted(set(redactor.secrets).union({self.api_key}), key=len, reverse=True)
        limits = self.httpx.Limits(
            max_connections=self.settings["dtk_max_connections"],
            max_keepalive_connections=self.settings["dtk_max_connections"],
            keepalive_expiry=90.0,
        )
        self.client = self.httpx.AsyncClient(
            base_url=self.base_url,
            headers={"X-API-Key": self.api_key, "Accept": "application/json"},
            timeout=self.httpx.Timeout(40.0, connect=10.0, write=20.0, pool=10.0),
            limits=limits,
            trust_env=False,
        )
        self.avatar_client = self.httpx.AsyncClient(
            timeout=self.httpx.Timeout(12.0, connect=8.0),
            limits=self.httpx.Limits(max_connections=max(4, min(32, self.gate.ceiling))),
            trust_env=False,
            follow_redirects=False,
        )
        try:
            await self._validate_key()
        except DtkApiError as exc:
            await self._record_dtk_error(exc, "DTK startup authentication")
            raise
        await self._ensure_identity_pool()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.avatar_backfill_task is not None:
            if not self.avatar_backfill_task.done():
                self.avatar_backfill_task.cancel()
            await asyncio.gather(self.avatar_backfill_task, return_exceptions=True)
        if self.avatars is not None:
            await s.disk_call(self.avatars.close)
        if self.avatar_client is not None:
            await self.avatar_client.aclose()
        if self.client is not None:
            await self.client.aclose()

    async def _control_request(self, method, path, **kwargs):
        """Normalize local DTK control-plane transport failures into safe DTK errors."""
        try:
            return await self.client.request(method, path, **kwargs)
        except (self.httpx.ConnectError, self.httpx.ConnectTimeout):
            raise DtkApiError(
                "INTERNAL",
                "DTK local API connection failed during a control-plane request",
                retry_after=2,
                details={"path": path, "transport": "connect"},
            ) from None
        except self.httpx.TimeoutException:
            raise DtkApiError(
                "INTERNAL",
                "DTK local API timed out during a control-plane request",
                retry_after=2,
                details={"path": path, "transport": "timeout"},
            ) from None
        except self.httpx.RequestError:
            raise DtkApiError(
                "INTERNAL",
                "DTK local API transport failed during a control-plane request",
                retry_after=2,
                details={"path": path, "transport": "request_error"},
            ) from None

    async def _wait_identity_control_window(self):
        """Honor one shared DTK Retry-After window across pool/task/proxy polling."""
        delay = self.identity_control_blocked_until - time.monotonic()
        if delay <= 0:
            return
        if self.identity_control_blocked_until > self.identity_control_last_notice_until:
            self.identity_control_last_notice_until = self.identity_control_blocked_until
            s.console(
                f"[DTK DEBUG] DTK control-plane rate limit active; pausing all identity "
                f"management requests for {delay:.1f}s."
            )
        await self.gate.wait(delay)

    def _apply_identity_control_retry_after(self, exc):
        if getattr(exc, "dtk_code", None) != "RATE_LIMITED":
            return
        delay = max(
            IDENTITY_RATE_LIMIT_FLOOR_SECONDS,
            float(getattr(exc, "retry_after", None) or self.settings["dtk_identity_control_poll_seconds"]),
        )
        self.identity_control_blocked_until = max(
            self.identity_control_blocked_until,
            time.monotonic() + delay,
        )

    async def _management_data(self, method, path, **kwargs):
        """One globally paced DTK management request with shared 429 handling."""
        await self._wait_identity_control_window()
        response = await self._control_request(method, path, **kwargs)
        try:
            return self._unwrap(response), response
        except DtkApiError as exc:
            self._apply_identity_control_retry_after(exc)
            raise

    async def _validate_key(self):
        data, _response = await self._management_data("GET", "/api/v1/auth/me")
        if not isinstance(data, dict):
            raise s.ExporterError("DTK /auth/me returned an invalid response.")
        user = data.get("user") if isinstance(data.get("user"), dict) else {}
        scopes = set(user.get("scopes") or [])
        # DTK API keys remain scope-bounded even when owned by an admin account.
        if "tiktok:read" not in scopes:
            raise s.ExporterError("DTK API key lacks the tiktok:read scope.")
        self.key_scopes = scopes
        limit = data.get("rate_limit_per_min")
        if limit is None:
            rendered = "not reported"
        elif isinstance(limit, (int, float)) and limit <= 0:
            rendered = "unlimited"
        else:
            rendered = f"{limit} requests/minute"
        s.console(f"[DTK DEBUG] Authenticated local API key; effective rate limit: {rendered}.")

    async def _record_dtk_error(self, exc, context):
        """Persist every DTK/API transport failure to errors.log and dtk_errors.jsonl."""
        kind = getattr(exc, "kind", type(exc).__name__)
        code = getattr(exc, "dtk_code", None) or getattr(exc, "code", None)
        retryable = bool(getattr(exc, "retryable", False))
        request_id = getattr(exc, "request_id", None)
        retry_after = getattr(exc, "retry_after", None)
        safe_message = s.REDACTOR.text(str(exc))
        payload = {
            "timestamp_utc": s.utc_iso(),
            "context": str(context),
            "error_type": type(exc).__name__,
            "kind": kind,
            "code": code,
            "retryable": retryable,
            "retry_after": retry_after,
            "request_id": request_id,
            "details": getattr(exc, "details", None),
            "message": safe_message,
        }
        s.console(f"[DTK ERROR] {context}: {safe_message}", error=True)
        if self.output_directory is not None:
            async with self.dtk_error_lock:
                await s.disk_call(_append_jsonl, self.output_directory / "dtk_errors.jsonl", payload)

    @staticmethod
    def _identity_debug(row, minimum, target):
        row = row if isinstance(row, dict) else {}
        parts = [
            f"usable={int(row.get('usable') or 0)}/{minimum}",
            f"target={target}",
        ]
        labels = {
            "total": "total",
            "healthy": "healthy",
            "unhealthy": "unhealthy",
            "pending": "pending",
            # This is a database identity-state count, NOT the number of queued
            # /identity/mint tasks. Label it plainly so minting=0 cannot mislead.
            "minting": "pool_minting_rows",
            "min_size": "min_size",
            "target_size": "target_size",
            "auto": "auto",
        }
        for key, label in labels.items():
            if key in row and row.get(key) is not None:
                parts.append(f"{label}={row.get(key)}")
        return " | ".join(parts)

    def _unwrap(self, response):
        request_id = response.headers.get("X-Request-ID")
        try:
            body = response.json()
        except ValueError:
            raise DtkApiError("INVALID_RESPONSE", "non-JSON response", status=response.status_code, request_id=request_id)
        if not isinstance(body, dict):
            raise DtkApiError("INVALID_RESPONSE", "invalid response envelope", status=response.status_code, request_id=request_id)
        if body.get("success") is False:
            error = body.get("error") if isinstance(body.get("error"), dict) else {}
            retry_after = error.get("retry_after")
            if retry_after is None:
                retry_after = s.retry_after_seconds(response.headers.get("Retry-After"))
            raise DtkApiError(
                str(error.get("code") or "DTK_ERROR"),
                str(error.get("message") or ""),
                status=response.status_code,
                retry_after=retry_after,
                request_id=request_id or body.get("meta", {}).get("request_id"),
                details=error.get("details"),
            )
        return body.get("data")

    async def _poll_task(self, task_id, deadline_seconds=120):
        delay = 0.35
        deadline = time.monotonic() + deadline_seconds
        while time.monotonic() < deadline:
            response = await self.client.get(f"/api/v1/tasks/{task_id}")
            task = self._unwrap(response)
            if not isinstance(task, dict):
                raise DtkApiError("INVALID_RESPONSE", "task result is not an object")
            state = task.get("state")
            if state == "done":
                return task.get("data")
            if state == "failed":
                error = task.get("error") if isinstance(task.get("error"), dict) else {}
                raise DtkApiError(
                    str(error.get("code") or "INTERNAL"),
                    str(error.get("message") or ""),
                    retry_after=error.get("retry_after"),
                    details=error.get("details"),
                )
            await asyncio.sleep(delay)
            delay = min(2.0, delay * 1.5)
        raise DtkApiError("QUEUE_FULL", "DTK task is still running", retry_after=2)

    async def _call(self, endpoint, params, operation):
        attempts = self.settings["dtk_request_attempts"]
        last = None
        for attempt in range(attempts):
            self.request_retries.set(attempt)
            self.gate.check()
            started = time.monotonic()
            status = None
            size = 0
            success = False
            failure = None
            records = 0
            try:
                await self.gate.pace()
                async with self.gate.slot():
                    self.gate.check()
                    self.metrics.begin("dtk")
                    if self.gate.stats:
                        self.gate.stats.request_started(retry=attempt > 0)
                    response = await self.client.get(endpoint, params=params)
                status = response.status_code
                size = len(response.content)
                data = self._unwrap(response)
                if status == 202:
                    if not isinstance(data, dict) or not data.get("task_id"):
                        raise DtkApiError("INVALID_RESPONSE", "202 response did not include task_id", status=202)
                    data = await self._poll_task(data["task_id"], max(60, self.settings["dtk_wait_seconds"] * 4))
                if operation == "profile":
                    records = 1
                elif isinstance(data, dict) and isinstance(data.get("items"), list):
                    records = len(data["items"])
                success = True
                return data
            except DtkApiError as exc:
                failure = exc
                last = exc
                await self._record_dtk_error(exc, f"{operation} request attempt {attempt + 1}/{attempts}")
                if exc.kind == "access_denied":
                    self.gate.blocked_reason = "DTK API authentication/scope failure; fix the local API key."
                if exc.dtk_code in IDENTITY_RECOVERY_CODES:
                    try:
                        await self._repair_identity_pool(exc.dtk_code)
                    except s.ExporterError:
                        # If DTK cannot restore the configured minimum, do not
                        # keep firing TikTok requests with an unhealthy pool.
                        raise
                    except DtkApiError as repair_exc:
                        await self._record_dtk_error(repair_exc, "identity self-heal")
                        s.console(
                            f"[DTK DEBUG] Identity self-heal could not complete ({repair_exc.dtk_code}); "
                            "the normal request retry policy will continue."
                        )
                if not exc.retryable or attempt + 1 >= attempts:
                    raise
                delay = float(exc.retry_after or min(30, 2 ** attempt))
                s.console(f"[DTK RETRY {attempt + 1}/{attempts - 1}] {exc.dtk_code}; waiting {delay:.1f}s.")
                await self.gate.wait(delay)
            except (self.httpx.ConnectError, self.httpx.ConnectTimeout):
                failure = s.ScannerApiError("DTK local API connection failed.", kind="network_error", retryable=True)
                last = failure
                await self._record_dtk_error(failure, f"{operation} local API connection")
                if attempt + 1 >= attempts:
                    raise failure from None
                await self.gate.wait(min(10, 2 ** attempt))
            except (self.httpx.TimeoutException, TimeoutError):
                failure = s.ScannerApiError("DTK local API timed out.", kind="response_timeout", retryable=True)
                last = failure
                await self._record_dtk_error(failure, f"{operation} local API timeout")
                if attempt + 1 >= attempts:
                    raise failure from None
                await self.gate.wait(min(10, 2 ** attempt))
            finally:
                elapsed = time.monotonic() - started
                if status is not None or failure is not None:
                    kind = getattr(failure, "kind", None)
                    code = status
                    self.metrics.end("dtk", operation, elapsed, success=success, records=records, kind=kind, code=code, size=size)
                    if self.gate.stats:
                        self.gate.stats.request_finished(
                            success=success,
                            latency=elapsed,
                            kind=kind,
                            code=code,
                            retry=attempt > 0,
                            cancelled=False,
                        )
                    if success:
                        self.gate.observe(elapsed, self.pool)
                    else:
                        self.gate.observe_failure()
        raise last or s.ExporterError("DTK retry budget exhausted.")

    async def _admin_get(self, path, **kwargs):
        data, _response = await self._management_data("GET", path, **kwargs)
        return data

    async def _admin_put(self, path, value):
        data, _response = await self._management_data("PUT", path, json={"value": value})
        return data

    async def _identity_pool_row(self):
        pool = await self._admin_get("/api/v1/admin/identities/pool")
        platforms = pool.get("platforms") if isinstance(pool, dict) else None
        row = next((x for x in platforms or [] if x.get("platform") == "tiktok"), None)
        if not row:
            raise s.ExporterError("DTK identity pool did not report TikTok.")
        return pool, row

    def _can_manage_identities(self):
        return bool({"identity:manage", "admin"} & getattr(self, "key_scopes", set()))

    async def _mint_proxy_inventory(self):
        """Healthy proxy candidates not already bound, cooling, or reserved by a mint task."""
        all_proxies = await self._admin_get("/api/v1/admin/proxies")
        identities = await self._admin_get(
            "/api/v1/admin/identities",
            params={"platform": "tiktok", "limit": 200},
        )
        all_proxies = all_proxies if isinstance(all_proxies, list) else []
        identities = identities if isinstance(identities, list) else []
        proxies = [
            row for row in all_proxies
            if isinstance(row, dict) and row.get("healthy") is True
        ]

        bound = {
            str(row.get("proxy_id"))
            for row in identities
            if isinstance(row, dict)
            and row.get("proxy_id")
            and str(row.get("state") or "").lower() != "retired"
        }
        reserved = {
            str(proxy_id)
            for task_id, proxy_id in self.identity_task_proxies.items()
            if proxy_id
            and self.identity_task_states.get(task_id, "submitted") in {"submitted", "queued", "running"}
        }
        now = time.monotonic()
        candidates = []
        cooling = []
        for proxy in proxies:
            if not proxy.get("id"):
                continue
            proxy_id = str(proxy["id"])
            self.identity_proxy_info[proxy_id] = proxy
            if not proxy.get("decryptable", True):
                continue
            if proxy_id in bound or proxy_id in reserved:
                continue
            until = float(self.identity_proxy_cooldown_until.get(proxy_id, 0.0) or 0.0)
            if until > now:
                cooling.append((proxy_id, until))
                continue
            candidates.append(proxy)

        return {
            "configured": len(all_proxies),
            "healthy": len(proxies),
            "bound": len(bound),
            "reserved": len(reserved),
            "candidates": candidates,
            "cooling": cooling,
        }

    def _proxy_debug_name(self, proxy_id):
        proxy = self.identity_proxy_info.get(str(proxy_id), {})
        label = proxy.get("label") if isinstance(proxy, dict) else None
        country = proxy.get("country") if isinstance(proxy, dict) else None
        bits = [str(proxy_id)[:8]]
        if label:
            bits.append(str(label))
        if country:
            bits.append(str(country))
        return "/".join(bits)

    def _cooldown_mint_proxy(self, proxy_id, reason):
        if not proxy_id:
            return
        seconds = float(self.settings["dtk_proxy_mint_cooldown_seconds"])
        until = time.monotonic() + seconds
        self.identity_proxy_cooldown_until[str(proxy_id)] = max(
            float(self.identity_proxy_cooldown_until.get(str(proxy_id), 0.0) or 0.0),
            until,
        )
        s.console(
            f"[DTK DEBUG] Proxy {self._proxy_debug_name(proxy_id)} failed TikTok identity minting "
            f"({reason}); excluding it from new mint attempts for {int(seconds)}s."
        )

    async def _request_identity_mint(self, count, *, reason):
        if count <= 0:
            return []
        if not self._can_manage_identities():
            raise s.ExporterError(
                "DTK needs more TikTok identities, but this API key cannot mint them "
                "(identity:manage or admin scope required)."
            )

        count = min(IDENTITY_MINT_TASK_CAP, max(1, int(count)))
        inventory = await self._mint_proxy_inventory()
        candidates = list(inventory["candidates"])
        configured = int(inventory["configured"])

        chosen = []
        if configured:
            if not candidates:
                now = time.monotonic()
                cooling = list(inventory["cooling"])
                retry_after = min(
                    [max(1.0, until - now) for _proxy_id, until in cooling]
                    or [float(self.settings["dtk_identity_control_poll_seconds"])]
                )
                raise DtkApiError(
                    "QUEUE_FULL",
                    "no eligible healthy unbound proxy is currently available for TikTok identity minting",
                    retry_after=retry_after,
                    details={
                        "reason": "no_eligible_proxy",
                        "configured": configured,
                        "healthy": inventory["healthy"],
                        "bound": inventory["bound"],
                        "reserved": inventory.get("reserved", 0),
                        "cooling": len(cooling),
                    },
                )
            offset = self.identity_proxy_cursor % len(candidates)
            ordered = candidates[offset:] + candidates[:offset]
            chosen = ordered[:count]
            self.identity_proxy_cursor = (offset + len(chosen)) % max(1, len(candidates))
        else:
            chosen = [None] * count

        task_ids = []
        last_submission_error = None
        for proxy in chosen:
            payload = {"platform": "tiktok", "count": 1}
            proxy_id = None
            if proxy is not None:
                proxy_id = str(proxy["id"])
                payload["proxy_id"] = proxy_id
            try:
                data, response = await self._management_data(
                    "POST",
                    "/api/v1/admin/identities/mint",
                    json=payload,
                )
                ids = [
                    str(task_id)
                    for task_id in ((data or {}).get("task_ids") or [])
                    if isinstance(task_id, (str, int)) and str(task_id)
                ] if isinstance(data, dict) else []
                if not ids:
                    raise DtkApiError(
                        "INVALID_RESPONSE",
                        "identity mint was accepted without any task ids",
                        status=response.status_code,
                        details={"proxy_id": proxy_id},
                    )
            except DtkApiError as exc:
                last_submission_error = exc
                await self._record_dtk_error(
                    exc,
                    "identity mint submission"
                    + (f" via proxy {self._proxy_debug_name(proxy_id)}" if proxy_id else " via direct egress"),
                )
                # A 429/queue lock is not evidence the egress is bad. Other
                # submission failures tied to a named proxy are quarantined so
                # the next attempt rotates away instead of fixating on it.
                if proxy_id and exc.dtk_code not in {"RATE_LIMITED", "QUEUE_FULL"}:
                    self._cooldown_mint_proxy(proxy_id, f"submission {exc.dtk_code}")
                # Shared Retry-After is already recorded by _management_data.
                # Once it trips, stop submitting more tasks this cycle.
                if exc.dtk_code == "RATE_LIMITED":
                    break
                continue

            for task_id in ids:
                self.identity_task_states[task_id] = "submitted"
                self.identity_task_proxies[task_id] = proxy_id
                task_ids.append(task_id)
                if proxy_id:
                    s.console(
                        f"[DTK DEBUG] Queued TikTok identity mint task {task_id[:8]} through "
                        f"proxy {self._proxy_debug_name(proxy_id)} ({reason})."
                    )
                else:
                    s.console(
                        f"[DTK DEBUG] Queued TikTok identity mint task {task_id[:8]} through "
                        f"direct egress ({reason})."
                    )

        if not task_ids and last_submission_error is not None:
            raise last_submission_error
        if not task_ids:
            raise DtkApiError(
                "QUEUE_FULL",
                "no TikTok identity mint task could be submitted",
                retry_after=self.settings["dtk_identity_control_poll_seconds"],
            )

        self.identity_repair_last_mint = time.monotonic()
        s.console(
            f"[DTK DEBUG] Tracking {len(task_ids)} new TikTok identity mint task"
            f"{'' if len(task_ids) == 1 else 's'} until success or failure."
        )
        return task_ids

    def _task_error(self, task_id, task):
        error = task.get("error") if isinstance(task, dict) and isinstance(task.get("error"), dict) else {}
        return DtkApiError(
            str(error.get("code") or "INTERNAL"),
            str(error.get("message") or "identity mint task failed"),
            retry_after=error.get("retry_after"),
            request_id=f"task:{task_id}",
            details=error.get("details"),
        )

    def _mint_failure_action(self, exc):
        details = exc.details if isinstance(getattr(exc, "details", None), dict) else {}
        reason = str(details.get("reason") or "")
        if exc.dtk_code == "NOT_CONFIGURED" or reason in IDENTITY_FATAL_REASONS:
            if reason == "no_free_proxy":
                return (
                    "DTK cannot create another TikTok identity because proxies are configured "
                    "but no unused proxy is available. DTK intentionally binds at most one live "
                    "identity to an automatically selected proxy. Add another usable proxy, retire "
                    "an old identity, or lower the scanner minimum."
                )
            if reason in {"browser_rpc_unconfigured"} or exc.dtk_code == "NOT_CONFIGURED":
                return "DTK browser-rpc is not configured, so automatic identity minting cannot work."
            if reason == "proxy_undecryptable":
                return "DTK cannot decrypt the selected proxy credential, so identity minting cannot continue safely."
            if reason == "proxy_not_found":
                return "DTK's identity mint task references a proxy that no longer exists."
            return f"DTK identity minting cannot continue: {reason or exc.dtk_code}."
        return None

    async def _identity_task_view(self, task_id):
        task, _response = await self._management_data("GET", f"/api/v1/tasks/{task_id}")
        if not isinstance(task, dict):
            raise DtkApiError(
                "INVALID_RESPONSE",
                f"identity mint task {task_id} returned an invalid task document",
            )
        return task

    async def _poll_identity_tasks(self, pending):
        """Poll all queued/running mint tasks once and return task outcomes."""
        failed = []
        succeeded = []
        max_retry_after = 0.0
        for task_id in list(pending):
            try:
                task = await self._identity_task_view(task_id)
            except DtkApiError as exc:
                await self._record_dtk_error(exc, f"identity mint task {task_id} poll")
                # A task that can no longer be read cannot count as an active
                # replacement forever. Remove it so the supervisor can submit
                # another one while staying within the active-task cap.
                if not exc.retryable:
                    pending.discard(task_id)
                    failed.append(exc)
                else:
                    max_retry_after = max(max_retry_after, float(exc.retry_after or 1))
                continue

            state = str(task.get("state") or "unknown")
            previous = self.identity_task_states.get(task_id)
            if state != previous:
                self.identity_task_states[task_id] = state
                s.console(f"[DTK DEBUG] Mint task {task_id[:8]}: {previous or 'submitted'} -> {state}.")

            if state == "done":
                data = task.get("data") if isinstance(task.get("data"), dict) else {}
                if data.get("minted") is True:
                    identity_id = str(data.get("identity_id") or "")
                    proxy_id = self.identity_task_proxies.get(task_id) or data.get("proxy_id")
                    if proxy_id:
                        self.identity_proxy_cooldown_until.pop(str(proxy_id), None)
                    s.console(
                        f"[DTK DEBUG] Mint task {task_id[:8]} completed successfully"
                        + (f"; identity {identity_id[:8]} created" if identity_id else "")
                        + (f" through proxy {self._proxy_debug_name(proxy_id)}." if proxy_id else " through direct egress.")
                    )
                    pending.discard(task_id)
                    succeeded.append(task_id)
                else:
                    exc = DtkApiError(
                        "INVALID_RESPONSE",
                        "identity mint task finished without minted=true",
                        request_id=f"task:{task_id}",
                        details={"task_data": data},
                    )
                    await self._record_dtk_error(exc, f"identity mint task {task_id}")
                    proxy_id = self.identity_task_proxies.get(task_id)
                    if proxy_id:
                        self._cooldown_mint_proxy(proxy_id, "invalid completed mint result")
                    pending.discard(task_id)
                    failed.append(exc)
            elif state == "failed":
                exc = self._task_error(task_id, task)
                await self._record_dtk_error(exc, f"identity mint task {task_id}")
                proxy_id = self.identity_task_proxies.get(task_id)
                # RATE_LIMITED/busy is a control-plane/lock condition, not proof
                # that the egress itself is bad. INTERNAL and similar mint
                # failures are exactly what the user's logs showed for the
                # repeated 502/msToken-zero proxy, so quarantine that egress.
                if proxy_id and exc.dtk_code not in {"RATE_LIMITED", "QUEUE_FULL"}:
                    self._cooldown_mint_proxy(proxy_id, exc.dtk_code)
                pending.discard(task_id)
                failed.append(exc)
                max_retry_after = max(max_retry_after, float(exc.retry_after or 0))
            elif state not in {"queued", "running"}:
                exc = DtkApiError(
                    "INVALID_RESPONSE",
                    f"identity mint task entered unknown state {state!r}",
                    request_id=f"task:{task_id}",
                )
                await self._record_dtk_error(exc, f"identity mint task {task_id}")
                proxy_id = self.identity_task_proxies.get(task_id)
                if proxy_id:
                    self._cooldown_mint_proxy(proxy_id, f"unknown task state {state}")
                pending.discard(task_id)
                failed.append(exc)

        # Keep only live task mappings. Successful identities are already bound
        # inside DTK; failed proxy ids live separately in the cooldown table.
        live = set(pending)
        for task_id in list(self.identity_task_proxies):
            if task_id not in live:
                self.identity_task_proxies.pop(task_id, None)
        return succeeded, failed, max_retry_after

    async def _emit_identity_activity(self, pool):
        """Surface DTK's official mint activity feed without repeating old rows."""
        activity = pool.get("activity") if isinstance(pool, dict) and isinstance(pool.get("activity"), dict) else {}
        current = activity.get("current") if isinstance(activity.get("current"), dict) else None
        current_sig = json.dumps(current, sort_keys=True, default=str) if current else None
        if current_sig != self.identity_activity_current:
            self.identity_activity_current = current_sig
            if current:
                s.console(
                    f"[DTK DEBUG] DTK browser mint in progress: "
                    f"platform={current.get('platform')} started_at={current.get('started_at')}."
                )
            elif current_sig is None:
                s.console("[DTK DEBUG] DTK reports no browser mint currently in flight.")

        recent = activity.get("recent") if isinstance(activity.get("recent"), list) else []
        for entry in reversed(recent):
            if not isinstance(entry, dict) or entry.get("platform") != "tiktok":
                continue
            signature = json.dumps(entry, sort_keys=True, default=str)
            if signature in self.identity_activity_seen:
                continue
            self.identity_activity_seen.add(signature)
            if len(self.identity_activity_seen) > 200:
                # Bounded dedupe only; the API itself keeps at most a few rows.
                self.identity_activity_seen = set(list(self.identity_activity_seen)[-100:])
            if entry.get("ok") is True:
                identity_id = str(entry.get("identity_id") or "")
                s.console(
                    f"[DTK DEBUG] DTK mint activity succeeded: reason={entry.get('reason')}"
                    + (f" identity={identity_id[:8]}." if identity_id else ".")
                )
            else:
                message = (
                    f"DTK mint activity failed: reason={entry.get('reason') or 'unknown'}"
                    + (f"; {entry.get('error')}" if entry.get("error") else "")
                )
                synthetic = DtkApiError(
                    "INTERNAL",
                    message,
                    request_id="mint-activity",
                    details={
                        "reason": entry.get("reason"),
                        "platform": entry.get("platform"),
                        "activity_timestamp": entry.get("ts"),
                    },
                )
                await self._record_dtk_error(synthetic, "DTK mint activity")

        backoff = activity.get("backoff") if isinstance(activity.get("backoff"), dict) else None
        backoff_sig = json.dumps(backoff, sort_keys=True, default=str) if backoff else None
        if backoff_sig != self.identity_activity_backoff:
            self.identity_activity_backoff = backoff_sig
            if backoff:
                s.console(
                    f"[DTK DEBUG] DTK automatic refill backoff: failures={backoff.get('failures')} "
                    f"until={backoff.get('until')}."
                )

    async def _wait_for_minimum_identities(self, minimum, *, initial_usable=0, reason="identity minimum"):
        """Mint, observe every task, replace failures, and never scan below minimum."""
        target = self.settings["dtk_target_identities"]
        deadline = time.monotonic() + self.settings["dtk_identity_wait_seconds"]
        best = int(initial_usable or 0)
        pending = set()
        next_log = 0.0
        next_mint = 0.0
        failure_streak = 0
        last_row = {"usable": best}
        last_pool = {}

        while True:
            self.gate.check()
            try:
                pool, row = await self._identity_pool_row()
                last_pool, last_row = pool, row
                best = int(row.get("usable") or 0)
                await self._emit_identity_activity(pool)
            except DtkApiError as exc:
                await self._record_dtk_error(exc, f"{reason} pool check")
                if exc.kind == "access_denied":
                    raise s.ExporterError(
                        "DTK could not verify the TikTok identity pool while waiting for identities."
                    ) from None
                pool, row = last_pool, last_row

            succeeded, failed, retry_after = await self._poll_identity_tasks(pending)
            if succeeded:
                failure_streak = 0
            if failed:
                failure_streak += len(failed)
                fatal = next((self._mint_failure_action(exc) for exc in failed if self._mint_failure_action(exc)), None)
                if fatal:
                    raise s.ExporterError(fatal)
                next_mint = max(
                    next_mint,
                    time.monotonic() + max(
                        retry_after,
                        min(IDENTITY_RETRY_MAX_SECONDS, float(2 ** min(failure_streak, 5))),
                    ),
                )

            # Re-read after completed task(s) so the just-committed identity can
            # satisfy the minimum immediately instead of waiting another tick.
            if succeeded:
                try:
                    pool, row = await self._identity_pool_row()
                    last_pool, last_row = pool, row
                    best = int(row.get("usable") or 0)
                    await self._emit_identity_activity(pool)
                except DtkApiError as exc:
                    await self._record_dtk_error(exc, f"{reason} post-mint pool check")

            if best >= minimum:
                s.console(
                    f"[DTK READY] Identity pool ready before scan traffic: "
                    f"{self._identity_debug(last_row, minimum, target)}."
                )
                return last_row

            now = time.monotonic()
            if now >= deadline:
                break

            # A task in queued/running state already represents one requested
            # identity. Only submit the uncovered shortfall and never keep more
            # than DTK's documented per-call maximum active at once.
            uncovered = max(0, minimum - best - len(pending))
            capacity = max(0, IDENTITY_MINT_TASK_CAP - len(pending))
            if uncovered and capacity and now >= next_mint:
                request_count = min(uncovered, capacity)
                try:
                    task_ids = await self._request_identity_mint(
                        request_count,
                        reason=f"{reason}; need {minimum}, currently {best}",
                    )
                    pending.update(task_ids)
                    next_mint = now + 1.0
                except DtkApiError as exc:
                    await self._record_dtk_error(exc, f"{reason} mint submission")
                    fatal = self._mint_failure_action(exc)
                    if fatal:
                        raise s.ExporterError(fatal) from None
                    failure_streak += 1
                    next_mint = now + max(
                        float(exc.retry_after or 0),
                        min(IDENTITY_RETRY_MAX_SECONDS, float(2 ** min(failure_streak, 5))),
                    )

            if now >= next_log:
                counts = {}
                for task_id in pending:
                    state = self.identity_task_states.get(task_id, "submitted")
                    counts[state] = counts.get(state, 0) + 1
                task_text = ", ".join(f"{key}={value}" for key, value in sorted(counts.items())) or "none"
                remaining = max(0, int(deadline - now))
                s.console(
                    f"[DTK DEBUG] Identity supervisor: "
                    f"{self._identity_debug(last_row, minimum, target)} | "
                    f"mint_tasks={task_text} | retry_streak={failure_streak} | "
                    f"budget_remaining={remaining}s. TikTok scan requests are still blocked."
                )
                next_log = now + IDENTITY_DEBUG_SECONDS

            await asyncio.sleep(min(float(self.settings['dtk_identity_control_poll_seconds']), max(0.1, deadline - now)))

        for task_id in sorted(pending):
            state = self.identity_task_states.get(task_id, "unknown")
            s.console(f"[DTK DEBUG] Mint task {task_id[:8]} still {state} when startup budget expired.")

        error = s.ExporterError(
            f"DTK identity startup blocked: need at least {minimum} usable TikTok identities, "
            f"but only {best} became usable after {self.settings['dtk_identity_wait_seconds']} seconds. "
            "Every submitted mint task was monitored and failed/stuck task details were saved. "
            "No TikTok scan requests were started."
        )
        await self._record_dtk_error(error, f"{reason} timeout")
        raise error

    async def _mint_one_replacement(self, reason):
        """Create one confirmed replacement identity, retrying failed mint tasks."""
        deadline = time.monotonic() + min(self.settings["dtk_identity_wait_seconds"], 300)
        pending = set()
        failure_streak = 0
        next_mint = 0.0
        while time.monotonic() < deadline:
            self.gate.check()
            succeeded, failed, retry_after = await self._poll_identity_tasks(pending)
            if succeeded:
                return True
            if failed:
                failure_streak += len(failed)
                fatal = next((self._mint_failure_action(exc) for exc in failed if self._mint_failure_action(exc)), None)
                if fatal:
                    raise s.ExporterError(fatal)
                next_mint = max(
                    next_mint,
                    time.monotonic() + max(
                        retry_after,
                        min(IDENTITY_RETRY_MAX_SECONDS, float(2 ** min(failure_streak, 5))),
                    ),
                )
            now = time.monotonic()
            if not pending and now >= next_mint:
                try:
                    pending.update(await self._request_identity_mint(1, reason=reason))
                except DtkApiError as exc:
                    await self._record_dtk_error(exc, f"{reason} replacement submission")
                    fatal = self._mint_failure_action(exc)
                    if fatal:
                        raise s.ExporterError(fatal) from None
                    failure_streak += 1
                    next_mint = now + max(
                        float(exc.retry_after or 0),
                        min(IDENTITY_RETRY_MAX_SECONDS, float(2 ** min(failure_streak, 5))),
                    )
            try:
                pool, _row = await self._identity_pool_row()
                await self._emit_identity_activity(pool)
            except DtkApiError as exc:
                await self._record_dtk_error(exc, f"{reason} replacement activity")
            await asyncio.sleep(float(self.settings['dtk_identity_control_poll_seconds']))
        error = s.ExporterError(f"DTK could not confirm a replacement TikTok identity after {reason}.")
        await self._record_dtk_error(error, "identity replacement timeout")
        raise error

    async def _ensure_identity_pool(self):
        minimum = self.settings["dtk_min_usable_identities"]
        target = self.settings["dtk_target_identities"]
        s.console(
            f"[DTK DEBUG] Starting strict identity preflight: minimum={minimum}, target={target}. "
            "Normal TikTok scan requests will remain disabled until the minimum is usable."
        )
        try:
            pool, row = await self._identity_pool_row()
        except DtkApiError as exc:
            await self._record_dtk_error(exc, "identity preflight initial pool check")
            if exc.kind == "access_denied":
                raise s.ExporterError(
                    "DTK identity preflight cannot inspect the pool with this API key. "
                    "Identity management access is required when automatic identity maintenance is enabled."
                ) from None
            raise

        # This scanner is TikTok-only. Keep DTK's background filler aligned with
        # the scanner's configured pool marks and stop it wasting browser mints on Douyin.
        if self._can_manage_identities():
            try:
                platforms = pool.get("platforms") if isinstance(pool, dict) else []
                desired = (
                    ("pool.tiktok.min_size", minimum, row.get("min_size")),
                    ("pool.tiktok.target_size", target, row.get("target_size")),
                )
                for name, value, current in desired:
                    if current != value:
                        await self._admin_put(f"/api/v1/admin/settings/{name}", value)
                douyin = next((x for x in platforms or [] if x.get("platform") == "douyin"), None)
                if douyin and douyin.get("min_size") != 0:
                    try:
                        await self._admin_put("/api/v1/admin/settings/pool.douyin.min_size", 0)
                    except DtkApiError as exc:
                        await self._record_dtk_error(exc, "disable unused Douyin identity refill")
                if row.get("min_size") != minimum or row.get("target_size") != target or not row.get("auto", True):
                    pool, row = await self._identity_pool_row()
            except DtkApiError as exc:
                await self._record_dtk_error(exc, "identity pool settings alignment")
                raise

        usable = int(row.get("usable") or 0)
        await self._emit_identity_activity(pool)
        s.console(f"[DTK DEBUG] Identity pool snapshot: {self._identity_debug(row, minimum, target)}.")
        if usable >= minimum:
            s.console(
                f"[DTK READY] Existing identity pool already satisfies the minimum: "
                f"{usable}/{minimum} usable."
            )
            return

        if not self.settings.get("dtk_auto_mint", True):
            raise s.ExporterError(
                f"DTK has only {usable}/{minimum} usable TikTok identities and automatic minting is disabled. "
                "No TikTok scan requests were started."
            )
        if not self._can_manage_identities():
            raise s.ExporterError(
                f"DTK has only {usable}/{minimum} usable TikTok identities and this API key cannot create more. "
                "No TikTok scan requests were started."
            )

        # The supervisor submits the shortfall, follows each task's queued /
        # running / done / failed state, replaces failed tasks, and only returns
        # when the hard minimum is genuinely usable.
        await self._wait_for_minimum_identities(
            minimum,
            initial_usable=usable,
            reason="startup identity preflight",
        )

    async def _repair_identity_pool(self, reason):
        if not self.settings.get("dtk_auto_mint", True):
            return
        if reason not in IDENTITY_RECOVERY_CODES:
            return

        async with self.identity_repair_lock:
            minimum = self.settings["dtk_min_usable_identities"]
            target = self.settings["dtk_target_identities"]
            pool, row = await self._identity_pool_row()
            await self._emit_identity_activity(pool)
            usable = int(row.get("usable") or 0)

            now = time.monotonic()
            below_minimum = usable < minimum
            cooldown_active = (
                self.identity_repair_last_mint > 0
                and now - self.identity_repair_last_mint < IDENTITY_REPAIR_COOLDOWN_SECONDS
            )

            s.console(
                f"[DTK DEBUG] Identity self-heal triggered by {reason}: "
                f"{usable} usable, minimum {minimum}, target {target}."
            )
            if below_minimum:
                # Hard gate: stop this failing request path here and restore the
                # configured pool before request retries can resume.
                await self._wait_for_minimum_identities(
                    minimum,
                    initial_usable=usable,
                    reason=f"runtime self-heal after {reason}",
                )
                return

            if cooldown_active:
                s.console(
                    f"[DTK DEBUG] Replacement mint suppressed by the {IDENTITY_REPAIR_COOLDOWN_SECONDS:.0f}s "
                    "global cooldown; another recovery already requested one."
                )
                return

            # Even when the pool count has not fallen yet, an identity-specific
            # failure is evidence that one session may be deteriorating. Request
            # one replacement and confirm the mint task instead of fire-and-forget.
            await self._mint_one_replacement(f"runtime self-heal after {reason}")

    async def lookup_profile(self, username):
        key = username.casefold()
        if key in self.profile_cache:
            self.profile_cache.move_to_end(key)
            return self.profile_cache[key]
        try:
            data = await self._call(
                "/api/v1/tiktok/user",
                {
                    "url": f"{s.TIKTOK_ORIGIN}/@{username}",
                    "wait": self.settings["dtk_wait_seconds"],
                    # Profile privacy is only exposed by DTK in the untouched TikTok user node.\n                    # Always request it for correctness, but only persist raw data when configured.\n                    "include_raw": "true",
                },
                "profile",
            )
        except DtkApiError as exc:
            if exc.dtk_code == "CONTENT_PRIVATE":
                raise s.PublicProfileRequired(f"@{username} is private; list scanning skipped.") from None
            raise
        if self.raw_directory is not None:
            await s.disk_call(
                s.atomic_write_json,
                self.raw_directory / f"profile_{s.parse_username(username)}" / "profile.json",
                {"data": data},
                compact=True,
                redactor=s.REDACTOR,
            )
        profile = _profile_from_author(data, username)
        profile.metadata["lookup_retry_count"] = self.request_retries.get()
        if username.casefold() == self.settings.get("target_username", "").casefold():
            self.settings["resolved_target_uid"] = profile.uid
        self.profile_cache[key] = profile
        if len(self.profile_cache) > max(256, self.gate.ceiling):
            self.profile_cache.popitem(last=False)
        return profile

    async def fetch_page(self, profile, list_name, cursor, page_number):
        params = {
            "sec_user_id": profile.sec_uid,
            "count": self.settings["dtk_page_size"],
            "wait": self.settings["dtk_wait_seconds"],
            "include_raw": str(bool(s.KEEP_RAW_MEMBER_DATA)).lower(),
        }
        if cursor and cursor != "0":
            params["cursor"] = cursor
        data = await self._call(f"/api/v1/tiktok/user/{list_name}", params, list_name)
        if not isinstance(data, dict):
            raise s.InvalidBackendResponse(f"DTK {list_name} response must be an object.")
        items = data.get("items")
        if not isinstance(items, list):
            raise s.InvalidBackendResponse(f"DTK {list_name} response is missing items.")
        batch = s.BatchResponse(
            records=[_member_from_author(item) for item in items],
            has_more=bool(data.get("has_more")),
            min_cursor=str(data.get("cursor") or ""),
            retry_count=self.request_retries.get(),
        )
        if self.raw_directory is not None:
            await s.disk_call(
                s.atomic_write_json,
                self.raw_directory / f"profile_{s.parse_username(profile.username)}" / f"{list_name}_{page_number:06d}.json",
                {"data": data},
                compact=True,
                redactor=s.REDACTOR,
            )
        return batch

    async def download_avatar(self, url):
        from avatar_cache import allowed_avatar_url, MAX_DOWNLOAD_BYTES
        if not allowed_avatar_url(url) or self.avatar_client is None:
            return b""
        try:
            async with self.avatar_client.stream(
                "GET",
                url,
                headers={"Accept": "image/*", "Accept-Encoding": "identity"},
            ) as response:
                if response.status_code != 200:
                    return b""
                kind = response.headers.get("Content-Type", "").split(";", 1)[0].strip().casefold()
                if kind not in {"image/jpeg", "image/jpg", "image/png", "image/webp", "image/gif"}:
                    return b""
                body = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=16384):
                    if len(body) + len(chunk) > MAX_DOWNLOAD_BYTES:
                        return b""
                    body.extend(chunk)
                return bytes(body)
        except (self.httpx.RequestError, self.httpx.TimeoutException):
            return b""

    def start_avatar_backfill(self, root):
        if self.avatars is None or self.gate.stop_event.is_set():
            return
        self.avatar_backfill_requested = True
        if self.avatar_backfill_task is not None and not self.avatar_backfill_task.done():
            return
        async def fill():
            while self.avatar_backfill_requested and not self.gate.stop_event.is_set():
                self.avatar_backfill_requested = False
                await self.cache_saved_avatars(root)
        self.avatar_backfill_task = asyncio.create_task(fill(), name="report-avatar-backfill")

    async def cache_saved_avatars(self, root):
        from report_generator import iter_avatar_profiles
        pending = iter(iter_avatar_profiles(root))
        lock = asyncio.Lock()
        counts = {"checked": 0, "available": 0, "unavailable": 0}
        capacity = min(self.settings.get("avatar_workers", 0) or self.gate.limit, self.gate.limit)
        consumers = []
        def take(limit):
            return list(itertools.islice((r for r in pending if r[2]), limit))
        try:
            buffered = deque(await s.disk_call(take, capacity))
            async def consume():
                while not self.gate.stop_event.is_set():
                    async with lock:
                        batch = [buffered.popleft()] if buffered else await s.disk_call(take, 16)
                    if not batch:
                        return
                    for uid, name, url in batch:
                        if self.gate.stop_event.is_set():
                            return
                        value = await self.avatars.ensure(uid, name, url)
                        counts["checked"] += 1
                        counts["available" if value else "unavailable"] += 1
            consumers = [asyncio.create_task(consume(), name="report-avatar-consumer") for _ in range(len(buffered))]
            outcomes = await asyncio.gather(*consumers, return_exceptions=True)
            if any(isinstance(v, Exception) for v in outcomes):
                s.console("[AVATAR] Some saved image references were unavailable; scan data preserved.")
            return counts
        finally:
            for task in consumers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*consumers, return_exceptions=True)
            await s.disk_call(pending.close)


def create_client(gate, pool, **kwargs):
    settings = migrate_config(kwargs.pop("settings", None) or {})
    return DtkClient(gate, pool, settings=settings, **kwargs)


def edit_backend_config(config):
    updated = migrate_config(json.loads(json.dumps(config)))
    s.console(
        "\nDTK backend (only backend in this build):\n"
        f"URL: {updated['dtk_base_url']}\n"
        f"Auto-start Docker/DTK: {updated['dtk_auto_start']}\n"
        f"Auto-mint identities: {updated['dtk_auto_mint']}\n"
        f"Identity minimum/target: {updated['dtk_min_usable_identities']}/{updated['dtk_target_identities']}\n"
        f"Identity create/recovery wait: {updated['dtk_identity_wait_seconds']}s\n"
        f"Failed proxy mint cooldown: {updated['dtk_proxy_mint_cooldown_seconds']}s\n"
        f"Identity control polling: {updated['dtk_identity_control_poll_seconds']}s\n"
        f"Page size: {updated['dtk_page_size']}\n"
        f"Max local DTK connections: {updated['dtk_max_connections']}"
    )
    updated["dtk_auto_start"] = s.ask_bool("Auto-start Docker Desktop and DTK when needed? [Y/N, Enter = keep]: ", updated["dtk_auto_start"])
    updated["dtk_auto_mint"] = s.ask_bool("Automatically maintain TikTok identities? [Y/N, Enter = keep]: ", updated["dtk_auto_mint"])
    updated["dtk_min_usable_identities"] = s.ask_number(
        f"Minimum usable TikTok identities [Enter = keep {updated['dtk_min_usable_identities']}]: ",
        updated["dtk_min_usable_identities"], integer=True, minimum=1, maximum=200)
    updated["dtk_target_identities"] = s.ask_number(
        f"DTK pool target [Enter = keep {max(updated['dtk_target_identities'], updated['dtk_min_usable_identities'])}]: ",
        max(updated["dtk_target_identities"], updated["dtk_min_usable_identities"]), integer=True,
        minimum=updated["dtk_min_usable_identities"], maximum=200)
    updated["dtk_identity_wait_seconds"] = s.ask_number(
        f"Identity create/recovery wait seconds [Enter = keep {updated['dtk_identity_wait_seconds']}]: ",
        updated["dtk_identity_wait_seconds"], integer=True, minimum=60, maximum=3600)
    updated["dtk_proxy_mint_cooldown_seconds"] = s.ask_number(
        f"Failed proxy mint cooldown seconds [Enter = keep {updated['dtk_proxy_mint_cooldown_seconds']}]: ",
        updated["dtk_proxy_mint_cooldown_seconds"], integer=True, minimum=30, maximum=86400)
    updated["dtk_identity_control_poll_seconds"] = s.ask_number(
        f"Identity control polling seconds [Enter = keep {updated['dtk_identity_control_poll_seconds']}]: ",
        updated["dtk_identity_control_poll_seconds"], integer=True, minimum=1, maximum=60)
    updated["dtk_page_size"] = s.ask_number(
        f"DTK list page size (1-50) [Enter = keep {updated['dtk_page_size']}]: ",
        updated["dtk_page_size"], integer=True, minimum=1, maximum=50)
    updated["dtk_max_connections"] = s.ask_number(
        f"DTK local API connection ceiling [Enter = keep {updated['dtk_max_connections']}]: ",
        updated["dtk_max_connections"], integer=True, minimum=1, maximum=512)
    validate(updated)
    return updated
