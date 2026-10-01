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
CLOAKBROWSER_COMMIT = "f04c23da285b3b3d3cf10c8f9d282e7adc1d52ce"

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
    "dtk_identity_wait_seconds": 120,
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


class DtkApiError(s.WorkerApiError):
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


def _compose_up(repo):
    environment = os.environ.copy()
    environment["CLOAKBROWSER_COMMIT"] = CLOAKBROWSER_COMMIT
    base = ["docker", "compose", "-p", "dtk", "-f", "docker/compose.yml", "--profile", "browser", "up", "-d"]
    result = _run(base, cwd=repo, env=environment, timeout=180)
    if result.returncode == 0:
        return
    s.console("[DTK] Existing images were not enough; building the DTK/browser images once ...")
    result = _run(base + ["--build"], cwd=repo, env=environment, timeout=900)
    if result.returncode != 0:
        tail = "\n".join(result.stdout.splitlines()[-12:])
        raise s.ExporterError("DTK Docker stack failed to start.\n" + tail)


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
    if await _health(base):
        return None
    if not settings.get("dtk_auto_start", True):
        raise s.ExporterError(f"DTK is not reachable at {base}; auto-start is disabled.")
    host = (urlparse(base).hostname or "").casefold()
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise s.ExporterError("Auto-start is only supported for a local DTK URL.")
    s.console("[DTK] Local API is offline; starting Docker Desktop and the DTK stack automatically ...")
    await asyncio.to_thread(_start_docker_desktop)
    repo = await asyncio.to_thread(_ensure_repo, settings)
    await asyncio.to_thread(_ensure_dtk_env, repo)
    await asyncio.to_thread(_compose_up, repo)
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
        raise s.InvalidWorkerResponse("DTK profile data must be an object.")
    unique = author.get("unique_id")
    if not isinstance(unique, str) or not unique:
        unique = username
    if unique.casefold() != username.casefold():
        raise s.InvalidWorkerResponse("DTK returned a different username than requested.")
    uid = author.get("uid")
    if isinstance(uid, bool) or not isinstance(uid, (str, int)):
        raise s.InvalidWorkerResponse("DTK profile uid is missing.")
    uid = str(uid)
    stats = author.get("stats") if isinstance(author.get("stats"), dict) else {}
    raw = author.get("raw") if isinstance(author.get("raw"), dict) else {}
    privacy = raw.get("privateAccount")
    if privacy is True:
        raise s.PublicProfileRequired(f"@{unique} is private; list scanning skipped.")
    return s.ProfileSnapshot(
        username=unique,
        uid=uid,
        display_name=author.get("nickname") if isinstance(author.get("nickname"), str) else unique,
        sec_uid=author.get("sec_uid") if isinstance(author.get("sec_uid"), str) else "",
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
        await self._validate_key()
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

    async def _validate_key(self):
        response = await self.client.get("/api/v1/auth/me")
        data = self._unwrap(response)
        scopes = set(data.get("scopes") or [])
        if "tiktok:read" not in scopes and "admin" not in scopes:
            raise s.ExporterError("DTK API key lacks the tiktok:read scope.")
        s.console(f"[DTK] Authenticated local API key; effective rate limit: {data.get('rate_limit_per_min')} requests/minute.")

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
                if exc.kind == "access_denied":
                    self.gate.blocked_reason = "DTK API authentication/scope failure; fix the local API key."
                if not exc.retryable or attempt + 1 >= attempts:
                    raise
                delay = float(exc.retry_after or min(30, 2 ** attempt))
                s.console(f"[DTK RETRY {attempt + 1}/{attempts - 1}] {exc.dtk_code}; waiting {delay:.1f}s.")
                await self.gate.wait(delay)
            except (self.httpx.ConnectError, self.httpx.ConnectTimeout) as exc:
                failure = s.WorkerApiError("DTK local API connection failed.", kind="network_error", retryable=True)
                last = failure
                if attempt + 1 >= attempts:
                    raise failure from None
                await self.gate.wait(min(10, 2 ** attempt))
            except (self.httpx.TimeoutException, TimeoutError):
                failure = s.WorkerApiError("DTK local API timed out.", kind="response_timeout", retryable=True)
                last = failure
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

    async def _admin_get(self, path):
        response = await self.client.get(path)
        return self._unwrap(response)

    async def _admin_put(self, path, value):
        response = await self.client.put(path, json={"value": value})
        return self._unwrap(response)

    async def _ensure_identity_pool(self):
        if not self.settings.get("dtk_auto_mint", True):
            return
        try:
            pool = await self._admin_get("/api/v1/admin/identities/pool")
        except DtkApiError as exc:
            if exc.kind == "access_denied":
                s.console("[DTK] API key cannot inspect/manage identities; relying on DTK's existing automatic pool.")
                return
            raise
        platforms = pool.get("platforms") if isinstance(pool, dict) else None
        row = next((x for x in platforms or [] if x.get("platform") == "tiktok"), None)
        if not row:
            raise s.ExporterError("DTK identity pool did not report TikTok.")
        minimum = self.settings["dtk_min_usable_identities"]
        target = self.settings["dtk_target_identities"]
        if not row.get("auto", True):
            s.console("[DTK] TikTok auto-refill was disabled; enabling it for this scanner.")
            await self._admin_put("/api/v1/admin/settings/pool.tiktok.min_size", minimum)
            await self._admin_put("/api/v1/admin/settings/pool.tiktok.target_size", target)
        usable = int(row.get("usable") or 0)
        s.console(f"[DTK] TikTok identity pool: {usable} usable; scanner minimum {minimum}; DTK target {target}.")
        if usable >= minimum:
            return
        shortfall = min(10, minimum - usable)
        try:
            response = await self.client.post(
                "/api/v1/admin/identities/mint",
                json={"platform": "tiktok", "count": shortfall},
            )
            data = self._unwrap(response)
            task_ids = list(data.get("task_ids") or []) if isinstance(data, dict) else []
            s.console(f"[DTK] Minting {len(task_ids)} TikTok guest identit{'y' if len(task_ids)==1 else 'ies'} automatically ...")
            if task_ids:
                await asyncio.gather(
                    *(self._poll_task(task_id, deadline_seconds=210) for task_id in task_ids),
                    return_exceptions=True,
                )
        except DtkApiError as exc:
            s.console(f"[DTK] Immediate mint request did not complete ({exc.dtk_code}); DTK background refill remains enabled.")
        deadline = time.monotonic() + self.settings["dtk_identity_wait_seconds"]
        best = usable
        while time.monotonic() < deadline:
            try:
                pool = await self._admin_get("/api/v1/admin/identities/pool")
                platforms = pool.get("platforms") if isinstance(pool, dict) else []
                row = next((x for x in platforms or [] if x.get("platform") == "tiktok"), None)
                best = int((row or {}).get("usable") or 0)
                if best >= minimum:
                    s.console(f"[DTK] Identity pool ready: {best} usable TikTok identities.")
                    return
            except DtkApiError:
                pass
            if best > 0 and time.monotonic() + 10 >= deadline:
                s.console(f"[DTK] Proceeding with {best} usable identity; DTK will continue refilling in the background.")
                return
            await asyncio.sleep(5)
        if best <= 0:
            raise s.ExporterError("DTK has no usable TikTok identity after automatic mint/refill attempts.")

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
                    "include_raw": str(bool(s.KEEP_RAW_MEMBER_DATA)).lower(),
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
            raise s.InvalidWorkerResponse(f"DTK {list_name} response must be an object.")
        items = data.get("items")
        if not isinstance(items, list):
            raise s.InvalidWorkerResponse(f"DTK {list_name} response is missing items.")
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
    updated["dtk_page_size"] = s.ask_number(
        f"DTK list page size (1-50) [Enter = keep {updated['dtk_page_size']}]: ",
        updated["dtk_page_size"], integer=True, minimum=1, maximum=50)
    updated["dtk_max_connections"] = s.ask_number(
        f"DTK local API connection ceiling [Enter = keep {updated['dtk_max_connections']}]: ",
        updated["dtk_max_connections"], integer=True, minimum=1, maximum=512)
    validate(updated)
    return updated
