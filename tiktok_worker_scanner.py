r"""TikTok relationship scanner: async Worker/Direct/Hybrid + Webshare proxies.

Python 3.11+ setup (Windows CMD):
    py -3.11 -m pip install -r requirements.txt
    py -3.11 tiktok_worker_scanner.py

Startup first lists unfinished search folders. Select one to load its own
scan_config.json, inspect saved settings and either resume unchanged or edit
safe settings. A new scan asks for source, target, size limits, normal/double
phase mode, workers (1-10000), request delays and proxy selection, then confirms.
Saved configuration, not current Python/environment defaults, controls resume.

Default paths:
    Input: C:\Users\vailo\Downloads\kt_expose_public_profiles_cleaned.json
    Proxies: C:\Users\vailo\Downloads\webshare_proxy.txt
    Searches: C:\Users\vailo\Downloads\TiktokSearch_Logs
New folders use the target username, then _2, _3, etc. Atomic mkdir and a
per-search process lock protect concurrent searches. Resume never creates a
new folder or mixes another search's results. Older folders inside the search
root can be resumed after a one-time explicit review of missing configuration.

Normal scans inspect only the starting dataset. Double-phase scans inspect
that dataset, then exactly ONE level of newly discovered public accounts.
Phase 2 never schedules a Phase 3. Numeric user IDs deduplicate jobs whenever
available; usernames are the fallback. All queues and phase transitions are
durable SQLite transactions. Both phases share the same clients and pacer.

A username bootstrap follows the exact saved cursors and saves a deduplicated,
public-only starting_dataset.json. Only members explicitly marked public are
retained. The member API does not supply follower/following counts: those fields
are null until a profile lookup supplies them; they are never fabricated.
Size limits (0 = unlimited) run after profile lookup and before list requests.
Formatted K/M/B counts use labelled estimates; unknown counts with an enabled
limit cause skipped_unknown_size. Exact values equal to a limit are allowed.

Each search contains:
    scan_config.json                      small authoritative settings
    starting_dataset.json                 self-contained input snapshot
    session.json, scan_state.json         identity/progress mirrors
    state.sqlite3                         durable jobs, discoveries, phases
    sucess_find.json                      immediately saved target matches
    results.json, failures.json, skipped_accounts.json
    phase2_queue.json, discovered_profiles.json
    run.log, errors.log                   redacted append-only logs
    *_followers_and_following.json        per-profile exports
    pages/, bootstrap/, raw_api/, skipped/, tmp/
    report_assets/avatars/                validated offline avatar thumbnails
SQLite WAL + FULL synchronization commits each page with its next cursor.
Retain -wal/-shm files while running. Ctrl+C/SIGTERM stop cleanly; forced kills
recover committed results and interrupted claims on the next execution.
Completed, private, HTTP-error and size-skipped attempts are terminal. On resume,
the explicit Retry failed profiles choice can requeue network errors, timeouts
and partial profiles without discarding saved pages. Interrupted/pending work
always resumes. A finished
scan means all selected-phase jobs were processed; individual exports still
report their own completeness. Cursor failures never trigger invented cursors.

TIKTOK_INPUT_JSON / TIKTOK_EXPORT_DIR / TIKTOK_PROXY_FILE customize new-scan
path suggestions. TIKTOK_PROXY_ONLY and TIKTOK_KEEP_RAW provide initial network
option defaults. Workers and delays are selected in the console and saved.
Legacy worker/delay environment variables do not override saved configuration.
Changing limits, workers or delays affects future work only. Target/source are
fixed for an existing search; changing either requires a new search.

Global pacing is between request starts; 0-0 means no intentional spacing.
In-flight requests use a connection budget derived from workers unless explicitly
configured, with measured latency/error adaptation. Workers specify concurrency,
not RPS. In hybrid mode a Worker HTTP 429 disables Worker for this execution and
migrates compatible work to Direct. Direct gets one central cooldown and gradual
recovery opportunity; another 429 stops safely. Worker-only mode retains its
immediate global 429 stop. Access denial stops the affected backend. Proxy rotation is
only for genuine connection failures, never to bypass access/rate controls.
Proxy credentials remain in the external file, never in scan_config or logs.
Direct mode uses signed TikTok API requests with isolated optional session cookies.
API-supplied avatar CDN URLs may be downloaded into this run's local cache.

HTTP(S) targets normally use an http:// Webshare proxy with CONNECT. Optional
https:// entries require a proxy that itself supports TLS; a TLS target does
not imply a TLS proxy. SOCKS5 entries require the httpx[socks] extra.

An optional pre-start check verifies every loaded proxy using Webshare's HTTPS
IP diagnostic. Skipping it retains lazy health checks on actual requests. Read timeouts cannot identify whether the proxy or Worker stalled, so
these retain the same route. Explicit connect/auth failures affect pool health.
Validation shows only proxy host/port; runtime uses opaque labels. Credentials
are never logged. Offline HTML reports are built from saved files on completion
and graceful shutdown, without any network requests or JSON download controls.
Copy report_assets alongside the HTML when moving a report. Missing images use
initials. Image downloads share the scanner pacer and proxy pool; they never
trigger Worker access/rate-limit bypasses or automatic alternate API endpoints.

Natural endpoint exhaustion is usable even if advertised counts disagree.
Explicitly hidden lists stay unavailable, with unknown relationship directions.
Such profiles are terminal when the other lists finish. Username bootstrap may
use available completed lists alongside explicit restrictions. HTTP 404 retries
the exact request once; backend-specific 429/access-denial admission is enforced.
Progress counts all terminal outcomes and separately reports private skips.
"""

from __future__ import annotations

import asyncio
import base64
import ipaddress
import itertools
import signal
import shutil
import uuid
import logging
import queue as thread_queue
import json
import math
import os
import random
import re
import sqlite3
import sys
import tempfile
import threading
import time
from collections import Counter, OrderedDict, deque
from scan_runtime import DiskLane, RetryQueue, runtime_ceiling
from scan_statistics import ScanStatistics
sys.dont_write_bytecode = True  # Runtime cache files must not leak outside a search.
from contextlib import asynccontextmanager, nullcontext, ExitStack
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator
from urllib.parse import quote, unquote, urlparse, urlsplit

APP_TITLE = "TikTok Multi-Profile Target Relationship Scanner"
# Used ONLY for textual output URLs and input URL normalization.
TIKTOK_ORIGIN = "https://www.tiktok.com"
WORKER_ORIGIN = "https://tiktokinfov10.itfreeforever.workers.dev"
FETCH_ATTEMPTS = 4
USERNAME_RE = re.compile(r"^[A-Za-z0-9._]{1,30}$")
# User-requested defaults. Environment variables can override every path and
# pacing/concurrency value without editing this file.
DEFAULT_INPUT_JSON_PATH = Path(
    r"C:\Users\vailo\Downloads\kt_expose_public_profiles_cleaned.json"
)
WEBSHARE_PROXY_FILE = Path(r"C:\Users\vailo\Downloads\webshare_proxy.txt")
TARGET_USER = "amlie7951"
SELECTED_LISTS = ("followers", "following")
DEFAULT_LOG_ROOT = Path(r"C:\Users\vailo\Downloads\TiktokSearch_Logs")
SUCCESS_FILE_NAME = "sucess_find.json"
STATE_FILE_NAME = "scan_state.json"

# One async task per profile worker; all requests share a global start-time pacer.
DEFAULT_WORKERS = 4
MAX_WORKERS = 10000
_DISK_LANE: ContextVar[Any] = ContextVar('scan_disk_lane', default=None)
_STATS: ContextVar[Any] = ContextVar('scan_statistics', default=None)
_LIVE_LOGGING = False
_LAST_CONSOLE = 0.0
KEEP_RAW_MEMBER_DATA = os.getenv("TIKTOK_KEEP_RAW", "").strip().casefold() in {"1", "true", "yes"}
PRINT_LOCK = threading.Lock()
_RUN_LOG: RunLog | None = None
_STARTUP_LOG: list[str] | None = None
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/152.0.0.0 Safari/537.36"
)


class ExporterError(RuntimeError):
    """Expected failure that should be displayed without a traceback."""


class MissingDependency(ExporterError):
    """A required Python package is unavailable."""


class PublicProfileRequired(ExporterError):
    """The target profile is private."""


@dataclass
class ProfileSnapshot:
    username: str
    uid: str
    display_name: str
    sec_uid: str
    profile_url: str
    private_account: bool
    verified: bool | None
    avatar_url: str
    follower_count: int | None
    following_count: int | None
    likes_count: int | None
    video_count: int | None
    advertised_counts: dict[str, Any]
    following_visible: bool | None
    metadata: dict[str, Any]
    raw_user: dict[str, Any]


@dataclass
class ListExportResult:
    list_name: str
    profile_count_at_start: int | None
    batches: int = 0
    raw_records_received: int = 0
    unique_records_saved: int = 0
    duplicates_ignored: int = 0
    invalid_records_ignored: int = 0
    endpoint_exhausted: bool = False
    complete: bool = False
    stop_reason: str = "not_started"
    error: str | None = None
    final_min_cursor: str = "0"
    warnings: list[str] = field(default_factory=list)

    http_code: int | None = None
    retry_count: int = 0
    visibility_reason: str | None = None
    see_following: str | None = None
    updated_at_utc: str | None = None
    advertised_count: Any = None
    returned_unique_count: int = 0
    count_difference: int | None = None
    usable: bool = False
    status: str = "not_started"
    backend: str = ""
    backend_failure: str | None = None
    cursor_chain_restarts: int = 0

    def finish(self) -> None:
        # Natural exhaustion establishes usable completion. Advertised counts
        # are snapshots, sometimes rounded; they are not a pagination protocol.
        self.updated_at_utc = utc_iso()
        self.returned_unique_count = self.unique_records_saved
        self.complete = self.endpoint_exhausted and not self.error and not self.invalid_records_ignored
        if self.endpoint_exhausted and self.invalid_records_ignored:
            self.stop_reason = "invalid_records"
            warning = "Some returned account records could not be normalized."
            if warning not in self.warnings: self.warnings.append(warning)
        if self.complete:
            count = self.profile_count_at_start
            self.count_difference = self.unique_records_saved - count if count is not None else None
            self.stop_reason = "complete_exact"
            if self.count_difference not in (None, 0):
                self.stop_reason = "complete_count_mismatch"
                warning = "Returned count differs from the advertised snapshot; endpoint exhausted normally."
                if warning not in self.warnings: self.warnings.append(warning)
        self.usable = self.complete
        self.status = self.stop_reason


@dataclass
class BatchResponse:
    records: list[Any]
    has_more: bool
    min_cursor: str
    retry_count: int = 0


@dataclass(frozen=True)
class ProfileJob:
    username: str
    profile_url: str
    source_lists: tuple[str, ...]
    uid: str = ""
    phase: int = 1


@dataclass
class ProfileProcessResult:
    username: str
    profile_url: str
    source_lists: list[str]
    status: str
    export_path: str | None
    complete: bool
    target_found_in: list[str]
    started_at_utc: str
    completed_at_utc: str
    list_results: dict[str, dict[str, Any]]
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def validate_worker_origin(value: Any) -> str:
    """One explicit Worker origin, never automatic failover or direct TikTok."""
    try:
        url = urlsplit(value)
        if (not isinstance(value, str) or any(c.isspace() for c in value) or "\\" in value
                or url.scheme != "https" or not (url.hostname or "").endswith(".workers.dev")
                or not re.fullmatch(r"[a-zA-Z0-9.-]+", url.hostname or "")
                or url.username is not None or url.password is not None or url.port not in (None, 443)
                or url.path not in ("", "/") or url.query or url.fragment):
            raise ValueError
        return "https://" + url.hostname.casefold()
    except (ValueError, TypeError, AttributeError):
        raise ExporterError("Worker origin must be a credential-free HTTPS *.workers.dev origin, without a path or query.") from None


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


_CONSOLE_BATCH: list[str] = []
_CONSOLE_BATCH_TOTAL = 0


def console(message: str = "", *, error: bool = False) -> None:
    """Keep full logs; batch repetitive terminal detail without hiding errors."""
    global _LAST_CONSOLE, _CONSOLE_BATCH_TOTAL
    if _LIVE_LOGGING and _RUN_LOG is not None and threading.current_thread() is not _RUN_LOG.thread:
        _RUN_LOG.queue.put(('console', message, error, False))
        return
    with PRINT_LOCK:
        safe_message = REDACTOR.text(message)
        important = error or any(tag in safe_message for tag in (
            '[LIVE]', 'SCAN STATS', '[TARGET FOUND]', 'RATE LIMIT', '[SHUTDOWN]',
            '[COMPLETE]', '[REPORT', '[RETRY', '[ERROR', '[PARTIAL]', '[CHECKPOINT]', '[HTTP_ERROR]', '[RESUME]'))
        important = important or ('PHASE ' in safe_message and ('Starting' in safe_message or ' COMPLETE' in safe_message)) or safe_message.startswith(('[ASYNC] Configured', '[BOOTSTRAP COMPLETE]'))
        now = time.monotonic()
        if _LIVE_LOGGING and not important:
            _CONSOLE_BATCH_TOTAL += 1
            _CONSOLE_BATCH.append(safe_message)
            if len(_CONSOLE_BATCH) > 20:
                del _CONSOLE_BATCH[0]
        if not _LIVE_LOGGING or important or now - _LAST_CONSOLE >= 1:
            lines = []
            omitted = _CONSOLE_BATCH_TOTAL - len(_CONSOLE_BATCH)
            if omitted:
                lines.append(f'[PROGRESS] {omitted:,} additional progress messages saved in run.log; latest detail:')
            lines.extend(_CONSOLE_BATCH)
            _CONSOLE_BATCH.clear(); _CONSOLE_BATCH_TOTAL = 0
            if not _LIVE_LOGGING or important:
                lines.append(safe_message)
            if lines:
                print('\n'.join(lines), flush=not _LIVE_LOGGING or important)
            _LAST_CONSOLE = now
        if _RUN_LOG is not None:
            _RUN_LOG.write(safe_message, error=error, sanitized=True)
        elif _STARTUP_LOG is not None:
            _STARTUP_LOG.append(safe_message)


def fsync_directory(directory: Path) -> None:
    # Persist rename metadata on POSIX; Windows does not expose directory fsync.
    if os.name != "nt":
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def atomic_write_json(path: Path, payload: Any, *, compact: bool = False, redactor=None) -> None:
    """Write valid JSON atomically so interruption never leaves half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(
        f"{path.name}.{os.getpid()}.{threading.get_ident()}.part"
    )
    try:
        with temporary_path.open("w", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps((redactor or REDACTOR).clean(payload), ensure_ascii=False,
                                    indent=None if compact else 2,
                                    separators=(",", ":") if compact else None))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
        fsync_directory(path.parent)
    except BaseException:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def first_value(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def parse_username(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise ExporterError("A TikTok username or public profile URL is required.")

    candidate = raw
    if "tiktok.com" in raw.casefold():
        url_text = raw if "://" in raw else f"https://{raw}"
        parsed = urlparse(url_text)
        hostname = (parsed.hostname or "").casefold()
        if not (hostname == "tiktok.com" or hostname.endswith(".tiktok.com")):
            raise ExporterError("The profile URL must be on tiktok.com.")
        parts = [unquote(part) for part in parsed.path.split("/") if part]
        candidate = next((part[1:] for part in parts if part.startswith("@")), "")
        if not candidate:
            raise ExporterError("That URL does not contain a TikTok @username.")
    else:
        candidate = raw.lstrip("@")

    candidate = candidate.split("?", 1)[0].split("#", 1)[0].strip().rstrip("/")
    if not USERNAME_RE.fullmatch(candidate):
        raise ExporterError(
            "Invalid TikTok username. Use letters, numbers, periods, or underscores."
        )
    return candidate


def configured_input_path() -> Path:
    configured = os.getenv("TIKTOK_INPUT_JSON", "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_INPUT_JSON_PATH


def configured_output_directory(input_path: Path) -> Path:
    configured = os.getenv("TIKTOK_EXPORT_DIR", "").strip()
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_LOG_ROOT


def load_profile_jobs(input_path: Path) -> tuple[list[ProfileJob], dict[str, Any]]:
    try:
        with input_path.open("r", encoding="utf-8-sig") as stream:
            document = json.load(stream)
    except FileNotFoundError as exc:
        raise ExporterError(f"Input JSON was not found: {input_path}") from exc
    except json.JSONDecodeError as exc:
        raise ExporterError(
            f"Input JSON is invalid at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc

    records: list[tuple[str, Any]] = []
    source_counts: dict[str, int] = {}
    if isinstance(document, dict):
        for source_name in SELECTED_LISTS:
            values = document.get(source_name)
            if not isinstance(values, list):
                continue
            source_counts[source_name] = len(values)
            records.extend((source_name, value) for value in values)
    elif isinstance(document, list):
        source_counts["profiles"] = len(document)
        records.extend(("profiles", value) for value in document)
    else:
        raise ExporterError(
            "Input JSON must be an object containing followers/following arrays, "
            "or a single array of profile records."
        )

    if not records:
        raise ExporterError(
            "Input JSON contains no profile records in followers or following."
        )

    ordered: dict[str, dict[str, Any]] = {}
    uid_keys: dict[str, str] = {}
    invalid_records = 0
    valid_records = 0
    for source_name, record in records:
        if isinstance(record, str):
            candidate = record
        elif isinstance(record, dict):
            candidate = str(
                first_value(
                    record.get("profile_url"),
                    record.get("url"),
                    record.get("username"),
                    record.get("unique_id"),
                    record.get("uniqueId"),
                )
                or ""
            )
        else:
            invalid_records += 1
            continue

        try:
            username = parse_username(candidate)
        except ExporterError:
            invalid_records += 1
            continue
        valid_records += 1
        key = username.casefold()
        uid = numeric_uid(first_value(record.get("id"), record.get("uid"), record.get("user_id"), record.get("userid"))) if isinstance(record, dict) else ""
        key = uid_keys.get(uid, key) if uid else key
        if uid:
            uid_keys[uid] = key
        item = ordered.setdefault(
            key,
            {
                "username": username, "uid": uid,
                "source_lists": set(),
            },
        )
        item["source_lists"].add(source_name)
        if isinstance(record, dict) and isinstance(record.get("source_lists"), list):
            item["source_lists"].update(name for name in record["source_lists"] if name in SELECTED_LISTS)
        if uid and not item.get("uid"):
            item["uid"] = uid

    jobs = [
        ProfileJob(
            username=item["username"],
            profile_url=f"{TIKTOK_ORIGIN}/@{item['username']}",
            source_lists=tuple(
                source
                for source in (*SELECTED_LISTS, "profiles")
                if source in item["source_lists"]
            ),
            uid=item.get("uid", ""),
        )
        for item in ordered.values()
    ]
    if not jobs:
        raise ExporterError("No valid TikTok profile URLs/usernames were found.")

    stats = {
        "source_records": len(records),
        "valid_records": valid_records,
        "invalid_records": invalid_records,
        "unique_profiles": len(jobs),
        "duplicates_removed": valid_records - len(jobs),
        "source_counts": source_counts,
    }
    return jobs, stats


def output_file_name(username: str, selected_lists: Iterable[str]) -> str:
    safe_username = re.sub(r"[^A-Za-z0-9._-]", "_", username.casefold())
    return f"{safe_username}_{'_and_'.join(selected_lists)}.json"


def output_file_path(
    output_directory: Path,
    username: str,
    selected_lists: Iterable[str],
) -> Path:
    return (output_directory / output_file_name(username, selected_lists)).resolve()


def compact_target_record(member: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(member.get("id") or ""),
        "username": str(member.get("username") or TARGET_USER),
        "display_name": str(member.get("display_name") or ""),
        "profile_url": str(
            member.get("profile_url") or f"{TIKTOK_ORIGIN}/@{TARGET_USER}"
        ),
    }


def relationship_from_found_lists(found_in: Iterable[str], completed_lists: Iterable[str] = ()) -> dict[str, bool | None]:
    found, completed = set(found_in), set(completed_lists)
    followers = True if "followers" in found else (False if "followers" in completed else None)
    following = True if "following" in found else (False if "following" in completed else None)
    return {"target_follows_profile": followers, "profile_follows_target": following,
            "mutual": None if followers is None or following is None else followers and following}


def explicit_restriction(info: dict[str, Any]) -> bool:
    # list_restricted is assigned only from explicit visibility evidence. Older
    # files used this exact status and an explanatory error, without extra fields.
    return (info.get("stop_reason") == "list_restricted"
            and not info.get("endpoint_exhausted") and info.get("complete") is not True)


def restricted_terminal(results: dict[str, Any]) -> bool:
    return (all(isinstance(results.get(name), dict) for name in SELECTED_LISTS)
            and any(explicit_restriction(results[name]) for name in SELECTED_LISTS)
            and all(results[name].get("complete") is True or explicit_restriction(results[name]) for name in SELECTED_LISTS))



class SuccessRecorder:
    """Thread-safe, immediate TARGET_USER match writer."""

    def __init__(
        self,
        path: Path,
        *,
        target_user: str,
        input_path: Path,
        started_at_utc: str,
    ) -> None:
        self.path = path
        self.target_user = target_user
        self.input_path = input_path
        self.started_at_utc = started_at_utc
        self.lock = threading.Lock()
        self.entries: dict[str, dict[str, Any]] = {}
        self.report: dict[str, Any] | None = None
        if self.path.is_file():
            try:
                previous = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise ExporterError(f"Cannot read existing success file; preserved it: {self.path}") from exc
            if not isinstance(previous, dict):
                raise ExporterError("Existing success file is not an object; it was preserved.")
            if not isinstance(previous.get("profiles", []), list):
                raise ExporterError("Existing success file has an invalid profiles array; it was preserved.")
            if (str(previous.get("target_user", "")).casefold() != target_user.casefold()
                    or previous.get("source_input_json") != str(input_path)):
                raise ExporterError("Existing success file belongs to a different input or target; use TIKTOK_EXPORT_DIR.")
            self.report = previous.get("report") if isinstance(previous.get("report"), dict) else None
            self.started_at_utc = previous.get("started_at_utc") or self.started_at_utc
            for entry in previous.get("profiles", []):
                if (isinstance(entry, dict) and isinstance(entry.get("username"), str)
                        and isinstance(entry.get("matches"), dict)
                        and isinstance(entry.get("found_in"), list)):
                    self.entries[entry["username"].casefold()] = entry
        with self.lock:
            self._persist_locked(scan_complete=False)

    def reconcile_complete_lists(self, job: ProfileJob, matches: dict[str, Any],
                                 completed_lists: Iterable[str]) -> None:
        """Clear old observations only when a complete new list disproves them.

        Partial/unavailable scans cannot prove absence, so previous confirmed
        observations survive. They retain their observation timestamps.
        """
        with self.lock:
            entry = self.entries.get(job.username.casefold())
            if entry is None:
                return
            completed_lists = list(completed_lists)
            if entry.get("completed_lists") == completed_lists and all(name in matches or name not in entry.get("found_in", []) for name in completed_lists):
                return
            entry["completed_lists"] = completed_lists
            for name in completed_lists:
                if name not in matches:
                    entry["found_in"] = [x for x in entry.get("found_in", []) if x != name]
                    entry.get("matches", {}).pop(name, None)
                    entry.get("match_observed_at_utc", {}).pop(name, None)
            if not entry.get("found_in"):
                self.entries.pop(job.username.casefold(), None)
            self._persist_locked(scan_complete=False)

    def record_match(
        self,
        job: ProfileJob,
        list_name: str,
        target_record: dict[str, Any],
        *,
        observed_at: str | None = None,
    ) -> None:
        if list_name not in SELECTED_LISTS:
            return
        now = utc_iso()
        key = job.username.casefold()
        with self.lock:
            entry = self.entries.get(key)
            if entry is None:
                entry = {
                    "username": job.username,
                    "profile_url": job.profile_url,
                    "source_lists": list(job.source_lists),
                    "found_in": [],
                    "matches": {},
                    "first_found_at_utc": now,
                    "last_updated_at_utc": now,
                }
                self.entries[key] = entry
            found = set(entry.get("found_in", []))
            already_recorded = list_name in found
            if already_recorded and observed_at is not None and entry.get("matches", {}).get(list_name) == compact_target_record(target_record) and entry.get("source_lists") == list(job.source_lists):
                return
            found.add(list_name)
            entry["found_in"] = [name for name in SELECTED_LISTS if name in found]
            entry["matches"][list_name] = compact_target_record(target_record)
            entry["last_updated_at_utc"] = now
            entry["source_lists"] = list(job.source_lists)
            entry.setdefault("match_observed_at_utc", {}).setdefault(list_name, observed_at or now)
            self._persist_locked(scan_complete=False)

        if not already_recorded:
            direction = (
                f"@{self.target_user} follows @{job.username}"
                if list_name == "followers"
                else f"@{job.username} follows @{self.target_user}"
            )
            console(
                f"[TARGET FOUND] {direction}; written immediately to {self.path.name}"
            )

    def found_lists_for(self, username: str) -> list[str]:
        with self.lock:
            entry = self.entries.get(username.casefold(), {})
            found = set(entry.get("found_in", []))
            return [name for name in SELECTED_LISTS if name in found]

    def _profiles_locked(self) -> list[dict[str, Any]]:
        profiles: list[dict[str, Any]] = []
        for entry in self.entries.values():
            copied = {
                **entry,
                "source_lists": list(entry.get("source_lists", [])),
                "found_in": list(entry.get("found_in", [])),
                "matches": dict(entry.get("matches", {})),
            }
            copied["relationship"] = relationship_from_found_lists(
                copied["found_in"], copied.get("completed_lists", [])
            )
            profiles.append(copied)
        profiles.sort(key=lambda item: str(item.get("username", "")).casefold())
        return profiles

    def _build_document_locked(
        self,
        *,
        scan_complete: bool,
        completed_at_utc: str | None = None,
    ) -> dict[str, Any]:
        profiles = self._profiles_locked()
        mutuals = [
            {
                "username": profile["username"],
                "profile_url": profile["profile_url"],
                "found_in": profile["found_in"],
            }
            for profile in profiles
            if profile["relationship"]["mutual"]
        ]
        return {
            **({"report": self.report} if self.report else {}),
            "schema_version": 1,
            "target_user": self.target_user,
            "target_profile_url": f"{TIKTOK_ORIGIN}/@{self.target_user}",
            "source_input_json": str(self.input_path),
            "started_at_utc": self.started_at_utc,
            "updated_at_utc": utc_iso(),
            "completed_at_utc": completed_at_utc,
            "scan_complete": scan_complete,
            "observation_note": (
                "Confirmed observations persist through incomplete scans. Compare per-list "
                "match_observed_at_utc timestamps and scan_state.json; an incomplete scan "
                "does not prove absence or that an older relationship still exists."
            ),
            "mutual_definition": (
                "A profile is mutual with the target when the target appears in "
                "both that profile's followers list and following list."
            ),
            "summary": {
                "profiles_with_target": len(profiles),
                "target_in_followers_count": sum(
                    "followers" in profile["found_in"] for profile in profiles
                ),
                "target_in_following_count": sum(
                    "following" in profile["found_in"] for profile in profiles
                ),
                "mutual_count": len(mutuals),
            },
            "mutuals": mutuals,
            "profiles": profiles,
        }

    def _persist_locked(
        self,
        *,
        scan_complete: bool,
        completed_at_utc: str | None = None,
    ) -> None:
        atomic_write_json(
            self.path,
            self._build_document_locked(
                scan_complete=scan_complete,
                completed_at_utc=completed_at_utc,
            ),
        )

    def finalize(self, *, scan_complete: bool) -> dict[str, Any]:
        """Re-read the success file from disk and recalculate final mutuals."""
        with self.lock:
            self._persist_locked(scan_complete=False)
            with self.path.open("r", encoding="utf-8") as stream:
                disk_document = json.load(stream)

            profiles = disk_document.get("profiles", [])
            if not isinstance(profiles, list):
                profiles = []
            clean_profiles: list[dict[str, Any]] = []
            for profile in profiles:
                if not isinstance(profile, dict):
                    continue
                found = {
                    name
                    for name in profile.get("found_in", [])
                    if name in SELECTED_LISTS
                }
                profile["found_in"] = [
                    name for name in SELECTED_LISTS if name in found
                ]
                profile["relationship"] = relationship_from_found_lists(found, profile.get("completed_lists", []))
                clean_profiles.append(profile)
            clean_profiles.sort(
                key=lambda item: str(item.get("username", "")).casefold()
            )
            mutuals = [
                {
                    "username": str(profile.get("username", "")),
                    "profile_url": str(profile.get("profile_url", "")),
                    "found_in": profile["found_in"],
                }
                for profile in clean_profiles
                if profile["relationship"]["mutual"]
            ]
            disk_document["profiles"] = clean_profiles
            disk_document["mutuals"] = mutuals
            disk_document["summary"] = {
                "profiles_with_target": len(clean_profiles),
                "target_in_followers_count": sum(
                    "followers" in profile["found_in"] for profile in clean_profiles
                ),
                "target_in_following_count": sum(
                    "following" in profile["found_in"] for profile in clean_profiles
                ),
                "mutual_count": len(mutuals),
            }
            disk_document["scan_complete"] = scan_complete
            disk_document["updated_at_utc"] = utc_iso()
            disk_document["completed_at_utc"] = utc_iso()
            atomic_write_json(self.path, disk_document)
            return disk_document


class ScanStateRecorder:
    """Small resume/progress checkpoint, written after every processed profile."""

    def __init__(
        self,
        path: Path,
        *,
        input_path: Path,
        target_user: str,
        total_profiles: int,
        worker_count: int,
        input_stats: dict[str, Any],
        started_at_utc: str,
    ) -> None:
        self.path = path
        self.total_profiles = total_profiles
        self.started_at_utc = started_at_utc
        self.lock = threading.Lock()
        self.profiles: dict[str, dict[str, Any]] = {}
        self.current_run: set[str] = set()
        if path.is_file():
            try:
                previous = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(previous, dict) or not isinstance(previous.get("profiles", []), list):
                    raise ValueError("Invalid checkpoint structure")
                if (isinstance(previous, dict)
                        and previous.get("source_input_json") == str(input_path)
                        and previous.get("target_user") == target_user):
                    for item in previous.get("profiles", []):
                        if isinstance(item, dict) and isinstance(item.get("username"), str):
                            self.profiles[item["username"].casefold()] = {**item, "from_previous_run": True}
            except (OSError, ValueError) as exc:
                raise ExporterError(f"Cannot read existing checkpoint; preserved it: {path}") from exc
        self.base = {
            "schema_version": 1,
            "source_input_json": str(input_path),
            "target_user": target_user,
            "selected_lists": list(SELECTED_LISTS),
            "worker_count": worker_count,
            "input": input_stats,
            "started_at_utc": started_at_utc,
        }
        with self.lock:
            self._persist_locked(scan_complete=False, fatal_error=None)

    def _summary_locked(self) -> dict[str, Any]:
        statuses: dict[str, int] = {}
        for key, result in self.profiles.items():
            if key not in self.current_run:
                continue
            status = str(result.get("status") or "unknown")
            statuses[status] = statuses.get(status, 0) + 1
        return {
            "total_profiles": self.total_profiles,
            "processed_profiles": len(self.current_run),
            "remaining_profiles": max(0, self.total_profiles - len(self.current_run)),
            "statuses": dict(sorted(statuses.items())),
        }

    def _persist_locked(
        self,
        *,
        scan_complete: bool,
        fatal_error: str | None,
    ) -> None:
        document = {
            **self.base,
            "updated_at_utc": utc_iso(),
            "completed_at_utc": utc_iso() if scan_complete else None,
            "scan_complete": scan_complete,
            "fatal_error": fatal_error,
            "summary": self._summary_locked(),
            "profiles": sorted(
                self.profiles.values(),
                key=lambda item: str(item.get("username", "")).casefold(),
            ),
        }
        atomic_write_json(self.path, document)

    def record(self, result: ProfileProcessResult) -> tuple[int, int]:
        with self.lock:
            self.profiles[result.username.casefold()] = asdict(result)
            self.current_run.add(result.username.casefold())
            self._persist_locked(scan_complete=False, fatal_error=None)
            return len(self.current_run), self.total_profiles

    def finalize(
        self,
        *,
        scan_complete: bool,
        fatal_error: str | None,
    ) -> dict[str, Any]:
        with self.lock:
            self._persist_locked(
                scan_complete=scan_complete,
                fatal_error=fatal_error,
            )
            with self.path.open("r", encoding="utf-8") as stream:
                return json.load(stream)


class WorkerApiError(ExporterError):
    """A Worker HTTP/API failure, with a checkpoint-friendly category."""

    def __init__(self, message: str, *, kind: str = "api_error",
                 retryable: bool = False, code: int | None = None) -> None:
        super().__init__(message)
        self.kind = kind
        self.retryable = retryable
        self.code = code


class InvalidWorkerResponse(WorkerApiError):
    def __init__(self, message: str) -> None:
        super().__init__(message, kind="invalid_response")


class ScanCancelled(WorkerApiError):
    def __init__(self) -> None:
        super().__init__("Scan cancelled by the user.", kind="cancelled")


def retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        seconds = float(value)
    except (ValueError, TypeError):
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=timezone.utc)
            seconds = date.timestamp() - time.time()
        except (ValueError, TypeError, OverflowError):
            return None
    return max(0.0, seconds) if math.isfinite(seconds) else None


def api_error(message: Any) -> WorkerApiError:
    # Only classify explicit human-readable errors. No guessed TikTok codes.
    text = str(message or "Worker reported an unspecified API error.")[:500]
    lower = text.casefold()
    if re.search(r"unauthori[sz]ed|access restricted|access denied|forbidden", lower):
        return WorkerApiError(text, kind="access_denied")
    if re.search(r"rate.?limit|too many requests", lower):
        return WorkerApiError(text, kind="rate_limited", retryable=True)
    if re.search(r"user(?:name)?[^.]*not found|account[^.]*not found|does not exist", lower):
        return WorkerApiError(text, kind="not_found")
    if "invalid username" in lower:
        return WorkerApiError(text, kind="invalid_username")
    # A list-level restriction must never be interpreted as profile privacy.
    if re.search(r"private|not publicly visible|list[^.]*restricted", lower):
        return WorkerApiError(text, kind="list_restricted")
    return WorkerApiError(text)


def check_worker_error(payload: Any, operation: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise InvalidWorkerResponse("Worker JSON root must be an object.")
    if payload.get("status") == "error" or payload.get("error"):
        raise api_error(payload.get("message") or payload.get("error"))
    if operation == "profile":
        if payload.get("status") != "ok":
            if payload.get("message"):
                raise api_error(payload["message"])
            raise InvalidWorkerResponse("Profile response is missing status='ok'.")
    elif payload.get("message"):
        # Both list handlers in the supplied frontend treat message as an error.
        raise api_error(payload["message"])
    return payload


# Official references checked 2026-09-18 (proxy login, not Webshare API tokens):
# https://help.webshare.io/en/articles/8596696-how-can-i-download-my-proxy-list
# https://apidocs.webshare.io/proxy-connection
# https://help.webshare.io/en/articles/8375281-what-is-concurrency
# https://help.webshare.io/en/articles/8570214-what-are-the-configuration-errors
# https://www.python-httpx.org/advanced/proxies/
# https://www.python-httpx.org/async/

@dataclass(frozen=True)
class ProxyConfig:
    host: str
    port: int
    username: str | None = field(default=None, repr=False)
    password: str | None = field(default=None, repr=False)
    scheme: str = "http"

    @property
    def endpoint(self) -> str:
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{self.scheme}://{host}:{self.port}"

    @property
    def mode(self) -> str:
        # p.webshare.io is shared by backbone, sticky and rotating products.
        # Do not invent username suffixes or infer a subscription from an IP.
        if self.host.casefold() == "p.webshare.io":
            return "webshare_gateway"
        return "direct_endpoint"


def parse_proxy_line(line: str) -> ProxyConfig:
    """Webshare export: host:port:username:password; URLs/IP authorization too.

    Split colon exports at most three times: a colon within the password must
    survive. In URLs, reserved credential characters must be percent-encoded.
    Invalid-input errors intentionally never contain the original line.
    """
    try:
        text = line.strip()
        if not text or any(ord(c) < 32 or ord(c) == 127 for c in text):
            raise ValueError
        if "://" in text:
            parsed = urlsplit(text)
            if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
                raise ValueError
            scheme = parsed.scheme.casefold()
            host, port = parsed.hostname, parsed.port
            username = unquote(parsed.username) if parsed.username is not None else None
            password = unquote(parsed.password) if parsed.password is not None else None
        else:
            scheme = "http"
            if text.startswith("["):
                close = text.index("]")
                host = text[1:close]
                if text[close + 1:close + 2] != ":":
                    raise ValueError
                parts = text[close + 2:].split(":", 2)
                port_text, *auth = parts
            else:
                parts = text.split(":", 3)
                host, port_text, *auth = parts
            port = int(port_text)
            if len(auth) not in (0, 2):
                raise ValueError
            username, password = auth if auth else (None, None)
        if scheme not in {"http", "https", "socks5", "socks5h"}:
            raise ValueError
        if not host or port is None or not 1 <= port <= 65535:
            raise ValueError
        if re.search(r"[\s/@?#\\]", host):
            raise ValueError
        if ":" in host:
            ipaddress.IPv6Address(host)
        elif not re.fullmatch(r"[A-Za-z0-9.-]+", host):
            raise ValueError
        if (username is None) != (password is None):
            raise ValueError
        if username is not None:
            if not username or not password or ":" in username:
                raise ValueError
            if any(ord(c) < 32 or ord(c) == 127 for c in username + password):
                raise ValueError
        return ProxyConfig(host.casefold(), port, username, password, scheme)
    except (ValueError, TypeError, IndexError):
        raise ExporterError("Invalid proxy entry (expected host:port:user:password or a proxy URL).") from None


class CredentialRedactor:
    """Separate diagnostic redaction from structure-preserving data cleaning."""

    def __init__(self, configs: Iterable[ProxyConfig] = ()) -> None:
        secrets = set()
        for config in configs:
            for value in (config.username, config.password):
                if value:
                    secrets.update((value, quote(value, safe="")))
            if config.username is not None:
                combined = f"{config.username}:{config.password}"
                secrets.add(base64.b64encode(combined.encode()).decode())
        self.secrets = sorted(secrets, key=len, reverse=True)

    _url = re.compile(r"(?:https?|socks5h?)://[^\s/]*@", re.I)
    _sensitive = frozenset({"authorization", "proxy_authorization", "password", "proxy_password", "proxy_username", "cookie", "cookies", "set_cookie", "mstoken", "sessionid", "sessionid_ss", "sid_tt", "sid_guard", "x_gnarly", "x_dynosaur"})
    _diagnostic = frozenset({"debug", "error", "message", "fatal_error", "exception", "traceback"})

    def text(self, value: Any) -> str:
        text = str(value)
        if "://" in text and "@" in text:
            text = self._url.sub("[REDACTED_PROXY]@", text)
        for secret in self.secrets:
            text = text.replace(secret, "[REDACTED]")
        return text

    def clean(self, value: Any) -> Any:
        if isinstance(value, str):
            # A name or biography can legitimately contain a proxy-login
            # substring. Only credential-bearing URL syntax is removed here.
            return self._url.sub("[REDACTED_PROXY]@", value) if "://" in value and "@" in value else value
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                label = str(key).casefold().replace("-", "_")
                if label in self._sensitive:
                    result[key] = "[REDACTED]"
                elif label in self._diagnostic and isinstance(item, str):
                    result[key] = self.text(item)
                else:
                    result[self.clean(key) if isinstance(key, str) and "://" in key else key] = self.clean(item)
            return result
        if isinstance(value, list):
            return [self.clean(item) for item in value]
        return value


REDACTOR = CredentialRedactor()


def configured_proxy_path() -> Path | None:
    raw = os.getenv("TIKTOK_PROXY_FILE", "").strip()
    if raw.casefold() in {"none", "off", "0"}:
        return None
    if raw:
        return Path(raw).expanduser()
    return WEBSHARE_PROXY_FILE


def load_proxy_configs(path: Path | None) -> list[ProxyConfig]:
    if path is None:
        console("[PROXY] No proxy file selected; direct connection is available.")
        return []
    try:
        lines = path.read_text(encoding="utf-8-sig").splitlines()
    except OSError:
        console(f"[PROXY] Proxy file is missing or unreadable: {path}")
        return []
    configs: list[ProxyConfig] = []
    seen: set[ProxyConfig] = set()
    invalid = duplicates = 0
    for line_number, line in enumerate(lines, 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        try:
            config = parse_proxy_line(line)
            if "://" not in line and config.username is None:
                raise ExporterError("Downloaded entries require all four fields.")
        except ExporterError:
            invalid += 1
            console(f"[PROXY] Skipping malformed entry on line {line_number}; expected host:port:username:password or an explicit proxy URL.")
            continue
        if config in seen:
            duplicates += 1
            continue
        seen.add(config)
        configs.append(config)
    gateways = sum(config.mode == "webshare_gateway" for config in configs)
    console(f"[PROXY] Loaded {len(configs)} entries: {len(configs) - gateways} direct endpoints, "
            f"{gateways} Webshare gateways; ignored {invalid} invalid / {duplicates} duplicate entries.")
    console("[PROXY] Connectivity is checked on real requests; untested entries are not declared healthy.")
    return configs


@dataclass
class ProxyState:
    config: ProxyConfig = field(repr=False)
    label: str = "proxy"
    success_count: int = 0
    failure_count: int = 0
    consecutive_failures: int = 0
    cooldown_until: float = 0.0
    last_failure: str | None = None
    disabled: bool = False
    in_use: int = 0
    last_used: float = 0.0


class ProxyPool:
    """State is owned by one asyncio event loop; selection never awaits mid-update."""

    def __init__(self, configs: list[ProxyConfig], *, proxy_only: bool = False) -> None:
        self.states = [ProxyState(config, f"proxy-{i:03d}") for i, config in enumerate(configs, 1)]
        self.proxy_only = proxy_only
        self.fallback_announced = False

    def available(self) -> list[ProxyState]:
        now = time.monotonic()
        return [p for p in self.states if not p.disabled and p.cooldown_until <= now]

    def capacity(self) -> int:
        usable = self.available()
        # Gateways and a single direct proxy can support simultaneous connections;
        # this is a scanner-side bound, not an inferred account entitlement.
        return max(4, len(usable)) if usable else (0 if self.proxy_only else 4)

    def choose(self) -> ProxyState | None:
        available = self.available()
        if available:
            selected = min(available, key=lambda p: (p.in_use, p.last_used))
            selected.last_used = time.monotonic()
            self.fallback_announced = False
            return selected
        if self.proxy_only:
            raise WorkerApiError("No usable proxies; proxy-only mode forbids direct fallback.", kind="no_usable_proxy")
        if not self.fallback_announced:
            console("[PROXY] No usable proxies remain; using direct connection.")
            self.fallback_announced = True
        return None

    def good_response(self, state: ProxyState | None) -> None:
        if state is None:
            return
        first = state.success_count == 0
        state.success_count += 1
        state.consecutive_failures = 0
        state.cooldown_until = 0.0
        if first:
            confirmed = sum(p.success_count > 0 and not p.disabled for p in self.states)
            console(f"[PROXY] {state.label} connected; {confirmed}/{len(self.states)} confirmed so far.")

    def failed(self, state: ProxyState, kind: str) -> None:
        state.failure_count += 1
        state.consecutive_failures += 1
        state.last_failure = kind  # fixed category, never exception text
        if kind in {"proxy_authentication_failure", "proxy_not_allocated"}:
            state.disabled = True
            console(f"[PROXY] {state.label}: {kind}; disabled for this run.")
        else:
            delay = min(300.0, 15.0 * 2 ** min(state.consecutive_failures - 1, 5))
            state.cooldown_until = time.monotonic() + delay
            console(f"[PROXY] {state.label}: {kind}; cooldown {delay:.0f}s; another connection may be tried.")

    def summary(self) -> dict[str, Any]:
        return {"loaded": len(self.states), "available": len(self.available()),
                "confirmed": sum(p.success_count > 0 for p in self.states),
                "disabled": sum(p.disabled for p in self.states),
                "proxy_only": self.proxy_only,
                "connections": [{"label": p.label, "mode": p.config.mode,
                                 "successes": p.success_count, "failures": p.failure_count,
                                 "last_failure": p.last_failure, "disabled": p.disabled}
                                for p in self.states]}


class AsyncRequestGate:
    """Hard semaphore plus adaptive concurrency and one global start-time pacer.

    The brief scheduling lock is released BEFORE HTTP. Slow responses overlap,
    while starts (including proxy retries) remain globally spaced. Cooldowns
    can extend a wait in progress. Idle periods never accumulate burst tokens.
    """

    def __init__(self, minimum: float, maximum: float, stop_event: asyncio.Event,
                 *, ceiling: int, initial: int, adaptive: bool) -> None:
        if not (math.isfinite(minimum) and math.isfinite(maximum) and 0 <= minimum <= maximum):
            raise ExporterError("Invalid request-delay range.")
        self.minimum, self.maximum = minimum, maximum
        self.ceiling, self.limit, self.adaptive = ceiling, initial, adaptive
        self.stop_event = stop_event
        self.semaphore = asyncio.Semaphore(ceiling)
        self.lock = asyncio.Lock()
        self.changed = asyncio.Event()
        self.capacity_waiters = deque()
        self.active = self.peak_active = self.started = 0
        self.next_allowed = self.cooldown_until = 0.0
        self.freeze_growth_until = 0.0
        self.latency: float | None = None
        self.success_samples = 0
        self.blocked_reason: str | None = None
        self.rate_limit_exhausted = False
        self.rate_limit_event = asyncio.Event()
        self.rate_limit_reason: str | None = None
        self.network_tasks: set[asyncio.Task] = set()
        self.stats = None
        self.retry_queue = None
        self.last_adjusted = time.monotonic()
        self.baseline_latency = None
        self.failure_samples = 0

    def stop_rate_limited(self, reason: str = 'HTTP 429 received from API') -> None:
        first = not self.rate_limit_event.is_set()
        self.rate_limit_reason = self.rate_limit_reason or reason
        self.rate_limit_exhausted = True  # Compatibility with existing integrations.
        self.rate_limit_event.set()
        self.stop_event.set()  # No await between detection and closing admission.
        self.changed.set()
        self.wake_capacity()
        current = asyncio.current_task()
        for task in tuple(self.network_tasks):
            if task is not current and not task.done():
                task.cancel()
        if self.stats:
            self.stats.stop('stopped_rate_limited', self.rate_limit_reason)
        if first:
            heading = 'HTTP 429 RATE LIMIT DETECTED' if reason.startswith('HTTP 429') else 'WORKER RATE LIMIT DETECTED'
            console('\n' + heading + '\nAll new API requests have been stopped.\n'
                    'Saving progress, pending profiles, failed profiles, retry state and statistics.\n'
                    'The scan can be resumed later.', error=True)

    def check(self) -> None:
        if self.blocked_reason:
            raise WorkerApiError(self.blocked_reason, kind="access_denied")
        if self.rate_limit_exhausted:
            raise WorkerApiError(self.rate_limit_reason or 'Rate limit detected; scan stopped.', kind="rate_limited")
        if self.stop_event.is_set():
            raise ScanCancelled()

    async def wait(self, seconds: float) -> None:
        self.check()
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=max(0.001, seconds))
        except asyncio.TimeoutError:
            return
        self.check()

    def wake_capacity(self, count=None):
        remaining = len(self.capacity_waiters) if count is None else max(0, count)
        while self.capacity_waiters and remaining:
            waiter = self.capacity_waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                remaining -= 1

    @asynccontextmanager
    async def slot(self):
        async with self.semaphore:
            acquired = False
            try:
                while not acquired:
                    async with self.lock:
                        self.check()
                        if self.active < self.limit:
                            self.active += 1
                            self.peak_active = max(self.peak_active, self.active)
                            acquired = True
                        else:
                            waiter = asyncio.get_running_loop().create_future()
                            self.capacity_waiters.append(waiter)
                    if not acquired:
                        try:
                            # FIFO notifications avoid waking every queued profile
                            # on every response. Timeout handles external stop events.
                            await asyncio.wait_for(waiter, timeout=.2)
                        except asyncio.TimeoutError:
                            pass
                        finally:
                            if waiter in self.capacity_waiters:
                                self.capacity_waiters.remove(waiter)
                yield
            finally:
                if acquired:
                    self.active -= 1
                    self.wake_capacity(self.limit-self.active)


    async def pace(self) -> None:
        """Space dispatch after client preparation, so setup cannot bunch starts."""
        while True:
            async with self.lock:
                self.check()
                now = time.monotonic()
                delay = max(self.next_allowed, self.cooldown_until) - now
                if delay <= 0:
                    self.next_allowed = now + random.uniform(self.minimum, self.maximum)
                    self.started += 1
                    return
            await self.wait(delay)

    def cooldown(self, seconds: float) -> None:
        self.cooldown_until = max(self.cooldown_until, time.monotonic() + seconds)

    def penalize(self, seconds: float) -> None:
        self.cooldown(seconds)
        self.freeze_growth_until = max(self.freeze_growth_until, time.monotonic() + seconds + 30)
        self.set_limit(max(1, self.limit // 2))

    def set_limit(self, value: int) -> None:
        value = max(1, min(self.ceiling, value))
        if value != self.limit:
            self.limit = value
            self.changed.set()
            self.wake_capacity(self.limit-self.active)
            console(f"[ASYNC] Active request limit adjusted to {value} (ceiling {self.ceiling}).")

    def observe(self, latency: float, pool: ProxyPool) -> None:
        self.latency = latency if self.latency is None else 0.75 * self.latency + 0.25 * latency
        self.success_samples += 1
        self.baseline_latency = latency if self.baseline_latency is None else min(self.baseline_latency, latency)
        now = time.monotonic()
        if not self.adaptive or now - self.last_adjusted < 1:
            return
        spacing = (self.minimum + self.maximum) / 2
        desired = min(self.ceiling, max(1, math.ceil(self.latency / spacing) + 1)) if spacing else self.ceiling
        if self.latency > max(2.0, self.baseline_latency * 3):
            self.set_limit(max(1, self.limit * 3 // 4))
            self.freeze_growth_until = now + 30
        elif now >= self.freeze_growth_until and self.success_samples >= 20:
            self.set_limit(min(desired, max(self.limit + 1, self.limit * 2)))
        self.last_adjusted = now
        self.success_samples = self.failure_samples = 0

    def observe_failure(self) -> None:
        self.failure_samples += 1
        now = time.monotonic()
        if self.adaptive and self.failure_samples >= 3 and now - self.last_adjusted >= 1:
            self.set_limit(max(1, self.limit // 2))
            self.freeze_growth_until = now + 30
            self.last_adjusted = now
            self.failure_samples = 0


async def disk_call(function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Finish an atomic disk operation before acknowledging cancellation."""
    lane = _DISK_LANE.get()
    if lane is not None:
        owner = getattr(function, '__self__', None)
        isolated = isinstance(owner, UserStore) or function in (UserStore, write_final_json, inspect_reusable_export)
        isolated = isolated or (function is atomic_write_json and args and 'raw_api' in Path(args[0]).parts)
        if isolated:
            return await lane.independent(function, *args, **kwargs)
        return await lane.call(function, *args, **kwargs)
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await asyncio.shield(task)
        raise


class WorkerApiClient:
    """Async HTTPX clients per route. Proxy credentials go ONLY to the proxy.

    Worker schema remains the live-verified schema from 2026-09-18. There are
    no alternate API sources, redirects, browser cookies or direct TikTok calls.
    """

    def __init__(self, gate: AsyncRequestGate, pool: ProxyPool, *,
                 raw_directory: Path | None = None, client_factory: Any = None, settings: dict[str, Any] | None = None,
                 avatar_directory: Path | None = None) -> None:
        try:
            import httpx
        except ImportError:
            raise MissingDependency('Install: py -3.11 -m pip install -r requirements.txt') from None
        self.httpx = httpx
        self.settings = settings or {}
        self.gate, self.pool = gate, pool
        if not hasattr(self, 'metrics'):
            from backend_metrics import BackendMetrics
            self.metrics = getattr(gate.stats, 'backends', None) or BackendMetrics()
            if gate.stats: gate.stats.backends = self.metrics
        self.raw_directory = raw_directory
        self.clients: dict[str, Any] = {}
        if not hasattr(self, 'name'):
            from connection_metrics import pool_snapshot
            self.metrics.pool_provider = lambda: {'worker': pool_snapshot(self.clients.values())}
            self.metrics.proxy_pool_provider = lambda: {
                'worker:'+key: pool_snapshot([client]) for key, client in self.clients.items() if key != 'direct'}
        self.connection_slots = {}
        self.ssl_context = None
        self.client_factory = client_factory
        self.profile_cache: OrderedDict[str, ProfileSnapshot] = OrderedDict()
        self.retries = RetryQueue(gate)
        self.gate.retry_queue = self.retries
        self.request_retries: ContextVar[int] = ContextVar("request_retries", default=0)
        self.redactor = CredentialRedactor(p.config for p in pool.states)
        self.worker_origin = validate_worker_origin(self.settings["worker_origin"]) if "worker_origin" in self.settings else WORKER_ORIGIN
        self.avatar_blocked_hosts: set[str] = set()
        self.avatar_transport_failures: dict[str, int] = {}
        self.avatars = None
        self.avatar_backfill_task = None
        self.avatar_backfill_requested = False
        if avatar_directory is not None:
            from avatar_cache import AvatarCache
            self.avatars = AvatarCache(avatar_directory, self.download_avatar, disk_io=disk_call,
                                      concurrency=self.settings.get('avatar_workers', 0) or gate.ceiling)
        # Dependency debug traces can contain proxy headers. The scanner does
        # not expose them, even if its embedding application's root logger is DEBUG.
        for name in ("httpx", "httpcore"):
            logging.getLogger(name).disabled = True
            logging.getLogger(name).addFilter(SecretLogFilter(self.redactor))
        for name in ("httpcore.connection", "httpcore.http11", "httpcore.proxy", "httpcore.socks", "httpcore.http2"):
            logging.getLogger(name).disabled = True

    async def __aenter__(self) -> "WorkerApiClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        # Stop image consumers before closing their shared cache/HTTP clients.
        if self.avatar_backfill_task is not None:
            if not self.avatar_backfill_task.done():
                self.avatar_backfill_task.cancel()
            await asyncio.gather(self.avatar_backfill_task, return_exceptions=True)
        await self.retries.close()
        if self.avatars is not None:
            await disk_call(self.avatars.close)
        for client in self.clients.values():
            try:
                await client.aclose()
            except Exception:
                console("[NETWORK] A connection could not be closed cleanly.")

    def start_avatar_backfill(self, root: Path) -> None:
        """Cache saved pending/error account avatars while the scan is running."""
        if self.avatars is None or self.gate.stop_event.is_set():
            return
        self.avatar_backfill_requested = True
        if self.avatar_backfill_task is not None and not self.avatar_backfill_task.done():
            return
        async def fill():
            while self.avatar_backfill_requested and not self.gate.stop_event.is_set():
                self.avatar_backfill_requested = False
                await self.cache_saved_avatars(root)
        self.avatar_backfill_task = asyncio.create_task(fill(), name='report-avatar-backfill')

    async def cache_saved_avatars(self, root: Path) -> dict[str, int]:
        """Image-only backfill, with workers created only for saved URLs."""
        from report_generator import iter_avatar_profiles
        pending = iter(iter_avatar_profiles(root))
        lock = asyncio.Lock()
        counts = {'checked': 0, 'available': 0, 'unavailable': 0}
        capacity = min(self.settings.get('avatar_workers', 0) or self.gate.limit, self.gate.limit)
        consumers = []
        def take(limit): return list(itertools.islice((r for r in pending if r[2]), limit))
        try:
            buffered = deque(await disk_call(take, capacity))
            async def consume():
                while not self.gate.stop_event.is_set():
                    async with lock:
                        batch = [buffered.popleft()] if buffered else await disk_call(take, 16)
                    if not batch: return
                    for uid, name, url in batch:
                        if self.gate.stop_event.is_set(): return
                        value = await self.avatars.ensure(uid, name, url)
                        counts['checked'] += 1
                        counts['available' if value else 'unavailable'] += 1
            consumers = [asyncio.create_task(consume(), name='report-avatar-consumer') for _ in range(len(buffered))]
            outcomes = await asyncio.gather(*consumers, return_exceptions=True)
            if any(isinstance(v, Exception) for v in outcomes):
                console('[AVATAR] Some saved image references were unavailable; scan data preserved.')
            return counts
        finally:
            for task in consumers:
                if not task.done(): task.cancel()
            await asyncio.gather(*consumers, return_exceptions=True)
            await disk_call(pending.close)

    async def borrow_client(self, route):
        """Bound retained per-route pools as well as aggregate active sockets."""
        if not hasattr(self, '_pool_lock'):
            self._pool_lock = asyncio.Lock()
            self._client_users = Counter()
        key = route.label if route else 'direct'
        async with self._pool_lock:
            if self.ssl_context is None:
                # HTTPX's trust_env=False default is Certifi. Load that same trust
                # store once off-loop and reuse it safely across origin connections.
                def trust_store():
                    import ssl
                    import certifi
                    return ssl.create_default_context(cafile=certifi.where())
                self.ssl_context = await disk_call(trust_store)
            self.gate.check()
            if key not in self.clients and len(self.clients) >= self.gate.ceiling:
                for old in list(self.clients):
                    if not self._client_users[old]:
                        unused = self.clients.pop(old)
                        await unused.aclose()
                        del self._client_users[old]
                        break
            client = self.get_client(route)
            self._client_users[key] += 1
            return client

    def release_client(self, route):
        self._client_users[route.label if route else 'direct'] -= 1

    def get_client(self, route: ProxyState | None) -> Any:
        key = route.label if route is not None else "direct"
        if key in self.clients:
            return self.clients[key]
        httpx = self.httpx
        proxy = None
        if route is not None:
            config = route.config
            proxy = httpx.Proxy(config.endpoint, auth=(config.username, config.password)
                                if config.username is not None else None)
        connection_limit = min(self.gate.ceiling, self.settings.get('worker_backend_max_connections', 0) or self.gate.ceiling)
        if route and self.settings.get('connections_per_proxy', 0):
            connection_limit = min(connection_limit, self.settings['connections_per_proxy'])
        keepalive = self.settings.get('worker_backend_keepalive_connections', 0) or self.gate.ceiling
        keepalive = min(connection_limit, max(1, math.ceil(keepalive / max(1, len(self.pool.states)))))
        self.connection_slots[key] = asyncio.Semaphore(connection_limit)
        options = {
            "proxy": proxy, "trust_env": False, "follow_redirects": False,
            'verify': self.ssl_context if self.ssl_context is not None else True,
            "timeout": httpx.Timeout(self.settings.get("timeouts", {}).get("response", 40.0),
                connect=self.settings.get("timeouts", {}).get("connect", 10.0),
                write=self.settings.get("timeouts", {}).get("write", 20.0),
                pool=self.settings.get("timeouts", {}).get("pool", 10.0)),
            "limits": httpx.Limits(max_connections=connection_limit,
                                   max_keepalive_connections=keepalive, keepalive_expiry=90.0),
            "event_hooks": {"request": [self.before_send], "response": [self.response_headers]},
            "headers": {"User-Agent": USER_AGENT, "Accept": "*/*",
                        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                        "Origin": "https://fintok.neocities.org",
                        "Referer": "https://fintok.neocities.org/"},
        }
        try:
            client = self.client_factory(route, options) if self.client_factory else httpx.AsyncClient(**options)
        except ImportError:
            raise MissingDependency('SOCKS support requires: py -3.11 -m pip install "httpx[socks]>=0.28.1,<1"') from None
        except Exception:
            raise ExporterError("Could not initialize the async HTTP client; check proxy configuration and HTTPX installation.") from None
        self.clients[key] = client
        return client

    async def before_send(self, request: Any) -> None:
        self.gate.check()  # Immediately before HTTPX sends, including queued calls.

    async def response_headers(self, response: Any) -> None:
        if response.status_code == 429 and str(response.request.url).startswith(self.worker_origin + '/'):
            self.gate.stop_rate_limited()
            # Don't wait for a slow/trickling 429 body. HTTPX closes on hook errors.
            raise WorkerApiError('Worker HTTP 429.', kind='rate_limited', code=429)

    async def retry_wait(self, seconds: float) -> None:
        # Preserve the injected wait hook used by old embedders/tests.
        if 'wait' in self.gate.__dict__:
            await self.gate.wait(seconds)
        else:
            await self.retries.wait(seconds)

    def proxy_exception(self, exc: Exception) -> WorkerApiError:
        # Only inspect known status/code tokens. Never return a library exception
        # string: it can include authenticated URLs or Proxy-Authorization.
        text = self.redactor.text(exc).casefold()
        # HTTPX's CONNECT ProxyError has no response in most transports. Its
        # status starts the message (e.g. "407 Proxy Authentication Required").
        # Never treat a number buried in a hostname, URL, or prose as a status.
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        if type(status) is not int:
            match = re.match(r"^\s*(?:(?:http/\d(?:\.\d)?|http|connect)\s+)?(403|407|429|502|503|504)(?=\s|:|$)", text)
            status = int(match[1]) if match else None
        if "client_connect_forbidden_host" in text or status == 403:
            return WorkerApiError("Webshare refused this destination; no route switching.", kind="access_denied")
        if "client_connect_high_concurrency" in text or status == 429:
            return WorkerApiError("Webshare concurrency limit reached.", kind="proxy_concurrency_limited", retryable=True)
        if "no_proxies_allocated" in text:
            return WorkerApiError("Proxy is no longer allocated to this account.", kind="proxy_not_allocated", retryable=True)
        if status == 407 or "authentication failed" in text or "authentication required" in text:
            return WorkerApiError("Proxy authentication failed (407 may also mean an unallocated proxy).", kind="proxy_authentication_failure", retryable=True)
        if status in (502, 503, 504):
            return WorkerApiError("Proxy gateway is temporarily unavailable.", kind="proxy_temporarily_unavailable", retryable=True)
        return WorkerApiError("Proxy tunnel could not be established.", kind="proxy_connection_failure", retryable=True)

    async def request_json(self, operation: str, params: dict[str, str], *, context: str | None = None) -> dict[str, Any]:
        if operation not in ("profile", "followers", "following"):
            raise ExporterError("Unknown Worker operation.")
        path = "/" if operation == "profile" else f"/api/{operation}"
        method = "GET" if operation == "profile" else "POST"
        route: ProxyState | None = None
        choose_route = True
        last_failure: WorkerApiError | None = None
        attempts = self.settings.get("worker_retry_attempts", self.settings.get("retry_attempts", FETCH_ATTEMPTS))
        retried_404 = False
        normal_failures = 0
        label = context or (f"@{params.get('username')} profile lookup" if operation == "profile" else operation.upper())
        for attempt in range(attempts + 1):
            self.request_retries.set(attempt)
            first_404 = False
            retry_after = None
            switch_route = False
            response = None
            failure = None
            async with self.gate.slot():
                if choose_route:
                    route = self.pool.choose()
                    choose_route = False
                client = await self.borrow_client(route)
                if route is not None:
                    route.in_use += 1
                started = time.monotonic()
                success = cancelled = False
                attempt_started = False
                network_task = None
                status = None
                connection_slot = self.connection_slots[route.label if route else 'direct']
                acquired = False
                try:
                    waiting = time.monotonic()
                    await connection_slot.acquire()
                    acquired = True
                    self.metrics.add('worker', connection_pool_wait_seconds=time.monotonic()-waiting)
                    self.gate.check()
                    async def send_request():
                        nonlocal started, attempt_started
                        await self.gate.pace()
                        started = time.monotonic()
                        attempt_started = True
                        self.metrics.begin('worker', proxy=route.label if route else None)
                        if self.gate.stats:
                            self.gate.stats.request_started(retry=attempt > 0)
                        # Waiting for pacing is not a network timeout. Once sent,
                        # the total timeout also bounds trickling response bodies.
                        async with asyncio.timeout(self.settings.get("timeouts", {}).get("total", 55.0)):
                            from connection_metrics import RequestTrace
                            return await client.request(
                                method, self.worker_origin + path, params=params,
                                content=None if method == "GET" else b"",
                                headers={} if method == "GET" else {"Content-Type": "application/json"},
                                extensions={'trace': RequestTrace(self.metrics, 'worker')})
                    network_task = asyncio.create_task(send_request(), name='worker-api-request')
                    self.gate.network_tasks.add(network_task)
                    response = await network_task
                    retry_after = retry_after_seconds(response.headers.get("Retry-After"))
                    status = response.status_code
                    if status == 429:
                        self.gate.stop_rate_limited()
                    # Any HTTP response from the TLS-verified Worker shows the
                    # proxy transport worked; 403/429/JSON errors do not mark it bad.
                    self.pool.good_response(route)
                    if not 200 <= status < 300:
                        kind = ("access_denied" if status in (401, 403) else
                                "rate_limited" if status == 429 else
                                "temporary_server_failure" if status >= 500 else "http_error")
                        raise WorkerApiError(f"Worker HTTP {status}.", kind=kind, code=status,
                                             retryable=status in (429, 500, 502, 503, 504, 520, 522, 524))
                    try:
                        payload = response.json()
                    except ValueError:
                        raise InvalidWorkerResponse("Worker returned empty or non-JSON content.") from None
                    checked = check_worker_error(payload, operation)
                    if operation == 'profile':
                        try: parse_profile_response(checked, checked.get('data', {}).get('username', params['username']))
                        except PublicProfileRequired: pass
                    else:
                        parse_list_page(checked, operation)
                    success = True
                    self.gate.observe(time.monotonic() - started, self.pool)
                    if retried_404:
                        console("[404 RETRY] Request recovered successfully.")
                    return checked
                except asyncio.CancelledError:
                    cancelled = True
                    if self.gate.rate_limit_event.is_set():
                        failure = WorkerApiError('Request interrupted by the global Worker rate-limit stop.', kind='rate_limited')
                    else:
                        raise
                except self.httpx.ProxyError as exc:
                    failure = self.proxy_exception(exc)
                except self.httpx.ConnectTimeout:
                    failure = WorkerApiError("Connection timed out.", kind="proxy_timeout" if route else "network_timeout", retryable=True)
                except self.httpx.ConnectError:
                    failure = WorkerApiError("Connection or TLS negotiation failed.", kind="proxy_connection_failure" if route else "network_error", retryable=True)
                except (self.httpx.TimeoutException, asyncio.TimeoutError):
                    # A timeout AFTER connection may be the Worker, not a broken
                    # proxy. Retry the SAME route; do not penalize proxy health.
                    failure = WorkerApiError("Worker/transport response timed out.", kind="response_timeout", retryable=True)
                except self.httpx.RequestError:
                    failure = WorkerApiError("Worker transport failed.", kind="network_error", retryable=True)
                except WorkerApiError as exc:
                    failure = exc
                finally:
                    if network_task is not None:
                        self.gate.network_tasks.discard(network_task)
                    if self.gate.stats and attempt_started:
                        self.gate.stats.request_finished(success=success, latency=time.monotonic() - started,
                            kind=failure.kind if failure else None, code=(failure.code if failure and failure.code else status),
                            retry=attempt > 0, cancelled=cancelled)
                    if attempt_started:
                        self.metrics.end('worker', operation, time.monotonic()-started, success=success,
                            records=(1 if operation == 'profile' else sum(member_identity(row) is not None for row in checked.get('users', []))) if success else 0,
                            kind=failure.kind if failure else None, code=failure.code if failure else status,
                            proxy=route.label if route else None, size=len(response.content) if response is not None else 0)
                    if route is not None:
                        route.in_use -= 1
                    if response is not None:
                        await response.aclose()
                    if acquired:
                        connection_slot.release()
                    self.release_client(route)
                if failure is None:
                    raise ExporterError("Request ended without a response.")
                failure.retry_count = attempt
                if failure.kind == 'rate_limited':
                    self.gate.stop_rate_limited('HTTP 429 received from API' if failure.code == 429 else 'Worker reported a rate limit')
                    raise failure
                if failure.kind == "access_denied":
                    self.gate.blocked_reason = self.redactor.text(failure)
                    self.gate.stop_event.set()
                    self.gate.wake_capacity()
                    raise failure
                if failure.retryable:
                    self.gate.observe_failure()
                if route is not None and failure.kind in {
                    "proxy_authentication_failure", "proxy_not_allocated",
                    "proxy_connection_failure", "proxy_timeout", "proxy_temporarily_unavailable",
                }:
                    self.pool.failed(route, failure.kind)
                    switch_route = True
                if failure.code == 404:
                    if retried_404:
                        console("[HTTP_ERROR] Worker HTTP 404 after 1 retry.", error=True)
                        raise failure
                    retried_404 = first_404 = True
                    console(f"[404 RETRY 1/1] {label} - retrying" + (" same cursor" if operation != "profile" else ""))
                elif not failure.retryable:
                    raise failure
                cooldown = max(retry_after or 0.0,
                               min(60.0, 2.0 ** (attempt + 1)) + random.uniform(0.0, 1.0))
                if failure.kind == "proxy_concurrency_limited":
                    self.gate.penalize(cooldown)
                    # Proxy CONNECT concurrency errors are separate from Worker
                    # HTTP 429. Keep the same route; do not evade its limit.
                    console(f"[PROXY LIMIT] {failure}; global cooldown {cooldown:.1f}s.")
                elif failure.kind == "temporary_server_failure":
                    self.gate.cooldown(cooldown)
                last_failure = failure
            if first_404:
                # Retain the same route, cursor, UID and logical page. This is
                # independent of the general transport/server retry budget.
                await self.retry_wait(cooldown)
                continue
            normal_failures += 1
            if normal_failures >= attempts:
                raise last_failure
            choose_route = switch_route  # Worker retries remain on the SAME route.
            console(f"[RETRY {normal_failures}/{attempts - 1}] {failure.kind}; waiting {cooldown:.1f}s.")
            await self.retry_wait(cooldown)
        raise last_failure or ExporterError("Retry budget exhausted.")

    async def download_avatar(self, url: str) -> bytes:
        """Fetch only Worker-supplied CDN images, through the same proxy and pacer.

        CDN errors do not masquerade as Worker failures. No redirects, cookies,
        API calls, response-body logs or route rotation for HTTP restrictions.
        """
        from avatar_cache import allowed_avatar_url, MAX_DOWNLOAD_BYTES
        if not allowed_avatar_url(url):
            return b""
        host = urlsplit(url).hostname
        if host in self.avatar_blocked_hosts:
            return b""
        for attempt in range(2):
            route = None
            borrowed = False
            failure = None
            try:
                async with self.gate.slot():
                    if host in self.avatar_blocked_hosts:
                        return b""
                    route = self.pool.choose()
                    if route is not None:
                        route.in_use += 1
                    try:
                        connection = await self.borrow_client(route)
                        borrowed = True
                        await self.gate.pace()
                        async with asyncio.timeout(12):
                            async with connection.stream("GET", url, timeout=8,
                                    headers={"Accept": "image/*", "Accept-Encoding": "identity"}) as response:
                                self.pool.good_response(route)
                                if response.status_code in (401, 403, 429):
                                    self.avatar_blocked_hosts.add(host)
                                    if response.status_code == 429:
                                        self.gate.cooldown(max(2, retry_after_seconds(response.headers.get("Retry-After")) or 0))
                                if response.status_code != 200:
                                    return b""
                                if not response.headers.get("Content-Type", "").split(";", 1)[0].strip().casefold() in {
                                        "image/jpeg", "image/jpg", "image/png", "image/webp", "image/gif"}:
                                    return b""
                                if response.headers.get("Content-Encoding", "identity").casefold() not in ("", "identity"):
                                    return b""  # Bound wire bytes as well as decoded image pixels.
                                length = response.headers.get("Content-Length")
                                if length and (not length.isdigit() or int(length) > MAX_DOWNLOAD_BYTES):
                                    return b""
                                body = bytearray()
                                async for chunk in response.aiter_bytes(chunk_size=16384):
                                    if len(body) + len(chunk) > MAX_DOWNLOAD_BYTES:
                                        return b""
                                    body.extend(chunk)
                                self.avatar_transport_failures.pop(host, None)
                                return bytes(body)
                    finally:
                        if borrowed:
                            self.release_client(route)
                            borrowed = False
                        if route is not None:
                            route.in_use -= 1
            except self.httpx.ProxyError as exc:
                failure = self.proxy_exception(exc).kind
            except self.httpx.ConnectTimeout:
                failure = "proxy_timeout" if route else "network_timeout"
            except self.httpx.ConnectError:
                failure = "proxy_connection_failure" if route else "network_error"
            except (self.httpx.TimeoutException, asyncio.TimeoutError, self.httpx.RequestError):
                failure = "avatar_transport_error"
            except asyncio.CancelledError:
                raise
            except Exception:
                return b""
            if route is not None and failure in {"proxy_authentication_failure", "proxy_not_allocated",
                    "proxy_connection_failure", "proxy_timeout", "proxy_temporarily_unavailable"}:
                self.pool.failed(route, failure)
                if attempt == 0:
                    continue
            if failure in {"access_denied", "proxy_concurrency_limited"}:
                self.avatar_blocked_hosts.add(host)
                if failure == "proxy_concurrency_limited":
                    self.gate.penalize(5)
            if failure in {"network_error", "network_timeout", "avatar_transport_error"}:
                # An offline CDN must not add a timeout to thousands of profiles.
                self.avatar_transport_failures[host] = self.avatar_transport_failures.get(host, 0) + 1
                if self.avatar_transport_failures[host] >= 3:
                    self.avatar_blocked_hosts.add(host)
            return b""
        return b""

    async def save_raw(self, username: str, name: str, payload: dict[str, Any]) -> None:
        if self.raw_directory is not None:
            safe_name = parse_username(username)
            await disk_call(atomic_write_json, self.raw_directory / f"profile_{safe_name}" / f"{name}.json",
                            payload, compact=True, redactor=self.redactor)

    async def lookup_profile(self, username: str) -> "ProfileSnapshot":
        key = username.casefold()
        if key in self.profile_cache:
            self.profile_cache.move_to_end(key)
            return self.profile_cache[key]
        payload = await self.request_json("profile", {"username": username})
        await self.save_raw(username, "profile", payload)
        try:
            profile = parse_profile_response(payload, username)
        except PublicProfileRequired as exc:
            data = payload.get("data", {})
            exc.profile_metadata = {"uid":numeric_uid(data.get("userid")), "display_name":data.get("fullname", ""),
                "username":username, "avatar_url":data.get("profile", ""),
                "follower_count":parse_count(data.get("followers")), "following_count":parse_count(data.get("following")),
                "advertised_counts":{"followers":data.get("followers"),"following":data.get("following")}}
            if self.avatars is not None:
                await self.avatars.ensure(exc.profile_metadata["uid"], username, data.get("profile"))
            raise
        profile.metadata["lookup_retry_count"] = self.request_retries.get()
        if username.casefold() == self.settings.get('target_username', '').casefold():
            self.settings['resolved_target_uid'] = profile.uid
        self.profile_cache[key] = profile
        if len(self.profile_cache) > max(256, self.gate.ceiling):
            self.profile_cache.popitem(last=False)
        return profile

    async def fetch_page(self, profile: "ProfileSnapshot", list_name: str,
                         cursor: str, page_number: int) -> "BatchResponse":
        payload = await self.request_json(list_name, {"Uid": profile.uid, "minCursor": cursor},
                                          context=f"@{profile.username} {list_name.upper()} page {page_number}")
        await self.save_raw(profile.username, f"{list_name}_{page_number:06d}", payload)
        batch = parse_list_page(payload, list_name)
        batch.retry_count = self.request_retries.get()
        return batch


class SecretLogFilter(logging.Filter):
    def __init__(self, redactor: CredentialRedactor) -> None:
        super().__init__()
        self.redactor = redactor

    def filter(self, record: logging.LogRecord) -> bool:
        # Discard credential-bearing dependency logs rather than risk redacting
        # only one representation of a CONNECT auth header.
        return False


def parse_count(value: Any) -> int | None:
    """Return only exact counts. '28.5M' is preserved separately, never an exact total."""
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
        return int(value.strip())
    return None


def approximate_count(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([KMB])", value.strip(), re.I)
    if match:
        return int(Decimal(match[1]) * {"K": 1000, "M": 1000000, "B": 1000000000}[match[2].upper()])
    return None


def worker_verified(value: Any) -> bool | None:
    return {"Yes✅": True, "No❌": False}.get(value) if isinstance(value, str) else None


def parse_profile_response(payload: Any, requested_username: str) -> "ProfileSnapshot":
    root = check_worker_error(payload, "profile")
    data = root.get("data")
    if not isinstance(data, dict):
        raise InvalidWorkerResponse("Profile data must be an object.")
    username = data.get("username")
    if not isinstance(username, str) or not USERNAME_RE.fullmatch(username):
        raise InvalidWorkerResponse("Profile data.username is missing or invalid.")
    if username.casefold() != requested_username.casefold():
        raise InvalidWorkerResponse("Worker returned a different username than requested.")
    privacy = data.get("private")
    if privacy == "Private Account":
        raise PublicProfileRequired(f"@{username} is private; list scanning skipped.")
    if privacy != "Public Account":
        raise InvalidWorkerResponse("Unknown profile privacy value; refusing to assume public.")
    raw_uid = data.get("userid")
    if isinstance(raw_uid, bool) or not isinstance(raw_uid, (str, int)):
        raise InvalidWorkerResponse("Profile data.userid must be a numeric string or integer.")
    uid = str(raw_uid)
    if not re.fullmatch(r"[0-9]+", uid) or int(uid) <= 0:
        raise InvalidWorkerResponse("Profile data.userid is not a usable numeric UID.")
    return ProfileSnapshot(
        username=username, uid=uid,
        display_name=data.get("fullname") if isinstance(data.get("fullname"), str) else username,
        sec_uid=data.get("secUid") if isinstance(data.get("secUid"), str) else "",
        profile_url=f"{TIKTOK_ORIGIN}/@{username}", private_account=False,
        verified=worker_verified(data.get("verified")),
        avatar_url=data.get("profile") if isinstance(data.get("profile"), str) else "",
        follower_count=parse_count(data.get("followers")),
        following_count=parse_count(data.get("following")),
        likes_count=parse_count(data.get("likes")), video_count=parse_count(data.get("video")),
        advertised_counts={name: data.get(field) for name, field in
                           (("followers", "followers"), ("following", "following"),
                            ("likes", "likes"), ("videos", "video"))},
        following_visible={"Yes": True, "No": False}.get(str(data.get("see_following"))),
        metadata={key: data[key] for key in (
            "region", "created_time", "last_change_name", "language",
            "open_favorite", "see_following", "private", "heart_count",
        ) if key in data},
        raw_user=data if KEEP_RAW_MEMBER_DATA else {},
    )


def parse_has_more(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.casefold() in ("true", "false"):
        return value.casefold() == "true"
    raise InvalidWorkerResponse("List hasMore must be a boolean, 0/1, or True/False string.")


def parse_list_page(payload: Any, list_name: str) -> "BatchResponse":
    data = check_worker_error(payload, list_name)
    records = data.get("users")
    if not isinstance(records, list):
        raise InvalidWorkerResponse(f"{list_name}: missing users array.")
    has_more = parse_has_more(data.get("hasMore"))
    cursor = data.get("minCursor")
    if cursor is None:
        cursor = ""
    elif isinstance(cursor, bool) or not isinstance(cursor, (str, int)):
        raise InvalidWorkerResponse(f"{list_name}: invalid minCursor type.")
    else:
        cursor = str(cursor)
    # Missing/stalled cursor is handled after saving this page, so a match on
    # an otherwise useful page is not lost merely because pagination is broken.
    return BatchResponse(records=records, has_more=has_more, min_cursor=cursor)


def member_identity(entry: Any) -> tuple[str, str, str] | None:
    """Validate identity without building another normalized record for metrics."""
    if not isinstance(entry, dict):
        return None
    username = entry.get("uniqueId")
    username = username if isinstance(username, str) and USERNAME_RE.fullmatch(username) else ""
    uid = entry.get("user_id")
    uid = str(uid) if isinstance(uid, (str, int)) and not isinstance(uid, bool) else ""
    uid = uid if re.fullmatch(r"[0-9]+", uid) and int(uid) > 0 else ""
    if not uid and not username:
        return None
    key = f"id:{uid}" if uid else f"username:{username.casefold()}"
    return key, uid, username


def normalize_member(entry: Any) -> tuple[str, dict[str, Any]] | None:
    identity = member_identity(entry)
    if identity is None:
        return None
    key, uid, username = identity
    normalized = {
        "id": uid, "username": username,
        "display_name": entry.get("nickname") if isinstance(entry.get("nickname"), str) else "",
        "profile_url": f"{TIKTOK_ORIGIN}/@{username}" if username else "",
        "bio": entry.get("signature") if isinstance(entry.get("signature"), str) else "",
        "avatar_url": entry.get("avatarThumb") if isinstance(entry.get("avatarThumb"), str) else "",
        # Keep the Worker's display value exactly; expose a separate boolean
        # for consumers that need a verification predicate.
        "verified": entry.get("verified") if isinstance(entry.get("verified"), str) else None,
        "is_verified": worker_verified(entry.get("verified")),
        "private_account": entry.get("privateAccount") if isinstance(entry.get("privateAccount"), bool) else None,
    }
    if KEEP_RAW_MEMBER_DATA:
        normalized["raw"] = entry
    for name in ('secUid', 'followers', 'following', 'videoCount', 'heartCount'):
        if name in entry:
            normalized[name] = entry[name]
    return key, normalized


def finish_list_result(result: ListExportResult, profile: ProfileSnapshot | dict[str, Any]) -> None:
    counts = profile.advertised_counts if isinstance(profile, ProfileSnapshot) else profile.get("advertised_counts", {})
    result.advertised_count = counts.get(result.list_name) if isinstance(counts, dict) else None
    result.finish()
    estimated = approximate_count(result.advertised_count)
    if result.complete and estimated is not None:
        warning = f"Advertised count {result.advertised_count} is rounded, not an exact total."
        if warning not in result.warnings: result.warnings.append(warning)
        result.count_difference = result.unique_records_saved - estimated
        if result.count_difference:
            result.stop_reason = result.status = "complete_count_mismatch"



async def replay_committed_list(store: "UserStore", list_name: str, *, target_username: str, target_uid: str | None = None,
                               on_target_match: Callable[..., Any] | None = None,
                               on_discovery: Callable[..., Any] | None = None) -> bool:
    """Recover secondary effects using committed data, including partial pages."""
    target_reported = False
    iterator = store.iter_members(list_name)
    while True:
        recovered = await disk_call(lambda: list(itertools.islice(iterator, 500)))
        if not recovered:
            break
        for member in recovered:
            if (not target_reported and (member['username'].casefold() == target_username.casefold() or bool(target_uid and member.get('id') == target_uid))
                    and on_target_match is not None):
                await on_target_match(list_name, member)
                target_reported = True
        if on_discovery:
            await on_discovery(list_name, recovered)
    return target_reported


async def export_one_list(
    client: WorkerApiClient, store: "UserStore", profile: "ProfileSnapshot", list_name: str,
    *, target_username: str,
    on_target_match: Callable[..., Any] | None = None,
    on_discovery: Callable[..., Any] | None = None,
    stop_event: asyncio.Event,
    replay_saved: bool = True,
) -> "ListExportResult":
    """Commit each page's members and returned cursor in ONE SQLite transaction."""
    count = profile.follower_count if list_name == "followers" else profile.following_count
    saved = await disk_call(store.checkpoint, list_name)
    result = ListExportResult(**saved["result"]) if saved else ListExportResult(list_name, count)
    cursor = saved["next_cursor"] if saved else "0"
    duplicate_only_pages = saved.get("duplicate_only_pages", 0) if saved else 0
    target_reported = False
    # A crash can occur after the page commit but before secondary JSON/queue writes.
    # Replay only local observations; never refetch a committed page.
    if saved:
        if replay_saved:
            target_reported = await replay_committed_list(store, list_name, target_username=target_username, target_uid=getattr(client, 'settings', {}).get('resolved_target_uid'),
                on_target_match=on_target_match, on_discovery=on_discovery)
        if result.endpoint_exhausted:
            finish_list_result(result, profile)  # Upgrade old count-only incomplete checkpoints.
            await disk_call(store.save_checkpoint, list_name, {**saved, "result": asdict(result)})
            return result
        if explicit_restriction(asdict(result)):
            return result  # Visibility is terminal; do not call a hidden endpoint.
        if result.stop_reason in {"cursor_repeated", "cursor_stalled", "missing_cursor",
                                  "empty_page_with_has_more", "duplicate_page_stall"}:
            return result  # The server must supply a usable continuation; never invent one.
        result.error = None
        result.stop_reason = "resuming"
    if list_name == "following" and profile.following_visible is False:
        result.stop_reason = "list_restricted"
        result.error = "Worker profile explicitly reports see_following=No."
        result.visibility_reason = result.error
        result.see_following = "No"
        finish_list_result(result, profile)
        await disk_call(store.save_checkpoint, list_name, {"result": asdict(result), "next_cursor": cursor})
        return result
    try:
        if hasattr(client, 'prepare_chain'):
            cursor = await client.prepare_chain(store, result, cursor)
        if await disk_call(store.cursor_seen, list_name, cursor):
            raise WorkerApiError("Worker repeated an earlier cursor.", kind="cursor_repeated")
        while True:
            if stop_event.is_set():
                raise ScanCancelled()
            if hasattr(client, 'fetch_chain_page'):
                batch, cursor, restarted = await client.fetch_chain_page(store, result, profile, list_name, cursor)
                if restarted:
                    duplicate_only_pages = 0
            else:
                batch = await client.fetch_page(profile, list_name, cursor, result.batches + 1)
            inserted, cursor, duplicate_only_pages, target_match, normalized = await disk_call(
                store.commit_page, list_name, batch, result, profile, cursor, duplicate_only_pages, target_username, target_uid=getattr(client, 'settings', {}).get('resolved_target_uid'))
            if target_match is not None and not target_reported:
                if on_target_match is not None:
                    await on_target_match(list_name, target_match)
                target_reported = True
            if on_discovery:
                await on_discovery(list_name, normalized)
            if getattr(client, "bootstrap_mode", False):
                advertised = profile.advertised_counts.get(list_name)
                console(f"[BOOTSTRAP {list_name.upper()}] Collected {result.unique_records_saved:,} accounts | "
                        f"Advertised: {advertised if advertised is not None else 'unknown'} (snapshot) | "
                        f"Page {result.batches} | has_more={batch.has_more}")
            console(f"[@{profile.username}] [{list_name.upper()}] Page {result.batches}: "
                    f"received {len(batch.records)}, +{inserted} unique, "
                    f"total {result.unique_records_saved}, has_more={batch.has_more}")
            if result.endpoint_exhausted or result.error:
                break
    except asyncio.CancelledError:
        stop_event.set()
        # The writer may have committed just as cancellation was delivered.
        # Re-read its exact cursor; never replace it with a stale local cursor.
        committed = await disk_call(store.checkpoint, list_name)
        if committed:
            result = ListExportResult(**committed['result'])
            cursor = committed['next_cursor']
            duplicate_only_pages = committed.get('duplicate_only_pages', 0)
        if not result.endpoint_exhausted:
            limited = bool(getattr(getattr(client, 'gate', None), 'rate_limit_exhausted', False))
            result.stop_reason = 'cancelled'
            result.error = 'Request interrupted by the global Worker rate-limit stop.' if limited else 'Scan cancelled by the user.'
    except WorkerApiError as exc:
        if hasattr(client, 'prepare_chain'):
            committed = await disk_call(store.checkpoint, list_name)
            if committed:
                cursor = committed['next_cursor']
        result.stop_reason, result.error = exc.kind, REDACTOR.text(exc)
        result.backend_failure = getattr(exc, 'backend_failure', result.backend_failure)
        result.http_code = exc.code if isinstance(exc.code, int) else None
        result.retry_count += getattr(exc, "retry_count", 0)
    finish_list_result(result, profile)
    await disk_call(store.save_checkpoint, list_name, {"result": asdict(result), "next_cursor": cursor,
                                     "duplicate_only_pages": duplicate_only_pages})
    return result


class UserStore:
    def __init__(self, path: Path) -> None:
        # One owner per profile. Only its awaited final writer may access this
        # connection from another thread; page writes and export never overlap.
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA temp_store=MEMORY")
        # Create a new profile's schema in one durable transaction rather than
        # flushing each CREATE separately. Existing databases are unchanged.
        self.connection.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS members (
                list_name TEXT NOT NULL,
                member_key TEXT NOT NULL,
                list_position INTEGER NOT NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (list_name, member_key)
            );
            CREATE TABLE IF NOT EXISTS checkpoint (name TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS page_cursors (name TEXT, cursor TEXT, PRIMARY KEY(name,cursor));
            COMMIT;
            """
        )
        self._counts: dict[str, int] = {}
        self.last_members: list[dict[str, Any]] = []

    def restart_chain(self, result, backend):
        """Keep observations/matches; replace only the incompatible cursor chain."""
        result.backend = backend
        result.cursor_chain_restarts += 1
        result.endpoint_exhausted = result.complete = result.usable = False
        result.error = None
        result.stop_reason = 'backend_restart'
        result.final_min_cursor = '0'
        with self.connection:
            self.connection.execute('DELETE FROM page_cursors WHERE name=?', (result.list_name,))
            self.save_checkpoint(result.list_name, {'result': asdict(result), 'next_cursor': '0',
                                 'duplicate_only_pages': 0}, commit=False)

    def commit_page(self, name, batch, result, profile, cursor, duplicate_pages, target, *, target_uid=None):
        try:
            self.connection.execute('BEGIN IMMEDIATE')
            inserted, duplicates, invalid, match = self.save_batch(name, batch.records, target_username=target, target_uid=target_uid, commit=False)
            result.retry_count += batch.retry_count
            result.batches += 1
            result.raw_records_received += len(batch.records)
            result.unique_records_saved += inserted
            result.duplicates_ignored += duplicates
            result.invalid_records_ignored += invalid
            duplicate_pages = duplicate_pages + 1 if batch.records and not inserted else 0
            following = batch.min_cursor
            result.final_min_cursor = following
            if not batch.has_more:
                result.endpoint_exhausted = True
                result.stop_reason = 'endpoint_exhausted'
                finish_list_result(result, profile)
            elif not batch.records:
                result.stop_reason, result.error = 'empty_page_with_has_more', 'Empty page with hasMore=true.'
            elif not following:
                result.stop_reason, result.error = 'missing_cursor', 'hasMore=true but minCursor is missing.'
            elif following == cursor:
                result.stop_reason, result.error = 'cursor_stalled', 'Worker cursor did not advance.'
            elif self.cursor_seen(name, following):
                result.stop_reason, result.error = 'cursor_repeated', 'Worker repeated an earlier cursor.'
            elif duplicate_pages >= 3 and not result.cursor_chain_restarts:
                result.stop_reason, result.error = 'duplicate_page_stall', 'Three pages contained no new accounts.'
            self.connection.execute('INSERT INTO page_cursors VALUES (?,?)', (name, cursor))
            self.save_checkpoint(name, {'result': asdict(result), 'next_cursor': following,
                                        'duplicate_only_pages': duplicate_pages}, commit=False)
            self.connection.commit()
            return inserted, following, duplicate_pages, match, self.last_members
        except BaseException:
            self.connection.rollback()
            self._counts.clear()
            raise

    def checkpoint(self, name: str) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT payload FROM checkpoint WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else None

    def save_checkpoint(self, name: str, payload: Any, *, commit: bool = True) -> None:
        encoded = json.dumps(REDACTOR.clean(payload), ensure_ascii=False, separators=(",", ":"))
        # A completed page already saved this checkpoint atomically with its
        # members. The final list pass need not write the identical row again.
        self.connection.execute("INSERT INTO checkpoint VALUES (?,?) "
                                "ON CONFLICT(name) DO UPDATE SET payload=excluded.payload "
                                "WHERE checkpoint.payload<>excluded.payload", (name, encoded))
        if commit:
            self.connection.commit()

    def cursor_seen(self, name: str, cursor: str) -> bool:
        return self.connection.execute("SELECT 1 FROM page_cursors WHERE name=? AND cursor=?",
                                       (name, cursor)).fetchone() is not None

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "UserStore":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def count(self, list_name: str) -> int:
        if list_name in self._counts:
            return self._counts[list_name]
        row = self.connection.execute(
            "SELECT COUNT(*) FROM members WHERE list_name = ?", (list_name,)
        ).fetchone()
        self._counts[list_name] = int(row[0]) if row else 0
        return self._counts[list_name]

    def save_batch(
        self,
        list_name: str,
        entries: Iterable[dict[str, Any]],
        *,
        target_username: str | None = None,
        target_uid: str | None = None,
        commit: bool = True,
    ) -> tuple[int, int, int, dict[str, Any] | None]:
        starting_count = self.count(list_name)
        inserted = 0
        duplicates = 0
        invalid = 0
        target_match: dict[str, Any] | None = None
        self.last_members = []
        wanted = target_username.casefold() if target_username else None
        for entry in entries:
            normalized = normalize_member(entry)
            if normalized is None:
                invalid += 1
                continue
            member_key, payload = normalized
            payload = REDACTOR.clean(payload)  # Persistence, after schema parsing.
            self.last_members.append(payload)
            if (
                wanted is not None
                and (str(payload.get("username") or "").casefold() == wanted or bool(target_uid and payload.get("id") == target_uid))
                and target_match is None
            ):
                target_match = payload
            position = starting_count + inserted + 1
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO members
                    (list_name, member_key, list_position, payload)
                VALUES (?, ?, ?, ?)
                """,
                (
                    list_name,
                    member_key,
                    position,
                    json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                ),
            )
            if cursor.rowcount == 1:
                inserted += 1
            else:
                duplicates += 1
                if wanted and (str(payload.get("username", "")).casefold() == wanted or bool(target_uid and payload.get("id") == target_uid)):
                    # A stable UID may have appeared earlier under an old name.
                    # Keep the confirmed target name in its one committed record
                    # so local export replay cannot erase that observation.
                    self.connection.execute("UPDATE members SET payload=? WHERE list_name=? AND member_key=?",
                        (json.dumps(payload,ensure_ascii=False,separators=(",", ":")),list_name,member_key))
        if commit:
            self.connection.commit()
        self._counts[list_name] = starting_count + inserted
        return inserted, duplicates, invalid, target_match

    def iter_encoded_members(self, list_name: str):
        # Payloads were normalized/redacted before their durable commit. SQLite
        # inserts the position without decoding and rebuilding each object in Python.
        for (payload,) in self.connection.execute(
                "SELECT json_set(payload,'$.list_position',list_position) FROM members WHERE list_name=? ORDER BY list_position", (list_name,)):
            yield payload

    def iter_members(self, list_name: str) -> Iterator[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT list_position, payload
            FROM members
            WHERE list_name = ?
            ORDER BY list_position
            """,
            (list_name,),
        )
        for position, payload in rows:
            member = json.loads(payload)
            member["list_position"] = int(position)
            yield member


def write_json_property(stream: Any, name: str, value: Any, *, comma: bool = True) -> None:
    stream.write(f"  {json.dumps(name)}: ")
    stream.write(json.dumps(REDACTOR.clean(value), ensure_ascii=False, indent=2))
    stream.write(",\n" if comma else "\n")


def write_final_json(
    output_path: Path,
    *,
    profile: ProfileSnapshot,
    selected_lists: list[str],
    results: dict[str, ListExportResult],
    store: UserStore,
    started_at: str,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".part")
    overall_complete = all(results[name].complete for name in selected_lists)
    profile_payload = asdict(profile)
    if not KEEP_RAW_MEMBER_DATA:
        profile_payload.pop("raw_user", None)
    try:
        with temporary_path.open("w", encoding="utf-8", newline="\n") as stream:
            stream.write("{\n")
            write_json_property(stream, "schema_version", 4)
            direct_used = profile.metadata.get('backend') == 'direct' or any(r.backend == 'direct' for r in results.values())
            write_json_property(stream, "source", "tiktok_hybrid_api" if direct_used else "fintok_worker_api")
            write_json_property(stream, "started_at_utc", started_at)
            write_json_property(stream, "completed_at_utc", utc_iso())
            write_json_property(stream, "requested_lists", selected_lists)
            write_json_property(stream, "complete", overall_complete)
            write_json_property(stream, "profile", profile_payload)
            write_json_property(
                stream,
                "list_results",
                {name: asdict(results[name]) for name in selected_lists},
            )
            write_json_property(
                stream,
                "notes",
                [
                    "Backend provenance is stored with the profile and each pagination chain.",
                    "Pagination follows exact returned opaque cursors; no artificial page cap.",
                    "complete=false can mean a failed, stalled, restricted or malformed list, with genuine collection failures.",
                    "Rounded advertised counts are retained as text, not treated as exact totals.",
                    "Target observations survive incomplete scans in sucess_find.json.",
                ],
            )
            for index, list_name in enumerate(("followers", "following")):
                stream.write(f"  {json.dumps(list_name)}: [")
                if list_name in selected_lists:
                    first = True
                    for member in store.iter_encoded_members(list_name):
                        stream.write("\n    " if first else ",\n    ")
                        stream.write(member)
                        first = False
                    if not first:
                        stream.write("\n  ")
                stream.write("]")
                stream.write(",\n" if index == 0 else "\n")
            stream.write("}\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, output_path)
        fsync_directory(output_path.parent)
    except BaseException:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def inspect_reusable_export(
    path: Path,
    *,
    expected_username: str,
    target_username: str,
    target_uid: str | None = None,
) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        with path.open("r", encoding="utf-8") as stream:
            document = json.load(stream)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(document, dict):
        return None
    if document.get("schema_version") != 4 or document.get("source") not in {"fintok_worker_api", "tiktok_hybrid_api"}:
        return None
    profile = document.get("profile")
    if not isinstance(profile, dict):
        return None
    saved_username = str(profile.get("username") or "")
    if saved_username.casefold() != expected_username.casefold():
        return None
    requested = document.get("requested_lists")
    if (not isinstance(requested, list) or not all(isinstance(name, str) for name in requested)
            or not set(SELECTED_LISTS).issubset(requested)):
        return None

    migrated = False
    if not isinstance(document.get("list_results"), dict): return None
    for name, info in document["list_results"].items():
        if (isinstance(info, dict) and info.get("endpoint_exhausted") is True
                and not info.get("error") and not info.get("invalid_records_ignored")
                and info.get("stop_reason") in {"endpoint_exhausted", "count_discrepancy"}):
            try:
                updated = ListExportResult(**{k:v for k,v in info.items() if k in ListExportResult.__dataclass_fields__})
                finish_list_result(updated, profile)
            except (TypeError, ValueError):
                return None
            document["list_results"][name] = asdict(updated)
            migrated = True
    matches: dict[str, dict[str, Any]] = {}
    wanted = target_username.casefold()
    for list_name in SELECTED_LISTS:
        members = document.get(list_name)
        if not isinstance(members, list):
            return None
        result = document.get("list_results", {})
        if not isinstance(result, dict) or not isinstance(result.get(list_name), dict):
            return None
        result = result[list_name]
        if result.get("unique_records_saved") != len(members):
            return None
        if not ((result.get("complete") is True and result.get("endpoint_exhausted") is True
                 and not result.get("error") and not result.get("invalid_records_ignored")) or explicit_restriction(result)):
            return None
        if any(not isinstance(member, dict)
               or not isinstance(member.get("username"), str)
               or not isinstance(member.get("id"), str)
               or not (member.get("id") or member.get("username"))
               for member in members):
            return None
        for member in members:
            if not isinstance(member, dict):
                continue
            if str(member.get("username") or "").casefold() == wanted or bool(target_uid and member.get("id") == target_uid):
                matches[list_name] = member
                break
    complete = all(document["list_results"][name].get("complete") is True for name in SELECTED_LISTS)
    if migrated:
        document["complete"] = complete
        atomic_write_json(path, document)
    list_results = document.get("list_results")
    return {
        "complete": complete,
        "status": "resumed_complete" if complete else "restricted",
        "profile_uid": numeric_uid(profile.get("uid")),
        'profile_metadata': {k:v for k,v in profile.items() if k != 'raw_user'},
        "members": {name: document[name] for name in SELECTED_LISTS},
        "matches": matches,
        "list_results": list_results if isinstance(list_results, dict) else {},
        "observed_at": str(document.get("completed_at_utc") or document.get("started_at_utc") or "unknown"),
    }


def error_profile_result(
    job: ProfileJob,
    *,
    status: str,
    started_at_utc: str,
    error: str,
    export_path: Path | None = None,
) -> ProfileProcessResult:
    return ProfileProcessResult(
        username=job.username,
        profile_url=job.profile_url,
        source_lists=list(job.source_lists),
        status=status,
        export_path=str(export_path) if export_path is not None else None,
        complete=False,
        target_found_in=[],
        started_at_utc=started_at_utc,
        completed_at_utc=utc_iso(),
        list_results={},
        error=error,
    )


def numeric_uid(value: Any) -> str:
    text = str(value) if isinstance(value, (str, int)) and not isinstance(value, bool) else ""
    return text if re.fullmatch(r"[0-9]+", text) and int(text) > 0 else ""


def size_skip_result(job: ProfileJob, profile: ProfileSnapshot, config: dict[str, Any], started: str) -> ProfileProcessResult | None:
    counts: dict[str, Any] = {}
    exceeded: list[str] = []
    unknown: list[str] = []
    for name, exact, setting in (("followers", profile.follower_count, "follower_skip_limit"),
                                  ("following", profile.following_count, "following_skip_limit")):
        raw = profile.advertised_counts.get(name)
        value = exact if exact is not None else approximate_count(raw)
        limit = config.get(setting, 0)
        counts[name] = {"raw": raw, "value": value, "exact": exact is not None, "limit": limit}
        if limit and value is None:
            unknown.append(name)
        elif limit and value > limit:
            qualifier = "approximately " if exact is None else ""
            exceeded.append(f"{name.capitalize()}={qualifier}{value:,} exceeds {limit:,}")
    if not exceeded and not unknown:
        return None
    reason = "; ".join(exceeded) if exceeded else "Cannot verify configured size limit: " + ", ".join(unknown)
    status = "skipped_size" if exceeded else "skipped_unknown_size"
    result = error_profile_result(job, status=status, started_at_utc=started, error=reason)
    result.metadata = {"user_id": profile.uid, "counts": counts, "limit_exceeded": exceeded,
                       "reason": reason, "timestamp": utc_iso()}
    console(f"[@{job.username}] [{status.upper()}] {reason}. "
            f"Followers={profile.advertised_counts.get('followers')}; Following={profile.advertised_counts.get('following')}.")
    return result


async def process_profile(
    client: WorkerApiClient,
    job: ProfileJob,
    *,
    output_directory: Path,
    success_recorder: SuccessRecorder,
    stop_event: asyncio.Event,
    use_resume: bool,
    store_directory: Path | None = None,
    on_discovery: Callable[..., Any] | None = None,
    config: dict[str, Any] | None = None,
    on_profile: Callable[..., Any] | None = None,
) -> ProfileProcessResult:
    started_at = utc_iso()
    target_username = success_recorder.target_user
    final_path = output_file_path(output_directory, job.username, SELECTED_LISTS)
    current_matches: dict[str, dict[str, Any]] = {}

    async def target_callback(list_name: str, target_record: dict[str, Any]) -> None:
        current_matches[list_name] = target_record
        await disk_call(success_recorder.record_match, job, list_name, target_record)

    if use_resume:
        reusable = await disk_call(inspect_reusable_export,
            final_path,
            expected_username=job.username,
            target_username=target_username, target_uid=getattr(client, 'settings', {}).get('resolved_target_uid'),
        )
        if reusable is not None:
            if on_discovery:
                for name, members in reusable["members"].items():
                    await on_discovery(name, members)
            await disk_call(success_recorder.reconcile_complete_lists, job, reusable["matches"],
                            [name for name, info in reusable["list_results"].items() if info.get("complete")])
            for list_name, target_record in reusable["matches"].items():
                await disk_call(success_recorder.record_match, job, list_name, target_record, observed_at=reusable["observed_at"])
            console(f"[@{job.username}] [RESUME] Reused saved terminal export ({reusable['status']}).")
            return ProfileProcessResult(
                username=job.username,
                profile_url=job.profile_url,
                source_lists=list(job.source_lists),
                status=reusable["status"],
                export_path=str(final_path),
                complete=reusable["complete"],
                target_found_in=success_recorder.found_lists_for(job.username),
                started_at_utc=started_at,
                completed_at_utc=utc_iso(),
                list_results=reusable["list_results"],
            )

    if stop_event.is_set():
        return error_profile_result(
            job,
            status="cancelled",
            started_at_utc=started_at,
            error="Scan stopped before this profile began.",
        )

    if store_directory is not None:
        await disk_call(store_directory.mkdir, parents=True, exist_ok=True)
    # Even the optional nonpersistent API path keeps its temporary files inside
    # this search. The CLI uses durable page stores in the same search folder.
    temporary_directory = output_directory / "tmp"
    if store_directory is None:
        temporary_directory.mkdir(parents=True, exist_ok=True)
    context = (nullcontext(str(store_directory)) if store_directory is not None
               else tempfile.TemporaryDirectory(prefix="profile_", dir=temporary_directory))
    with context as temp_directory:
        database_path = Path(temp_directory) / f"{sanitize_run_name(job.username)}.sqlite3"
        async with open_user_store(database_path) as store:
            saved_profile = await disk_call(store.checkpoint, "profile")
            if saved_profile:
                profile = ProfileSnapshot(**saved_profile)
                console(f"[@{job.username}] [RESUME] Using saved profile and page checkpoints.")
            else:
                console(f"[@{job.username}] [CHECK] Querying profile API ...")
                profile = await client.lookup_profile(job.username)
                await disk_call(store.save_checkpoint, "profile", asdict(profile))
                console(f"[@{job.username}] [CHECK] Public profile confirmed: UID={profile.uid}")
            # Page commits precede the success/queue JSON writes. Recover those
            # effects BEFORE duplicate UID or changed size-limit early returns.
            for name in SELECTED_LISTS:
                await replay_committed_list(store, name, target_username=target_username, target_uid=getattr(client, 'settings', {}).get('resolved_target_uid'),
                    on_target_match=target_callback, on_discovery=on_discovery)
            if getattr(client, "avatars", None) is not None:
                await client.avatars.ensure(profile.uid, job.username, profile.avatar_url)
            if on_profile is not None and not await on_profile(profile):
                return error_profile_result(job, status="skipped_duplicate", started_at_utc=started_at,
                                            error=f"Numeric UID {profile.uid} already belongs to another queued/processed profile.")
            skipped = size_skip_result(job, profile, config or {}, started_at)
            if skipped is not None:
                await disk_call(atomic_write_json, output_directory / "skipped" / f"{sanitize_run_name(job.username)}.json", asdict(skipped))
                return skipped
            results: dict[str, ListExportResult] = {}

            for list_name in SELECTED_LISTS:
                if stop_event.is_set():
                    saved_list = await disk_call(store.checkpoint, list_name)
                    if saved_list:
                        # Preserve complete data in a direction not revisited at shutdown.
                        results[list_name] = ListExportResult(**saved_list['result'])
                        continue
                    profile_count = (
                        profile.follower_count
                        if list_name == "followers"
                        else profile.following_count
                    )
                    cancelled = ListExportResult(
                        list_name,
                        profile_count,
                    )
                    cancelled.stop_reason = "cancelled"
                    cancelled.error = "Scan stopped before this list began."
                    cancelled.finish()
                    results[list_name] = cancelled
                    continue

                results[list_name] = await export_one_list(
                    client,
                    store,
                    profile,
                    list_name,
                    target_username=target_username,
                    on_target_match=target_callback,
                    on_discovery=on_discovery,
                    stop_event=stop_event,
                    replay_saved=False,
                )
                list_result = results[list_name]
                if list_result.complete and list_result.warnings:
                    console(f"[@{job.username}] [{list_name.upper()} WARNING] "
                            f"advertised={list_result.advertised_count}, returned={list_result.unique_records_saved}; "
                            f"{list_result.stop_reason}; usable=yes.")
                if explicit_restriction(asdict(list_result)):
                    console(f"[@{job.username}] [{list_name.upper()}] Unavailable: list_restricted; {list_result.error}")
                elif not list_result.complete:
                    console(f"[@{job.username}] [{list_name.upper()}] Incomplete: "
                            f"{list_result.stop_reason}; {list_result.error or '; '.join(list_result.warnings)}")

            await disk_call(write_final_json,
                final_path,
                profile=profile,
                selected_lists=list(SELECTED_LISTS),
                results=results,
                store=store,
                started_at=started_at,
            )

    serialized_results = {
        name: asdict(result) for name, result in results.items()
    }
    complete = all(results[name].complete for name in SELECTED_LISTS)
    await disk_call(success_recorder.reconcile_complete_lists,
                    job, current_matches, [name for name, result in results.items() if result.complete])
    cancelled = any(result.stop_reason in {'cancelled', 'rate_limited'} for result in results.values())
    cancelled = cancelled or (stop_event.is_set() and not complete and not restricted_terminal(serialized_results))
    list_errors = [result.error for result in results.values() if result.error]
    if cancelled:
        status = "cancelled"
    elif complete:
        status = "complete"
    elif restricted_terminal(serialized_results):
        status = "restricted"
        console(f"[@{job.username}] [RESTRICTED] Accessible relationship data saved; hidden direction remains unknown.")
    else:
        status = "partial"

    processed_result = ProfileProcessResult(
        username=job.username,
        profile_url=profile.profile_url,
        source_lists=list(job.source_lists),
        status=status,
        export_path=str(final_path),
        complete=complete,
        target_found_in=success_recorder.found_lists_for(job.username),
        started_at_utc=started_at,
        completed_at_utc=utc_iso(),
        list_results=serialized_results,
        error="; ".join(dict.fromkeys(list_errors)) if list_errors else None,
        metadata={"user_id":profile.uid,"profile_retries":profile.metadata.get("lookup_retry_count",0),
                  "profile": {k:v for k,v in asdict(profile).items() if k != 'raw_user'}},
    )
    processed_result._export_committed = True  # In-memory proof, never accepted from JSON.
    return processed_result


@asynccontextmanager
async def open_user_store(path: Path):
    store = await disk_call(UserStore, path)
    try:
        yield store
    finally:
        await disk_call(store.close)


class SharedScanControl:
    def __init__(self) -> None:
        self.stop_event = asyncio.Event()
        self.fatal_error: str | None = None

    def stop_with_fatal_error(self, message: str) -> None:
        if self.fatal_error is None:
            self.fatal_error = REDACTOR.text(message)
        self.stop_event.set()


async def worker_loop(
    worker_number: int, jobs: asyncio.Queue, *, client: WorkerApiClient,
    output_directory: Path, success_recorder: SuccessRecorder,
    state_recorder: ScanStateRecorder, control: SharedScanControl, use_resume: bool,
) -> None:
    while not control.stop_event.is_set():
        durable = isinstance(state_recorder, DurableScanState)
        try:
            job = await disk_call(state_recorder.claim) if durable else jobs.get_nowait()
            if job is None:
                return
        except asyncio.QueueEmpty:
            return
        started_at = utc_iso()
        try:
            try:
                result = await process_profile(
                    client, job, output_directory=output_directory,
                    success_recorder=success_recorder, stop_event=control.stop_event,
                    use_resume=use_resume,
                    store_directory=state_recorder.directory / "pages" if durable else None,
                    on_discovery=(lambda name, rows: disk_call(state_recorder.discover, job.username, name, rows, phase=job.phase)) if durable and job.phase == 1 else None,
                    config=state_recorder.config if durable else None,
                    on_profile=(lambda profile: disk_call(state_recorder.register_profile, job, profile)) if durable else None,
                )
            except asyncio.CancelledError:
                control.stop_event.set()
                if client.gate.stats:
                    client.gate.stats.done.set()
                result = error_profile_result(job, status="cancelled", started_at_utc=started_at,
                                              error='Request interrupted by the global Worker rate-limit stop.'
                                              if client.gate.rate_limit_exhausted else 'Scan cancelled by the user.')
            except PublicProfileRequired as exc:
                result = error_profile_result(job, status="private", started_at_utc=started_at,
                                              error=REDACTOR.text(exc))
                result.metadata = {"profile":getattr(exc,"profile_metadata",{})}
            except WorkerApiError as exc:
                result = error_profile_result(job, status='cancelled' if exc.kind == 'rate_limited' else
                                              'network_timeout' if exc.kind == 'response_timeout' else exc.kind, started_at_utc=started_at,
                                              error=REDACTOR.text(exc))
                if getattr(exc, 'backend_failure', None) and exc.kind in ('empty_response', 'risk_control', 'invalid_response', 'backend_unavailable'):
                    result.status = 'partial'
                result.metadata = {'backend_failure':getattr(exc,'backend_failure',None), "http_code":exc.code if isinstance(exc.code,int) else None,
                                   "profile_retries":getattr(exc,"retry_count",0)}
            except (OSError, sqlite3.Error) as exc:
                result = error_profile_result(job, status="file_error", started_at_utc=started_at,
                                              error=REDACTOR.text(exc))
                control.stop_with_fatal_error("File persistence failed; stopped to protect progress.")
            except ExporterError as exc:
                result = error_profile_result(job, status="configuration_error", started_at_utc=started_at,
                                              error=REDACTOR.text(exc))
                control.stop_with_fatal_error(REDACTOR.text(exc))
            except Exception as exc:
                # Avoid leaking arbitrary transport exception text/credentials.
                result = error_profile_result(job, status="unexpected_error", started_at_utc=started_at,
                                              error=f"Unexpected {type(exc).__name__}; no private exception details logged.")
                control.stop_with_fatal_error(result.error)
            if client.gate.blocked_reason:
                control.stop_with_fatal_error(client.gate.blocked_reason)
            if client.gate.rate_limit_exhausted:
                control.stop_with_fatal_error(client.gate.rate_limit_reason or 'Rate limit detected; stopped.')
            result.target_found_in = success_recorder.found_lists_for(job.username)
            if result.error and result.status != "restricted":
                console(f"[@{job.username}] [{result.status.upper()}] {result.error}", error=True)
            processed, total = await disk_call(state_recorder.record, result)
            if client.gate.stats:
                client.gate.stats.account_finished(result)
            phase_label = f"[PHASE {job.phase}] " if durable else ""
            console(f"{phase_label}[PROGRESS {processed:,}/{total:,}] @{job.username}: "
                    f"{result.status} (async worker {worker_number})")
        except BaseException:
            control.stop_event.set()
            raise
        finally:
            if not durable:
                jobs.task_done()


async def _run_scan(
    jobs: list[ProfileJob], *, output_directory: Path, worker_count: int,
    pacing: tuple[float, float], success_recorder: SuccessRecorder,
    state_recorder: ScanStateRecorder, use_resume: bool,
    pool: ProxyPool | None = None, initial_concurrency: int | None = None,
    adaptive: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    job_queue: asyncio.Queue = asyncio.Queue()
    for job in jobs:
        job_queue.put_nowait(job)
    control = SharedScanControl()
    pool = pool if pool is not None else ProxyPool([])
    settings = state_recorder.config if isinstance(state_recorder, DurableScanState) else {}
    ceiling = runtime_ceiling(worker_count, settings)
    gate = AsyncRequestGate(*pacing, control.stop_event, ceiling=ceiling,
                            initial=min(ceiling, initial_concurrency or ceiling), adaptive=True)
    gate.stats = _STATS.get()
    if gate.stats:
        gate.stats.gate = gate
        if gate.stats.status != 'running':
            control.stop_with_fatal_error(gate.stats.stop_reason or 'Scan stopped before request scheduling.')
    profile_workers = min(worker_count, ceiling * 2)
    console(f'[ASYNC] Configured workers: {worker_count}; bounded profile tasks: {profile_workers}; '
            f'connection ceiling: {ceiling}; initial effective concurrency: {gate.limit}.')
    raw_directory = output_directory / "raw_api" if KEEP_RAW_MEMBER_DATA else None
    from hybrid_backend import create_client
    async with create_client(gate, pool, raw_directory=raw_directory,
                               settings=state_recorder.config if isinstance(state_recorder, DurableScanState) else None,
                               avatar_directory=output_directory) as client:
        while True:
            if getattr(client, 'avatars', None) is not None:
                client.start_avatar_backfill(output_directory)
            summary = await disk_call(statistics_summary, state_recorder)
            available_jobs = summary.get('remaining_profiles', len(jobs))
            task_budget = min(profile_workers*2, max(1, available_jobs))
            tasks = []
            async def dispatch_profiles():
                # Grow useful profile workers with request admission. A 10,000
                # setting does not construct 10,000 idle tasks on startup.
                live, outcomes = set(), []
                try:
                    while True:
                        desired = min(task_budget, min(profile_workers, gate.limit*2) + getattr(gate, 'parked_retries', 0))
                        while len(tasks) < desired and not control.stop_event.is_set():
                            number = len(tasks) + 1
                            task = asyncio.create_task(worker_loop(
                                number, job_queue, client=client, output_directory=output_directory,
                                success_recorder=success_recorder, state_recorder=state_recorder,
                                control=control, use_resume=use_resume), name=f'profile-worker-{number}')
                            tasks.append(task); live.add(task)
                        if not live:
                            return outcomes
                        finished, live = await asyncio.wait(live, timeout=.25, return_when=asyncio.FIRST_COMPLETED)
                        for task in finished:
                            outcomes.append(task.exception() if not task.cancelled() else asyncio.CancelledError())
                        if not live:
                            return outcomes
                finally:
                    for task in live:
                        task.cancel()
                    if live:
                        await asyncio.gather(*live, return_exceptions=True)
            group = asyncio.create_task(dispatch_profiles(), name='profile-dispatcher')
            stopped = asyncio.create_task(control.stop_event.wait())
            try:
                await asyncio.wait((group, stopped), return_when=asyncio.FIRST_COMPLETED)
                if stopped.done() and not group.done():
                    # Let completed responses commit, then bound shutdown latency.
                    try:
                        await asyncio.wait_for(asyncio.shield(group), timeout=2)
                    except asyncio.TimeoutError:
                        for task in tasks:
                            task.cancel()
                outcomes = await asyncio.shield(group)
            except asyncio.CancelledError:
                control.stop_event.set()
                for task in tasks:
                    task.cancel()
                if gate.stats:
                    gate.stats.done.set()
                # Workers save partial exports. Await all atomic disk operations
                # before final snapshots and closing clients/SQLite connections.
                await asyncio.shield(group)
                await disk_call(success_recorder.finalize, scan_complete=False)
                # session_runtime owns the one final state export on interruption.
                raise
            finally:
                stopped.cancel()
                await asyncio.gather(stopped, return_exceptions=True)
            for outcome in outcomes:
                if isinstance(outcome, BaseException):
                    control.stop_with_fatal_error(f"Worker failed ({type(outcome).__name__}); progress preserved.")
            if control.stop_event.is_set() or not isinstance(state_recorder, DurableScanState):
                break
            if not await disk_call(state_recorder.advance_phase):
                break
        if not control.stop_event.is_set() and getattr(client, 'avatar_backfill_task', None) is not None:
            await client.avatar_backfill_task
        state_recorder.base["network"] = {
            "mode": "async_httpx", "auto_workers": True, 'configured_workers': worker_count,
            'profile_worker_tasks': profile_workers,
            "concurrency_ceiling": gate.ceiling, "final_active_limit": gate.limit,
            "peak_in_flight": gate.peak_active, "requests_started": gate.started,
            "proxy_pool": pool.summary(),
        }
    all_profiles_complete = await disk_call(state_recorder.all_complete) if isinstance(state_recorder, DurableScanState) else (
        state_recorder.total_profiles == len(state_recorder.current_run)
        and all(bool(result.get("complete")) for key, result in state_recorder.profiles.items()
                if key in state_recorder.current_run)
    )
    if control.fatal_error and gate.stats:
        gate.stats.stop('stopped_error', control.fatal_error)
    if gate.stats:
        gate.stats.done.set()
    state_snapshot = await disk_call(state_recorder.finalize,
        scan_complete=control.fatal_error is None and all_profiles_complete,
        fatal_error=control.fatal_error,
    )
    success_snapshot = await disk_call(success_recorder.finalize,
                                       scan_complete=bool(state_snapshot.get("scan_complete")))
    return success_snapshot, state_snapshot, control.fatal_error


def statistics_summary(state):
    if isinstance(state, DurableScanState):
        return state.summary()
    with state.lock:
        return state._summary_locked()


@asynccontextmanager
async def session_runtime(state, root, workers):
    global _LIVE_LOGGING
    if _STATS.get() is not None:
        yield _STATS.get()
        return
    stats = ScanStatistics(root, workers)
    old_logging = _LIVE_LOGGING
    settings = state.config if isinstance(state, DurableScanState) else {}
    io_workers = settings.get('io_workers', 0)
    async with DiskLane(capacity=max(1, workers*2), io_workers=io_workers, profile_workers=workers, emit=console) as lane:
        stats.disk_lane = lane
        console(f"[IO] Profile writers: {lane.io_limit}/{lane.io_ceiling} ({'automatic' if lane.io_auto else 'configured'}); shared state writes stay ordered.")
        disk_token, stats_token = _DISK_LANE.set(lane), _STATS.set(stats)
        async def rate_limit_checkpoint():
            if isinstance(state, DurableScanState):
                state.base['backend_rate_limits'] = stats.rate_limits.snapshot()
                await disk_call(state.snapshot)
            snapshot = stats.snapshot(await disk_call(statistics_summary, state))
            await disk_call(stats.write, snapshot, atomic_write_json)
        stats.rate_limit_checkpoint = rate_limit_checkpoint
        reporter = None
        try:
            first = stats.snapshot(await disk_call(statistics_summary, state))
            await disk_call(stats.write, first, atomic_write_json)
            _LIVE_LOGGING = True
            if isinstance(state, DurableScanState):
                reporter = asyncio.create_task(stats.report_loop(state, disk_call, atomic_write_json, console), name='scan-statistics')
                def reporter_done(task):
                    if not task.cancelled() and task.exception() is not None:
                        stats.stop('stopped_error', 'Statistics persistence failed.')
                        if stats.gate:
                            stats.gate.stop_event.set()
                reporter.add_done_callback(reporter_done)
            yield stats
        except BaseException as exc:
            stats.stop('stopped_interrupted' if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)) else 'stopped_error',
                       'Interrupted by the user.' if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)) else type(exc).__name__)
            if isinstance(state, DurableScanState):
                state.base['scan_status'] = stats.status
                await disk_call(state.finalize, scan_complete=False, fatal_error=stats.stop_reason)
                state._interrupted_finalized = True
            raise
        finally:
            try:
                stats.done.set()
                reporter_error = None
                if reporter:
                    outcome = await asyncio.gather(reporter, return_exceptions=True)
                    if isinstance(outcome[0], BaseException):
                        reporter_error = outcome[0]
                        stats.stop('stopped_error', 'Statistics persistence failed.')
                summary = await disk_call(statistics_summary, state)
                if stats.status == 'running':
                    finished = summary.get('remaining_profiles', 0) == 0
                    stats.stop(('completed_with_errors' if summary.get('failed_profiles', 0) else 'completed') if finished
                               else 'stopped_incomplete')
                if isinstance(state, DurableScanState):
                    state.base['scan_status'] = stats.status
                    if getattr(stats, 'rate_limits', None):
                        state.base['backend_rate_limits'] = stats.rate_limits.snapshot()
                    await disk_call(state.snapshot, scan_complete=state.metadata.get('scan_complete', False), fatal_error=stats.stop_reason)
                final = stats.snapshot(summary)
                await disk_call(stats.write, final, atomic_write_json, history=True)
                if reporter_error:
                    raise ExporterError('Statistics could not be saved; progress retained.') from None
            finally:
                _LIVE_LOGGING = old_logging
                _STATS.reset(stats_token)
                _DISK_LANE.reset(disk_token)


async def run_scan(jobs, *, output_directory, worker_count, **kwargs):
    async with session_runtime(kwargs['state_recorder'], output_directory, worker_count):
        return await _run_scan(jobs, output_directory=output_directory, worker_count=worker_count, **kwargs)


def print_final_summary(
    *,
    success_document: dict[str, Any],
    state_document: dict[str, Any],
    success_path: Path,
    state_path: Path,
    output_directory: Path,
) -> None:
    success_summary = success_document.get("summary", {})
    state_summary = state_document.get("summary", {})
    console("\n" + "=" * 76)
    console(
        "[COMPLETE] SCAN FINISHED (see statuses for partial/skipped outcomes)"
        if state_document.get("scan_complete")
        else "SCAN STOPPED OR FINISHED WITH INCOMPLETE PROFILES"
    )
    console(
        f"Profiles checked: {state_summary.get('processed_profiles', 0):,} / "
        f"{state_summary.get('total_profiles', 0):,}"
    )
    console(f"Completed successfully: {state_summary.get('completed_overall', 0):,}")
    console(f"Failed: {state_summary.get('failed_profiles', 0):,}")
    console(f"Skipped total: {state_summary.get('skipped_profiles', 0):,}")
    statuses = state_summary.get("statuses", {})
    console(f"Private accounts skipped: {statuses.get('private', 0):,}")
    for name in ("not_found", "invalid_username", "skipped_size", "skipped_unknown_size", "skipped_duplicate"):
        if statuses.get(name): console(f"{name.replace('_', ' ').title()}: {statuses[name]:,}")
    console(f"Restricted-list profiles: {statuses.get('restricted', 0):,}")
    console(f"Pending: {state_summary.get('remaining_profiles', 0):,}")
    console(f"Reused from previous run: {state_summary.get('already_processed', 0):,}")
    console(f"Checked during this execution: {state_summary.get('processed_this_run', 0):,}")
    console(f"Unknown mutual status due to restricted lists: {state_summary.get('unknown_restricted_mutuals', 0):,}")
    console(
        f"Profiles containing @{success_document.get('target_user', TARGET_USER)}: "
        f"{success_summary.get('profiles_with_target', 0):,}"
    )
    console(f"Mutuals: {success_summary.get('mutual_count', 0):,}")
    console("Profile statuses: " + json.dumps(state_summary.get("statuses", {})))
    console(f"Per-profile exports: {output_directory}")
    console(f"Successful finds: {success_path}")
    console(f"Progress/checkpoint: {state_path}")
    console("=" * 76)


class ScanLock:
    """Per-search OS lock, released on crash; independent searches can overlap."""
    def __init__(self, directory: Path) -> None:
        self.path = directory / "scanner.lock"
        self.stream: Any = None

    def __enter__(self) -> "ScanLock":
        self.stream = self.path.open("a+b")
        if self.path.stat().st_size == 0:
            self.stream.write(b"0")
            self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.stream.close()
            raise ExporterError("Another scanner is already using this output directory.") from None
        return self

    def __exit__(self, *args: Any) -> None:
        if self.stream is not None:
            self.stream.close()


def source_identity(source_type: str, value: str, target: str = TARGET_USER) -> dict[str, Any]:
    return {"source_type": source_type, "source": value, "target_user": target.casefold(),
            "worker_origin": WORKER_ORIGIN, "export_schema": 4, "selected_lists": list(SELECTED_LISTS)}


def sanitize_run_name(username: str) -> str:
    """One short Windows-safe component, including DOS device-name protection."""
    value = str(username).strip().lstrip("@").casefold()
    value = re.sub(r'[<>:"/\\|?*\x00-\x1f\x7f]', "_", value)
    value = value[:60].strip(" .") or "search"
    # Windows reserves these names even when followed by a file extension.
    if re.fullmatch(r"(?:con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])", value.split(".", 1)[0].rstrip(" ")):
        value = "_" + value
    return value


def create_search_directory(base: Path, username: str) -> Path:
    """mkdir is the reservation: no exists-then-create race or shared counter."""
    base.mkdir(parents=True, exist_ok=True)
    name = sanitize_run_name(username)
    number = 1
    while True:
        candidate = base / (name if number == 1 else f"{name}_{number}")
        try:
            candidate.mkdir()  # Must not use exist_ok=True here.
        except FileExistsError:
            number += 1
            continue
        fsync_directory(base)
        return candidate


class RunLog:
    """Sanitized, flushed, append-only console/error logs belonging to one run."""
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.stream: Any = None
        self.errors: Any = None
        self.queue = thread_queue.SimpleQueue()
        self.thread = None
        self.failure = None
        self.write_seconds = 0.

    def consume(self):
        while True:
            item = self.queue.get()
            if item is None: return
            kind, message, error, sanitized = item
            try:
                if kind == 'console': console(message, error=error)
                else: self.write(message, error=error, sanitized=sanitized)
            except Exception as exc:
                self.failure = type(exc).__name__

    def __enter__(self) -> "RunLog":
        global _RUN_LOG, _STARTUP_LOG
        self.stream = (self.directory / "run.log").open("a", encoding="utf-8", buffering=65536)
        try:
            self.errors = (self.directory / "errors.log").open("a", encoding="utf-8", buffering=65536)
        except BaseException:
            self.stream.close()
            raise
        self.thread = threading.Thread(target=self.consume, name='scan-log-writer', daemon=True)
        self.thread.start()
        with PRINT_LOCK:
            _RUN_LOG = self
            self.last_flush = time.monotonic()
            self.write("--- Search execution started ---")
            for message in _STARTUP_LOG or []:
                self.write(REDACTOR.text(message))
            _STARTUP_LOG = None
        return self

    def write(self, message: str, *, error: bool = False, sanitized: bool = False) -> None:
        if threading.current_thread() is not self.thread:
            self.queue.put(('write', message, error, sanitized))
            return
        started = time.perf_counter()
        line = f"{utc_iso()} {message if sanitized else REDACTOR.text(message)}\n"
        self.stream.write(line)
        if error or re.search(r"error|fail|incomplete|\[retry|\[429\]|\[403\]", message, re.I):
            self.errors.write(line)
        if time.monotonic() - self.last_flush >= 1 or '[TARGET FOUND]' in message or 'RATE LIMIT' in message:
            self.stream.flush()
            self.errors.flush()
            self.last_flush = time.monotonic()
        self.write_seconds += time.perf_counter()-started

    def __exit__(self, *args: Any) -> None:
        global _RUN_LOG
        self.queue.put(None)
        if self.thread is not None: self.thread.join()
        with PRINT_LOCK:
            _RUN_LOG = None
            for stream in (self.stream, self.errors):
                if stream is not None:
                    try:
                        stream.flush()
                        os.fsync(stream.fileno())
                    finally:
                        stream.close()
        if self.failure: raise OSError('Background log writer failed; check available disk space.')


class DurableScanState:
    """Durable jobs and phase queues; JSON files are compact readable mirrors."""
    def __init__(self, directory: Path, metadata: dict[str, Any], output_directory: Path,
                 config: dict[str, Any] | None = None) -> None:
        self.directory, self.output_directory = directory, output_directory
        self.path = directory / STATE_FILE_NAME
        self.metadata = metadata
        self.config = config if config is not None else default_scan_config(metadata)
        self.target = self.config["target_username"]
        self.discovery_payload_cache = OrderedDict()
        self.base: dict[str, Any] = {}
        self.lock = threading.RLock()
        self.current_run: set[str] = set()
        self.already_completed = self.already_processed = 0
        self.last_snapshot = 0.0
        self.connection = sqlite3.connect(directory / "state.sqlite3", check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA temp_store=MEMORY")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                username TEXT PRIMARY KEY, job TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', result TEXT,
                attempts INTEGER NOT NULL DEFAULT 0, updated TEXT NOT NULL,
                uid TEXT NOT NULL DEFAULT '', phase INTEGER NOT NULL DEFAULT 1);
            CREATE INDEX IF NOT EXISTS jobs_status ON jobs(status);
            CREATE TABLE IF NOT EXISTS discoveries (
                username TEXT PRIMARY KEY, payload TEXT NOT NULL, discovered_at TEXT NOT NULL,
                uid TEXT NOT NULL DEFAULT '', public INTEGER, phase INTEGER NOT NULL DEFAULT 1);
            CREATE TABLE IF NOT EXISTS discovery_sources (
                username TEXT NOT NULL, owner TEXT NOT NULL, list_name TEXT NOT NULL,
                PRIMARY KEY(username,owner,list_name));
            CREATE TABLE IF NOT EXISTS scan_control (name TEXT PRIMARY KEY, value TEXT NOT NULL);
        """)
        # Additive migration keeps earlier runs, terminal results and page stores.
        for table, fields in {"jobs": [("uid", "TEXT NOT NULL DEFAULT ''"), ("phase", "INTEGER NOT NULL DEFAULT 1")],
                              "discoveries": [("uid", "TEXT NOT NULL DEFAULT ''"), ("public", "INTEGER"),
                                              ("phase", "INTEGER NOT NULL DEFAULT 1")]}.items():
            existing = {row[1] for row in self.connection.execute(f"PRAGMA table_info({table})")}
            for name, declaration in fields:
                if name not in existing:
                    self.connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")
        self.connection.execute("CREATE INDEX IF NOT EXISTS job_phase_status ON jobs(phase,status)")
        self.connection.execute("CREATE INDEX IF NOT EXISTS job_uid ON jobs(uid)")
        self.connection.execute("CREATE INDEX IF NOT EXISTS discovery_uid ON discoveries(uid)")
        self.connection.execute('CREATE INDEX IF NOT EXISTS discovery_public ON discoveries(public)')
        # Counts updated in the SAME transaction as the job, including migrations.
        # Progress is O(number of statuses), not O(entire queue) per profile.
        self.connection.executescript('''
            CREATE TABLE IF NOT EXISTS job_counts(phase INTEGER, status TEXT, n INTEGER NOT NULL, PRIMARY KEY(phase,status));
            CREATE TRIGGER IF NOT EXISTS job_count_insert AFTER INSERT ON jobs BEGIN
                INSERT INTO job_counts VALUES(new.phase,new.status,1) ON CONFLICT(phase,status) DO UPDATE SET n=n+1;
            END;
            CREATE TRIGGER IF NOT EXISTS job_count_delete AFTER DELETE ON jobs BEGIN
                UPDATE job_counts SET n=n-1 WHERE phase=old.phase AND status=old.status;
            END;
            CREATE TRIGGER IF NOT EXISTS job_count_update AFTER UPDATE OF status,phase ON jobs BEGIN
                UPDATE job_counts SET n=n-1 WHERE phase=old.phase AND status=old.status;
                INSERT INTO job_counts VALUES(new.phase,new.status,1) ON CONFLICT(phase,status) DO UPDATE SET n=n+1;
            END;
            CREATE TABLE IF NOT EXISTS retry_history(id INTEGER PRIMARY KEY, username TEXT NOT NULL,
                previous_status TEXT NOT NULL, previous_result TEXT, requeued_at TEXT NOT NULL);
        ''')
        if self._control('count_schema') != 1:
            self.connection.execute('DELETE FROM job_counts')
            self.connection.execute('INSERT INTO job_counts SELECT phase,status,COUNT(*) FROM jobs GROUP BY phase,status')
            self._set_control('count_schema', 1)
        if self._control("schema") is None:
            for key, payload in self.connection.execute("SELECT username,payload FROM discoveries").fetchall():
                row = json.loads(payload)
                self.connection.execute("UPDATE discoveries SET uid=?,public=? WHERE username=?",
                    (numeric_uid(row.get("id")), int(row["private_account"] is False) if isinstance(row.get("private_account"), bool) else None, key))
            self._set_control("schema", 1)
        if self._control("current_phase") is None:
            self._set_control("current_phase", int(metadata.get("current_phase", 1)))
        self.connection.commit()

    def _control(self, name: str) -> Any:
        row = self.connection.execute("SELECT value FROM scan_control WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else None

    def _set_control(self, name: str, value: Any) -> None:
        self.connection.execute("INSERT OR REPLACE INTO scan_control VALUES(?,?)", (name, json.dumps(value)))

    @property
    def current_phase(self) -> int:
        # Normal mode never consumes a saved Phase 2 queue. The queue remains
        # intact if the user changes their mind before starting/resuming.
        return 1 if self.config["scan_mode"] == "normal" else int(self._control("current_phase") or 1)

    def close(self) -> None:
        self.connection.close()

    @property
    def total_profiles(self) -> int:
        return self.summary()["total_profiles"]

    def add_jobs(self, jobs: Iterable[ProfileJob], phase: int = 1) -> None:
        with self.lock, self.connection:
            for job in jobs:
                self._add_job(job, phase)

    def _add_job(self, job: ProfileJob, phase: int) -> bool:
        key = parse_username(job.username).casefold()
        uid = numeric_uid(job.uid)
        if self.connection.execute("SELECT 1 FROM jobs WHERE username=? OR (uid<>'' AND uid=?) LIMIT 1", (key, uid)).fetchone():
            return False
        canonical = ProfileJob(key, f"{TIKTOK_ORIGIN}/@{key}", job.source_lists, uid, phase)
        self.connection.execute("INSERT INTO jobs(username,job,updated,uid,phase) VALUES(?,?,?,?,?)",
                                (key, json.dumps(asdict(canonical)), utc_iso(), uid, phase))
        return True

    def register_profile(self, job: ProfileJob, profile: ProfileSnapshot) -> bool:
        with self.lock, self.connection:
            other = self.connection.execute("SELECT username FROM jobs WHERE uid=? AND username<>? LIMIT 1",
                                            (profile.uid, job.username.casefold())).fetchone()
            if other:
                return False
            self.connection.execute("UPDATE jobs SET uid=? WHERE username=?", (profile.uid, job.username.casefold()))
            return True

    def discover(self, owner: str, list_name: str, members: Iterable[dict[str, Any]], *, phase: int = 1) -> None:
        if phase == 2:
            return  # There is deliberately no Phase 3 discovery path.
        with self.lock, self.connection:
            now = utc_iso()
            for member in members:
                try:
                    name = parse_username(str(member.get("username") or "")).casefold()
                except ExporterError:
                    continue
                uid = numeric_uid(member.get("id"))
                existing = self.connection.execute("SELECT username FROM discoveries WHERE username=? OR (uid<>'' AND uid=?) ORDER BY rowid", (name, uid)).fetchall()
                key = existing[0][0] if existing else name
                for (alias,) in existing[1:]:
                    self.connection.execute("INSERT OR IGNORE INTO discovery_sources SELECT ?,owner,list_name FROM discovery_sources WHERE username=?", (key, alias))
                    self.connection.execute("DELETE FROM discovery_sources WHERE username=?", (alias,))
                    self.connection.execute("DELETE FROM discoveries WHERE username=?", (alias,))
                privacy = member.get("private_account")
                public = int(privacy is False) if isinstance(privacy, bool) else None
                cached = self.discovery_payload_cache.get(key)
                if cached is not None and cached[0] == member:
                    encoded = cached[1]
                    self.discovery_payload_cache.move_to_end(key)
                else:
                    encoded = json.dumps(REDACTOR.clean({**member, "username": key}), ensure_ascii=False)
                    self.discovery_payload_cache[key] = (member, encoded)
                    if len(self.discovery_payload_cache) > max(256, self.config.get('workers', 4)):
                        self.discovery_payload_cache.popitem(last=False)
                self.connection.execute("INSERT INTO discoveries(username,payload,discovered_at,uid,public,phase) VALUES(?,?,?,?,?,?) ON CONFLICT(username) DO UPDATE SET uid=CASE WHEN excluded.uid<>'' THEN excluded.uid ELSE discoveries.uid END, public=COALESCE(excluded.public,discoveries.public), payload=excluded.payload, phase=MAX(discoveries.phase,excluded.phase)",
                    (key, encoded, now, uid, public, phase))
                self.connection.execute("INSERT OR IGNORE INTO discovery_sources VALUES(?,?,?)", (key, owner.casefold(), list_name))

    def recover(self, success: SuccessRecorder) -> None:
        with self.lock:
            # One directory enumeration replaces a failed stat for every untouched
            # pending account. This run holds an exclusive folder lock.
            exports = {entry.name for entry in os.scandir(self.output_directory) if entry.name.endswith('.json')}
            rows = self.connection.execute("SELECT username,job,status,phase,result FROM jobs ORDER BY rowid")
            for key, job_json, status, phase, old_result_json in rows:
                if status == 'pending' and not old_result_json and output_file_name(key, SELECTED_LISTS) not in exports:
                    continue
                old_result = json.loads(old_result_json) if old_result_json else {}
                if status == 'completed' and old_result.get('metadata', {}).get('export_stamp'):
                    path = output_file_path(self.output_directory, key, SELECTED_LISTS)
                    try:
                        stamp = path.stat()
                        unchanged = [stamp.st_size, stamp.st_mtime_ns] == old_result['metadata']['export_stamp']
                    except OSError:
                        unchanged = False
                    if unchanged and set(old_result.get('target_found_in', [])) <= set(success.found_lists_for(key)):
                        # Only set after the export, discoveries and immediate matches
                        # were durably committed. Changed/legacy files are revalidated.
                        continue
                # Repair the legacy crash/duplicate window even if an earlier
                # execution already recorded this alias as terminally skipped.
                if status == "skipped_duplicate":
                    info = json.loads(job_json)
                    recovered_job = ProfileJob(info["username"], info["profile_url"], tuple(info["source_lists"]), info.get("uid", ""), phase)
                    path = self.directory / "pages" / (sanitize_run_name(key) + ".sqlite3")
                    if path.is_file():
                        with UserStore(path) as store:
                            for name in SELECTED_LISTS:
                                batch = []
                                for member in store.iter_members(name):
                                    if str(member.get("username", "")).casefold() == self.target.casefold() or bool(self.config.get("resolved_target_uid") and member.get("id") == self.config["resolved_target_uid"]):
                                        success.record_match(recovered_job, name, member)
                                    if phase == 1:
                                        batch.append(member)
                                        if len(batch) == 500:
                                            self.discover(key, name, batch, phase=1)
                                            batch = []
                                if batch:
                                    self.discover(key, name, batch, phase=1)
                        old_result["target_found_in"] = success.found_lists_for(key)
                        self.connection.execute("UPDATE jobs SET result=? WHERE username=?", (json.dumps(old_result), key))
                # Only old partial/failed rows proven terminal by saved list
                # outcomes qualify for migration. Explicit restricted rows are
                # already checked and are not reprocessed on each execution.
                old_lists = old_result.get("list_results", {})
                if status in {"partial", "failed"}:
                    for name, info in old_lists.items():
                        if (isinstance(info, dict) and info.get("endpoint_exhausted") is True
                                and not info.get("error") and not info.get("invalid_records_ignored")
                                and info.get("stop_reason") in {"endpoint_exhausted", "count_discrepancy"}):
                            try:
                                updated = ListExportResult(**{k:v for k,v in info.items() if k in ListExportResult.__dataclass_fields__})
                                updated.finish()
                                old_lists[name] = asdict(updated)
                            except (TypeError,ValueError):
                                pass  # Inconclusive old evidence is never promoted.
                migrate_restricted = status in {"partial", "failed"} and restricted_terminal(old_lists)
                migrate = migrate_restricted or (status in {"partial", "failed"}
                    and all(old_lists.get(name, {}).get("complete") is True
                            and old_lists.get(name, {}).get("endpoint_exhausted") is True for name in SELECTED_LISTS))
                if status not in {"pending", "in_progress", "completed"} and not migrate:
                    continue
                info = json.loads(job_json)
                job = ProfileJob(info["username"], info["profile_url"], tuple(info["source_lists"]), info.get("uid", ""), phase)
                path = output_file_path(self.output_directory, key, SELECTED_LISTS)
                reusable = inspect_reusable_export(path, expected_username=key, target_username=self.target, target_uid=self.config.get('resolved_target_uid'))
                if reusable:
                    if phase == 1:
                        for name, members in reusable["members"].items():
                            self.discover(key, name, members, phase=1)
                    for name, member in reusable["matches"].items():
                        success.record_match(job, name, member, observed_at=reusable["observed_at"])
                    success.reconcile_complete_lists(job, reusable["matches"],
                        [name for name, info in reusable["list_results"].items() if info.get("complete")])
                    result = ProfileProcessResult(key, job.profile_url, list(job.source_lists), reusable["status"], str(path), reusable["complete"],
                        success.found_lists_for(key), old_result.get("started_at_utc") or reusable["observed_at"],
                        old_result.get("completed_at_utc") or reusable["observed_at"], reusable["list_results"])
                    stamp = path.stat()
                    result.metadata = {**old_result.get('metadata', {}), 'profile': reusable['profile_metadata'],
                                       'export_stamp': [stamp.st_size, stamp.st_mtime_ns]}
                    self.connection.execute("UPDATE jobs SET status=?,result=?,updated=? WHERE username=?",
                        ("completed" if reusable["complete"] else "restricted", json.dumps(asdict(result)), utc_iso(), key))
                    # Restore numeric identity even if the crash preceded the job update.
                    self.connection.execute("UPDATE jobs SET uid=? WHERE username=?", (reusable["profile_uid"], key))
                elif migrate_restricted:
                    # A prior durable result may outlive its export. Preserve
                    # the known unavailable state and timestamps without traffic.
                    old_result["status"] = "restricted"
                    self.connection.execute("UPDATE jobs SET status='restricted',result=? WHERE username=?",
                                            (json.dumps(old_result),key))
                elif status == "completed":
                    self.connection.execute("UPDATE jobs SET status='missing_export' WHERE username=?", (key,))
                    console(f"[@{key}] [ERROR] Completed export is missing/invalid; retained as terminal for review, not silently rescanned.", error=True)
                elif status == "in_progress":
                    self.connection.execute("UPDATE jobs SET status='pending',updated=? WHERE username=?", (utc_iso(), key))
            self.connection.commit()
            summary = self.summary()
            self.already_completed, self.already_processed = summary["completed_overall"], summary["processed_profiles"]
            self.snapshot()

    def retry_failed(self) -> int:
        """Explicit, transactional requeue. Pages, matches, UID and exports survive.

        History and pending transitions commit together. A crash anywhere after
        this transaction is handled by ordinary pending/in_progress recovery.
        """
        maximum = 2 if self.config['scan_mode'] == 'double_phase' else 1
        with self.lock, self.connection:
            self.connection.execute('BEGIN IMMEDIATE')
            where = "phase<=? AND status IN ('network_timeout','response_timeout','network_error','partial')"
            now = utc_iso()
            self.connection.execute('INSERT INTO retry_history(username,previous_status,previous_result,requeued_at) '
                                    'SELECT username,status,result,? FROM jobs WHERE ' + where, (now, maximum))
            count = self.connection.execute('UPDATE jobs SET status=\'pending\',updated=? WHERE ' + where, (now, maximum)).rowcount
            # Retrying a Phase 1 profile must not strand it behind Phase 2.
            if self.connection.execute("SELECT 1 FROM jobs WHERE phase=1 AND status='pending' LIMIT 1").fetchone():
                self._set_control('current_phase', 1)
            if count:
                self.metadata['scan_complete'] = False
                self.base['retry_failed_profiles'] = {'requeued':count, 'at':now}
        summary = self.summary()
        self.already_completed, self.already_processed = summary['completed_overall'], summary['processed_profiles']
        self.snapshot()
        return count

    def claim(self) -> ProfileJob | None:
        with self.lock, self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            row = self.connection.execute("SELECT username,job,uid,phase FROM jobs WHERE status='pending' AND phase=? ORDER BY rowid LIMIT 1", (self.current_phase,)).fetchone()
            if row is None:
                return None
            self.connection.execute("UPDATE jobs SET status='in_progress',attempts=attempts+1,updated=? WHERE username=?", (utc_iso(), row[0]))
            item = json.loads(row[1])
            return ProfileJob(item["username"], item["profile_url"], tuple(item["source_lists"]), row[2], row[3])

    def record(self, result: ProfileProcessResult) -> tuple[int, int]:
        key = result.username.casefold()
        if result.complete:
            if not getattr(result, '_export_committed', False) and inspect_reusable_export(Path(result.export_path or ""), expected_username=key, target_username=self.target, target_uid=self.config.get('resolved_target_uid')) is None:
                raise ExporterError("Refusing completion without a valid saved export.")
            status = "completed"
            stamp = Path(result.export_path).stat()
            result.metadata['export_stamp'] = [stamp.st_size, stamp.st_mtime_ns]
        elif result.status in {"cancelled", 'rate_limited'}:
            status = "pending"
        else:
            status = result.status  # All finished attempts, including errors, are terminal.
        with self.lock, self.connection:
            old = self.connection.execute('SELECT status,result FROM jobs WHERE username=?', (key,)).fetchone()
            if old and old[0] == 'completed' and not result.complete:
                return self.phase_summary(self.current_phase)['processed'], self.phase_summary(self.current_phase)['total']
            if status == 'response_timeout':
                status = result.status = 'network_timeout'
            if status == 'pending' and old and old[1] and not result.list_results:
                # A cancellation before a request is not a new failed result.
                previous = json.loads(old[1])
                result.list_results = previous.get('list_results', {})
                result.export_path = previous.get('export_path')
                result.metadata = {**previous.get('metadata', {}), **result.metadata}
            self.connection.execute("UPDATE jobs SET status=?,result=?,updated=? WHERE username=?", (status, json.dumps(REDACTOR.clean(asdict(result))), utc_iso(), key))
        with self.lock:
            self.current_run.add(key)
            if time.monotonic() - self.last_snapshot >= 15:
                self.snapshot()
            counts = self.phase_summary(self.current_phase)
            return counts["processed"], counts["total"]

    def phase_summary(self, phase: int) -> dict[str, int]:
        with self.lock:
            statuses = dict(self.connection.execute("SELECT status,n FROM job_counts WHERE phase=? AND n>0", (phase,)))
            total = sum(statuses.values())
            pending = statuses.get("pending", 0) + statuses.get("in_progress", 0)
            return {"total": total, "processed": total - pending, "remaining": pending,
                    "completed": statuses.get("completed", 0)}

    def advance_phase(self) -> bool:
        with self.lock, self.connection:
            if self.config["scan_mode"] != "double_phase" or self.current_phase != 1 or self.phase_summary(1)["remaining"]:
                return False
            self.connection.execute("BEGIN IMMEDIATE")
            added = duplicates = 0
            if not self._control("phase2_built") or self.connection.execute('SELECT 1 FROM retry_history LIMIT 1').fetchone():
                for name, uid, payload in self.connection.execute("SELECT username,uid,payload FROM discoveries WHERE public=1 AND phase=1 ORDER BY rowid"):
                    item = json.loads(payload)
                    job = ProfileJob(name, item.get("profile_url") or f"{TIKTOK_ORIGIN}/@{name}", ("phase1_discovery",), uid, 2)
                    if self._add_job(job, 2): added += 1
                    else: duplicates += 1
                self._set_control("phase2_built", True)
                self._set_control("phase2_duplicates", duplicates)
            self._set_control("current_phase", 2)
        self.snapshot()
        summary = self.phase_summary(1)
        private = self.connection.execute("SELECT COUNT(*) FROM discoveries WHERE public=0 AND phase=1").fetchone()[0]
        skipped = self.connection.execute("SELECT COUNT(*) FROM jobs WHERE phase=1 AND status='skipped_size'").fetchone()[0]
        console(f"\n{'='*50}\nPHASE 1 COMPLETE\nOriginal profiles checked: {summary['processed']:,}\n"
                f"Unique public Phase 2 profiles: {self.phase_summary(2)['total']:,}\n"
                f"Already processed/duplicate: {duplicates:,}\nPrivate profiles ignored: {private:,}\nSize-limit skipped: {skipped:,}\n"
                f"{'='*50}\n[PHASE 2] Starting Phase 2...\n"
                f"[PHASE 2] [PROGRESS {self.phase_summary(2)['processed']:,}/{self.phase_summary(2)['total']:,}]")
        return True

    def summary(self) -> dict[str, Any]:
        with self.lock:
            max_phase = 2 if self.config["scan_mode"] == "double_phase" else 1
            statuses = dict(self.connection.execute("SELECT status,SUM(n) FROM job_counts WHERE phase<=? GROUP BY status HAVING SUM(n)>0", (max_phase,)))
            total = sum(statuses.values())
            completed = statuses.get("completed", 0)
            pending, active = statuses.get("pending", 0), statuses.get("in_progress", 0)
            processed = total - pending - active
            skipped = sum(count for status, count in statuses.items() if status.startswith("skipped") or status in {"private", "not_found", "invalid_username"})
            return {"total_profiles": total, "processed_profiles": processed, "completed_overall": completed,
                    "completed_this_run": max(0, completed-self.already_completed), "processed_this_run": max(0, processed-self.already_processed),
                    "skipped_already_completed": self.already_completed, "already_processed": self.already_processed,
                    "attempted_this_run": len(self.current_run), "remaining_profiles": pending+active,
                    "pending_profiles": pending, "in_progress_profiles": active,
                    "failed_profiles": processed-completed-skipped-statuses.get("restricted", 0), "skipped_profiles": skipped,
                    "restricted_profiles": statuses.get("restricted", 0), "private_profiles": statuses.get("private", 0),
                    "unknown_restricted_mutuals": self.connection.execute("SELECT COUNT(*) FROM jobs WHERE phase<=? AND status='restricted' AND COALESCE(json_array_length(json_extract(result,'$.target_found_in')),0)<2", (max_phase,)).fetchone()[0],
                    "discovered_profiles": self.connection.execute("SELECT COUNT(*) FROM discoveries WHERE public=1").fetchone()[0],
                    "current_phase": self.current_phase, "phases": {"1": self.phase_summary(1), "2": self.phase_summary(2)}, "statuses": statuses}

    def all_complete(self) -> bool:
        return bool(self.metadata.get("input_imported")) and self.summary()["remaining_profiles"] == 0 and (
            self.config["scan_mode"] == "normal" or self.current_phase == 2)

    def snapshot(self, *, scan_complete: bool = False, fatal_error: str | None = None) -> dict[str, Any]:
        with self.lock:
            summary = self.summary()
            self.metadata.update(updated_at_utc=utc_iso(), scan_complete=scan_complete, summary=summary, current_phase=self.current_phase)
            document = {**self.metadata, **self.base, "schema_version": 3, "fatal_error": fatal_error,
                        "profiles_database": str(self.directory / "state.sqlite3"), "data_complete": summary["completed_overall"] == summary["total_profiles"]}
            atomic_write_json(self.path, document)
            atomic_write_json(self.directory / "session.json", self.metadata)
            self.config.update(current_phase=self.current_phase, progress={"processed": summary["processed_profiles"], "total": summary["total_profiles"]})
            save_scan_config(self.directory, self.config)
            self.last_snapshot = time.monotonic()
            return document

    def finalize(self, *, scan_complete: bool, fatal_error: str | None) -> dict[str, Any]:
        with self.lock:
            with self.connection:
                self.connection.execute("UPDATE jobs SET status='pending' WHERE status='in_progress'")
            document = self.snapshot(scan_complete=scan_complete, fatal_error=fatal_error)
            self.export_rows(self.directory / "results.json", "SELECT result FROM jobs WHERE status IN ('completed','restricted') AND result IS NOT NULL")
            self.export_rows(self.directory / "failures.json", "SELECT result FROM jobs WHERE status NOT IN ('completed','restricted','pending','in_progress') AND status NOT LIKE 'skipped%' AND status NOT IN ('private','not_found','invalid_username') AND result IS NOT NULL")
            self.export_rows(self.directory / "restricted_accounts.json", "SELECT result FROM jobs WHERE status='restricted' AND result IS NOT NULL")
            self.export_rows(self.directory / "skipped_accounts.json", "SELECT result FROM jobs WHERE (status LIKE 'skipped%' OR status IN ('private','not_found','invalid_username')) AND result IS NOT NULL")
            self.export_rows(self.directory / "phase2_queue.json", "SELECT json_object('username',username,'uid',uid,'phase',phase,'status',status) FROM jobs WHERE phase=2 ORDER BY rowid")
            self.export_discoveries()
            return document

    def export_discoveries(self) -> None:
        def rows() -> Iterator[dict[str, Any]]:
            query = """SELECT d.payload,
                (SELECT json_group_array('@'||owner||'/'||list_name) FROM
                 (SELECT owner,list_name FROM discovery_sources WHERE username=d.username ORDER BY owner,list_name))
                FROM discoveries d WHERE public=1 ORDER BY d.rowid"""
            for payload, sources in self.connection.execute(query):
                item = json.loads(payload)
                item["discovered_from"] = json.loads(sources)
                yield item
        write_json_array(self.directory / "discovered_profiles.json", rows())

    def export_rows(self, path: Path, query: str) -> None:
        write_json_array(path, (json.loads(row[0]) for row in self.connection.execute(query)))


def write_json_array(path: Path, rows: Iterable[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".part")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write("[\n")
            for index, row in enumerate(rows):
                stream.write((",\n" if index else "") + json.dumps(REDACTOR.clean(row), ensure_ascii=False))
            stream.write("\n]\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def ask_choice(prompt: str, choices: set[str]) -> str:
    while True:
        try:
            answer = input(prompt).strip().casefold()
        except EOFError:
            raise ExporterError("Startup requires answers; run this script in an interactive terminal.") from None
        if answer in choices:
            return answer
        console("Please enter " + "/".join(sorted(choices)) + ".")


CONFIG_FILE_NAME = "scan_config.json"


def default_scan_config(metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    from hybrid_backend import DEFAULTS
    identity = (metadata or {}).get("identity", {})
    return {**DEFAULTS, "config_version": 1, "target_username": identity.get("target_user", TARGET_USER),
            "worker_origin": validate_worker_origin(identity.get("worker_origin") or os.getenv("TIKTOK_WORKER_ORIGIN") or WORKER_ORIGIN),
            "input_source": identity.get("source_type", "json"),
            "starting_username": identity.get("source") if identity.get("source_type") == "username" else None,
            "source_json_path": identity.get("source", str(configured_input_path())) if identity.get("source_type", "json") == "json" else None,
            "starting_json": "starting_dataset.json", "generated_dataset_path": "starting_dataset.json",
            "follower_skip_limit": 0, "following_skip_limit": 0,
            "scan_mode": "normal", "double_phase_enabled": False,
            "workers": DEFAULT_WORKERS, "max_connections": 0, "request_delay": {"minimum_seconds": 0.0, "maximum_seconds": 0.0},
            "use_proxies": False, "proxy_file": str(configured_proxy_path() or WEBSHARE_PROXY_FILE),
            "proxy_only": os.getenv("TIKTOK_PROXY_ONLY", "0").strip().casefold() in {"1", "true", "yes"}, "keep_raw": KEEP_RAW_MEMBER_DATA, "retry_attempts": FETCH_ATTEMPTS,
            "timeouts": {"response": 40.0, "connect": 10.0, "write": 20.0, "pool": 10.0, "total": 55.0},
            "current_phase": int((metadata or {}).get("current_phase", 1)),
            "progress": {"processed": 0, "total": 0}, "created_at_utc": utc_iso(),
            "last_resumed_at_utc": None}


def validate_scan_config(config: dict[str, Any]) -> None:
    if not isinstance(config, dict) or config.get("config_version") != 1:
        raise ExporterError("Unsupported or missing scan configuration version.")
    validate_worker_origin(config.get("worker_origin", WORKER_ORIGIN))
    from hybrid_backend import validate as validate_backends
    validate_backends(config)
    target = parse_username(str(config.get("target_username", ""))).casefold()
    if target != config.get("target_username"):
        raise ExporterError("Saved target must be a normalized username.")
    if config.get("input_source") not in {"json", "username"}:
        raise ExporterError("Invalid saved input source.")
    if config["input_source"] == "username":
        parse_username(str(config.get("starting_username", "")))
    if config.get("scan_mode") not in {"normal", "double_phase"}:
        raise ExporterError("Invalid saved scan mode.")
    for field_name in ("workers", "follower_skip_limit", "following_skip_limit", "retry_attempts"):
        value = config.get(field_name)
        minimum = 1 if field_name in {"workers", "retry_attempts"} else 0
        if type(value) is not int or value < minimum:
            raise ExporterError(f"Invalid saved {field_name}.")
    if type(config.get('io_workers', 0)) is not int or config.get('io_workers', 0) < 0:
        raise ExporterError('io_workers must be 0 (automatic) or a positive whole number.')
    if type(config.get('max_connections', 0)) is not int or config.get('max_connections', 0) < 0:
        raise ExporterError('max_connections must be 0 (automatic) or a positive whole number.')
    if config["retry_attempts"] > 20:
        raise ExporterError("Saved retry_attempts exceeds 20.")
    delay = config.get("request_delay", {})
    values = [delay.get("minimum_seconds"), delay.get("maximum_seconds")]
    if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in values):
        raise ExporterError("Saved request delays must be finite, nonnegative numbers.")
    if values[0] > values[1]:
        raise ExporterError("Saved minimum delay exceeds maximum delay.")
    for name in ("use_proxies", "proxy_only", "keep_raw"):
        if type(config.get(name)) is not bool:
            raise ExporterError(f"Saved {name} must be a boolean.")
    timeouts = config.get("timeouts", {})
    for name in ("response", "connect", "write", "pool", "total"):
        value = timeouts.get(name)
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise ExporterError(f"Invalid saved {name} timeout.")
    # Generated paths cannot escape the selected search folder.
    for name in ("starting_json", "generated_dataset_path"):
        if config.get(name) != "starting_dataset.json":
            raise ExporterError(f"Invalid {name}; the dataset must stay in this search folder.")


def save_scan_config(root: Path, config: dict[str, Any]) -> None:
    config["double_phase_enabled"] = config["scan_mode"] == "double_phase"
    validate_scan_config(config)
    atomic_write_json(root / CONFIG_FILE_NAME, config)


def read_scan_config(root: Path) -> dict[str, Any]:
    try:
        config = json.loads((root / CONFIG_FILE_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ExporterError(f"Cannot read this search's scan_config.json: {root.name}") from exc
    validate_scan_config(config)
    return config


def ask_number(prompt: str, default: int | float, *, integer: bool = False, minimum: float = 0,
               maximum: float | None = None) -> int | float:
    while True:
        text = input(prompt).strip()
        if not text:
            value = default
        else:
            try:
                if integer and not re.fullmatch(r"[0-9]+|[0-9]{1,3}(?:,[0-9]{3})+", text):
                    raise ValueError
                value = int(text.replace(",", "")) if integer else float(text)
            except ValueError:
                console("[ERROR] Enter a whole number." if integer else "[ERROR] Enter a numeric delay.")
                continue
        if (not integer and not math.isfinite(value)) or value < minimum or (maximum is not None and value > maximum):
            console(f"[ERROR] Value must be at least {minimum:g}" + (f" and at most {maximum:g}." if maximum is not None else "."))
            continue
        return value


def ask_username(prompt: str, default: str | None = None) -> str:
    while True:
        text = input(prompt).strip() or default or ""
        try:
            return parse_username(text).casefold()
        except ExporterError as exc:
            console(f"[ERROR] {exc}")


def ask_bool(prompt: str, default: bool) -> bool:
    value = ask_choice(prompt, {"y", "n", ""})
    return default if not value else value == "y"


def choose_mode(default: str | None = None) -> str:
    console("\nScan mode:\n[1] Normal — scan the starting dataset only.\n[2] Double phase — scan it, then one level of new public accounts; stop after Phase 2.")
    value = ask_choice("Scan mode [1/2" + (", Enter = keep" if default else "") + "]: ", {"1", "2", ""} if default else {"1", "2"})
    return default if not value else {"1": "normal", "2": "double_phase"}[value]


def edit_scan_config(config: dict[str, Any], *, new: bool = False) -> dict[str, Any]:
    updated = json.loads(json.dumps(config))
    console("\nSize limits: 0 = no limit. Rounded API counts (K/M/B) are treated as estimates and labelled as such.")
    for name, label in (("follower_skip_limit", "follower skip limit"), ("following_skip_limit", "following skip limit")):
        old = updated[name]
        if not new: console(f"Current {label}: {old:,}")
        prompt = (f"Maximum {'followers' if name.startswith('follower_') else 'following'} before skipping account [default: {old:,}]: " if new
                  else f"New {label} [Enter = keep {old:,}]: ")
        updated[name] = ask_number(prompt, old, integer=True)
    if new:
        updated["scan_mode"] = choose_mode()
    else:
        console(f"Current scan mode: {updated['scan_mode']}\n[1] Keep current mode\n[2] Change scan mode")
        if ask_choice("Mode [1/2, Enter = keep]: ", {"1", "2", ""}) == "2":
            updated["scan_mode"] = choose_mode(updated["scan_mode"])
    old = updated["workers"]
    if not new: console(f"Current workers: {old}")
    updated["workers"] = ask_number(f"Number of workers (1 or more) [{'default' if new else 'Enter = keep'}: {old}]: ", old, integer=True, minimum=1)
    while True:
        old_min, old_max = updated["request_delay"]["minimum_seconds"], updated["request_delay"]["maximum_seconds"]
        if not new: console(f"Current minimum request delay: {old_min}\nCurrent maximum request delay: {old_max}")
        low = ask_number(f"Minimum request delay in seconds [{'default' if new else 'Enter = keep'}: {old_min}]: ", old_min)
        high = ask_number(f"Maximum request delay in seconds [{'default' if new else 'Enter = keep'}: {old_max}]: ", old_max)
        if low <= high:
            updated["request_delay"] = {"minimum_seconds": low, "maximum_seconds": high}
            break
        console("[ERROR] Minimum delay cannot exceed maximum delay. Enter the pair again.")
    console(f"Current proxy mode: {'Webshare' if updated['use_proxies'] else 'direct'}")
    updated["use_proxies"] = ask_bool("Use proxies? [Y/N, Enter = keep]: ", updated["use_proxies"])
    if updated["use_proxies"]:
        console(f"Proxy file: {updated['proxy_file']} (credentials remain in that file)")
    if not new:
        console(f"Current proxy-only mode: {updated['proxy_only']}; raw API files: {updated['keep_raw']}")
        updated["proxy_only"] = ask_bool("Require proxies, without direct fallback? [Y/N, Enter = keep]: ", updated["proxy_only"])
        updated["keep_raw"] = ask_bool("Keep raw API/member data? [Y/N, Enter = keep]: ", updated["keep_raw"])
    validate_scan_config(updated)
    return updated


def display_config(root: Path, config: dict[str, Any], state: dict[str, Any], heading: str) -> None:
    summary = state.get("summary", {})
    source = "@" + str(config["starting_username"]) if config["input_source"] == "username" else config.get("source_json_path")
    delay = config["request_delay"]
    console(f"\n{'='*50}\n{heading}\n{'='*50}\nTarget: @{config['target_username']}\nStarting source: {source}\n"
            f"Follower skip limit: {config['follower_skip_limit']:,}\nFollowing skip limit: {config['following_skip_limit']:,}\n"
            f"Scan mode: {'Double Phase' if config['scan_mode']=='double_phase' else 'Normal'}\nWorkers: {config['workers']}\n"
            f"Backend: {config.get('backend_mode', 'worker')}\n"
            f"Minimum request delay: {delay['minimum_seconds']} seconds\nMaximum request delay: {delay['maximum_seconds']} seconds\n"
            f"Current phase: Phase {state.get('current_phase', config.get('current_phase', 1))}\n"
            f"Progress: {summary.get('processed_profiles', 0):,} / {summary.get('total_profiles', 0):,}\n"
            f"Proxy mode: {'Webshare' if config['use_proxies'] else 'direct'}\nExisting progress preserved: yes\n"
            f"Output folder:\n{root}\n{'='*50}")


_PREPARED_POOLS: dict[str, ProxyPool] = {}
_RETRY_FAILED: dict[str, bool] = {}  # One execution only; never sticky in configuration.


def prepare_proxy_pool(root: Path, config: dict[str, Any]) -> ProxyPool | None:
    """Last setup step, before the final Start/Resume confirmation."""
    global REDACTOR
    configs = load_proxy_configs(Path(config["proxy_file"])) if config["use_proxies"] else []
    REDACTOR = CredentialRedactor(configs)
    pool = ProxyPool(configs, proxy_only=config["proxy_only"])
    if not config["use_proxies"]:
        if config["proxy_only"]:
            console("Proxy-only mode requires proxies. Edit the configuration.")
            return None
        return pool
    console("\n[1] Check all proxies before starting/resuming\n[2] Skip check/use current configuration")
    choice = ask_choice("Proxy validation: ", {"1", "2"})
    if choice == "2" and (configs or not config["proxy_only"]): return pool
    while True:
        if configs and choice == "1":
            from proxy_validation import validate_pool
            if asyncio.run(validate_pool(sys.modules[__name__], pool, config, root)):
                return pool
        console("[PROXY] No usable proxies.\n[1] Continue without proxies (explicitly disable proxy-only mode)\n[2] Retest\n[0] Return to configuration")
        choice = ask_choice("Select an option: ", {"0", "1", "2"})
        if choice == "0": return None
        if choice == "1":
            config.update(use_proxies=False, proxy_only=False)
            save_scan_config(root, config)
            return ProxyPool([])
        choice = "1"
        # Retest the same loaded configurations, including previously invalid
        # routes. It is a new pass; never edit the uploaded proxy file.
        pool = ProxyPool(configs, proxy_only=config["proxy_only"])


def confirmation_menu(root: Path, config: dict[str, Any], state: dict[str, Any], *, resume: bool) -> dict[str, Any] | None:
    heading = "SAVED SCAN CONFIGURATION" if resume else "SCAN CONFIGURATION"
    if resume:
        console("\nResume configuration:\n[1] Resume using saved configuration\n[2] Change configuration before resuming\n[0] Return to startup menu")
        choice = ask_choice("Select an option: ", {"0", "1", "2"})
        if choice == "0": return None
        if choice == "2":
            config = edit_scan_config(config)
            save_scan_config(root, config)
            heading = "UPDATED SCAN CONFIGURATION"
    while True:
        display_config(root, config, state, heading)
        pool = prepare_proxy_pool(root, config)
        if pool is None:
            config = edit_scan_config(config)
            save_scan_config(root, config)
            continue
        console(f"[1] {'Resume' if resume else 'Start'} scan\n[2] Edit configuration\n[3] Backend configuration\n[0] Cancel")
        choice = ask_choice("Select an option: ", {"0", "1", "2", "3"})
        if choice == "0": return None
        if choice == "1":
            if resume:
                console('Completed profiles will not be rescanned. Eligible failures retain saved pages, cursors and matches.')
                _RETRY_FAILED[str(root)] = ask_bool('Retry failed profiles? [Y/N, default N]: ', False)
            _PREPARED_POOLS[str(root)] = pool
            return config
        if choice == '3':
            from hybrid_backend import edit_backend_config
            config = edit_backend_config(config)
        else:
            config = edit_scan_config(config)
        save_scan_config(root, config)
        heading = "UPDATED SCAN CONFIGURATION"


def load_json_document(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict): raise ExporterError(f"Invalid JSON object: {path.name}")
    return value


def resume_progress(root: Path, config: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    database = root / "state.sqlite3"
    if not database.is_file():
        return snapshot
    with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "jobs" not in tables:
            raise ExporterError("Saved queue database has no jobs table.")
        phase = snapshot.get("current_phase", 1)
        if "scan_control" in tables:
            row = connection.execute("SELECT value FROM scan_control WHERE name='current_phase'").fetchone()
            if row: phase = json.loads(row[0])
        if config["scan_mode"] == "normal": phase = 1
        columns = {row[1] for row in connection.execute("PRAGMA table_info(jobs)")}
        where = " WHERE phase<=1" if config["scan_mode"] == "normal" and "phase" in columns else ""
        statuses = dict(connection.execute("SELECT status,COUNT(*) FROM jobs" + where + " GROUP BY status"))
        total = sum(statuses.values())
        remaining = statuses.get("pending", 0)+statuses.get("in_progress", 0)
    return {**snapshot, "current_phase": phase, "summary": {**snapshot.get("summary", {}),
            "processed_profiles": total-remaining, "remaining_profiles": remaining, "total_profiles": total,
            'retryable_profiles': sum(statuses.get(s, 0) for s in ('network_timeout', 'response_timeout', 'network_error', 'partial'))}}


def retryable_session(metadata: dict[str, Any]) -> bool:
    statuses = metadata.get('summary', {}).get('statuses', {})
    return any(statuses.get(s, 0) for s in ('network_timeout', 'response_timeout', 'network_error', 'partial'))


def choose_startup(base: Path, resources: ExitStack, *, source_selection: bool = False) -> tuple[Path, dict[str, Any], dict[str, Any]] | str:
    base.mkdir(parents=True, exist_ok=True)
    existing = []
    for path in base.glob("*/session.json"):
        if path.parent.is_symlink(): continue
        try:
            metadata = load_json_document(path)
            if (not metadata.get("scan_complete") or retryable_session(metadata)) and not metadata.get("setup_rejected") and not metadata.get("setup_cancelled"):
                existing.append((path.parent, metadata))
        except (OSError, ValueError, ExporterError):
            console(f"[ERROR] Cannot read saved session in {path.parent.name}; files preserved.")
    existing.sort(key=lambda item: item[1].get("updated_at_utc", ""), reverse=True)
    choice = "0"
    if not source_selection:
        console("\nResumable scans (unfinished or retryable failures):" if existing else "\nNo unfinished scans found.")
        for index, (root, _) in enumerate(existing, 1): console(f"[{index}] {root.name}")
        console("[0] Start a new search\n[Q] Exit")
        choice = ask_choice("Select an option: ", {"0", "q"} | {str(i) for i in range(1, len(existing)+1)})
        if choice == "q": return "quit"
    if choice != "0":
        root, metadata = existing[int(choice)-1]
        resources.enter_context(ScanLock(root))
        metadata = load_json_document(root / "session.json")
        if metadata.get("scan_complete") and not retryable_session(metadata):
            console("[COMPLETE] That scan is already finished.")
            return "back"
        state = load_json_document(root / STATE_FILE_NAME) if (root / STATE_FILE_NAME).is_file() else metadata
        if (root / CONFIG_FILE_NAME).is_file():
            config = read_scan_config(root)
        else:
            # Version-4 runs predate saved configuration. Recover known values,
            # require review of missing ones, and preserve the original success-file identity.
            console("[RESUME] This older scan has no scan_config.json. Review settings once; all existing progress is retained.")
            config = default_scan_config(metadata)
            network = state.get("network", {})
            config["workers"] = network.get("concurrency_ceiling", DEFAULT_WORKERS)
            config["use_proxies"] = metadata.get("network_mode") == "webshare"
            if (root / SUCCESS_FILE_NAME).is_file():
                config["success_source"] = load_json_document(root / SUCCESS_FILE_NAME).get("source_input_json")
            config = edit_scan_config(config)
            save_scan_config(root, config)
        immutable = metadata.get("identity", {})
        configured_source = config["starting_username"] if config["input_source"] == "username" else config["source_json_path"]
        if (immutable.get("target_user") != config["target_username"] or immutable.get("source_type") != config["input_source"]
                or immutable.get("source") != configured_source):
            raise ExporterError("Saved configuration identity does not match this search. Target/source changes require a new search.")
        state = resume_progress(root, config, state)
        if metadata.get("input_imported") and not state.get("summary", {}).get("remaining_profiles") and (config["scan_mode"] == "normal" or state.get("current_phase") == 2):
            console("[COMPLETE] All jobs are already processed; continuing only reconciles saved files, without rescanning them.")
        config = confirmation_menu(root, config, state, resume=True)
        if config is None: return "back"
        config["last_resumed_at_utc"] = utc_iso()
        save_scan_config(root, config)
        return root, config, metadata
    console("\nStarting source:\n[1] Default input JSON\n[2] TikTok username\n[0] Back")
    choice = ask_choice("Select starting source: ", {"0", "1", "2"})
    if choice == "0": return "back"
    config = default_scan_config()
    source_document: Any = None
    if choice == "1":
        source = configured_input_path().resolve()
        try:
            # Validate without creating any output folder yet.
            load_jobs_allow_empty(source)
            source_document = json.loads(source.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError, ExporterError) as exc:
            console(f"[ERROR] {exc}")
            return "source"
        config.update(input_source="json", source_json_path=str(source), starting_username=None)
    else:
        config.update(input_source="username", starting_username=ask_username("Enter starting TikTok username: "), source_json_path=None)
    config["target_username"] = ask_username(f"Target TikTok username [default: {TARGET_USER}]: ", TARGET_USER)
    config = edit_scan_config(config, new=True)
    root = create_search_directory(base, config["target_username"])
    resources.enter_context(ScanLock(root))
    identity = source_identity(config["input_source"], config["starting_username"] or config["source_json_path"], config["target_username"])
    identity["worker_origin"] = config["worker_origin"]
    metadata = {"session_id": uuid.uuid4().hex, "layout_version": 3, "search_folder": root.name,
                "identity": identity, "created_at_utc": utc_iso(), "updated_at_utc": utc_iso(),
                "network_mode": "webshare" if config["use_proxies"] else "direct", "scan_complete": False,
                "input_imported": False, "current_phase": 1}
    atomic_write_json(root / "session.json", metadata)
    if source_document is not None:
        atomic_write_json(root / config["starting_json"], source_document)
    save_scan_config(root, config)
    config = confirmation_menu(root, config, metadata, resume=False)
    if config is None:
        metadata["setup_cancelled"] = True
        atomic_write_json(root / "session.json", metadata)
        return "back"
    return root, config, metadata


async def bootstrap_dataset(
    client: WorkerApiClient, seed: str, *, root: Path, state: DurableScanState,
    success: SuccessRecorder, stop_event: asyncio.Event,
) -> Path:
    client.bootstrap_mode = True
    directory = root / "bootstrap"
    directory.mkdir(exist_ok=True)
    job = ProfileJob(seed, f"{TIKTOK_ORIGIN}/@{seed}", ("bootstrap",))
    result = await process_profile(client, job, output_directory=directory,
        success_recorder=success, stop_event=stop_event, use_resume=True,
        store_directory=directory / "pages",
        on_discovery=lambda name, rows: disk_call(state.discover, seed, name, rows, phase=0))
    if stop_event.is_set():
        limited = bool(getattr(getattr(client, 'gate', None), 'rate_limit_exhausted', False))
        state.metadata["bootstrap"] = {"status":"stopped_rate_limited" if limited else "cancelled", "username":seed,
                                      "list_results":result.list_results, "completed_at_utc":utc_iso()}
        await disk_call(state.snapshot)
        if limited:
            raise WorkerApiError('HTTP 429 received from API; bootstrap stopped.', kind='rate_limited', code=429)
        raise asyncio.CancelledError
    usable = [name for name in SELECTED_LISTS if result.list_results.get(name, {}).get("complete") is True]
    restricted = [name for name in SELECTED_LISTS if explicit_restriction(result.list_results.get(name, {}))]
    warning = any(info.get("warnings") for info in result.list_results.values())
    genuine_failure = len(usable) + len(restricted) < len(SELECTED_LISTS)
    status = ("incomplete" if genuine_failure else "unavailable" if not usable
              else "usable_with_restricted_list" if restricted
              else "complete_with_count_warning" if warning else "complete")
    state.metadata["bootstrap"] = {"status": status, "list_results": result.list_results,
                                  "completed_at_utc": utc_iso(), "username": seed}
    await disk_call(state.snapshot)
    if genuine_failure:
        reasons = "; ".join(f"{name}: {info['stop_reason']}" for name, info in result.list_results.items())
        raise ExporterError(f"Bootstrap is incomplete ({reasons}). Committed pages and exact cursors are preserved.")
    for name in SELECTED_LISTS:
        info = result.list_results[name]
        if name in restricted:
            console(f"[BOOTSTRAP WARNING] {name.title()} unavailable: restricted ({info.get('visibility_reason') or info.get('error')}).")
        else:
            console(f"[BOOTSTRAP] {name.title()} usable: {info.get('unique_records_saved', 0)}; advertised={info.get('advertised_count')}; {info['stop_reason']}.")
    if not usable:
        console("[BOOTSTRAP] No accessible source lists are available to build the starting dataset.")
    elif restricted:
        console("[BOOTSTRAP] Continuing with available source profiles.")
    else:
        console("[BOOTSTRAP COMPLETE WITH WARNING]" if warning else "[BOOTSTRAP COMPLETE]")
    def write_dataset() -> None:
        def rows() -> Iterator[dict[str, Any]]:
            query = "SELECT d.username,d.uid,d.payload FROM discoveries d WHERE d.public=1 AND EXISTS (SELECT 1 FROM discovery_sources s WHERE s.username=d.username AND s.owner=?) ORDER BY d.rowid"
            for name, uid, payload in state.connection.execute(query, (seed.casefold(),)):
                item = json.loads(payload)
                kinds = [row[0] for row in state.connection.execute("SELECT list_name FROM discovery_sources WHERE username=? AND owner=? ORDER BY list_name", (name, seed.casefold()))]
                yield {**item, "id": uid, "uid": uid, "username": name, "follower_count": item.get("follower_count"),
                       "following_count": item.get("following_count"), "counts_available": False,
                       "source_lists": kinds, "from_followers": "followers" in kinds, "from_following": "following" in kinds,
                       "discovered_through_both": set(SELECTED_LISTS).issubset(kinds),
                       "discovered_from": [f"@{seed}/{kind}" for kind in kinds]}
        with state.lock:
            write_json_array(root / "starting_dataset.json", rows() if usable else [])
    await disk_call(write_dataset)
    available_jobs, _ = load_jobs_allow_empty(root / "starting_dataset.json")
    if not available_jobs and usable:
        state.metadata["bootstrap"]["status"] = "empty_dataset"
        console("[BOOTSTRAP] Completed successfully, but no usable source profiles were found.")
    elif available_jobs:
        console(f"[BOOTSTRAP] Starting main scan with {len(available_jobs):,} public, deduplicated profiles...")
    await disk_call(state.snapshot)
    console(f"[DISCOVERED] Saved the public-only starting dataset: {root / 'starting_dataset.json'}")
    return root / "starting_dataset.json"


def load_jobs_allow_empty(path: Path) -> tuple[list[ProfileJob], dict[str, Any]]:
    document = json.loads(path.read_text(encoding="utf-8-sig"))
    if document == [] or (isinstance(document, dict) and document.get("followers") == [] and document.get("following") == []):
        return [], {"source_records": 0, "unique_profiles": 0}
    return load_profile_jobs(path)


async def _execute_session(
    *, root: Path, state: DurableScanState, source_type: str, input_path: Path, seed: str | None,
    success: SuccessRecorder, pool: ProxyPool, pacing: tuple[float, float],
) -> tuple[dict[str, Any], dict[str, Any], str | None]:
    config = state.config
    if not state.metadata.get("input_imported"):
        if source_type == "username":
            stop_event = asyncio.Event()
            gate = AsyncRequestGate(*pacing, stop_event, ceiling=1, initial=1, adaptive=False)
            gate.stats = _STATS.get()
            if gate.stats:
                gate.stats.gate = gate
            from hybrid_backend import create_client
            async with create_client(gate, pool, settings=config, raw_directory=root / "raw_api" if config["keep_raw"] else None,
                                       avatar_directory=root) as client:
                input_path = await bootstrap_dataset(client, str(seed), root=root, state=state, success=success, stop_event=stop_event)
                await gate.wait(pacing[1])  # Preserve spacing across the client boundary.
        elif not input_path.is_file():
            # One-time migration of an older scan that had not imported input.
            source = Path(str(config["source_json_path"]))
            document = await disk_call(lambda: json.loads(source.read_text(encoding="utf-8-sig")))
            await disk_call(atomic_write_json, input_path, document)
        jobs, stats = await disk_call(load_jobs_allow_empty, input_path)
        await disk_call(state.add_jobs, jobs)
        del jobs  # The durable queue now owns the input; don't retain 180k jobs.
        if 'document' in locals():
            del document
        state.metadata.update(input_imported=True, source_input_json=str(input_path), input=stats)
        await disk_call(state.snapshot)
    elif not input_path.exists():
        # Older resumed runs may have only a durable imported jobs table.
        await disk_call(lambda: write_json_array(input_path,
                        (json.loads(row[0]) for row in state.connection.execute("SELECT job FROM jobs WHERE phase=1 ORDER BY rowid"))))
    await disk_call(state.recover, success)
    if _RETRY_FAILED.pop(str(root), False):
        retried = await disk_call(state.retry_failed)
        console(f'[RESUME] Requeued {retried:,} failed/partial profiles; completed profiles and all saved evidence retained.')
    summary = await disk_call(state.summary)
    console(f"[PROGRESS {summary['processed_profiles']:,}/{summary['total_profiles']:,}] "
            f"Checked overall; reused from previous run: {summary['already_processed']:,}; "
            f"remaining: {summary['remaining_profiles']:,}; current phase: {state.current_phase}.")
    # Preserve the requested count, while reporting the separate safe runtime cap.
    workers = config["workers"]
    console(f"[ASYNC] Configured workers: {workers}; global request spacing {pacing[0]}-{pacing[1]}s.")
    console(f"[PHASE {state.current_phase}] Starting/resuming {'double-phase' if config['scan_mode']=='double_phase' else 'normal'} scan.")
    return await run_scan([], output_directory=root, worker_count=workers, pacing=pacing,
        success_recorder=success, state_recorder=state, use_resume=True,
        pool=pool, initial_concurrency=workers, adaptive=True)


async def execute_session(*, root, state, **kwargs):
    # Includes bootstrap requests, recovery, and shutdown in the same session stats.
    async with session_runtime(state, root, state.config['workers']):
        return await _execute_session(root=root, state=state, **kwargs)


def main() -> int:
    global REDACTOR, _STARTUP_LOG, KEEP_RAW_MEMBER_DATA
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="backslashreplace")
    source_selection = False
    original_raw = KEEP_RAW_MEMBER_DATA
    while True:
        REDACTOR = CredentialRedactor()
        _STARTUP_LOG = []
        console(APP_TITLE + "\nWorker API | saved per-search configuration | one optional expansion phase")
        state: DurableScanState | None = None
        resources = ExitStack()
        root: Path | None = None
        success: SuccessRecorder | None = None
        summary_printed = False
        try:
            base = configured_output_directory(configured_input_path()).resolve()
            selected = choose_startup(base, resources, source_selection=source_selection)
            source_selection = False
            if selected == "quit": return 0
            if selected in ("back", "source"):
                source_selection = selected == "source"
                continue
            root, config, metadata = selected
            resources.enter_context(RunLog(root))
            KEEP_RAW_MEMBER_DATA = config["keep_raw"]
            console(f"Search folder: {root}")
            state = DurableScanState(root, metadata, root, config)
            pool = _PREPARED_POOLS.pop(str(root), None)
            if pool is None:
                raise ExporterError("Network configuration was not prepared before confirmation.")
            metadata["network_mode"] = "webshare" if pool.available() else "direct"
            input_path = root / config["starting_json"]
            success_path = root / SUCCESS_FILE_NAME
            success_source = Path(config.get("success_source") or str(input_path))
            success = SuccessRecorder(success_path, target_user=config["target_username"], input_path=success_source, started_at_utc=utc_iso())
            pacing = (config["request_delay"]["minimum_seconds"], config["request_delay"]["maximum_seconds"])
            from scan_runtime import ShutdownGuard
            guard = resources.enter_context(ShutdownGuard(console))
            with asyncio.Runner() as runner:
                loop = runner.get_loop()
                async def start() -> tuple[dict[str, Any], dict[str, Any], str | None]:
                    session = asyncio.current_task()
                    session.set_name("scanner-session")
                    guard.cancel = lambda: loop.call_soon_threadsafe(session.cancel)
                    try:
                        return await execute_session(root=root, state=state, source_type=config["input_source"],
                            input_path=input_path, seed=config["starting_username"], success=success, pool=pool, pacing=pacing)
                    finally:
                        guard.cancel = None
                success_document, state_document, fatal = runner.run(start())
            print_final_summary(success_document=success_document, state_document=state_document,
                                success_path=success_path, state_path=state.path, output_directory=root)
            summary_printed = True
            return 2 if fatal else (0 if state_document.get("scan_complete") else 5)
        except (KeyboardInterrupt, asyncio.CancelledError):
            # Async cleanup already persisted the state. Do not regenerate every
            # final JSON a second time unless interruption preceded that cleanup.
            if state is not None and not getattr(state, '_interrupted_finalized', False):
                state.finalize(scan_complete=False, fatal_error="Interrupted by the user.")
            console("[SHUTDOWN] Stopped. Configuration, progress, queues, matches and exact cursors are preserved.")
            return 130
        except (PublicProfileRequired, WorkerApiError) as exc:
            rejected = isinstance(exc, PublicProfileRequired) or (isinstance(exc, WorkerApiError) and exc.kind in {"not_found", "invalid_username"})
            if state is not None:
                state.metadata["setup_rejected"] = rejected and not state.metadata.get("input_imported")
                state.finalize(scan_complete=False, fatal_error=REDACTOR.text(exc))
            console(f"[PRIVATE] {exc}" if isinstance(exc, PublicProfileRequired) else f"[HTTP_ERROR] {exc}", error=True)
            if rejected and state is not None and not state.metadata.get("input_imported"):
                console("That starting profile cannot be used. Returning to starting-source selection.")
                source_selection = True
                continue
            return 2
        except (ExporterError, OSError, sqlite3.Error, EOFError, ValueError) as exc:
            if state is not None: state.finalize(scan_complete=False, fatal_error=REDACTOR.text(exc))
            console(f"[ERROR] {exc}", error=True)
            return 2
        finally:
            try:
                if state is not None:
                    if success is not None:
                        try:
                            final_state = load_json_document(state.path)
                            final_success = success.finalize(scan_complete=bool(final_state.get("scan_complete")))
                            if not summary_printed:
                                print_final_summary(success_document=final_success, state_document=final_state,
                                    success_path=success.path, state_path=state.path, output_directory=root)
                            from report_generator import generate_report
                            generate_report(root, clean=REDACTOR.clean, emit=console)
                        except Exception as exc:
                            console(f"[REPORT_ERROR] Failed to generate HTML report: {type(exc).__name__}.", error=True)
                    state.close()
            finally:
                resources.close()
                KEEP_RAW_MEMBER_DATA = original_raw
                _STARTUP_LOG = None


if __name__ == "__main__":
    raise SystemExit(main())
