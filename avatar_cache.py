"""Bounded raster avatar cache. Reports read it offline; only the scanner fetches.

URLs come from the confirmed Worker fields, never constructed TikTok URLs.
Filenames and the index contain hashes, not URLs, usernames or authentication.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import os
import re
import sqlite3
import time
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable
from urllib.parse import urlsplit

MAX_DOWNLOAD_BYTES = 2 * 1024 * 1024
MAX_IMAGE_PIXELS = 4_000_000
MAX_CACHE_BYTES = 32 * 1024
AVATAR_SIZE = 96
FAILURE_COOLDOWN = 3600
ASSET_DIRECTORY = 'report_assets/avatars'
CDN_DOMAINS = ('tiktokcdn.com', 'tiktokcdn-us.com', 'tiktokcdn-eu.com',
               'byteoversea.com', 'ibytedtos.com', 'ibyteimg.com', 'byteimg.com', 'muscdn.com')
DATA_IMAGE = re.compile(r'data:image/(?:png|jpeg|webp|gif);base64,([A-Za-z0-9+/=\s]+)')


def digest(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def account_key(uid: Any, username: str) -> str:
    identity = 'id:' + str(uid) if str(uid or '').isascii() and str(uid).isdigit() else 'name:' + username.casefold()
    return digest(identity)


def allowed_avatar_url(value: Any) -> bool:
    if not isinstance(value, str) or not value or len(value) > 8192:
        return False
    if any(ord(c) < 33 for c in value) or '\\' in value:
        return False
    try:
        url = urlsplit(value)
        host = (url.hostname or '').casefold()
        return (url.scheme == 'https' and url.username is None and url.password is None
                and url.port in (None, 443) and not url.fragment
                and any(host == domain or host.endswith('.' + domain) for domain in CDN_DOMAINS))
    except ValueError:
        return False


def thumbnail(data: bytes) -> bytes:
    """Fully decode, limit dimensions, strip metadata, then encode one small JPEG."""
    from PIL import Image, ImageOps
    if not data or len(data) > MAX_DOWNLOAD_BYTES:
        raise ValueError('avatar_size')
    with Image.open(io.BytesIO(data), formats=('JPEG', 'PNG', 'WEBP', 'GIF')) as original:
        if original.width * original.height > MAX_IMAGE_PIXELS:
            raise ValueError('avatar_pixels')
        original.verify()
    with Image.open(io.BytesIO(data), formats=('JPEG', 'PNG', 'WEBP', 'GIF')) as original:
        original.seek(0)  # Animated images contribute only their first frame.
        original.load()
        oriented = ImageOps.exif_transpose(original)
        oriented.thumbnail((AVATAR_SIZE, AVATAR_SIZE), Image.Resampling.LANCZOS)
        rgba = oriented.convert('RGBA')
        flattened = Image.new('RGB', rgba.size, (27, 25, 36))
        flattened.paste(rgba, mask=rgba.getchannel('A'))
        flattened.info.clear()
        output = io.BytesIO()
        flattened.save(output, format='JPEG', quality=82, optimize=True)
    result = output.getvalue()
    if len(result) > MAX_CACHE_BYTES:
        raise ValueError('avatar_output_size')
    return result


def embedded_avatar(value: Any) -> str:
    """Backward compatibility for already embedded images, with real validation."""
    if not isinstance(value, str) or len(value) > MAX_DOWNLOAD_BYTES * 2:
        return ''
    match = DATA_IMAGE.fullmatch(value)
    if not match:
        return ''
    try:
        raw = base64.b64decode(re.sub(r'\s', '', match[1]), validate=True)
        return 'data:image/jpeg;base64,' + base64.b64encode(thumbnail(raw)).decode('ascii')
    except Exception:
        return ''


def valid_cached_image(path: Path) -> bool:
    try:
        from PIL import Image
        if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= MAX_CACHE_BYTES:
            return False
        with Image.open(path, formats=('JPEG',)) as img:
            if not 0 < img.width <= AVATAR_SIZE or not 0 < img.height <= AVATAR_SIZE:
                return False
            img.load()
        return True
    except Exception:
        return False


def write_image(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.jpg.tmp')
    try:
        with temporary.open('wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class AvatarReader:
    """Read-only local lookup. Missing/corrupt caches never prevent a report."""
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.directory = self.root / ASSET_DIRECTORY
        self.db = None
        self.verified: dict[str, str] = {}
        self.safe_directory = self.directory.resolve().is_relative_to(self.root)
        index = self.directory / 'cache.sqlite3'
        if self.safe_directory and index.is_file() and not index.is_symlink():
            try:
                self.db = sqlite3.connect(index.resolve().as_uri() + '?mode=ro', uri=True, check_same_thread=False)
            except sqlite3.Error:
                pass

    def close(self) -> None:
        if self.db is not None:
            self.db.close()
            self.db = None

    def reference(self, uid: Any, username: str, url: Any) -> str:
        if not self.safe_directory:
            return ''
        keys = [digest(url)] if isinstance(url, str) and url else []
        if self.db is not None:
            try:
                row = self.db.execute('SELECT source_key FROM accounts WHERE identity=?', (account_key(uid, username),)).fetchone()
                if row and row[0] not in keys:
                    keys.append(row[0])
            except sqlite3.Error:
                pass
        for key in keys:
            if not isinstance(key, str) or not re.fullmatch(r'[a-f0-9]{64}', key):
                continue
            if key not in self.verified:
                self.verified[key] = (ASSET_DIRECTORY + '/' + key + '.jpg'
                                      if valid_cached_image(self.directory / (key + '.jpg')) else '')
            if self.verified[key]:
                return self.verified[key]
        return ''


class AvatarCache:
    def __init__(self, root: Path, download: Callable[[str], Awaitable[bytes]], *, disk_io=None, concurrency=4):
        self.root, self.download = root.resolve(), download
        self.directory = self.root / ASSET_DIRECTORY
        self.reader = AvatarReader(root)
        self.db = None
        self.attempted: set[str] = set()
        self.user_locks = {}
        self.url_locks = {}
        self.slots = asyncio.Semaphore(max(1, concurrency))
        self.disk_io = disk_io
        self.io_lock = asyncio.Lock()

    @asynccontextmanager
    async def key_lock(self, locks, key):
        entry = locks.setdefault(key, [asyncio.Lock(), 0])
        entry[1] += 1
        try:
            async with entry[0]: yield
        finally:
            entry[1] -= 1
            if not entry[1]: locks.pop(key, None)

    async def io(self, function, *args):
        # A cache connection is never used concurrently, including by to_thread.
        async with self.io_lock:
            if self.disk_io:
                return await self.disk_io(function, *args)
            task = asyncio.create_task(asyncio.to_thread(function, *args))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                await asyncio.shield(task)
                raise

    def close(self) -> None:
        self.reader.close()
        if self.db is not None:
            self.db.close()
            self.db = None

    def database(self):
        if self.db is None:
            if not self.reader.safe_directory:
                raise ValueError('unsafe_avatar_directory')
            self.directory.mkdir(parents=True, exist_ok=True)
            path = self.directory / 'cache.sqlite3'
            if path.is_symlink():
                raise ValueError('unsafe_avatar_index')
            self.db = sqlite3.connect(path, check_same_thread=False)
            self.db.execute('PRAGMA synchronous=FULL')
            self.db.executescript('CREATE TABLE IF NOT EXISTS accounts(identity TEXT PRIMARY KEY, source_key TEXT NOT NULL);'
                                 'CREATE TABLE IF NOT EXISTS failures(source_key TEXT PRIMARY KEY, retry_after REAL NOT NULL);')
            self.reader.close()
            self.reader = AvatarReader(self.root)
        return self.db

    def remember(self, identity: str, key: str) -> None:
        db = self.database()
        row = db.execute('SELECT source_key FROM accounts WHERE identity=?', (identity,)).fetchone()
        if row != (key,):
            with db:
                db.execute('INSERT OR REPLACE INTO accounts VALUES(?,?)', (identity, key))

    def prepare(self, identity, key, uid, username, url):
        self.reader.verified.pop(key, None)
        existing = self.reader.reference(uid, username, url)
        db = self.database()
        if existing:
            self.remember(identity, Path(existing).stem)
            return existing, False
        previous = db.execute('SELECT retry_after FROM failures WHERE source_key=?', (key,)).fetchone()
        return '', bool(previous and previous[0] > time.time())

    def committed(self, identity, key, uid, username, url):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO accounts VALUES(?,?)', (identity, key))
            self.db.execute('DELETE FROM failures WHERE source_key=?', (key,))
        self.reader.verified.pop(key, None)
        return self.reader.reference(uid, username, url)

    def failed(self, key):
        if self.db is not None:
            try:
                with self.db:
                    self.db.execute('INSERT OR REPLACE INTO failures VALUES(?,?)', (key, time.time() + FAILURE_COOLDOWN))
            except sqlite3.Error:
                pass

    async def ensure(self, uid: Any, username: str, url: Any) -> str:
        """Single-flight per account and URL; failure remains a harmless placeholder."""
        identity = account_key(uid, username)
        async with self.key_lock(self.user_locks, identity):
            existing = await self.io(self.reader.reference, uid, username, url)
            if existing:
                try:
                    await self.io(self.remember, identity, Path(existing).stem)
                except Exception:
                    pass
                return existing
            if not isinstance(url, str) or not url:
                return ''
            embedded = DATA_IMAGE.fullmatch(url) if len(url) <= MAX_DOWNLOAD_BYTES * 2 else None
            if not embedded and not allowed_avatar_url(url):
                return ''
            key = digest(url)
            async with self.key_lock(self.url_locks, key):
                # Another account may have just downloaded this exact URL.
                try:
                    existing, waiting = await self.io(self.prepare, identity, key, uid, username, url)
                    if existing:
                        return existing
                    if key in self.attempted or waiting:
                        return ''
                    self.attempted.add(key)
                    async with self.slots:
                        raw = (base64.b64decode(re.sub(r'\s', '', embedded[1]), validate=True)
                               if embedded else await self.download(url))
                        # Cancellation waits for the bounded disk operation to finish;
                        # the index is updated only after a valid file is in place.
                        task = asyncio.create_task(asyncio.to_thread(self._store, key, raw))
                        try:
                            await asyncio.shield(task)
                        except asyncio.CancelledError as cancelled:
                            try:
                                await asyncio.shield(task)
                            except Exception:
                                pass
                            raise cancelled
                    return await self.io(self.committed, identity, key, uid, username, url)
                except asyncio.CancelledError:
                    self.attempted.discard(key)
                    raise
                except Exception:
                    # No URL, credentials, library exception text, or raw response
                    # body is logged or stored. Retry on a later run after cooldown.
                    await self.io(self.failed, key)
                    return ''

    def _store(self, key: str, raw: bytes) -> None:
        write_image(self.directory / (key + '.jpg'), thumbnail(raw))
