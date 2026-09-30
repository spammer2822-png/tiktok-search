"""Session counters and bounded one-second buckets for rolling 300s metrics."""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections import Counter, deque
from datetime import datetime, timedelta, timezone


class ScanStatistics:
    def __init__(self, root, configured_workers, *, clock=time.monotonic, wall=time.time):
        self.root = root
        self.configured_workers = configured_workers
        self.clock, self.wall = clock, wall
        self.started = clock()
        self.started_at = self.iso(wall())
        self.session_id = uuid.uuid4().hex
        self.totals = Counter()
        self.active_requests = self.peak_in_flight = 0
        self.buckets = deque()
        self.accounts = set()
        self.status = 'running'
        self.stop_reason = None
        self.stopped_at = None
        self.gate = None
        self.done = asyncio.Event()

    @staticmethod
    def iso(stamp):
        return datetime.fromtimestamp(stamp, timezone.utc).isoformat()

    def add(self, **values):
        now = int(self.clock())
        if not self.buckets or self.buckets[-1][0] != now:
            self.buckets.append((now, Counter()))
        self.totals.update(values)
        self.buckets[-1][1].update(values)
        self.prune()

    def prune(self):
        cutoff = self.clock() - 300
        while self.buckets and self.buckets[0][0] + 1 <= cutoff:
            self.buckets.popleft()

    def request_started(self, retry=False):
        self.active_requests += 1
        self.peak_in_flight = max(self.peak_in_flight, self.active_requests)
        self.add(requests_total=1, retry_attempts=int(retry))

    def request_finished(self, *, success, latency, kind=None, code=None, retry=False, cancelled=False):
        self.active_requests = max(0, self.active_requests - 1)
        values = {'latency_seconds': max(0, latency), 'requests_finished': 1,
                  'requests_cancelled' if cancelled else 'requests_successful' if success else 'requests_failed': 1}
        if success and retry:
            values['recovered_after_retry'] = 1
        if code == 429:
            values['http_429_count'] = 1
        if isinstance(code, int) and code >= 500:
            values['http_5xx_count'] = 1
        if kind in ('response_timeout', 'network_timeout'):
            values['network_timeouts'] = 1
        if kind == 'network_error':
            values['network_errors'] = 1
        if kind and kind.startswith('proxy_'):
            values['proxy_errors'] = 1
        self.add(**values)

    def account_finished(self, result):
        if result.status in ('cancelled', 'pending', 'in_progress', 'rate_limited'):
            return
        key = result.username.casefold()
        if key in self.accounts:
            return
        self.accounts.add(key)
        skipped = result.status.startswith('skipped') or result.status in ('private', 'not_found', 'invalid_username')
        restricted = result.status == 'restricted'
        self.add(accounts_processed=1, successful_accounts=int(result.complete),
                 failed_accounts=int(not result.complete and not skipped and not restricted),
                 skipped_accounts=int(skipped), restricted_accounts=int(restricted),
                 partial_profiles=int(result.status == 'partial'))

    def stop(self, status, reason=None):
        # Never downgrade the primary reason during cancellation/finalization.
        if self.status == 'stopped_rate_limited' and status != self.status:
            return
        self.status, self.stop_reason = status, reason
        if status != 'running' and self.stopped_at is None:
            self.stopped_at = self.iso(self.wall())

    def snapshot(self, summary):
        self.prune()
        recent = Counter()
        for _, bucket in self.buckets:
            recent.update(bucket)
        elapsed = max(0, self.clock() - self.started)
        window = min(300, elapsed)
        def rate(count, seconds):
            return round(count / seconds, 4) if seconds > 0 else 0.0
        def metrics(counts, seconds):
            keys = ('requests_total', 'requests_successful', 'requests_failed', 'requests_cancelled',
                    'retry_attempts', 'recovered_after_retry', 'network_timeouts', 'network_errors',
                    'proxy_errors', 'http_429_count', 'http_5xx_count', 'accounts_processed',
                    'successful_accounts', 'failed_accounts', 'skipped_accounts', 'restricted_accounts', 'partial_profiles')
            data = {key: counts[key] for key in keys}
            data.update(requests_per_second=rate(counts['requests_total'], seconds),
                        successful_requests_per_second=rate(counts['requests_successful'], seconds),
                        failed_requests_per_second=rate(counts['requests_failed'], seconds),
                        accounts_per_second=rate(counts['accounts_processed'], seconds),
                        successful_accounts_per_second=rate(counts['successful_accounts'], seconds),
                        average_request_latency_ms=round(1000 * counts['latency_seconds'] / counts['requests_finished'], 2)
                        if counts['requests_finished'] else None)
            return data
        lifetime, rolling = metrics(self.totals, elapsed), metrics(recent, window)
        remaining = summary.get('remaining_profiles', 0)
        throughput = recent['successful_accounts'] / window if window else 0
        eta = (remaining / throughput if self.status == 'running' and elapsed >= 30
               and recent['successful_accounts'] >= 5 and throughput > 0 and remaining > 0 else None)
        gate = self.gate
        disk = getattr(self, 'disk_lane', None)
        doc = {**lifetime, 'schema_version': 1, 'session_id': self.session_id,
               'scan_status': self.status, 'stop_reason': self.stop_reason, 'stopped_at': self.stopped_at,
               'started_at': self.started_at, 'last_updated_at': self.iso(self.wall()),
               'elapsed_seconds': round(elapsed, 3), 'configured_workers': self.configured_workers,
               'effective_concurrency': gate.limit if gate else 0,
               'connection_ceiling': gate.ceiling if gate else 0,
               'active_requests': self.active_requests, 'current_phase': summary.get('current_phase'),
               'peak_in_flight': self.peak_in_flight,
               'active_request_slots': gate.active if gate else 0,
               'profile_io_workers': disk.io_limit if disk else 0,
               'profile_io_ceiling': disk.io_ceiling if disk else 0,
               'profile_io_active': disk.io_active if disk else 0,
               'profile_io_waiting': disk.io_waiting if disk else 0,
               'profile_io_peak': disk.io_peak if disk else 0,
               'total_accounts_discovered': summary.get('total_profiles', 0),
               'discovered_public_accounts': summary.get('discovered_profiles', 0),
               'total_accounts_processed': summary.get('processed_profiles', 0),
               'pending_accounts': remaining, 'overall_successful_accounts': summary.get('completed_overall', 0),
               'overall_failed_accounts': summary.get('failed_profiles', 0),
               'overall_statuses': summary.get('statuses', {}), 'lifetime': lifetime,
               'rolling_5_minutes': {**rolling, 'window_seconds': round(window, 3)},
               'accounts_last_5_minutes': recent['accounts_processed'],
               'successful_accounts_last_5_minutes': recent['successful_accounts'],
               'failed_accounts_last_5_minutes': recent['failed_accounts'],
               'successful_requests_last_5_minutes': recent['requests_successful'],
               'requests_last_5_minutes': recent['requests_total'],
               'failed_requests_last_5_minutes': recent['requests_failed'],
               'successful_requests_per_second_last_5_minutes': rolling['successful_requests_per_second'],
               'average_accounts_per_second_last_5_minutes': rolling['accounts_per_second'],
               'average_successful_requests_per_second_last_5_minutes': rolling['successful_requests_per_second'],
               'average_failed_requests_per_second_last_5_minutes': rolling['failed_requests_per_second'],
               'estimated_accounts_remaining': remaining, 'estimated_seconds_remaining': round(eta) if eta is not None else None,
               'estimated_completion_time': self.iso(self.wall() + eta) if eta is not None else None,
               'eta_basis': 'recent successful profiles; excludes skips, failures and restricted profiles',
               'request_scope': 'Worker API attempts only; avatars and proxy validation are excluded'}
        if getattr(self, 'backends', None) is not None:
            doc.update(self.backends.snapshot())
            doc['request_scope'] = 'Worker and Direct API attempts; avatars and proxy validation excluded'
        if disk is not None:
            doc['persistence_timings'] = dict(getattr(disk, 'timings', {}))
        return doc

    def write(self, doc, atomic_write, *, history=False):
        atomic_write(self.root / 'scan_stats.json', doc)
        if history:
            path = self.root / 'scan_stats_history.jsonl'
            # Repair only a torn final line from a forced kill; older lines stay.
            if path.exists():
                with path.open('rb+') as stream:
                    stream.seek(0, 2)
                    size = stream.tell()
                    if size:
                        stream.seek(size - 1)
                        if stream.read(1) != b'\n':
                            position = size
                            end = 0
                            while position:
                                start = max(0, position - 65536)
                                stream.seek(start)
                                chunk = stream.read(position - start)
                                found = chunk.rfind(b'\n')
                                if found >= 0:
                                    end = start + found + 1
                                    break
                                position = start
                            stream.truncate(end)
            with path.open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(doc, separators=(',', ':'), ensure_ascii=True) + '\n')
                stream.flush()
                os.fsync(stream.fileno())

    @staticmethod
    def display(doc, emit, detailed=False):
        r = doc['rolling_5_minutes']
        eta = doc['estimated_seconds_remaining']
        eta_text = f'{eta // 3600}h {eta % 3600 // 60}m' if eta is not None else 'unavailable'
        if not detailed:
            emit(f"[LIVE] Phase {doc.get('current_phase') or '?'} | "
                 f"Processed {doc['total_accounts_processed']:,}/{doc['total_accounts_discovered']:,} | "
                 f"OK {doc['overall_successful_accounts']:,} | Failed {doc['overall_failed_accounts']:,} | "
                 f"Partial {doc['overall_statuses'].get('partial',0):,} | Restricted {doc['overall_statuses'].get('restricted',0):,} | "
                 f"Pending {doc['pending_accounts']:,}\n"
                 f"[LIVE] Requests {doc['requests_total']:,} (OK {doc['requests_successful']:,}, failed {doc['requests_failed']:,}, "
                 f"retries {doc['retry_attempts']:,}) | {r['requests_per_second']:.2f} req/s / "
                 f"{r['successful_requests_per_second']:.2f} successful req/s | "
                 f"{r['accounts_per_second']:.2f} accounts/s / {r['accounts_per_second']*60:.1f} accounts/min | "
                 f"In flight {doc['active_requests']} | Limit {doc['effective_concurrency']}/{doc['connection_ceiling']} | "
                 f"Workers {doc['configured_workers']} | I/O writers {doc['profile_io_active']}/{doc['profile_io_workers']} | Elapsed {doc['elapsed_seconds']:.0f}s | ETA {eta_text}")
            if 'hybrid_total_successful_rps' in doc:
                lines = [f"{name.upper():6} | {doc[name+'_successful_rps']:.1f} success/s | "
                         f"{doc[name+'_records_per_second']:.1f} users/s | "
                         f"{1000*doc[name+'_average_latency']:.0f}ms | "
                         f"{100-doc[name+'_recent_success_percent']:.1f}% unsuccessful | "
                         f"{100*doc[name+'_risk_control_rate']:.1f}% risk"
                         for name in ('worker', 'direct')]
                lines.append(f"TOTAL  | {doc['hybrid_total_successful_rps']:.1f} success/s | "
                             f"{doc['hybrid_records_per_second']:.1f} users/s")
                emit('\n'.join(lines))
            return
        lines = ['========== 5 MINUTE SCAN STATS ==========']
        for key, value in r.items():
            lines.append(f'{key.replace("_", " ").capitalize():40} {value}')
        lines.extend([f"Configured workers / effective: {doc['configured_workers']} / {doc['effective_concurrency']}",
                      f"Total processed / remaining: {doc['total_accounts_processed']:,} / {doc['pending_accounts']:,}",
                      f"Estimated remaining time: {eta_text}",
                      f"Estimated completion: {doc['estimated_completion_time'] or 'unavailable'}",
                      '========================================'])
        emit('\n'.join(lines))

    async def report_loop(self, state, disk_call, atomic_write, emit):
        next_write = self.clock() + 15
        next_history = self.clock() + 300
        while not self.done.is_set():
            try:
                await asyncio.wait_for(self.done.wait(), timeout=1)
                break
            except asyncio.TimeoutError:
                pass
            summary = await disk_call(state.summary)
            if self.done.is_set():
                break
            doc = self.snapshot(summary)
            self.display(doc, emit)
            now = self.clock()
            if now >= next_write or now >= next_history:
                history = now >= next_history
                if hasattr(state, 'snapshot'):
                    await disk_call(state.snapshot)
                await disk_call(self.write, doc, atomic_write, history=history)
                emit('[CHECKPOINT] Progress and scan_stats.json saved; committed page cursors remain durable.')
                next_write = now + 15
                if history:
                    self.display(doc, emit, detailed=True)
                    next_history = now + 300
