"""Requirements discovered in the final review: healthy-load adaptation and telemetry."""
import asyncio
import json
import unittest
from unittest.mock import patch
from collections import Counter
import httpx
import hybrid_backend as h
import tiktok_worker_scanner as s
from backend_metrics import BackendMetrics
from scan_statistics import ScanStatistics
import test_hybrid_backend as fixtures


class AuditTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.HybridTests.asyncSetUp
    asyncTearDown = fixtures.HybridTests.asyncTearDown
    make = fixtures.HybridTests.make

    def test_resume_preview_closes_database_on_success_and_error(self):
        connect = s.sqlite3.connect
        db = connect(self.root/'state.sqlite3')
        db.execute('CREATE TABLE jobs(status TEXT)')
        db.execute("INSERT INTO jobs VALUES ('pending')")
        db.commit()
        db.close()
        for valid in (True, False):
            connections = []
            def tracked(*args, **kwargs):
                connection = connect(*args, **kwargs)
                connections.append(connection)
                return connection
            with patch.object(s.sqlite3, 'connect', side_effect=tracked):
                if valid:
                    self.assertEqual(s.resume_progress(self.root, {'scan_mode':'normal'}, {})['summary']['remaining_profiles'], 1)
                else:
                    with self.assertRaises(s.ExporterError):
                        s.resume_progress(self.root, {'scan_mode':'normal'}, {})
            self.assertEqual(len(connections), 1)
            with self.assertRaises(s.sqlite3.ProgrammingError):
                connections[0].execute('SELECT 1')
            if valid:
                db = connect(self.root/'state.sqlite3')
                db.execute('DROP TABLE jobs')
                db.commit()
                db.close()

    async def test_latency_spike_reduces_direct_even_without_errors(self):
        metrics = BackendMetrics()
        gate = h.BackendGate(self.gate, 'direct', self.settings, metrics)
        metrics.totals['direct'].update(requests_total=10, requests_successful=10, records=350, latency_seconds=1)
        with patch.object(h.time, 'monotonic', return_value=gate.last_adjustment+2):
            gate.observe(.1, None)
        metrics.totals['direct'].update(requests_total=10, requests_successful=10, records=350, latency_seconds=4)
        with patch.object(h.time, 'monotonic', return_value=gate.last_adjustment+2):
            gate.observe(.4, None)
        self.assertEqual(gate.limit, 50)
        metrics.totals['direct'].update(requests_total=10, requests_successful=10, records=350, latency_seconds=1)
        with patch.object(h.time, 'monotonic', return_value=gate.last_adjustment+2):
            gate.observe(.1, None)
        self.assertEqual(gate.limit, 100)

    async def test_lower_yield_with_more_attempts_reduces_direct(self):
        metrics = BackendMetrics()
        gate = h.BackendGate(self.gate, 'direct', self.settings, metrics)
        metrics.totals['direct'].update(requests_total=10, requests_successful=10, records=350, latency_seconds=1)
        with patch.object(h.time, 'monotonic', return_value=gate.last_adjustment+2): gate.observe(.1, None)
        metrics.totals['direct'].update(requests_total=20, requests_successful=20, records=100, latency_seconds=2)
        with patch.object(h.time, 'monotonic', return_value=gate.last_adjustment+2): gate.observe(.1, None)
        self.assertEqual(gate.limit, 50)

    async def test_proxy_metrics_rate_activity_and_errors(self):
        now = [0.]
        metrics = BackendMetrics(clock=lambda: now[0])
        metrics.begin('direct', proxy='proxy-001')
        metrics.begin('direct', proxy='proxy-001')
        self.assertEqual(metrics.snapshot()['backend_proxy_metrics']['direct:proxy-001']['active_requests'], 2)
        metrics.end('direct', 'profile', .1, success=True, records=1, proxy='proxy-001')
        metrics.end('direct', 'profile', .3, success=False, kind='proxy_timeout', proxy='proxy-001')
        now[0] = 2
        row = metrics.snapshot()['backend_proxy_metrics']['direct:proxy-001']
        self.assertEqual(row['requests_per_second'], 1)
        self.assertEqual(row['active_requests'], 0)
        self.assertEqual(row['peak_active_requests'], 2)
        self.assertEqual(row['timeout_count'], 1)
        self.assertAlmostEqual(row['average_latency'], .2)
        self.assertEqual(metrics.snapshot()['direct_429_count'], 0)

    async def test_live_console_includes_both_backends_and_total(self):
        stats = ScanStatistics(self.root, 100)
        stats.backends = BackendMetrics()
        lines = []
        stats.display(stats.snapshot({}), lines.append)
        text = '\n'.join(lines)
        for value in ('WORKER', 'DIRECT', 'TOTAL', '% risk'):
            self.assertIn(value, text)

    async def test_worker_continues_during_direct_cooldown(self):
        paused = asyncio.Event()
        async def handle(req):
            if req.url.host == 'www.tiktok.com':
                paused.set()
                return httpx.Response(429)
            return httpx.Response(200, json=fixtures.worker_profile())
        async with self.make(handle) as client:
            direct = asyncio.create_task(client.backends['direct'].lookup_profile('person1'))
            await paused.wait()
            await asyncio.sleep(.01)
            self.assertEqual(client.choose(), 'worker')
            self.assertEqual((await client.lookup_profile('person1')).uid, fixtures.user()['id'])
            self.assertFalse(self.gate.stop_event.is_set())
            direct.cancel()
            await asyncio.gather(direct, return_exceptions=True)

    async def test_http_200_rate_limit_text_does_not_disable_or_stop(self):
        async def handle(req):
            return httpx.Response(200, json={'status':'error','message':'rate limit exceeded'})
        async with self.make(handle) as client:
            with self.assertRaises(s.WorkerApiError) as caught:
                await client.backends['worker'].request_json('profile', {'username':'person1'})
            self.assertNotEqual(caught.exception.kind, 'rate_limited')
            self.assertFalse(client.rate_limits.worker_disabled)
            self.assertFalse(self.gate.stop_event.is_set())

    async def test_cooldown_and_recovery_survive_client_boundary(self):
        stats = ScanStatistics(self.root, 100)
        self.gate.stats = stats
        calls = 0
        async def handle(req):
            nonlocal calls
            calls += 1
            return httpx.Response(429, headers={'Retry-After': '.1'}) if calls == 1 else httpx.Response(200, json=fixtures.direct_profile())
        async with self.make(handle) as client:
            task = asyncio.create_task(client.backends['direct'].lookup_profile('person1'))
            while client.rate_limits.direct_state != 'COOLDOWN': await asyncio.sleep(0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.gate = s.AsyncRequestGate(0, 0, asyncio.Event(), ceiling=1, initial=1, adaptive=False)
        self.gate.stats = stats
        async with self.make(handle) as client:
            self.assertEqual(client.rate_limits.direct_state, 'COOLDOWN')
            self.assertIsNotNone(client.rate_limits.timer)
            await asyncio.wait_for(client.backends['direct'].lookup_profile('person1'), 2)
            self.assertEqual(client.rate_limits.direct_state, 'HEALTHY')
        self.gate = s.AsyncRequestGate(0, 0, asyncio.Event(), ceiling=2500, initial=2500, adaptive=False)
        self.gate.stats = stats
        async with self.make(handle) as client:
            self.assertEqual(client.rate_limits.direct_state, 'RECOVERING')
            self.assertEqual(client.backends['direct'].gate.limit, 1)

    async def test_invalid_members_do_not_inflate_successful_records(self):
        payload = fixtures.direct_page([1, 2])
        payload['userList'].extend([None, {'user': {'id': False, 'uniqueId': '?'}}])
        async def handle(req): return httpx.Response(200, json=payload)
        async with self.make(handle) as client:
            await client.backends['direct'].request_json('followers', {'secUid': 'sec1', 'minCursor': '0'})
            self.assertEqual(client.metrics.snapshot()['direct_followers_received'], 2)

    async def test_committed_direct_export_recovers_before_queue_record(self):
        import test_persistent_scanner as saved
        state, success = saved.setup(self.root, 1)
        try:
            self.assertEqual(state.claim().username, 'user0')
            saved.write_valid(self.root, 'user0')
            path = s.output_file_path(self.root, 'user0', s.SELECTED_LISTS)
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload['source'] = 'tiktok_hybrid_api'
            s.atomic_write_json(path, payload)
            state.recover(success)
            self.assertEqual(state.summary()['completed_overall'], 1)
            self.assertIsNone(state.claim())
        finally:
            state.close()
