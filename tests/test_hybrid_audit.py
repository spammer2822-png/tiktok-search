"""Requirements discovered in the final review: healthy-load adaptation and telemetry."""
import asyncio
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
