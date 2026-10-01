"""Backend rate limits: queued/active load, exact cursors and durable restart."""
import asyncio
import json
import time
import unittest
from unittest.mock import patch

import httpx
import tiktok_worker_scanner as s
import hybrid_backend as h
import test_hybrid_backend as fixtures
from test_hybrid_backend import direct_profile, direct_page, worker_profile, worker_page, user


class RateLimitTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.HybridTests.asyncSetUp
    asyncTearDown = fixtures.HybridTests.asyncTearDown
    make = fixtures.HybridTests.make

    def worker_first(self, client):
        return patch.object(client, 'choose', side_effect=lambda exclude=None:
                            'direct' if exclude == 'worker' or client.rate_limits.worker_disabled else 'worker')

    async def test_150_queued_worker_requests_migrate_without_leakage(self):
        worker_calls = 0
        async def handle(req):
            nonlocal worker_calls
            if req.url.host.endswith('workers.dev'):
                worker_calls += 1
                return httpx.Response(429)
            return httpx.Response(200, json=direct_profile(int(req.url.params['uniqueId'][6:])))
        async with self.make(handle, settings={**self.settings, 'worker_backend_max_connections': 1}) as client:
            with self.worker_first(client):
                results = await asyncio.wait_for(asyncio.gather(*(client.lookup_profile(f'person{i}') for i in range(150))), 20)
            self.assertEqual(worker_calls, 1)
            self.assertEqual(len({p.uid for p in results}), 150)
            self.assertTrue(client.rate_limits.worker_disabled)
            self.assertFalse(self.gate.stop_event.is_set())
            self.assertGreater(client.metrics.events['findtik_jobs_moved_to_direct'], 0)
        self.assertTrue(all(c.is_closed for c in self.clients))

    async def test_2500_active_worker_requests_cancel_and_migrate(self):
        self.gate = s.AsyncRequestGate(0, 0, asyncio.Event(), ceiling=2500, initial=2500, adaptive=False)
        started, cancelled = 0, 0
        all_started = asyncio.Event()
        async def handle(req):
            nonlocal started, cancelled
            if req.url.host.endswith('workers.dev'):
                started += 1
                index = started
                if started == 2500: all_started.set()
                try:
                    await all_started.wait()
                    if index == 1: return httpx.Response(429)
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled += 1
                    raise
            return httpx.Response(200, json=direct_profile(int(req.url.params['uniqueId'][6:])))
        async with self.make(handle) as client:
            with self.worker_first(client):
                results = await asyncio.wait_for(asyncio.gather(*(client.lookup_profile(f'person{i}') for i in range(2500))), 60)
            self.assertEqual(started, 2500)
            self.assertEqual(cancelled, 2499)
            self.assertEqual(len({p.uid for p in results}), 2500)
            self.assertEqual(client.metrics.events['findtik_requests_cancelled_after_429'], 2499)
            self.assertFalse(self.gate.stop_event.is_set())
        self.assertEqual(self.gate.active, 0)
        self.assertFalse(self.gate.network_tasks)

    async def test_worker_429_in_both_cursor_directions_preserves_dedup_and_target(self):
        for direction in ('followers', 'following'):
            gate = self.gate = s.AsyncRequestGate(0, 0, asyncio.Event(), ceiling=100, initial=100, adaptive=False)
            async def handle(req):
                cursor = req.url.params['minCursor']
                if req.url.host.endswith('workers.dev'):
                    return httpx.Response(200, json=worker_page([1, 2], 'worker+/=', True)) if cursor == '0' else httpx.Response(429)
                self.assertEqual(cursor, '0')
                return httpx.Response(200, json=direct_page([2, 3]))
            async with self.make(handle) as client:
                with s.UserStore(self.root/(direction+'.sqlite3')) as store, self.worker_first(client):
                    matches = []
                    async def found(_, member): matches.append(member['id'])
                    result = await s.export_one_list(client, store, s.parse_profile_response(worker_profile(), 'person1'),
                        direction, target_username='person2', on_target_match=found, stop_event=gate.stop_event)
                    self.assertTrue(result.complete, result)
                    self.assertEqual(result.unique_records_saved, 3)
                    self.assertEqual(result.cursor_chain_restarts, 1)
                    self.assertEqual(matches, [user(2)['id']])
                    self.assertEqual(store.checkpoint(direction)['result']['backend'], 'direct')

    async def test_direct_cooldown_single_timer_probe_and_gradual_recovery(self):
        calls, recovery_peak, active = 0, 0, 0
        first, probe, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        times = []
        async def handle(req):
            nonlocal calls, recovery_peak, active
            calls += 1; times.append(time.monotonic())
            if calls == 1:
                first.set()
                return httpx.Response(429, headers={'Retry-After': '0.12'})
            active += 1
            recovery_peak = max(recovery_peak, active)
            try:
                if calls == 2:
                    probe.set(); await release.wait()
                return httpx.Response(200, json=direct_profile())
            finally: active -= 1
        async with self.make(handle, settings={**self.settings, 'backend_mode': 'direct', 'direct_429_cooldown': 0}) as client:
            backend = client.backends['direct']
            tasks = [asyncio.create_task(backend.request_json('profile', {'username': 'person1'})) for _ in range(150)]
            await first.wait(); await asyncio.sleep(.02)
            self.assertEqual(calls, 1)
            self.assertEqual(self.gate.active, 0)
            self.assertEqual(len([t for t in asyncio.all_tasks() if t.get_name() == 'direct-rate-limit-timer']), 1)
            await asyncio.wait_for(probe.wait(), 5)
            await asyncio.sleep(.02)
            self.assertEqual(calls, 2)
            self.assertEqual(active, 1)
            self.assertGreaterEqual(times[1]-times[0], .11)
            release.set(); await asyncio.wait_for(asyncio.gather(*tasks), 15)
            self.assertEqual(client.metrics.events['direct_recovery_attempts'], 1)
            self.assertEqual(client.metrics.events['direct_recovery_successes'], 1)
            self.assertFalse(self.gate.stop_event.is_set())
        self.assertEqual(self.gate.active, 0)
        self.assertFalse(any(t.get_name() == 'direct-rate-limit-timer' for t in asyncio.all_tasks()))

    async def test_repeated_direct_429_stops_without_releasing_queue(self):
        direct_calls = 0
        async def handle(req):
            nonlocal direct_calls
            if req.url.host == 'www.tiktok.com': direct_calls += 1
            return httpx.Response(429, headers={'Retry-After': '0.01'})
        async with self.make(handle) as client:
            client.choose = lambda exclude=None: 'direct' if exclude == 'worker' or client.rate_limits.worker_disabled else 'worker'
            results = await asyncio.wait_for(asyncio.gather(*(client.lookup_profile(f'person{i}') for i in range(150)), return_exceptions=True), 20)
            self.assertEqual(direct_calls, 2)
            self.assertTrue(all(isinstance(r, BaseException) for r in results))
            self.assertTrue(self.gate.rate_limit_exhausted)
            self.assertTrue(client.rate_limits.snapshot()['scanner_stopped_due_to_all_backends_rate_limited'])
            self.assertEqual(client.metrics.events['direct_recovery_429s'], 1)
        self.assertTrue(all(c.is_closed for c in self.clients))

    async def test_direct_cooldown_rechecks_early_timer_and_stop(self):
        from rate_limit_control import RateLimitController
        from backend_metrics import BackendMetrics
        for stop_early in (False, True):
            with self.subTest(stop_early=stop_early):
                controller = RateLimitController(BackendMetrics(), {})
                controller.parent = self.gate
                controller.direct_state = 'COOLDOWN'
                controller.cooldown_until = 10.1
                controller.ready.clear()
                self.gate.stop_event.clear()
                now, waits = [10.], []
                async def early_timeout(awaitable, *, timeout):
                    awaitable.close()
                    waits.append(timeout)
                    self.assertEqual(controller.direct_state, 'COOLDOWN')
                    self.assertFalse(controller.ready.is_set())
                    now[0] += min(timeout, .04)
                    if stop_early:
                        self.gate.stop_event.set()
                        return True
                    raise asyncio.TimeoutError
                with patch('rate_limit_control.time.perf_counter', side_effect=lambda: now[0]), \
                     patch('rate_limit_control.asyncio.wait_for', side_effect=early_timeout):
                    await controller.cooldown()
                self.assertTrue(controller.ready.is_set())
                self.assertIsNone(controller.timer)
                self.assertEqual(controller.metrics.events['direct_recovery_attempts'], 0 if stop_early else 1)
                if not stop_early:
                    self.assertGreaterEqual(now[0], controller.cooldown_until)
                    self.assertEqual(len(waits), 3)
                    self.assertEqual(controller.direct_state, 'PROBING')
                else:
                    self.assertEqual(controller.direct_state, 'COOLDOWN')
        self.gate.stop_event.clear()

    async def test_bootstrap_client_boundary_retains_disable_but_new_execution_resets(self):
        from scan_statistics import ScanStatistics
        stats = ScanStatistics(self.root, 100)
        self.gate.stats = stats
        async def handle(req):
            return httpx.Response(429) if req.url.host.endswith('workers.dev') else httpx.Response(200, json=direct_profile(int(req.url.params['uniqueId'][6:])))
        async with self.make(handle) as client:
            client.choose = lambda exclude=None: 'direct' if exclude == 'worker' or client.rate_limits.worker_disabled else 'worker'
            await client.lookup_profile('person1')
            self.assertTrue(client.rate_limits.worker_disabled)
        self.gate = s.AsyncRequestGate(0, 0, asyncio.Event(), ceiling=100, initial=100, adaptive=False)
        self.gate.stats = stats
        async with self.make(handle) as client:
            self.assertTrue(client.rate_limits.worker_disabled)
            self.assertEqual(client.choose(), 'direct')
            await client.lookup_profile('person2')
        self.gate = s.AsyncRequestGate(0, 0, asyncio.Event(), ceiling=100, initial=100, adaptive=False)
        self.gate.stats = ScanStatistics(self.root, 100)
        async with self.make(handle) as client:
            self.assertFalse(client.rate_limits.worker_disabled)

    async def test_cancel_during_cooldown_leaves_no_timers_or_permits(self):
        entered = asyncio.Event()
        async def handle(req):
            entered.set(); return httpx.Response(429)
        async with self.make(handle, settings={**self.settings, 'backend_mode': 'direct'}) as client:
            task = asyncio.create_task(client.lookup_profile('person1'))
            await entered.wait(); await asyncio.sleep(.01)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(self.gate.active, 0)
        self.assertFalse(self.gate.network_tasks)
        self.assertFalse(any(t.get_name() == 'direct-rate-limit-timer' for t in asyncio.all_tasks()))

    async def test_full_scan_stop_and_resume_keeps_pages_jobs_and_matches(self):
        cfg = {**s.default_scan_config(), **self.settings, 'workers': 8,
               'target_username': 'person2', 'direct_429_cooldown': .01}
        state = s.DurableScanState(self.root, {'input_imported': True}, self.root, cfg)
        state.add_jobs([s.ProfileJob(f'person{i}', f'https://www.tiktok.com/@person{i}', ('profiles',))
                        for i in range(10, 14)])
        success = s.SuccessRecorder(self.root/s.SUCCESS_FILE_NAME, target_user='person2',
            input_path=self.root/'input.json', started_at_utc=s.utc_iso())
        resumed, controllers = False, []
        async def handle(req):
            if req.url.host.endswith('workers.dev'):
                return httpx.Response(429)
            if req.url.path == '/api/user/detail/':
                payload = direct_profile(int(req.url.params['uniqueId'][6:]))
                payload['userInfo']['user']['avatarThumb'] = ''
                return httpx.Response(200, json=payload)
            if req.url.params['minCursor'] == '0':
                return httpx.Response(200, json=direct_page([1, 2], 'direct+/one=', True))
            self.assertEqual(req.url.params['minCursor'], 'direct+/one=')
            return httpx.Response(200, json=direct_page([2, 3])) if resumed else httpx.Response(429)
        def factory(gate, pool, **kwargs):
            client = h.HybridClient(gate, pool, **kwargs, client_factory=lambda route, options:
                httpx.AsyncClient(transport=httpx.MockTransport(handle), **options))
            client.choose = lambda exclude=None: 'direct' if exclude == 'worker' or client.rate_limits.worker_disabled else 'worker'
            controllers.append(client.rate_limits)
            return client
        args = dict(output_directory=self.root, worker_count=8, pacing=(0, 0),
                    success_recorder=success, state_recorder=state, use_resume=True)
        try:
            with patch.object(h, 'create_client', factory), patch.object(s, 'console'):
                _, _, fatal = await asyncio.wait_for(s.run_scan([], **args), 20)
                self.assertIsNotNone(fatal)
                summary = state.summary()
                self.assertEqual(summary['remaining_profiles'], 4)
                self.assertEqual(summary['failed_profiles'], 0)
                saved = []
                for path in (self.root/'pages').glob('*.sqlite3'):
                    with s.UserStore(path) as store:
                        checkpoint = store.checkpoint('followers')
                        if checkpoint and store.count('followers'):
                            self.assertEqual(store.count('followers'), 2)
                            self.assertEqual(checkpoint['next_cursor'], 'direct+/one=')
                            saved.append(path)
                self.assertTrue(saved)
                stats = json.loads((self.root/'scan_stats.json').read_text(encoding="utf-8"))
                self.assertEqual(stats['scan_status'], 'stopped_rate_limited')
                self.assertTrue(stats['findtik_disabled_after_429'])
                resumed = True
                state.recover(success)
                _, _, fatal = await asyncio.wait_for(s.run_scan([], **args), 20)
                self.assertIsNone(fatal)
                self.assertEqual(state.summary()['completed_overall'], 4)
                self.assertEqual(state.summary()['remaining_profiles'], 0)
                for i in range(10, 14):
                    with s.UserStore(self.root/'pages'/f'person{i}.sqlite3') as store:
                        for direction in ('followers', 'following'):
                            self.assertEqual(store.count(direction), 3)
                            self.assertTrue(store.checkpoint(direction)['result']['complete'])
                    self.assertEqual(set(success.found_lists_for(f'person{i}')), {'followers', 'following'})
                self.assertIsNot(controllers[0], controllers[1])
        finally:
            state.close()
