"""Deterministic protocol, routing, persistence and transport regressions."""
import asyncio
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import tiktok_worker_scanner as s
import hybrid_backend as h
import direct_protocol as d
from backend_metrics import BackendMetrics


def user(i=1):
    return {'id': str(7100000000000000000+i), 'uniqueId': f'person{i}', 'nickname': f'Person {i}',
            'secUid': f'sec{i}', 'avatarThumb': 'https://p16-sign.tiktokcdn-us.com/image.jpg',
            'signature': 'bio', 'privateAccount': False, 'verified': False}


def direct_profile(i=1):
    return {'statusCode': 0, 'userInfo': {'user': user(i), 'stats': {'followerCount': 5, 'followingCount': 5}}}


def direct_page(indices, cursor='', more=False):
    return {'statusCode': 0, 'userList': [{'user': user(i), 'stats': {}} for i in indices],
            'minCursor': cursor, 'hasMore': more}


def worker_profile(i=1): return d.normalize_profile(direct_profile(i))
def worker_page(indices, cursor='', more=False): return d.normalize_page(direct_page(indices, cursor, more))


class ProtocolTests(unittest.TestCase):
    def test_profile_and_members_keep_private_ids_counts(self):
        raw = direct_profile(); raw['userInfo']['user']['privateAccount'] = True
        payload = d.normalize_profile(raw)
        self.assertEqual(payload['data']['private'], 'Private Account')
        self.assertIsNone(payload['data']['see_following'])
        with self.assertRaises(s.PublicProfileRequired): s.parse_profile_response(payload, 'person1')
        entry = d.normalize_page(direct_page([1]), keep_raw=True)['users'][0]
        self.assertIs(entry['privateAccount'], False)
        self.assertEqual(entry['user_id'], user()['id'])
        self.assertEqual(entry['raw_direct']['user'], user())
        self.assertEqual(s.normalize_member(entry)[0], 'id:'+user()['id'])

    def test_unknown_privacy_never_assumed_public(self):
        raw = direct_profile(); del raw['userInfo']['user']['privateAccount']
        with self.assertRaises(s.InvalidWorkerResponse):
            s.parse_profile_response(d.normalize_profile(raw), 'person1')

    def test_unknown_count_not_invented(self):
        raw = direct_profile(); raw['userInfo']['stats'] = {'followingCount': '2.25K'}
        result = s.parse_profile_response(d.normalize_profile(raw), 'person1')
        self.assertIsNone(result.follower_count)
        self.assertIsNone(result.following_count)
        self.assertEqual(result.advertised_counts['following'], '2.25K')

    def test_partial_missing_and_empty_pages(self):
        for raw in ({'hasMore': False}, {'userList': []}, direct_page([], 'abc', True)):
            with self.subTest(raw=raw), self.assertRaises(d.ProtocolError): d.normalize_page(raw)
        with self.assertRaises(d.ProtocolError): d.normalize_page(direct_page([]), expected_count=5)
        self.assertEqual(d.normalize_page(direct_page([]), expected_count=0)['users'], [])

    def test_risk_is_distinct_from_http_429_and_business_errors(self):
        for raw in ({'captcha': {} ,'verify_event': True}, {'statusCode': '10000'}):
            with self.assertRaises(d.ProtocolError) as caught: d.normalize_profile(raw)
            self.assertEqual(caught.exception.kind, 'risk_control')
        with self.assertRaises(d.ProtocolError) as caught: d.normalize_profile({'statusCode': 10221})
        self.assertEqual(caught.exception.kind, 'api_error')

    def test_signing_binds_wire_query_and_opaque_cursor(self):
        session = d.Session({})
        origin, signed = session.signed_url('followers', {'secUid': 'abc+def==', 'minCursor': 'a/b+c&d=1'}, 35)
        url = httpx.URL(origin).copy_with(query=signed)
        self.assertEqual(url.query, signed)
        self.assertEqual(url.params['minCursor'], 'a/b+c&d=1')
        self.assertEqual(url.params['secUid'], 'abc+def==')
        self.assertEqual(url.params['scene'], '67')
        self.assertEqual(url.params['count'], '35')
        from vendor.tiktok_sign import unseal, unpack_payload, hash_state
        body, _ = unseal(url.params['X-Dynosaur'])
        fields = unpack_payload(body)
        business = signed.decode().split('&X-Dynosaur=')[0]
        # Native oracle-vector coverage is separately retained from upstream.
        self.assertTrue(fields)
        self.assertIn('browser_version=5.0%20(Windows)', business)
        self.assertNotIn('%2520', business)

    def test_config_rejects_guessed_worker_count(self):
        with self.assertRaises(s.ExporterError): h.validate({'worker_page_size': 35})
        for mode in ('worker', 'direct', 'hybrid'): h.validate({'backend_mode': mode})

    def test_backend_score_prefers_successful_records(self):
        metrics = BackendMetrics()
        for _ in range(20):
            for name, count in (('worker', 20), ('direct', 35)):
                metrics.begin(name); metrics.end(name, 'followers', .1, success=True, records=count)
        self.assertGreater(metrics.score('direct'), metrics.score('worker'))
        for _ in range(100):
            metrics.begin('direct'); metrics.end('direct', 'followers', .01, success=False, kind='risk_control')
        self.assertLess(metrics.score('direct'), metrics.score('worker'))


class HybridTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.gate = s.AsyncRequestGate(0, 0, asyncio.Event(), ceiling=100, initial=100, adaptive=False)
        self.settings = {**h.DEFAULTS, 'backend_mode': 'hybrid', 'direct_retry_attempts': 1, 'worker_retry_attempts': 1}
        self.requests, self.clients = [], []

    async def asyncTearDown(self): self.temp.cleanup()

    def make(self, handler, *, settings=None, pool=None):
        async def handle(req):
            self.requests.append(req)
            return await handler(req)
        def factory(route, options):
            client = httpx.AsyncClient(transport=httpx.MockTransport(handle), **{k:v for k,v in options.items() if k != 'proxy'})
            self.clients.append(client)
            return client
        return h.HybridClient(self.gate, pool or s.ProxyPool([]), settings=settings or self.settings, client_factory=factory)

    async def test_profile_coalescing_and_separate_caches(self):
        async def handle(req):
            await asyncio.sleep(.01)
            return httpx.Response(200, json=worker_profile() if req.url.host.endswith('workers.dev') else direct_profile())
        async with self.make(handle) as client:
            with patch.object(client, 'choose', return_value='direct'):
                results = await asyncio.gather(*(client.lookup_profile('person1') for _ in range(100)))
            self.assertEqual(len(self.requests), 1)
            self.assertTrue(all(p.uid == user()['id'] for p in results))
            await client.backends['worker'].lookup_profile('person1')
            self.assertEqual(len(self.requests), 2)
            self.assertEqual(len(self.clients), 2)
        self.assertTrue(all(c.is_closed for c in self.clients))

    async def test_cookie_credentials_never_reach_worker(self):
        session = self.root/'private.json'
        session.write_text(json.dumps({'cookies': {'sessionid': 'fixture-session', 'msToken': 'fixture-token'}}), encoding="utf-8")
        settings = {**self.settings, 'direct_session_file': str(session)}
        async def handle(req):
            if req.url.host.endswith('workers.dev'):
                self.assertNotIn('cookie', req.headers)
                self.assertNotIn('proxy-authorization', req.headers)
                return httpx.Response(200, json=worker_profile())
            self.assertIn('sessionid=fixture-session', req.headers['cookie'])
            self.assertEqual(req.url.params['msToken'], 'fixture-token')
            return httpx.Response(200, json=direct_profile())
        async with self.make(handle, settings=settings) as client:
            await client.backends['direct'].lookup_profile('person1')
            await client.backends['worker'].lookup_profile('person1')
            self.assertNotIn('fixture-session', client.backends['direct'].redactor.text('err fixture-session'))

    async def test_followers_and_following_use_owned_exact_cursors(self):
        for direction in ('followers', 'following'):
            self.requests.clear()
            async def handle(req):
                self.assertEqual(req.url.host, 'www.tiktok.com')
                self.assertEqual(req.url.params['scene'], '67' if direction == 'followers' else '21')
                cursor = req.url.params['minCursor']
                self.assertIn(cursor, ('0', 'opaque+one/=='))
                return httpx.Response(200, json=direct_page([1, 2], 'opaque+one/==', True) if cursor == '0' else direct_page([2, 3]))
            profile = s.parse_profile_response(worker_profile(), 'person1')
            async with self.make(handle) as client:
                with s.UserStore(self.root/(direction+'.sqlite3')) as store, patch.object(client, 'choose', return_value='direct'):
                    result = await s.export_one_list(client, store, profile, direction, target_username='person3', stop_event=self.gate.stop_event)
                    self.assertTrue(result.complete, result)
                    self.assertEqual(result.unique_records_saved, 3)
                    self.assertEqual(result.backend, 'direct')
                    self.assertEqual(len(self.requests), 2)

    async def test_incompatible_failover_restarts_preserves_members_matches(self):
        async def handle(req):
            cursor = req.url.params['minCursor']
            if req.url.host.endswith('workers.dev'):
                if cursor == '0': return httpx.Response(200, json=worker_page([1, 2], 'worker/only', True))
                raise httpx.ConnectError('fixture TLS failure', request=req)
            self.assertEqual(cursor, '0')
            return httpx.Response(200, json=direct_page([2, 3, 4]))
        async with self.make(handle) as client:
            with s.UserStore(self.root/'failover.sqlite3') as store:
                with patch.object(client, 'choose', side_effect=lambda exclude=None: 'direct' if exclude else 'worker'):
                    matches=[]
                    async def matched(name, member): matches.append(member['id'])
                    result = await s.export_one_list(client, store, s.parse_profile_response(worker_profile(), 'person1'),
                        'followers', target_username='PERSON2', on_target_match=matched, stop_event=self.gate.stop_event)
                self.assertTrue(result.complete, result)
                self.assertEqual(result.unique_records_saved, 4)
                self.assertEqual(result.cursor_chain_restarts, 1)
                self.assertEqual(matches, [user(2)['id']])
                self.assertEqual(store.checkpoint('followers')['result']['backend'], 'direct')

    async def test_failed_replacement_keeps_zero_checkpoint(self):
        async def handle(req):
            if req.url.host.endswith('workers.dev') and req.url.params['minCursor'] == '0':
                return httpx.Response(200, json=worker_page([1, 2], 'worker/only', True))
            raise httpx.ConnectError('fixture failed', request=req)
        async with self.make(handle) as client:
            with s.UserStore(self.root/'failed.sqlite3') as store, patch.object(client, 'choose', side_effect=lambda exclude=None: 'direct' if exclude else 'worker'):
                result = await s.export_one_list(client, store, s.parse_profile_response(worker_profile(), 'person1'),
                    'followers', target_username='person2', stop_event=self.gate.stop_event)
                self.assertFalse(result.complete)
                self.assertEqual(store.checkpoint('followers')['next_cursor'], '0')
                self.assertEqual(store.count('followers'), 2)

    async def test_restriction_does_not_switch_backend_or_proxy(self):
        async def handle(req): return httpx.Response(403)
        async with self.make(handle) as client:
            with self.assertRaises(s.WorkerApiError), patch.object(client, 'choose', return_value='direct'):
                await client.lookup_profile('person1')
        self.assertEqual(len(self.requests), 1)

    async def test_direct_risk_does_not_stop_worker(self):
        async def handle(req):
            return httpx.Response(200, json={'captcha': True} if req.url.host == 'www.tiktok.com' else worker_profile())
        async with self.make(handle) as client:
            with self.assertRaises(s.WorkerApiError): await client.backends['direct'].lookup_profile('person1')
            self.assertFalse(self.gate.stop_event.is_set())
            self.assertEqual((await client.backends['worker'].lookup_profile('person1')).uid, user()['id'])


if __name__ == '__main__': unittest.main()
