import asyncio, contextlib, json
from unittest.mock import patch, AsyncMock
import httpx
import tiktok_worker_scanner as s
import hybrid_backend as h
from test_hybrid_backend import HybridTests, worker_profile, direct_profile, direct_page, user
from test_proxy_wire import ProxyWireTests

class ReviewTests(HybridTests):
    async def test_switch_to_worker_mode_restarts_direct_cursor(self):
        from dataclasses import asdict
        from test_hybrid_backend import worker_page
        async def handler(req):
            self.assertEqual(req.url.params['minCursor'], '0')
            return httpx.Response(200, json=worker_page([1,2]))
        def factory(route, options):
            return httpx.AsyncClient(transport=httpx.MockTransport(handler), **options)
        async with h.create_client(self.gate, s.ProxyPool([]), settings={**self.settings, 'backend_mode':'worker'}, client_factory=factory) as client:
            with s.UserStore(self.root/'switch.sqlite3') as store:
                result=s.ListExportResult('followers',2);result.backend='direct';result.batches=1;result.unique_records_saved=1
                store.save_batch('followers',worker_page([1])['users'])
                store.save_checkpoint('followers',{'result':asdict(result),'next_cursor':'direct-only-cursor'})
                result=await s.export_one_list(client,store,s.parse_profile_response(worker_profile(),'person1'),'followers',target_username='person2',stop_event=self.gate.stop_event)
                self.assertTrue(result.complete,result);self.assertEqual(result.unique_records_saved,2)
                self.assertEqual(result.backend,'worker');self.assertEqual(result.cursor_chain_restarts,1)

    async def test_coalesced_cancel_does_not_cancel_another_waiter(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def handler(req):
            started.set(); await release.wait(); return httpx.Response(200, json=worker_profile())
        async with self.make(handler) as client:
            with patch.object(client, 'choose', return_value='worker'):
                first=asyncio.create_task(client.lookup_profile('person1'));await started.wait()
                second=asyncio.create_task(client.lookup_profile('person1'));await asyncio.sleep(0)
                first.cancel();await asyncio.gather(first,return_exceptions=True);release.set()
                self.assertEqual((await second).uid,user()['id']);self.assertEqual(len(self.requests),1)
                self.assertFalse(client.inflight_profiles)

    async def test_partial_page_saves_valid_matches(self):
        async def handler(req):
            payload=direct_page([1,2]);payload['userList'].append(None)
            return httpx.Response(200,json=payload)
        async with self.make(handler) as client:
            with s.UserStore(self.root/'mixed.sqlite3') as store,patch.object(client,'choose',return_value='direct'):
                matches=[]
                async def matched(name,member):matches.append(member['id'])
                result=await s.export_one_list(client,store,s.parse_profile_response(worker_profile(),'person1'),'followers',target_username='person2',on_target_match=matched,stop_event=self.gate.stop_event)
                self.assertEqual(store.count('followers'),2);self.assertFalse(result.complete)
                self.assertEqual(result.invalid_records_ignored,1);self.assertEqual(matches,[user(2)['id']])

    async def test_uid_match_survives_renamed_username_on_resume(self):
        import direct_protocol as d
        with s.UserStore(self.root/'uid.sqlite3') as store:
            member=d.normalize_page(direct_page([1]))['users'][0];member['uniqueId']='oldname';store.save_batch('followers',[member])
            matches=[]
            async def matched(name,value):matches.append(value['id'])
            result=await s.replay_committed_list(store,'followers',target_username='newname',target_uid=user()['id'],on_target_match=matched)
            self.assertTrue(result);self.assertEqual(matches,[user()['id']])

    async def test_private_profile_avatar_is_cached(self):
        async def handler(req):
            data=worker_profile();data['data']['private']='Private Account';return httpx.Response(200,json=data)
        async with self.make(handler) as client:
            client.avatars=type('Cache',(),{'ensure':AsyncMock(return_value='local.jpg')})()
            with patch.object(client,'choose',return_value='worker'),self.assertRaises(s.PublicProfileRequired):await client.lookup_profile('person1')
            client.avatars.ensure.assert_awaited_once()

    async def test_proxy_capacity_cancel_does_not_release_unowned_permit(self):
        active=peak=0;entered,release=asyncio.Event(),asyncio.Event()
        async def handler(req):
            nonlocal active,peak
            active+=1;peak=max(peak,active);entered.set()
            try:
                await release.wait();return httpx.Response(200,json=direct_profile() if req.url.host=='www.tiktok.com' else worker_profile())
            finally:active-=1
        pool=s.ProxyPool([s.ProxyConfig('127.0.0.1',9999,'fixture','fixture-password')])
        async with self.make(handler,pool=pool,settings={**self.settings,'connections_per_proxy':1}) as client:
            first=asyncio.create_task(client.backends['worker'].request_json('profile',{'username':'person1'}));await entered.wait()
            second=asyncio.create_task(client.backends['direct'].request_json('profile',{'username':'person1'}));await asyncio.sleep(.02)
            second.cancel();await asyncio.gather(second,return_exceptions=True);release.set();await first
            await client.backends['direct'].request_json('profile',{'username':'person1'})
            self.assertEqual(peak,1);self.assertEqual(self.gate.active,0);self.assertEqual(pool.states[0].in_use,0)

class WireReuseTests(ProxyWireTests):
    async def origin_handler(self,reader,writer):
        task=asyncio.current_task();self.handler_tasks.add(task)
        try:
            while True:
                data=await reader.readuntil(b'\r\n\r\n');self.headers.append(data.decode());await asyncio.sleep(.01)
                body=json.dumps(worker_profile()).encode();writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '+str(len(body)).encode()+b'\r\n\r\n'+body);await writer.drain()
        except (OSError,asyncio.IncompleteReadError):pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):await writer.wait_closed()
            self.handler_tasks.discard(task)
    async def test_real_tls_connect_is_reused(self):
        gate=s.AsyncRequestGate(0,0,asyncio.Event(),ceiling=100,initial=100,adaptive=False)
        pool=s.ProxyPool([await self.proxy()])
        def factory(route,options):return httpx.AsyncClient(**{**options,'verify':self.verify})
        async with h.HybridClient(gate,pool,settings={**h.DEFAULTS,'connections_per_proxy':1},client_factory=factory) as client:
            for _ in range(5):await client.backends['worker'].request_json('profile',{'username':'person1'})
            values=client.metrics.snapshot()
            self.assertEqual(values['worker_connect_tcp_successful'],1);self.assertEqual(values['worker_tls_handshake_successful'],1)
            self.assertGreaterEqual(values['worker_reused_connection_requests'],4);self.assertEqual(len(self.connect_headers),1)
            self.assertEqual(values['backend_connection_pools']['worker']['idle_keepalive'],1)
            self.assertTrue(all('proxy-authorization' not in x.lower() for x in self.headers))
