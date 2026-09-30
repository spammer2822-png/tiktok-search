"""Regression cases from the avatar/reliability review; entirely synthetic."""
import asyncio
import base64
import copy
import io
import json
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from PIL import Image

import avatar_cache as avatars
import report_generator as report
import tiktok_worker_scanner as s
from test_async_scanner import PROFILE, AsyncFakeClient, member, page, profile
from test_configured_scanner import ConfigurationTests, job, response_profile

URL = 'https://p16-sign.tiktokcdn.com/avatar/report-test.jpeg?expires=synthetic'
OTHER = 'https://p16-sign.tiktokcdn.com/avatar/second-test.jpeg'


def picture(color='purple', size=(160, 120), format='PNG'):
    data = io.BytesIO()
    Image.new('RGB', size, color).save(data, format=format)
    return data.getvalue()


class AvatarTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.caches = []

    async def asyncTearDown(self):
        for cache in self.caches:
            cache.close()
        self.tmp.cleanup()

    def cache(self, download=None):
        value = avatars.AvatarCache(self.root, download or AsyncMock(return_value=picture()))
        self.caches.append(value)
        return value

    def client(self, handler, configs=(), spacing=0):
        pool = s.ProxyPool(list(configs))
        gate = s.AsyncRequestGate(spacing, spacing, asyncio.Event(), ceiling=4, initial=4, adaptive=False)
        def factory(route, options):
            async def dispatch(request):
                self.assertNotIn('proxy-authorization', request.headers)
                return await handler(route, request)
            return httpx.AsyncClient(transport=httpx.MockTransport(dispatch), headers=options['headers'])
        return s.WorkerApiClient(gate, pool, client_factory=factory, avatar_directory=self.root)


class AvatarTests(AvatarTestCase):
    async def test_valid_avatar_is_small_valid_relative_jpeg(self):
        cache = self.cache()
        ref = await cache.ensure('1234567890123456789', 'annabelle', URL)
        self.assertRegex(ref, r'^report_assets/avatars/[a-f0-9]{64}\.jpg$')
        self.assertTrue(avatars.valid_cached_image(self.root/ref))
        self.assertLessEqual((self.root/ref).stat().st_size, avatars.MAX_CACHE_BYTES)
        with Image.open(self.root/ref) as img:
            self.assertEqual(img.size, (96, 72))
            self.assertFalse(img.getexif())

    async def test_missing_empty_and_forbidden_urls_do_not_request(self):
        cache = self.cache()
        for url in (None, '', 'https://www.tiktok.com/@abc', 'http://p16.tiktokcdn.com/x',
                    'https://p16.tiktokcdn.com.evil.example/a', 'file:///private',
                    'https://secret:password@p16.tiktokcdn.com/a', 'https://127.0.0.1/a',
                    'https://p16.tiktokcdn.com:80/a', 'javascript:alert(1)'):
            self.assertEqual(await cache.ensure('1', 'abc', url), '')
        cache.download.assert_not_awaited()

    async def test_cached_reuse_and_new_instance_resume(self):
        cache = self.cache()
        ref = await cache.ensure('1', 'a', URL)
        self.assertEqual(await cache.ensure('1', 'a', URL), ref)
        cache.download.assert_awaited_once()
        cache.close()
        resumed = self.cache(AsyncMock(side_effect=AssertionError('unexpected download')))
        self.assertEqual(await resumed.ensure('1', 'renamed', OTHER), ref)
        self.assertEqual(await resumed.ensure('1', 'renamed', None), ref)
        resumed.download.assert_not_awaited()

    async def test_concurrent_duplicates_deduplicate_url_and_uid(self):
        calls = []
        async def download(url):
            calls.append(url)
            await asyncio.sleep(.01)
            return picture()
        cache = self.cache(download)
        refs = await asyncio.gather(*(cache.ensure(str(i), 'a'+str(i), URL) for i in range(12)))
        self.assertEqual(len(set(refs)), 1)
        self.assertEqual(calls, [URL])
        self.assertEqual(await cache.ensure('3', 'alias', OTHER), refs[0])
        self.assertEqual(calls, [URL])
        self.assertEqual(len(list((self.root/avatars.ASSET_DIRECTORY).glob('*.jpg'))), 1)

    async def test_different_users_images_and_unusual_names(self):
        cache = self.cache(AsyncMock(side_effect=[picture('red'), picture('blue')]))
        first = await cache.ensure('1', '../../CON:<é>"', URL)
        second = await cache.ensure('2', 'LPT1.🐢', OTHER)
        self.assertNotEqual(first, second)
        self.assertNotEqual((self.root/first).read_bytes(), (self.root/second).read_bytes())
        self.assertFalse(any('CON' in p.name or 'é' in p.name for p in self.root.rglob('*')))

    async def test_http_failure_timeout_malformed_and_unavailable_network(self):
        cases = [httpx.Response(404), httpx.Response(403), httpx.Response(500),
                 httpx.Response(200, headers={'Content-Type':'image/png'}, content=b'not an image'),
                 httpx.Response(200, headers={'Content-Type':'text/html'}, content=b'<html>fail</html>'),
                 httpx.ReadTimeout('private credential text'), httpx.ConnectError('network unavailable')]
        for index, response in enumerate(cases):
            with self.subTest(index=index):
                async def handler(route, request):
                    if isinstance(response, Exception):
                        raise response
                    return response
                async with self.client(handler) as client:
                    result = await client.avatars.ensure(str(index), 'u'+str(index), URL+'&case='+str(index))
                    self.assertEqual(result, '')
                    self.assertIsNone(client.gate.blocked_reason)
                    self.assertFalse(client.gate.rate_limit_exhausted)
        self.assertFalse(list(self.root.rglob('*.jpg')))

    async def test_download_limits_and_bad_encoding(self):
        class Oversize(httpx.AsyncByteStream):
            async def __aiter__(self):
                for _ in range(140):
                    yield b'x'*16384
        responses = [httpx.Response(200, headers={'Content-Type':'image/png','Content-Length':'99999999'}),
                     httpx.Response(200, headers={'Content-Type':'image/png'}, stream=Oversize()),
                     httpx.Response(200, headers={'Content-Type':'image/png','Content-Encoding':'gzip'}, content=b'')]
        for response in responses:
            async def handler(route, request):
                return response
            async with self.client(handler) as client:
                self.assertEqual(await client.download_avatar(URL), b'')
        with self.assertRaises(ValueError):
            avatars.thumbnail(picture(size=(2100, 2100)))

    async def test_interrupted_download_no_final_file_then_resume(self):
        reached = asyncio.Event()
        closed = asyncio.Event()
        class InterruptedBody(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield picture()[:30]
                reached.set()
                await asyncio.Event().wait()
            async def aclose(self):closed.set()
        async def handler(route,request):
            return httpx.Response(200,headers={'Content-Type':'image/png'},stream=InterruptedBody())
        async with self.client(handler) as client:
            task = asyncio.create_task(client.avatars.ensure('1', 'a', URL))
            await reached.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(closed.is_set())
        self.assertFalse(list(self.root.rglob('*.jpg')))
        self.assertFalse(list(self.root.rglob('*.tmp')))
        resumed = self.cache()
        self.assertTrue(await resumed.ensure('1', 'a', URL))

    async def test_partial_temp_and_corrupt_file_are_never_used(self):
        directory = self.root/avatars.ASSET_DIRECTORY
        directory.mkdir(parents=True)
        (directory/(avatars.digest(URL)+'.jpg.tmp')).write_bytes(b'partial')
        (directory/(avatars.digest(URL)+'.jpg')).write_bytes(b'corrupt')
        cache = self.cache()
        ref = await cache.ensure('1', 'a', URL)
        self.assertTrue(avatars.valid_cached_image(self.root/ref))
        self.assertFalse(list(directory.glob('*.tmp')))
        cache.download.assert_awaited_once()

    async def test_image_write_failure_is_nonfatal_and_atomic(self):
        cache = self.cache()
        with patch.object(avatars.os, 'replace', side_effect=OSError('private URL')):
            self.assertEqual(await cache.ensure('1', 'a', URL), '')
        self.assertFalse(list(self.root.rglob('*.jpg')))
        self.assertFalse(list(self.root.rglob('*.tmp')))

    async def test_failed_source_not_redownloaded_until_later_run(self):
        cache = self.cache(AsyncMock(return_value=b'invalid'))
        self.assertEqual(await cache.ensure('1','a',URL), '')
        self.assertEqual(await cache.ensure('2','b',URL), '')
        cache.download.assert_awaited_once()
        cache.close()
        resumed = self.cache()
        self.assertEqual(await resumed.ensure('1','a',URL), '')
        resumed.download.assert_not_awaited()
        with resumed.database() as db:
            db.execute('UPDATE failures SET retry_after=0')
        self.assertTrue(await resumed.ensure('1','a',URL))

    async def test_report_valid_missing_old_and_escaped_avatar_data(self):
        cache = self.cache()
        ref = await cache.ensure('1','a',URL)
        reader = avatars.AvatarReader(self.root)
        try:
            row = report.make_row('a','1',1,'completed',{}, {'avatar_url':URL,'display_name':'</script><script>window.BAD=1</script>'}, {},[],1,'',avatars=reader)
            self.assertEqual(row['avatar'],ref)
            self.assertEqual(report.make_row('old','2',1,'pending',{}, {},{},[],0,'',avatars=reader)['avatar'],'')
            bad = report.make_row('evil','3',1,'pending',{}, {'avatar_url':'data:image/svg+xml;base64,PHN2Zz4='},{},[],0,'',avatars=reader)
            self.assertEqual(bad['avatar'],'')
        finally:
            reader.close()
        s.atomic_write_json(self.root/'scan_config.json',s.default_scan_config())
        s.atomic_write_json(self.root/'scan_state.json',{'summary':{}})
        original = report.build_data(self.root)
        original['rows'] = [row, bad]
        with patch.object(report,'build_data',return_value=original):
            path = report.generate_report(self.root,emit=lambda _:None)
        text = path.read_text()
        self.assertIn(ref,text)
        self.assertNotIn('</script><script>window.BAD',text)
        self.assertIn("connect-src 'none'",text)
        self.assertIn('object-fit:cover',text)
        self.assertIn('@media',text)
        self.assertNotIn(URL,text)
        cache.close()
        moved = self.root/'moved folder'
        shutil.copytree(self.root/avatars.ASSET_DIRECTORY,moved/avatars.ASSET_DIRECTORY)
        shutil.copy2(path,moved/path.name)
        reader = avatars.AvatarReader(moved)
        self.assertEqual(reader.reference('1','a',URL),ref)
        reader.close()
        (moved/ref).unlink()
        reader = avatars.AvatarReader(moved)
        self.assertEqual(reader.reference('1','a',URL),'')
        reader.close()

    async def test_embedded_image_validation_and_legacy_record(self):
        embedded = 'data:image/png;base64,'+base64.b64encode(picture()).decode()
        self.assertTrue(report.local_avatar(embedded).startswith('data:image/jpeg;base64,'))
        self.assertEqual(report.local_avatar('data:image/png;base64,YmFk'),'')
        cache = self.cache()
        self.assertTrue(await cache.ensure('1','a',embedded))
        cache.download.assert_not_awaited()

    async def test_cdn_http_errors_never_rotate_or_become_worker_restrictions(self):
        calls=[]
        async def handler(route, request):
            calls.append(route.label)
            return httpx.Response(403)
        configs=[s.ProxyConfig('p1.example',80,'name-403','pass-429'),s.ProxyConfig('p2.example',80,'u','p')]
        async with self.client(handler,configs) as client:
            self.assertEqual(await client.download_avatar(URL),b'')
            self.assertEqual(await client.download_avatar(OTHER),b'')
            self.assertEqual(calls,['proxy-001'])
            self.assertTrue(all(p.failure_count==0 for p in client.pool.states))
            self.assertIsNone(client.gate.blocked_reason)

    async def test_avatar_requests_share_global_spacing_and_healthy_proxy_pool(self):
        calls=[]
        async def handler(route, request):
            calls.append((time.monotonic(),route.label))
            await asyncio.sleep(.025)
            return httpx.Response(200,headers={'Content-Type':'image/png'},content=picture())
        configs=[s.ProxyConfig('p1.example',80,'name','pass'),s.ProxyConfig('p2.example',80,'name','pass')]
        async with self.client(handler,configs,spacing=.015) as client:
            await asyncio.gather(*(client.avatars.ensure(str(i),'a'+str(i),URL+'&n='+str(i)) for i in range(4)))
            self.assertGreater(client.gate.peak_active,1)
            self.assertEqual({route for _,route in calls},{'proxy-001','proxy-002'})
            self.assertTrue(all(b[0]-a[0]>=.014 for a,b in zip(calls,calls[1:])))

    async def test_asset_metadata_and_paths_contain_no_proxy_credentials(self):
        original=Image.new('RGB',(128,128),'green')
        exif=Image.Exif();exif[270]='fixture-proxy-user fixture-proxy-password'
        raw=io.BytesIO();original.save(raw,format='JPEG',exif=exif)
        cache=self.cache(AsyncMock(return_value=raw.getvalue()))
        ref=await cache.ensure('1','a',URL)
        cache.close()
        for path in (self.root/avatars.ASSET_DIRECTORY).iterdir():
            self.assertNotIn(b'fixture-proxy-user',path.read_bytes())
            self.assertNotIn(b'fixture-proxy-password',path.read_bytes())
            self.assertNotIn('fixture-proxy',path.name)
        self.assertTrue(avatars.valid_cached_image(self.root/ref))

    async def test_missing_assets_and_corrupt_index_do_not_break_report(self):
        root=self.root;cfg=s.default_scan_config()
        state=s.DurableScanState(root,{},root,cfg);state.add_jobs([job('a','1')]);state.snapshot();state.close()
        s.atomic_write_json(root/'scan_config.json',cfg)
        directory=root/'pages';directory.mkdir()
        p=profile('a');p.avatar_url=URL
        with s.UserStore(directory/'a.sqlite3') as store:store.save_checkpoint('profile',s.asdict(p))
        assets=root/avatars.ASSET_DIRECTORY;assets.mkdir(parents=True)
        (assets/'cache.sqlite3').write_bytes(b'not a database')
        self.assertEqual(report.build_data(root)['rows'][0]['avatar'],'')
        self.assertIsNotNone(report.generate_report(root,emit=lambda _:None))

    async def test_member_avatar_fallback_preserves_report_for_skipped_profile(self):
        root=self.root;cfg=s.default_scan_config()
        state=s.DurableScanState(root,{},root,cfg);state.add_jobs([job('a','1')])
        raw=member('a','1');raw['avatarThumb']=URL
        state.discover('seed','followers',[s.normalize_member(raw)[1]],phase=0)
        claimed=state.claim();state.record(s.error_profile_result(claimed,status='not_found',started_at_utc=s.utc_iso(),error='not found'))
        state.close();s.atomic_write_json(root/'scan_config.json',cfg)
        self.assertEqual(list(report.iter_avatar_profiles(root)),[('1','a',URL)])
        cache=self.cache();ref=await cache.ensure('1','a',URL)
        self.assertEqual(report.build_data(root)['rows'][0]['avatar'],ref)

    async def test_repeated_network_timeouts_stop_optional_cdn_traffic(self):
        calls=[]
        async def handler(route,request):
            calls.append(str(request.url));raise httpx.ReadTimeout('offline CDN')
        async with self.client(handler) as client:
            for i in range(10):self.assertEqual(await client.download_avatar(URL+'&n='+str(i)),b'')
            self.assertEqual(len(calls),3)
            self.assertIsNone(client.gate.blocked_reason)

    async def test_avatar_proxy_auth_failure_can_use_healthy_route_or_direct(self):
        for direct in (True,False):
            calls=[]
            async def handler(route,request):
                calls.append(route.label if route else 'direct')
                if len(calls)==1:raise httpx.ProxyError('407 Proxy Authentication Required')
                return httpx.Response(200,headers={'Content-Type':'image/png'},content=picture())
            configs=[s.ProxyConfig('p1.example',80,'private-user','bad-password')]
            if not direct:configs.append(s.ProxyConfig('p2.example',80,'valid-user','valid-password'))
            async with self.client(handler,configs) as client:
                self.assertTrue(await client.download_avatar(URL))
                self.assertEqual(calls,['proxy-001','direct' if direct else 'proxy-002'])
                self.assertTrue(client.pool.states[0].disabled)


class ReviewFixTests(AvatarTestCase):
    async def test_404_does_not_disable_transient_retries(self):
        for middle in (500,httpx.ReadTimeout('slow')):
            with self.subTest(middle=middle):
                events=iter([404,middle,200]);calls=[]
                async def handler(route,request):
                    calls.append((route.label,str(request.url)))
                    item=next(events)
                    if isinstance(item,Exception):raise item
                    return httpx.Response(item,json=PROFILE)
                async with self.client(handler,[s.ProxyConfig('p.example',80)]) as client:
                    with patch.object(client.gate,'wait',new=AsyncMock()),patch.object(client.gate,'cooldown'):
                        self.assertEqual((await client.request_json('profile',{'username':'roblox'}))['status'],'ok')
                    self.assertEqual(len(calls),3)
                    self.assertEqual(len(set(calls)),1)
                    self.assertEqual(client.request_retries.get(),2)
                    self.assertFalse(client.gate.rate_limit_exhausted)
                    self.assertEqual(client.pool.states[0].failure_count,0)

    async def test_normal_retry_budget_still_bounded_after_404(self):
        calls=[]
        async def handler(route,request):
            calls.append(1)
            return httpx.Response(404 if len(calls)==1 else 500)
        async with self.client(handler) as client:
            client.settings['retry_attempts']=2
            with patch.object(client.gate,'wait',new=AsyncMock()),patch.object(client.gate,'cooldown'):
                with self.assertRaises(s.WorkerApiError):
                    await client.request_json('profile',{'username':'a'})
            self.assertEqual(len(calls),3)

    async def test_credentials_cannot_spoof_proxy_status_or_error_tokens(self):
        configs=[s.ProxyConfig('p.example',80,'ann-403','abc-429-x'),
                 s.ProxyConfig('q.example',80,'client_connect_forbidden_host','authentication failed')]
        async def unused(*_):raise AssertionError('network')
        async with self.client(unused,configs) as client:
            for config in configs:
                url='http://'+config.username+':'+config.password+'@'+config.host+':80'
                result=client.proxy_exception(httpx.ProxyError('Tunnel disconnected at '+url))
                self.assertEqual(result.kind,'proxy_connection_failure')
            for status,kind in [(403,'access_denied'),(407,'proxy_authentication_failure'),(429,'proxy_concurrency_limited'),(503,'proxy_temporarily_unavailable')]:
                self.assertEqual(client.proxy_exception(httpx.ProxyError(str(status)+' test response')).kind,kind)
                exc=httpx.ProxyError('tunnel refused');exc.response=SimpleNamespace(status_code=status)
                self.assertEqual(client.proxy_exception(exc).kind,kind)
            self.assertEqual(client.proxy_exception(httpx.ProxyError('host proxy-403.example closed connection')).kind,'proxy_connection_failure')

    async def test_response_and_persistence_keep_credential_substrings(self):
        config=s.ProxyConfig('p.example',80,'ann','123')
        payload=response_profile('annabelle','123456','123','3')
        payload['data'].update(fullname='Joann',region='banner',secUid='secret123marker')
        async def handler(route,request):return httpx.Response(200,json=copy.deepcopy(payload))
        async with self.client(handler,[config]) as client:
            original=await client.request_json('profile',{'username':'annabelle'})
            self.assertEqual(original,payload)
            parsed=s.parse_profile_response(original,'annabelle')
            self.assertEqual(parsed.uid,'123456')
            self.assertEqual(parsed.display_name,'Joann')
            with patch.object(s,'REDACTOR',client.redactor):
                s.atomic_write_json(self.root/'profile.json',original)
                with s.UserStore(self.root/'profile.sqlite3') as store:
                    store.save_checkpoint('profile',s.asdict(parsed))
                    self.assertEqual(store.checkpoint('profile')['username'],'annabelle')
            self.assertEqual(json.loads((self.root/'profile.json').read_text()),payload)
            cleaned=client.redactor.clean({'bio':'banner','debug':'ann 123 http://ann:123@p.example:80','proxy_password':'123'})
            self.assertEqual(cleaned['bio'],'banner')
            self.assertNotIn('123',cleaned['debug']);self.assertNotIn('ann',cleaned['debug'])
            self.assertEqual(cleaned['proxy_password'],'[REDACTED]')
            raw_member=member('annabelle','123456');raw_member.update(signature='banner',nickname='Joann')
            with patch.object(s,'REDACTOR',client.redactor):
                with s.UserStore(self.root/'members.sqlite3') as store:
                    store.save_batch('following',[raw_member])
                    saved=list(store.iter_members('following'))[0]
                    self.assertEqual((saved['username'],saved['id'],saved['bio'],saved['display_name']),('annabelle','123456','banner','Joann'))
            self.assertEqual(s.parse_list_page(page([raw_member],True,'123-opaque-ann'),'following').min_cursor,'123-opaque-ann')

    async def test_duplicate_early_return_recovers_committed_target_idempotently(self):
        pages=self.root/'pages';pages.mkdir()
        p=profile('alias')
        with s.UserStore(pages/'alias.sqlite3') as store:
            store.save_checkpoint('profile',s.asdict(p))
            store.save_batch('followers',[member('wanted')],target_username='wanted')
            result=s.ListExportResult('followers',1,unique_records_saved=1,stop_reason='resuming')
            store.save_checkpoint('followers',{'result':s.asdict(result),'next_cursor':'opaque-committed'})
        recorder=s.SuccessRecorder(self.root/s.SUCCESS_FILE_NAME,target_user='wanted',input_path=self.root/'input.json',started_at_utc=s.utc_iso())
        client=AsyncFakeClient(p,{})
        discovered=AsyncMock()
        first_time=None
        for _ in range(2):
            out=await s.process_profile(client,job('alias'),output_directory=self.root,success_recorder=recorder,
                stop_event=asyncio.Event(),use_resume=True,store_directory=pages,
                on_profile=AsyncMock(return_value=False),on_discovery=discovered)
            self.assertEqual(out.status,'skipped_duplicate')
            data=json.loads(recorder.path.read_text())
            self.assertEqual(len(data['profiles']),1)
            self.assertEqual(data['profiles'][0]['found_in'],['followers'])
            stamp=data['profiles'][0]['match_observed_at_utc']['followers']
            if first_time:self.assertEqual(stamp,first_time)
            first_time=stamp
        self.assertEqual(client.calls,[])
        self.assertEqual(discovered.await_count,2)

    async def test_old_terminal_duplicate_repaired_at_startup(self):
        cfg=s.default_scan_config();cfg['target_username']='wanted'
        state=s.DurableScanState(self.root,{},self.root,cfg)
        state.add_jobs([job('alias')]);claimed=state.claim()
        state.record(s.error_profile_result(claimed,status='skipped_duplicate',started_at_utc=s.utc_iso(),error='fixture'))
        pages=self.root/'pages';pages.mkdir()
        with s.UserStore(pages/'alias.sqlite3') as store:
            store.save_batch('followers',[member('wanted')],target_username='wanted')
        recorder=s.SuccessRecorder(self.root/s.SUCCESS_FILE_NAME,target_user='wanted',input_path=self.root/'input.json',started_at_utc=s.utc_iso())
        state.recover(recorder);state.recover(recorder)
        self.assertEqual(recorder.found_lists_for('alias'),['followers'])
        self.assertEqual(state.summary()['processed_profiles'],1)
        self.assertEqual(state.connection.execute('SELECT COUNT(*) FROM discoveries').fetchone()[0],1)
        state.close()

    async def test_phase_two_discovery_count_not_copied(self):
        cfg=s.default_scan_config();cfg['scan_mode']='double_phase'
        state=s.DurableScanState(self.root,{},self.root,cfg)
        state.discover('a','followers',[{'username':'b','id':'2','private_account':False}],phase=1)
        state.snapshot();state.close()
        data=report.build_data(self.root)
        self.assertEqual(data['phases']['1']['discovered'],1)
        self.assertEqual(data['phases']['2']['discovered'],0)

    async def test_configurable_worker_and_legacy_config(self):
        with patch.dict(os.environ,{'TIKTOK_WORKER_ORIGIN':'https://other-worker.example.workers.dev/'}):
            cfg=s.default_scan_config()
        self.assertEqual(cfg['worker_origin'],'https://other-worker.example.workers.dev')
        seen=[]
        def factory(route,opts):
            async def handler(req):seen.append(req.url.host);return httpx.Response(200,json=PROFILE)
            return httpx.AsyncClient(transport=httpx.MockTransport(handler))
        gate=s.AsyncRequestGate(0,0,asyncio.Event(),ceiling=1,initial=1,adaptive=False)
        async with s.WorkerApiClient(gate,s.ProxyPool([]),settings=cfg,client_factory=factory) as client:
            await client.request_json('profile',{'username':'a'})
        self.assertEqual(seen,['other-worker.example.workers.dev'])
        cfg.pop('worker_origin');s.validate_scan_config(cfg)
        for origin in ('https://www.tiktok.com','https://u:p@x.workers.dev','http://x.workers.dev','https://x.workers.dev/path'):
            with self.assertRaises(s.ExporterError):s.validate_worker_origin(origin)


class AvatarIntegrationTests(ConfigurationTests):
    def test_main_caches_public_private_and_skipped_avatars(self):
        self.input.write_text('["a","b","c"]')
        async def handler(req):
            if req.url.host=='p16-sign.tiktokcdn.com':
                return httpx.Response(200,headers={'Content-Type':'image/png'},content=picture())
            name=req.url.params.get('username')
            if name:
                data=response_profile(name,{'a':1,'b':2,'c':3}[name],followers='20' if name=='c' else '0',private=name=='b')
                data['data']['profile']=URL
                return httpx.Response(200,json=data)
            return httpx.Response(200,json=page())
        code,_,_=self.run_main(self.new(limit='10'),handler)
        self.assertEqual(code,0)
        root=self.base/'wanted'
        data=report.build_data(root)
        self.assertEqual({r['status'] for r in data['rows']},{'completed','private','skipped_size'})
        self.assertTrue(all(r['avatar'].startswith('report_assets/avatars/') for r in data['rows']))
        self.assertEqual(sum('p16-sign.tiktokcdn.com' in u for u in self.calls),1)
        self.assertTrue((root/'report_wanted.html').is_file())

    def test_resume_backfills_old_records_and_reuses_saved_cache(self):
        root,cfg,meta=self.create_saved()
        state=s.DurableScanState(root,meta,root,cfg);claimed=state.claim()
        p=s.parse_profile_response(response_profile('a',1),'a');p.avatar_url=URL
        pages=root/'pages';pages.mkdir()
        with s.UserStore(pages/'a.sqlite3') as store:store.save_checkpoint('profile',s.asdict(p))
        state.record(s.error_profile_result(claimed,status='skipped_size',started_at_utc=s.utc_iso(),error='saved skip'))
        state.close()
        async def handler(req):
            self.assertEqual(req.url.host,'p16-sign.tiktokcdn.com')
            return httpx.Response(200,headers={'Content-Type':'image/png'},content=picture())
        code,_,_=self.run_main(['1','1','1','n'],handler)
        self.assertEqual(code,0);self.assertEqual(len(self.calls),1)
        self.assertTrue(report.build_data(root)['rows'][0]['avatar'])
        # Reconcile the same saved state again: completed jobs and cache produce
        # no Worker lookup, list retrieval, or second image request.
        session=json.loads((root/'session.json').read_text());session['scan_complete']=False
        s.atomic_write_json(root/'session.json',session)
        self.calls.clear()
        code,_,_=self.run_main(['1','1','1','n'],handler)
        self.assertEqual(code,0);self.assertEqual(self.calls,[])
