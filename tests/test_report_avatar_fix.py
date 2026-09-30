"""Missing avatars in pending/error rows, shared sections and image-only repair."""
import asyncio
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import httpx
import report_generator as report
import repair_report
import tiktok_worker_scanner as s
from avatar_cache import AvatarReader
from test_avatar_update import picture, URL


def fixture(root):
    cfg=s.default_scan_config();cfg.update(target_username='wanted',workers=4)
    cfg['request_delay']={'minimum_seconds':0,'maximum_seconds':0}
    s.atomic_write_json(root/'scan_config.json',cfg)
    state=s.DurableScanState(root,{'input_imported':True},root,cfg)
    names=('pending','failed','partial','skipped','matched','noimage')
    for i,name in enumerate(names):
        uid=str(100+i);state.add_jobs([s.ProfileJob(name,'https://www.tiktok.com/@'+name,('fixture',),uid)])
        if name in ('pending','failed'):
            state.discover('seed','followers',[{'username':name,'id':uid,'private_account':False,'avatar_url':URL+'&who='+name}])
        if name not in ('pending','noimage'):
            status={'failed':'network_error','partial':'partial','skipped':'skipped_size','matched':'completed'}[name]
            meta={'profile':{'username':name,'uid':uid,'display_name':name}}
            if name in ('skipped','matched'):meta['profile']['avatar_url']=URL+'&who='+name
            result={'username':name,'status':status,'complete':name=='matched','metadata':meta,'list_results':{}}
            with state.connection:
                state.connection.execute('UPDATE jobs SET status=?,result=? WHERE username=?',(status,json.dumps(result),name))
    pages=root/'pages';pages.mkdir()
    with s.UserStore(pages/'partial.sqlite3') as store:
        store.save_checkpoint('profile',{'username':'partial','uid':'102','avatar_url':URL+'&who=partial'})
    state.snapshot();state.close()
    s.atomic_write_json(root/'sucess_find.json',{'target_user':'wanted','profiles':[{'username':'matched','found_in':['followers','following']}],'summary':{}})
    return cfg


class ReportAvatarFixTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.cfg=fixture(self.root)
        self.calls=[]

    async def asyncTearDown(self):
        self.temp.cleanup()

    def factory(self,route,options):
        async def handle(request):
            self.calls.append(request)
            self.assertEqual(request.url.host,'p16-sign.tiktokcdn.com')
            return httpx.Response(200,content=picture(),headers={'Content-Type':'image/png'})
        return httpx.AsyncClient(transport=httpx.MockTransport(handle),event_hooks=options.get('event_hooks',{}))

    async def test_pending_error_and_partial_avatars_repair_without_rescan(self):
        original=report.build_data(self.root)
        self.assertTrue(all(not row['avatar'] for row in original['rows']))
        files=('state.sqlite3','scan_state.json','scan_config.json')
        before={n:hashlib.sha256((self.root/n).read_bytes()).hexdigest() for n in files}
        old=s.WorkerApiClient
        def client(gate,pool,**kwargs):return old(gate,pool,client_factory=self.factory,**kwargs)
        with patch.object(s,'WorkerApiClient',side_effect=client),patch.object(s,'console'):
            counts=await repair_report.repair_avatars(self.root,self.cfg,s.ProxyPool([]))
            self.assertEqual(counts['available'],5)
            self.assertEqual(len(self.calls),5)
            await repair_report.repair_avatars(self.root,self.cfg,s.ProxyPool([]))
        self.assertEqual(len(self.calls),5)
        after=report.build_data(self.root)
        for a,b in zip(original['rows'],after['rows']):
            if b['username']=='noimage':self.assertEqual(b['avatar'],'')
            else:self.assertTrue((self.root/b['avatar']).is_file())
            self.assertEqual({k:v for k,v in a.items() if k!='avatar'},{k:v for k,v in b.items() if k!='avatar'})
        self.assertEqual(before,{n:hashlib.sha256((self.root/n).read_bytes()).hexdigest() for n in files})

    async def test_saved_input_avatars_survive_identity_only_queue(self):
        s.atomic_write_json(self.root/'starting_dataset.json',{'followers':[{'uniqueId':'noimage','user_id':'105','avatarThumb':URL+'&from=input'}]})
        old=s.WorkerApiClient
        with patch.object(s,'WorkerApiClient',side_effect=lambda gate,pool,**kw:old(gate,pool,client_factory=self.factory,**kw)),patch.object(s,'console'):
            await repair_report.repair_avatars(self.root,self.cfg,s.ProxyPool([]))
        row=next(r for r in report.build_data(self.root)['rows'] if r['username']=='noimage')
        self.assertTrue(row['avatar'])

    async def test_background_backfill_runs_before_scan_completion_and_closes(self):
        gate=s.AsyncRequestGate(0,0,asyncio.Event(),ceiling=4,initial=4,adaptive=False)
        async with s.WorkerApiClient(gate,s.ProxyPool([]),client_factory=self.factory,avatar_directory=self.root) as client:
            client.start_avatar_backfill(self.root)
            await asyncio.wait_for(client.avatar_backfill_task,5)
            self.assertEqual(len(self.calls),5)
            self.assertEqual(next(r for r in report.build_data(self.root)['rows'] if r['username']=='pending')['status'],'pending')
        self.assertFalse(any(t.get_name().startswith('report-avatar') for t in asyncio.all_tasks() if t is not asyncio.current_task()))

    async def test_global_stop_prevents_background_image_dispatch(self):
        gate=s.AsyncRequestGate(0,0,asyncio.Event(),ceiling=4,initial=4,adaptive=False)
        async with s.WorkerApiClient(gate,s.ProxyPool([]),client_factory=self.factory,avatar_directory=self.root) as client:
            gate.stop_event.set()
            client.start_avatar_backfill(self.root)
            self.assertIsNone(client.avatar_backfill_task)
            await client.cache_saved_avatars(self.root)
        self.assertEqual(self.calls,[])

    async def test_background_cancellation_finishes_before_cache_close(self):
        started=asyncio.Event()
        async def slow(url):started.set();await asyncio.sleep(60)
        gate=s.AsyncRequestGate(0,0,asyncio.Event(),ceiling=4,initial=4,adaptive=False)
        async with s.WorkerApiClient(gate,s.ProxyPool([]),client_factory=self.factory,avatar_directory=self.root) as client:
            client.avatars.download=slow
            client.start_avatar_backfill(self.root)
            await asyncio.wait_for(started.wait(),5)
        self.assertTrue(client.avatar_backfill_task.done())
        self.assertFalse(any(t.get_name().startswith('report-avatar') for t in asyncio.all_tasks() if t is not asyncio.current_task()))


if __name__=='__main__':unittest.main()
