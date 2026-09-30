import asyncio,copy,io,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch,AsyncMock
from contextlib import redirect_stdout
import httpx
import tiktok_worker_scanner as s
import report_generator as report
from test_async_scanner import page,profile,member,AsyncFakeClient,AsyncTests,apatch
from test_configured_scanner import ConfigurationTests,response_profile,public_member,job

class BootstrapFlowTests(ConfigurationTests):
 def test_real_kyriakakii_398_main_scan_and_resume(self):
  target='nature_7007';seed='_kyriakakii';uid='6702723284414858245'
  names=['a'+str(i) for i in range(397)]+[target]
  rows=[public_member(name,20000+i) for i,name in enumerate(names)]
  chunks=[rows[i:i+20] for i in range(0,380,20)]+[rows[380:394],rows[394:]]
  async def handler(req):
   name=req.url.params.get('username')
   if name:
    if name==seed:
     d=response_profile(seed,uid,'420','235');d['data']['see_following']='No';return httpx.Response(200,json=d)
    self.assertIn(name,names)
    return httpx.Response(200,json=response_profile(name,20000+names.index(name),private=True))
   self.assertEqual(req.url.params['Uid'],uid);self.assertEqual(req.url.path,'/api/followers')
   cursor=req.url.params['minCursor'];idx=0 if cursor=='0' else int(cursor.removeprefix('opaque-'))
   return httpx.Response(200,json=page(chunks[idx],idx<20,'opaque-'+str(idx+1) if idx<20 else ''))
  code,out,_=self.run_main(['0','2',seed,target,'0','0','1','4','0','0','n','1'],handler)
  self.assertEqual(code,0);self.assertNotIn('Bootstrap is incomplete',out);self.assertIn('[PROGRESS 398/398]',out)
  self.assertIn('Private accounts skipped: 398',out);self.assertIn('[PROGRESS 0/398]',out)
  root=self.base/target;state=json.loads((root/'scan_state.json').read_text(encoding="utf-8"))
  self.assertEqual(state['bootstrap']['status'],'usable_with_restricted_list');self.assertEqual(state['summary']['processed_profiles'],398)
  dataset=json.loads((root/'starting_dataset.json').read_text(encoding="utf-8"));self.assertEqual(len(dataset),398);self.assertTrue(all(r['from_followers'] and not r['from_following'] for r in dataset))
  doc=json.loads((root/'sucess_find.json').read_text(encoding="utf-8"));entry=next(p for p in doc['profiles'] if p['username']==seed)
  self.assertEqual(entry['relationship'],{'target_follows_profile':True,'profile_follows_target':None,'mutual':None})
  data=report.build_data(root);source=next(r for r in data['rows'] if r['username']==seed);self.assertIsNone(source['mutual'])
  # Resume between a finished bootstrap and dataset import: local export and
  # restricted checkpoint must be reused; all already checked jobs stay terminal.
  metadata=json.loads((root/'session.json').read_text(encoding="utf-8"));metadata.update(scan_complete=False,input_imported=False);s.atomic_write_json(root/'session.json',metadata)
  state.update(scan_complete=False,input_imported=False);s.atomic_write_json(root/'scan_state.json',state)
  self.calls.clear()
  async def no_network(req):raise AssertionError('Completed/restricted resume used network')
  code,out,_=self.run_main(['1','1','1','n'],no_network)
  self.assertEqual(code,0);self.assertFalse(self.calls);self.assertIn('Reused from previous run: 398',out);self.assertIn('Checked during this execution: 0',out)
  self.assertEqual(json.loads((root/'scan_state.json').read_text(encoding="utf-8"))['summary']['private_profiles'],398)
 def test_empty_followers_restricted_following_clean_completion(self):
  async def handler(req):
   if req.url.params.get('username'):
    d=response_profile('seed',9);d['data']['see_following']='No';return httpx.Response(200,json=d)
   self.assertTrue(req.url.path.endswith('followers'));return httpx.Response(200,json=page())
  code,out,_=self.run_main(['0','2','seed','wanted','0','0','1','4','0','0','n','1'],handler)
  self.assertEqual(code,0);self.assertIn('no usable source profiles',out)
  root=self.base/'wanted';self.assertEqual(json.loads((root/'starting_dataset.json').read_text(encoding="utf-8")),[])
  self.assertEqual(json.loads((root/'scan_state.json').read_text(encoding="utf-8"))['bootstrap']['status'],'empty_dataset')
 def test_private_not_found_http_error_progress_all_count(self):
  self.input.write_text('["a","b","c","d"]', encoding="utf-8")
  async def handler(req):
   name=req.url.params.get('username')
   if name=='a':return httpx.Response(200,json=response_profile('a',1,private=True))
   if name=='b':return httpx.Response(200,json={'status':'error','message':'User not found'})
   if name=='c':return httpx.Response(418)
   return await self.empty(req)
  code,out,_=self.run_main(self.new(),handler);self.assertEqual(code,0)
  self.assertIn('[PROGRESS 4/4]',out);self.assertIn('Private accounts skipped: 1',out);self.assertIn('Not Found: 1',out)
  summary=json.loads((self.base/'wanted'/'scan_state.json').read_text(encoding="utf-8"))['summary']
  self.assertEqual(summary['processed_profiles'],4);self.assertEqual(summary['completed_overall'],1);self.assertEqual(summary['failed_profiles'],1);self.assertEqual(summary['skipped_profiles'],2)

class BootstrapAcceptanceTests(unittest.IsolatedAsyncioTestCase):
 async def test_all_source_outcomes_and_queue_import(self):
  good={'complete':True,'endpoint_exhausted':True,'stop_reason':'complete_exact','unique_records_saved':1,'warnings':[]}
  mismatch={**good,'stop_reason':'complete_count_mismatch','warnings':['count warning']}
  restricted={'complete':False,'endpoint_exhausted':False,'stop_reason':'list_restricted','error':'Explicit saved visibility restriction','unique_records_saved':0}
  bad={'complete':False,'endpoint_exhausted':False,'stop_reason':'http_error','error':'Worker failed','unique_records_saved':1}
  timeout={**bad,'stop_reason':'response_timeout'}
  cases=[(good,good,'complete'),(mismatch,good,'complete_with_count_warning'),(good,restricted,'usable_with_restricted_list'),(mismatch,restricted,'usable_with_restricted_list'),(restricted,good,'usable_with_restricted_list'),(bad,good,'incomplete'),(restricted,restricted,'unavailable'),(good,timeout,'incomplete')]
  for left,right,expected in cases:
   with self.subTest(expected=expected,left=left['stop_reason'],right=right['stop_reason']),tempfile.TemporaryDirectory() as d,patch.object(s,'console'):
    root=Path(d);cfg=s.default_scan_config();state=s.DurableScanState(root,{},root,cfg);success=s.SuccessRecorder(root/'sucess_find.json',target_user='wanted',input_path=root/'starting_dataset.json',started_at_utc=s.utc_iso())
    for name,info in [('followers',left),('following',right)]:
     if info['complete']:state.discover('seed',name,[{'username':'a','id':'1','private_account':False}],phase=0)
    result=s.ProfileProcessResult('seed','',[], 'partial',None,left['complete'] and right['complete'],[],s.utc_iso(),s.utc_iso(),{'followers':left,'following':right})
    with patch.object(s,'process_profile',new=AsyncMock(return_value=result)):
     if expected=='incomplete':
      with self.assertRaises(s.ExporterError):await s.bootstrap_dataset(type('Client',(),{})(),'seed',root=root,state=state,success=success,stop_event=asyncio.Event())
     else:
      path=await s.bootstrap_dataset(type('Client',(),{})(),'seed',root=root,state=state,success=success,stop_event=asyncio.Event());jobs,_=s.load_jobs_allow_empty(path);state.add_jobs(jobs)
      self.assertEqual(state.summary()['total_profiles'],0 if expected=='unavailable' else 1)
    self.assertEqual(state.metadata['bootstrap']['status'],expected);state.close()

class RetryTests(AsyncTests):
 async def test_profile_404_recovers_or_exactly_two_requests(self):
  for final in (200,404):
   routes=[];urls=[]
   async def handler(route,req):
    routes.append(route.label);urls.append(str(req.url));return httpx.Response(404 if len(urls)==1 else final,json=response_profile('example',1))
   c,_=self.setup_http(handler);c.settings={'retry_attempts':1}
   async with c,apatch(c.gate,'wait',new=AsyncMock()):
    if final==200:self.assertEqual((await c.lookup_profile('example')).username,'example')
    else:
     with self.assertRaises(s.WorkerApiError) as caught:await c.lookup_profile('example')
     self.assertEqual(caught.exception.kind,'http_error');self.assertEqual(caught.exception.code,404)
   self.assertEqual(len(urls),2);self.assertEqual(urls[0],urls[1]);self.assertEqual(routes[0],routes[1]);self.assertFalse(any(p.disabled for p in c.pool.states))
 async def test_list_404_recover_both_lists_and_saved_cursor_on_failure(self):
  for name in ('followers','following'):
   for final in (200,404):
    with self.subTest(name=name,final=final):
     calls=[];p=profile(followers='2',following='2')
     async def handler(route,req):
      calls.append(req.url.params['minCursor'])
      if len(calls)==1:return httpx.Response(200,json=page([member('a','1')],True,'cursor+/=='))
      if len(calls)==2 or final==404:return httpx.Response(404)
      return httpx.Response(200,json=page([member('b','2')]))
     c,_=self.setup_http(handler);db=self.root/f'{name}{final}.sqlite3'
     async with c,apatch(c.gate,'wait',new=AsyncMock()):
      with s.UserStore(db) as store:
       result=await s.export_one_list(c,store,p,name,target_username='wanted',stop_event=asyncio.Event())
       saved=store.checkpoint(name);self.assertEqual(store.count(name),2 if final==200 else 1)
     self.assertEqual(calls,['0','cursor+/==','cursor+/==']);self.assertEqual(result.complete,final==200)
     self.assertEqual(result.retry_count,1)
     if final==404:
      self.assertEqual(result.stop_reason,'http_error');self.assertEqual(saved['next_cursor'],'cursor+/==');self.assertFalse(result.endpoint_exhausted)
      replacement=AsyncFakeClient(p,{name:[page([member('b','2')])]})
      with s.UserStore(db) as store:
       resumed=await s.export_one_list(replacement,store,p,name,target_username='wanted',stop_event=asyncio.Event())
       self.assertTrue(resumed.complete);self.assertEqual(store.count(name),2)
      self.assertEqual(replacement.calls,[(name,'cursor+/==',2)])
 async def test_404_after_hundreds_of_committed_pages(self):
  calls=[];p=profile();counter=0
  async def handler(route,req):
   nonlocal counter
   cursor=req.url.params['minCursor'];calls.append(cursor)
   if cursor=='cursor-220':return httpx.Response(404)
   counter+=1;return httpx.Response(200,json=page([member('a'+str(counter),str(counter))],True,'cursor-'+str(counter)))
  c,_=self.setup_http(handler)
  async with c,apatch(c.gate,'wait',new=AsyncMock()):
   with s.UserStore(self.root/'hundreds.sqlite3') as store:
    r=await s.export_one_list(c,store,p,'followers',target_username='wanted',stop_event=asyncio.Event())
    self.assertEqual(store.count('followers'),220);self.assertEqual(store.checkpoint('followers')['next_cursor'],'cursor-220')
  self.assertEqual(len(calls),222);self.assertEqual(calls[-2:],['cursor-220']*2);self.assertEqual(r.stop_reason,'http_error')

class RestrictedStateTests(unittest.IsolatedAsyncioTestCase):
 async def test_terminal_restriction_resume_and_old_partial_migration(self):
  for advertised,observed in [('1',True),('367',True),('0',False)]:
   with self.subTest(advertised=advertised),tempfile.TemporaryDirectory() as d,patch.object(s,'console'):
    root=Path(d);p=profile(followers=advertised);p.following_visible=False;p.metadata['see_following']='No'
    cfg=s.default_scan_config();cfg['target_username']='wanted';metadata={'input_imported':True}
    state=s.DurableScanState(root,metadata,root,cfg);state.add_jobs([job('example')]);j=state.claim()
    rec=s.SuccessRecorder(root/'sucess_find.json',target_user='wanted',input_path=root/'starting_dataset.json',started_at_utc=s.utc_iso())
    client=AsyncFakeClient(p,{'followers':[page([member('wanted','2')] if observed else [])]})
    result=await s.process_profile(client,j,output_directory=root,success_recorder=rec,stop_event=asyncio.Event(),use_resume=True,store_directory=root/'pages')
    self.assertEqual(result.status,'restricted');self.assertFalse(result.complete);self.assertEqual(client.calls,[('followers','0',1)])
    self.assertEqual(state.record(result),(1,1));self.assertEqual(state.summary()['restricted_profiles'],1);self.assertEqual(state.summary()['failed_profiles'],0)
    if observed:
     entry=json.loads(rec.path.read_text(encoding="utf-8"))['profiles'][0];self.assertTrue(entry['relationship']['target_follows_profile']);self.assertIsNone(entry['relationship']['profile_follows_target']);self.assertIsNone(entry['relationship']['mutual'])
    # Downgrade to the old partial representation, then migrate from evidence.
    old=s.asdict(result);old['status']='partial'
    with state.connection:state.connection.execute("UPDATE jobs SET status='failed',result=?",(json.dumps(old),))
    state.recover(rec);self.assertEqual(state.summary()['restricted_profiles'],1);self.assertIsNone(state.claim())
    state.finalize(scan_complete=True,fatal_error=None)
    row=report.build_data(root)['rows'][0];self.assertEqual(row['relationship']['target_follows_profile'],observed);self.assertIsNone(row['mutual']);state.close()
    state=s.DurableScanState(root,metadata,root,cfg);state.recover(rec);self.assertIsNone(state.claim());self.assertEqual(state.summary()['already_processed'],1);state.close()
 async def test_restricted_plus_genuine_failure_is_not_terminal_restricted(self):
  results={'followers':{'complete':False,'stop_reason':'http_error'},'following':{'complete':False,'stop_reason':'list_restricted'}}
  self.assertFalse(s.restricted_terminal(results))
 async def test_131_of_398_queue_reconciliation(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);state=s.DurableScanState(root,{},root);state.add_jobs(job('a'+str(i)) for i in range(398))
   with state.connection:
    for status,start,end in [('completed',0,69),('http_error',69,97),('private',97,131)]:
     state.connection.executemany('UPDATE jobs SET status=? WHERE username=?',[(status,'a'+str(i)) for i in range(start,end)])
   summary=state.summary();self.assertEqual(summary['processed_profiles'],131);self.assertEqual(summary['total_profiles'],398);self.assertEqual(summary['failed_profiles'],28);self.assertEqual(summary['private_profiles'],34)
   self.assertEqual(summary['completed_overall']+summary['failed_profiles']+summary['skipped_profiles']+summary['restricted_profiles']+summary['remaining_profiles'],398);state.close()

if __name__=='__main__':unittest.main()
