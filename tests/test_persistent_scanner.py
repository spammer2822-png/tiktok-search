import asyncio, ast, concurrent.futures, copy, json, os, selectors, signal, subprocess, sys, tempfile, time, unittest
from pathlib import Path
from unittest.mock import patch
import tiktok_worker_scanner as s
from test_async_scanner import profile, member, page, AsyncFakeClient, PROFILE


def metadata(root, mode='direct'):
 return {'session_id':'test','identity':s.source_identity('json',str(root/'input.json')),'created_at_utc':s.utc_iso(),'network_mode':mode,'input_imported':True,'scan_complete':False}
def setup(root,n=40,mode='direct'):
 directory=root/'session';directory.mkdir(exist_ok=True)
 state=s.DurableScanState(directory,metadata(root,mode),root)
 state.add_jobs([s.ProfileJob(f'user{i}',f'https://www.tiktok.com/@user{i}',('profiles',)) for i in range(n)])
 success=s.SuccessRecorder(directory/s.SUCCESS_FILE_NAME,target_user=s.TARGET_USER,input_path=root/'input.json',started_at_utc=s.utc_iso())
 return state,success

def write_valid(root,name):
 p=profile(name);results={}
 with s.UserStore(root/(name+'.members.sqlite')) as store:
  for kind in s.SELECTED_LISTS:
   result=s.ListExportResult(kind,0);result.endpoint_exhausted=True;result.stop_reason='endpoint_exhausted';result.finish();results[kind]=result
  out=s.output_file_path(root,name,s.SELECTED_LISTS)
  s.write_final_json(out,profile=p,selected_lists=list(s.SELECTED_LISTS),results=results,store=store,started_at=s.utc_iso())
 return s.ProfileProcessResult(name,f'https://www.tiktok.com/@{name}',['profiles'],'complete',str(out),True,[],s.utc_iso(),s.utc_iso(),{k:s.asdict(v) for k,v in results.items()})

class DurableTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.patch=patch.object(s,'console');self.patch.start()
 async def asyncTearDown(self):self.patch.stop();self.temp.cleanup()
 async def test_40_profiles_restart_at_5_without_completed_network(self):
  state,success=setup(self.root)
  for i in range(5):
   job=state.claim();self.assertEqual(job.username,f'user{i}');state.record(write_valid(self.root,job.username))
  self.assertEqual(state.summary()['completed_overall'],5);state.close()
  state,success=setup(self.root);state.recover(success)
  self.assertEqual(state.summary()['completed_overall'],5);self.assertEqual(state.summary()['completed_this_run'],0);self.assertEqual(state.summary()['pending_profiles'],35)
  claimed=[]
  while job:=state.claim():claimed.append(job.username)
  self.assertEqual(len(claimed),35);self.assertTrue(all(f'user{i}' not in claimed for i in range(5)));state.close()
 async def test_completed_export_saved_before_state_checkpoint(self):
  state,success=setup(self.root,1);job=state.claim();write_valid(self.root,job.username);state.close()
  state,success=setup(self.root,1);state.recover(success)
  self.assertEqual(state.summary()['completed_overall'],1);self.assertIsNone(state.claim());state.close()
 async def test_in_progress_recovery_and_corrupt_export(self):
  state,success=setup(self.root,2);state.claim();state.claim();s.output_file_path(self.root,'user0',s.SELECTED_LISTS).write_text('{"complete":true', encoding="utf-8")
  state.close();state,success=setup(self.root,2);state.recover(success);self.assertEqual(state.summary()['pending_profiles'],2);state.close()
 async def test_claim_is_atomic_and_no_duplicate_jobs(self):
  state,success=setup(self.root,40)
  with concurrent.futures.ThreadPoolExecutor(8) as ex:jobs=list(ex.map(lambda _:state.claim(),range(48)))
  names=[j.username for j in jobs if j];self.assertEqual(len(names),40);self.assertEqual(len(set(names)),40);state.close()
 async def test_resume_proxy_to_direct_without_reset(self):
  state,success=setup(self.root,2,'webshare');state.claim();state.record(write_valid(self.root,'user0'));state.close()
  state,success=setup(self.root,2,'direct');state.recover(success);self.assertEqual(state.summary()['completed_overall'],1);self.assertEqual(state.claim().username,'user1');state.close()
 async def test_failed_and_private_are_terminal_after_restart(self):
  state,success=setup(self.root,2)
  for status in ['private','network_timeout']:
   job=state.claim();state.record(s.error_profile_result(job,status=status,started_at_utc=s.utc_iso(),error='fixture'))
  self.assertIsNone(state.claim());self.assertEqual(state.summary()['completed_overall'],0);state.close()
  state,success=setup(self.root,2);state.recover(success);self.assertEqual(state.summary()['skipped_profiles'],1);self.assertIsNone(state.claim());self.assertEqual(state.summary()['processed_profiles'],2);state.close()
 async def test_partial_pages_resume_opaque_cursor(self):
  p=profile('seed','2','0');database=self.root/'page.sqlite'
  c=AsyncFakeClient(p,{'followers':[page([member('one','1')],True,'a+/=='),s.WorkerApiError('timeout',kind='network_timeout')]})
  with s.UserStore(database) as store:
   first=await s.export_one_list(c,store,p,'followers',target_username=s.TARGET_USER,stop_event=asyncio.Event())
   self.assertFalse(first.complete);self.assertEqual(store.count('followers'),1)
  c=AsyncFakeClient(p,{'followers':[page([member('two','2')],False,'')]})
  with s.UserStore(database) as store:
   final=await s.export_one_list(c,store,p,'followers',target_username=s.TARGET_USER,stop_event=asyncio.Event())
   self.assertTrue(final.complete);self.assertEqual(final.batches,2);self.assertEqual(store.count('followers'),2)
  self.assertEqual(c.calls,[('followers','a+/==',2)])
 async def test_bootstrap_dedupe_and_cached_dataset(self):
  state,success=setup(self.root,0);p=profile('seed','2','2')
  c=AsyncFakeClient(p,{'followers':[page([member('one','1'),member('Two','2')])], 'following':[page([member('two','2'),member('three','3')])]})
  path=await s.bootstrap_dataset(c,'seed',root=self.root,state=state,success=success,stop_event=asyncio.Event());jobs,_=s.load_profile_jobs(path)
  self.assertEqual({j.username.casefold() for j in jobs},{'one','two','three'});state.add_jobs(jobs);self.assertEqual(state.total_profiles,3)
  cached=AsyncFakeClient(p,{})
  cached.lookup_profile=lambda _: (_ for _ in ()).throw(AssertionError('cache must prevent lookup'))
  await s.bootstrap_dataset(cached,'seed',root=self.root,state=state,success=success,stop_event=asyncio.Event());self.assertEqual(cached.calls,[]);state.close()
 async def test_discoveries_saved_not_recursively_queued(self):
  state,success=setup(self.root,1)
  state.discover('user0','followers',[{'username':'NewUser','id':'8','private_account':False},{'username':'newuser','id':'8','private_account':False}]);self.assertEqual(state.total_profiles,1)
  self.assertEqual(state.summary()['discovered_profiles'],1);state.close()
  state,success=setup(self.root,1);self.assertEqual(state.summary()['discovered_profiles'],1);self.assertEqual(state.total_profiles,1);state.close()
 async def test_complete_page_checkpoint_recovers_target_side_effect(self):
  p=profile('seed','1','0');database=self.root/'page.sqlite'
  async def cancelled(*args):raise asyncio.CancelledError
  c=AsyncFakeClient(p,{'followers':[page([member(s.TARGET_USER,'1')])]})
  with s.UserStore(database) as store:
   await s.export_one_list(c,store,p,'followers',target_username=s.TARGET_USER,on_target_match=cancelled,stop_event=asyncio.Event())
  c=AsyncFakeClient(p,{});matches=[]
  async def callback(*args):matches.append(args)
  with s.UserStore(database) as store:
   result=await s.export_one_list(c,store,p,'followers',target_username=s.TARGET_USER,on_target_match=callback,stop_event=asyncio.Event())
  self.assertTrue(result.complete);self.assertEqual(len(matches),1);self.assertEqual(c.calls,[])
 async def test_page_transaction_failure_rolls_back_members_and_cursor(self):
  p=profile('seed','1','0');database=self.root/'page.sqlite';c=AsyncFakeClient(p,{'followers':[page([member('one','1')],True,'next')]})
  with s.UserStore(database) as store,patch.object(s.UserStore,'save_checkpoint',side_effect=OSError('disk full')):
   with self.assertRaises(OSError):await s.export_one_list(c,store,p,'followers',target_username=s.TARGET_USER,stop_event=asyncio.Event())
  with s.UserStore(database) as store:
   self.assertEqual(store.count('followers'),0);self.assertIsNone(store.checkpoint('followers'));self.assertFalse(store.cursor_seen('followers','0'))
 async def test_new_session_reuses_previous_exports(self):
  write_valid(self.root,'user0');state,success=setup(self.root,1);state.recover(success);self.assertIsNone(state.claim());self.assertEqual(state.summary()['completed_overall'],1);state.close()
 async def test_default_proxy_path_and_malformed_lines(self):
  self.assertEqual(str(s.WEBSHARE_PROXY_FILE),r'C:\Users\vailo\Downloads\webshare_proxy.txt')
  path=self.root/'proxy.txt';path.write_text('\n1.2.3.4:8000:u:p\n1.2.3.4:0:u:p\n1.2.3.4:8000:u\n1.2.3.4:8000\nhttp://p.webshare.io:80\n', encoding="utf-8")
  parsed=s.load_proxy_configs(path);self.assertEqual(len(parsed),2);self.assertEqual(parsed[0].password,'p')
 async def test_python311_syntax(self):ast.parse(Path(s.__file__).read_text(encoding="utf-8"),feature_version=(3,11))
 async def test_run_scan_uses_persistent_claims(self):
  import httpx
  state,success=setup(self.root,8);state.recover(success);calls=[]
  Original=s.WorkerApiClient
  def factory(gate,pool,**kwargs):
   def client_factory(route,options):
    async def handler(req):
     name=req.url.params.get('username');calls.append(str(req.url));await asyncio.sleep(.005)
     if name:
      d=copy.deepcopy(PROFILE);d['data'].update(username=name,userid=str(1000+int(name.removeprefix('user'))),followers='0',following='0');return httpx.Response(200,json=d)
     return httpx.Response(200,json=page())
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))
   return Original(gate,pool,client_factory=client_factory,**kwargs)
  with patch.object(s,'WorkerApiClient',side_effect=factory):
   _,document,fatal=await s.run_scan([],output_directory=self.root,worker_count=4,pacing=(0,0),success_recorder=success,state_recorder=state,use_resume=True)
  self.assertIsNone(fatal);self.assertTrue(document['scan_complete']);self.assertEqual(state.summary()['completed_overall'],8);self.assertEqual(len(calls),24);state.close()
  state,success=setup(self.root,8);state.recover(success);calls.clear()
  with patch.object(s,'WorkerApiClient',side_effect=factory):
   await s.run_scan([],output_directory=self.root,worker_count=4,pacing=(0,0),success_recorder=success,state_recorder=state,use_resume=True)
  self.assertEqual(calls,[]);state.close()


class ProcessTests(unittest.TestCase):
 def child(self,root,mode,graceful=False):
  env=dict(os.environ);env['PYTHONPATH']=os.pathsep.join([str(Path.cwd()), *[str(Path(p).resolve()) for p in sys.path if p]])
  child=subprocess.Popen([sys.executable,__file__,'child',str(root),mode],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env)
  selector=selectors.DefaultSelector();selector.register(child.stdout,selectors.EVENT_READ)
  if not selector.select(15):child.kill();self.fail('child did not reach crash point')
  line=child.stdout.readline().strip();selector.close()
  if line!='READY':
   child.kill();out,err=child.communicate();self.fail(f'child failed: {line} {err}')
  child.send_signal(signal.SIGTERM) if graceful else child.kill()
  child.communicate(timeout=5)
  self.assertEqual(child.returncode,130) if graceful else self.assertNotEqual(child.returncode,0)
 def test_sigterm_main_flushes_claim_and_exits(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);self.child(root,'sigterm',graceful=True)
   paths=list(root.glob('*/scan_state.json'));self.assertEqual(len(paths),1)
   data=json.loads(paths[0].read_text(encoding="utf-8"));self.assertEqual(data['summary']['pending_profiles'],1);self.assertEqual(data['summary']['completed_overall'],0)
   reports=list(paths[0].parent.glob('report_*.html'));self.assertEqual(len(reports),1);self.assertEqual(json.loads((paths[0].parent/'sucess_find.json').read_text(encoding="utf-8"))['report']['filename'],reports[0].name)
 def test_forcekill_four_workers_three_completed_one_active(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);self.child(root,'workers');state,success=setup(root,4);state.recover(success)
   self.assertEqual(state.summary()['completed_overall'],3);self.assertEqual(state.summary()['pending_profiles'],1);self.assertEqual(state.claim().username,'user3');state.close()
 def test_forcekill_bootstrap_restarts_at_saved_next_cursor(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);self.child(root,'bootstrap')
   async def resume():
    state,success=setup(root,0);p=profile('seed','2','0');c=AsyncFakeClient(p,{'followers':[page([member('two','2')])],'following':[page()]})
    async def forbidden(name):raise AssertionError('must reuse persisted profile lookup')
    c.lookup_profile=forbidden
    path=await s.bootstrap_dataset(c,'seed',root=root,state=state,success=success,stop_event=asyncio.Event());document=json.loads(path.read_text(encoding="utf-8"))
    self.assertEqual(c.calls,[('followers','opaque+/==',2),('following','0',1)]);self.assertEqual(len(document),2);state.close()
   asyncio.run(resume())
 def test_forcekill_after_result_before_checkpoint(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);self.child(root,'result');state,success=setup(root,1);state.recover(success);self.assertEqual(state.summary()['completed_overall'],1);self.assertIsNone(state.claim());state.close()
 def test_forcekill_before_response_retries_job(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);self.child(root,'request');state,success=setup(root,1);state.recover(success);self.assertEqual(state.summary()['completed_overall'],0);self.assertEqual(state.claim().username,'user0');state.close()

def child_sigterm(root):
 import httpx
 s.console=lambda *args,**kwargs:None
 (root/'input.json').write_text('["user0"]', encoding="utf-8")
 os.environ.update(TIKTOK_INPUT_JSON=str(root/'input.json'),TIKTOK_EXPORT_DIR=str(root),TIKTOK_PROXY_ONLY='0',TIKTOK_MIN_DELAY_SECONDS='0',TIKTOK_MAX_DELAY_SECONDS='0')
 Original=s.WorkerApiClient
 def factory(gate,pool,**kwargs):
  def client_factory(route,options):
   async def handler(req):
    print('READY',flush=True);await asyncio.sleep(1000)
   return httpx.AsyncClient(transport=httpx.MockTransport(handler))
  return Original(gate,pool,client_factory=client_factory,**kwargs)
 with patch('builtins.input',side_effect=['0','1','','0','0','1','4','0','0','n','1']),patch.object(s,'WorkerApiClient',side_effect=factory):return s.main()

async def child_mode(root,mode):
 s.console=lambda *args,**kwargs:None
 def ready():print('READY',flush=True)
 if mode in {'result','request'}:
  state,success=setup(root,1);job=state.claim()
  if mode=='result':write_valid(root,job.username)
  ready();await asyncio.sleep(1000)
 elif mode=='bootstrap':
  state,success=setup(root,0);p=profile('seed','2','0')
  async def hang():ready();await asyncio.sleep(1000)
  c=AsyncFakeClient(p,{'followers':[page([member('one','1')],True,'opaque+/=='),hang]})
  await s.bootstrap_dataset(c,'seed',root=root,state=state,success=success,stop_event=asyncio.Event())
 else:
  import httpx
  state,success=setup(root,4);state.recover(success);Original=s.WorkerApiClient
  async def watch():
   while state.summary()['completed_overall']<3:await asyncio.sleep(.01)
   ready()
  asyncio.create_task(watch())
  def factory(gate,pool,**kwargs):
   def client_factory(route,options):
    async def handler(req):
     name=req.url.params.get('username')
     if name=='user3':await asyncio.sleep(1000)
     await asyncio.sleep(.01)
     if name:
      d=copy.deepcopy(PROFILE);d['data'].update(username=name,userid=str(1000+int(name.removeprefix('user'))),followers='0',following='0');return httpx.Response(200,json=d)
     return httpx.Response(200,json=page())
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))
   return Original(gate,pool,client_factory=client_factory,**kwargs)
  with patch.object(s,'WorkerApiClient',side_effect=factory):await s.run_scan([],output_directory=root,worker_count=4,pacing=(0,0),success_recorder=success,state_recorder=state,use_resume=True)

if __name__=='__main__':
 if len(sys.argv)>1 and sys.argv[1]=='child':
  if sys.argv[3]=='sigterm':sys.exit(child_sigterm(Path(sys.argv[2])))
  asyncio.run(child_mode(Path(sys.argv[2]),sys.argv[3]))
 else:unittest.main(verbosity=2)
