import asyncio, base64, copy, io, json, logging, os, ssl, tempfile, threading, time, unittest
from contextlib import redirect_stdout, asynccontextmanager
from pathlib import Path
from unittest.mock import patch
import httpx
import tiktok_worker_scanner as s
PROFILE=json.loads((Path(__file__).parent/'fixtures'/'sample_profile.json').read_text(encoding="utf-8"))
FOLLOWERS=json.loads((Path(__file__).parent/'fixtures'/'sample_followers.json').read_text(encoding="utf-8"))
FOLLOWING=json.loads((Path(__file__).parent/'fixtures'/'sample_following.json').read_text(encoding="utf-8"))

def profile(name='example',followers='0',following='0'):
 d=copy.deepcopy(PROFILE);d['data'].update(username=name,followers=followers,following=following)
 return s.parse_profile_response(d,name)

def member(name,uid='1'):
 d=copy.deepcopy(FOLLOWING['users'][0]);d.update(uniqueId=name,user_id=uid)
 return d

def page(records=(),more=False,cursor=''):
 return {'users':list(records),'hasMore':more,'minCursor':cursor}



@asynccontextmanager
async def apatch(*args,**kwargs):
 with patch.object(*args,**kwargs) as value:yield value

class AsyncFakeClient:
 def __init__(self,p,lists):self.p=p;self.lists={k:iter(v) for k,v in lists.items()};self.calls=[]
 async def lookup_profile(self,name):return self.p
 async def fetch_page(self,p,name,cursor,number):
  self.calls.append((name,cursor,number));v=next(self.lists[name])
  if callable(v):v=await v()
  if isinstance(v,Exception):raise v
  return s.parse_list_page(v,name)

class AsyncTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.logs=[]
  self.logpatch=patch.object(s,'console',side_effect=lambda msg='',**kwargs:self.logs.append(str(msg)));self.logpatch.start()
 async def asyncTearDown(self):self.logpatch.stop();self.temp.cleanup()
 def configs(self,n=2):return [s.ProxyConfig(f'proxy{i}.example',8000+i,f'private-user-{i}',f'private-pass-{i}') for i in range(n)]
 def setup_http(self,handler,configs=None,spacing=0,ceiling=4,initial=None,proxy_only=False):
  pool=s.ProxyPool(self.configs() if configs is None else configs,proxy_only=proxy_only)
  gate=s.AsyncRequestGate(spacing,spacing,asyncio.Event(),ceiling=ceiling,initial=initial or ceiling,adaptive=False)
  options=[]
  def factory(route,opts):
   options.append((route,opts))
   async def dispatch(req):return await handler(route,req)
   return httpx.AsyncClient(transport=httpx.MockTransport(dispatch),headers=opts['headers'],trust_env=False,follow_redirects=False,event_hooks=opts.get('event_hooks',{}))
  return s.WorkerApiClient(gate,pool,client_factory=factory),options
 def recorder(self):return s.SuccessRecorder(self.root/'sucess_find.json',target_user=s.TARGET_USER,input_path=self.root/'input.json',started_at_utc=s.utc_iso())
 def state(self,n):return s.ScanStateRecorder(self.root/'scan_state.json',input_path=self.root/'input.json',target_user=s.TARGET_USER,total_profiles=n,worker_count=4,input_stats={},started_at_utc=s.utc_iso())
 async def run_list(self,pages,p=None,name='followers'):
  p=p or profile();c=AsyncFakeClient(p,{name:pages});found=[]
  async def callback(name,row):found.append((name,row))
  with s.UserStore(self.root/f'{time.monotonic_ns()}.sqlite') as store:
   r=await s.export_one_list(c,store,p,name,target_username=s.TARGET_USER,on_target_match=callback,stop_event=asyncio.Event());rows=list(store.iter_members(name))
  return r,rows,found,c
 async def test_multiple_proxies_overlap_with_aggregate_pacing(self):
  starts=[];routes=[];active=0;peak=0
  async def handler(route,request):
   nonlocal active,peak
   starts.append(time.perf_counter());routes.append(route.label);active+=1;peak=max(peak,active)
   await asyncio.sleep(.08);active-=1;return httpx.Response(200,json=PROFILE)
  c,_=self.setup_http(handler,spacing=.02,ceiling=3)
  async with c:await asyncio.gather(*(c.request_json('profile',{'username':'roblox'}) for _ in range(8)))
  self.assertGreater(peak,1);self.assertLessEqual(peak,3);self.assertEqual(set(routes),{'proxy-001','proxy-002'})
  self.assertTrue(all(b-a>=.018 for a,b in zip(starts,starts[1:])),starts)
 async def test_pacing_rechecks_deadline_after_early_timer_wakeup(self):
  now=[100.0];waits=[]
  gate=s.AsyncRequestGate(.02,.02,asyncio.Event(),ceiling=2,initial=2,adaptive=False)
  async def early_wait(seconds):
   waits.append(seconds)
   now[0]+=max(.001,seconds/2)
  with patch.object(s.time,'perf_counter',side_effect=lambda:now[0]),patch.object(gate,'wait',side_effect=early_wait):
   await gate.pace();first=now[0]
   await gate.pace();second=now[0]
   self.assertGreaterEqual(second-first,.02)
   self.assertGreater(len(waits),1)
   gate.cooldown(.03)
   await gate.pace()
   self.assertGreaterEqual(now[0]-second,.03)
 async def test_one_request_concurrency(self):
  active=peak=0
  async def handler(route,request):
   nonlocal active,peak
   active+=1;peak=max(peak,active);await asyncio.sleep(.01);active-=1;return httpx.Response(200,json=PROFILE)
  c,_=self.setup_http(handler,ceiling=1)
  async with c:await asyncio.gather(*(c.request_json('profile',{'username':'roblox'}) for _ in range(3)))
  self.assertEqual(peak,1)
 async def test_delayed_client_preparation_cannot_bunch_request_starts(self):
  starts=[];prepared=0;ready=asyncio.Event()
  async def handler(route,request):
   starts.append(time.perf_counter())
   return httpx.Response(200,json=PROFILE)
  c,_=self.setup_http(handler,spacing=.03,ceiling=4)
  original=c.borrow_client
  async def delayed(route):
   nonlocal prepared
   client=await original(route);prepared+=1
   if prepared==4:ready.set()
   await ready.wait()
   return client
  c.borrow_client=delayed
  async with c:
   await asyncio.wait_for(asyncio.gather(*(c.request_json('profile',{'username':'roblox'}) for _ in range(4))),5)
  self.assertEqual(len(starts),4)
  self.assertTrue(all(b-a>=.028 for a,b in zip(starts,starts[1:])),starts)
 async def test_auth_is_proxy_only_and_clients_reused(self):
  requests=[]
  async def handler(route,request):requests.append(request);return httpx.Response(200,json=PROFILE if request.url.path == "/" else page())
  c,options=self.setup_http(handler,configs=self.configs(1))
  async with c:
   await c.request_json('profile',{'username':'roblox'});await c.request_json('following',{'Uid':'123','minCursor':'a+/=='})
  self.assertEqual(len(options),1);opts=options[0][1];self.assertFalse(opts['trust_env']);self.assertFalse(opts['follow_redirects'])
  self.assertEqual(opts['proxy'].auth,('private-user-0','private-pass-0'))
  self.assertTrue(all('authorization' not in r.headers and 'proxy-authorization' not in r.headers for r in requests));self.assertEqual(requests[1].content,b'');self.assertIn('minCursor=a%2B%2F%3D%3D',str(requests[1].url))
 async def test_bad_auth_fails_over_without_leaking(self):
  routes=[]
  async def handler(route,request):
   routes.append(route.label if route else 'direct')
   if route and route.label=='proxy-001':raise httpx.ProxyError('407 http://private-user-0:private-pass-0@proxy0.example:8000')
   return httpx.Response(200,json=PROFILE)
  c,_=self.setup_http(handler)
  async with c,apatch(c.gate,'wait',return_value=None):await c.request_json('profile',{'username':'roblox'})
  self.assertEqual(routes,['proxy-001','proxy-002']);self.assertTrue(c.pool.states[0].disabled)
  output=json.dumps(c.pool.summary())+'\n'.join(self.logs);self.assertNotIn('private-pass-0',output);self.assertNotIn('private-user-0',output)
 async def test_dead_proxy_direct_fallback(self):
  routes=[]
  async def handler(route,req):
   routes.append(route.label if route else 'direct')
   if route:raise httpx.ConnectError('secret exception')
   return httpx.Response(200,json=PROFILE)
  c,_=self.setup_http(handler,configs=self.configs(1))
  async with c,apatch(c.gate,'wait',return_value=None):await c.request_json('profile',{'username':'roblox'})
  self.assertEqual(routes,['proxy-001','direct']);self.assertFalse(c.pool.states[0].disabled);self.assertGreater(c.pool.states[0].cooldown_until,time.monotonic())
 async def test_connect_timeout_rotates_but_response_timeout_does_not(self):
  for exception,expected in [(httpx.ConnectTimeout('secret'),['proxy-001','proxy-002']),(httpx.ReadTimeout('secret'),['proxy-001','proxy-001'])]:
   routes=[]
   async def handler(route,req):
    routes.append(route.label)
    if len(routes)==1:raise exception
    return httpx.Response(200,json=PROFILE)
   c,_=self.setup_http(handler)
   async with c,apatch(c.gate,'wait',return_value=None):await c.request_json('profile',{'username':'roblox'})
   self.assertEqual(routes,expected)
 async def test_proxy_only_never_falls_back(self):
  async def handler(route,req):self.assertIsNotNone(route);raise httpx.ConnectError('private')
  c,_=self.setup_http(handler,configs=self.configs(1),proxy_only=True)
  async with c,apatch(c.gate,'wait',return_value=None):
   with self.assertRaises(s.WorkerApiError) as caught:await c.request_json('profile',{'username':'roblox'})
  self.assertEqual(caught.exception.kind,'no_usable_proxy')
 async def test_no_proxy_file_direct(self):
  async def handler(route,req):self.assertIsNone(route);return httpx.Response(200,json=PROFILE)
  c,_=self.setup_http(handler,configs=[])
  async with c:await c.request_json('profile',{'username':'roblox'})
  self.assertTrue(any('direct' in text for text in self.logs))
 async def test_worker403_blocks_all_without_proxy_penalty(self):
  routes=[]
  async def handler(route,req):routes.append(route.label);return httpx.Response(403)
  c,_=self.setup_http(handler)
  async with c:
   for _ in range(2):
    with self.assertRaises(s.WorkerApiError) as caught:await c.request_json('profile',{'username':'roblox'})
    self.assertEqual(caught.exception.kind,'access_denied')
  self.assertEqual(routes,['proxy-001']);self.assertEqual(c.pool.states[0].failure_count,0)
 async def test_worker_error200_access_denied_blocks_all(self):
  calls=[]
  async def handler(route,req):calls.append(route);return httpx.Response(200,json={'status':'error','message':'Unauthorized: Access restricted.'})
  c,_=self.setup_http(handler)
  async with c:
   with self.assertRaises(s.WorkerApiError):await c.request_json('profile',{'username':'roblox'})
   with self.assertRaises(s.WorkerApiError):await c.request_json('profile',{'username':'other'})
  self.assertEqual(len(calls),1);self.assertEqual(sum(p.failure_count for p in c.pool.states),0)
 async def test_429_stops_all_routes_without_retry(self):
  routes=[];starts=[];limited=asyncio.Event()
  async def handler(route,req):
   routes.append(route.label);starts.append(time.monotonic())
   if len(routes)==1:limited.set();return httpx.Response(429,headers={'Retry-After':'0.06'})
   return httpx.Response(200,json=PROFILE)
  c,_=self.setup_http(handler)
  original=c.gate.penalize
  with patch.object(s.random,'uniform',return_value=0),patch.object(c.gate,'penalize',side_effect=lambda seconds:original(.06)):
   async with c,apatch(c.gate,'wait',wraps=c.gate.wait) as wait:
    task=asyncio.create_task(c.request_json('profile',{'username':'roblox'}))
    await limited.wait();other=asyncio.create_task(c.request_json('profile',{'username':'other'}))
    results=await asyncio.gather(task,other,return_exceptions=True)
    self.assertTrue(all(isinstance(r,s.WorkerApiError) and r.kind=='rate_limited' for r in results))
    wait.assert_not_awaited()
  self.assertEqual(routes,['proxy-001']);self.assertTrue(c.gate.rate_limit_event.is_set());self.assertEqual(sum(p.failure_count for p in c.pool.states),0)
 async def test_429_retry_after_does_not_restart_stopped_scan(self):
  async def handler(route,req):return httpx.Response(429,headers={'Retry-After':'120'})
  c,_=self.setup_http(handler)
  async with c,apatch(c.gate,'penalize') as penalize,apatch(c.gate,'wait',return_value=None):
   with self.assertRaises(s.WorkerApiError):await c.request_json('profile',{'username':'roblox'})
  penalize.assert_not_called();self.assertTrue(c.gate.rate_limit_exhausted)
 async def test_retry_5xx_all_required_codes(self):
  for code in (500,502,503,504,520,522,524):
   routes=[]
   async def handler(route,req):routes.append(route.label);return httpx.Response(code) if len(routes)==1 else httpx.Response(200,json=PROFILE)
   c,_=self.setup_http(handler)
   async with c,apatch(c.gate,'wait',return_value=None),apatch(c.gate,'cooldown'):
    await c.request_json('profile',{'username':'roblox'})
   self.assertEqual(routes,['proxy-001','proxy-001'])
 async def test_proxy403_no_direct_fallback(self):
  calls=[]
  async def handler(route,req):calls.append(route);raise httpx.ProxyError('403 client_connect_forbidden_host')
  c,_=self.setup_http(handler)
  async with c:
   with self.assertRaises(s.WorkerApiError):await c.request_json('profile',{'username':'roblox'})
  self.assertEqual(len(calls),1);self.assertTrue(c.gate.blocked_reason)
 async def test_invalid_json_not_proxy_failure(self):
  async def handler(route,req):return httpx.Response(200,content=b'<html>bad</html>')
  c,_=self.setup_http(handler)
  async with c:
   with self.assertRaises(s.InvalidWorkerResponse):await c.request_json('profile',{'username':'roblox'})
  self.assertEqual(c.pool.states[0].failure_count,0)
 async def test_unknown_profile_is_not_private(self):
  async def handler(route,req):return httpx.Response(200,json={'status':'error','message':'User not found'})
  c,_=self.setup_http(handler)
  async with c:
   with self.assertRaises(s.WorkerApiError) as caught:await c.lookup_profile('missing')
  self.assertEqual(caught.exception.kind,'not_found')
 async def test_private_profile_skips_lists(self):
  data=copy.deepcopy(PROFILE);data['data']['private']='Private Account';calls=[]
  async def handler(route,req):calls.append(str(req.url));return httpx.Response(200,json=data)
  c,_=self.setup_http(handler)
  async with c:
   with self.assertRaises(s.PublicProfileRequired):await c.lookup_profile('roblox')
  self.assertEqual(len(calls),1)
 async def test_redaction_raw_payload_and_diagnostics(self):
  secret=self.configs(1)[0]
  async def handler(route,req):return httpx.Response(200,json={'users':[],'hasMore':False,'minCursor':'','debug':secret.password})
  c,_=self.setup_http(handler,configs=[secret]);c.raw_directory=self.root/'raw'
  async with c:
   data=await c.request_json('followers',{'Uid':'1','minCursor':'0'});await c.save_raw('example','followers_000001',data)
  text=next((self.root/'raw').rglob('*.json')).read_text(encoding="utf-8");self.assertNotIn(secret.password,text);self.assertIn('[REDACTED]',text)
 async def test_list_one_page_and_empty(self):
  for name in ('followers','following'):
   result,rows,_,_=await self.run_list([page()],name=name);self.assertTrue(result.complete);self.assertEqual(rows,[])
 async def test_sequential_pages_target_later_and_dedupe(self):
  r,rows,found,c=await self.run_list([page([member('a')],True,'opaque+/='),page([member('a'),member(s.TARGET_USER.upper(),'2')],True,'next'),page([member('b','3')])])
  self.assertTrue(r.complete);self.assertEqual(len(rows),3);self.assertEqual(r.duplicates_ignored,1);self.assertEqual(len(found),1);self.assertEqual([call[1] for call in c.calls],['0','opaque+/=','next'])
 async def test_loop_missing_empty_stall(self):
  cases=[([page([member(s.TARGET_USER)],True,'0')],'cursor_stalled'),([page([member('a')],True,'a'),page([member('b','2')],True,'b'),page([member('c','3')],True,'a')],'cursor_repeated'),([page([member(s.TARGET_USER)],True)],'missing_cursor'),([page([],True,'a')],'empty_page_with_has_more')]
  for pages,reason in cases:
   r,*_=await self.run_list(pages);self.assertFalse(r.complete);self.assertEqual(r.stop_reason,reason)
 async def test_duplicate_only_stall(self):
  r,*_=await self.run_list([page([member('a')],True,str(i)) for i in range(1,5)]);self.assertEqual(r.stop_reason,'duplicate_page_stall')
 async def test_target_preserved_on_later_failure(self):
  r,rows,found,_=await self.run_list([page([member(s.TARGET_USER)],True,'a'),s.WorkerApiError('failed')]);self.assertFalse(r.complete);self.assertEqual(len(found),1);self.assertEqual(len(rows),1)
 async def test_resume_no_network_and_original_schema(self):
  p=profile();job=s.ProfileJob('example',p.profile_url,('profiles',));rec=self.recorder()
  c=AsyncFakeClient(p,{'followers':[page([member(s.TARGET_USER)])],'following':[page([member(s.TARGET_USER)])]})
  r=await s.process_profile(c,job,output_directory=self.root,success_recorder=rec,stop_event=asyncio.Event(),use_resume=True)
  self.assertTrue(r.complete);doc=json.loads(Path(r.export_path).read_text(encoding="utf-8"));self.assertEqual(doc['schema_version'],4);before=Path(r.export_path).read_bytes()
  with patch.object(c,'lookup_profile',side_effect=AssertionError('resume used network')):
   resumed=await s.process_profile(c,job,output_directory=self.root,success_recorder=rec,stop_event=asyncio.Event(),use_resume=True)
  self.assertEqual(resumed.status,'resumed_complete');self.assertEqual(before,Path(r.export_path).read_bytes());self.assertTrue(json.loads(rec.path.read_text(encoding="utf-8"))['profiles'][0]['relationship']['mutual'])
 async def test_partial_restarts_and_preserves_success(self):
  p=profile();job=s.ProfileJob('example',p.profile_url,('profiles',));rec=self.recorder()
  c=AsyncFakeClient(p,{'followers':[page([member(s.TARGET_USER)],True,'a'),s.WorkerApiError('failed')],'following':[page()]})
  first=await s.process_profile(c,job,output_directory=self.root,success_recorder=rec,stop_event=asyncio.Event(),use_resume=True);self.assertFalse(first.complete)
  rec=self.recorder();self.assertEqual(rec.found_lists_for('example'),['followers'])
  c2=AsyncFakeClient(p,{'followers':[page([member(s.TARGET_USER)])],'following':[page()]})
  second=await s.process_profile(c2,job,output_directory=self.root,success_recorder=rec,stop_event=asyncio.Event(),use_resume=True);self.assertTrue(second.complete);self.assertEqual(c2.calls[0][1],'0')
 async def test_cancel_mid_list_writes_partial(self):
  p=profile();job=s.ProfileJob('example',p.profile_url,('profiles',));rec=self.recorder();reached=asyncio.Event()
  async def wait_forever():reached.set();await asyncio.Future()
  c=AsyncFakeClient(p,{'followers':[page([member(s.TARGET_USER)],True,'a'),wait_forever],'following':[]})
  task=asyncio.create_task(s.process_profile(c,job,output_directory=self.root,success_recorder=rec,stop_event=asyncio.Event(),use_resume=True));await reached.wait();task.cancel();result=await task
  self.assertEqual(result.status,'cancelled');self.assertEqual(len(json.loads(Path(result.export_path).read_text(encoding="utf-8"))['followers']),1);self.assertEqual(self.recorder().found_lists_for('example'),['followers'])
 async def test_concurrent_shared_files_and_restart(self):
  rec=self.recorder();state=self.state(12);jobs=[s.ProfileJob(f'user{i}',f'url{i}',('profiles',)) for i in range(12)]
  class Client(AsyncFakeClient):
   def __init__(self,gate,pool,**kwargs):self.gate=gate
   async def __aenter__(self):return self
   async def __aexit__(self,*args):return None
   async def lookup_profile(self,name):await asyncio.sleep(.002);return profile(name)
   async def fetch_page(self,p,name,cursor,number):await asyncio.sleep(.002);return s.parse_list_page(page([member(s.TARGET_USER)]),name)
  with patch.object(s,'WorkerApiClient',Client):
   _,snapshot,_=await s.run_scan(jobs,output_directory=self.root,worker_count=4,pacing=(0,0),success_recorder=rec,state_recorder=state,use_resume=True)
  self.assertTrue(snapshot['scan_complete']);self.assertEqual(snapshot['summary']['processed_profiles'],12)
  for file in self.root.glob('*.json'):json.loads(file.read_text(encoding="utf-8"))
  self.assertEqual(len(json.loads(rec.path.read_text(encoding="utf-8"))['profiles']),12)
  state2=self.state(12);rec2=self.recorder()
  with patch.object(s,'WorkerApiClient',Client),patch.object(Client,'lookup_profile',side_effect=AssertionError('network on restart')):
   _,snapshot,_=await s.run_scan(jobs,output_directory=self.root,worker_count=4,pacing=(0,0),success_recorder=rec2,state_recorder=state2,use_resume=True)
  self.assertEqual(snapshot['summary']['statuses'],{'resumed_complete':12})
 async def test_cancel_orchestrator_waits_for_partial_writes(self):
  rec=self.recorder();state=self.state(1);reached=asyncio.Event();closed=[]
  class Client:
   def __init__(self,gate,pool,**kwargs):self.gate=gate
   async def __aenter__(self):return self
   async def __aexit__(self,*args):closed.append(True)
   async def lookup_profile(self,name):return profile(name)
   async def fetch_page(self,p,name,cursor,number):
    if number==1:return s.parse_list_page(page([member(s.TARGET_USER)],True,'a'),name)
    reached.set();await asyncio.Future()
  with patch.object(s,'WorkerApiClient',Client):
   task=asyncio.create_task(s.run_scan([s.ProfileJob('example','url',('profiles',))],output_directory=self.root,worker_count=1,pacing=(0,0),success_recorder=rec,state_recorder=state,use_resume=True));await reached.wait();task.cancel()
   with self.assertRaises(asyncio.CancelledError):await task
  self.assertTrue(closed);checkpoint=json.loads(state.path.read_text(encoding="utf-8"));self.assertEqual(checkpoint['profiles'][0]['status'],'cancelled');self.assertEqual(len(json.loads((self.root/'example_followers_and_following.json').read_text(encoding="utf-8"))['followers']),1)

class ConfigTests(unittest.TestCase):
 def test_colon_password_and_exact_auth(self):
  p=s.parse_proxy_line('1.2.3.4:8000:private-user:pass:word');self.assertEqual((p.host,p.port,p.username,p.password),('1.2.3.4',8000,'private-user','pass:word'));self.assertNotIn('pass:word',repr(p));self.assertEqual(p.scheme,'http')
 def test_urls_reserved_chars_and_socks(self):
  for scheme in ('http','https','socks5','socks5h'):
   p=s.parse_proxy_line(f'{scheme}://a%40b:c%3Ad%40e@p.webshare.io:80/');self.assertEqual(p.username,'a@b');self.assertEqual(p.password,'c:d@e');self.assertEqual(p.mode,'webshare_gateway');self.assertEqual(p.scheme,scheme)
 def test_ipv6_and_ip_authorization(self):
  p=s.parse_proxy_line('[::1]:8000:private-user:pass');self.assertEqual(p.endpoint,'http://[::1]:8000');self.assertIsNone(s.parse_proxy_line('127.0.0.1:8000').username)
 def test_invalid_entries_never_echo_secrets(self):
  for line in ('host:bad:secretuser:secretpass','host:8000:secretuser','ftp://secretuser:secretpass@host:21','http://secretuser:secretpass@host:0','http://secretuser:secretpass@host:80/path'):
   with self.assertRaises(s.ExporterError) as caught:s.parse_proxy_line(line)
   self.assertNotIn('secret',str(caught.exception))
 def test_missing_empty_invalid_proxy_files(self):
  with tempfile.TemporaryDirectory() as temp,patch.object(s,'console'):
   p=Path(temp)/'proxy.txt';self.assertEqual(s.load_proxy_configs(p),[]);p.write_text('', encoding="utf-8");self.assertEqual(s.load_proxy_configs(p),[]);p.write_text('bad\n# comment\n', encoding="utf-8");self.assertEqual(s.load_proxy_configs(p),[])
 def test_40_webshare_entries_parsed_without_exposure(self):
  messages=[]
  with tempfile.TemporaryDirectory() as d,patch.object(s,'console',side_effect=lambda msg,**kwargs:messages.append(msg)):
   path=Path(d)/'webshare_proxy.txt'
   path.write_text('\n'.join(f'192.0.2.{i}:8000:synthetic-login:synthetic-password' for i in range(1,41)), encoding="utf-8")
   proxies=s.load_proxy_configs(path)
  self.assertEqual(len(proxies),40);self.assertTrue(all(p.username and p.password and p.mode=='direct_endpoint' for p in proxies))
  self.assertNotIn('synthetic-login','\n'.join(messages));self.assertNotIn('synthetic-password','\n'.join(messages))
 def test_default_and_explicit_workers(self):
  config=s.default_scan_config();self.assertEqual(config['workers'],s.DEFAULT_WORKERS)
  config['workers']=6;s.validate_scan_config(config);self.assertEqual(config['workers'],6)
 def test_auto_grows_for_latency_and_shrinks_for_rate_limits(self):
  pool=s.ProxyPool([s.ProxyConfig(f'p{i}.example',80) for i in range(40)]);gate=s.AsyncRequestGate(1,2.5,asyncio.Event(),ceiling=32,initial=4,adaptive=True)
  with patch.object(s,'console'):
   for _ in range(19):gate.observe(15,pool)
   self.assertEqual(gate.limit,4)
   gate.last_adjusted-=6;gate.observe(15,pool)
   self.assertEqual(gate.limit,8);gate.penalize(10);self.assertEqual(gate.limit,4)
   for _ in range(8):gate.observe(15,pool)
   self.assertEqual(gate.limit,4)
 def test_proxy_cooldown_recovery(self):
  pool=s.ProxyPool([s.ProxyConfig('p.example',80)])
  with patch.object(s,'console'):pool.failed(pool.states[0],'proxy_timeout')
  self.assertEqual(pool.available(),[]);pool.states[0].cooldown_until=time.monotonic()-1;self.assertEqual(len(pool.available()),1)
 def test_credential_redaction_encoded_and_basic(self):
  p=s.ProxyConfig('p.example',80,'private@user','private:pass');r=s.CredentialRedactor([p]);token=base64.b64encode(b'private@user:private:pass').decode();result=r.clean({'debug':f'private@user private:pass private%3Apass {token}'})
  self.assertNotIn('private',json.dumps(result));self.assertNotIn(token,json.dumps(result))
 def test_exact_schemas(self):
  p=s.parse_profile_response(PROFILE,'roblox');self.assertEqual(p.uid,PROFILE['data']['userid']);self.assertFalse(p.private_account)
  for data in (FOLLOWERS,FOLLOWING):
   batch=s.parse_list_page(data,'followers');self.assertEqual(batch.min_cursor,data['minCursor'])
   for raw in data['users']:
    key,m=s.normalize_member(raw);self.assertEqual(key,'id:'+raw['user_id']);self.assertEqual(m['verified'],raw['verified']);self.assertIs(m['private_account'],raw['privateAccount']);self.assertEqual(m['username'],raw['uniqueId'])
 def test_missing_pagination_not_empty_success(self):
  with self.assertRaises(s.InvalidWorkerResponse):s.parse_list_page({'users':[]},'followers')
 def test_relationship_mapping(self):
  self.assertEqual(tuple(s.relationship_from_found_lists(['followers']).values()),(True,None,None));self.assertEqual(tuple(s.relationship_from_found_lists(['following']).values()),(None,True,None));self.assertTrue(s.relationship_from_found_lists(['followers','following'])['mutual'])

if __name__=='__main__':unittest.main(verbosity=2)
