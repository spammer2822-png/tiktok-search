import asyncio, ast, copy, io, json, os, tempfile, unittest
from pathlib import Path
from contextlib import redirect_stdout
from unittest.mock import patch
import httpx
import tiktok_worker_scanner as s
from test_async_scanner import PROFILE, member, page, AsyncFakeClient


def response_profile(name,uid,followers='0',following='0',private=False):
 value=copy.deepcopy(PROFILE);value['data'].update(username=name,userid=str(uid),followers=followers,following=following,private='Private Account' if private else 'Public Account');return value

def public_member(name,uid,private=False):
 value=member(name,str(uid));value['privateAccount']=private;return value

def job(name,uid='',phase=1):return s.ProfileJob(name,'https://www.tiktok.com/@'+name,('profiles',),uid,phase)

class ConfigurationTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.base=self.root/'logs';self.input=self.root/'input.json';self.input.write_text('["a"]', encoding="utf-8");self.calls=[];self.options=[]
 def tearDown(self):self.tmp.cleanup()
 def run_main(self,answers,handler,env_extra=None):
  original=s.WorkerApiClient;output=io.StringIO();prompts=[];items=iter(answers)
  log_offsets={p:p.stat().st_size for p in self.base.glob('*/run.log')}
  def input_value(prompt):
   prompts.append(prompt)
   try:return next(items)
   except StopIteration:raise AssertionError('Unexpected prompt: '+prompt)
  def factory(gate,pool,**kwargs):
   self.options.append((gate.ceiling,gate.minimum,gate.maximum,copy.deepcopy(kwargs.get('settings'))))
   def cf(route,options):
    async def respond(request):self.calls.append(str(request.url));return await handler(request)
    return httpx.AsyncClient(transport=httpx.MockTransport(respond))
   return original(gate,pool,client_factory=cf,**kwargs)
  env={'TIKTOK_INPUT_JSON':str(self.input),'TIKTOK_EXPORT_DIR':str(self.base)};env.update(env_extra or {})
  with patch.dict(os.environ,env),patch('builtins.input',side_effect=input_value),patch.object(s,'WorkerApiClient',side_effect=factory),redirect_stdout(output):code=s.main()
  # Detailed progress is intentionally throttled on stdout in the new runtime.
  # Existing log-message assertions inspect the persisted log too.
  log_text='\n'.join(p.read_bytes()[log_offsets.get(p,0):].decode('utf-8') for p in self.base.glob('*/run.log'))
  return code,output.getvalue()+'\n'+log_text,prompts
 async def empty(self,req):
  name=req.url.params.get('username')
  return httpx.Response(200,json=response_profile(name,{'a':1,'b':2,'seed':9}.get(name,99))) if name else httpx.Response(200,json=page())
 def new(self,mode='1',target='wanted',workers='4',limit='0'):
  return ['0','1',target,limit,'0',mode,workers,'0','0','n','1']
 def create_saved(self,names=('a',),mode='normal',phase=1):
  root=s.create_search_directory(self.base,'wanted');cfg=s.default_scan_config();cfg.update(target_username='wanted',input_source='json',source_json_path=str(self.input),workers=10,scan_mode=mode);cfg['request_delay']={'minimum_seconds':0.,'maximum_seconds':0.}
  meta={'identity':s.source_identity('json',str(self.input),'wanted'),'input_imported':True,'scan_complete':False,'session_id':root.name,'layout_version':3,'current_phase':phase}
  s.atomic_write_json(root/'starting_dataset.json',list(names));s.save_scan_config(root,cfg)
  state=s.DurableScanState(root,meta,root,cfg);state.add_jobs([job(name) for name in names]);state.snapshot();state.close();return root,cfg,meta
 def test_new_startup_order_snapshot_and_custom_target(self):
  code,out,prompts=self.run_main(self.new(workers='10'),self.empty);self.assertEqual(code,0)
  root=self.base/'wanted';cfg=json.loads((root/'scan_config.json').read_text(encoding="utf-8"));self.assertEqual(cfg['workers'],10);self.assertEqual(cfg['request_delay'],{'minimum_seconds':0,'maximum_seconds':0});self.assertEqual(self.options[-1][:3],(10,0.,0.));self.assertEqual(json.loads((root/'sucess_find.json').read_text(encoding="utf-8"))['target_user'],'wanted')
  self.assertEqual(json.loads((root/'starting_dataset.json').read_text(encoding="utf-8")),['a']);self.assertEqual(len(self.calls),3)
  positions=[next(i for i,x in enumerate(prompts) if token in x) for token in ['Select starting','Target TikTok','Maximum followers','Maximum following','Scan mode','Number of workers','Minimum request','Maximum request']];self.assertEqual(positions,sorted(positions))
 def test_saved_config_beats_environment_and_python_defaults(self):
  root,cfg,_=self.create_saved();self.input.unlink()
  with patch.object(s,'DEFAULT_WORKERS',1),patch.dict(os.environ,{'TIKTOK_MIN_DELAY_SECONDS':'20'}):
   code,out,_=self.run_main(['1','1','1','n'],self.empty,{'TIKTOK_WORKERS':'2','TIKTOK_MAX_CONCURRENT_REQUESTS':'1','TIKTOK_MIN_DELAY_SECONDS':'99','TIKTOK_MAX_DELAY_SECONDS':'100','TIKTOK_INPUT_JSON':'absent'})
  self.assertEqual(code,0);self.assertEqual(self.options[-1][:3],(10,0.,0.));self.assertEqual(len(list(self.base.iterdir())),1);self.assertIn('SAVED SCAN CONFIGURATION',out);self.assertTrue((root/'scan_state.json').exists())
 def test_edit_keep_values_and_validation_reprompts(self):
  cfg=s.default_scan_config();cfg.update(follower_skip_limit=10000,following_skip_limit=5000,workers=10,scan_mode='double_phase')
  answers=['','', '1','0','oops','8','-1','nan','3','2','0','0','','','']
  with patch('builtins.input',side_effect=answers),redirect_stdout(io.StringIO()):edited=s.edit_scan_config(cfg)
  self.assertEqual(edited['follower_skip_limit'],10000);self.assertEqual(edited['following_skip_limit'],5000);self.assertEqual(edited['workers'],8);self.assertEqual(edited['request_delay'],{'minimum_seconds':0,'maximum_seconds':0});self.assertEqual(edited['scan_mode'],'double_phase')
 def test_resume_edit_saves_workers_delay_same_folder(self):
  root,cfg,_=self.create_saved()
  answers=['1','2','20000','8000','1','6','0','0','','','','1','n']
  code,out,_=self.run_main(answers,self.empty);self.assertEqual(code,0);saved=json.loads((root/'scan_config.json').read_text(encoding="utf-8"));self.assertEqual(saved['workers'],6);self.assertEqual(saved['follower_skip_limit'],20000);self.assertEqual(len(list(self.base.iterdir())),1);self.assertIn('UPDATED SCAN CONFIGURATION',out)
 def test_invalid_saved_config_no_fallback_or_new_folder(self):
  root,cfg,_=self.create_saved();cfg['workers']=0;s.atomic_write_json(root/'scan_config.json',cfg)
  code,out,_=self.run_main(['1'],self.empty);self.assertEqual(code,2);self.assertEqual(self.calls,[]);self.assertIn('Invalid saved workers',out);self.assertEqual(len(list(self.base.iterdir())),1)
 def test_resume_back_does_not_create_folder(self):
  self.create_saved();code,out,_=self.run_main(['1','0','q'],self.empty);self.assertEqual(code,0);self.assertEqual(len(list(self.base.iterdir())),1);self.assertEqual(self.calls,[])
 def test_size_limits_skip_pages_and_save_details(self):
  async def handle(req):
   self.assertEqual(req.url.path,'/');return httpx.Response(200,json=response_profile('a',1,'10.5K','821'))
  code,out,_=self.run_main(self.new(limit='10000'),handle);self.assertEqual(code,0);self.assertEqual(len(self.calls),1);self.assertIn('SKIPPED_SIZE',out)
  root=self.base/'wanted';item=json.loads((root/'skipped_accounts.json').read_text(encoding="utf-8"))[0];self.assertEqual(item['status'],'skipped_size');self.assertEqual(item['metadata']['user_id'],'1');self.assertEqual(item['metadata']['counts']['followers']['raw'],'10.5K');self.assertFalse(item['metadata']['counts']['followers']['exact'])
 def test_both_limits_exact_boundary_zero_and_unknown(self):
  for followers,following,fl,fg,expected in [('10','5',10,5,None),('11','6',10,5,'skipped_size'),('1M','999',0,0,None),('Unknown','0',1,0,'skipped_unknown_size')]:
   profile=s.parse_profile_response(response_profile('a',1,followers,following),'a')
   with redirect_stdout(io.StringIO()):result=s.size_skip_result(job('a'),profile,{'follower_skip_limit':fl,'following_skip_limit':fg},s.utc_iso())
   self.assertEqual(result.status if result else None,expected)
 def test_bootstrap_public_only_uid_dedup_metadata(self):
  async def handle(req):
   name=req.url.params.get('username')
   if name:return httpx.Response(200,json=response_profile(name,{'seed':9,'a':1,'b':2}.get(name,99),'3' if name=='seed' else '0','2' if name=='seed' else '0'))
   uid=req.url.params['Uid']
   if uid=='9':
    rows=[public_member('a',1),public_member('private_user',3,True),public_member('b',2)] if req.url.path.endswith('followers') else [public_member('A',1),public_member('b_alias',2)]
    return httpx.Response(200,json=page(rows))
   return httpx.Response(200,json=page())
  answers=['0','2','@Seed','wanted','0','0','1','4','0','0','n','1']
  code,out,_=self.run_main(answers,handle);self.assertEqual(code,0)
  dataset=json.loads((self.base/'wanted'/'starting_dataset.json').read_text(encoding="utf-8"));self.assertEqual({r['id'] for r in dataset},{'1','2'});self.assertEqual(len(dataset),2)
  self.assertTrue(all(r['private_account'] is False for r in dataset));self.assertTrue(all(r['discovered_through_both'] for r in dataset));self.assertTrue(all(r['follower_count'] is None and r['following_count'] is None for r in dataset));self.assertFalse(any('username=private_user' in c for c in self.calls))
 def test_private_seed_returns_to_source_selection(self):
  async def handle(req):return httpx.Response(200,json=response_profile('seed',9,private=True))
  answers=['0','2','seed','wanted','0','0','1','4','0','0','n','1','0','q']
  code,out,prompts=self.run_main(answers,handle);self.assertEqual(code,0);self.assertEqual(len(self.calls),1);self.assertIn('Returning to starting-source selection',out);self.assertEqual(sum('Select starting source' in p for p in prompts),2)
 def test_double_phase_exactly_one_expansion_custom_target(self):
  async def handle(req):
   name=req.url.params.get('username')
   if name:return httpx.Response(200,json=response_profile(name,{'a':1,'b':2,'wanted':99,'c':4}.get(name,999)))
   uid=req.url.params['Uid']
   if uid=='1':return httpx.Response(200,json=page([public_member('b',2),public_member('b_alias',2),public_member('a',1),public_member('private_user',3,True),public_member('wanted',99)]))
   if uid=='2':return httpx.Response(200,json=page([public_member('c',4)]))
   return httpx.Response(200,json=page())
  code,out,_=self.run_main(self.new(mode='2'),handle);self.assertEqual(code,0);lookups=[u for u in self.calls if '?username=' in u];self.assertEqual(len(lookups),3);self.assertFalse(any('username=c' in u or 'username=private_user' in u for u in lookups));self.assertIn('PHASE 1 COMPLETE',out)
  root=self.base/'wanted';queue=json.loads((root/'phase2_queue.json').read_text(encoding="utf-8"));self.assertEqual({q['username'] for q in queue},{'b','wanted'});self.assertTrue(all(q['status']=='completed' for q in queue));finds=json.loads((root/'sucess_find.json').read_text(encoding="utf-8"));entry=next(x for x in finds['profiles'] if x['username']=='a');self.assertEqual(set(entry['found_in']),{'followers','following'});self.assertTrue(entry['relationship']['mutual'])
  discoveries=json.loads((root/'discovered_profiles.json').read_text(encoding="utf-8"));self.assertFalse(any(r['username']=='c' for r in discoveries));b=next(r for r in discoveries if r['username']=='b');self.assertEqual(set(b['discovered_from']),{'@a/followers','@a/following'})
 def test_normal_mode_never_expands(self):
  async def handle(req):
   if req.url.params.get('username'):return httpx.Response(200,json=response_profile('a',1))
   return httpx.Response(200,json=page([public_member('b',2)]))
  code,_,_=self.run_main(self.new(),handle);self.assertEqual(code,0);self.assertEqual(len(self.calls),3);self.assertEqual(json.loads((self.base/'wanted'/'phase2_queue.json').read_text(encoding="utf-8")),[])
 def test_terminal_outcomes_preserved_with_changed_limits(self):
  root,cfg,meta=self.create_saved(('a','b','c','d','e','f'))
  state=s.DurableScanState(root,meta,root,cfg)
  for kind in ['partial','private','http_error','skipped_size','not_found']:
   item=state.claim();state.record(s.error_profile_result(item,status=kind,started_at_utc=s.utc_iso(),error='fixture'))
  state.close();cfg['follower_skip_limit']=20000;s.save_scan_config(root,cfg)
  async def handle(req):
   if req.url.params.get('username'):
    self.assertEqual(req.url.params['username'],'f');return httpx.Response(200,json=response_profile('f',6))
   return httpx.Response(200,json=page())
  code,_,_=self.run_main(['1','1','1','n'],handle);self.assertEqual(code,0);self.assertEqual(len(self.calls),3);summary=json.loads((root/'scan_state.json').read_text(encoding="utf-8"))['summary'];self.assertEqual(summary['processed_profiles'],6);self.assertEqual(summary['statuses']['skipped_size'],1)
 def test_resume_phase_two_does_not_repeat_phase_one(self):
  root,cfg,meta=self.create_saved(('a',),'double_phase');state=s.DurableScanState(root,meta,root,cfg);item=state.claim();state.record(s.error_profile_result(item,status='partial',started_at_utc=s.utc_iso(),error='fixture'));state.discover('a','followers',[{'username':'b','id':'2','private_account':False}]);state.advance_phase();self.assertEqual(state.claim().username,'b');state.close()
  async def handle(req):
   if req.url.params.get('username'):
    self.assertEqual(req.url.params['username'],'b');return httpx.Response(200,json=response_profile('b',2))
   return httpx.Response(200,json=page())
  code,_,_=self.run_main(['1','1','1','n'],handle);self.assertEqual(code,0);self.assertEqual(len(self.calls),3);self.assertEqual(json.loads((root/'scan_state.json').read_text(encoding="utf-8"))['current_phase'],2)
 def test_uid_register_prevents_concurrent_alias_page_scans(self):
  self.input.write_text('["a","a_alias"]', encoding="utf-8")
  async def handle(req):
   name=req.url.params.get('username')
   if name:return httpx.Response(200,json=response_profile(name,1))
   return httpx.Response(200,json=page())
  code,_,_=self.run_main(self.new(),handle);self.assertEqual(code,0);self.assertEqual(len([c for c in self.calls if '/api/' in c]),2);summary=json.loads((self.base/'wanted'/'scan_state.json').read_text(encoding="utf-8"))['summary'];self.assertEqual(summary['statuses']['skipped_duplicate'],1)
 def test_config_validation_python311_and_saved_limits(self):
  ast.parse(Path(s.__file__).read_text(encoding="utf-8"),feature_version=(3,11));cfg=s.default_scan_config()
  for field,value in [('workers',True),('workers',0),('following_skip_limit',-1)]:
   changed=copy.deepcopy(cfg);changed[field]=value
   with self.assertRaises(s.ExporterError):s.validate_scan_config(changed)
  changed=copy.deepcopy(cfg);changed['request_delay']['minimum_seconds']=float('nan')
  with self.assertRaises(s.ExporterError):s.validate_scan_config(changed)

if __name__=='__main__':unittest.main(verbosity=2)
