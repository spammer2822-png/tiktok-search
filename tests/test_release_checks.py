import ast,asyncio,io,json,os,subprocess,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch,AsyncMock
from contextlib import redirect_stdout,ExitStack
import httpx
import tiktok_worker_scanner as s
import proxy_validation as pv
import report_generator as report
from test_configured_scanner import ConfigurationTests,response_profile
from test_async_scanner import page,profile,member,AsyncFakeClient

class FinalSetupTests(ConfigurationTests):
 def test_validation_is_last_step_pool_excludes_bad_proxy(self):
  proxies=self.root/'webshare.txt';proxies.write_text('192.0.2.1:80:fixture-name:fixture-password\n192.0.2.2:80:fixture-name:fixture-password')
  async def handler(req):
   if req.url.host=='ipv4.webshare.io':return httpx.Response(200,text='203.0.113.10')
   return await self.empty(req)
  answers=self.new();answers[-2]='y';answers.insert(-1,'1')
  code,out,prompts=self.run_main(answers,handler,{'TIKTOK_PROXY_FILE':str(proxies)})
  self.assertEqual(code,0);self.assertEqual(prompts[-2],'Proxy validation: ')
  doc=json.loads((self.base/'wanted'/'proxy_validation.json').read_text());self.assertEqual(doc['valid'],2)
  self.assertEqual(sum('ipv4.webshare.io' in u for u in self.calls),2)
  self.assertNotIn('fixture-password',out);self.assertNotIn('fixture-name',out)
 def test_404_twice_main_no_list_requests(self):
  async def handler(req):return httpx.Response(404)
  with patch.object(s.AsyncRequestGate,'wait',new=AsyncMock()):code,out,_=self.run_main(self.new(),handler)
  self.assertEqual(code,0);self.assertEqual(len(self.calls),2);self.assertTrue(all('/?username=a' in u for u in self.calls))
  doc=json.loads((self.base/'wanted'/'failures.json').read_text());self.assertEqual(doc[0]['status'],'http_error');self.assertEqual(doc[0]['metadata']['http_code'],404)
  self.assertIn('Profiles checked: 1 / 1',out);self.assertIn('Failed: 1',out)
 def test_profile_404_recovers_main_continues(self):
  async def handler(req):
   if len(self.calls)==1:return httpx.Response(404)
   return await self.empty(req)
  with patch.object(s.AsyncRequestGate,'wait',new=AsyncMock()):code,_,_=self.run_main(self.new(),handler)
  self.assertEqual(code,0);self.assertEqual(len(self.calls),4)
  result=json.loads((self.base/'wanted'/'results.json').read_text())[0];self.assertTrue(result['complete']);self.assertEqual(result['metadata']['profile_retries'],1)

class FinalChecks(unittest.TestCase):
 def test_no_usable_proxy_explicit_direct_retest_and_back(self):
  for choices,expected,checks in [(['1','1'],'direct',1),(['1','2','1'],'direct',2),(['1','0'],'back',1)]:
   with self.subTest(choices=choices),tempfile.TemporaryDirectory() as d,patch.object(s,'console'):
    root=Path(d);proxy=root/'proxy.txt';proxy.write_text('192.0.2.1:80:unique-fixture-user:unique-fixture-pass')
    config=s.default_scan_config();config.update(use_proxies=True,proxy_only=True,proxy_file=str(proxy))
    with patch('builtins.input',side_effect=choices),patch.object(pv,'validate_pool',new=AsyncMock(return_value=0)) as validation:
     result=s.prepare_proxy_pool(root,config)
    self.assertEqual(validation.await_count,checks)
    if expected=='back':self.assertIsNone(result);self.assertTrue(config['proxy_only'])
    else:self.assertFalse(result.proxy_only);self.assertFalse(config['use_proxies']);self.assertEqual(result.states,[])
 def test_report_resume_keeps_metadata_and_existing_fields(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);rec=s.SuccessRecorder(root/'sucess_find.json',target_user='wanted',input_path=root/'starting_dataset.json',started_at_utc=s.utc_iso())
   rec.record_match(s.ProfileJob('a','https://www.tiktok.com/@a',()),'followers',{'username':'wanted','id':'1'})
   rec.finalize(scan_complete=False);path=report.generate_report(root,emit=lambda x:None)
   before=json.loads(rec.path.read_text());rec=s.SuccessRecorder(rec.path,target_user='wanted',input_path=root/'starting_dataset.json',started_at_utc=s.utc_iso());rec.finalize(scan_complete=False)
   after=json.loads(rec.path.read_text());self.assertEqual(after['report'],before['report']);self.assertIsNone(after['profiles'][0]['relationship']['mutual']);self.assertTrue(path.exists())
 def test_python311_syntax_all_runtime_files(self):
  for file in ['main.py','tiktok_worker_scanner.py','report_generator.py','proxy_validation.py','avatar_cache.py']:
   ast.parse(Path(file).read_text(),filename=file,feature_version=(3,11))
 def test_actual_entry_completes_scan_and_report(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);(root/'input.json').write_text('["a"]')
   env=dict(os.environ);env.update(TIKTOK_INPUT_JSON=str(root/'input.json'),TIKTOK_EXPORT_DIR=str(root/'logs'),TIKTOK_PROXY_ONLY='0')
   env['PYTHONPATH']=os.pathsep.join(str(Path(x).resolve()) for x in sys.path if x)
   script="""import runpy,httpx
from unittest.mock import patch
import tiktok_worker_scanner as s
from test_configured_scanner import response_profile
from test_async_scanner import page
original=s.WorkerApiClient
def factory(gate,pool,**kwargs):
 def make(route,options):
  def handle(req):
   return httpx.Response(200,json=response_profile('a',1) if req.url.path=='/' else page())
  return httpx.AsyncClient(transport=httpx.MockTransport(handle))
 return original(gate,pool,client_factory=make,**kwargs)
with patch.object(s,'WorkerApiClient',side_effect=factory):runpy.run_path(%r,run_name='__main__')
""" % str(Path('main.py').resolve())
   completed=subprocess.run([sys.executable,'-c',script],input='0\n1\nwanted\n0\n0\n1\n4\n0\n0\nn\n1\n',text=True,capture_output=True,env=env,timeout=15)
   self.assertEqual(completed.returncode,0,completed.stdout+completed.stderr)
   self.assertTrue((root/'logs'/'wanted'/'report_wanted.html').exists())
   self.assertTrue(json.loads((root/'logs'/'wanted'/'scan_state.json').read_text())['scan_complete'])
 def test_actual_main_entry(self):
  with tempfile.TemporaryDirectory() as d:
   env=dict(os.environ);env['TIKTOK_EXPORT_DIR']=str(Path(d)/'logs')
   result=subprocess.run([sys.executable,str(Path('main.py').resolve())],input='q\n',capture_output=True,text=True,env=env,timeout=10,cwd=d)
   self.assertEqual(result.returncode,0,result.stdout+result.stderr);self.assertIn('TikTok',result.stdout)

class ExtraRecoveryChecks(unittest.IsolatedAsyncioTestCase):
 async def test_target_alias_duplicate_keeps_observation_on_resume(self):
  with tempfile.TemporaryDirectory() as d,patch.object(s,'console'):
   root=Path(d);p=profile();rec=s.SuccessRecorder(root/'sucess_find.json',target_user='wanted',input_path=root/'input.json',started_at_utc=s.utc_iso());j=s.ProfileJob('example',p.profile_url,())
   client=AsyncFakeClient(p,{'followers':[page([member('oldname','777')],True,'next'),page([member('wanted','777')])],'following':[page()]})
   result=await s.process_profile(client,j,output_directory=root,success_recorder=rec,stop_event=asyncio.Event(),use_resume=True,store_directory=root/'pages')
   before=json.loads(rec.path.read_text())['profiles'][0]['match_observed_at_utc']
   replacement=AsyncFakeClient(p,{})
   result=await s.process_profile(replacement,j,output_directory=root,success_recorder=rec,stop_event=asyncio.Event(),use_resume=True,store_directory=root/'pages')
   self.assertEqual(result.target_found_in,['followers']);self.assertEqual(replacement.calls,[]);self.assertEqual(json.loads(rec.path.read_text())['profiles'][0]['match_observed_at_utc'],before)

class SignalChecks(unittest.TestCase):
 def test_ctrl_c_writes_report_and_preserves_pending_claim(self):
  import signal
  from test_persistent_scanner import ProcessTests
  with tempfile.TemporaryDirectory() as d:
   root=Path(d)
   with patch.object(signal,'SIGTERM',signal.SIGINT):ProcessTests().child(root,'sigterm',graceful=True)
   state_path=next(root.glob('*/scan_state.json'));state=json.loads(state_path.read_text());self.assertEqual(state['summary']['pending_profiles'],1)
   self.assertEqual(len(list(state_path.parent.glob('report_*.html'))),1)

if __name__=='__main__':unittest.main()
