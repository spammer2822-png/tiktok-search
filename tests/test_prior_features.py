import asyncio,copy,io,json,tempfile,unittest
from pathlib import Path
from contextlib import redirect_stdout
from unittest.mock import patch
import httpx
import tiktok_worker_scanner as s
import report_generator as report
import proxy_validation as pv
from test_configured_scanner import ConfigurationTests,response_profile,public_member
from test_async_scanner import page,profile,member,AsyncFakeClient

class CountBootstrapTests(ConfigurationTests):
 def test_seven_count_cases_reach_main_scan(self):
  # natural exact, mismatch followers, following one page, real failure,
  # followers mismatch+following exact, reverse, both mismatched.
  for case,(advertised,returned,fail) in enumerate([((2,2),(2,2),False),((367,12),(344,12),False),((0,12),(0,12),False),((367,12),(200,12),True),((3,2),(2,2),False),((2,3),(2,2),False),((3,3),(2,2),False)]):
   with self.subTest(case=case):
    self.calls.clear();target='wanted'+str(case)
    async def handler(req):
     name=req.url.params.get('username')
     if name:return httpx.Response(200,json=response_profile(name,9 if name=='seed' else 10000+int(name[1:]),*(map(str,advertised) if name=='seed' else ('0','0'))))
     if req.url.params['Uid']=='9':
      if req.url.params.get('minCursor')=='continue':return httpx.Response(418)
      idx=0 if req.url.path.endswith('followers') else 1
      return httpx.Response(200,json=page([public_member('a'+str(i),str(10000+i)) for i in range(returned[idx])],fail and idx==0,'continue' if fail and idx==0 else ''))
     return httpx.Response(200,json=page())
    code,out,_=self.run_main(['0','2','seed',target,'0','0','1','4','0','0','n','1'],handler)
    self.assertEqual(code,2 if fail else 0)
    looked_up=[u for u in self.calls if '?username=a' in u]
    self.assertEqual(len(looked_up),0 if fail else max(returned))
    if not fail:self.assertIn('Starting/resuming normal scan',out)
    self.assertTrue((self.base/target/f'report_{target}.html').exists())

class PriorTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
 async def asyncTearDown(self):self.tmp.cleanup()
 async def test_migrate_count_checkpoint_no_network(self):
  p=profile(followers='367');store=s.UserStore(self.root/'pages.sqlite3')
  r=s.ListExportResult('followers',367,unique_records_saved=344,endpoint_exhausted=True,stop_reason='count_discrepancy')
  store.save_checkpoint('followers',{'result':s.asdict(r),'next_cursor':'opaque'})
  client=AsyncFakeClient(p,{})
  out=await s.export_one_list(client,store,p,'followers',target_username='wanted',stop_event=asyncio.Event())
  self.assertTrue(out.usable);self.assertEqual(out.count_difference,-23);self.assertFalse(client.calls);store.close()
 async def test_invalid_records_not_hidden_by_count_mismatch(self):
  r=s.ListExportResult('followers',10,unique_records_saved=2,endpoint_exhausted=True,invalid_records_ignored=1);r.finish()
  self.assertFalse(r.complete);self.assertEqual(r.stop_reason,'invalid_records')
 async def test_validate_every_proxy_once_auth_dead_timeout_and_redaction(self):
  cfg=s.default_scan_config();cfg['workers']=4
  proxies=[s.parse_proxy_line(f'192.0.2.{i}:8000:secretuser{i}:secretpass{i}') for i in range(1,6)]
  pool=s.ProxyPool(proxies);seen=[];options=[];active=peak=0
  def factory(route,opts):
   options.append(opts)
   async def handler(req):
    nonlocal active,peak
    self.assertEqual(str(req.url),pv.TEST_URL);self.assertNotIn('proxy-authorization',req.headers)
    seen.append(route.label);active+=1;peak=max(active,peak)
    try:
     await asyncio.sleep(.005)
     if route.label=='proxy-003':raise httpx.ProxyError('407 secretuser3:secretpass3')
     if route.label=='proxy-004':raise httpx.ConnectError('secretpass4')
     if route.label=='proxy-005':raise httpx.ReadTimeout('secretpass5')
     return httpx.Response(200,text='203.0.113.1')
    finally:active-=1
   return httpx.AsyncClient(transport=httpx.MockTransport(handler))
  output=io.StringIO()
  with redirect_stdout(output):valid=await pv.validate_pool(s,pool,cfg,self.root,client_factory=factory)
  self.assertEqual(valid,2);self.assertEqual(len(set(seen)),5);self.assertEqual(len(seen),5);self.assertGreater(peak,1)
  self.assertEqual(len(pool.available()),2);self.assertTrue(all(opts['proxy'].auth for opts in options))
  text=output.getvalue()+(self.root/'proxy_validation.json').read_text();self.assertNotIn('secretuser',text);self.assertNotIn('secretpass',text)
  self.assertEqual(pool.states[2].last_failure,'proxy_authentication_failure')
 async def test_one_proxy_and_skip_validation(self):
  cfg=s.default_scan_config();cfg.update(use_proxies=True);cfg['proxy_file']=str(self.root/'proxy.txt');Path(cfg['proxy_file']).write_text('192.0.2.1:80:user:pass')
  with patch('builtins.input',return_value='2'),patch.object(pv,'validate_pool',side_effect=AssertionError('skip')):pool=s.prepare_proxy_pool(self.root,cfg)
  self.assertEqual(len(pool.available()),1)
 async def test_report_safe_offline_atomic_metadata(self):
  s.atomic_write_json(self.root/'scan_config.json',s.default_scan_config());s.atomic_write_json(self.root/'scan_state.json',{'summary':{},'scan_complete':False})
  success={'target_user':'wanted','summary':{'profiles_with_target':1},'mutuals':[], 'profiles':[{'username':'a','found_in':['followers'],'nickname':'</script><script>alert(1)</script>'}]}
  s.atomic_write_json(self.root/'sucess_find.json',success)
  path=report.generate_report(self.root,emit=lambda x:None);self.assertIsNotNone(path);text=path.read_text()
  self.assertNotIn('</script><script>alert(1)',text);self.assertNotIn('Download JSON',text);self.assertIn("connect-src 'none'",text)
  doc=json.loads((self.root/'sucess_find.json').read_text());self.assertEqual(doc['summary'],success['summary']);self.assertEqual(doc['profiles'],success['profiles']);self.assertEqual(doc['report']['filename'],path.name)
  old=path.read_bytes()
  with patch.object(report.os,'replace',side_effect=OSError('sensitive')):self.assertIsNone(report.generate_report(self.root,emit=lambda x:None))
  self.assertEqual(path.read_bytes(),old);self.assertFalse(list(self.root.glob('*.tmp')))

if __name__=='__main__':unittest.main()
