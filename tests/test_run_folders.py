import asyncio, copy, io, json, os, re, subprocess, sys, tempfile, unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from unittest.mock import patch
import httpx
import tiktok_worker_scanner as s
from test_async_scanner import PROFILE, member, page, profile, AsyncFakeClient

class FolderTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.base=self.root/'logs';self.input=self.root/'input.json';self.input.write_text('["user0"]', encoding="utf-8")
  self.old_redactor=s.REDACTOR
 def tearDown(self):s.REDACTOR=self.old_redactor;self.temp.cleanup()
 def config(self):return {'TIKTOK_EXPORT_DIR':str(self.base),'TIKTOK_INPUT_JSON':str(self.input),'TIKTOK_PROXY_ONLY':'0','TIKTOK_MIN_DELAY_SECONDS':'0','TIKTOK_MAX_DELAY_SECONDS':'0'}
 def scan(self,answers,handler,raw=False):
  Original=s.WorkerApiClient;events=[]
  def factory(gate,pool,**kwargs):
   def client_factory(route,options):
    async def respond(req):
     events.append(str(req.url));return await handler(req)
    return httpx.AsyncClient(transport=httpx.MockTransport(respond))
   return Original(gate,pool,client_factory=client_factory,**kwargs)
  with patch.dict(os.environ,self.config()),patch('builtins.input',side_effect=answers),patch.object(s,'WorkerApiClient',side_effect=factory),patch.object(s,'KEEP_RAW_MEMBER_DATA',raw),patch.object(s,'FETCH_ATTEMPTS',1),redirect_stdout(io.StringIO()):
   code=s.main()
  return code,events
 async def empty_handler(self,req):
  name=req.url.params.get('username')
  if name:
   value=copy.deepcopy(PROFILE);value['data'].update(username=name,followers='0',following='0');return httpx.Response(200,json=value)
  return httpx.Response(200,json=page())
 def test_default_parent_and_short_windows_safe_names(self):
  with patch.dict(os.environ,{'TIKTOK_EXPORT_DIR':''}):self.assertEqual(str(s.configured_output_directory(self.input)),r'C:\Users\vailo\Downloads\TiktokSearch_Logs')
  for name in ['../escape','a/b\\c:*?"<>|\x00\n','CON','nul.txt','LPT1','COM¹','...','x '*100]:
   result=s.sanitize_run_name(name);self.assertTrue(result);self.assertLessEqual(len(result),61);self.assertIsNone(re.search(r'[<>:"/\\|?*\x00-\x1f]',result));self.assertEqual(result,result.rstrip(' .'));self.assertNotIn(result.casefold(),{'con','lpt1','com¹','nul.txt','..'})
  self.assertEqual(s.sanitize_run_name('@AmLie7951'),'amlie7951')
 def test_existing_files_and_folders_are_never_overwritten(self):
  self.base.mkdir();(self.base/'amlie7951').write_text('keep', encoding="utf-8");(self.base/'amlie7951_2').mkdir()
  created=s.create_search_directory(self.base,'amlie7951');self.assertEqual(created.name,'amlie7951_3');self.assertEqual((self.base/'amlie7951').read_text(encoding="utf-8"),'keep')
 def test_multiple_processes_get_unique_sequential_folders(self):
  code='import sys; from pathlib import Path; from tiktok_worker_scanner import create_search_directory; print(create_search_directory(Path(sys.argv[1]), "amlie7951").name)'
  def run(_):return subprocess.check_output([sys.executable,'-c',code,str(self.base)],text=True).strip()
  with ThreadPoolExecutor(8) as pool:names=list(pool.map(run,range(16)))
  self.assertEqual(len(set(names)),16);self.assertEqual(set(names),{'amlie7951'}|{f'amlie7951_{i}' for i in range(2,17)})
 def test_run_and_error_logs_redact_credentials_and_append(self):
  folder=s.create_search_directory(self.base,'amlie7951');proxy=s.ProxyConfig('test.example',80,'privateusername','privatepassword');s.REDACTOR=s.CredentialRedactor([proxy])
  with redirect_stdout(io.StringIO()),s.RunLog(folder):s.console('ERROR: http://privateusername:privatepassword@test.example:80',error=True)
  before=(folder/'run.log').read_bytes()
  with redirect_stdout(io.StringIO()),s.RunLog(folder):s.console('resumed')
  self.assertTrue((folder/'run.log').read_bytes().startswith(before))
  for path in [folder/'run.log',folder/'errors.log']:
   content=path.read_text(encoding="utf-8");self.assertNotIn(proxy.username,content);self.assertNotIn(proxy.password,content)
 def test_nonpersistent_profile_temporary_files_stay_in_search(self):
  folder=s.create_search_directory(self.base,'amlie7951');success=s.SuccessRecorder(folder/s.SUCCESS_FILE_NAME,target_user=s.TARGET_USER,input_path=self.input,started_at_utc=s.utc_iso());job=s.ProfileJob('user0','https://www.tiktok.com/@user0',('profiles',));client=AsyncFakeClient(profile('user0'),{'followers':[page()],'following':[page()]})
  original=tempfile.TemporaryDirectory;seen=[]
  def factory(*args,**kwargs):
   self.assertTrue(Path(kwargs['dir']).is_relative_to(folder));seen.append(kwargs['dir']);return original(*args,**kwargs)
  with patch.object(s.tempfile,'TemporaryDirectory',side_effect=factory),redirect_stdout(io.StringIO()):
   result=asyncio.run(s.process_profile(client,job,output_directory=folder,success_recorder=success,stop_event=asyncio.Event(),use_resume=True))
  self.assertTrue(result.complete);self.assertEqual(len(seen),1)

if __name__=='__main__':unittest.main(verbosity=2)
