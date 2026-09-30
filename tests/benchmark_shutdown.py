"""Measure real subprocess SIGINT, durable state/report and same-folder recovery."""
import argparse,json,os,selectors,signal,subprocess,sys,tempfile,time
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--source',type=Path,required=True);p.add_argument('--result',type=Path,required=True);p.add_argument('--repeat',action='store_true');a=p.parse_args()
root=Path(tempfile.mkdtemp(prefix='stop_',dir=a.result.parent));script=r'''
import asyncio,json,os,sys,time
from pathlib import Path
from unittest.mock import patch
import httpx
import tiktok_worker_scanner as s
if os.environ.get('BENCH_REPEAT_SIGNALS') == '1':
 import report_generator as report
 old_report=report.generate_report
 def slow_report(*args,**kwargs):
  time.sleep(.25)  # Ensure repeated signals arrive DURING cleanup, not after exit.
  return old_report(*args,**kwargs)
 report.generate_report=slow_report
root=Path(sys.argv[1]);(root/'input.json').write_text(json.dumps(['account'+str(i) for i in range(1000)]))
os.environ.update(TIKTOK_INPUT_JSON=str(root/'input.json'),TIKTOK_EXPORT_DIR=str(root),TIKTOK_PROXY_ONLY='0',TIKTOK_MIN_DELAY_SECONDS='0',TIKTOK_MAX_DELAY_SECONDS='0')
s.console=lambda *a,**k:None
old=s.WorkerApiClient;started=False
async def handler(req):
 global started
 if not started:started=True;print('READY',flush=True)
 await asyncio.sleep(1000)
def factory(gate,pool,**kwargs):
 def clients(route,opts):return httpx.AsyncClient(transport=httpx.MockTransport(handler),event_hooks=opts.get('event_hooks',{}))
 return old(gate,pool,client_factory=clients,**kwargs)
with patch('builtins.input',side_effect=['0','1','','0','0','1','10000','0','0','n','1']),patch.object(s,'WorkerApiClient',side_effect=factory):raise SystemExit(s.main())
'''
env=dict(os.environ);env['PYTHONPATH']=str(a.source.resolve());env['BENCH_REPEAT_SIGNALS']='1' if a.repeat else '0'
child=subprocess.Popen([sys.executable,'-u','-c',script,str(root.resolve())],cwd=a.source.resolve(),env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
try:
 selector=selectors.DefaultSelector();selector.register(child.stdout,selectors.EVENT_READ)
 if not selector.select(30):raise RuntimeError('No READY')
 assert child.stdout.readline().strip()=='READY'
 t=time.perf_counter();child.send_signal(signal.SIGINT)
 if a.repeat:
  for _ in range(4):time.sleep(.02);child.send_signal(signal.SIGINT)
 stdout,stderr=child.communicate(timeout=60);elapsed=time.perf_counter()-t
 checkpoints=list(root.glob('*/scan_state.json'));state=json.loads(checkpoints[0].read_text()) if checkpoints else {}
 reports=list(root.glob('*/report_*.html'))
 out={'version':a.source.parent.name,'repeat':a.repeat,'injected_report_delay_seconds':.25 if a.repeat else 0,'shutdown_seconds':elapsed,'exit_code':child.returncode,'report_count':len(reports),'pending':state.get('summary',{}).get('pending_profiles'),'stderr_type':'traceback' if stderr else 'none'}
 assert child.returncode==130 and len(reports)==1 and out['pending']==1000,out
 sys.path.insert(0,str(a.source.resolve()));import tiktok_worker_scanner as s
 run=checkpoints[0].parent;cfg=json.loads((run/'scan_config.json').read_text());resume=s.DurableScanState(run,state,run,cfg)
 success_meta=json.loads((run/'sucess_find.json').read_text())
 success=s.SuccessRecorder(run/'sucess_find.json',target_user=cfg['target_username'],input_path=Path(success_meta['source_input_json']),started_at_utc=s.utc_iso())
 t=time.perf_counter();resume.recover(success);out['resume_seconds']=time.perf_counter()-t;assert resume.summary()['pending_profiles']==1000;resume.close()
 a.result.write_text(json.dumps(out,indent=2));print(json.dumps(out))
finally:
 if child.poll() is None:child.kill();child.communicate()
 child.stdout.close();child.stderr.close()
 import shutil;shutil.rmtree(root,ignore_errors=True)
