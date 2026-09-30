"""Reproducible LOCAL benchmarks. Never contacts TikTok, Fintok or Webshare.

python tests/benchmark_optimization.py report --output <new-directory> --accounts 180000
python tests/benchmark_optimization.py scanner --output <new-directory> --accounts 500 --workers 10000
--module-dir can point to an extracted older version for an apples-to-apples comparison.
"""
import argparse
import asyncio
import json
import platform
from pathlib import Path
import sys
import time
from unittest.mock import patch

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('mode',choices=['report','scanner'])
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--accounts',type=int,default=500)
parser.add_argument('--workers',type=int,default=10000)
parser.add_argument('--module-dir',type=Path,default=Path(__file__).resolve().parents[1])
args=parser.parse_args()
sys.path.insert(0,str(args.module_dir.resolve()))
import tiktok_worker_scanner as s
import report_generator
root=args.output.resolve();root.mkdir(parents=True,exist_ok=False)
cfg=s.default_scan_config();cfg.update(workers=args.workers,target_username='wanted')
metadata={'input_imported':True,'scan_complete':False,'created_at_utc':s.utc_iso()}
state=s.DurableScanState(root,metadata,root,cfg)
measurement={'mode':args.mode,'accounts':args.accounts,'workers':args.workers,'python':platform.python_version(),
             'platform':platform.platform(),'source':str(args.module_dir.resolve()),'network':'mock only'}

def memory():
    try:
        import resource
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1024*1024 if sys.platform=='darwin' else 1024),2)
    except ImportError:return None

def make_job(i):
    name=f'synthetic{i:06d}'
    return s.ProfileJob(name,'https://www.tiktok.com/@'+name,('fixture',),str(7100000000000000000+i))

if args.mode=='report':
    # Rich but synthetic rows; no real account data and no fictitious API measurements.
    def rows():
        for i in range(args.accounts):
            job=make_job(i);kind=('completed','completed','completed','completed','completed','completed','partial','private','skipped_size','restricted')[i%10]
            profile={'username':job.username,'uid':job.uid,'display_name':'Synthetic account '+str(i),
                     'follower_count':i*17%200000,'following_count':i*13%10000,'avatar_url':'',
                     'advertised_counts':{'followers':str(i*17%200000),'following':str(i*13%10000)}}
            if i==1:profile['display_name']='</script><script>window.EXFILTRATED=1</script>'
            good={'complete':True,'endpoint_exhausted':True,'unique_records_saved':0,'stop_reason':'complete_exact','retry_count':0,'warnings':[]}
            lists={name:{**good} for name in s.SELECTED_LISTS}
            if kind=='partial':lists['followers'].update(complete=False,endpoint_exhausted=False,stop_reason='network_timeout',error='Synthetic timeout')
            if kind=='restricted':lists['following'].update(complete=False,endpoint_exhausted=False,stop_reason='list_restricted')
            result={'username':job.username,'status':kind,'complete':kind=='completed','list_results':lists,
                    'metadata':{'profile':profile,'reason':'Size limit exceeded' if kind=='skipped_size' else '', 'profile_retries':0}}
            yield(job.username,json.dumps(s.asdict(job)),kind,json.dumps(result),s.utc_iso(),job.uid)
    before=time.perf_counter()
    with state.connection:
        state.connection.executemany('INSERT INTO jobs(username,job,status,result,updated,uid) VALUES(?,?,?,?,?,?)',rows())
        state.connection.executemany('INSERT INTO discovery_sources VALUES(?,?,?)',
                                    ((make_job(i).username,'fixture_source','followers') for i in range(args.accounts)))
    state.snapshot();state.close()
    matches=[{'username':make_job(i).username,'found_in':['followers','following'] if i%2000==0 else ['followers']} for i in range(0,args.accounts,1000)]
    s.atomic_write_json(root/s.SUCCESS_FILE_NAME,{'target_user':'wanted','profiles':matches,'summary':{},'mutuals':[]})
    measurement['fixture_seconds']=round(time.perf_counter()-before,3)
    before=time.perf_counter()
    path=report_generator.generate_report(root,clean=s.REDACTOR.clean,emit=lambda _:None)
    if path is None:raise RuntimeError('Report generation failed')
    measurement.update(generation_seconds=round(time.perf_counter()-before,3),html_bytes=path.stat().st_size,
                       peak_rss_mb=memory(),report=str(path))
else:
    import httpx
    state.add_jobs(make_job(i) for i in range(args.accounts))
    success=s.SuccessRecorder(root/s.SUCCESS_FILE_NAME,target_user='wanted',input_path=root/'input.json',started_at_utc=s.utc_iso())
    original=s.WorkerApiClient
    peak_tasks=peak_requests=requests=active=0;lags=[];done=asyncio.Event()
    def factory(gate,pool,**kwargs):
        async def handle(request):
            global requests,active,peak_requests,peak_tasks
            requests+=1;active+=1;peak_requests=max(active,peak_requests)
            peak_tasks=max(peak_tasks,len(asyncio.all_tasks()))
            try:
                await asyncio.sleep(.02)  # Fixed 20ms latency; NOT real service speed.
                name=request.url.params.get('username')
                if name:
                    payload={'status':'ok','data':{'username':name,'userid':str(7100000000000000000+int(name[9:])),
                            'private':'Public Account','fullname':'Synthetic','profile':'','followers':'0','following':'0','see_following':'Yes'}}
                else:payload={'users':[],'hasMore':False,'minCursor':''}
                return httpx.Response(200,json=payload)
            finally:active-=1
        def clients(route,opts):return httpx.AsyncClient(transport=httpx.MockTransport(handle),event_hooks=opts.get('event_hooks',{}))
        return original(gate,pool,client_factory=clients,**kwargs)
    async def ticker():
        while not done.is_set():
            then=time.perf_counter();await asyncio.sleep(.01);lags.append(max(0,time.perf_counter()-then-.01))
    async def run():
        task=asyncio.create_task(ticker())
        try:
            with patch.object(s,'WorkerApiClient',side_effect=factory),patch.object(s,'console'):
                return await s.run_scan([],output_directory=root,worker_count=args.workers,pacing=(0,0),success_recorder=success,state_recorder=state,use_resume=True)
        finally:done.set();await task
    before=time.perf_counter();result=asyncio.run(run());elapsed=time.perf_counter()-before
    if result[2] or state.summary()['completed_overall']!=args.accounts:raise RuntimeError('Synthetic scan incomplete')
    state.close();lags.sort()
    measurement.update(scan_seconds=round(elapsed,3),mock_requests=requests,accounts_per_second=round(args.accounts/elapsed,2),
        mock_requests_per_second=round(requests/elapsed,2),peak_tasks=peak_tasks,peak_requests=peak_requests,
        event_loop_p95_lag_ms=round(lags[int(len(lags)*.95)]*1000,2),event_loop_max_lag_ms=round(max(lags)*1000,2),peak_rss_mb=memory())
(root/'benchmark.json').write_text(json.dumps(measurement,indent=2),encoding='utf-8')
print(json.dumps(measurement,indent=2))
