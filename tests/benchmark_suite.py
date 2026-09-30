"""Controlled synthetic workload; no external requests. Run each sample in a fresh process."""
import argparse, asyncio, contextlib, functools, gc, json, math, os, platform
import shutil, statistics, sys, tempfile, threading, time
try:
    import resource
except ImportError:
    resource = None
from pathlib import Path
from unittest.mock import patch

p=argparse.ArgumentParser()
p.add_argument('mode',choices=['scan','micro','report'])
p.add_argument('--source',type=Path,required=True)
p.add_argument('--result',type=Path,required=True)
p.add_argument('--workers',type=int,default=2500)
p.add_argument('--accounts',type=int,default=96)
p.add_argument('--pages',type=int,default=12)
p.add_argument('--latency',type=float,default=.02)
p.add_argument('--routes',type=int,default=0)
p.add_argument('--console',choices=['normal','minimal','disabled'],default='normal')
p.add_argument('--keep',action='store_true')
p.add_argument('--profile',action='store_true')
p.add_argument('--io-workers',type=int,default=None)
a=p.parse_args()
boot=time.perf_counter();sys.path.insert(0,str(a.source.resolve()))
import tiktok_worker_scanner as s
import report_generator as report
import httpx
startup=time.perf_counter()-boot
if a.profile:
    import cProfile
    profiler=cProfile.Profile();profiler.enable()
root=Path(tempfile.mkdtemp(prefix=a.mode+'_',dir=a.result.parent))
timings={};counts={};timing_lock=threading.Lock()
def timed(obj,name,label=None):
    old=getattr(obj,name);key=label or name
    @functools.wraps(old)
    def wrap(*args,**kwargs):
        then=time.perf_counter()
        try:return old(*args,**kwargs)
        finally:
            with timing_lock:
                timings[key]=timings.get(key,0)+time.perf_counter()-then
                counts[key]=counts.get(key,0)+1
    setattr(obj,name,wrap)
for obj,name,label in [(s,'atomic_write_json','atomic_save'),(s,'write_final_json','final_export'),
    (s.UserStore,'save_batch','member_parse_dedup'),(s.DurableScanState,'claim','claim'),
    (s.DurableScanState,'record','record'),(s.DurableScanState,'summary','summary'),
    (s.DurableScanState,'snapshot','checkpoint'),(s.DurableScanState,'recover','recover'),
    (s.DurableScanState,'finalize','state_finalization')]:timed(obj,name,label)
cfg=s.default_scan_config();cfg.update(workers=a.workers,target_username='wanted',keep_raw=True,
    scan_mode='double_phase',double_phase_enabled=True,current_phase=2)
# Match the user's saved configuration: no explicit max_connections override.
cfg.pop('max_connections',None)
if a.io_workers is not None:cfg['io_workers']=a.io_workers
s.KEEP_RAW_MEMBER_DATA=True
meta={'input_imported':True,'scan_complete':False,'created_at_utc':s.utc_iso(),'current_phase':2}
base_uid=7100000000000000000
def job(i):return s.ProfileJob(f'bench{i:06d}',f'https://www.tiktok.com/@bench{i:06d}',('fixture',),str(base_uid+i),2)
def member(i):return {'uniqueId':f'account{i:08d}','user_id':str(7200000000000000000+i),
    'nickname':'Synthetic person '+str(i),'avatarThumb':'','signature':'Synthetic bio for reproducible pagination',
    'privateAccount':False,'verified':'No❌'}
def profile(i):return {'status':'ok','data':{'username':job(i).username,'userid':job(i).uid,
    'fullname':'Synthetic person '+str(i),'private':'Public Account','profile':'','followers':str(a.pages*20),
    'following':str(a.pages*20),'see_following':'Yes','verified':'No❌','secUid':'synthetic-sec-'+str(i)}}
doc={'schema':1,'mode':a.mode,'version':a.source.parent.name,'python':platform.python_version(),
     'platform':platform.platform(),'workers':a.workers,'accounts':a.accounts,'pages_per_direction':a.pages,
     'members_per_page':20,'raw_enabled':True,'latency_model_seconds':a.latency,'proxy_routes':a.routes,
     'network':'HTTPX MockTransport; no TCP/TLS/service measurements','startup_seconds':startup}
cpu_start=time.process_time();wall_start=time.perf_counter()
with (root/'terminal.txt').open('w',encoding='utf-8') as terminal,contextlib.redirect_stdout(terminal),s.RunLog(root):
    old_console=s.console
    if a.console=='disabled':s.console=lambda *args,**kwargs:None
    elif a.console=='minimal':
        def minimal(message='',**kwargs):
            if any(tag in message for tag in ('[LIVE]','[SHUTDOWN]','[TARGET FOUND]','RATE LIMIT')):old_console(message,**kwargs)
            elif s._RUN_LOG is not None:s._RUN_LOG.write(s.REDACTOR.text(message),error=kwargs.get('error',False))
        s.console=minimal
    if a.mode=='scan':
        t=time.perf_counter();state=s.DurableScanState(root,meta,root,cfg);state.add_jobs((job(i) for i in range(a.accounts)),phase=2)
        s.atomic_write_json(root/'scan_config.json',cfg);state.snapshot()
        doc['dataset_import_seconds']=time.perf_counter()-t
        success=s.SuccessRecorder(root/'sucess_find.json',target_user='wanted',input_path=root/'input.json',started_at_utc=s.utc_iso())
        original=s.WorkerApiClient
        requests=active=peak=peak_tasks=0;latencies=[];lags=[];cpu_samples=[];peak_queue=0
        claim_wait=[];scan_started=[0.0];gates=[];done=asyncio.Event()
        claim_impl=state.claim
        def claim():
            value=claim_impl()
            if value:claim_wait.append(time.perf_counter()-scan_started[0])
            return value
        state.claim=claim
        payloads={}
        for page in range(a.pages):
            payloads[page]=json.dumps({'users':[member(page*20+n) for n in range(20)],
                'hasMore':page+1<a.pages,'minCursor':f'opaque/{page+1}+cursor=' if page+1<a.pages else ''}).encode()
        def factory(gate,pool,**kwargs):
            gates.append(gate)
            async def handle(req):
                global requests,active,peak,peak_tasks
                then=time.perf_counter();requests+=1;active+=1;peak=max(peak,active)
                peak_tasks=max(peak_tasks,len(asyncio.all_tasks()))
                try:
                    await asyncio.sleep(a.latency)
                    if req.url.path=='/':data=json.dumps(profile(int(req.url.params['username'][5:]))).encode()
                    else:
                        cursor=req.url.params['minCursor'];n=0 if cursor=='0' else int(cursor.split('/')[1].split('+')[0]);data=payloads[n]
                    return httpx.Response(200,content=data,headers={'content-type':'application/json'})
                finally:latencies.append(time.perf_counter()-then);active-=1
            def clients(route,opts):
                return httpx.AsyncClient(transport=httpx.MockTransport(handle),event_hooks=opts.get('event_hooks',{}),trust_env=False)
            return original(gate,pool,client_factory=clients,**kwargs)
        async def ticker():
            prior=time.perf_counter();prior_cpu=time.process_time()
            while not done.is_set():
                await asyncio.sleep(.02);now=time.perf_counter();cpu=time.process_time()
                lags.append(max(0,now-prior-.02));cpu_samples.append((cpu-prior_cpu)/(now-prior)*100)
                prior,prior_cpu=now,cpu
        async def run():
            tick=asyncio.create_task(ticker())
            pool=s.ProxyPool([s.ProxyConfig(f'p{i}.example',8000+i,'fixture-user','fixture-password') for i in range(a.routes)])
            try:
                with patch.object(s,'WorkerApiClient',side_effect=factory):
                    return await s.run_scan([],output_directory=root,worker_count=a.workers,pacing=(0,0),
                        success_recorder=success,state_recorder=state,use_resume=True,pool=pool)
            finally:done.set();await tick
        scan_started[0]=time.perf_counter();result=asyncio.run(run());scan_seconds=time.perf_counter()-scan_started[0]
        assert not result[2],result[2]
        assert state.summary()['completed_overall']==a.accounts,state.summary()
        assert requests==a.accounts*(1+2*a.pages)
        t=time.perf_counter();path=report.generate_report(root,clean=s.REDACTOR.clean,emit=s.console)
        assert path;doc['html_generation_seconds']=time.perf_counter()-t;doc['html_bytes']=path.stat().st_size
        t=time.perf_counter();state.recover(success);doc['resume_seconds']=time.perf_counter()-t
        assert state.summary()['completed_overall']==a.accounts
        state.close()
        def quant(values,q):return sorted(values)[min(len(values)-1,int(len(values)*q))] if values else None
        doc.update(scan_seconds=scan_seconds,total_requests=requests,successful_requests=requests,failed_requests=0,
            requests_per_second=requests/scan_seconds,successful_requests_per_second=requests/scan_seconds,
            failed_requests_per_second=0,accounts_per_second=a.accounts/scan_seconds,accounts_per_minute=60*a.accounts/scan_seconds,
            latency_mean_ms=statistics.mean(latencies)*1000,latency_median_ms=statistics.median(latencies)*1000,
            latency_p95_ms=quant(latencies,.95)*1000,latency_p99_ms=quant(latencies,.99)*1000,
            event_loop_p95_ms=quant(lags,.95)*1000,event_loop_max_ms=max(lags)*1000,
            peak_cpu_percent_one_core=max(cpu_samples),peak_in_flight=peak,observed_tasks_at_dispatch=peak_tasks,
            mean_initial_queue_wait_ms=statistics.mean(claim_wait)*1000,queue_size_initial=a.accounts,
            connection_ceiling=gates[0].ceiling,effective_concurrency_final=gates[0].limit,
            retry_attempts=0,timeouts=0,console_mode=a.console)
    else:
        # Sparse large scan: most rows still pending, matching the supplied run.
        state=s.DurableScanState(root,meta,root,cfg)
        t=time.perf_counter()
        with state.connection:
            state.connection.executemany('INSERT INTO jobs(username,job,status,updated,uid,phase) VALUES(?,?,?,?,?,?)',
                ((job(i).username,json.dumps(s.asdict(job(i))),'partial' if i%50==0 else 'pending',s.utc_iso(),job(i).uid,2) for i in range(a.accounts)))
            state.connection.executemany('INSERT INTO discoveries(username,payload,discovered_at,uid,public,phase) VALUES(?,?,?,?,?,?)',
                ((job(i).username,json.dumps({'username':job(i).username,'display_name':'Synthetic '+str(i),'avatar_url':'','id':job(i).uid}),s.utc_iso(),job(i).uid,1,1) for i in range(a.accounts)))
            state.connection.executemany('INSERT INTO discovery_sources VALUES(?,?,?)',
                ((job(i).username,'source'+str(n),'followers') for i in range(a.accounts) for n in range(3)))
        doc['fixture_seconds']=time.perf_counter()-t
        s.atomic_write_json(root/'scan_config.json',cfg);state.snapshot()
        success=s.SuccessRecorder(root/'sucess_find.json',target_user='wanted',input_path=root/'input.json',started_at_utc=s.utc_iso())
        t=time.perf_counter();state.recover(success);doc['resume_seconds']=time.perf_counter()-t
        samples=[]
        for _ in range(20):
            t=time.perf_counter();state.snapshot();samples.append(time.perf_counter()-t)
        doc['checkpoint_mean_seconds']=statistics.mean(samples)
        t=time.perf_counter()
        for _ in range(100):state.summary()
        doc['summary_mean_seconds']=(time.perf_counter()-t)/100
        t=time.perf_counter()
        doc['retry_candidates']=state.connection.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('partial','network_error','response_timeout')").fetchone()[0]
        doc['failed_lookup_seconds']=time.perf_counter()-t
        if a.mode=='report':
            t=time.perf_counter();path=report.generate_report(root,clean=s.REDACTOR.clean,emit=s.console)
            assert path;doc['html_generation_seconds']=time.perf_counter()-t;doc['html_bytes']=path.stat().st_size;doc['report_path']=str(path)
        state.close()
    doc['total_seconds']=time.perf_counter()-wall_start
    doc['cpu_seconds']=time.process_time()-cpu_start
    doc['cpu_percent_one_core']=doc['cpu_seconds']/doc['total_seconds']*100
    doc['peak_rss_mib']=(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1048576 if sys.platform=='darwin' else 1024) if resource else None)
    doc['call_inclusive_seconds']=timings;doc['call_counts']=counts
if a.profile:
    profiler.disable();profiler.dump_stats(str(a.result)+'.prof')
a.result.write_text(json.dumps(doc,indent=2),encoding='utf-8')
print(json.dumps({k:v for k,v in doc.items() if k not in ('call_counts','call_inclusive_seconds')},indent=2))
if not a.keep:shutil.rmtree(root,ignore_errors=True)
